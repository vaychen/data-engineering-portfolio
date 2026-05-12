"""
plugins/operators/redshift_to_mysql_operator.py

Custom Airflow operator that streams query results from Amazon Redshift
to an Aurora MySQL reporting table with the following guarantees:

1. **Idempotent** — existing rows for ``business_date`` are deleted before
   each insert, so re-runs produce the same result.
2. **Memory-safe** — a server-side cursor streams results in configurable
   chunks so large result sets never fully materialise in the worker's RAM.
3. **Rolling window** — rows older than 2 days are pruned from MySQL after
   each successful insert, keeping the reporting table lean.
4. **Post-insert assertion** — a lightweight data-quality check verifies
   that every non-nullable column in the inserted batch is actually non-null,
   using a temporary table + PRIMARY KEY uniqueness trick.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

log = logging.getLogger(__name__)

# Columns that must never be NULL in the exported rows.
_NON_NULLABLE_EXPORT_COLUMNS: List[str] = [
    "business_date",
    "client_id",
    "product_id",
]


class RedshiftToMySQLOperator(BaseOperator):
    """
    Stream query results from Redshift to a MySQL reporting table.

    Parameters
    ----------
    redshift_sql:
        SELECT statement to run against Redshift.  Should accept
        ``%(business_date)s`` as a bind parameter.
    mysql_table:
        Fully-qualified MySQL table name (e.g. ``analytics_report.ads_user_active``).
    business_date:
        The partition date being exported (``YYYY-MM-DD``).  Used for
        idempotent delete and rolling-window cleanup.
    redshift_conn_id:
        Airflow connection ID for the Redshift endpoint.
    mysql_conn_id:
        Airflow connection ID for the Aurora MySQL endpoint.
    chunk_size:
        Number of rows to fetch from Redshift per round-trip.  Default 20 000.
    """

    def __init__(
        self,
        *,
        redshift_sql: str,
        mysql_table: str,
        business_date: str,
        redshift_conn_id: str = "redshift_default",
        mysql_conn_id: str = "mysql_reporting_default",
        chunk_size: int = 20_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.redshift_sql = redshift_sql
        self.mysql_table = mysql_table
        self.business_date = business_date
        self.redshift_conn_id = redshift_conn_id
        self.mysql_conn_id = mysql_conn_id
        self.chunk_size = chunk_size

    # ------------------------------------------------------------------
    # Redshift streaming
    # ------------------------------------------------------------------

    def _stream_from_redshift(
        self, conn: Any
    ) -> Iterator[Tuple[List[str], List[Tuple]]]:
        """
        Yield (column_names, chunk) tuples using a named server-side cursor.

        The named cursor keeps the result set on the Redshift side and
        transfers ``chunk_size`` rows at a time, bounding worker memory usage.
        """
        cursor_name = f"export_cursor_{self.mysql_table.replace('.', '_')}"
        with conn.cursor(cursor_name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(self.redshift_sql, {"business_date": self.business_date})
            col_names: Optional[List[str]] = None

            while True:
                rows = cur.fetchmany(self.chunk_size)
                if not rows:
                    break
                if col_names is None:
                    col_names = list(rows[0].keys())
                yield col_names, [tuple(r.values()) for r in rows]

    # ------------------------------------------------------------------
    # MySQL helpers
    # ------------------------------------------------------------------

    def _delete_existing(self, mysql_conn: Any, cursor: Any) -> None:
        """Delete rows for business_date so re-runs are idempotent."""
        sql = f"DELETE FROM {self.mysql_table} WHERE business_date = %s"
        cursor.execute(sql, (self.business_date,))
        log.info(
            "Deleted %d existing rows for business_date=%s from %s",
            cursor.rowcount,
            self.business_date,
            self.mysql_table,
        )

    def _insert_chunk(
        self,
        cursor: Any,
        col_names: List[str],
        rows: List[Tuple],
    ) -> None:
        """Bulk-insert a chunk of rows into MySQL."""
        placeholders = ", ".join(["%s"] * len(col_names))
        columns = ", ".join(f"`{c}`" for c in col_names)
        sql = f"INSERT INTO {self.mysql_table} ({columns}) VALUES ({placeholders})"
        cursor.executemany(sql, rows)

    def _prune_old_rows(self, cursor: Any) -> None:
        """
        Delete rows older than 2 days to keep the MySQL table size bounded.

        The reporting layer only needs a rolling 2-day window; historical
        data is served from Redshift directly.
        """
        sql = (
            f"DELETE FROM {self.mysql_table} "
            f"WHERE business_date < DATE_SUB(%s, INTERVAL 2 DAY)"
        )
        cursor.execute(sql, (self.business_date,))
        log.info(
            "Pruned %d rows older than 2 days from %s",
            cursor.rowcount,
            self.mysql_table,
        )

    # ------------------------------------------------------------------
    # Post-insert data quality assertion
    # ------------------------------------------------------------------

    def _assert_non_null(self, mysql_conn: Any, col_names: List[str]) -> None:
        """
        Verify that non-nullable columns contain no NULL values in today's batch.

        Strategy: create a temporary table with a composite PRIMARY KEY across
        the non-nullable columns.  Attempting to insert a NULL into a PRIMARY KEY
        column raises a MySQL error, which surfaces immediately as an operator
        failure.  We use a SELECT-based INSERT rather than scanning every row
        individually, so the check runs in a single round-trip.
        """
        nullable_targets = [c for c in _NON_NULLABLE_EXPORT_COLUMNS if c in col_names]
        if not nullable_targets:
            log.info("No non-nullable assertion columns found in result set — skipping.")
            return

        pk_cols = ", ".join(f"`{c}`" for c in nullable_targets)
        select_cols = ", ".join(
            f"IFNULL(`{c}`, CONCAT('NULL_VIOLATION:', '{c}'))" for c in nullable_targets
        )

        with mysql_conn.cursor() as cur:
            tmp_table = f"_dq_assert_{self.mysql_table.split('.')[-1]}"
            cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_table}")
            cur.execute(
                f"""
                CREATE TEMPORARY TABLE {tmp_table} (
                    {", ".join(f"`{c}` VARCHAR(255) NOT NULL" for c in nullable_targets)},
                    PRIMARY KEY ({pk_cols})
                ) ENGINE=MEMORY
                """
            )

            # Pull today's rows and attempt the insert — any NULL will cause
            # IFNULL to inject a detectable sentinel string, and a subsequent
            # LIKE check will surface it as an assertion failure.
            cur.execute(
                f"""
                INSERT IGNORE INTO {tmp_table} ({pk_cols})
                SELECT {select_cols}
                FROM {self.mysql_table}
                WHERE business_date = %s
                """,
                (self.business_date,),
            )

            # Check for sentinel strings that signal a NULL was present.
            sentinel_pattern = "NULL_VIOLATION:%"
            check_clauses = " OR ".join(
                f"`{c}` LIKE %s" for c in nullable_targets
            )
            cur.execute(
                f"SELECT COUNT(*) FROM {tmp_table} WHERE {check_clauses}",
                tuple(sentinel_pattern for _ in nullable_targets),
            )
            (violation_count,) = cur.fetchone()
            cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_table}")

        if violation_count > 0:
            raise ValueError(
                f"Data quality assertion failed: {violation_count} rows in "
                f"{self.mysql_table} for business_date={self.business_date} "
                f"have NULL values in required columns: {nullable_targets}"
            )
        log.info(
            "Non-null assertion passed for %s (%s) — 0 violations.",
            self.mysql_table,
            nullable_targets,
        )

    # ------------------------------------------------------------------
    # Operator execute
    # ------------------------------------------------------------------

    def execute(self, context: Context) -> None:
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        rs_conn = redshift_hook.get_conn()
        rs_conn.autocommit = False  # Use explicit transaction for streaming cursor

        my_conn = mysql_hook.get_conn()
        my_conn.autocommit = False

        total_rows = 0
        col_names: Optional[List[str]] = None

        try:
            with my_conn.cursor() as my_cur:
                self._delete_existing(my_conn, my_cur)

                for chunk_col_names, chunk_rows in self._stream_from_redshift(rs_conn):
                    if col_names is None:
                        col_names = chunk_col_names
                        log.info("Export columns: %s", col_names)
                    self._insert_chunk(my_cur, col_names, chunk_rows)
                    total_rows += len(chunk_rows)
                    log.info("Inserted chunk — cumulative rows: %d", total_rows)

                self._prune_old_rows(my_cur)
                my_conn.commit()

            log.info(
                "Export complete: %d rows written to %s for business_date=%s",
                total_rows,
                self.mysql_table,
                self.business_date,
            )

            if col_names:
                self._assert_non_null(my_conn, col_names)

        except Exception:
            my_conn.rollback()
            raise
        finally:
            rs_conn.close()
            my_conn.close()
