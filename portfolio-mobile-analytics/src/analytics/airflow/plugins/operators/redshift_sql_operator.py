"""
plugins/operators/redshift_sql_operator.py

Custom Airflow operator for executing one or more external SQL files against
Amazon Redshift Serverless.

Each SQL file is rendered as a Jinja template before execution, allowing
runtime parameters (e.g. {{ ds }}, {{ params.backfill_scan_date }}) to be
injected without string concatenation.  All files share a single connection
and are executed in sequence, making multi-step transformations atomic from
a connection-lifecycle perspective.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Sequence

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

log = logging.getLogger(__name__)

# Root directory used to resolve relative SQL file paths.
# In MWAA the DAGs folder is the natural anchor; adjust via the
# SQL_BASE_DIR environment variable if your layout differs.
_SQL_BASE_DIR: str = os.environ.get(
    "SQL_BASE_DIR",
    os.path.join(os.path.dirname(__file__), "..", "..", "sql"),
)


class RedshiftSQLOperator(BaseOperator):
    """
    Execute one or more SQL files against Amazon Redshift.

    Parameters
    ----------
    sql_files:
        Ordered list of SQL file paths relative to ``SQL_BASE_DIR``.
        Each file is rendered as a Jinja2 template before execution.
    params:
        Dictionary of template parameters available as ``{{ params.<key> }}``
        inside each SQL file.  Merged with Airflow's built-in macros.
    redshift_conn_id:
        Airflow connection ID for the Redshift Serverless endpoint.
    autocommit:
        Whether to autocommit each statement.  Defaults to ``True`` because
        Redshift DDL/DML statements are transactional but most pipeline steps
        are idempotent and benefit from auto-commit for visibility.
    """

    # Declare template_fields so Airflow's Jinja engine processes them.
    template_fields: Sequence[str] = ("params",)

    def __init__(
        self,
        *,
        sql_files: List[str],
        params: Optional[Dict[str, Any]] = None,
        redshift_conn_id: str = "redshift_default",
        autocommit: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql_files = sql_files
        self.params = params or {}
        self.redshift_conn_id = redshift_conn_id
        self.autocommit = autocommit

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_jinja_env(self) -> Environment:
        """Return a Jinja2 Environment anchored at the SQL base directory."""
        return Environment(
            loader=FileSystemLoader(searchpath=_SQL_BASE_DIR),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )

    def _render_sql(self, file_path: str, context: Context) -> str:
        """
        Load a SQL file and render it as a Jinja2 template.

        Airflow's macro context (ds, ds_nodash, execution_date, etc.) and the
        operator's ``params`` dict are both available inside the template.
        """
        jinja_env = self._build_jinja_env()
        # Make the path relative to SQL_BASE_DIR so the FileSystemLoader
        # can locate it regardless of the caller's CWD.
        relative_path = os.path.relpath(
            os.path.join(_SQL_BASE_DIR, file_path),
            start=_SQL_BASE_DIR,
        )
        template = jinja_env.get_template(relative_path)

        # Build the template context: Airflow macros + operator params.
        template_context = {
            "ds": context["ds"],
            "ds_nodash": context["ds_nodash"],
            "execution_date": context["execution_date"],
            "next_ds": context.get("next_ds"),
            "prev_ds": context.get("prev_ds"),
            "params": self.params,
        }
        return template.render(**template_context)

    # ------------------------------------------------------------------
    # Operator execute
    # ------------------------------------------------------------------

    def execute(self, context: Context) -> None:
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        conn = hook.get_conn()
        conn.autocommit = self.autocommit

        try:
            with conn.cursor() as cursor:
                for sql_file in self.sql_files:
                    rendered_sql = self._render_sql(sql_file, context)
                    log.info(
                        "Executing SQL file: %s\n--- SQL ---\n%s\n-----------",
                        sql_file,
                        rendered_sql,
                    )
                    cursor.execute(rendered_sql)

                    # Log row count for observability.
                    row_count: int = cursor.rowcount if cursor.rowcount >= 0 else 0
                    log.info(
                        "File '%s' complete — rows affected: %d",
                        sql_file,
                        row_count,
                    )
        finally:
            conn.close()
