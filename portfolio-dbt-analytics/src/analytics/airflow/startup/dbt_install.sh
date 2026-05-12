#!/bin/sh
set -eu

# Install dbt Fusion and a dedicated dbt-core virtualenv for MWAA workers.
# This script also writes a profiles.yml template that reads credentials from env vars.

# Optional environment variables:
#   DBT_FUSION_VERSION   (default: 2.0.0-preview.92)
#   DBT_FUSION_TARGET    (default: x86_64-unknown-linux-gnu)
#   DBT_FUSION_DEST      (default: /usr/local/airflow/.local/dbt_fusion/bin)
#   DBT_FUSION_S3_URI    (optional S3 prefix that contains the tarball)
#   DBT_CORE_VENV_PATH   (default: /usr/local/airflow/.local/dbt_core_venv)
#   DBT_CORE_PACKAGES    (default: dbt-core==1.10.* dbt-redshift==1.10.*)

VERSION="${DBT_FUSION_VERSION:-2.0.0-preview.92}"
TARGET="${DBT_FUSION_TARGET:-x86_64-unknown-linux-gnu}"
DEST="${DBT_FUSION_DEST:-/usr/local/airflow/.local/dbt_fusion/bin}"
S3_URI="${DBT_FUSION_S3_URI:-}"
HOME_DIR="${HOME:-/usr/local/airflow}"
DBT_CORE_VENV_PATH="${DBT_CORE_VENV_PATH:-/usr/local/airflow/.local/dbt_core_venv}"
DBT_CORE_PACKAGES="${DBT_CORE_PACKAGES:-dbt-core==1.10.* dbt-redshift==1.10.*}"

if [ ! -w "$DEST" ]; then
  DEST="$HOME_DIR/.local/dbt_fusion/bin"
fi
mkdir -p "$DEST"

# Ensure tar is available (required by the dbt Fusion installer).
if ! command -v tar >/dev/null 2>&1; then
  if command -v yum >/dev/null 2>&1; then
    if [ "${MWAA_AIRFLOW_COMPONENT:-}" != "webserver" ]; then
      sudo yum -y install tar gzip
    fi
  fi
fi

tmp_dir="$(mktemp -d 2>/dev/null || mktemp -d -t dbt-fusion)"
cleanup() {
  if [ -n "${tmp_dir:-}" ]; then
    rm -rf "$tmp_dir"
  fi
}
trap cleanup EXIT

archive="fs-v${VERSION}-${TARGET}.tar.gz"

if [ -n "$S3_URI" ]; then
  if ! command -v aws >/dev/null 2>&1; then
    echo "aws CLI not found; cannot fetch $archive from $S3_URI" >&2
    exit 1
  fi
  aws s3 cp "${S3_URI%/}/${archive}" "$tmp_dir/$archive"
  tar -xzf "$tmp_dir/$archive" -C "$tmp_dir"

  dbt_path=""
  if [ -f "$tmp_dir/dbt" ]; then
    dbt_path="$tmp_dir/dbt"
  else
    dbt_path="$(find "$tmp_dir" -maxdepth 2 -type f -name dbt | head -n 1)"
  fi

  if [ -z "$dbt_path" ]; then
    echo "dbt binary not found in $archive" >&2
    exit 1
  fi

  cp "$dbt_path" "$DEST/dbt"
  chmod 755 "$DEST/dbt"
else
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl not found; cannot fetch installer from CDN" >&2
    exit 1
  fi
  curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | \
    sh -s -- --update --version "$VERSION" --target "$TARGET" --to "$DEST"
fi

if [ -f "$DEST/dbt" ]; then
  ln -sf "$DEST/dbt" "$DEST/dbtf"
fi

if [ -w "/etc/profile.d" ]; then
  echo "export PATH=\$PATH:$DEST" > /etc/profile.d/dbt-fusion.sh
fi

"$DEST/dbt" --version || true

# Install dbt-core adapter into a dedicated virtualenv for Cosmos/local execution mode.
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found; cannot create dbt-core virtualenv" >&2
  exit 1
fi

if [ ! -x "$DBT_CORE_VENV_PATH/bin/dbt" ]; then
  python3 -m venv "$DBT_CORE_VENV_PATH"
  "$DBT_CORE_VENV_PATH/bin/pip" install --upgrade pip
  "$DBT_CORE_VENV_PATH/bin/pip" install $DBT_CORE_PACKAGES
fi

PROFILES_DIR="${DBT_PROFILES_DIR:-/usr/local/airflow/.dbt}"
mkdir -p "$PROFILES_DIR"
ls "$PROFILES_DIR"
# Write a local-development profiles.yml for dbt / dbtf CLI use (not used by the
# production MWAA DAG).  The DAG (dbt_pipeline_dag.py) writes its own ephemeral
# profiles.yml to /tmp/dbt_profiles/ at task runtime by fetching credentials from
# AWS Secrets Manager, and points dbt there via --profiles-dir.  This profile
# is only consulted when running dbt/dbtf manually on the worker or locally,
# where credentials are expected to be present as environment variables.
cat > "$PROFILES_DIR/profiles.yml" <<'YML'
analytics_pipeline:
  target: dev
  outputs:
    dev:
      type: redshift
      port: 5439
      database: analytics_dw
      schema: analytics_source
      ra3_node: true
      method: database
      host: "{{ env_var('DBT_ENV_SECRET_REDSHIFT_HOST') }}"
      user: "{{ env_var('DBT_ENV_SECRET_REDSHIFT_USER') }}"
      password: "{{ env_var('DBT_ENV_SECRET_REDSHIFT_PASSWORD') }}"
      threads: 4
      connect_timeout: 30
      keepalives_idle: 240
YML
