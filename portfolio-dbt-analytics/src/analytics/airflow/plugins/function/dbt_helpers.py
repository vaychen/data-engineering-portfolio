from __future__ import annotations


def profiles_dir_snippet(dbt_profiles_dir: str) -> str:
    """Return a bash snippet that expands and sets PROFILES_DIR.

    Handles tilde expansion so that a default_var of ``~/.dbt`` works
    correctly inside a BashOperator subshell.
    """
    return (
        f"PROFILES_DIR='{dbt_profiles_dir}'\n"
        "PROFILES_DIR=${PROFILES_DIR/#\\~/$HOME}\n"
    )


def redshift_env_snippet(secret_id: str) -> str:
    """Return a bash snippet that exports DBT_ENV_SECRET_* values via eval.

    Fetches the Redshift credentials JSON from AWS Secrets Manager, prints
    shell ``export`` lines, then ``eval``s them so the env vars are visible
    to the dbt process in the same BashOperator shell.

    Why eval rather than direct Python export?
    ------------------------------------------
    Exporting inside a Python subprocess only affects that subprocess; the
    parent shell running dbt would not see the variables.  Printing export
    lines and eval-ing them sets the vars in the calling shell.
    """
    return "".join(
        [
            f"SECRET_ID={secret_id!r}\n",
            "export SECRET_ID\n",
            "DBT_ENV=\"$(python3 - <<'PY'\n",
            "import json\n",
            "import os\n",
            "import shlex\n",
            "import boto3\n",
            "\n",
            "secret_id = os.environ.get('SECRET_ID')\n",
            "if not secret_id:\n",
            "    raise SystemExit('Missing SECRET_ID')\n",
            "region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION')\n",
            "client = boto3.client('secretsmanager', region_name=region) if region else boto3.client('secretsmanager')\n",
            "secret = client.get_secret_value(SecretId=secret_id)\n",
            "data = json.loads(secret['SecretString'])\n",
            "host = data.get('host') or data.get('hostname')\n",
            "user = data.get('user') or data.get('username')\n",
            "password = data.get('password')\n",
            "if not (host and user and password):\n",
            "    raise SystemExit('Missing host/user/password in secret')\n",
            "print('export DBT_ENV_SECRET_REDSHIFT_HOST=' + shlex.quote(host))\n",
            "print('export DBT_ENV_SECRET_REDSHIFT_USER=' + shlex.quote(user))\n",
            "print('export DBT_ENV_SECRET_REDSHIFT_PASSWORD=' + shlex.quote(password))\n",
            "PY\n",
            ")\"\n",
            "eval \"$DBT_ENV\"\n",
        ]
    )
