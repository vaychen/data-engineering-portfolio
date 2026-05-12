VENV        := .venv
PYTHON      := python3
PIP         := $(VENV)/bin/pip
VENV_PYTHON := $(VENV)/bin/python
DBT         := $(VENV)/bin/dbt

.PHONY: help venv install dbt-deps clean

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  make venv      Create Python virtual environment (.venv)"
	@echo "  make install   Install dependencies into .venv"
	@echo "  make dbt-deps  Install dbt package dependencies (packages.yml)"
	@echo "  make clean     Remove .venv and dbt_packages/"
	@echo ""

# ── Virtual environment ───────────────────────────────────────────────────────
venv:
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created at $(VENV)/"
	@echo "Activate with: source $(VENV)/bin/activate"

# ── Dependencies ──────────────────────────────────────────────────────────────
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r src/analytics/airflow/requirements/requirements.txt

# ── dbt package dependencies ──────────────────────────────────────────────────
dbt-deps: install
	$(DBT) deps

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -rf $(VENV) dbt_packages/
