VENV        := .venv
PYTHON      := python3
PIP         := $(VENV)/bin/pip
VENV_PYTHON := $(VENV)/bin/python

.PHONY: help venv install clean

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  make venv      Create Python virtual environment (.venv)"
	@echo "  make install   Install dependencies into .venv"
	@echo "  make clean     Remove .venv"
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

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -rf $(VENV)
