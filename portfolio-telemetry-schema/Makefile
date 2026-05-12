VENV        := .venv
PYTHON      := python3
PIP         := $(VENV)/bin/pip
VENV_PYTHON := $(VENV)/bin/python

.PHONY: help venv install test clean

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  make venv      Create Python virtual environment (.venv)"
	@echo "  make install   Install dependencies into .venv"
	@echo "  make test      Run tests"
	@echo "  make clean     Remove .venv and __pycache__"
	@echo ""

# ── Virtual environment ───────────────────────────────────────────────────────
venv:
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created at $(VENV)/"
	@echo "Activate with: source $(VENV)/bin/activate"

# ── Dependencies ──────────────────────────────────────────────────────────────
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

# ── Tests ─────────────────────────────────────────────────────────────────────
test: install
	$(VENV)/bin/pytest tests/ -v

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -rf $(VENV)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
