"""Root conftest: add project root to sys.path so that 'schemas' and
'lambda_src' are importable without installing the package."""
import sys
from pathlib import Path

# Insert the project root (directory containing this file) at the front of
# sys.path so pytest can resolve 'schemas.*' and 'lambda_src.*' imports.
sys.path.insert(0, str(Path(__file__).parent))
