#!/usr/bin/env bash
set -euo pipefail

# Install Python dependencies
pip install -r backend/requirements.txt

# Verify at least one parquet engine is importable in the deploy image.
python - <<'PY'
import importlib
engines = []
for name in ("pyarrow", "fastparquet"):
    try:
        mod = importlib.import_module(name)
        engines.append((name, getattr(mod, "__version__", "unknown")))
    except Exception:
        pass
if not engines:
    raise SystemExit("No parquet engine available (pyarrow/fastparquet). Failing build.")
print("Parquet engine(s) available:", ", ".join(f"{n}=={v}" for n, v in engines))
PY

# Build React frontend
cd frontend
npm install
REACT_APP_API_URL=/api npm run build
cd ..
