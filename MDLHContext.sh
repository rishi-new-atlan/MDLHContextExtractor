#!/usr/bin/env bash
# MDLHContext.sh — one-shot setup + extraction
# Usage: bash MDLHContext.sh
# Output: output/context.txt

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── 0. Python check ─────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo ""
    echo "ERROR: python3 is not installed or not on PATH."
    echo "Install Python 3.9+ and ensure 'python3' is available, then re-run."
    exit 1
fi

PY_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)

if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 9 ]; }; then
    echo ""
    echo "ERROR: Python 3.9+ required (found $PY_VERSION)."
    exit 1
fi

echo "[setup] Using Python $PY_VERSION"

# ── 1. Virtual environment ────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
    echo "[setup] Creating virtual environment..."
    python3 -m venv .venv
fi

source .venv/bin/activate

# ── 2. Dependencies ───────────────────────────────────────────────────────────
echo "[setup] Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet pyiceberg pandas requests python-dotenv pyarrow adlfs

# ── 3. Environment check ──────────────────────────────────────────────────────
ENV_FILE="$SCRIPT_DIR/.env"

# Create .env with blank placeholders if it doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    echo "[setup] .env not found — creating it with required keys..."
    cat > "$ENV_FILE" <<'EOF'
ATLAN_TENANT=
MDLH_CLIENT_ID=
MDLH_CLIENT_SECRET=
EOF
    echo ""
    echo "ERROR: .env created at $ENV_FILE"
    echo "Fill in the 3 values, save the file, then re-run this script."
    exit 1
fi

# Ensure all 3 required keys are present and non-empty
MISSING=()
for KEY in ATLAN_TENANT MDLH_CLIENT_ID MDLH_CLIENT_SECRET; do
    VALUE=$(grep -E "^${KEY}=" "$ENV_FILE" | cut -d= -f2- | tr -d '[:space:]')
    if [ -z "$VALUE" ]; then
        MISSING+=("$KEY")
    fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
    echo ""
    echo "ERROR: The following required values are missing in .env:"
    for KEY in "${MISSING[@]}"; do
        echo "  $KEY"
    done
    echo ""
    echo "Open $ENV_FILE, fill in the missing values, and re-run."
    exit 1
fi

# ── 4. Run extraction ─────────────────────────────────────────────────────────
echo ""
echo "[run] Starting extraction..."
python main.py

# ── 5. Done ───────────────────────────────────────────────────────────────────
echo ""
echo "Done. Output file:"
echo "  $SCRIPT_DIR/output/context.txt"
