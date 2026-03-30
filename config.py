"""Configuration and MDLH catalog connection."""

import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent
load_dotenv(PROJECT_ROOT / ".env")

# --- Atlan / MDLH ---
ATLAN_TENANT = os.getenv("ATLAN_TENANT", "fs3.atlan.com")
MDLH_CLIENT_ID = os.getenv("MDLH_CLIENT_ID")
MDLH_CLIENT_SECRET = os.getenv("MDLH_CLIENT_SECRET")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
OPENAI_API_KEY = os.getenv("OPEN_AI_KEY")

POLARIS_BASE = f"https://{ATLAN_TENANT}/api/polaris/api/catalog"
POLARIS_TOKEN_URL = f"{POLARIS_BASE}/v1/oauth/tokens"

WAREHOUSES = ["atlan-wh", "context_store", "atlan_context_store"]

# --- Paths ---
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
for d in [DATA_DIR, OUTPUT_DIR]:
    d.mkdir(exist_ok=True)


def get_mdlh_token() -> str:
    """Get a fresh Polaris OAuth token."""
    import requests
    resp = requests.post(POLARIS_TOKEN_URL, data={
        "client_id": MDLH_CLIENT_ID,
        "client_secret": MDLH_CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "PRINCIPAL_ROLE:ALL",
    }, headers={"Content-Type": "application/x-www-form-urlencoded"})
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_mdlh_catalog():
    """Connect to MDLH via PyIceberg with warehouse auto-discovery."""
    from pyiceberg.catalog import load_catalog

    token = get_mdlh_token()

    for wh in WAREHOUSES:
        try:
            catalog = load_catalog(
                "mdlh", type="rest", uri=POLARIS_BASE,
                token=token, warehouse=wh,
            )
            namespaces = catalog.list_namespaces()
            print(f"Connected to {ATLAN_TENANT} (warehouse={wh})")
            print(f"  Namespaces: {['.'.join(ns) for ns in namespaces]}")
            return catalog, namespaces, wh
        except Exception as e:
            print(f"  warehouse={wh} failed: {e}")
            continue

    raise RuntimeError(f"Could not connect to any warehouse on {ATLAN_TENANT}")
