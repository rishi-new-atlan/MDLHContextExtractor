# MDLH Context Extractor

Extract, score, and prioritize data asset metadata from an Atlan MDLH (Metadata Lake House) catalog. Produces a ranked context file used for downstream question generation and data governance workflows.

## Overview

This pipeline connects to an Atlan tenant's Polaris/MDLH catalog via PyIceberg, extracts rich metadata for all data assets (tables, views, columns, BI dashboards, dbt models, glossary terms, etc.), scores them by documentation quality, and outputs a prioritized context file.

**Key features:**
- Extracts assets, READMEs, tags, custom metadata, lineage, and glossary relationships
- Scores assets by metadata richness (weighted scoring system)
- Case-insensitive table/field name handling (works across all MDLH tenants)
- Single command execution with automatic dependency management
- Multi-tenant: same code, different `.env` per tenant

## Quick Start

```bash
# 1. Clone the repo
git clone <repo-url> && cd mdlh-context-extractor

# 2. Set up credentials
cp .env.example .env
# Edit .env with your tenant values

# 3. Run
bash MDLHContext.sh
```

Output will be at `output/context.txt`.

## Prerequisites

- **Python 3.9+** — the script checks for this automatically
- **Network access** to your Atlan tenant's MDLH API (`https://<tenant>.atlan.com/api/polaris/api/catalog`)
- **MDLH OAuth credentials** — client ID and secret from your Atlan admin console

## Architecture

```
.env (credentials)
  |
  v
MDLHContext.sh ──> Setup venv, install deps, validate .env
  |
  v
main.py ──> Connect to MDLH, select namespace, orchestrate pipeline
  |
  ├──> metadata_extractor.py (6-phase extraction)
  |     [1/6] Build asset index (tables, views, columns, BI, transforms...)
  |     [2/6] Pull asset READMEs
  |     [3/6] Attach tags
  |     [4/6] Attach custom metadata
  |     [5/6] Build table → column relationships
  |     [6/6] Build lineage relationships (process, columnprocess, biprocess)
  |     [+]   Build glossary relationships
  |
  └──> asset_scorer.py (score, rank, write output)
        ├── data/scored_assets.json  (JSON cache)
        └── output/context.txt       (hand-off file)
```

## File Descriptions

| File | Purpose |
|------|---------|
| `MDLHContext.sh` | Entry point. Creates venv, installs dependencies, validates `.env`, runs `main.py` |
| `main.py` | Orchestrator. Connects to MDLH, selects namespace, calls extractor then scorer |
| `config.py` | Configuration. Loads `.env`, handles OAuth token, provides catalog connection with warehouse auto-discovery |
| `metadata_extractor.py` | Core extraction engine. Builds in-memory asset index and relationship graph across 6 phases |
| `asset_scorer.py` | Scoring and output. Ranks assets by metadata richness, writes `context.txt` and `scored_assets.json` |
| `.env.example` | Template for credentials |
| `.gitignore` | Excludes `.env`, `.venv/`, `output/`, `data/` from git |

## What Gets Extracted

### Per-asset metadata
For every asset, the following fields are extracted (when available):
- **Description** — the asset's description field (or `shortdescription`/`longdescription` for glossary terms)
- **README** — full README content attached to the asset (URL-decoded, HTML-cleaned)
- **Qualified name** — full path identifier (e.g., `snowflake/analytics/raw/customer_master`)
- **Connector, database, schema** — location context
- **Owner users/groups** — asset ownership
- **Certificate status** — VERIFIED, DRAFT, or empty
- **Column descriptions** — for tables/views, each column's description is extracted and associated
- **Tags** — classification tags (e.g., PII, sensitivity labels)
- **Glossary terms** — linked business glossary terms
- **Custom metadata** — all custom metadata attributes (set.attribute = value)
- **Lineage** — upstream and downstream relationships via process/ETL tables

### What is NOT extracted
- **Data quality rule definitions/logic** — DQ tools (Monte Carlo, Soda, Anomalo) are indexed as assets with basic metadata (name, description), but their actual rule definitions, thresholds, check results, and pass/fail status are not extracted
- **Data previews/samples** — no actual data rows are read
- **Access policies/permissions** — not extracted from MDLH
- **Usage analytics** — the `usage_analytics` namespace exists but is not scanned

## Extraction Phases

### Phase 1: Build Asset Index
Scans all asset tables in the MDLH catalog (filtered to `status == 'ACTIVE'`):

**Core assets:** Table, View, MaterialisedView, Column, DataProduct, DataDomain, GlossaryTerm, GlossaryCategory, Connection

**BI assets:** Power BI (dashboards, reports, datasets, tables), Tableau (dashboards, workbooks), Looker (dashboards, explores, fields, views), Metabase, Superset, Sigma, Redash, Mode, Cognos, Domo, Qlik, ThoughtSpot, MicroStrategy

**Transform/DQ assets:** dbt (models, sources, metrics), Airflow (DAGs, tasks), ADF pipelines, Fivetran connectors, Spark jobs, Monte Carlo monitors, Soda checks, Anomalo checks — *note: only basic asset metadata is extracted for DQ tools, not rule definitions or check results*

### Phase 2: Pull READMEs
Matches README content to parent assets via GUID reference or qualified name inference.

### Phase 3: Attach Tags
Links tag relationships to assets (e.g., PII, sensitivity labels).

### Phase 4: Attach Custom Metadata
Links custom metadata attributes (set.attribute = value) to assets.

### Phase 5: Table-Column Relationships
Parses qualified name hierarchy to associate columns with their parent tables/views.

### Phase 6: Lineage
Resolves process/columnprocess/biprocess tables to build upstream/downstream lineage edges.

## Scoring System

Each asset receives a score based on its metadata richness:

| Signal | Weight | Condition |
|--------|--------|-----------|
| README | 3 | Asset has a README attached |
| Description | 2 | Asset has a description |
| Custom Metadata (rich) | 3 | 3+ custom metadata attributes |
| Custom Metadata | 2 | 1-2 custom metadata attributes |
| Lineage | 2 | Has upstream or downstream lineage |
| Certificate: VERIFIED | 3 | Certificate status is VERIFIED |
| Tags | 1 | Has any tags |
| Glossary Terms | 1 | Linked to glossary terms |
| Documented Columns | 1 | Table has 1+ column with a description |
| Certificate: DRAFT | 1 | Certificate status is DRAFT |
| Is View | 1 | Asset is a View or MaterialisedView |

**Maximum possible score: 17**

### Filtering & Output

- Bare columns (no description) are excluded
- Assets with score < 2 are excluded (unless they are GlossaryTerms, DataProducts, or DataDomains)
- Top 5,000 assets by score are written to output files

## Output Files

### `output/context.txt`
Human-readable context file grouped by asset type. Each asset block includes:
```
=== Table: customer_master ===
Connector: snowflake | DB: analytics | Schema: raw | Certificate: VERIFIED
Qualified Name: snowflake/analytics/raw/customer_master
Description: Master table of all customers...
README: This table is the single source of truth...
Columns: id, name (customer full name), email, +5 more
Upstream: raw_events, web_clickstream
Downstream: customer_segments
Tags: pii, customer-facing
Glossary: Customer Entity
Custom Metadata: dq.confidence=high, governance.owner=alice
Owners: alice@company.com
Score: 14
---
```

### `data/scored_assets.json`
JSON cache of all qualifying assets with full metadata and score breakdowns. Can be consumed by downstream tools without re-running extraction.

## Customization

### Adding New Asset Types
Edit the lists in `metadata_extractor.py`:
- `CORE_ASSET_TABLES` — for assets with specific field requirements
- `BI_ASSET_TYPES` — for BI/dashboard tools
- `TRANSFORM_ASSET_TYPES` — for ETL/transform tools

### Adjusting Scores
Edit the constants in `asset_scorer.py`:
- `WEIGHTS` — change importance of each metadata signal
- `MIN_SCORE` — minimum score to include (default: 2)
- `TOP_N` — max assets in output (default: 5,000)
- `ALWAYS_INCLUDE_TYPES` — asset types always included regardless of score

### Warehouse Discovery
Edit `WAREHOUSES` in `config.py` to add/reorder warehouse names tried during connection.

## Multi-Tenant Usage

The same codebase works for any Atlan tenant. For each tenant:

1. Clone the repo into a new folder
2. `cp .env.example .env`
3. Fill in the tenant's `ATLAN_TENANT`, `MDLH_CLIENT_ID`, `MDLH_CLIENT_SECRET`
4. `bash MDLHContext.sh`

## Troubleshooting

### `No module named 'pyarrow'`
PyIceberg requires `pyarrow` to read Iceberg table data. This is already included in `MDLHContext.sh`. If you see this error on an older copy, add `pyarrow` to the pip install line.

### `python3: command not found`
Install Python 3.9+ and ensure `python3` is on your PATH. On macOS: `brew install python3`.

### `ModuleNotFoundError: No module named 'metadata_extractor'`
All 5 Python files must be in the same directory. If you copied only some files, make sure `main.py`, `config.py`, `metadata_extractor.py`, and `asset_scorer.py` are all present.

### 0 assets extracted (but connection succeeded)
**Case sensitivity issue.** Different MDLH tenants use different naming conventions for tables and fields (lowercase vs PascalCase). The script handles this automatically with case-insensitive lookups. If you're on an older version, update to the latest.

### Tag key errors (`KeyError: 'tagName'`)
Different MDLH tenants store tag keys in different cases (`tagname` vs `tagName`). The current version handles both cases defensively. Update to the latest if you see this.

### `NotFoundException: Unable to find warehouse`
The script auto-discovers warehouses by trying `atlan-wh`, `context_store`, and `atlan_context_store` in order. If your tenant uses a different warehouse name, add it to the `WAREHOUSES` list in `config.py`.

### Connection errors / OAuth failures
Verify your `.env` values:
- `ATLAN_TENANT` should be just the hostname (e.g., `mycompany.atlan.com`)
- `MDLH_CLIENT_ID` and `MDLH_CLIENT_SECRET` must be valid OAuth credentials from the Atlan admin console
- Ensure network access to `https://<tenant>/api/polaris/api/catalog`
