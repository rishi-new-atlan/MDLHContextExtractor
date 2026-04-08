# Changelog

## [0.3.0] — 2026-04-08

### Dynamic Entity Discovery + Connector-Agnostic Scoring

**Breaking changes:**
- `extract_metadata()` now returns 3 values: `(asset_index, all_edges, discovery_report)` (was 2)
- `AssetDetail` fields renamed: `dbt_raw_sql` → `transform_raw_sql`, `dbt_compiled_sql` → `transform_compiled_sql`, `dbt_materialization_type` → `transform_materialization_type`
- New field: `AssetDetail.transform_tool` (str — "dbt", "matillion", "adf", etc.)
- Scorer weight key renamed: `dbt_sql` → `transform_sql`

**What changed:**

`metadata_extractor.py`:
- **Dynamic discovery**: `discover_and_categorize()` calls `catalog.list_tables()` (1 API call) to find all entity tables, then categorizes each into one of 20 categories using exact/prefix matching. Previously hardcoded 42 entity types across 3 lists (`CORE_ASSET_TABLES`, `BI_ASSET_TYPES`, `TRANSFORM_ASSET_TYPES`) — now covers all 459+ MDLH tables.
- **Category registry**: `ENTITY_CATEGORIES` dict defines 20 categories: core (table/view/matview/column), glossary, data_mesh, custom_entity, connection, governance, ai_ml, semantic, bi, transform, data_quality, streaming, storage, erp, snowflake_native, databricks_native, nosql. Each category specifies exact matches, prefix patterns, extra fields, probe fields, label mappings, and post-processing hooks.
- **Algorithmic PascalCase**: `_to_pascal()` replaces the 42-entry `_pascal_map` dict with prefix+suffix decomposition (`_PREFIX_CASING`, `_SUFFIX_CASING`, `_COMPOUND_OVERRIDES`). Covers all 459 tables without manual entries.
- **Generic field mapping**: `FIELD_ALIASES` + `FIELD_CONVERTERS` + `_row_to_asset()` replace per-category field-mapping code with a single generic path.
- **Single scan loop**: `build_asset_index()` iterates discovered categories in a single loop instead of 3 separate loops.
- **Discovery report**: returned as third value from `extract_metadata()` — contains categorized/skipped/uncategorized table lists.

`asset_scorer.py`:
- `WEIGHTS["dbt_sql"]` → `WEIGHTS["transform_sql"]`
- `score_asset()` checks `transform_raw_sql`/`transform_compiled_sql`

`context_writer_v2.py`:
- Section 13 "dbt Model SQL" → "Transform Tool SQL" — groups assets by `transform_tool` (dbt, matillion, adf, etc.)
- All `dbt_*` field references updated to `transform_*`

`main.py`:
- Unpacks 3 return values from `extract_metadata()`

**Verified against:** loopback.atlan.com — 459 tables discovered, 335 categorized, 3,775,924 assets extracted, all 13 context sections validated.

---

## [0.2.0] — 2026-04-03

### Context Writer v2 — 13-section structured output
- Added `context_writer_v2.py` with 13-section structured context format
- v2 fields: AI descriptions, SQL definitions, announcements, popularity, matview refresh info
- Custom metadata propagation to context
- Post-extraction validation and self-heal for table→column edges

## [0.1.0] — 2026-03-28

### Initial release
- 6-phase extraction pipeline (assets, READMEs, tags, custom metadata, columns, lineage)
- Weighted scoring system with JSON cache
- Case-insensitive MDLH table/field handling
- Multi-tenant `.env` support
