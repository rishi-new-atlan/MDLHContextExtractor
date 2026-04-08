"""
metadata_extractor.py

Extract per-asset metadata details and relationship map from MDLH for a given tenant instance.
Returns asset_index, all_edges, and discovery_report — writing is handled by asset_scorer.py.
"""

import time
import pandas as pd
from dataclasses import dataclass, field, asdict
from typing import Optional
from collections import defaultdict


def _p(msg):
    """Print with immediate flush so progress is visible in real time."""
    print(msg, flush=True)


@dataclass
class AssetDetail:
    guid: str
    name: str
    asset_type: str
    qualified_name: str = ""
    description: str = ""
    readme: str = ""
    connector: str = ""
    database: str = ""
    schema_name: str = ""
    owner_users: list = field(default_factory=list)
    owner_groups: list = field(default_factory=list)
    certificate_status: str = ""
    tags: list = field(default_factory=list)
    glossary_terms: list = field(default_factory=list)
    custom_metadata: list = field(default_factory=list)
    columns: list = field(default_factory=list)
    lineage_upstream: list = field(default_factory=list)
    lineage_downstream: list = field(default_factory=list)
    # --- v2 fields ---
    display_name: str = ""
    user_description: str = ""
    ai_description: str = ""
    definition: str = ""              # SQL definition (views, matviews)
    announcement_type: str = ""
    announcement_title: str = ""
    announcement_message: str = ""
    certificate_message: str = ""
    popularity_score: float = 0.0
    row_count: int = 0
    size_bytes: int = 0
    source_url: str = ""
    # matview-specific
    refresh_method: str = ""
    refresh_mode: str = ""
    staleness: str = ""
    # transform-generic (replaces dbt-specific)
    transform_raw_sql: str = ""
    transform_compiled_sql: str = ""
    transform_materialization_type: str = ""
    transform_tool: str = ""          # "dbt", "matillion", "adf", etc.


@dataclass
class RelationshipEdge:
    source_guid: str
    source_name: str
    source_type: str
    target_guid: str
    target_name: str
    target_type: str
    relationship_type: str
    metadata: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# PascalCase conversion for MDLH table names
# ---------------------------------------------------------------------------

# Exact compound overrides that can't be decomposed by prefix+suffix
_COMPOUND_OVERRIDES = {
    "materialisedview": "MaterialisedView",
    "glossaryterm": "GlossaryTerm",
    "glossarycategory": "GlossaryCategory",
    "dataproduct": "DataProduct",
    "datadomain": "DataDomain",
    "datameshdataset": "DataMeshDataset",
    "customentity": "CustomEntity",
    "aiapplication": "AIApplication",
    "aimodel": "AIModel",
    "aimodelversion": "AIModelVersion",
    "dataqualityrule": "DataQualityRule",
    "datacontract": "DataContract",
    "businesspolicy": "BusinessPolicy",
    "columnprocess": "ColumnProcess",
    "biprocess": "BIProcess",
    "tagrelationship": "TagRelationship",
    "custommetadatarelationship": "CustomMetadataRelationship",
    "dbtsemanticmodel": "DbtSemanticModel",
}

# Prefix → PascalCase (sorted by length descending for greedy matching)
_PREFIX_CASING = [
    ("snowflakesemantic", "SnowflakeSemantic"),
    ("azureservicebus", "AzureServiceBus"),
    ("azureeventhub", "AzureEventHub"),
    ("schemaregistry", "SchemaRegistry"),
    ("cosmosmongodb", "CosmosMongoDB"),
    ("microstrategy", "Microstrategy"),
    ("snowflakeai", "SnowflakeAI"),
    ("databricksai", "DatabricksAI"),
    ("thoughtspot", "Thoughtspot"),
    ("datastudio", "DataStudio"),
    ("quicksight", "Quicksight"),
    ("documentdb", "DocumentDB"),
    ("salesforce", "Salesforce"),
    ("sagemaker", "SageMaker"),
    ("dataverse", "Dataverse"),
    ("snowflake", "Snowflake"),
    ("databricks", "Databricks"),
    ("fivetran", "Fivetran"),
    ("matillion", "Matillion"),
    ("metabase", "Metabase"),
    ("dynamodb", "DynamoDB"),
    ("superset", "Superset"),
    ("cassandra", "Cassandra"),
    ("mongodb", "MongoDB"),
    ("powerbi", "PowerBI"),
    ("tableau", "Tableau"),
    ("airflow", "Airflow"),
    ("anomalo", "Anomalo"),
    ("anaplan", "Anaplan"),
    ("cognos", "Cognos"),
    ("looker", "Looker"),
    ("fabric", "Fabric"),
    ("redash", "Redash"),
    ("sisense", "Sisense"),
    ("preset", "Preset"),
    ("sigma", "Sigma"),
    ("spark", "Spark"),
    ("kafka", "Kafka"),
    ("saperp", "SapERP"),
    ("domo", "Domo"),
    ("qlik", "Qlik"),
    ("mode", "Mode"),
    ("adls", "ADLS"),
    ("soda", "Soda"),
    ("flow", "Flow"),
    ("dbt", "Dbt"),
    ("adf", "Adf"),
    ("gcs", "GCS"),
    ("hex", "Hex"),
    ("mc", "MC"),
    ("s3", "S3"),
]

# Suffix → PascalCase (for the part after the tool prefix)
_SUFFIX_CASING = {
    "dashboard": "Dashboard", "report": "Report", "dataset": "Dataset",
    "table": "Table", "workbook": "Workbook", "look": "Look",
    "explore": "Explore", "field": "Field", "view": "View",
    "sheet": "Sheet", "liveboard": "Liveboard", "dossier": "Dossier",
    "model": "Model", "source": "Source", "metric": "Metric",
    "dag": "Dag", "task": "Task", "pipeline": "Pipeline",
    "connector": "Connector", "job": "Job", "monitor": "Monitor",
    "check": "Check", "term": "Term", "category": "Category",
    "product": "Product", "domain": "Domain", "entity": "Entity",
    "application": "Application", "version": "Version",
    "process": "Process", "column": "Column",
    "relationship": "Relationship", "stream": "Stream",
    "pipe": "Pipe", "stage": "Stage", "notebook": "Notebook",
    "volume": "Volume", "tag": "Tag", "function": "Function",
    "procedure": "Procedure", "sequence": "Sequence", "alert": "Alert",
}


def _to_pascal(name):
    """Convert a lowercase MDLH table name to PascalCase.

    Uses compound overrides for irregular names, then prefix+suffix
    decomposition for tool-specific names (e.g. powerbidashboard → PowerBIDashboard).
    """
    lower = name.lower()

    if lower in _COMPOUND_OVERRIDES:
        return _COMPOUND_OVERRIDES[lower]

    for prefix, cased_prefix in _PREFIX_CASING:
        if lower.startswith(prefix):
            remainder = lower[len(prefix):]
            if not remainder:
                return cased_prefix
            cased_suffix = _SUFFIX_CASING.get(remainder, remainder.capitalize())
            return cased_prefix + cased_suffix

    return name[0].upper() + name[1:] if name else name


def _load_table_ci(catalog, namespace, table_name):
    """Load an Iceberg table with case-insensitive name matching.

    Tries lowercase → PascalCase → Capitalize.
    """
    # Try as-is (lowercase)
    try:
        return catalog.load_table((namespace, table_name))
    except Exception:
        pass
    # Try PascalCase
    pascal = _to_pascal(table_name)
    if pascal != table_name:
        try:
            return catalog.load_table((namespace, pascal))
        except Exception:
            pass
    # Try capitalize first letter only
    cap = table_name.capitalize()
    if cap != pascal and cap != table_name:
        try:
            return catalog.load_table((namespace, cap))
        except Exception:
            pass
    raise Exception(f"Table not found: tried '{table_name}', '{pascal}', '{cap}'")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_columns(df):
    """Normalize DataFrame column names to lowercase for consistent access."""
    df.columns = [c.lower() for c in df.columns]
    return df


def _probe_fields(tbl, desired_fields):
    """Return the subset of desired_fields that actually exist in the table schema.

    This prevents scan failures when MDLH schema doesn't have a field we expect.
    Comparison is case-insensitive.
    """
    try:
        schema = tbl.schema()
        available = {f.name.lower() for f in schema.fields}
        probed = tuple(f for f in desired_fields if f.lower() in available)
        dropped = set(f.lower() for f in desired_fields) - set(f.lower() for f in probed)
        if dropped:
            _p(f"    [probe] fields not in schema, skipped: {sorted(dropped)}")
        return probed
    except Exception:
        # If schema introspection fails, return all and let scan handle it
        return desired_fields


def _safe_scan_all(catalog, namespace, table_name, selected_fields):
    try:
        tbl = _load_table_ci(catalog, namespace, table_name)
        scan = tbl.scan(selected_fields=selected_fields)
        df = scan.to_pandas()
        return _normalize_columns(df)
    except Exception as e:
        # Retry without selected_fields filter (field names may differ in case)
        try:
            tbl = _load_table_ci(catalog, namespace, table_name)
            scan = tbl.scan()
            df = _normalize_columns(scan.to_pandas())
            # Keep only columns that match (case-insensitive)
            wanted = {f.lower() for f in selected_fields}
            available = [c for c in df.columns if c.lower() in wanted]
            if available:
                return df[available]
            return df
        except Exception:
            pass
        _p(f"  SKIP {namespace}.{table_name}: {e}")
        return pd.DataFrame()


def _to_list(val):
    if val is None:
        return []
    if isinstance(val, (list, set)):
        return [str(v) for v in val if v]
    if isinstance(val, str) and val.strip():
        return [val.strip()]
    try:
        return [str(v) for v in val if v]
    except TypeError:
        return []


def _to_str(val):
    if val is None:
        return ""
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return ""
    except Exception:
        pass
    return str(val).strip()


def _to_float(val):
    if val is None:
        return 0.0
    try:
        import math
        f = float(val)
        return 0.0 if math.isnan(f) else f
    except (TypeError, ValueError):
        return 0.0


def _to_int(val):
    if val is None:
        return 0
    try:
        import math
        if isinstance(val, float):
            return 0 if math.isnan(val) else int(val)
        return int(val)
    except (TypeError, ValueError):
        return 0


# ---------------------------------------------------------------------------
# Entity Category Registry
# ---------------------------------------------------------------------------

BASE_FIELDS = ("guid", "name", "qualifiedname", "description", "connectorname")

SKIP_TABLES = frozenset({
    # Relationship tables — handled by dedicated extractors
    "tagrelationship", "custommetadatarelationship",
    # Process tables — handled by lineage builder
    "process", "columnprocess", "biprocess",
    # System/UI tables
    "readme", "link", "badge", "tag",
})

SKIP_PREFIXES = (
    "gold_",        # historical/audit namespace tables
    "workflow",     # Atlan workflow system
    "authpolicy",   # access control internals
    "authservice",  # access control internals
)

# Shared extra-field tuples for category definitions
_CORE_EXTRA = (
    "databasename", "schemaname", "ownerusers", "ownergroups",
    "certificatestatus", "certificatemessage", "meanings",
    "displayname", "userdescription", "assetaigenerateddescription",
    "announcementtype", "announcementtitle", "announcementmessage",
    "popularityscore", "sourceurl",
)

_LIGHT_EXTRA = (
    "ownerusers", "ownergroups", "certificatestatus",
    "assetaigenerateddescription",
)

_BI_EXTRA = (
    "ownerusers", "ownergroups", "certificatestatus", "meanings",
    "assetaigenerateddescription", "announcementtype",
    "announcementtitle", "announcementmessage", "sourceurl",
)

_TRANSFORM_EXTRA = (
    "ownerusers", "certificatestatus", "assetaigenerateddescription",
)

# Category definitions — order matters: specific before general (first match wins)
ENTITY_CATEGORIES = {
    # --- Core relational ---
    "core_table": {
        "exact": ["table"],
        "prefixes": [],
        "extra_fields": _CORE_EXTRA + ("tabledefinition", "rowcount", "sizebytes"),
        "label_map": {"table": "Table"},
    },
    "core_view": {
        "exact": ["view"],
        "prefixes": [],
        "extra_fields": _CORE_EXTRA + ("definition",),
        "label_map": {"view": "View"},
    },
    "core_matview": {
        "exact": ["materialisedview"],
        "prefixes": [],
        "extra_fields": _CORE_EXTRA + ("definition", "refreshmethod", "refreshmode", "staleness"),
        "label_map": {"materialisedview": "MaterialisedView"},
    },
    "core_column": {
        "exact": ["column"],
        "prefixes": [],
        "extra_fields": (
            "datatype", "tablename", "databasename", "schemaname",
            "userdescription", "assetaigenerateddescription", "ownerusers",
        ),
        "label_map": {"column": "Column"},
    },
    # --- Glossary ---
    "glossary": {
        "exact": ["glossaryterm", "glossarycategory"],
        "prefixes": [],
        "extra_fields": (
            "shortdescription", "longdescription",
            "assetaigenerateddescription", "ownerusers", "ownergroups",
            "certificatestatus",
        ),
        "label_map": {"glossaryterm": "GlossaryTerm", "glossarycategory": "GlossaryCategory"},
        "post_process": "glossary",
    },
    # --- Data Mesh ---
    "data_mesh": {
        "exact": ["dataproduct", "datadomain", "datameshdataset"],
        "prefixes": [],
        "extra_fields": _LIGHT_EXTRA,
        "label_map": {
            "dataproduct": "DataProduct",
            "datadomain": "DataDomain",
            "datameshdataset": "DataMeshDataset",
        },
    },
    # --- Custom Entity ---
    "custom_entity": {
        "exact": ["customentity"],
        "prefixes": [],
        "extra_fields": (
            "ownerusers", "ownergroups", "certificatestatus", "meanings",
            "assetaigenerateddescription",
        ),
        "label_map": {"customentity": "CustomEntity"},
    },
    # --- Connection ---
    "connection": {
        "exact": ["connection"],
        "prefixes": [],
        "extra_fields": ("ownerusers", "ownergroups"),
        "label_map": {"connection": "Connection"},
    },
    # --- Governance ---
    "governance": {
        "exact": ["persona", "purpose"],
        "prefixes": ["stakeholder"],
        "extra_fields": _LIGHT_EXTRA,
        "label_map": {"persona": "Persona", "purpose": "Purpose"},
    },
    # --- AI / ML (before snowflake_native / databricks_native) ---
    "ai_ml": {
        "exact": ["aiapplication", "aimodel", "aimodelversion"],
        "prefixes": ["sagemaker", "databricksai", "snowflakeai"],
        "extra_fields": _LIGHT_EXTRA,
        "label_map": {
            "aiapplication": "AIApplication",
            "aimodel": "AIModel",
            "aimodelversion": "AIModelVersion",
        },
    },
    # --- Semantic models (before snowflake_native) ---
    "semantic": {
        "exact": ["dbtsemanticmodel"],
        "prefixes": ["semantic", "snowflakesemantic"],
        "extra_fields": _LIGHT_EXTRA,
        "label_map": {"dbtsemanticmodel": "DbtSemanticModel"},
    },
    # --- BI tools ---
    "bi": {
        "exact": [],
        "prefixes": [
            "powerbi", "tableau", "looker", "sigma", "mode", "metabase",
            "superset", "preset", "domo", "qlik", "thoughtspot", "cognos",
            "microstrategy", "redash", "sisense", "quicksight", "datastudio",
            "fabric", "anaplan", "hex",
        ],
        "extra_fields": _BI_EXTRA,
    },
    # --- Transform / ELT ---
    "transform": {
        "exact": [],
        "prefixes": ["dbt", "matillion", "adf", "fivetran", "spark", "airflow", "flow"],
        "extra_fields": _TRANSFORM_EXTRA,
        "probe_extra": ("dbtrawsql", "dbtcompiledsql", "dbtmaterializationtype"),
    },
    # --- Data Quality ---
    "data_quality": {
        "exact": [],
        "prefixes": ["mc", "soda", "anomalo", "dataqualityrule", "datacontract", "businesspolicy"],
        "extra_fields": _LIGHT_EXTRA,
    },
    # --- Streaming ---
    "streaming": {
        "exact": [],
        "prefixes": ["kafka", "azureeventhub", "azureservicebus", "schemaregistry"],
        "extra_fields": _LIGHT_EXTRA,
    },
    # --- Cloud storage ---
    "storage": {
        "exact": [],
        "prefixes": ["s3", "adls", "gcs"],
        "extra_fields": _LIGHT_EXTRA,
    },
    # --- ERP / Business apps ---
    "erp": {
        "exact": [],
        "prefixes": ["salesforce", "saperp", "dataverse"],
        "extra_fields": _LIGHT_EXTRA,
    },
    # --- Snowflake native (after ai_ml and semantic) ---
    "snowflake_native": {
        "exact": [],
        "prefixes": ["snowflake"],
        "exclude": ["snowflakeai", "snowflakesemantic"],
        "extra_fields": _LIGHT_EXTRA,
    },
    # --- Databricks native (after ai_ml) ---
    "databricks_native": {
        "exact": [],
        "prefixes": ["databricks"],
        "exclude": ["databricksai"],
        "extra_fields": _LIGHT_EXTRA,
    },
    # --- NoSQL ---
    "nosql": {
        "exact": [],
        "prefixes": ["cosmosmongodb", "dynamodb", "mongodb", "cassandra", "documentdb"],
        "extra_fields": _LIGHT_EXTRA,
    },
}

# MDLH column name → AssetDetail field name
FIELD_ALIASES = {
    "databasename": "database",
    "schemaname": "schema_name",
    "qualifiedname": "qualified_name",
    "connectorname": "connector",
    "ownerusers": "owner_users",
    "ownergroups": "owner_groups",
    "certificatestatus": "certificate_status",
    "certificatemessage": "certificate_message",
    "displayname": "display_name",
    "userdescription": "user_description",
    "assetaigenerateddescription": "ai_description",
    "tabledefinition": "definition",
    "rowcount": "row_count",
    "sizebytes": "size_bytes",
    "announcementtype": "announcement_type",
    "announcementtitle": "announcement_title",
    "announcementmessage": "announcement_message",
    "popularityscore": "popularity_score",
    "sourceurl": "source_url",
    "refreshmethod": "refresh_method",
    "refreshmode": "refresh_mode",
    "dbtrawsql": "transform_raw_sql",
    "dbtcompiledsql": "transform_compiled_sql",
    "dbtmaterializationtype": "transform_materialization_type",
}

# Fields that need type conversion (field name → converter)
FIELD_CONVERTERS = {
    "owner_users": _to_list,
    "owner_groups": _to_list,
    "popularity_score": _to_float,
    "row_count": _to_int,
    "size_bytes": _to_int,
}


# ---------------------------------------------------------------------------
# Dynamic Discovery
# ---------------------------------------------------------------------------

def discover_and_categorize(catalog, ns):
    """Discover all MDLH tables and categorize by entity type.

    Returns (categorized: dict[cat→list], skipped: list, uncategorized: list).
    """
    _p("\n  Discovering entity tables...")

    all_tables = catalog.list_tables(ns)
    table_names = sorted(set(t[-1].lower() for t in all_tables))
    _p(f"  Found {len(table_names):,} tables in namespace '{ns}'")

    categorized = defaultdict(list)
    skipped = []
    uncategorized = []

    for name in table_names:
        # Skip system/relationship tables
        if name in SKIP_TABLES:
            skipped.append(name)
            continue
        if any(name.startswith(p) for p in SKIP_PREFIXES):
            skipped.append(name)
            continue

        # Category match: exact first, then prefix (first match wins)
        matched = False
        for cat_name, cat_def in ENTITY_CATEGORIES.items():
            # Exact match
            if name in cat_def.get("exact", []):
                categorized[cat_name].append(name)
                matched = True
                break

            # Prefix match
            for prefix in cat_def.get("prefixes", []):
                if name.startswith(prefix):
                    # Check exclusions
                    excludes = cat_def.get("exclude", [])
                    if any(name.startswith(ex) for ex in excludes):
                        continue
                    categorized[cat_name].append(name)
                    matched = True
                    break
            if matched:
                break

        if not matched:
            uncategorized.append(name)

    # Log discovery summary
    total_cat = sum(len(v) for v in categorized.values())
    _p(f"  Discovery: {total_cat:,} categorized, "
       f"{len(skipped):,} skipped, {len(uncategorized):,} uncategorized")
    for cat_name in ENTITY_CATEGORIES:
        if cat_name in categorized:
            _p(f"    {cat_name}: {len(categorized[cat_name])} — "
               f"{', '.join(categorized[cat_name][:8])}"
               f"{'...' if len(categorized[cat_name]) > 8 else ''}")
    if uncategorized:
        _p(f"    uncategorized: {', '.join(sorted(uncategorized)[:20])}")
        if len(uncategorized) > 20:
            _p(f"      ... +{len(uncategorized) - 20} more")

    return dict(categorized), skipped, uncategorized


def _row_to_asset(row, asset_type, safe_fields, cat_def, table_name=""):
    """Convert a DataFrame row to an AssetDetail using FIELD_ALIASES."""
    guid = _to_str(row.get("guid"))
    if not guid:
        return None

    detail = AssetDetail(
        guid=guid,
        name=_to_str(row.get("name")),
        asset_type=asset_type,
    )

    # Generic field mapping via FIELD_ALIASES
    safe_lower = {f.lower() for f in safe_fields}
    for col in safe_lower:
        if col in ("guid", "name"):
            continue

        field_name = FIELD_ALIASES.get(col, col)

        if not hasattr(detail, field_name):
            continue

        raw_val = row.get(col)

        if field_name in FIELD_CONVERTERS:
            val = FIELD_CONVERTERS[field_name](raw_val)
        else:
            val = _to_str(raw_val)

        setattr(detail, field_name, val)

    # Glossary post-processing: description ← shortdescription, readme ← longdescription
    if cat_def.get("post_process") == "glossary":
        detail.description = _to_str(row.get("shortdescription", ""))
        detail.readme = _to_str(row.get("longdescription", ""))

    # Transform tool detection from table name prefix
    for prefix in ENTITY_CATEGORIES.get("transform", {}).get("prefixes", []):
        if table_name.startswith(prefix):
            detail.transform_tool = prefix
            break

    # Glossary terms from "meanings" column
    detail.glossary_terms = _to_list(row.get("meanings"))

    return detail


# ---------------------------------------------------------------------------
# Asset Index Builder
# ---------------------------------------------------------------------------

def build_asset_index(catalog, ns):
    """Build asset index from all discovered entity tables."""
    _p("\n[1/6] Building asset index...")
    t0 = time.time()

    categorized, skipped, uncategorized = discover_and_categorize(catalog, ns)

    asset_index = {}

    for cat_name in ENTITY_CATEGORIES:
        if cat_name not in categorized:
            continue
        table_names = categorized[cat_name]
        cat_def = ENTITY_CATEGORIES[cat_name]
        _p(f"  Scanning {cat_name} ({len(table_names)} tables)...")
        cat_count = 0

        for table_name in table_names:
            # Determine asset_type from label_map or use table_name directly
            label_map = cat_def.get("label_map", {})
            asset_type = label_map.get(table_name, table_name)

            try:
                tbl = _load_table_ci(catalog, ns, table_name)
                desired = BASE_FIELDS + cat_def["extra_fields"] + cat_def.get("probe_extra", ())
                safe = _probe_fields(tbl, desired)
                scan = tbl.scan(row_filter="status == 'ACTIVE'", selected_fields=safe)
                df = _normalize_columns(scan.to_pandas())
            except Exception as e:
                _p(f"    SKIP {table_name}: {e}")
                continue

            for _, row in df.iterrows():
                detail = _row_to_asset(row, asset_type, safe, cat_def, table_name)
                if detail:
                    asset_index[detail.guid] = detail
                    cat_count += 1

        _p(f"    {cat_name}: {cat_count:,} loaded (total so far: {len(asset_index):,})")

    discovery_report = {
        "categorized": {k: list(v) for k, v in categorized.items()},
        "skipped": skipped,
        "uncategorized": uncategorized,
    }

    _p(f"  Asset index complete: {len(asset_index):,} assets in {time.time()-t0:.1f}s")
    return asset_index, discovery_report


# ---------------------------------------------------------------------------
# Extraction steps 2–6 (unchanged)
# ---------------------------------------------------------------------------

def pull_asset_readmes(catalog, ns, asset_index):
    from urllib.parse import unquote
    _p("\n[2/6] Pulling asset READMEs...")
    t0 = time.time()
    attached = 0
    try:
        tbl = _load_table_ci(catalog, ns, "readme")
        scan = tbl.scan(selected_fields=("guid", "qualifiedname", "asset", "description"))
        df = _normalize_columns(scan.to_pandas())
        _p(f"  {len(df):,} README rows found")
    except Exception as e:
        _p(f"  SKIP Readme table: {e}")
        return

    qn_to_guid = {a.qualified_name: guid for guid, a in asset_index.items() if a.qualified_name}

    for _, row in df.iterrows():
        content = unquote(_to_str(row.get("description")))
        if not content:
            continue

        asset_guid = None

        # Method 1: use the 'asset' field (list of parent GUIDs)
        asset_refs = row.get("asset")
        try:
            asset_refs = list(asset_refs) if asset_refs is not None else []
        except TypeError:
            asset_refs = []
        if asset_refs:
            asset_guid = _to_str(asset_refs[0])
            if asset_guid not in asset_index:
                asset_guid = None

        # Method 2: derive parent from qualifiedname (strip /readme suffix)
        if not asset_guid:
            readme_qn = _to_str(row.get("qualifiedname"))
            if readme_qn.endswith("/readme"):
                parent_qn = readme_qn[: -len("/readme")]
                asset_guid = qn_to_guid.get(parent_qn)

        if asset_guid and asset_guid in asset_index:
            asset_index[asset_guid].readme = content
            attached += 1

    _p(f"  READMEs attached: {attached:,} in {time.time()-t0:.1f}s")


def attach_tags(catalog, ns, asset_index):
    _p("\n[3/6] Attaching tags...")
    t0 = time.time()
    edges = []
    df = _safe_scan_all(catalog, ns, "tagrelationship", (
        "entityguid", "entitytypename", "tagname", "tagvalue",
    ))
    if df.empty:
        _p("  No tag relationships found")
        return edges
    _p(f"  {len(df):,} tag rows found")
    for _, row in df.iterrows():
        guid = _to_str(row.get("entityguid"))
        tag_name = _to_str(row.get("tagname"))
        tag_value = _to_str(row.get("tagvalue"))
        entity_type = _to_str(row.get("entitytypename"))
        if not guid or not tag_name:
            continue
        if guid in asset_index:
            asset_index[guid].tags.append({"tagname": tag_name, "tagvalue": tag_value})
            asset = asset_index[guid]
            edges.append(RelationshipEdge(
                source_guid=guid, source_name=asset.name, source_type=asset.asset_type,
                target_guid="", target_name=tag_name, target_type="Tag",
                relationship_type="TAG", metadata={"tagvalue": tag_value},
            ))
        else:
            edges.append(RelationshipEdge(
                source_guid=guid, source_name="", source_type=entity_type,
                target_guid="", target_name=tag_name, target_type="Tag",
                relationship_type="TAG", metadata={"tagvalue": tag_value},
            ))
    _p(f"  Tags done: {len(edges):,} relationships in {time.time()-t0:.1f}s")
    return edges


def attach_custom_metadata(catalog, ns, asset_index):
    _p("\n[4/6] Attaching custom metadata...")
    t0 = time.time()
    edges = []
    df = _safe_scan_all(catalog, ns, "custommetadatarelationship", (
        "entityguid", "setdisplayname", "attributedisplayname", "attributevalue",
    ))
    if df.empty:
        _p("  No custom metadata found")
        return edges
    _p(f"  {len(df):,} custom metadata rows found")
    for _, row in df.iterrows():
        guid = _to_str(row.get("entityguid"))
        set_name = _to_str(row.get("setdisplayname"))
        attr_name = _to_str(row.get("attributedisplayname"))
        attr_value = _to_str(row.get("attributevalue"))
        if not guid or not set_name:
            continue
        cm_entry = {"set": set_name, "attribute": attr_name, "value": attr_value}
        if guid in asset_index:
            asset_index[guid].custom_metadata.append(cm_entry)
            asset = asset_index[guid]
            edges.append(RelationshipEdge(
                source_guid=guid, source_name=asset.name, source_type=asset.asset_type,
                target_guid="", target_name=f"{set_name}.{attr_name}", target_type="CustomMetadata",
                relationship_type="CUSTOM_METADATA",
                metadata={"set": set_name, "attribute": attr_name, "value": attr_value},
            ))
    _p(f"  Custom metadata done: {len(edges):,} relationships in {time.time()-t0:.1f}s")
    return edges


def build_table_column_edges(asset_index):
    _p("\n[5/6] Building table→column relationships...")
    t0 = time.time()
    edges = []
    qn_to_table = {
        a.qualified_name: a
        for a in asset_index.values()
        if a.asset_type in ("Table", "View", "MaterialisedView") and a.qualified_name
    }
    _p(f"  {len(qn_to_table):,} tables/views indexed")
    for i, asset in enumerate(asset_index.values()):
        if asset.asset_type != "Column" or not asset.qualified_name:
            continue
        parts = asset.qualified_name.rsplit("/", 1)
        if len(parts) == 2:
            parent_qn = parts[0]
            if parent_qn in qn_to_table:
                table = qn_to_table[parent_qn]
                table.columns.append({
                    "guid": asset.guid,
                    "name": asset.name,
                    "description": asset.description,
                    "connector": asset.connector,
                    "custom_metadata": asset.custom_metadata if asset.custom_metadata else [],
                })
                edges.append(RelationshipEdge(
                    source_guid=table.guid, source_name=table.name, source_type=table.asset_type,
                    target_guid=asset.guid, target_name=asset.name, target_type="Column",
                    relationship_type="TABLE_COLUMN", metadata={"description": asset.description},
                ))
        if len(edges) > 0 and len(edges) % 1_000_000 == 0:
            _p(f"  ...{len(edges):,} edges built so far ({time.time()-t0:.0f}s elapsed)")
    _p(f"  Table→Column done: {len(edges):,} edges in {time.time()-t0:.1f}s")
    return edges


def build_lineage_edges(catalog, ns, asset_index):
    _p("\n[6/6] Building lineage relationships...")
    t0 = time.time()
    edges = []
    guid_map = asset_index
    qn_map = {a.qualified_name: a for a in asset_index.values() if a.qualified_name}

    for process_type in ["process", "columnprocess", "biprocess"]:
        try:
            tbl = _load_table_ci(catalog, ns, process_type)
            desired = ("guid", "name", "qualifiedname", "inputs", "outputs", "sql", "code")
            safe_fields = _probe_fields(tbl, desired)
            scan = tbl.scan(selected_fields=safe_fields)
            df = _normalize_columns(scan.to_pandas())
            _p(f"  {process_type}: {len(df):,} rows to process")
        except Exception:
            _p(f"  {process_type}: not found, skipping")
            continue
        for idx, row in df.iterrows():
            inputs_raw = row.get("inputs")
            try:
                inputs = list(inputs_raw) if inputs_raw is not None else []
            except TypeError:
                inputs = []
            outputs_raw = row.get("outputs")
            try:
                outputs = list(outputs_raw) if outputs_raw is not None else []
            except TypeError:
                outputs = []
            process_name = _to_str(row.get("name"))
            process_sql = _to_str(row.get("sql", ""))
            process_code = _to_str(row.get("code", ""))
            input_assets, output_assets = [], []
            for ref in inputs:
                ref_str = str(ref) if ref else ""
                asset = guid_map.get(ref_str) or qn_map.get(ref_str)
                if asset:
                    input_assets.append(asset)
            for ref in outputs:
                ref_str = str(ref) if ref else ""
                asset = guid_map.get(ref_str) or qn_map.get(ref_str)
                if asset:
                    output_assets.append(asset)
            for inp in input_assets:
                for out in output_assets:
                    inp.lineage_downstream.append(out.qualified_name or out.name)
                    out.lineage_upstream.append(inp.qualified_name or inp.name)
                    edge_meta = {"process_type": process_type, "process_name": process_name}
                    if process_sql:
                        edge_meta["sql"] = process_sql
                    if process_code:
                        edge_meta["code"] = process_code
                    edges.append(RelationshipEdge(
                        source_guid=inp.guid, source_name=inp.name, source_type=inp.asset_type,
                        target_guid=out.guid, target_name=out.name, target_type=out.asset_type,
                        relationship_type="LINEAGE",
                        metadata=edge_meta,
                    ))
            if (idx + 1) % 10_000 == 0:
                _p(f"    ...processed {idx+1:,} / {len(df):,} rows ({len(edges):,} lineage edges so far)")
        _p(f"  {process_type} done: {len(edges):,} total lineage edges so far")

    _p(f"  Lineage done: {len(edges):,} edges in {time.time()-t0:.1f}s")
    return edges


def build_glossary_edges(asset_index):
    _p("\n[+] Building glossary relationships...")
    t0 = time.time()
    edges = []
    term_by_name = {a.name: a for a in asset_index.values() if a.asset_type == "GlossaryTerm"}
    category_by_qn = {
        a.qualified_name: a for a in asset_index.values()
        if a.asset_type == "GlossaryCategory" and a.qualified_name
    }
    for asset in asset_index.values():
        for term_name in asset.glossary_terms:
            term_asset = term_by_name.get(term_name)
            edges.append(RelationshipEdge(
                source_guid=asset.guid, source_name=asset.name, source_type=asset.asset_type,
                target_guid=term_asset.guid if term_asset else "",
                target_name=term_name, target_type="GlossaryTerm",
                relationship_type="GLOSSARY_TERM", metadata={},
            ))
        if asset.asset_type == "GlossaryTerm" and asset.qualified_name:
            parts = asset.qualified_name.rsplit("/", 1)
            if len(parts) == 2:
                parent_qn = parts[0]
                cat = category_by_qn.get(parent_qn)
                if cat:
                    edges.append(RelationshipEdge(
                        source_guid=cat.guid, source_name=cat.name, source_type="GlossaryCategory",
                        target_guid=asset.guid, target_name=asset.name, target_type="GlossaryTerm",
                        relationship_type="GLOSSARY_TERM",
                        metadata={"direction": "category_contains_term"},
                    ))
    _p(f"  Glossary done: {len(edges):,} edges in {time.time()-t0:.1f}s")
    return edges


def resolve_glossary_uuids(asset_index):
    """Replace UUID strings in glossary_terms lists with resolved term names."""
    _p("\n[+] Resolving glossary term UUIDs...")
    t0 = time.time()
    guid_to_name = {
        guid: a.name for guid, a in asset_index.items()
        if a.asset_type == "GlossaryTerm" and a.name
    }
    resolved_count = 0
    for asset in asset_index.values():
        if not asset.glossary_terms:
            continue
        new_terms = []
        for term in asset.glossary_terms:
            if term in guid_to_name:
                new_terms.append(guid_to_name[term])
                resolved_count += 1
            else:
                new_terms.append(term)
        asset.glossary_terms = new_terms
    _p(f"  Resolved {resolved_count:,} UUID references in {time.time()-t0:.1f}s")


def validate_extraction(asset_index, all_edges):
    """Post-extraction health checks. Returns (ok: bool, issues: list[str], warnings: list[str])."""
    issues = []   # Fatal — extraction is broken
    warnings = [] # Non-fatal — data may be incomplete

    # --- Check 1: Core entity types loaded ---
    type_counts = {}
    for a in asset_index.values():
        type_counts[a.asset_type] = type_counts.get(a.asset_type, 0) + 1

    required_types = ["Table", "Column"]
    for t in required_types:
        if type_counts.get(t, 0) == 0:
            issues.append(f"CRITICAL: 0 {t} assets loaded — extraction likely failed")

    expected_types = ["View", "MaterialisedView", "GlossaryTerm", "DataProduct", "DataDomain"]
    for t in expected_types:
        if type_counts.get(t, 0) == 0:
            warnings.append(f"0 {t} assets — may be absent in tenant or scan failed")

    # --- Check 2: Table→Column edges exist ---
    tc_edges = sum(1 for e in all_edges if hasattr(e, "relationship_type") and e.relationship_type == "TABLE_COLUMN")
    tables_count = type_counts.get("Table", 0) + type_counts.get("View", 0) + type_counts.get("MaterialisedView", 0)
    if tables_count > 0 and tc_edges == 0:
        issues.append(f"CRITICAL: {tables_count:,} tables loaded but 0 table→column edges — column attachment broken")

    # --- Check 3: Lineage edges exist ---
    lineage_edges = sum(1 for e in all_edges if hasattr(e, "relationship_type") and e.relationship_type == "LINEAGE")
    if lineage_edges == 0:
        warnings.append("0 lineage edges — no process/columnprocess/biprocess data found")

    # --- Check 4: Key v2 fields populated (at least some) ---
    ai_desc_count = sum(1 for a in asset_index.values() if a.ai_description)
    definition_count = sum(1 for a in asset_index.values()
                          if a.asset_type in ("View", "MaterialisedView") and a.definition)
    views_count = type_counts.get("View", 0) + type_counts.get("MaterialisedView", 0)

    if ai_desc_count == 0:
        warnings.append("0 assets have AI descriptions — field may not exist or tenant has none")
    if views_count > 0 and definition_count == 0:
        warnings.append(f"{views_count:,} views loaded but 0 SQL definitions — definition field may not exist")

    # --- Check 5: READMEs attached ---
    readme_count = sum(1 for a in asset_index.values() if a.readme)
    if readme_count == 0:
        warnings.append("0 READMEs attached — readme table may be empty or attachment failed")

    # --- Check 6: Glossary terms have definitions ---
    glossary_terms = [a for a in asset_index.values() if a.asset_type == "GlossaryTerm"]
    if glossary_terms:
        with_desc = sum(1 for a in glossary_terms if a.description)
        if with_desc == 0:
            warnings.append(f"{len(glossary_terms):,} glossary terms but 0 have definitions — shortdescription field may be missing")

    return len(issues) == 0, issues, warnings


def _self_heal_table_columns(catalog, ns, asset_index):
    """Re-attempt table→column edge building if tables have 0 columns.

    This can happen when Tables are loaded from a full unfiltered scan
    that returns all columns (including non-matching ones) causing the
    column parent lookup to fail.
    """
    tables_with_columns = sum(1 for a in asset_index.values()
                              if a.asset_type in ("Table", "View", "MaterialisedView") and a.columns)
    tables_total = sum(1 for a in asset_index.values()
                       if a.asset_type in ("Table", "View", "MaterialisedView"))
    if tables_total > 0 and tables_with_columns == 0:
        _p("\n[SELF-HEAL] Tables have 0 columns — re-indexing qualified names...")
        # Rebuild qn_to_table with exact matching
        qn_to_table = {
            a.qualified_name: a
            for a in asset_index.values()
            if a.asset_type in ("Table", "View", "MaterialisedView") and a.qualified_name
        }
        _p(f"  {len(qn_to_table):,} tables/views indexed by qualified name")
        reattached = 0
        for asset in asset_index.values():
            if asset.asset_type != "Column" or not asset.qualified_name:
                continue
            parts = asset.qualified_name.rsplit("/", 1)
            if len(parts) == 2:
                parent_qn = parts[0]
                if parent_qn in qn_to_table:
                    qn_to_table[parent_qn].columns.append({
                        "guid": asset.guid,
                        "name": asset.name,
                        "description": asset.description,
                        "connector": asset.connector,
                        "custom_metadata": asset.custom_metadata if asset.custom_metadata else [],
                    })
                    reattached += 1
        _p(f"  [SELF-HEAL] Re-attached {reattached:,} columns")
        return reattached
    return 0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def extract_metadata(catalog, ns, tenant):
    t_total = time.time()
    _p(f"\n{'='*60}")
    _p(f"Metadata Extractor — {tenant}")
    _p(f"{'='*60}")

    asset_index, discovery_report = build_asset_index(catalog, ns)
    pull_asset_readmes(catalog, ns, asset_index)
    tag_edges      = attach_tags(catalog, ns, asset_index)
    cm_edges       = attach_custom_metadata(catalog, ns, asset_index)
    tc_edges       = build_table_column_edges(asset_index)
    lineage_edges  = build_lineage_edges(catalog, ns, asset_index)
    glossary_edges = build_glossary_edges(asset_index)
    resolve_glossary_uuids(asset_index)

    all_edges = tag_edges + cm_edges + tc_edges + lineage_edges + glossary_edges

    # --- Validation & self-heal ---
    _p(f"\n{'='*60}")
    _p("Post-extraction validation")
    _p(f"{'='*60}")
    ok, issues, warnings = validate_extraction(asset_index, all_edges)

    for w in warnings:
        _p(f"  WARNING: {w}")

    if not ok:
        for i in issues:
            _p(f"  ISSUE: {i}")

        # Self-heal: table→column edges
        if any("table→column edges" in i for i in issues):
            healed = _self_heal_table_columns(catalog, ns, asset_index)
            if healed > 0:
                _p("  Re-building table→column edges after self-heal...")
                tc_edges = build_table_column_edges(asset_index)
                all_edges = tag_edges + cm_edges + tc_edges + lineage_edges + glossary_edges

        # Re-validate
        ok2, issues2, _ = validate_extraction(asset_index, all_edges)
        if ok2:
            _p("  Self-heal RESOLVED all issues")
        else:
            for i in issues2:
                _p(f"  UNRESOLVED: {i}")
            _p("  Continuing with partial results — check output carefully")
    else:
        _p("  All checks passed")

    # Summary stats
    type_counts = {}
    for a in asset_index.values():
        type_counts[a.asset_type] = type_counts.get(a.asset_type, 0) + 1
    _p(f"\n  Asset breakdown:")
    for atype in sorted(type_counts.keys()):
        _p(f"    {atype}: {type_counts[atype]:,}")

    v2_stats = {
        "ai_descriptions": sum(1 for a in asset_index.values() if a.ai_description),
        "sql_definitions": sum(1 for a in asset_index.values() if a.definition),
        "announcements": sum(1 for a in asset_index.values() if a.announcement_type),
        "readmes": sum(1 for a in asset_index.values() if a.readme),
        "transform_sql": sum(1 for a in asset_index.values() if a.transform_raw_sql or a.transform_compiled_sql),
    }
    _p(f"  v2 field coverage:")
    for k, v in v2_stats.items():
        _p(f"    {k}: {v:,}")

    # Discovery report summary
    _p(f"\n  Discovery report:")
    _p(f"    Categorized: {sum(len(v) for v in discovery_report['categorized'].values()):,} tables")
    _p(f"    Skipped: {len(discovery_report['skipped']):,} tables")
    _p(f"    Uncategorized: {len(discovery_report['uncategorized']):,} tables")

    _p(f"\nExtraction complete: {len(asset_index):,} assets, {len(all_edges):,} edges in {time.time()-t_total:.1f}s")
    return asset_index, all_edges, discovery_report
