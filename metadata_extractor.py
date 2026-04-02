"""
metadata_extractor.py

Extract per-asset metadata details and relationship map from MDLH for a given tenant instance.
Returns asset_index and all_edges in memory — writing is handled by asset_scorer.py.
"""

import time
import pandas as pd
from dataclasses import dataclass, field, asdict
from typing import Optional


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


def _load_table_ci(catalog, namespace, table_name):
    """Load an Iceberg table with case-insensitive name matching.

    Tries lowercase first, then PascalCase (e.g. 'readme' → 'Readme',
    'tagrelationship' → 'TagRelationship'). Returns the loaded table
    or raises the original exception if neither works.
    """
    # Try as-is first (lowercase)
    try:
        return catalog.load_table((namespace, table_name))
    except Exception:
        pass
    # Try PascalCase: "tagrelationship" → "TagRelationship"
    pascal = table_name[0].upper() + table_name[1:]
    # Handle known compound names
    _pascal_map = {
        "tagrelationship": "TagRelationship",
        "custommetadatarelationship": "CustomMetadataRelationship",
        "columnprocess": "ColumnProcess",
        "biprocess": "BIProcess",
        "glossaryterm": "GlossaryTerm",
        "glossarycategory": "GlossaryCategory",
        "dataproduct": "DataProduct",
        "datadomain": "DataDomain",
        "materialisedview": "MaterialisedView",
        "powerbidashboard": "PowerBIDashboard",
        "powerbireport": "PowerBIReport",
        "powerbidataset": "PowerBIDataset",
        "powerbitable": "PowerBITable",
        "tableaudashboard": "TableauDashboard",
        "tableauworkbook": "TableauWorkbook",
        "lookerdashboard": "LookerDashboard",
        "lookerlook": "LookerLook",
        "lookerexplore": "LookerExplore",
        "lookerfield": "LookerField",
        "lookerview": "LookerView",
        "metabasedashboard": "MetabaseDashboard",
        "supersetdashboard": "SupersetDashboard",
        "sigmaworkbook": "SigmaWorkbook",
        "redashdashboard": "RedashDashboard",
        "modereport": "ModeReport",
        "cognosdashboard": "CognosDashboard",
        "domodashboard": "DomoDashboard",
        "qliksheet": "QlikSheet",
        "thoughtspotliveboard": "ThoughtspotLiveboard",
        "microstrategydossier": "MicrostrategyDossier",
        "microstrategyreport": "MicrostrategyReport",
        "dbtmodel": "DbtModel",
        "dbtsource": "DbtSource",
        "dbtmetric": "DbtMetric",
        "airflowdag": "AirflowDag",
        "airflowtask": "AirflowTask",
        "adfpipeline": "AdfPipeline",
        "fivetranconnector": "FivetranConnector",
        "sparkjob": "SparkJob",
        "mcmonitor": "MCMonitor",
        "sodacheck": "SodaCheck",
        "anomalocheck": "AnomaloCheck",
    }
    pascal = _pascal_map.get(table_name, pascal)
    try:
        return catalog.load_table((namespace, pascal))
    except Exception:
        pass
    # Final attempt: capitalize first letter only
    cap = table_name.capitalize()
    if cap != pascal:
        try:
            return catalog.load_table((namespace, cap))
        except Exception:
            pass
    raise Exception(f"Table not found: tried '{table_name}', '{pascal}', '{cap}'")


def _normalize_columns(df):
    """Normalize DataFrame column names to lowercase for consistent access."""
    df.columns = [c.lower() for c in df.columns]
    return df


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


CORE_ASSET_TABLES = [
    ("table",            "Table",            ("databasename", "schemaname", "ownerusers", "ownergroups", "certificatestatus", "meanings")),
    ("view",             "View",             ("databasename", "schemaname", "ownerusers", "certificatestatus")),
    ("materialisedview", "MaterialisedView", ("databasename", "schemaname", "ownerusers", "certificatestatus")),
    ("column",           "Column",           ("datatype", "tablename", "databasename", "schemaname")),
    ("dataproduct",      "DataProduct",      ("ownerusers",)),
    ("datadomain",       "DataDomain",       ()),
    ("glossaryterm",     "GlossaryTerm",     ("shortdescription", "longdescription")),
    ("glossarycategory", "GlossaryCategory", ()),
    ("connection",       "Connection",       ()),
    ("customentity",     "CustomEntity",     ("ownerusers", "ownergroups", "certificatestatus", "meanings")),
]

BI_ASSET_TYPES = [
    "powerbidashboard", "powerbireport", "tableaudashboard", "tableauworkbook",
    "lookerdashboard", "lookerlook", "metabasedashboard", "supersetdashboard",
    "sigmaworkbook", "redashdashboard", "modereport", "cognosdashboard",
    "domodashboard", "qliksheet", "thoughtspotliveboard",
    "microstrategydossier", "microstrategyreport",
    "powerbidataset", "powerbitable", "lookerexplore", "lookerfield", "lookerview",
]

TRANSFORM_ASSET_TYPES = [
    "dbtmodel", "dbtsource", "dbtmetric",
    "airflowdag", "airflowtask",
    "adfpipeline", "fivetranconnector", "sparkjob",
    "mcmonitor", "sodacheck", "anomalocheck",
]


def build_asset_index(catalog, ns) -> dict:
    _p("\n[1/6] Building asset index...")
    t0 = time.time()
    asset_index = {}

    for table_name, asset_type, extra in CORE_ASSET_TABLES:
        base_fields = ("guid", "name", "qualifiedname", "description", "connectorname")
        fields = base_fields + extra
        try:
            tbl = _load_table_ci(catalog, ns, table_name)
            _p(f"  Scanning {asset_type}...")
            scan = tbl.scan(row_filter="status == 'ACTIVE'", selected_fields=fields)
            df = _normalize_columns(scan.to_pandas())
        except Exception as e:
            _p(f"  SKIP {table_name}: {e}")
            continue

        before = len(asset_index)
        for _, row in df.iterrows():
            guid = _to_str(row.get("guid"))
            if not guid:
                continue
            detail = AssetDetail(
                guid=guid,
                name=_to_str(row.get("name")),
                asset_type=asset_type,
                qualified_name=_to_str(row.get("qualifiedname")),
                description=_to_str(row.get("description")),
                connector=_to_str(row.get("connectorname")),
                database=_to_str(row.get("databasename", "")),
                schema_name=_to_str(row.get("schemaname", "")),
                owner_users=_to_list(row.get("ownerusers")),
                owner_groups=_to_list(row.get("ownergroups")),
                certificate_status=_to_str(row.get("certificatestatus", "")),
            )
            if asset_type == "GlossaryTerm":
                detail.description = _to_str(row.get("shortdescription", ""))
                detail.readme = _to_str(row.get("longdescription", ""))
            detail.glossary_terms = _to_list(row.get("meanings"))
            asset_index[guid] = detail
        _p(f"    {asset_type}: {len(asset_index) - before:,} loaded (total so far: {len(asset_index):,})")

    for group_name, type_list in [("BI assets", BI_ASSET_TYPES), ("Transform assets", TRANSFORM_ASSET_TYPES)]:
        _p(f"  Scanning {group_name}...")
        group_count = 0
        for table_name in type_list:
            try:
                tbl = _load_table_ci(catalog, ns, table_name)
                if group_name == "BI assets":
                    scan = tbl.scan(
                        row_filter="status == 'ACTIVE'",
                        selected_fields=("guid", "name", "qualifiedname", "description", "connectorname", "ownerusers", "certificatestatus"),
                    )
                else:
                    scan = tbl.scan(
                        row_filter="status == 'ACTIVE'",
                        selected_fields=("guid", "name", "qualifiedname", "description", "connectorname"),
                    )
                df = _normalize_columns(scan.to_pandas())
            except Exception:
                continue
            for _, row in df.iterrows():
                guid = _to_str(row.get("guid"))
                if not guid:
                    continue
                asset_index[guid] = AssetDetail(
                    guid=guid,
                    name=_to_str(row.get("name")),
                    asset_type=table_name,
                    qualified_name=_to_str(row.get("qualifiedname")),
                    description=_to_str(row.get("description")),
                    connector=_to_str(row.get("connectorname")),
                    owner_users=_to_list(row.get("ownerusers")) if group_name == "BI assets" else [],
                    certificate_status=_to_str(row.get("certificatestatus", "")) if group_name == "BI assets" else "",
                )
                group_count += 1
        _p(f"    {group_name}: {group_count:,} loaded (total so far: {len(asset_index):,})")

    _p(f"  Asset index complete: {len(asset_index):,} assets in {time.time()-t0:.1f}s")
    return asset_index


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
            scan = tbl.scan(selected_fields=("guid", "name", "qualifiedname", "inputs", "outputs"))
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
                    edges.append(RelationshipEdge(
                        source_guid=inp.guid, source_name=inp.name, source_type=inp.asset_type,
                        target_guid=out.guid, target_name=out.name, target_type=out.asset_type,
                        relationship_type="LINEAGE",
                        metadata={"process_type": process_type, "process_name": process_name},
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


def extract_metadata(catalog, ns, tenant):
    t_total = time.time()
    _p(f"\n{'='*60}")
    _p(f"Metadata Extractor — {tenant}")
    _p(f"{'='*60}")

    asset_index   = build_asset_index(catalog, ns)
    pull_asset_readmes(catalog, ns, asset_index)
    tag_edges      = attach_tags(catalog, ns, asset_index)
    cm_edges       = attach_custom_metadata(catalog, ns, asset_index)
    tc_edges       = build_table_column_edges(asset_index)
    lineage_edges  = build_lineage_edges(catalog, ns, asset_index)
    glossary_edges = build_glossary_edges(asset_index)
    resolve_glossary_uuids(asset_index)

    all_edges = tag_edges + cm_edges + tc_edges + lineage_edges + glossary_edges
    _p(f"\nExtraction complete: {len(asset_index):,} assets, {len(all_edges):,} edges in {time.time()-t_total:.1f}s")
    return asset_index, all_edges
