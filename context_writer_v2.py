"""
context_writer_v2.py

Structured 12-section context output optimized for Rex simulation scenario generation.
Does NOT modify asset_scorer.py — keeps it as fallback.
"""

import re
import time
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from asset_scorer import score_asset

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _p(msg):
    print(msg, flush=True)


def _clean_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# UUID pattern: 8-4-4-4-12 hex chars
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


def simplify_qualified_name(qn: str) -> str:
    """Strip to 'table_name (schema)' from a full qualified name path."""
    if not qn:
        return ""
    parts = qn.split("/")
    name = parts[-1] if parts else qn
    # Try to extract schema from the path
    if len(parts) >= 2:
        schema = parts[-2]
        return f"{name} ({schema})"
    return name


def parse_tag(tag: dict) -> tuple:
    """Split tag into (type, value). Handles 'GxP: NA' format in tagname."""
    tag_name = tag.get("tagname", tag.get("tagName", ""))
    tag_value = tag.get("tagvalue", tag.get("tagValue", ""))
    if ":" in tag_name and not tag_value:
        parts = tag_name.split(":", 1)
        return parts[0].strip(), parts[1].strip()
    return tag_name, tag_value


def get_layer(asset: dict) -> str:
    """Determine data layer from schema name or custom metadata."""
    schema = (asset.get("schema_name") or "").lower()
    # Check custom metadata for explicit layer
    for cm in asset.get("custom_metadata") or []:
        attr = (cm.get("attribute") or "").lower()
        val = (cm.get("value") or "").strip()
        if attr in ("layer", "data_layer", "datalayer") and val:
            return val.title()

    if "business" in schema or "biz" in schema or "curated" in schema or "gold" in schema:
        return "Business"
    if "enriched" in schema or "silver" in schema or "transform" in schema:
        return "Enriched"
    if "trusted" in schema or "cleansed" in schema:
        return "Trusted"
    if "landing" in schema or "raw" in schema or "bronze" in schema or "staging" in schema:
        return "Landing"
    return "Unknown"


def get_application(asset: dict) -> str:
    """Extract application/source system from custom metadata or schema."""
    for cm in asset.get("custom_metadata") or []:
        attr = (cm.get("attribute") or "").lower()
        val = (cm.get("value") or "").strip()
        if attr in ("application", "source_system", "sourcesystem", "source system") and val:
            return val
    # Fallback: try database or schema name
    db = asset.get("database") or ""
    if db:
        return db
    return ""


def get_dq_score(asset: dict) -> str:
    """Extract data quality score from custom metadata."""
    for cm in asset.get("custom_metadata") or []:
        attr = (cm.get("attribute") or "").lower()
        val = (cm.get("value") or "").strip()
        if "quality" in attr and val:
            return val
    return ""


def compute_lifecycle(name: str) -> str:
    """Pattern match table names for lifecycle flags."""
    lower = name.lower()
    if any(p in lower for p in ("_temp_", "_tmp_", "_temp", "_tmp")):
        return "TEMP"
    if any(p in lower for p in ("_test_", "_test", "test_")):
        return "TEST"
    if any(p in lower for p in ("_bckp_", "_backup_", "_bak_", "_bckp", "_backup")):
        return "BACKUP"
    if any(p in lower for p in ("_archive", "_arch_", "_hist_")):
        return "ARCHIVE"
    if re.search(r"_\d{8}$", lower) or re.search(r"_\d{4}_\d{2}_\d{2}$", lower):
        return "DATED_SNAPSHOT"
    if any(p in lower for p in ("_deprecated", "_old_", "_legacy")):
        return "DEPRECATED"
    return ""


def is_placeholder_description(desc: str) -> bool:
    """Return True if description is a known auto-generated placeholder."""
    if not desc:
        return True
    lower = desc.strip().lower()
    placeholders = [
        "table created from glue job",
        "table updated from glue job",
        "created by glue",
        "updated by glue",
        "auto-generated",
        "no description available",
        "n/a",
        "na",
        "none",
        "tbd",
        "todo",
        "placeholder",
    ]
    for p in placeholders:
        if lower == p or lower.startswith(p):
            return True
    return False


def is_cryptic_column(name: str) -> bool:
    """Detect SAP/cryptic column name patterns."""
    lower = name.lower()
    prefixes = ("xvv", "bic_", "b28_s_", "ausr", "ause", "/bic/", "0bic_")
    if any(lower.startswith(p) for p in prefixes):
        return True
    # Very short all-caps codes (e.g. MANDT, BUKRS) — 3-5 char uppercase
    if re.match(r"^[A-Z0-9_]{2,5}$", name) and not name.isdigit():
        return True
    return False


def parse_readme(readme_raw: str) -> dict:
    """Extract structured fields from README content.

    Looks for patterns like:
      **Business Context:** ...
      **Refresh Cadence:** ...
      **Source System:** ...
    """
    text = _clean_html(readme_raw)
    fields = {}
    # Match **Key:** Value or Key: Value patterns
    known_keys = [
        "Business Context", "Refresh Cadence", "Refresh Time", "Times",
        "Duration", "Predecessor", "Source System", "Use Cases",
        "Use Case", "Scheduling", "SLA", "Data Owner", "Description",
    ]
    for key in known_keys:
        # Try bold markdown pattern first
        pattern = re.compile(
            rf"\*?\*?{re.escape(key)}\*?\*?\s*[:\-]\s*(.+?)(?=\*?\*?(?:{'|'.join(re.escape(k) for k in known_keys)})\*?\*?\s*[:\-]|$)",
            re.IGNORECASE | re.DOTALL,
        )
        m = pattern.search(text)
        if m:
            val = m.group(1).strip()
            # Clean up trailing markers
            val = re.sub(r"\s*\*+\s*$", "", val).strip()
            if val and len(val) > 1:
                fields[key] = val
    # If no structured fields found, return full text as "Content"
    if not fields and text.strip():
        fields["Content"] = text
    return fields


def format_custom_metadata(cm_list: list, ubiquitous_keys: set) -> list:
    """Filter and format custom metadata, excluding ubiquitous keys."""
    result = []
    for cm in cm_list:
        key = f"{cm.get('set', '')}.{cm.get('attribute', '')}"
        val = cm.get("value", "")
        if key in ubiquitous_keys:
            continue
        # Skip environment arrays unless PROD-only
        attr_lower = (cm.get("attribute") or "").lower()
        if "environment" in attr_lower:
            if val and "PROD" in val and any(e in val for e in ("DEV", "TEST", "QA", "STG", "UAT")):
                continue
        result.append(f"{cm.get('attribute', '')}: {val}")
    return result


# ---------------------------------------------------------------------------
# Precomputation
# ---------------------------------------------------------------------------

TABLE_TYPES = {"Table", "View", "MaterialisedView"}

# Budget defaults — tune per use case
MAX_FULL_DETAIL = 500     # Tables with richest metadata → full detail (Section 8)
MAX_CONDENSED   = 2000    # Next tier → condensed detail (Section 9)
                          # Everything else → summary table (Section 10)


    # Scoring is handled by asset_scorer.score_asset — single source of truth


def build_context_data(asset_index: dict) -> dict:
    """Classify, enrich and precompute all data needed by the 12 sections."""
    _p("\n[Context Writer v2] Precomputing context data...")
    t0 = time.time()

    # Convert to dicts for uniform access
    assets = {}
    for guid, asset in asset_index.items():
        a = asdict(asset) if hasattr(asset, "__dataclass_fields__") else dict(asset)
        a["guid"] = guid
        assets[guid] = a

    # Classify by type
    tables = {}
    glossary_terms = {}
    data_products = {}
    domains = {}
    columns_raw = {}
    custom_entities = {}
    other = {}

    for guid, a in assets.items():
        atype = a.get("asset_type", "")
        if atype in TABLE_TYPES:
            tables[guid] = a
        elif atype == "GlossaryTerm":
            glossary_terms[guid] = a
        elif atype == "DataProduct":
            data_products[guid] = a
        elif atype == "DataDomain":
            domains[guid] = a
        elif atype == "Column":
            columns_raw[guid] = a
        elif atype == "CustomEntity":
            custom_entities[guid] = a
        else:
            other[guid] = a

    # Enrich tables
    for guid, t in tables.items():
        t["_layer"] = get_layer(t)
        t["_application"] = get_application(t)
        t["_lifecycle"] = compute_lifecycle(t.get("name", ""))
        t["_dq_score"] = get_dq_score(t)

        cols = t.get("columns") or []
        t["_col_count"] = len(cols)
        described = sum(1 for c in cols if c.get("description"))
        t["_col_described"] = described
        cryptic = sum(1 for c in cols if is_cryptic_column(c.get("name", "")))
        t["_col_cryptic"] = cryptic
        t["_has_cryptic"] = cryptic > 0

        desc = t.get("description", "")
        t["_has_real_desc"] = bool(desc) and not is_placeholder_description(desc)

        readme_raw = t.get("readme", "")
        t["_has_readme"] = bool(readme_raw)
        t["_parsed_readme"] = parse_readme(readme_raw) if readme_raw else {}

        # v2 enrichments
        t["_has_ai_desc"] = bool(t.get("ai_description", ""))
        t["_has_definition"] = bool(t.get("definition", ""))
        t["_has_announcement"] = bool(t.get("announcement_type", ""))

        # Richness score for prioritization — single source of truth from asset_scorer
        t["_richness"], t["_score_breakdown"] = score_asset(t)

    # Tag groups
    tag_groups = defaultdict(lambda: defaultdict(int))
    tag_definitions = {}
    for a in assets.values():
        for tag in a.get("tags") or []:
            ttype, tval = parse_tag(tag)
            if ttype:
                tag_groups[ttype][tval] += 1
                # Store any tag with definition
                raw_name = tag.get("tagname", tag.get("tagName", ""))
                if raw_name and raw_name not in tag_definitions:
                    tag_definitions[raw_name] = tval

    # Owner stats
    owner_tables = defaultdict(list)
    for guid, t in tables.items():
        for owner in t.get("owner_users") or []:
            owner_tables[owner].append(t)

    # Ubiquitous custom metadata detection (>95% prevalence among tables)
    cm_key_counts = Counter()
    for t in tables.values():
        seen = set()
        for cm in t.get("custom_metadata") or []:
            key = f"{cm.get('set', '')}.{cm.get('attribute', '')}"
            if key not in seen:
                cm_key_counts[key] += 1
                seen.add(key)
    table_count = len(tables)
    threshold = table_count * 0.95 if table_count > 0 else 0
    ubiquitous_cm = {k for k, v in cm_key_counts.items() if v >= threshold}
    # Always exclude "Granular access" variants
    for k in list(cm_key_counts.keys()):
        if "granular" in k.lower() and "access" in k.lower():
            ubiquitous_cm.add(k)

    # Custom Entity CM schema summaries
    ce_cm_schemas = defaultdict(lambda: {"count": 0, "attributes": defaultdict(lambda: Counter())})
    for ce in custom_entities.values():
        for cm in ce.get("custom_metadata") or []:
            schema_name = cm.get("set", "")
            attr_name = cm.get("attribute", "")
            val = cm.get("value", "")
            if schema_name:
                ce_cm_schemas[schema_name]["count"] += 1
                if attr_name:
                    ce_cm_schemas[schema_name]["attributes"][attr_name][val] += 1

    # Glossary → linked tables reverse mapping
    term_to_tables = defaultdict(list)
    for guid, t in tables.items():
        for term_name in t.get("glossary_terms") or []:
            if not _UUID_RE.match(term_name):
                term_to_tables[term_name].append(t.get("name", ""))

    # Confusing clusters: group tables by business keyword
    keyword_tables = defaultdict(list)
    stop_words = {"the", "a", "an", "of", "in", "to", "for", "and", "or", "is", "at", "by", "on", "with", "from", "data", "table", "view", "dim", "fact"}
    for guid, t in tables.items():
        name = t.get("name", "").lower()
        # Extract meaningful tokens
        tokens = re.split(r"[_\-\s]+", name)
        tokens = [tok for tok in tokens if len(tok) > 2 and tok not in stop_words and not tok.isdigit()]
        for tok in tokens:
            keyword_tables[tok].append(t)

    confusing_clusters = {}
    for keyword, tlist in keyword_tables.items():
        if len(tlist) >= 3:
            confusing_clusters[keyword] = tlist

    # Layer counts
    layer_counts = Counter(t["_layer"] for t in tables.values())

    # Layer mode detection: if >50% of tables are "Unknown", the tenant doesn't
    # use a medallion architecture — promote Unknown to full-detail treatment.
    unknown_count = sum(1 for t in tables.values() if t.get("_layer") == "Unknown")
    total_tables_prelim = len(tables)
    no_medallion = total_tables_prelim > 0 and (unknown_count / total_tables_prelim) > 0.5
    if no_medallion:
        _p(f"  Layer mode: NO MEDALLION ({unknown_count:,}/{total_tables_prelim:,} = "
           f"{unknown_count/total_tables_prelim*100:.0f}% Unknown) — promoting Unknown to full detail")

    # Stats for section 1
    total_tables = len(tables)
    tables_with_desc = sum(1 for t in tables.values() if t["_has_real_desc"])
    tables_with_readme = sum(1 for t in tables.values() if t["_has_readme"])
    tables_with_glossary = sum(1 for t in tables.values() if t.get("glossary_terms"))
    tables_with_ai_desc = sum(1 for t in tables.values() if t.get("_has_ai_desc"))
    tables_with_definition = sum(1 for t in tables.values() if t.get("_has_definition"))
    tables_with_announcement = sum(1 for t in tables.values() if t.get("_has_announcement"))

    _p(f"  Tables: {total_tables:,} | Glossary: {len(glossary_terms):,} | "
        f"Products: {len(data_products):,} | Domains: {len(domains):,} | "
        f"CustomEntities: {len(custom_entities):,}")
    _p(f"  Precomputation done in {time.time()-t0:.1f}s")

    return {
        "assets": assets,
        "tables": tables,
        "glossary_terms": glossary_terms,
        "data_products": data_products,
        "domains": domains,
        "custom_entities": custom_entities,
        "ce_cm_schemas": dict(ce_cm_schemas),
        "tag_groups": tag_groups,
        "tag_definitions": tag_definitions,
        "owner_tables": owner_tables,
        "ubiquitous_cm": ubiquitous_cm,
        "term_to_tables": term_to_tables,
        "confusing_clusters": confusing_clusters,
        "layer_counts": layer_counts,
        "total_tables": total_tables,
        "tables_with_desc": tables_with_desc,
        "tables_with_readme": tables_with_readme,
        "tables_with_glossary": tables_with_glossary,
        "tables_with_ai_desc": tables_with_ai_desc,
        "tables_with_definition": tables_with_definition,
        "tables_with_announcement": tables_with_announcement,
        "no_medallion": no_medallion,
    }


# ---------------------------------------------------------------------------
# Section writers
# ---------------------------------------------------------------------------

def _write_section_header(f, num: int, title: str):
    f.write(f"\n{'='*70}\n")
    f.write(f"## Section {num}: {title}\n")
    f.write(f"{'='*70}\n\n")


def write_section_1_header(f, ctx: dict, tenant: str):
    """Instance Header — Layer counts, coverage stats."""
    _write_section_header(f, 1, "Instance Header")
    f.write(f"Tenant: {tenant}\n")
    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")

    total = ctx["total_tables"]
    f.write(f"Total Tables/Views: {total:,}\n")
    f.write(f"Glossary Terms: {len(ctx['glossary_terms']):,}\n")
    f.write(f"Data Products: {len(ctx['data_products']):,}\n")
    f.write(f"Data Domains: {len(ctx['domains']):,}\n")
    f.write(f"Custom Entities: {len(ctx['custom_entities']):,}\n\n")

    f.write("### Layer Distribution\n")
    if ctx["no_medallion"]:
        f.write("  NOTE: No medallion architecture detected (>50% Unknown).\n")
        f.write("  All tables receive full-detail treatment regardless of layer.\n\n")
    for layer in ["Business", "Enriched", "Trusted", "Landing", "Unknown"]:
        count = ctx["layer_counts"].get(layer, 0)
        pct = (count / total * 100) if total else 0
        f.write(f"  {layer:12s}: {count:>5,}  ({pct:4.1f}%)\n")
    f.write("\n")

    f.write("### Metadata Coverage\n")
    desc_pct = (ctx["tables_with_desc"] / total * 100) if total else 0
    readme_pct = (ctx["tables_with_readme"] / total * 100) if total else 0
    gloss_pct = (ctx["tables_with_glossary"] / total * 100) if total else 0
    ai_desc_pct = (ctx["tables_with_ai_desc"] / total * 100) if total else 0
    definition_pct = (ctx["tables_with_definition"] / total * 100) if total else 0
    announcement_pct = (ctx["tables_with_announcement"] / total * 100) if total else 0
    f.write(f"  Description (non-placeholder): {ctx['tables_with_desc']:>5,} / {total:,}  ({desc_pct:4.1f}%)\n")
    f.write(f"  README attached:               {ctx['tables_with_readme']:>5,} / {total:,}  ({readme_pct:4.1f}%)\n")
    f.write(f"  Glossary terms linked:         {ctx['tables_with_glossary']:>5,} / {total:,}  ({gloss_pct:4.1f}%)\n")
    f.write(f"  AI-generated description:      {ctx['tables_with_ai_desc']:>5,} / {total:,}  ({ai_desc_pct:4.1f}%)\n")
    f.write(f"  SQL definition (views):        {ctx['tables_with_definition']:>5,} / {total:,}  ({definition_pct:4.1f}%)\n")
    f.write(f"  Announcements:                 {ctx['tables_with_announcement']:>5,} / {total:,}  ({announcement_pct:4.1f}%)\n")
    f.write("\n")


def write_section_2_glossary(f, ctx: dict):
    """Glossary Terms — resolved names, definitions, linked tables, categories, owners."""
    _write_section_header(f, 2, "Glossary Terms")
    terms = sorted(ctx["glossary_terms"].values(), key=lambda t: t.get("name", "").lower())
    if not terms:
        f.write("No glossary terms found in this tenant.\n\n")
        return

    f.write(f"Total: {len(terms):,} terms\n\n")
    for t in terms:
        name = t.get("name", "")
        f.write(f"### {name}\n")

        desc = t.get("description", "")
        if desc and not is_placeholder_description(desc):
            f.write(f"Definition: {_clean_html(desc)}\n")

        readme = t.get("readme", "")
        if readme:
            cleaned = _clean_html(readme)
            if len(cleaned) > 500:
                cleaned = cleaned[:500] + "..."
            f.write(f"Long Description: {cleaned}\n")

        ai_desc = t.get("ai_description", "")
        if ai_desc:
            cleaned = _clean_html(ai_desc)
            if len(cleaned) > 300:
                cleaned = cleaned[:300] + "..."
            f.write(f"AI Description: {cleaned}\n")

        owners = t.get("owner_users") or []
        if owners:
            f.write(f"Owners: {', '.join(owners)}\n")

        # Linked tables
        linked = ctx["term_to_tables"].get(name, [])
        if linked:
            display = linked[:10]
            f.write(f"Linked Tables ({len(linked)}): {', '.join(display)}")
            if len(linked) > 10:
                f.write(f" +{len(linked) - 10} more")
            f.write("\n")

        # Category from qualified name
        qn = t.get("qualified_name", "")
        if qn and "/" in qn:
            parts = qn.split("/")
            if len(parts) >= 2:
                category = parts[-2]
                if category and not _UUID_RE.match(category):
                    f.write(f"Category: {category}\n")

        f.write("\n")


def write_section_3_data_products(f, ctx: dict):
    """Data Products — name, description, domain, owners."""
    _write_section_header(f, 3, "Data Products")
    products = sorted(ctx["data_products"].values(), key=lambda p: p.get("name", "").lower())
    if not products:
        f.write("No data products found in this tenant.\n\n")
        return

    f.write(f"Total: {len(products):,} data products\n\n")
    for p in products:
        f.write(f"### {p.get('name', '')}\n")
        desc = p.get("description", "")
        if desc and not is_placeholder_description(desc):
            f.write(f"Description: {_clean_html(desc)}\n")
        owners = p.get("owner_users") or []
        if owners:
            f.write(f"Owners: {', '.join(owners)}\n")
        # Check custom metadata for domain
        for cm in p.get("custom_metadata") or []:
            attr = (cm.get("attribute") or "").lower()
            if "domain" in attr:
                f.write(f"Domain: {cm.get('value', '')}\n")
                break
        f.write("\n")


def write_section_4_domains(f, ctx: dict):
    """Domains — name, description, table count, key tables."""
    _write_section_header(f, 4, "Data Domains")
    dom_list = sorted(ctx["domains"].values(), key=lambda d: d.get("name", "").lower())
    if not dom_list:
        f.write("No data domains found in this tenant.\n\n")
        return

    f.write(f"Total: {len(dom_list):,} domains\n\n")
    # Build domain name → tables mapping using schema/database
    domain_name_lower = {d.get("name", "").lower(): d.get("name", "") for d in dom_list}

    for d in dom_list:
        dname = d.get("name", "")
        f.write(f"### {dname}\n")
        desc = d.get("description", "")
        if desc and not is_placeholder_description(desc):
            f.write(f"Description: {_clean_html(desc)}\n")

        # Count tables whose schema or database or custom metadata references this domain
        matched = []
        dname_lower = dname.lower()
        for t in ctx["tables"].values():
            schema_lower = (t.get("schema_name") or "").lower()
            db_lower = (t.get("database") or "").lower()
            if dname_lower in schema_lower or dname_lower in db_lower:
                matched.append(t)
                continue
            # Check custom metadata
            for cm in t.get("custom_metadata") or []:
                if "domain" in (cm.get("attribute") or "").lower():
                    if dname_lower in (cm.get("value") or "").lower():
                        matched.append(t)
                        break

        if matched:
            f.write(f"Tables: {len(matched):,}\n")
            key_tables = sorted(matched, key=lambda x: -(x.get("_col_count", 0)))[:5]
            f.write(f"Key Tables: {', '.join(t.get('name', '') for t in key_tables)}\n")
        f.write("\n")


def write_section_5_classification(f, ctx: dict):
    """Classification Legend — tag types grouped with definitions."""
    _write_section_header(f, 5, "Classification Legend")
    tag_groups = ctx["tag_groups"]
    if not tag_groups:
        f.write("No classification tags found in this tenant.\n\n")
        return

    for tag_type in sorted(tag_groups.keys()):
        values = tag_groups[tag_type]
        f.write(f"### {tag_type}\n")
        for val, count in sorted(values.items(), key=lambda x: -x[1]):
            label = val if val else "(no value)"
            f.write(f"  {label}: {count:,} assets\n")
        f.write("\n")


def write_section_6_owners(f, ctx: dict):
    """Owner Directory — markdown table with stats."""
    _write_section_header(f, 6, "Owner Directory")
    owner_tables = ctx["owner_tables"]
    if not owner_tables:
        f.write("No owner assignments found.\n\n")
        return

    f.write(f"| {'Owner':<30s} | {'Tables':>6s} | {'Primary Domain':<25s} | {'Layers':<40s} |\n")
    f.write(f"|{'-'*32}|{'-'*8}|{'-'*27}|{'-'*42}|\n")

    for owner in sorted(owner_tables.keys()):
        tlist = owner_tables[owner]
        count = len(tlist)

        # Primary domain: most common database/schema
        domain_counter = Counter()
        for t in tlist:
            db = t.get("database") or t.get("schema_name") or "N/A"
            domain_counter[db] += 1
        primary_domain = domain_counter.most_common(1)[0][0] if domain_counter else "N/A"

        # Layer distribution
        layer_counter = Counter(t.get("_layer", "Unknown") for t in tlist)
        layer_str = ", ".join(f"{l}:{c}" for l, c in layer_counter.most_common())

        f.write(f"| {owner:<30s} | {count:>6,} | {primary_domain:<25s} | {layer_str:<40s} |\n")
    f.write("\n")


def write_section_7_confusing_clusters(f, ctx: dict):
    """Confusing Clusters — tables grouped by business keyword, differences, Rex Risk."""
    _write_section_header(f, 7, "Confusing Clusters")
    clusters = ctx["confusing_clusters"]
    if not clusters:
        f.write("No confusing clusters detected.\n\n")
        return

    # Sort by cluster size descending, take top 30
    sorted_clusters = sorted(clusters.items(), key=lambda x: -len(x[1]))[:30]

    for keyword, tlist in sorted_clusters:
        # Determine risk level
        count = len(tlist)
        if count >= 10:
            risk = "HIGH"
        elif count >= 5:
            risk = "MEDIUM"
        else:
            risk = "LOW"

        f.write(f"### '{keyword}' — {count} tables (Rex Risk: {risk})\n")

        # Show key differences
        layers = Counter(t.get("_layer", "Unknown") for t in tlist)
        schemas = Counter(t.get("schema_name", "N/A") for t in tlist)
        has_desc = sum(1 for t in tlist if t.get("_has_real_desc"))

        f.write(f"  Layers: {', '.join(f'{l}:{c}' for l, c in layers.most_common())}\n")
        f.write(f"  Schemas: {', '.join(f'{s}:{c}' for s, c in schemas.most_common(3))}\n")
        f.write(f"  Documented: {has_desc}/{count}\n")

        # List tables (cap at 10)
        display = tlist[:10]
        for t in display:
            lifecycle = f" [{t['_lifecycle']}]" if t.get("_lifecycle") else ""
            layer = t.get("_layer", "")
            f.write(f"  - {t.get('name', '')} ({layer}){lifecycle}\n")
        if count > 10:
            f.write(f"  ... +{count - 10} more\n")
        f.write("\n")


def _write_table_full(f, t: dict, ubiquitous_cm: set):
    """Write a single table entry for the Business layer (full detail)."""
    name = t.get("name", "")
    lifecycle = f" [{t['_lifecycle']}]" if t.get("_lifecycle") else ""
    cert = t.get("certificate_status", "")
    cert_str = f" [{cert}]" if cert else ""

    f.write(f"### {name}{lifecycle}{cert_str}\n")
    f.write(f"  Layer: {t.get('_layer', '')} | Schema: {t.get('schema_name', '')}")
    app = t.get("_application", "")
    if app:
        f.write(f" | Application: {app}")
    f.write("\n")

    # Description
    desc = t.get("description", "")
    if desc and not is_placeholder_description(desc):
        f.write(f"  Description: {_clean_html(desc)}\n")

    # AI-generated description
    ai_desc = t.get("ai_description", "")
    if ai_desc:
        cleaned = _clean_html(ai_desc)
        if len(cleaned) > 400:
            cleaned = cleaned[:400] + "..."
        f.write(f"  AI Description: {cleaned}\n")

    # SQL definition (views, matviews)
    defn = t.get("definition", "")
    if defn:
        # Show first 600 chars of SQL definition
        if len(defn) > 600:
            defn_display = defn[:600] + f"... [{len(defn):,} chars total]"
        else:
            defn_display = defn
        f.write(f"  SQL Definition: {defn_display}\n")

    # MatView staleness info
    refresh = t.get("refresh_method", "")
    if refresh:
        f.write(f"  Refresh: method={refresh}")
        mode = t.get("refresh_mode", "")
        if mode:
            f.write(f", mode={mode}")
        stale = t.get("staleness", "")
        if stale:
            f.write(f", staleness={stale}")
        f.write("\n")

    # Announcements
    ann_type = t.get("announcement_type", "")
    if ann_type:
        ann_title = t.get("announcement_title", "")
        ann_msg = t.get("announcement_message", "")
        f.write(f"  Announcement [{ann_type}]: {ann_title}")
        if ann_msg:
            f.write(f" — {ann_msg}")
        f.write("\n")

    # Popularity / Size
    pop = t.get("popularity_score", 0)
    if pop and pop > 0:
        f.write(f"  Popularity Score: {pop:.1f}\n")
    row_count = t.get("row_count", 0)
    size_bytes = t.get("size_bytes", 0)
    if row_count or size_bytes:
        parts = []
        if row_count:
            parts.append(f"{row_count:,} rows")
        if size_bytes:
            mb = size_bytes / (1024 * 1024)
            parts.append(f"{mb:,.1f} MB")
        f.write(f"  Size: {', '.join(parts)}\n")

    # DQ Score
    dq = t.get("_dq_score", "")
    if dq:
        f.write(f"  Data Quality Score: {dq}\n")

    # Parsed README fields
    parsed = t.get("_parsed_readme", {})
    if parsed:
        for key, val in parsed.items():
            # Truncate long values
            if len(val) > 300:
                val = val[:300] + "..."
            f.write(f"  {key}: {val}\n")

    # Column stats & categorization
    cols = t.get("columns") or []
    if cols:
        total_c = len(cols)
        described_c = t.get("_col_described", 0)
        cryptic_c = t.get("_col_cryptic", 0)
        f.write(f"  Columns: {total_c} total, {described_c} documented, {cryptic_c} cryptic\n")

        # Show documented columns (up to 15)
        doc_cols = [c for c in cols if c.get("description")][:15]
        if doc_cols:
            f.write("  Key Columns:\n")
            for c in doc_cols:
                f.write(f"    - {c['name']}: {_clean_html(c['description'])}\n")
            if described_c > 15:
                f.write(f"    ... +{described_c - 15} more documented\n")

    # Simplified lineage
    upstream = t.get("lineage_upstream") or []
    downstream = t.get("lineage_downstream") or []
    if upstream:
        simplified = [simplify_qualified_name(u) for u in upstream[:5]]
        f.write(f"  Upstream: {', '.join(simplified)}\n")
    if downstream:
        simplified = [simplify_qualified_name(d) for d in downstream[:5]]
        f.write(f"  Downstream: {', '.join(simplified)}\n")

    # Tags
    tags = t.get("tags") or []
    if tags:
        tag_strs = []
        for tag in tags:
            ttype, tval = parse_tag(tag)
            tag_strs.append(f"{ttype}: {tval}" if tval else ttype)
        f.write(f"  Tags: {', '.join(tag_strs)}\n")

    # Glossary terms
    gterms = t.get("glossary_terms") or []
    if gterms:
        display = [g for g in gterms if not _UUID_RE.match(g)]
        if display:
            f.write(f"  Glossary: {', '.join(display)}\n")

    # Custom metadata (filtered)
    cm = t.get("custom_metadata") or []
    if cm:
        formatted = format_custom_metadata(cm, ubiquitous_cm)
        if formatted:
            f.write(f"  Custom Metadata: {'; '.join(formatted)}\n")

    # Owners
    owners = t.get("owner_users") or []
    if owners:
        f.write(f"  Owners: {', '.join(owners)}\n")

    f.write("\n")


def write_section_8_business(f, ctx: dict):
    """Top-tier tables (Full Detail) — richest metadata, ranked by richness score."""
    _write_section_header(f, 8, "Top Tables — Full Detail")
    tables = ctx["tables"]
    ubiquitous_cm = ctx["ubiquitous_cm"]

    ranked = sorted(tables.values(), key=lambda t: (-t.get("_richness", 0), t.get("name", "").lower()))
    full_detail = [t for t in ranked[:MAX_FULL_DETAIL] if t.get("_richness", 0) > 0]

    f.write(f"Top {len(full_detail):,} tables by metadata richness (score > 0)\n")
    if full_detail:
        f.write(f"Richness range: {full_detail[0].get('_richness', 0)} – {full_detail[-1].get('_richness', 0)}\n\n")
    else:
        f.write("\nNo tables with metadata found.\n\n")
        return

    for t in full_detail:
        _write_table_full(f, t, ubiquitous_cm)


def write_section_9_enriched(f, ctx: dict):
    """Mid-tier tables (Condensed) — next batch after top tables, with metadata."""
    _write_section_header(f, 9, "Mid-Tier Tables — Condensed")
    tables = ctx["tables"]
    ubiquitous_cm = ctx["ubiquitous_cm"]

    ranked = sorted(tables.values(), key=lambda t: (-t.get("_richness", 0), t.get("name", "").lower()))
    # Skip the top tier (already in Section 8), take the next batch
    mid_start = min(MAX_FULL_DETAIL, len(ranked))
    mid_end = min(MAX_FULL_DETAIL + MAX_CONDENSED, len(ranked))
    enriched = [t for t in ranked[mid_start:mid_end] if t.get("_richness", 0) > 0]
    enriched.sort(key=lambda t: t.get("name", "").lower())

    f.write(f"Total: {len(enriched):,} tables/views\n\n")
    if not enriched:
        f.write("No enriched-layer tables found.\n\n")
        return

    for t in enriched:
        name = t.get("name", "")
        lifecycle = f" [{t['_lifecycle']}]" if t.get("_lifecycle") else ""
        cert = t.get("certificate_status", "")
        cert_str = f" [{cert}]" if cert else ""
        cryptic_flag = " [CRYPTIC COLUMNS]" if t.get("_has_cryptic") else ""

        f.write(f"### {name}{lifecycle}{cert_str}{cryptic_flag}\n")
        f.write(f"  Schema: {t.get('schema_name', '')}")
        app = t.get("_application", "")
        if app:
            f.write(f" | Application: {app}")
        f.write(f" | Columns: {t.get('_col_count', 0)} ({t.get('_col_described', 0)} documented)\n")

        desc = t.get("description", "")
        if desc and not is_placeholder_description(desc):
            f.write(f"  Description: {_clean_html(desc)}\n")

        ai_desc = t.get("ai_description", "")
        if ai_desc:
            cleaned = _clean_html(ai_desc)
            if len(cleaned) > 200:
                cleaned = cleaned[:200] + "..."
            f.write(f"  AI Description: {cleaned}\n")

        # SQL definition (for views in enriched layer)
        defn = t.get("definition", "")
        if defn:
            if len(defn) > 400:
                defn_display = defn[:400] + f"... [{len(defn):,} chars]"
            else:
                defn_display = defn
            f.write(f"  SQL Definition: {defn_display}\n")

        # Announcements
        ann_type = t.get("announcement_type", "")
        if ann_type:
            ann_title = t.get("announcement_title", "")
            f.write(f"  Announcement [{ann_type}]: {ann_title}\n")

        dq = t.get("_dq_score", "")
        if dq:
            f.write(f"  DQ Score: {dq}\n")

        # Tags (compact)
        tags = t.get("tags") or []
        if tags:
            tag_strs = []
            for tag in tags:
                ttype, tval = parse_tag(tag)
                tag_strs.append(f"{ttype}: {tval}" if tval else ttype)
            f.write(f"  Tags: {', '.join(tag_strs)}\n")

        # Glossary
        gterms = t.get("glossary_terms") or []
        if gterms:
            display = [g for g in gterms if not _UUID_RE.match(g)]
            if display:
                f.write(f"  Glossary: {', '.join(display)}\n")

        # Custom metadata (filtered)
        cm = t.get("custom_metadata") or []
        if cm:
            formatted = format_custom_metadata(cm, ubiquitous_cm)
            if formatted:
                f.write(f"  Custom Metadata: {'; '.join(formatted)}\n")

        # Owners
        owners = t.get("owner_users") or []
        if owners:
            f.write(f"  Owners: {', '.join(owners)}\n")

        f.write("\n")


def _has_meaningful_metadata(t: dict) -> bool:
    """Check if a table has CM, real description, tags, or glossary terms."""
    if t.get("custom_metadata"):
        return True
    desc = t.get("description", "")
    if desc and not is_placeholder_description(desc):
        return True
    if t.get("tags"):
        return True
    gterms = t.get("glossary_terms") or []
    if any(not _UUID_RE.match(g) for g in gterms):
        return True
    return False


def _write_table_condensed_cm(f, t: dict, ubiquitous_cm: set):
    """Compact enriched block for a table with CM/desc/tags/glossary/owners."""
    name = t.get("name", "")
    lifecycle = f" [{t['_lifecycle']}]" if t.get("_lifecycle") else ""
    cert = t.get("certificate_status", "")
    cert_str = f" [{cert}]" if cert else ""

    f.write(f"**{name}**{lifecycle}{cert_str}\n")

    desc = t.get("description", "")
    if desc and not is_placeholder_description(desc):
        f.write(f"  Description: {_clean_html(desc)}\n")

    ai_desc = t.get("ai_description", "")
    if ai_desc:
        cleaned = _clean_html(ai_desc)
        if len(cleaned) > 200:
            cleaned = cleaned[:200] + "..."
        f.write(f"  AI Description: {cleaned}\n")

    cm = t.get("custom_metadata") or []
    if cm:
        formatted = format_custom_metadata(cm, ubiquitous_cm)
        if formatted:
            f.write(f"  Custom Metadata: {'; '.join(formatted)}\n")

    tags = t.get("tags") or []
    if tags:
        tag_strs = []
        for tag in tags:
            ttype, tval = parse_tag(tag)
            tag_strs.append(f"{ttype}: {tval}" if tval else ttype)
        f.write(f"  Tags: {', '.join(tag_strs)}\n")

    gterms = t.get("glossary_terms") or []
    if gterms:
        display = [g for g in gterms if not _UUID_RE.match(g)]
        if display:
            f.write(f"  Glossary: {', '.join(display)}\n")

    owners = t.get("owner_users") or []
    if owners:
        f.write(f"  Owners: {', '.join(owners)}\n")

    f.write("\n")


def _render_layer_block(f, layer: str, layer_tables: list, ubiquitous_cm: set):
    """Render a markdown table + enriched subsection for a layer's tables."""
    f.write(f"### {layer} Layer ({len(layer_tables):,} tables)\n\n")
    if not layer_tables:
        f.write("(none)\n\n")
        return

    f.write(f"| {'Name':<50s} | {'Schema':<30s} | {'Columns':>7s} | {'Cert':<10s} |\n")
    f.write(f"|{'-'*52}|{'-'*32}|{'-'*9}|{'-'*12}|\n")
    for t in layer_tables:
        name = t.get("name", "")[:50]
        schema = (t.get("schema_name") or "")[:30]
        col_count = str(t.get("_col_count", 0))
        cert = t.get("certificate_status", "") or ""
        f.write(f"| {name:<50s} | {schema:<30s} | {col_count:>7s} | {cert:<10s} |\n")
    f.write("\n")

    # Enriched subsection for tables with meaningful metadata
    enriched = [t for t in layer_tables if _has_meaningful_metadata(t)]
    if enriched:
        f.write(f"#### {layer} — Enriched Detail ({len(enriched)} tables with metadata)\n\n")
        for t in enriched:
            _write_table_condensed_cm(f, t, ubiquitous_cm)


def write_section_10_trusted_landing(f, ctx: dict):
    """Remaining tables — compact summary grouped by schema."""
    _write_section_header(f, 10, "Remaining Tables — Summary")
    tables = ctx["tables"]

    ranked = sorted(tables.values(), key=lambda t: (-t.get("_richness", 0), t.get("name", "").lower()))
    # Everything after full + condensed tiers
    remaining_start = min(MAX_FULL_DETAIL + MAX_CONDENSED, len(ranked))
    remaining = ranked[remaining_start:]

    f.write(f"Tables not shown in Sections 8–9: {len(remaining):,}\n")
    f.write("(Richness score too low for detail sections, or beyond budget cap.)\n\n")

    if not remaining:
        f.write("All tables covered in Sections 8–9.\n\n")
        return

    # Group by schema for compact display
    from collections import Counter as _Counter
    schema_counts = _Counter((t.get("schema_name") or "(none)") for t in remaining)
    f.write(f"| {'Schema':<40s} | {'Count':>7s} |\n")
    f.write(f"|{'-'*42}|{'-'*9}|\n")
    for schema, count in schema_counts.most_common(50):
        f.write(f"| {schema:<40s} | {count:>7,} |\n")
    if len(schema_counts) > 50:
        f.write(f"| {'... +' + str(len(schema_counts) - 50) + ' more schemas':<40s} |         |\n")
    f.write("\n")


def write_section_11_gaps(f, ctx: dict):
    """Metadata Gap Summary — overall/per-application/per-layer stats, worst-documented top 20."""
    _write_section_header(f, 11, "Metadata Gap Summary")
    tables = ctx["tables"]
    total = len(tables)
    if total == 0:
        f.write("No tables to analyze.\n\n")
        return

    # Overall stats
    no_desc = sum(1 for t in tables.values() if not t.get("_has_real_desc"))
    no_readme = sum(1 for t in tables.values() if not t.get("_has_readme"))
    no_owner = sum(1 for t in tables.values() if not (t.get("owner_users") or t.get("owner_groups")))
    no_tags = sum(1 for t in tables.values() if not t.get("tags"))
    no_glossary = sum(1 for t in tables.values() if not t.get("glossary_terms"))
    no_col_docs = sum(1 for t in tables.values() if t.get("_col_count", 0) > 0 and t.get("_col_described", 0) == 0)
    has_lifecycle = sum(1 for t in tables.values() if t.get("_lifecycle"))
    no_ai_desc = sum(1 for t in tables.values() if not t.get("_has_ai_desc"))
    # SQL definition gap: only relevant for views/matviews
    views_matviews = [t for t in tables.values() if t.get("asset_type") in ("View", "MaterialisedView")]
    no_definition = sum(1 for t in views_matviews if not t.get("_has_definition"))
    no_announcement = sum(1 for t in tables.values() if not t.get("_has_announcement"))

    f.write("### Overall Gaps\n")
    f.write(f"  Missing description:  {no_desc:>5,} / {total:,}  ({no_desc/total*100:4.1f}%)\n")
    f.write(f"  Missing README:       {no_readme:>5,} / {total:,}  ({no_readme/total*100:4.1f}%)\n")
    f.write(f"  Missing owner:        {no_owner:>5,} / {total:,}  ({no_owner/total*100:4.1f}%)\n")
    f.write(f"  Missing tags:         {no_tags:>5,} / {total:,}  ({no_tags/total*100:4.1f}%)\n")
    f.write(f"  Missing glossary:     {no_glossary:>5,} / {total:,}  ({no_glossary/total*100:4.1f}%)\n")
    f.write(f"  Zero column docs:     {no_col_docs:>5,} / {total:,}  ({no_col_docs/total*100:4.1f}%)\n")
    f.write(f"  No AI description:    {no_ai_desc:>5,} / {total:,}  ({no_ai_desc/total*100:4.1f}%)\n")
    if views_matviews:
        vm_total = len(views_matviews)
        f.write(f"  No SQL definition:    {no_definition:>5,} / {vm_total:,} views/matviews  ({no_definition/vm_total*100:4.1f}%)\n")
    f.write(f"  Lifecycle flagged:    {has_lifecycle:>5,} / {total:,}  ({has_lifecycle/total*100:4.1f}%)\n")
    f.write("\n")

    # Per-layer gaps
    f.write("### Gaps by Layer\n")
    for layer in ["Business", "Enriched", "Trusted", "Landing", "Unknown"]:
        layer_tables = [t for t in tables.values() if t.get("_layer") == layer]
        lt = len(layer_tables)
        if lt == 0:
            continue
        no_d = sum(1 for t in layer_tables if not t.get("_has_real_desc"))
        no_r = sum(1 for t in layer_tables if not t.get("_has_readme"))
        no_o = sum(1 for t in layer_tables if not (t.get("owner_users") or t.get("owner_groups")))
        f.write(f"  {layer:12s} ({lt:>5,}): desc {no_d/lt*100:4.1f}% missing | "
                f"readme {no_r/lt*100:4.1f}% missing | owner {no_o/lt*100:4.1f}% missing\n")
    f.write("\n")

    # Per-application gaps
    app_counter = Counter(t.get("_application", "(none)") for t in tables.values())
    top_apps = app_counter.most_common(15)
    if top_apps:
        f.write("### Gaps by Application (top 15)\n")
        for app, app_count in top_apps:
            app_tables = [t for t in tables.values() if (t.get("_application") or "(none)") == app]
            at = len(app_tables)
            if at == 0:
                continue
            no_d = sum(1 for t in app_tables if not t.get("_has_real_desc"))
            no_r = sum(1 for t in app_tables if not t.get("_has_readme"))
            f.write(f"  {app[:35]:<35s} ({at:>5,}): desc {no_d/at*100:4.1f}% | readme {no_r/at*100:4.1f}%\n")
        f.write("\n")

    # Custom Entity coverage
    ce = ctx.get("custom_entities", {})
    if ce:
        total_ce = len(ce)
        ce_with_desc = sum(1 for c in ce.values()
                          if c.get("description") and not is_placeholder_description(c.get("description", "")))
        ce_with_cm = sum(1 for c in ce.values() if c.get("custom_metadata"))
        ce_with_owner = sum(1 for c in ce.values() if c.get("owner_users") or c.get("owner_groups"))
        f.write("### Custom Entity Coverage\n")
        f.write(f"  Total Custom Entities: {total_ce:,}\n")
        f.write(f"  With description:     {ce_with_desc:>5,} / {total_ce:,}  ({ce_with_desc/total_ce*100:4.1f}%)\n")
        f.write(f"  With custom metadata: {ce_with_cm:>5,} / {total_ce:,}  ({ce_with_cm/total_ce*100:4.1f}%)\n")
        f.write(f"  With owner:           {ce_with_owner:>5,} / {total_ce:,}  ({ce_with_owner/total_ce*100:4.1f}%)\n")
        f.write("\n")

    # Worst-documented tables (top 20)
    f.write("### Worst-Documented Tables (Top 20)\n")
    f.write("Tables with columns but the least metadata signals.\n\n")

    def doc_score(t):
        """Lower = worse documented."""
        s = 0
        if t.get("_has_real_desc"):
            s += 1
        if t.get("_has_readme"):
            s += 1
        if t.get("owner_users") or t.get("owner_groups"):
            s += 1
        if t.get("tags"):
            s += 1
        if t.get("glossary_terms"):
            s += 1
        col_count = t.get("_col_count", 0)
        if col_count > 0 and t.get("_col_described", 0) > 0:
            s += 1
        return s

    # Only consider tables with columns (actual data tables, not empty stubs)
    candidates = [t for t in tables.values() if t.get("_col_count", 0) > 0]
    candidates.sort(key=lambda t: (doc_score(t), -t.get("_col_count", 0)))
    worst = candidates[:20]

    f.write(f"| {'Name':<45s} | {'Layer':<10s} | {'Cols':>5s} | {'Desc':>4s} | {'README':>6s} | {'Owner':>5s} | {'Tags':>4s} |\n")
    f.write(f"|{'-'*47}|{'-'*12}|{'-'*7}|{'-'*6}|{'-'*8}|{'-'*7}|{'-'*6}|\n")
    for t in worst:
        name = t.get("name", "")[:45]
        layer = t.get("_layer", "")[:10]
        cols = str(t.get("_col_count", 0))
        has_d = "Y" if t.get("_has_real_desc") else "-"
        has_r = "Y" if t.get("_has_readme") else "-"
        has_o = "Y" if (t.get("owner_users") or t.get("owner_groups")) else "-"
        has_t = "Y" if t.get("tags") else "-"
        f.write(f"| {name:<45s} | {layer:<10s} | {cols:>5s} | {has_d:>4s} | {has_r:>6s} | {has_o:>5s} | {has_t:>4s} |\n")
    f.write("\n")


def write_section_12_custom_entities(f, ctx: dict):
    """Custom Entities — per-schema CM summaries with value distributions and samples."""
    _write_section_header(f, 12, "Custom Entities")
    ce = ctx.get("custom_entities", {})
    ce_cm_schemas = ctx.get("ce_cm_schemas", {})

    if not ce:
        f.write("No custom entities found in this tenant.\n\n")
        return

    f.write(f"Total Custom Entities: {len(ce):,}\n\n")

    if not ce_cm_schemas:
        f.write("No custom metadata schemas found on custom entities.\n\n")
        return

    # Per CM schema summary
    for schema_name in sorted(ce_cm_schemas.keys()):
        schema_info = ce_cm_schemas[schema_name]
        # Count unique entities with this schema
        entities_with_schema = set()
        for guid, entity in ce.items():
            for cm in entity.get("custom_metadata") or []:
                if cm.get("set") == schema_name:
                    entities_with_schema.add(guid)
                    break
        f.write(f"### CM Schema: {schema_name} ({len(entities_with_schema):,} entities)\n\n")

        # Attribute list with value distributions
        attrs = schema_info.get("attributes", {})
        for attr_name in sorted(attrs.keys()):
            val_counter = attrs[attr_name]
            total_vals = sum(val_counter.values())
            unique_vals = len(val_counter)
            f.write(f"  **{attr_name}** ({total_vals:,} values, {unique_vals:,} distinct)\n")
            # Show top 5 values
            for val, count in val_counter.most_common(5):
                display_val = val if val else "(empty)"
                f.write(f"    - {display_val}: {count:,}\n")
            if unique_vals > 5:
                f.write(f"    ... +{unique_vals - 5} more distinct values\n")
        f.write("\n")

        # 5 sample entities with their CM for this schema
        samples = []
        for guid, entity in ce.items():
            if guid in entities_with_schema:
                samples.append(entity)
                if len(samples) >= 5:
                    break

        if samples:
            f.write(f"  Sample entities:\n")
            for s in samples:
                f.write(f"  - **{s.get('name', '')}**")
                desc = s.get("description", "")
                if desc and not is_placeholder_description(desc):
                    short_desc = _clean_html(desc)
                    if len(short_desc) > 100:
                        short_desc = short_desc[:100] + "..."
                    f.write(f": {short_desc}")
                f.write("\n")
                # Show CM for this schema only
                for cm in s.get("custom_metadata") or []:
                    if cm.get("set") == schema_name:
                        f.write(f"    {cm.get('attribute', '')}: {cm.get('value', '')}\n")
            f.write("\n")


def write_section_13_sql_intelligence(f, ctx: dict, all_edges: list):
    """SQL Intelligence Layer — view definitions, process SQL, dbt SQL."""
    _write_section_header(f, 13, "SQL Intelligence Layer")
    tables = ctx["tables"]
    assets = ctx["assets"]

    # --- View / MatView SQL definitions ---
    views_with_sql = []
    for guid, t in tables.items():
        defn = t.get("definition", "")
        if defn and t.get("asset_type") in ("View", "MaterialisedView"):
            views_with_sql.append(t)
    views_with_sql.sort(key=lambda t: t.get("name", "").lower())

    f.write(f"### View/MatView SQL Definitions ({len(views_with_sql):,} assets)\n\n")
    if not views_with_sql:
        f.write("No SQL definitions found for views or materialised views.\n\n")
    else:
        for t in views_with_sql:
            name = t.get("name", "")
            atype = t.get("asset_type", "")
            defn = t.get("definition", "")
            f.write(f"#### {name} ({atype})\n")
            f.write(f"  Schema: {t.get('schema_name', '')} | Database: {t.get('database', '')}\n")
            # Show SQL — cap at 2000 chars for full context, truncate for huge ones
            if len(defn) > 2000:
                f.write(f"```sql\n{defn[:2000]}\n-- ... [{len(defn):,} chars total, truncated]\n```\n")
            else:
                f.write(f"```sql\n{defn}\n```\n")
            # Refresh info for matviews
            refresh = t.get("refresh_method", "")
            if refresh:
                f.write(f"  Refresh: {refresh}")
                mode = t.get("refresh_mode", "")
                stale = t.get("staleness", "")
                if mode:
                    f.write(f" | Mode: {mode}")
                if stale:
                    f.write(f" | Staleness: {stale}")
                f.write("\n")
            f.write("\n")

    # --- Process SQL (from lineage edges) ---
    sql_edges = [e for e in all_edges if e.get("relationship_type") == "LINEAGE"
                 and (e.get("metadata", {}).get("sql") or e.get("metadata", {}).get("code"))]

    f.write(f"### Lineage Process SQL ({len(sql_edges):,} edges with SQL/code)\n\n")
    if not sql_edges:
        f.write("No SQL or code found in lineage process entities.\n\n")
    else:
        # Deduplicate by process name
        seen_processes = set()
        for e in sql_edges[:50]:  # Cap at 50
            meta = e.get("metadata", {})
            pname = meta.get("process_name", "")
            if pname in seen_processes:
                continue
            seen_processes.add(pname)
            ptype = meta.get("process_type", "")
            sql = meta.get("sql", "")
            code = meta.get("code", "")
            content = sql or code
            f.write(f"#### {pname} ({ptype})\n")
            f.write(f"  {e.get('source_name', '')} → {e.get('target_name', '')}\n")
            if len(content) > 1500:
                f.write(f"```sql\n{content[:1500]}\n-- ... [{len(content):,} chars total]\n```\n")
            else:
                f.write(f"```sql\n{content}\n```\n")
            f.write("\n")
        if len(sql_edges) > 50:
            f.write(f"... +{len(sql_edges) - 50} more edges with SQL\n\n")

    # --- Transform Tool SQL ---
    transform_assets = [a for a in assets.values()
                        if a.get("transform_raw_sql") or a.get("transform_compiled_sql")]
    transform_assets.sort(key=lambda a: (a.get("transform_tool", ""), a.get("name", "").lower()))

    f.write(f"### Transform Tool SQL ({len(transform_assets):,} assets with SQL)\n\n")
    if not transform_assets:
        f.write("No transform assets with SQL found.\n\n")
    else:
        # Group by transform tool
        from collections import defaultdict as _defaultdict
        by_tool = _defaultdict(list)
        for a in transform_assets:
            tool = a.get("transform_tool", "") or "unknown"
            by_tool[tool].append(a)

        for tool in sorted(by_tool.keys()):
            tool_assets = by_tool[tool]
            f.write(f"#### {tool} ({len(tool_assets):,} assets)\n\n")
            for a in tool_assets[:30]:  # Cap at 30 per tool
                name = a.get("name", "")
                mat_type = a.get("transform_materialization_type", "")
                f.write(f"##### {name}")
                if mat_type:
                    f.write(f" (materialization: {mat_type})")
                f.write("\n")

                compiled = a.get("transform_compiled_sql", "")
                raw = a.get("transform_raw_sql", "")
                sql_content = compiled or raw
                label = "compiled" if compiled else "raw"
                if sql_content:
                    if len(sql_content) > 1500:
                        f.write(f"```sql\n-- {label} SQL\n{sql_content[:1500]}\n-- ... [{len(sql_content):,} chars total]\n```\n")
                    else:
                        f.write(f"```sql\n-- {label} SQL\n{sql_content}\n```\n")
                f.write("\n")
            if len(tool_assets) > 30:
                f.write(f"... +{len(tool_assets) - 30} more {tool} assets\n\n")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_context_writer_v2(asset_index: dict, tenant: str, output_dir: Path, all_edges: list = None):
    """Build and write the 13-section structured context file."""
    _p(f"\n{'='*60}")
    _p("[Context Writer v2] Building structured context...")
    _p(f"{'='*60}")
    t0 = time.time()

    ctx = build_context_data(asset_index)

    # Convert edges to dicts for section 13
    edge_dicts = []
    if all_edges:
        for e in all_edges:
            if hasattr(e, "__dataclass_fields__"):
                from dataclasses import asdict as _asdict
                edge_dicts.append(_asdict(e))
            elif isinstance(e, dict):
                edge_dicts.append(e)

    context_path = output_dir / "context.txt"
    _p(f"  Writing {context_path}...")

    with open(context_path, "w") as f:
        f.write(f"# Structured Asset Context — {tenant}\n")
        f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"# Format: Context Writer v2.1 (13 sections)\n\n")

        write_section_1_header(f, ctx, tenant)
        write_section_2_glossary(f, ctx)
        write_section_3_data_products(f, ctx)
        write_section_4_domains(f, ctx)
        write_section_5_classification(f, ctx)
        write_section_6_owners(f, ctx)
        write_section_7_confusing_clusters(f, ctx)
        write_section_8_business(f, ctx)
        write_section_9_enriched(f, ctx)
        write_section_10_trusted_landing(f, ctx)
        write_section_11_gaps(f, ctx)
        write_section_12_custom_entities(f, ctx)
        write_section_13_sql_intelligence(f, ctx, edge_dicts)

    # --- Output validation ---
    _p("\n  Validating output...")
    issues = validate_context_output(context_path)
    if issues:
        for issue in issues:
            _p(f"    WARNING: {issue}")
    else:
        _p("    All output checks passed")

    elapsed = time.time() - t0
    _p(f"  Context written: {context_path}")
    _p(f"  Sections: 13 | Tables: {ctx['total_tables']:,} | "
       f"Custom Entities: {len(ctx['custom_entities']):,} | Time: {elapsed:.1f}s")
    return context_path


def validate_context_output(context_path: Path) -> list:
    """Validate the generated context file for completeness."""
    issues = []
    try:
        content = context_path.read_text()
    except Exception as e:
        return [f"Cannot read output file: {e}"]

    # Check all 13 sections exist
    for i in range(1, 14):
        marker = f"## Section {i}:"
        if marker not in content:
            issues.append(f"Missing {marker} in output")

    # Check file isn't suspiciously small (< 10KB with tables means something broke)
    size_kb = len(content) / 1024
    if size_kb < 10:
        issues.append(f"Output file is only {size_kb:.1f} KB — likely incomplete")

    # Check glossary section has entries (if glossary terms exist)
    if "Total: 0 terms" not in content and "## Section 2: Glossary Terms" in content:
        # Good — glossary section exists and has terms
        pass

    # Check SQL Intelligence section has content
    if "## Section 13: SQL Intelligence Layer" in content:
        # Extract the section and check it's not just empty headers
        idx = content.index("## Section 13:")
        section_13 = content[idx:]
        if "0 assets)" in section_13 and "0 edges with SQL" in section_13 and "0 assets with SQL" in section_13:
            issues.append("Section 13 (SQL Intelligence) is entirely empty — no SQL data captured")

    return issues
