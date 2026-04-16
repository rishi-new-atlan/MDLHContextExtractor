"""
discover_schemas.py

General-purpose schema inspector for MDLH Iceberg tables.

Connect to MDLH and dump the full schema (all field names + types), sample rows,
and population statistics for any entity category — SQL, glossary, governance, BI, etc.

Usage:
  python discover_schemas.py                          # all SQL target tables
  python discover_schemas.py process view             # specific tables only
  python discover_schemas.py --gold                   # also probe GOLD namespace tables
  python discover_schemas.py --category glossary      # inspect glossary tables
  python discover_schemas.py --category bi            # inspect BI tables
  python discover_schemas.py --list-categories        # show all available categories
  python discover_schemas.py --sample 5               # 5 sample rows per table
  python discover_schemas.py --stats-sample 200       # population stats over 200 rows
"""

import argparse
import json
import math
import time
from pathlib import Path

from config import get_mdlh_catalog, ATLAN_TENANT
from metadata_extractor import (
    ENTITY_CATEGORIES,
    BASE_FIELDS,
    _load_table_ci,
    _probe_fields,
    _to_pascal,
)

# ─── Raw namespace (entity_metadata) tables — default SQL targets ───
TARGET_TABLES = [
    "process",
    "columnprocess",
    "biprocess",
    "view",
    "materialisedview",
    "dbtmodel",
    "dbtsource",
    "dbtmetric",
    "table",  # baseline comparison
]

# GOLD namespace tables with SQL-relevant columns
GOLD_TABLES = [
    "relational_asset_details",
    "pipeline_details",
    "assets",
]

# Fields we expect to find based on Atlan developer docs
EXPECTED_SQL_FIELDS = {
    "process": ["sql", "code", "ast", "inputs", "outputs"],
    "columnprocess": ["sql", "code", "inputs", "outputs"],
    "biprocess": ["sql", "code", "inputs", "outputs"],
    "view": ["definition"],
    "materialisedview": ["definition", "refreshmethod", "refreshmode", "staleness"],
    "dbtmodel": ["dbtcompiledsql", "dbtrawsql", "dbtmaterializationtype", "dbtstatus"],
    "dbtmetric": ["dbtmetricfilters"],
    "relational_asset_details": ["view_definition", "materialised_view_definition",
                                  "procedure_definition", "query_raw_query_text"],
}

# ─── Keyword groups for auto-flagging fields ───

SQL_KEYWORDS = [
    "sql", "query", "definition", "raw", "compiled", "code",
    "logic", "formula", "expression", "transform", "script",
    "ast", "procedure",
]

DESCRIPTION_KEYWORDS = [
    "description", "longdescription", "shortdescription",
    "readme", "userdescription", "assetaigenerateddescription",
    "summary", "detail", "note", "comment", "documentation",
]

GOVERNANCE_KEYWORDS = [
    "owner", "certificate", "announcement", "tag", "glossary",
    "meaning", "classification", "policy", "persona", "purpose",
    "compliance", "steward", "domain", "governance",
]

ALL_KEYWORD_GROUPS = {
    "SQL": SQL_KEYWORDS,
    "DESC": DESCRIPTION_KEYWORDS,
    "GOV": GOVERNANCE_KEYWORDS,
}


# ─── Helpers ───

def _is_populated(val):
    """Check if a value is meaningfully non-empty."""
    if val is None:
        return False
    if isinstance(val, float) and math.isnan(val):
        return False
    s = str(val).strip()
    return s not in ("", "None", "nan", "null", "[]", "{}")


def resolve_category_tables(category_name):
    """Look up tables + expected fields from ENTITY_CATEGORIES.

    Returns (table_names, expected_fields_per_table) or raises ValueError.
    """
    # Try exact match first, then substring match
    cat_def = ENTITY_CATEGORIES.get(category_name)
    if not cat_def:
        # Try partial match
        matches = [k for k in ENTITY_CATEGORIES if category_name in k]
        if len(matches) == 1:
            category_name = matches[0]
            cat_def = ENTITY_CATEGORIES[category_name]
        elif len(matches) > 1:
            raise ValueError(
                f"Ambiguous category '{category_name}' — matches: {matches}"
            )
        else:
            raise ValueError(
                f"Unknown category '{category_name}'. "
                f"Use --list-categories to see available categories."
            )

    table_names = list(cat_def.get("exact", []))
    expected_fields = {}
    all_extra = BASE_FIELDS + cat_def.get("extra_fields", ()) + cat_def.get("probe_extra", ())
    for t in table_names:
        expected_fields[t] = list(all_extra)

    return category_name, table_names, cat_def.get("prefixes", []), expected_fields


def discover_prefix_tables(catalog, ns, prefixes):
    """Find tables in the live catalog that match the given prefixes."""
    if not prefixes:
        return []
    all_tables = catalog.list_tables(ns)
    table_names = sorted(set(t[-1].lower() for t in all_tables))
    matched = []
    for name in table_names:
        for prefix in prefixes:
            if name.startswith(prefix):
                matched.append(name)
                break
    return matched


def compute_population_stats(catalog, ns, table_name, sample_limit=100):
    """Sample N rows and report per-field population rate.

    Returns dict of {field_name: {"populated": N, "total": N, "rate": float}}.
    """
    try:
        tbl = _load_table_ci(catalog, ns, table_name)
    except Exception as e:
        print(f"  Population stats SKIP — {e}")
        return {}

    try:
        try:
            scan = tbl.scan(row_filter="status == 'ACTIVE'", limit=sample_limit)
            df = scan.to_pandas()
        except Exception:
            scan = tbl.scan(limit=sample_limit)
            df = scan.to_pandas()
    except Exception as e:
        print(f"  Population stats scan failed — {e}")
        return {}

    df.columns = [c.lower() for c in df.columns]
    total = len(df)
    if total == 0:
        return {}

    stats = {}
    for col in df.columns:
        populated = sum(1 for val in df[col] if _is_populated(val))
        stats[col] = {
            "populated": populated,
            "total": total,
            "rate": round(populated / total, 3),
        }
    return stats


def find_namespace(namespaces, prefer_gold=False):
    """Find the right namespace."""
    for ns in namespaces:
        ns_str = ".".join(ns)
        if prefer_gold and "gold" in ns_str.lower():
            return ns_str
        if not prefer_gold and ns_str in ("atlan-ns", "entity_metadata"):
            return ns_str
    if not prefer_gold:
        for ns in namespaces:
            ns_str = ".".join(ns)
            if "history" not in ns_str and "gold" not in ns_str:
                return ns_str
    return None


def _match_keyword_groups(field_name):
    """Return list of keyword group tags that match the field name."""
    lower = field_name.lower()
    matched = []
    for group_name, keywords in ALL_KEYWORD_GROUPS.items():
        if any(kw in lower for kw in keywords):
            matched.append(group_name)
    return matched


def inspect_table(catalog, ns, table_name, sample_limit=3,
                  expected_fields=None, stats_sample=100):
    """Dump schema + sample rows + population stats for a single table."""
    print(f"\n{'='*70}")
    print(f"TABLE: {ns}.{table_name}")
    print(f"{'='*70}")

    try:
        tbl = _load_table_ci(catalog, ns, table_name)
    except Exception as e:
        print(f"  SKIP — {e}")
        return None

    # Schema
    schema = tbl.schema()
    print(f"\nSchema ({len(schema.fields)} fields):")
    print(f"{'  #':<5} {'Name':<45} {'Type':<30} {'Required':<10} {'Tags'}")
    print(f"  {'-'*100}")

    field_names = []
    for field in schema.fields:
        field_names.append(field.name)
        required = "Y" if field.required else ""
        type_str = str(field.field_type)
        if len(type_str) > 28:
            type_str = type_str[:28] + ".."
        # Multi-keyword-group highlighting
        tags = _match_keyword_groups(field.name)
        tag_str = " ".join(f"[{t}]" for t in tags) if tags else ""
        marker = " **" if tags else ""
        print(f"  {field.field_id:<4} {field.name:<45} {type_str:<30} {required:<10}{tag_str}{marker}")

    # Auto-flagged fields by group
    for group_name, keywords in ALL_KEYWORD_GROUPS.items():
        flagged = [f for f in field_names if any(kw in f.lower() for kw in keywords)]
        if flagged:
            print(f"\n  ** [{group_name}] fields: {flagged}")

    # Check expected fields
    if expected_fields is None:
        expected = EXPECTED_SQL_FIELDS.get(table_name, [])
    else:
        expected = expected_fields
    found_expected = []
    missing_expected = []
    if expected:
        field_names_lower = {f.lower() for f in field_names}
        found_expected = [f for f in expected if f.lower() in field_names_lower]
        missing_expected = [f for f in expected if f.lower() not in field_names_lower]
        print(f"\n  Expected fields:")
        if found_expected:
            print(f"    FOUND: {found_expected}")
        if missing_expected:
            print(f"    MISSING: {missing_expected}")

    # Sample rows — focus on keyword-relevant fields
    print(f"\nSample rows (up to {sample_limit}):")
    samples_data = []
    try:
        try:
            scan = tbl.scan(row_filter="status == 'ACTIVE'", limit=sample_limit)
            df = scan.to_pandas()
        except Exception:
            scan = tbl.scan(limit=sample_limit)
            df = scan.to_pandas()

        df.columns = [c.lower() for c in df.columns]

        if len(df) == 0:
            print("  (empty table)")
        else:
            for idx, row in df.iterrows():
                print(f"\n  --- Row {idx} ---")
                # Separate keyword-relevant fields from others
                relevant_cols = [c for c in df.columns if _match_keyword_groups(c)]
                other_cols = [c for c in df.columns if c not in relevant_cols]

                if relevant_cols:
                    print(f"  [Keyword-relevant fields]")
                    for col in relevant_cols:
                        val = row[col]
                        val_str = str(val) if val is not None else "(null)"
                        if len(val_str) > 800:
                            val_str = val_str[:800] + f"\n    ... [{len(val_str)} chars total]"
                        populated = "POPULATED" if _is_populated(val) else "EMPTY"
                        tags = _match_keyword_groups(col)
                        tag_str = " ".join(f"[{t}]" for t in tags)
                        print(f"  >> {col} {tag_str} [{populated}]: {val_str}")

                print(f"  [Other fields]")
                for col in other_cols:
                    val = row[col]
                    val_str = str(val) if val is not None else "(null)"
                    if len(val_str) > 150:
                        val_str = val_str[:150] + f"... [{len(val_str)} chars]"
                    print(f"  {col}: {val_str}")

                samples_data.append({col: str(row[col])[:200] for col in df.columns})

    except Exception as e:
        print(f"  Sample failed: {e}")

    # Population stats
    pop_stats = {}
    if stats_sample > 0:
        print(f"\nField population stats (over up to {stats_sample} rows):")
        pop_stats = compute_population_stats(catalog, ns, table_name, stats_sample)
        if pop_stats:
            # Sort by rate ascending to highlight empty fields
            sorted_fields = sorted(pop_stats.items(), key=lambda x: x[1]["rate"])
            for fname, s in sorted_fields:
                bar = "#" * int(s["rate"] * 20)
                tags = _match_keyword_groups(fname)
                tag_str = " ".join(f"[{t}]" for t in tags) if tags else ""
                marker = " **" if tags else ""
                print(f"  {fname:<45} {s['populated']:>4}/{s['total']:<4} "
                      f"({s['rate']:>5.1%}) |{bar:<20}| {tag_str}{marker}")
        else:
            print("  (no data)")

    # Collect keyword-relevant fields per group for JSON output
    desc_fields = [f for f in field_names
                   if any(kw in f.lower() for kw in DESCRIPTION_KEYWORDS)]
    gov_fields = [f for f in field_names
                  if any(kw in f.lower() for kw in GOVERNANCE_KEYWORDS)]
    sql_fields = [f for f in field_names
                  if any(kw in f.lower() for kw in SQL_KEYWORDS)]

    return {
        "table": table_name,
        "namespace": ns,
        "fields": field_names,
        "sql_relevant_fields": sql_fields,
        "description_relevant_fields": desc_fields,
        "governance_relevant_fields": gov_fields,
        "expected_found": found_expected,
        "expected_missing": missing_expected,
        "row_count_sample": len(samples_data),
        "field_population": pop_stats,
        "population_rows_sampled": pop_stats[next(iter(pop_stats))]["total"] if pop_stats else 0,
    }


def list_categories():
    """Print all available categories with their tables and fields."""
    print(f"\n{'='*70}")
    print(f"AVAILABLE CATEGORIES (from ENTITY_CATEGORIES registry)")
    print(f"{'='*70}")
    for cat_name, cat_def in ENTITY_CATEGORIES.items():
        exact = cat_def.get("exact", [])
        prefixes = cat_def.get("prefixes", [])
        extra = cat_def.get("extra_fields", ())
        probe = cat_def.get("probe_extra", ())
        label_map = cat_def.get("label_map", {})

        print(f"\n  {cat_name}:")
        if exact:
            print(f"    Tables (exact): {exact}")
        if prefixes:
            print(f"    Prefixes: {prefixes}")
        if label_map:
            print(f"    Label map: {label_map}")
        all_fields = list(BASE_FIELDS) + list(extra) + list(probe)
        print(f"    Fields ({len(all_fields)}): {all_fields}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="General-purpose MDLH schema inspector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "tables", nargs="*",
        help="Specific table names to inspect (default: SQL target tables)",
    )
    parser.add_argument(
        "--gold", action="store_true",
        help="Also probe GOLD namespace tables",
    )
    parser.add_argument(
        "--category", type=str, default=None,
        help="Inspect tables for a category (e.g. glossary, bi, transform)",
    )
    parser.add_argument(
        "--list-categories", action="store_true",
        help="List all available entity categories and exit",
    )
    parser.add_argument(
        "--sample", type=int, default=3,
        help="Number of sample rows per table (default: 3)",
    )
    parser.add_argument(
        "--stats-sample", type=int, default=100,
        help="Number of rows for population stats (default: 100, 0 to skip)",
    )
    args = parser.parse_args()

    # --list-categories: print and exit (no connection needed)
    if args.list_categories:
        list_categories()
        return

    print(f"Connecting to {ATLAN_TENANT}...")
    catalog, namespaces, warehouse = get_mdlh_catalog()
    print(f"\nAvailable namespaces: {['.'.join(ns) for ns in namespaces]}")

    results = {}
    gold_results = {}

    if args.category:
        # ─── Category mode ───
        cat_name, exact_tables, prefixes, expected_map = resolve_category_tables(args.category)

        ns = find_namespace(namespaces, prefer_gold=False)
        if not ns:
            print("WARNING: Could not find entity_metadata namespace")
            return

        print(f"\n{'#'*70}")
        print(f"# CATEGORY: {cat_name}")
        print(f"# NAMESPACE: {ns}")
        print(f"{'#'*70}")

        # Discover prefix-matched tables from live catalog
        prefix_tables = discover_prefix_tables(catalog, ns, prefixes)
        all_tables = list(dict.fromkeys(exact_tables + prefix_tables))  # dedup, preserve order

        if not all_tables:
            print(f"  No tables found for category '{cat_name}'")
            return

        print(f"  Tables to inspect: {all_tables}")

        for table_name in all_tables:
            expected = expected_map.get(table_name,
                                        expected_map.get(exact_tables[0]) if exact_tables else [])
            result = inspect_table(
                catalog, ns, table_name,
                sample_limit=args.sample,
                expected_fields=expected,
                stats_sample=args.stats_sample,
            )
            if result:
                results[table_name] = result

    else:
        # ─── Default / positional mode (backward compat) ───
        targets = args.tables if args.tables else TARGET_TABLES

        ns = find_namespace(namespaces, prefer_gold=False)
        if ns:
            print(f"\n{'#'*70}")
            print(f"# ENTITY METADATA NAMESPACE: {ns}")
            print(f"{'#'*70}")

            for table_name in targets:
                result = inspect_table(
                    catalog, ns, table_name,
                    sample_limit=args.sample,
                    stats_sample=args.stats_sample,
                )
                if result:
                    results[table_name] = result
        else:
            print("WARNING: Could not find entity_metadata namespace")

        # ─── GOLD namespace ───
        if args.gold:
            gold_ns = find_namespace(namespaces, prefer_gold=True)
            if gold_ns:
                print(f"\n{'#'*70}")
                print(f"# GOLD NAMESPACE: {gold_ns}")
                print(f"{'#'*70}")

                for table_name in GOLD_TABLES:
                    result = inspect_table(
                        catalog, gold_ns, table_name,
                        sample_limit=min(args.sample, 2),
                        stats_sample=args.stats_sample,
                    )
                    if result:
                        gold_results[table_name] = result
            else:
                print("\nWARNING: No GOLD namespace found")

    # ─── Write results ───
    all_results = {
        "tenant": ATLAN_TENANT,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "entity_metadata": results,
        "gold": gold_results,
    }
    if args.category:
        all_results["category"] = args.category

    out_path = Path("discovery_results.json")
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    # ─── Summary ───
    print(f"\n{'='*70}")
    print(f"DISCOVERY SUMMARY")
    print(f"{'='*70}")
    print(f"Tenant: {ATLAN_TENANT}")
    if args.category:
        print(f"Category: {args.category}")
    print(f"Entity metadata tables inspected: {len(results)}")
    if gold_results:
        print(f"GOLD tables inspected: {len(gold_results)}")

    for table_name, r in {**results, **gold_results}.items():
        sql_fields = r.get("sql_relevant_fields", [])
        desc_fields = r.get("description_relevant_fields", [])
        gov_fields = r.get("governance_relevant_fields", [])
        expected_found = r.get("expected_found", [])
        expected_missing = r.get("expected_missing", [])
        status = "OK" if not expected_missing else f"MISSING: {expected_missing}"

        print(f"\n  {table_name}:")
        if sql_fields:
            print(f"    [SQL]  {sql_fields}")
        if desc_fields:
            print(f"    [DESC] {desc_fields}")
        if gov_fields:
            print(f"    [GOV]  {gov_fields}")
        if expected_found or expected_missing:
            print(f"    docs check: {status}")

        # Population highlights for keyword-relevant fields
        pop = r.get("field_population", {})
        if pop:
            interesting = set(sql_fields + desc_fields + gov_fields)
            empty_relevant = [f for f in interesting
                              if f.lower() in pop and pop[f.lower()]["rate"] == 0]
            low_relevant = [f for f in interesting
                            if f.lower() in pop and 0 < pop[f.lower()]["rate"] < 0.5]
            if empty_relevant:
                print(f"    EMPTY (0%): {sorted(empty_relevant)}")
            if low_relevant:
                rates = {f: f"{pop[f.lower()]['rate']:.0%}" for f in low_relevant}
                print(f"    LOW POPULATION: {rates}")

    print(f"\nFull results: {out_path}")


if __name__ == "__main__":
    main()
