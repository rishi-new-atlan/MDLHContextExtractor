"""
asset_scorer.py

Score and prioritise extracted assets by metadata richness.
Produces:
  - data/scored_assets.json   (cache — re-derive without re-hitting MDLH)
  - output/context.txt        (hand-off file for question-generation skill)
"""

import json
import re
import time
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from pathlib import Path


def _p(msg):
    print(msg, flush=True)

# ---------------------------------------------------------------------------
# Config — tune per tenant
# ---------------------------------------------------------------------------
WEIGHTS = {
    "readme":                    3,   # richest text signal
    "description":               2,
    "custom_metadata":           3,   # 1–2 CM attributes
    "custom_metadata_rich":      4,   # 3–9 CM attributes
    "custom_metadata_very_rich": 5,   # ≥10 CM attributes — richest signal
    "lineage":                   2,
    "tags":                      1,
    "glossary_terms":            1,
    "documented_columns":        1,   # table has ≥1 column with a description
    "documented_columns_cm":     1,   # table has ≥1 column with custom metadata
    "certificate_verified":      3,   # VERIFIED — strongest trust signal
    "certificate_draft":         1,   # DRAFT — some curation intent
    "is_view":                   1,   # View / MaterialisedView — curated/derived assets
    # v2 fields
    "ai_description":            1,   # AI-generated description present
    "sql_definition":            2,   # SQL definition (views, matviews) — transformation logic
    "transform_sql":             2,   # transform compiled/raw SQL present
    "announcement":              1,   # active announcement on asset
    "popularity":                1,   # non-zero popularity score
    "owner":                     1,   # has assigned owner (user or group)
}

ALWAYS_INCLUDE_TYPES = {"GlossaryTerm", "DataProduct", "DataDomain", "CustomEntity"}
EXCLUDE_BARE_COLUMNS  = True   # drop columns with no description AND no custom metadata
MIN_SCORE             = 2      # minimum richness score to include
TOP_N                 = 5000   # cap on assets written to context.txt
# ---------------------------------------------------------------------------


def _clean_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def score_asset(asset: dict) -> tuple:
    cm_count = len(asset.get("custom_metadata") or [])
    cert = (asset.get("certificate_status") or "").upper()
    atype = asset.get("asset_type", "")

    breakdown = {
        "readme":               WEIGHTS["readme"]               if asset.get("readme")          else 0,
        "description":          WEIGHTS["description"]          if asset.get("description")     else 0,
        "custom_metadata":      WEIGHTS["custom_metadata_very_rich"] if cm_count >= 10
                                else (WEIGHTS["custom_metadata_rich"] if cm_count >= 3
                                else (WEIGHTS["custom_metadata"] if cm_count > 0 else 0)),
        "lineage":              WEIGHTS["lineage"]              if (asset.get("lineage_upstream") or asset.get("lineage_downstream")) else 0,
        "tags":                 WEIGHTS["tags"]                 if asset.get("tags")            else 0,
        "glossary_terms":       WEIGHTS["glossary_terms"]       if asset.get("glossary_terms")  else 0,
        "documented_columns":   WEIGHTS["documented_columns"]   if any(c.get("description") for c in asset.get("columns", [])) else 0,
        "documented_columns_cm": WEIGHTS["documented_columns_cm"] if any(c.get("custom_metadata") for c in asset.get("columns", [])) else 0,
        "certificate_verified": WEIGHTS["certificate_verified"] if cert == "VERIFIED"           else 0,
        "certificate_draft":    WEIGHTS["certificate_draft"]    if cert == "DRAFT"              else 0,
        "is_view":              WEIGHTS["is_view"]              if atype in ("View", "MaterialisedView") else 0,
        # v2 fields
        "ai_description":       WEIGHTS["ai_description"]       if asset.get("ai_description")  else 0,
        "sql_definition":       WEIGHTS["sql_definition"]       if asset.get("definition")      else 0,
        "transform_sql":        WEIGHTS["transform_sql"]        if (asset.get("transform_raw_sql") or asset.get("transform_compiled_sql")) else 0,
        "announcement":         WEIGHTS["announcement"]         if asset.get("announcement_type") else 0,
        "popularity":           WEIGHTS["popularity"]           if (asset.get("popularity_score") or 0) > 0 else 0,
        "owner":                WEIGHTS["owner"]                if (asset.get("owner_users") or asset.get("owner_groups")) else 0,
    }
    return sum(breakdown.values()), breakdown


def _fmt_asset_block(asset: dict, score: int) -> str:
    lines = [f"=== {asset['asset_type']}: {asset['name']} ==="]

    meta = []
    if asset.get("connector"):          meta.append(f"Connector: {asset['connector']}")
    if asset.get("database"):           meta.append(f"DB: {asset['database']}")
    if asset.get("schema_name"):        meta.append(f"Schema: {asset['schema_name']}")
    if asset.get("certificate_status"): meta.append(f"Certificate: {asset['certificate_status']}")
    if meta:
        lines.append(" | ".join(meta))

    if asset.get("qualified_name"):
        lines.append(f"Qualified Name: {asset['qualified_name']}")

    if asset.get("description"):
        lines.append(f"Description: {asset['description']}")

    if asset.get("readme"):
        readme = _clean_html(asset["readme"])
        words = readme.split()
        if len(words) > 300:
            readme = " ".join(words[:300]) + "…"
        lines.append(f"README: {readme}")

    cols = asset.get("columns", [])
    if cols:
        parts = []
        for c in cols[:20]:
            parts.append(f"{c['name']} ({c['description']})" if c.get("description") else c["name"])
        if len(cols) > 20:
            parts.append(f"+{len(cols) - 20} more")
        lines.append(f"Columns: {', '.join(parts)}")

    if asset.get("lineage_upstream"):
        lines.append(f"Upstream: {', '.join(asset['lineage_upstream'][:5])}")
    if asset.get("lineage_downstream"):
        lines.append(f"Downstream: {', '.join(asset['lineage_downstream'][:5])}")

    if asset.get("tags"):
        lines.append(f"Tags: {', '.join(t.get('tagName', t.get('tagname', '')) for t in asset['tags'])}")

    if asset.get("glossary_terms"):
        lines.append(f"Glossary: {', '.join(asset['glossary_terms'])}")

    if asset.get("custom_metadata"):
        cm = ", ".join(f"{m['set']}.{m['attribute']}={m['value']}" for m in asset["custom_metadata"])
        lines.append(f"Custom Metadata: {cm}")

    if asset.get("owner_users"):
        lines.append(f"Owners: {', '.join(asset['owner_users'])}")

    lines.append(f"Score: {score}")
    lines.append("---")
    return "\n".join(lines)


def run_scorer(asset_index: dict, all_edges: list, tenant: str, data_dir: Path, output_dir: Path):
    _p(f"\n{'='*60}")
    _p("[Scorer] Scoring and prioritising assets...")
    _p(f"{'='*60}")
    t0 = time.time()

    scored = []
    skipped = 0
    for i, (guid, asset) in enumerate(asset_index.items()):
        a = asdict(asset) if hasattr(asset, "__dataclass_fields__") else asset
        atype = a.get("asset_type", "")

        if EXCLUDE_BARE_COLUMNS and atype == "Column" and not a.get("description") and not a.get("custom_metadata"):
            skipped += 1
            continue

        score, breakdown = score_asset(a)

        if atype in ALWAYS_INCLUDE_TYPES or score >= MIN_SCORE:
            scored.append({"guid": guid, "score": score, "score_breakdown": breakdown, **a})

        if (i + 1) % 1_000_000 == 0:
            _p(f"  ...scored {i+1:,} / {len(asset_index):,} assets ({len(scored):,} qualifying so far)")

    scored.sort(key=lambda x: -x["score"])
    top = scored[:TOP_N]

    _p(f"  Skipped {skipped:,} bare columns")
    _p(f"  Qualifying assets (score >= {MIN_SCORE} or always-include): {len(scored):,}")
    _p(f"  Writing top {len(top):,} assets to output files")

    # --- scored_assets.json (cache) ---
    _p("\n  Writing data/scored_assets.json (cache)...")
    cache_path = data_dir / "scored_assets.json"
    with open(cache_path, "w") as f:
        json.dump({
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "tenant": tenant,
            "total_qualifying": len(scored),
            "assets": top,
        }, f, indent=2, default=str)
    _p(f"  Cache saved: {cache_path}")

    # --- README truncation stats ---
    readme_truncated = 0
    readme_word_counts = []
    for a in top:
        raw_readme = a.get("readme", "")
        if raw_readme:
            cleaned = _clean_html(raw_readme)
            wc = len(cleaned.split())
            readme_word_counts.append(wc)
            if wc > 300:
                readme_truncated += 1

    # --- context.txt (hand-off file) ---
    _p("  Writing output/context.txt (hand-off file)...")
    context_path = output_dir / "context.txt"
    by_type = defaultdict(list)
    for a in top:
        by_type[a["asset_type"]].append(a)

    with open(context_path, "w") as f:
        f.write(f"# Asset Context — {tenant}\n")
        f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"# Assets: {len(top):,} | Total edges: {len(all_edges):,}\n\n")
        for atype in sorted(by_type.keys()):
            assets = by_type[atype]
            f.write(f"\n{'='*60}\n# {atype} ({len(assets)})\n{'='*60}\n\n")
            for a in assets:
                f.write(_fmt_asset_block(a, a["score"]))
                f.write("\n\n")

    _p(f"  Context saved: {context_path}")
    _p(f"\nScorer complete in {time.time()-t0:.1f}s")

    assets_with_readme = len(readme_word_counts)
    avg_words = int(sum(readme_word_counts) / assets_with_readme) if assets_with_readme else 0
    max_words = max(readme_word_counts) if readme_word_counts else 0
    _p(f"\nREADME coverage (top {len(top):,} assets):")
    _p(f"  Assets with a README : {assets_with_readme:,}")
    _p(f"  Avg word count       : {avg_words:,} words")
    _p(f"  Max word count       : {max_words:,} words")
    _p(f"  Truncated at 300 words: {readme_truncated:,}  "
       f"({readme_truncated/assets_with_readme*100:.1f}% of READMEs)" if assets_with_readme else "  Truncated at 300 words: 0")

    _p(f"\nOutput files:")
    _p(f"  {cache_path}")
    _p(f"  {context_path}")
    return top
