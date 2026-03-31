"""Entry point: connect to MDLH, extract metadata, score and build context."""

from config import get_mdlh_catalog, ATLAN_TENANT, DATA_DIR, OUTPUT_DIR
from metadata_extractor import extract_metadata
from asset_scorer import run_scorer
from context_writer_v2 import run_context_writer_v2


def find_entity_namespace(namespaces):
    for ns in namespaces:
        ns_str = ".".join(ns)
        if ns_str in ("atlan-ns", "entity_metadata"):
            return ns_str
    for ns in namespaces:
        ns_str = ".".join(ns)
        if "history" not in ns_str and "gold" not in ns_str:
            return ns_str
    return ".".join(namespaces[0])


def main():
    print("=" * 60)
    print(f"Connecting to {ATLAN_TENANT}...")
    print("=" * 60)

    catalog, namespaces, warehouse = get_mdlh_catalog()
    ns = find_entity_namespace(namespaces)
    print(f"Using namespace: {ns}")

    asset_index, all_edges = extract_metadata(catalog, ns, ATLAN_TENANT)
    run_scorer(asset_index, all_edges, ATLAN_TENANT, DATA_DIR, OUTPUT_DIR)
    run_context_writer_v2(asset_index, ATLAN_TENANT, OUTPUT_DIR)


if __name__ == "__main__":
    main()
