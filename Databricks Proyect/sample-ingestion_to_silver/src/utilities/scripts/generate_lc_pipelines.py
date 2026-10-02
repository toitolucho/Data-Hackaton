#!/usr/bin/env python3
"""
generate_lc_pipelines.py — Generate Lakeflow Connect pipeline YAML from JSON table lists.

Reads JSON table-list files from src/utilities/table_lists/<target>/ and produces
pipeline YAML files in resources/pipelines/. Preserves pipeline resource keys and
names so Databricks sees the same pipeline (no new pipeline created on deploy).

Usage:
    # Generate all pipeline YAMLs for dev target
    python src/utilities/scripts/generate_lc_pipelines.py --target dev

    # Generate only one pipeline group
    python src/utilities/scripts/generate_lc_pipelines.py --target dev --group default

    # Dry-run (print YAML to stdout, don't write files)
    python src/utilities/scripts/generate_lc_pipelines.py --target dev --dry-run

    # Export current YAML back to JSON (for initial migration)
    python src/utilities/scripts/generate_lc_pipelines.py --export --target dev

Notes:
    - This script is NOT part of the bundle deploy path — it only runs when
      you add/remove tables from the JSON files.
    - The generated YAML files ARE committed to git (they're the DABs input).
    - The JSON files are the source of truth for table lists.
"""

import argparse
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants — customise these per bundle
# ---------------------------------------------------------------------------

# Map from group name to the output YAML filename
GROUP_TO_YAML = {
    "default": "lakeflow_connect_to_staging.yml",
    # Add more groups as needed, e.g.:
    # "medium": "lakeflow_connect_to_staging_medium.yml",
    # "large": "lakeflow_connect_to_staging_large.yml",
}

# Size-category descriptions for the header comment
GROUP_DESCRIPTIONS = {
    "default": "default pipeline (all tables)",
    # "medium": "medium tables (3–10 GB)",
    # "large": "large tables (10–100 GB)",
}


def find_bundle_root() -> Path:
    """Walk up from this script to find the bundle root (where databricks.yml is)."""
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "databricks.yml").exists():
            return parent
    # Fallback: assume script is at src/utilities/scripts/ relative to root
    return current.parent.parent.parent.parent


def load_json(json_path: Path) -> dict:
    """Load and validate a table-list JSON file."""
    if not json_path.exists():
        print(f"ERROR: JSON file not found: {json_path}", file=sys.stderr)
        sys.exit(1)
    with open(json_path) as f:
        data = json.load(f)
    # Validate required fields
    required = ["source_catalog", "source_schema", "destination_schema",
                "pipeline_resource_key", "pipeline_name",
                "cluster", "tables"]
    missing = [k for k in required if k not in data]
    if missing:
        print(f"ERROR: {json_path.name} missing fields: {missing}", file=sys.stderr)
        sys.exit(1)
    return data


def generate_yaml(data: dict, group: str) -> str:
    """Generate pipeline YAML string from the JSON data structure."""
    lines = []
    desc = GROUP_DESCRIPTIONS.get(group, group)

    # Header comment
    lines.append("# =============================================================================")
    lines.append(f"# Lakeflow Connect -> Staging — {desc}")
    lines.append("# =============================================================================")
    lines.append(f"# Auto-generated from src/utilities/table_lists/<target>/{group}_tables.json")
    lines.append(f"# by generate_lc_pipelines.py. Edit the JSON, not this file.")
    lines.append("# =============================================================================")
    lines.append("")

    # Pipeline resource definition
    key = data["pipeline_resource_key"]
    name = data["pipeline_name"]
    cluster = data["cluster"]
    src_cat = data["source_catalog"]
    src_schema = data["source_schema"]
    dst_schema = data["destination_schema"]

    lines.append("resources:")
    lines.append("  pipelines:")
    lines.append(f"    {key}:")
    lines.append(f"      name: {name}")
    lines.append("      catalog: ${var.catalog}")
    lines.append(f"      schema: {dst_schema}")
    lines.append("      serverless: false")
    lines.append("      clusters:")
    lines.append("        - label: default")
    lines.append(f"          node_type_id: {cluster['node_type_id']}")
    lines.append("          autoscale:")
    lines.append(f"            min_workers: {cluster['min_workers']}")
    lines.append(f"            max_workers: {cluster['max_workers']}")
    lines.append("      ingestion_definition:")
    lines.append("        connection_name: ${var.connection_name}")
    lines.append("        objects:")

    # Table objects
    for table in data["tables"]:
        src_table = table["source_table"]
        dst_table = table["destination_table"]
        cursor_cols = table.get("cursor_columns", [src_table + "Id"])
        scd_type = table.get("scd_type", "APPEND_ONLY")

        lines.append("          - table:")
        lines.append(f"              source_catalog: {src_cat}")
        lines.append(f"              source_schema: {src_schema}")
        lines.append(f"              source_table: {src_table}")
        lines.append("              destination_catalog: ${var.catalog}")
        lines.append(f"              destination_schema: {dst_schema}")
        lines.append(f"              destination_table: {dst_table}")
        lines.append("              table_configuration:")
        lines.append(f"                scd_type: {scd_type}")
        lines.append("                query_based_connector_config:")
        lines.append("                  cursor_columns:")
        for col in cursor_cols:
            lines.append(f"                    - {col}")

    lines.append("")  # trailing newline
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate LC pipeline YAML from JSON table lists."
    )
    parser.add_argument(
        "--target", "-t", required=True,
        help="Target environment (dev, prd)"
    )
    parser.add_argument(
        "--group", "-g", choices=list(GROUP_TO_YAML.keys()),
        help="Generate only this pipeline group (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print YAML to stdout instead of writing files"
    )
    parser.add_argument(
        "--export", action="store_true",
        help="Reverse: export current pipeline YAML to JSON (initial migration)"
    )
    args = parser.parse_args()

    bundle_root = find_bundle_root()
    table_lists_dir = bundle_root / "src" / "utilities" / "table_lists" / args.target
    pipelines_dir = bundle_root / "resources" / "pipelines"

    if not table_lists_dir.exists():
        print(f"ERROR: Table lists directory not found: {table_lists_dir}", file=sys.stderr)
        print(f"  Create it with: mkdir -p {table_lists_dir}", file=sys.stderr)
        sys.exit(1)

    groups = [args.group] if args.group else list(GROUP_TO_YAML.keys())

    for group in groups:
        json_path = table_lists_dir / f"{group}_tables.json"

        if args.export:
            _export_yaml_to_json(pipelines_dir, table_lists_dir, group)
            continue

        data = load_json(json_path)
        yaml_content = generate_yaml(data, group)

        if args.dry_run:
            print(f"\n{'='*60}")
            print(f"  {GROUP_TO_YAML[group]}")
            print(f"{'='*60}")
            print(yaml_content)
        else:
            out_path = pipelines_dir / GROUP_TO_YAML[group]
            out_path.write_text(yaml_content)
            table_count = len(data["tables"])
            print(f"  Written: {out_path.relative_to(bundle_root)} ({table_count} tables)")

    if not args.dry_run and not args.export:
        print(f"\nDone. Run 'databricks bundle validate --target {args.target}' to verify.")


def _export_yaml_to_json(pipelines_dir: Path, table_lists_dir: Path, group: str):
    """Export an existing pipeline YAML to JSON format (reverse operation)."""
    import yaml  # Only needed for export mode

    yaml_path = pipelines_dir / GROUP_TO_YAML[group]
    if not yaml_path.exists():
        print(f"  SKIP: {yaml_path.name} not found", file=sys.stderr)
        return

    data = yaml.safe_load(yaml_path.read_text())
    pipelines = data["resources"]["pipelines"]
    pipeline_key = list(pipelines.keys())[0]
    pipeline = pipelines[pipeline_key]

    cluster_spec = pipeline.get("clusters", [{}])[0]
    ingestion = pipeline["ingestion_definition"]

    output = {
        "source_catalog": "<SOURCE_CATALOG>",
        "source_schema": "<SOURCE_SCHEMA>",
        "destination_schema": "<DESTINATION_SCHEMA>",
        "pipeline_resource_key": pipeline_key,
        "pipeline_name": pipeline.get("name"),
        "cluster": {
            "node_type_id": cluster_spec.get("node_type_id"),
            "min_workers": cluster_spec.get("autoscale", {}).get("min_workers"),
            "max_workers": cluster_spec.get("autoscale", {}).get("max_workers"),
        },
        "tables": [],
    }

    for obj in ingestion.get("objects", []):
        t = obj.get("table", {})
        entry = {
            "source_table": t.get("source_table"),
            "destination_table": t.get("destination_table"),
        }
        tc = t.get("table_configuration", {})
        qbc = tc.get("query_based_connector_config", {})
        if qbc.get("cursor_columns"):
            entry["cursor_columns"] = qbc["cursor_columns"]
        if tc.get("scd_type"):
            entry["scd_type"] = tc["scd_type"]
        output["tables"].append(entry)

    table_lists_dir.mkdir(parents=True, exist_ok=True)
    out_path = table_lists_dir / f"{group}_tables.json"
    out_path.write_text(json.dumps(output, indent=2) + "\n")
    print(f"  Exported: {out_path} ({len(output['tables'])} tables)")


if __name__ == "__main__":
    main()
