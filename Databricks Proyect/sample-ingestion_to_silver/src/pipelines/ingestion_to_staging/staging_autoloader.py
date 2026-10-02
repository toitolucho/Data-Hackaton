"""
staging_autoloader.py
================================================================================
Config-Driven Auto Loader Staging — Lakeflow Spark Declarative Pipelines
================================================================================

PURPOSE: Raw file acquisition only. Pure append streaming tables.
         No CDC, no deduplication, no transformation of any kind.
         Bronze reads from the tables this pipeline produces.

         Uses schema_parser to auto-generate schemaHints from .schema files
         when available, ensuring proper type inference (e.g. TIMESTAMP columns
         arrive typed instead of as strings from CSV).

ROUTING: Only processes YAML configs that have source_path + file_format
         (file-based sources). Configs with ingestion_source or federated_source
         are handled by their respective pipeline files.

PIPELINE: ingestion_to_staging (Pipeline 1)
================================================================================
"""

import os
import sys
import glob
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Make utilities importable
# ─────────────────────────────────────────────────────────────────────────────

# Project root from pipeline configuration (resolved by DAB at deploy time)
_PROJECT_ROOT = spark.conf.get("pipeline.project_root")

if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.utilities.schema_parser import parse_schema_file, to_spark_schema_hints


# ─────────────────────────────────────────────────────────────────────────────
# Config folder scan — filter to file-based sources only
# ─────────────────────────────────────────────────────────────────────────────

_CONFIG_DIR = os.path.join(_PROJECT_ROOT, "src", "configs")

if not os.path.isdir(_CONFIG_DIR):
    raise FileNotFoundError(
        f"Config directory not found: '{_CONFIG_DIR}'. "
        "Ensure src/configs/ exists with your YAML source definitions."
    )

_ALL_YAML_FILES = sorted(glob.glob(os.path.join(_CONFIG_DIR, "*.yml")))


def _load_source(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if isinstance(data, list):
        return data[0]
    return data


def _is_file_source(src: dict) -> bool:
    """Return True if this config is a file-based source (has source_path)."""
    return bool(src.get("source_path"))


# Load all configs, then filter to file-based sources only
_ALL_SOURCES = [(_load_source(f), f) for f in _ALL_YAML_FILES]
_FILTERED = [(src, f) for src, f in _ALL_SOURCES if _is_file_source(src)]

if not _FILTERED:
    _SOURCES = []
    _YAML_FILES = []
else:
    _SOURCES = [src for src, f in _FILTERED]
    _YAML_FILES = [f for src, f in _FILTERED]


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

_VALID_FORMATS         = {"json", "parquet", "csv", "avro", "orc", "text", "binaryfile"}
_VALID_EVOLUTION_MODES = {"rescue", "addNewColumns", "failOnNewColumns", "none"}


def _validate_sources(sources: list, yaml_files: list):
    seen_names = set()

    for src, filepath in zip(sources, yaml_files):
        filename = os.path.basename(filepath)
        name     = src.get("source_table", "<unnamed>")

        if not src.get("source_table"):
            raise ValueError(
                f"[{filename}] Missing required field 'source_table'."
            )
        if name in seen_names:
            raise ValueError(
                f"[{filename}] Duplicate source_table '{name}'."
            )
        seen_names.add(name)

        if not src.get("source_path"):
            raise ValueError(
                f"[{filename}] Missing required field 'source_path' for '{name}'."
            )

        fmt = src.get("file_format", "").lower()
        if not fmt or fmt not in _VALID_FORMATS:
            raise ValueError(
                f"[{filename}] Missing or invalid 'file_format' for '{name}'. "
                f"Valid options: {sorted(_VALID_FORMATS)}"
            )

        opts = src.get("bronze_options", {})

        if opts.get("schema_evolution_mode", "rescue") not in _VALID_EVOLUTION_MODES:
            raise ValueError(
                f"[{filename}] Invalid schema_evolution_mode for '{name}'. "
                f"Valid options: {sorted(_VALID_EVOLUTION_MODES)}"
            )

        if opts.get("use_managed_file_events") and opts.get("backfill_interval"):
            raise ValueError(
                f"[{filename}] '{name}' has both use_managed_file_events: true "
                "and backfill_interval — these are incompatible."
            )

        if fmt == "csv" and not opts.get("csv_options"):
            raise ValueError(
                f"[{filename}] '{name}' uses file_format: csv but has no "
                "csv_options block under bronze_options."
            )


_validate_sources(_SOURCES, _YAML_FILES)


# ─────────────────────────────────────────────────────────────────────────────
# Options incompatible with cloudFiles.useManagedFileEvents=true
# ─────────────────────────────────────────────────────────────────────────────

_MANAGED_FILE_EVENTS_INCOMPATIBLE = {
    "cloudFiles.useNotifications",
    "cloudFiles.backfillInterval",
    "cloudFiles.fetchParallelism",
    "cloudFiles.pathRewrites",
    "cloudFiles.resourceTag",
}


# ─────────────────────────────────────────────────────────────────────────────
# Schema hints from .schema files
# ─────────────────────────────────────────────────────────────────────────────

_SCHEMA_DIR = os.path.join(_PROJECT_ROOT, "src", "utilities")


def _get_schema_hints_from_file(source_table: str) -> str | None:
    """Generate schemaHints string from {source_table}.schema if it exists."""
    schema_file = f"{source_table}.schema"
    schema_path = os.path.join(_SCHEMA_DIR, schema_file)
    if not os.path.isfile(schema_path):
        return None
    columns = parse_schema_file(schema_file, schema_dir=_SCHEMA_DIR)
    if not columns:
        return None
    return to_spark_schema_hints(columns)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build cloudFiles reader options
# ─────────────────────────────────────────────────────────────────────────────

def _cloud_files_options(src: dict) -> dict:
    opts                    = src.get("bronze_options", {})
    file_format             = src["file_format"].lower()
    use_managed_file_events = opts.get("use_managed_file_events", False)

    options = {
        "cloudFiles.format":              file_format,
        "cloudFiles.schemaEvolutionMode": opts.get("schema_evolution_mode", "rescue"),
        "cloudFiles.inferColumnTypes":    str(opts.get("infer_column_types", True)).lower(),
    }

    # Schema hints priority: YAML config > .schema file > none
    schema_hints = opts.get("schema_hints") or _get_schema_hints_from_file(src["source_table"])
    if schema_hints:
        options["cloudFiles.schemaHints"] = schema_hints

    if opts.get("include_existing_files") is not None:
        options["cloudFiles.includeExistingFiles"] = str(opts["include_existing_files"]).lower()

    if opts.get("file_name_pattern"):
        options["pathGlobFilter"] = opts["file_name_pattern"]

    if opts.get("partition_columns"):
        options["cloudFiles.partitionColumns"] = ",".join(opts["partition_columns"])

    if opts.get("allow_overwrites"):
        options["cloudFiles.allowOverwrites"] = str(opts["allow_overwrites"]).lower()

    if opts.get("ignore_corrupt_files"):
        options["ignoreCorruptFiles"] = str(opts["ignore_corrupt_files"]).lower()

    if file_format == "csv":
        csv = opts.get("csv_options", {})
        options["header"]    = str(csv.get("header", True)).lower()
        options["delimiter"] = csv.get("delimiter", ",")
        options["multiline"] = str(csv.get("multiline", False)).lower()

    if use_managed_file_events:
        options["cloudFiles.useManagedFileEvents"] = "true"
        for key in _MANAGED_FILE_EVENTS_INCOMPATIBLE:
            options.pop(key, None)
    else:
        if opts.get("backfill_interval"):
            options["cloudFiles.backfillInterval"] = opts["backfill_interval"]

    return options


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic staging table registration
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    @dp.table(
        name=f"{src['staging_catalog']}.{src['staging_schema']}.{src['source_table']}",
        comment=src.get("description", f"Staging raw append table: {src['source_table']}"),
        table_properties={
            "quality":                        "staging",
            "pipelines.autoOptimize.managed": "true",
            "ingestion.source_type":          "autoloader",
            "ingestion.file_discovery_mode":  (
                "managed_file_events"
                if src.get("bronze_options", {}).get("use_managed_file_events", False)
                else "directory_listing"
            ),
            **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
        },
    )
    def ingest_staging(src=src):
        return (
            spark.readStream
            .format("cloudFiles")
            .options(**_cloud_files_options(src))
            .load(src["source_path"])
            .withColumn("_staging_ingested_at",  F.current_timestamp())
            .withColumn("_source_file_path",     F.col("_metadata.file_path"))
            .withColumn("_source_file_name",     F.col("_metadata.file_name"))
            .withColumn("_source_file_size",     F.col("_metadata.file_size").cast("long"))
            .withColumn("_source_file_modified", F.col("_metadata.file_modification_time"))
        )
