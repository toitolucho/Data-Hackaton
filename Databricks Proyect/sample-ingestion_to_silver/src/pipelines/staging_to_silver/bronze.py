"""
bronze.py
================================================================================
Config-Driven Bronze Layer — Lakeflow Spark Declarative Pipelines
================================================================================

PURPOSE: Reads from Staging streaming tables, pure append.
         All columns from staging pass through — no column restriction.
         No dedup at bronze — bronze is an immutable audit layer.
         Silver owns all dedup/SCD2 logic.
         Silver reads from the tables this pipeline produces.

         Supports multiple ingestion methods:
           - File-based (staging via Auto Loader) → streaming table
           - Federated (staging via Lakehouse Federation) → streaming table
           - Lakeflow Connect CDC (ingestion pipeline replicates with CDC) → streaming table + skipChangeCommits
           - Lakeflow Connect snapshot (ingestion pipeline overwrites daily) → materialized view (batch read)

PIPELINE: staging_to_silver (Pipeline 2)
================================================================================
"""

import os
import sys
import glob
import re
import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Make utilities importable
# ─────────────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = spark.conf.get("pipeline.project_root")

if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# ─────────────────────────────────────────────────────────────────────────────
# Config folder scan
# ─────────────────────────────────────────────────────────────────────────────

_CONFIG_DIR = spark.conf.get(
    "pipeline.config_dir",
    os.path.join(_PROJECT_ROOT, "src", "configs"),
)

if not os.path.isdir(_CONFIG_DIR):
    raise FileNotFoundError(
        f"Config directory not found: '{_CONFIG_DIR}'. "
        "Set pipeline_param.config_dir to the full path of your sources/ folder."
    )

_YAML_FILES = sorted(glob.glob(os.path.join(_CONFIG_DIR, "*.yml")))

if not _YAML_FILES:
    raise FileNotFoundError(
        f"No .yaml files found in '{_CONFIG_DIR}'."
    )

def _load_source(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if isinstance(data, list):
        return data[0]
    return data

_SOURCES_ALL = [_load_source(f) for f in _YAML_FILES]

# pipeline_group filter ─────────────────────────────────────────────────────
# If pipeline.table_group is set (e.g. "large", "xlarge", "glmast"), only
# process configs whose `pipeline_group` field matches.
# If not set, process only UNTAGGED configs (excludes group-specific tables).
_TABLE_GROUP = spark.conf.get("pipeline.table_group", None)

if _TABLE_GROUP:
    _SOURCES  = [s for s in _SOURCES_ALL if s.get("pipeline_group") == _TABLE_GROUP]
    _YAML_FILES = [f for f, s in zip(_YAML_FILES, _SOURCES_ALL)
                   if s.get("pipeline_group") == _TABLE_GROUP]
    if not _SOURCES:
        raise ValueError(
            f"pipeline.table_group='{_TABLE_GROUP}' matched no configs in '{_CONFIG_DIR}'. "
            "Add pipeline_group: {_TABLE_GROUP} to the relevant src/configs/*.yml files."
        )
else:
    _SOURCES = [s for s in _SOURCES_ALL if not s.get("pipeline_group")]
    _YAML_FILES = [f for f, s in zip(_YAML_FILES, _SOURCES_ALL)
                   if not s.get("pipeline_group")]


# ─────────────────────────────────────────────────────────────────────────────
# Catalog override — use pipeline.catalog so configs are environment-agnostic
# ─────────────────────────────────────────────────────────────────────────────

_CATALOG = spark.conf.get("pipeline.catalog", None)

if _CATALOG:
    for _s in _SOURCES:
        _s["staging_catalog"] = _CATALOG
        _s["source_catalog"] = _CATALOG
        _s["target_catalog_prefix"] = _CATALOG


# ─────────────────────────────────────────────────────────────────────────────
# Schema file check (for manifest observability only)
# ─────────────────────────────────────────────────────────────────────────────

_SCHEMA_DIR = os.path.join(_PROJECT_ROOT, "src", "utilities")

def _has_schema_file(source_table: str) -> bool:
    """Return True if a .schema file exists for this table."""
    schema_file = f"{source_table}.schema"
    schema_path = os.path.join(_SCHEMA_DIR, schema_file)
    return os.path.isfile(schema_path)


# ─────────────────────────────────────────────────────────────────────────────
# Ingestion source detection
# ─────────────────────────────────────────────────────────────────────────────



def _sanitize_column_names(df):
    """Replace Delta-invalid characters (spaces, hyphens, etc.) in column names with underscores."""
    return df.toDF(*[re.sub(r"[ ,;{}()\n\t=\-]+", "_", col) for col in df.columns])

_VALID_INGESTION_LOAD_MODES = {"cdc", "snapshot"}


def _is_ingestion_source(src: dict) -> bool:
    """Return True if this source is a Lakeflow Connect ingestion table."""
    return bool(src.get("ingestion_source"))


def _get_ingestion_load_mode(src: dict) -> str:
    """Return the ingestion load mode: 'cdc' (default) or 'snapshot'."""
    ing = src.get("ingestion_source") or {}
    return ing.get("load_mode", "cdc").lower()


def _should_skip_change_commits(src: dict) -> bool:
    """Return True if Bronze should use skipChangeCommits."""
    ing = src.get("ingestion_source") or {}
    return ing.get("skip_change_commits", True)



# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

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

        if not src.get("staging_catalog"):
            raise ValueError(
                f"[{filename}] Missing required field 'staging_catalog' for '{name}'."
            )

        if not src.get("staging_schema"):
            raise ValueError(
                f"[{filename}] Missing required field 'staging_schema' for '{name}'."
            )

        if _is_ingestion_source(src):
            mode = _get_ingestion_load_mode(src)
            if mode not in _VALID_INGESTION_LOAD_MODES:
                raise ValueError(
                    f"[{filename}] Invalid ingestion_source.load_mode '{mode}' for "
                    f"'{name}'. Valid: {sorted(_VALID_INGESTION_LOAD_MODES)}."
                )


_validate_sources(_SOURCES, _YAML_FILES)


# ─────────────────────────────────────────────────────────────────────────────
# Filter out configs whose staging table does not exist yet
# (dev LC list may be a subset of prd — skip gracefully instead of failing)
#
# Uses information_schema with fallback: if the query fails during graph
# loading (serverless SDP PREVIEW), all configs pass through unconditionally.
# ─────────────────────────────────────────────────────────────────────────────

def _get_existing_staging_tables() -> set:
    """Fetch all table names in each distinct staging schema in one pass."""
    schema_groups = {}
    for s in _SOURCES:
        key = (s["staging_catalog"], s["staging_schema"])
        schema_groups.setdefault(key, [])
    existing = set()
    for (cat, sch) in schema_groups:
        try:
            rows = spark.sql(
                f"SELECT table_name FROM `{cat}`.information_schema.tables "
                f"WHERE table_schema = '{sch}'"
            ).collect()
            for row in rows:
                existing.add((cat, sch, row.table_name))
        except Exception as e:
            print(f"WARN: Could not query information_schema for {cat}.{sch}: {e}")
    return existing

_EXISTING_STAGING = _get_existing_staging_tables()

# If the query returned results, filter normally; otherwise bypass (graph loading limitation)
if _EXISTING_STAGING:
    def _staging_table_exists(src: dict) -> bool:
        return (src["staging_catalog"], src["staging_schema"], src["source_table"]) in _EXISTING_STAGING

    _pairs_exist = [(s, f) for s, f in zip(_SOURCES, _YAML_FILES) if _staging_table_exists(s)]
    _skipped_missing = [s["source_table"] for s in _SOURCES if not _staging_table_exists(s)]

    if _skipped_missing:
        _preview = _skipped_missing[:10]
        _suffix = f" ... +{len(_skipped_missing)-10} more" if len(_skipped_missing) > 10 else ""
        print(f"INFO: {len(_skipped_missing)} config(s) skipped (staging table not found): {_preview}{_suffix}")

    if _pairs_exist:
        _SOURCES, _YAML_FILES = map(list, zip(*_pairs_exist))
    else:
        _SOURCES, _YAML_FILES = [], []
else:
    print("WARN: information_schema query returned empty; bypassing staging table filter.")


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic Bronze table registration
#
# Branches by ingestion mode:
#   - File-based / Federated / Lakeflow CDC → streaming table (readStream)
#   - Lakeflow snapshot                     → materialized view (batch read)
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    _bronze_fqn = f"{src['source_catalog']}.{src['source_schema']}.{src['source_table']}"
    _staging_table = f"{src['staging_catalog']}.{src['staging_schema']}.{src['source_table']}"
    _common_props = {
        "quality":                        "bronze",
        "pipelines.autoOptimize.managed": "true",
        **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
    }

    # ───────────────────────────────────────────────────────────────────────────
    # SNAPSHOT mode — Materialized View (batch read)
    # ───────────────────────────────────────────────────────────────────────────

    if _is_ingestion_source(src) and _get_ingestion_load_mode(src) == "snapshot":

        @dp.materialized_view(
            name=_bronze_fqn,
            comment=src.get("description", f"Bronze snapshot table: {src['source_table']}"),
            table_properties={
                **_common_props,
                "ingestion.load_mode": "snapshot",
            },
        )
        def ingest_bronze_snapshot(src=src, _staging_table=_staging_table):
            df = spark.read.table(_staging_table)
            df = _sanitize_column_names(df)
            df = df.withColumn("_bronze_ingested_at", F.current_timestamp())
            # Add standard metadata columns if missing (Lakeflow Connect doesn't provide them)
            if _is_ingestion_source(src):
                for col_name, col_type in [
                    ("_staging_ingested_at", "timestamp"),
                    ("_source_file_path", "string"),
                    ("_source_file_name", "string"),
                    ("_source_file_size", "long"),
                    ("_source_file_modified", "timestamp"),
                ]:
                    if col_name not in df.columns:
                        df = df.withColumn(col_name, F.lit(None).cast(col_type))
            return df

    # ───────────────────────────────────────────────────────────────────────────
    # CDC mode / File-based / Federated — Streaming Table (readStream)
    # ───────────────────────────────────────────────────────────────────────────

    else:

        @dp.table(
            name=_bronze_fqn,
            comment=src.get("description", f"Bronze append table: {src['source_table']}"),
            table_properties={
                **_common_props,
                "ingestion.load_mode": (
                    "cdc" if _is_ingestion_source(src) else "file_or_federated"
                ),
            },
        )
        def ingest_bronze(src=src, _staging_table=_staging_table):
            reader = spark.readStream

            # For Lakeflow Connect CDC sources, skip update/delete commits
            if _is_ingestion_source(src) and _should_skip_change_commits(src):
                reader = reader.option("skipChangeCommits", "true")

            df = reader.table(_staging_table)
            df = _sanitize_column_names(df)
            df = df.withColumn("_bronze_ingested_at", F.current_timestamp())
            # Add standard metadata columns if missing (Lakeflow Connect doesn't provide them)
            if _is_ingestion_source(src):
                for col_name, col_type in [
                    ("_staging_ingested_at", "timestamp"),
                    ("_source_file_path", "string"),
                    ("_source_file_name", "string"),
                    ("_source_file_size", "long"),
                    ("_source_file_modified", "timestamp"),
                ]:
                    if col_name not in df.columns:
                        df = df.withColumn(col_name, F.lit(None).cast(col_type))
            return df


# ─────────────────────────────────────────────────────────────────────────────
# Bronze manifest
# ─────────────────────────────────────────────────────────────────────────────

def _get_ingestion_method(src: dict) -> str:
    if not _is_ingestion_source(src):
        return "file_or_federated"
    return f"lakeflow_connect_{_get_ingestion_load_mode(src)}"


@dp.temporary_view(name="_bronze_manifest")
def bronze_manifest():
    rows = [
        {
            "source_table":          s["source_table"],
            "staging_catalog":       s.get("staging_catalog", ""),
            "staging_schema":        s.get("staging_schema", ""),
            "source_catalog":        s.get("source_catalog", ""),
            "source_schema":         s.get("source_schema", ""),
            "target_catalog_prefix": s.get("target_catalog_prefix", ""),
            "target_schema":         s.get("target_schema", ""),
            "config_file":           os.path.basename(f),
            "tags":                  str(s.get("tags", {})),
            "has_schema_file":       str(_has_schema_file(s["source_table"])),
            "ingestion_method":      _get_ingestion_method(s),
        }
        for s, f in zip(_SOURCES, _YAML_FILES)
    ]
    return spark.createDataFrame(rows)
