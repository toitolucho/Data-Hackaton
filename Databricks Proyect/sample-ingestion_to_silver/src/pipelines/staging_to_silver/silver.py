"""
silver.py
================================================================================
Config-Driven Silver Layer — Lakeflow Spark Declarative Pipelines
================================================================================

PURPOSE: Reads from Bronze streaming tables, applies SCD Type 2 via
         apply_changes. Produces clean, history-tracked Silver tables.

         Supports multiple Bronze source types:
           - Streaming tables (file-based / federated / Lakeflow CDC)
             → uses dp.apply_changes (streaming CDC flow)
           - Materialized views (Lakeflow snapshot)
             → uses dp.create_auto_cdc_from_snapshot_flow (snapshot comparison)

PIPELINE: staging_to_silver (Pipeline 2)
================================================================================
"""

import os
import re
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

from src.utilities.schema_parser import get_bit_column_names


# ─────────────────────────────────────────────────────────────────────────────
# Config folder scan
# ─────────────────────────────────────────────────────────────────────────────

_CONFIG_DIR = spark.conf.get(
    "pipeline.config_dir",
    os.path.join(_PROJECT_ROOT, "src", "configs"),
)
_SCHEMA_DIR = os.path.join(_PROJECT_ROOT, "src", "utilities")

if not os.path.isdir(_CONFIG_DIR):
    raise FileNotFoundError(
        f"Config directory not found: '{_CONFIG_DIR}'."
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

# Skip stub configs not yet configured for silver (empty scd_2_key_list) ─────
_pairs   = [(s, f) for s, f in zip(_SOURCES, _YAML_FILES) if s.get("scd_2_key_list")]
_skipped = [s["source_table"] for s in _SOURCES if not s.get("scd_2_key_list")]
if _skipped:
    _preview = _skipped[:5]
    _suffix  = f" ... +{len(_skipped)-5} more" if len(_skipped) > 5 else ""
    print(f"INFO: {len(_skipped)} config(s) skipped (no scd_2_key_list): {_preview}{_suffix}")
if _pairs:
    _SOURCES, _YAML_FILES = map(list, zip(*_pairs))
else:
    _SOURCES, _YAML_FILES = [], []

# Normalize scd_2_key_list to list (some configs store it as a scalar string)
for _s in _SOURCES:
    _keys = _s.get("scd_2_key_list")
    if isinstance(_keys, str):
        _s["scd_2_key_list"] = [k.strip() for k in _keys.split(",") if k.strip()]


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
# Ingestion source detection
# ─────────────────────────────────────────────────────────────────────────────

def _is_ingestion_source(src: dict) -> bool:
    return bool(src.get("ingestion_source"))

def _get_ingestion_load_mode(src: dict) -> str:
    ing = src.get("ingestion_source") or {}
    return ing.get("load_mode", "cdc").lower()

def _is_snapshot_source(src: dict) -> bool:
    return _is_ingestion_source(src) and _get_ingestion_load_mode(src) == "snapshot"


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate_sources(sources: list, yaml_files: list):
    seen_names = set()
    for src, filepath in zip(sources, yaml_files):
        filename = os.path.basename(filepath)
        name     = src.get("source_table", "<unnamed>")
        if not src.get("source_table"):
            raise ValueError(f"[{filename}] Missing required field 'source_table'.")
        if name in seen_names:
            raise ValueError(f"[{filename}] Duplicate source_table '{name}'.")
        seen_names.add(name)
        if not src.get("source_catalog"):
            raise ValueError(f"[{filename}] Missing 'source_catalog' for '{name}'.")
        if not src.get("source_schema"):
            raise ValueError(f"[{filename}] Missing 'source_schema' for '{name}'.")
        if not src.get("target_catalog_prefix"):
            raise ValueError(f"[{filename}] Missing 'target_catalog_prefix' for '{name}'.")
        if not src.get("target_schema"):
            raise ValueError(f"[{filename}] Missing 'target_schema' for '{name}'.")
        if not src.get("scd_2_key_list"):
            raise ValueError(f"[{filename}] Missing 'scd_2_key_list' for '{name}'.")

_validate_sources(_SOURCES, _YAML_FILES)


# ─────────────────────────────────────────────────────────────────────────────
# Stream helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_table_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "", name)

def _normalize_column_name(name: str) -> str:
    """Column names kept as-is (Delta supports special chars with backticks)."""
    return name

def _silver_col_name(col_def: dict) -> str:
    return col_def.get("rename") or col_def["name"]


def _translate_expr_columns(expr: str, rename_map: dict) -> str:
    for from_name, to_name in sorted(rename_map.items(), key=lambda x: -len(x[0])):
        expr = expr.replace(f"`{from_name}`", f"`{to_name}`")
        expr = re.sub(rf"\b{re.escape(from_name)}\b", f"`{to_name}`", expr)
    return expr


def _translate_expr_to_bronze(expr: str, silver_columns: list) -> str:
    reverse_rename = {}
    for c in (silver_columns or []):
        bronze_name = c["name"]
        silver_name = _silver_col_name(c)
        if silver_name != bronze_name:
            reverse_rename[silver_name] = bronze_name
    return _translate_expr_columns(expr, reverse_rename)


def _apply_common_transforms(df, src: dict, include_sequencing: bool = True):
    """Apply renames, selects, excludes, bit casting, and optionally sequencing."""
    silver_cols_cfg = src.get("silver_columns") or []

    # DQ gate: keep only rows that pass every rule.
    dq_rules = src.get("data_quality_rules") or []
    if dq_rules:
        combined_expr = " AND ".join(
            f"(({_translate_expr_to_bronze(r['expr'], silver_cols_cfg)}) IS NOT FALSE)"
            for r in dq_rules
        )
        df = df.filter(F.expr(combined_expr))

    # Build rename map: bronze_name -> user_silver_name.
    rename_map = {}
    for col_def in silver_cols_cfg:
        bronze_name = col_def["name"]
        silver_name = _silver_col_name(col_def)
        if silver_name != bronze_name:
            rename_map[bronze_name] = silver_name

    # Apply renames
    for orig, final in rename_map.items():
        df = df.withColumnRenamed(orig, final)

    if silver_cols_cfg:
        allow_missing = bool(src.get("allow_missing_columns", False))
        if allow_missing:
            existing = set(df.columns)
            select_exprs = []
            for c in silver_cols_cfg:
                user_silver = _silver_col_name(c)
                if user_silver in existing:
                    select_exprs.append(F.col(user_silver))
                else:
                    select_exprs.append(F.lit(None).cast("string").alias(user_silver))
            df = df.select(*select_exprs)
        else:
            df = df.select(*[_silver_col_name(c) for c in silver_cols_cfg])

    # Drop excluded columns
    for col in (src.get("exclude_columns") or []):
        if col in df.columns:
            df = df.drop(col)

    # Cast bit-origin columns to BOOLEAN.
    bit_cols = get_bit_column_names(src["source_table"], schema_dir=_SCHEMA_DIR)
    for bit_col in bit_cols:
        silver_name = rename_map.get(bit_col, bit_col)
        if silver_name in df.columns:
            df = df.withColumn(silver_name, F.col(silver_name).cast("boolean"))

    # Stamp sequencing column
    if include_sequencing:
        scd_seq = src.get("history_timestamp_source") or "pipeline_timestamp"
        if scd_seq == "pipeline_timestamp":
            df = df.withColumn("_silver_processed_at", F.current_timestamp())
        elif isinstance(scd_seq, list):
            ts_format = src["history_timestamp_format"]
            silver_seq_cols = [rename_map.get(c, c) for c in scd_seq]
            df = df.withColumn(
                "_scd_sequence_ts",
                F.to_timestamp(
                    F.concat_ws(" ", *[F.col(c).cast("string") for c in silver_seq_cols]),
                    ts_format
                )
            )
        else:
            silver_col = rename_map.get(scd_seq, scd_seq)
            ts_format = src.get("history_timestamp_format")
            if ts_format:
                df = df.withColumn(silver_col, F.to_timestamp(F.col(silver_col), ts_format))

    return df


def _build_silver_stream(src: dict):
    bronze_fqn = f"{src['source_catalog']}.{src['source_schema']}.{src['source_table']}"
    df = spark.readStream.table(bronze_fqn)
    return _apply_common_transforms(df, src, include_sequencing=True)


def _build_silver_batch(src: dict):
    bronze_fqn = f"{src['source_catalog']}.{src['source_schema']}.{src['source_table']}"
    df = spark.read.table(bronze_fqn)
    return _apply_common_transforms(df, src, include_sequencing=False)


def _build_quarantine_stream(src: dict):
    bronze_fqn = f"{src['source_catalog']}.{src['source_schema']}.{src['source_table']}"
    df = spark.readStream.option("skipChangeCommits", "true").table(bronze_fqn)

    silver_cols_cfg = src.get("silver_columns") or []
    rules           = src.get("data_quality_rules") or []
    failed_exprs    = [
        F.when(
            ~F.expr(_translate_expr_to_bronze(rule["expr"], silver_cols_cfg)),
            F.lit(rule["description"])
        )
        for rule in rules
    ]

    df = (
        df
        .withColumn("_failed_dq_rules",     F.array_compact(F.array(*failed_exprs)))
        .withColumn("_quarantine_timestamp", F.current_timestamp())
        .withColumn("_source_table",         F.lit(src["source_table"]))
        .filter(F.size(F.col("_failed_dq_rules")) > 0)
    )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Pre-compute _identity_hash columns from catalog (avoids df.columns in SDP plan)
# ─────────────────────────────────────────────────────────────────────────────

_IDENTITY_HASH_COLS = {}  # {table_name: sorted list of hash columns}

_identity_hash_sources = [s for s in _SOURCES if s.get("scd_2_key_list") == ["_identity_hash"]]
if _identity_hash_sources:
    # Derive hash columns from STAGING table schemas. Staging tables are external
    # to this pipeline (created by LC) so spark.table() works at top-level.
    # NOTE: information_schema.columns is NOT accessible to the pipeline SP.
    _ih_catalog = _identity_hash_sources[0].get("staging_catalog", "enterprise_dev")
    _ih_schema  = _identity_hash_sources[0].get("staging_schema", "staging_jackhenry")

    for _ih_src in _identity_hash_sources:
        _ih_tbl = _ih_src["source_table"]
        _ih_fqn = f"{_ih_catalog}.{_ih_schema}.{_ih_tbl}"
        try:
            _ih_fields = spark.table(_ih_fqn).schema.fields
            _IDENTITY_HASH_COLS[_ih_tbl] = sorted([
                re.sub(r"[ ,;{}()\n\t=\-]+", "_", f.name) for f in _ih_fields
                if not f.name.startswith("_") and not f.name.startswith("Jha")
            ])
        except Exception as _e:
            print(f"WARN: Could not read schema for {_ih_fqn}: {_e}")

    if _IDENTITY_HASH_COLS:
        print(f"INFO: Pre-computed _identity_hash columns for {len(_IDENTITY_HASH_COLS)} tables")
    else:
        raise RuntimeError(
            f"FATAL: Could not derive hash columns for any table. "
            f"Tried: {[s['source_table'] for s in _identity_hash_sources]}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Dynamic Silver table registration
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    table_name              = src["source_table"]
    silver_table            = _normalize_table_name(table_name)
    silver_stream_view_name = f"{table_name}_silver_stream"
    silver_batch_view_name  = f"{table_name}_silver_snapshot"
    scd_type                = src.get("scd_type", 2)

    # Pre-resolve hash columns for _identity_hash OUTSIDE the view function.
    # Uses staging schema (pre-computed above) — bronze is a pipeline-internal dataset
    # and CANNOT be referenced outside a @dp.temporary_view in SDP.
    _hash_cols_for_table = []
    if src.get("scd_2_key_list") == ["_identity_hash"]:
        _hash_cols_for_table = _IDENTITY_HASH_COLS.get(table_name, [])
        print(f"DEBUG: {table_name} _hash_cols_for_table has {len(_hash_cols_for_table)} cols (from _IDENTITY_HASH_COLS keys: {list(_IDENTITY_HASH_COLS.keys())[:5]})")
        if not _hash_cols_for_table:
            print(f"WARN: No pre-computed hash columns for {table_name} — _identity_hash will use df.columns fallback")

    silver_fqn = (
        f"{src['target_catalog_prefix']}"
        f".{src['target_schema']}"
        f".{silver_table}"
    )
    quarantine_fqn = (
        f"{src['target_catalog_prefix']}"
        f".{src['target_schema']}"
        f".{silver_table}_qtn"
    )

    silver_cols_cfg  = src.get("silver_columns") or []
    rename_map       = {
        c["name"]: _silver_col_name(c)
        for c in silver_cols_cfg
        if _silver_col_name(c) != c["name"]
    }
    silver_cols_user = [_silver_col_name(c) for c in silver_cols_cfg]

    cdf_renames = {
        user: _normalize_column_name(user)
        for user in silver_cols_user
        if _normalize_column_name(user) != user
    }
    silver_cols = [cdf_renames.get(u, u) for u in silver_cols_user]

    if silver_cols_cfg:
        def to_silver(bronze_name, _rm=rename_map, _cdf=cdf_renames):
            user_silver = _rm.get(bronze_name, bronze_name)
            return _cdf.get(user_silver, user_silver)
    else:
        def to_silver(bronze_name):
            return _normalize_column_name(bronze_name)

    # DQ expectations
    if silver_cols_cfg:
        dq_expectations = {
            rule["description"]: _translate_expr_columns(rule["expr"], cdf_renames)
            for rule in (src.get("data_quality_rules") or [])
        }
    else:
        def _build_implicit_dq_renames(rules):
            referenced = set()
            for rule in rules:
                referenced.update(re.findall(r"`([^`]+)`", rule["expr"]))
            return {n: _normalize_column_name(n) for n in referenced
                    if _normalize_column_name(n) != n}
        implicit_dq_renames = _build_implicit_dq_renames(
            src.get("data_quality_rules") or []
        )
        dq_expectations = {
            rule["description"]: _translate_expr_columns(rule["expr"], implicit_dq_renames)
            for rule in (src.get("data_quality_rules") or [])
        }

    # ───────────────────────────────────────────────────────────────────────────
    # SNAPSHOT MODE — create_auto_cdc_from_snapshot_flow
    # ───────────────────────────────────────────────────────────────────────────

    if _is_snapshot_source(src):

        @dp.temporary_view(name=silver_batch_view_name)
        def silver_snapshot_view(src=src, silver_cols_cfg=silver_cols_cfg, cdf_renames=cdf_renames, _ihcols=_hash_cols_for_table):
            df = _build_silver_batch(src)
            if silver_cols_cfg:
                for user_name, norm_name in cdf_renames.items():
                    df = df.withColumnRenamed(user_name, norm_name)
            else:
                for c in list(df.columns):
                    if c.startswith("_"):
                        continue
                    norm = _normalize_column_name(c)
                    if norm != c:
                        df = df.withColumnRenamed(c, norm)
            # _identity_hash is now computed at bronze level (@dp.table) where SDP
            # reliably executes function bodies. No need to add it here.
            return df

        cluster_by_normalized = [to_silver(c) for c in (src.get("cluster_by") or [])]
        dp.create_streaming_table(
            name               = silver_fqn,
            comment            = src.get("description", f"Silver SCD{scd_type} table: {table_name}"),
            cluster_by         = cluster_by_normalized,
            table_properties   = {
                "quality":                        "silver",
                "pipelines.autoOptimize.managed": "true",
                "silver.scd_type":                str(scd_type),
                "silver.source_mode":             "snapshot",
                **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
            },
        )

        keys_normalized = [to_silver(k) for k in src["scd_2_key_list"]]
        _bronze_cols = None
        try:
            _bronze_cols = set(
                r.column_name for r in spark.sql(
                    f"SELECT column_name FROM {src['source_catalog']}.information_schema.columns "
                    f"WHERE table_schema = '{src['source_schema']}' "
                    f"AND table_name = '{src['source_table']}'"
                ).collect()
            )
            scd_2_exclude_normalized = [
                to_silver(c) for c in (src.get("scd_2_exclude_list") or [])
                if to_silver(c) in _bronze_cols
            ]
        except Exception:
            scd_2_exclude_normalized = []

        if silver_cols_cfg:
            silver_cols_tracked = [c for c in silver_cols if c not in scd_2_exclude_normalized]
            track_history_kwargs = {"track_history_column_list": silver_cols_tracked}
        else:
            track_history_kwargs = {
                "track_history_except_column_list": scd_2_exclude_normalized
            }

        dp.create_auto_cdc_from_snapshot_flow(
            target                    = silver_fqn,
            source                    = silver_batch_view_name,
            keys                      = keys_normalized,
            stored_as_scd_type        = scd_type,
            **track_history_kwargs,
        )

    # ───────────────────────────────────────────────────────────────────────────
    # STREAMING MODE — apply_changes
    # ───────────────────────────────────────────────────────────────────────────

    else:

        @dp.temporary_view(name=silver_stream_view_name)
        def silver_stream(src=src, silver_cols_cfg=silver_cols_cfg, cdf_renames=cdf_renames, _ihcols=_hash_cols_for_table):
            df = _build_silver_stream(src)
            if silver_cols_cfg:
                for user_name, norm_name in cdf_renames.items():
                    df = df.withColumnRenamed(user_name, norm_name)
            else:
                for c in list(df.columns):
                    if c.startswith("_"):
                        continue
                    norm = _normalize_column_name(c)
                    if norm != c:
                        df = df.withColumnRenamed(c, norm)
            # _identity_hash is now computed at bronze level (@dp.table) where SDP
            # reliably executes function bodies. No need to add it here.
            return df

        cluster_by_normalized = [to_silver(c) for c in (src.get("cluster_by") or [])]
        dp.create_streaming_table(
            name               = silver_fqn,
            comment            = src.get("description", f"Silver SCD{scd_type} table: {table_name}"),
            cluster_by         = cluster_by_normalized,
            expect_all_or_drop = dq_expectations,
            table_properties   = {
                "quality":                        "silver",
                "pipelines.autoOptimize.managed": "true",
                "silver.scd_type":                str(scd_type),
                **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
            },
        )

        # When scd_2_key_list is ["_identity_hash"], use the pre-computed data
        # columns directly as composite keys. _identity_hash can't exist in the
        # bronze table's catalog schema without dropping/recreating it, and SDP
        # resolves apply_changes keys against the existing catalog schema.
        if src.get("scd_2_key_list") == ["_identity_hash"] and _hash_cols_for_table:
            keys_normalized = _hash_cols_for_table
        else:
            keys_normalized = [to_silver(k) for k in src["scd_2_key_list"]]

        scd_seq = src.get("history_timestamp_source") or "pipeline_timestamp"
        if scd_seq == "pipeline_timestamp":
            sequence_by_col = "_silver_processed_at"
            except_cols     = ["_silver_processed_at"]
        elif isinstance(scd_seq, list):
            sequence_by_col = "_scd_sequence_ts"
            except_cols     = ["_scd_sequence_ts"]
        else:
            sequence_by_col = to_silver(scd_seq)
            except_cols     = []

        _SYSTEM_AUDIT_COLS = [
            "_source_file_path",
            "_source_file_name",
            "_source_file_size",
            "_source_file_modified",
            "_bronze_ingested_at",
        ]
        if scd_seq == "pipeline_timestamp":
            _SYSTEM_AUDIT_COLS = _SYSTEM_AUDIT_COLS + ["_silver_processed_at"]
        elif isinstance(scd_seq, list):
            _SYSTEM_AUDIT_COLS = _SYSTEM_AUDIT_COLS + ["_scd_sequence_ts"]

        _bronze_cols = None
        try:
            _bronze_cols = set(
                r.column_name for r in spark.sql(
                    f"SELECT column_name FROM {src['source_catalog']}.information_schema.columns "
                    f"WHERE table_schema = '{src['source_schema']}' "
                    f"AND table_name = '{src['source_table']}'"
                ).collect()
            )
            scd_2_exclude_normalized = [
                to_silver(c) for c in (src.get("scd_2_exclude_list") or [])
                if to_silver(c) in _bronze_cols
            ]
        except Exception:
            scd_2_exclude_normalized = []

        if silver_cols_cfg:
            silver_cols_tracked = [c for c in silver_cols if c not in scd_2_exclude_normalized]
            track_history_kwargs = {"track_history_column_list": silver_cols_tracked}
        else:
            track_history_kwargs = {
                "track_history_except_column_list": [
                    c for c in list(dict.fromkeys(_SYSTEM_AUDIT_COLS + scd_2_exclude_normalized))
                    if _bronze_cols is not None and c in _bronze_cols
                ]
            }

        dp.apply_changes(
            target                    = silver_fqn,
            source                    = silver_stream_view_name,
            keys                      = keys_normalized,
            sequence_by               = sequence_by_col,
            stored_as_scd_type        = scd_type,
            except_column_list        = except_cols,
            **track_history_kwargs,
        )

    # Quarantine table + append flow (only when DQ rules exist, not for snapshot)
    if dq_expectations and not _is_snapshot_source(src):

        dp.create_streaming_table(
            name             = quarantine_fqn,
            comment          = f"Quarantine: rows from {table_name} that failed data quality rules.",
            table_properties = {
                "quality":                        "quarantine",
                "pipelines.autoOptimize.managed": "true",
                **{f"source.{k}": str(v) for k, v in src.get("tags", {}).items()},
            },
        )

        @dp.append_flow(target=quarantine_fqn, name=f"{silver_table}_quarantine_flow")
        def quarantine_flow(src=src):
            return _build_quarantine_stream(src)


# ─────────────────────────────────────────────────────────────────────────────
# Silver manifest
# ─────────────────────────────────────────────────────────────────────────────

@dp.temporary_view(name="_silver_manifest")
def silver_manifest():
    rows = [
        {
            "source_table":      s["source_table"],
            "source_catalog":    s.get("source_catalog", ""),
            "source_schema":     s.get("source_schema", ""),
            "target_catalog":    s.get("target_catalog_prefix", ""),
            "target_schema":     s.get("target_schema", ""),
            "scd_type":          str(s.get("scd_type", 2)),
            "scd_2_key_list":    str(s.get("scd_2_key_list", [])),
            "history_timestamp_source": str(s.get("history_timestamp_source") or "pipeline_timestamp"),
            "source_mode":       "snapshot" if _is_snapshot_source(s) else "streaming",
            "config_file":       os.path.basename(f),
            "tags":              str(s.get("tags", {})),
        }
        for s, f in zip(_SOURCES, _YAML_FILES)
    ]
    if not rows:
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField("source_table", StringType()),
            StructField("source_catalog", StringType()),
            StructField("source_schema", StringType()),
            StructField("target_catalog", StringType()),
            StructField("target_schema", StringType()),
            StructField("scd_type", StringType()),
            StructField("scd_2_key_list", StringType()),
            StructField("history_timestamp_source", StringType()),
            StructField("source_mode", StringType()),
            StructField("config_file", StringType()),
            StructField("tags", StringType()),
        ])
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows)
