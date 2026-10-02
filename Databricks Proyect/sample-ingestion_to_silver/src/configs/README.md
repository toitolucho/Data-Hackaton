# Source Configs

Place YAML config files here (one per source table). Both pipelines read from this directory.

## Config fields by pipeline:

### Pipeline 1 (ingestion_to_staging) uses:
- `source_table` - table identifier
- `staging_catalog` / `staging_schema` - where staging tables land
- `source_path` / `file_format` / `bronze_options` - for Auto Loader sources
- `federated_source` - for Lakehouse Federation sources
- `ingestion_source` - tells Pipeline 2 how to read from Lakeflow Connect staging

### Pipeline 2 (staging_to_silver) uses:
- `source_table` - table identifier
- `staging_catalog` / `staging_schema` - where to read staging from
- `source_catalog` / `source_schema` - where Bronze tables are written
- `target_catalog_prefix` / `target_schema` - where Silver tables are written
- `scd_2_key_list` - primary keys for SCD2
- `history_timestamp_source` - sequencing column
- `scd_type` - 1 or 2
- `silver_columns` / `exclude_columns` - column selection
- `data_quality_rules` - DQ expectations
- `cluster_by` - liquid clustering
- `ingestion_source` - tells Bronze how to read (CDC vs snapshot)

## Example:
See `cdmast.yml` for a complete reference config.
