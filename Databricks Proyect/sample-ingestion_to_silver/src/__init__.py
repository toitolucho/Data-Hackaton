"""
src — dev-ingestion-poc pipeline source package.

Subpackages:
    configs/     — YAML source table definitions (drive both pipelines)
    utilities/   — schema_parser, .schema type files
    pipelines/   — SDP pipeline code
        ingestion_to_staging/  — Pipeline 1 (Auto Loader + Federated → staging)
        staging_to_silver/     — Pipeline 2 (staging → bronze → silver)
"""
