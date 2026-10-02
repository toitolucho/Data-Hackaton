# Agent build guide — config-driven ingestion bundle on Databricks

This file is complete on its own. You do not need any sample repository. Every source file you must create is printed in full in the appendix at the end. Copy those files exactly. Only the YAML files and the bundle settings change per project.

The design was proven end to end on Azure Databricks with ServiceNow as the source: 10 API tables landed in bronze, and one table (`cmn_schedule`) went through flatten and SCD Type 2 silver, matched to the SQL Server copy 42 of 42 keys.

---

## 0. How to use this file

1. Read sections 1 to 5 before you write anything.
2. Ask the user for every value in section 3. Stop if a value is missing. Do not guess hosts, catalogs, keys, or column names.
3. Pick the pattern in section 5. Build only that pattern.
4. Follow the steps for that pattern in order. Each step names the exact files to create and where their content is.
5. After each step, run the check the step lists. Do not continue past a failing check.
6. Never deploy or run a job unless the user tells you to. The user runs deploy and run commands.

---

## 1. What you are building

A Databricks Asset Bundle (a folder of YAML and Python that the Databricks CLI deploys). It moves data through these layers:

| Layer | What it holds | Who writes it |
|---|---|---|
| Staging | Raw files exactly as received (JSON pages or CSV files) in a Unity Catalog volume or table | Extract task, copy task, or Auto Loader |
| Bronze | Append-only Delta table. Nothing is updated or deleted. Adds load metadata. | Promote task or bronze pipeline |
| Flat (API pattern only) | Bronze projected into real columns | Fan-out pipeline |
| Silver | SCD Type 2 history table. One current row per key plus old versions. | Silver pipeline |

### Pattern A — REST API source

```text
HTTPS API
  │  extract task (Python): pages → NDJSON files
  ▼
staging volume   <catalog>.staging_<sor>.<table>         /Volumes/... batch=<id>/part-0001.ndjson
  │  promote task (Python, Auto Loader): one row per record, whole record in payload (VARIANT)
  ▼
bronze table     <catalog>.bronze_<sor>.<table>_raw
  │  raw_to_flat pipeline: payload:<field> → columns
  ▼
flat table       <catalog>.bronze_<sor>.<table>_flat
  │  staging_to_silver pipeline: SCD Type 2
  ▼
silver table     <catalog>.silver_<sor>.<table>_flat
```

Job task order: `bootstrap → extract → promote → raw_to_flat → staging_to_silver`.

The last two tasks are added only when a table is ready for silver. Until then a table's job stops at `promote`.

### Pattern B — files (CSV) on AWS S3

```text
s3://<bucket>/<prefix>*.csv
  │  s3_copy task (Python, boto3, keys from a secret scope)     ← only when there is no UC external location
  ▼
staging volume   /Volumes/<catalog>/staging_<sor>/landing/<table>/
  │  file_loader pipeline (Auto Loader): CSV → rows, all columns STRING, plus file metadata
  ▼
staging table    <catalog>.staging_<sor>.<table>
  │  staging_to_silver pipeline, bronze.py: append copy + _bronze_ingested_at
  ▼
bronze table     <catalog>.bronze_<sor>.<table>
  │  staging_to_silver pipeline, silver.py: SCD Type 2
  ▼
silver table     <catalog>.silver_<sor>.<table>
```

Job task order: `s3_copy → file_loader_to_staging → staging_to_silver`.

CSV is already flat, so there is no fan-out.

---

## 2. Rules

1. One YAML file = one source table. Every setting for that table, including silver settings, goes in that one file. Do not create a second config folder for silver.
2. Secrets live only in a Databricks secret scope. Code reads them at run time with `dbutils.secrets.get`. Never put a password, token, or key in YAML, Python, a notebook, a commit, or a log line.
3. Do not edit `.bundle/` or `.databricks/`. The CLI generates them.
4. Do not deploy, run, commit, or push unless the user says to.
5. Build and prove one table first. Add the next table only after the first one passes its checks.
6. Use Serverless compute for jobs and pipelines. On the original workspace, classic clusters could not open TLS to the API (`UNEXPECTED_EOF_WHILE_READING`).
7. Bronze is append-only. Never `MERGE`, `UPDATE`, `DELETE`, or overwrite a bronze table.
8. Do not rewrite the framework files in the appendix. If a source needs different behavior, add a new config key and a small branch for it, and say so to the user.
9. Use only `.yml` (not `.yaml`) for config files.
10. Deploy to target `dev_sandbox` only. Do not create or use a shared or production target unless the user asks.

---

## 3. Inputs to get from the user

Write these into a table in the bundle `README.md` before you create files.

| Input | Example | Used in |
|---|---|---|
| Bundle name | `servicenow-sor-databricks` | `databricks.yml` |
| Bundle folder | `databricks/servicenow-sor-databricks` | where you create files |
| Workspace URL | `https://adb-7405617824787548.8.azuredatabricks.net/` | `databricks.yml` |
| CLI profile name | `axos-dev` | `databricks.yml`, commands |
| Catalog | `enterprise_dev` | every table name |
| SOR token (short, lowercase) | `servicenow` | schema names `staging_<sor>`, `bronze_<sor>`, `silver_<sor>` |
| Secret scope name | `servicenow` | YAML `secret_scope` |
| Secret key names (not values) | `snow-username`, `snow-password` | YAML auth block |
| First table name | `cmn_schedule` | first YAML and job |

Pattern A also needs:

| Input | Example |
|---|---|
| API host | `https://bofi.service-now.com` |
| Path of the table or collection | `/api/now/table/cmn_schedule` |
| Auth type | `basic` |
| Paging style and parameter names | offset/limit: `sysparm_offset`, `sysparm_limit` |
| JSON key that holds the list of records | `result` |
| Primary key field | `sys_id` |
| Full reload or incremental | full reload for small tables, watermark for large |
| Watermark field (incremental only) | `sys_updated_on` |
| Columns to keep in silver, matching the downstream SQL table | `sys_id`, `name`, `type`, `sys_class_name` |

Pattern B also needs:

| Input | Example |
|---|---|
| Bucket | `factored-datathon-2026-s3-157725502942-us-east-2-an` |
| Region | `us-east-2` |
| Prefix | `data/` |
| File name pattern for this table | `orders_*.csv` |
| Header row and delimiter | yes, `,` |
| S3 access | UC external location exists (option 1) or access keys only (option 2) |
| Row key column (after the first load) | `order_id` |
| Timestamp column that orders versions, if any | `updated_at` |

---

## 4. Naming standard

| Object | Name |
|---|---|
| Schemas | `<catalog>.staging_<sor>`, `<catalog>.bronze_<sor>`, `<catalog>.silver_<sor>` |
| Config file | `configs/sources/<sor>_<table>.yml` |
| Job resource key | `<sor>_<table>` |
| Job display name | Pattern A `api_loader_<sor>_<table>`. Pattern B `<sor>_files_to_silver`. |
| Pattern A staging volume | `<catalog>.staging_<sor>.<table>` |
| Pattern A bronze | `<catalog>.bronze_<sor>.<table>_raw` |
| Pattern A flat | `<catalog>.bronze_<sor>.<table>_flat` |
| Pattern A silver | `<catalog>.silver_<sor>.<table>_flat` (silver keeps the flat table's name) |
| Pattern B landing volume | `<catalog>.staging_<sor>.landing` |
| Pattern B staging / bronze / silver | `<catalog>.<layer>_<sor>.<table>` |

If a table name already exists in the catalog from earlier manual work, do not overwrite it. Use the `_raw` and `_flat` suffixes above, which keep the new tables separate.

---

## 5. Pick the pattern

| The source is | Use |
|---|---|
| An HTTPS endpoint that returns JSON pages | Pattern A |
| Files (CSV) in an S3 bucket | Pattern B |

Build only one pattern per bundle. A different source type gets its own bundle folder and its own Git repository.

---

## 6. Common setup (both patterns)

### Step 6.1 — Create the bundle folder

Create the folder the user named in section 3. All paths below are relative to it.

### Step 6.2 — `.gitignore`

Create `.gitignore` with the content from appendix file **`.gitignore`**.

### Step 6.3 — `databricks.yml`

Create `databricks.yml`:

```yaml
bundle:
  name: <bundle_name>

include:
  - resources/jobs/*.yml

variables:
  catalog:
    description: Unity Catalog catalog for every schema this bundle creates
    default: <catalog>

presets:
  tags:
    project: ${bundle.name}
    environment: ${bundle.target}
    managed_by: databricks-asset-bundle
    source_system: <sor>

targets:
  dev_sandbox:
    mode: development
    default: true
    workspace:
      host: <workspace_url>
      profile: <cli_profile>
      root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/dev_sandbox
    variables:
      catalog: <catalog>
```

`mode: development` prefixes every job and pipeline with `[dev <user>]` and deploys under the user's home folder, so it does not collide with anyone else.

Add `- resources/pipelines/*.yml` to `include` only when you create the first pipeline file.

### Step 6.4 — `README.md`

Create `README.md` with:

- the input table from section 3 (names only, never secret values)
- the pattern and the layer diagram for it from section 1
- the folder tree for the pattern
- the commands from section 9

---

## 7. Pattern A — REST API source

### Step A1 — Folder tree

Create exactly this tree. Every file listed is in the appendix.

```text
<bundle>/
├── .gitignore
├── README.md
├── databricks.yml
├── configs/
│   └── sources/
│       └── <sor>_<table>.yml                    # you write this (Step A4)
├── resources/
│   ├── jobs/
│   │   └── <sor>_<table>.yml                    # you write this (Step A5)
│   └── pipelines/                               # created in Step A9 only
├── scripts/
│   └── validate_config.py
└── src/
    ├── api_loader/
    │   ├── __init__.py                          # empty file
    │   ├── bootstrap.py
    │   ├── config.py
    │   ├── extractor.py
    │   ├── promoter.py
    │   ├── retry.py
    │   ├── secrets.py
    │   ├── utils.py
    │   ├── auth/
    │   │   ├── __init__.py
    │   │   ├── api_key.py
    │   │   ├── base.py
    │   │   ├── basic.py
    │   │   ├── bearer.py
    │   │   └── oauth2_client_credentials.py
    │   ├── connectivity/
    │   │   ├── __init__.py
    │   │   ├── base.py
    │   │   ├── direct.py
    │   │   └── uc_proxy.py
    │   └── pagination/
    │       ├── __init__.py
    │       ├── base.py
    │       ├── cursor.py
    │       ├── link_header.py
    │       └── offset_limit.py
    ├── python/
    │   ├── run_bootstrap.py
    │   ├── run_extract.py
    │   └── run_promote.py
    ├── pipelines/                               # created in Step A9 only
    │   ├── raw_to_flat/fanout.py
    │   └── staging_to_silver/silver.py
    └── utilities/                               # created in Step A9 only
        └── schema_parser.py
```

### Step A2 — Engine files

Create every file under `src/api_loader/`, `src/python/`, and `scripts/` from the appendix, with the same path and the same content. `src/api_loader/__init__.py` is an empty file.

What each part does, so you can explain it to the user:

| File | Job |
|---|---|
| `config.py` | Loads one YAML and checks every required key. A missing or wrong key stops the run with a list of errors. |
| `bootstrap.py` | Creates the staging schema, bronze schema, staging volume, and empty bronze table if they do not exist. |
| `extractor.py` | Calls the API page by page and writes each page as `part-NNNN.ndjson` under `batch=<batch_id>/` in the volume. Never writes a table. |
| `promoter.py` | Reads only that batch folder with Auto Loader and appends to bronze. The whole record goes into the VARIANT column `payload`. Never calls the API. |
| `auth/` | Builds the request auth from secret-scope keys. `basic`, `bearer`, `api_key`, `oauth2_client_credentials`, or `none`. |
| `pagination/` | `offset_limit` stops at a short page. `cursor` follows a token in the body. `link_header` follows the `Link` response header. |
| `connectivity/` | `direct` calls the host with `requests`. `uc_proxy` calls through a Unity Catalog HTTP connection (only if one exists). |
| `retry.py` | Retries 429 and 5xx with exponential backoff and honors `Retry-After`. |
| `secrets.py` | Reads `dbutils.secrets`. On a laptop it falls back to an env var named `<SCOPE>__<KEY>` (upper case, `-` → `_`). |
| `run_extract.py` | Reads `MAX(_watermark_val)` from bronze for incremental tables, runs the extractor, and passes `batch_id` to the next task as a task value. |
| `run_promote.py` | Runs the promoter for the `batch_id` passed in. |
| `validate_config.py` | Runs `config.py` checks on every YAML without Databricks. |

Bronze columns written by the promoter:

| Column | Meaning |
|---|---|
| `payload` | The full JSON record (VARIANT) |
| `_load_ts` | When the row was written |
| `_batch_id` | The extract run that produced it |
| `_source` | `source.name` from the YAML |
| `_pk` | Value of the first `primary_key` field |
| `_watermark_val` | Watermark field value (incremental tables only) |
| `_snapshot_date` | Load date (full-reload tables only) |
| `_corrupt_record` | Always NULL today. Reserved. |

Check: `python scripts/validate_config.py` runs and prints nothing yet (there are no YAML files). It must not raise an import error. The machine needs `pyyaml` installed.

### Step A3 — Understand the two load modes

| Mode | When | What the extractor does | Bronze |
|---|---|---|---|
| `full_only` | Small tables (up to tens of thousands of rows) | Reads every page every run | Every run appends a full copy. `_snapshot_date` is set. Read one snapshot with `WHERE _batch_id = (SELECT MAX(_batch_id) ...)`. |
| `watermark` | Large tables | First run (bronze empty) reads all history. Later runs read only records changed after `MAX(_watermark_val)`. | Appends only changed records. `_watermark_val` is set. Read current state with `QUALIFY ROW_NUMBER() OVER (PARTITION BY _pk ORDER BY _load_ts DESC) = 1`. |

How the watermark is sent to the API:

- `incrementality.watermark_param` set to a query parameter name (for example `since`): the extractor adds `since=<last watermark>` to the first request.
- `incrementality.watermark_param: N/A`: this is the ServiceNow rule in `extractor.apply_servicenow_sysparm_query`. With a watermark it sends `sysparm_query=sys_updated_on>{watermark}^ORDERBYsys_id`. With no watermark it sends only the ordering, which is a full history load. Use `N/A` only for ServiceNow. For any other API set the real parameter name.

The cursor column must be a field that changes every time the record changes. For ServiceNow it is always `sys_updated_on`, even if the downstream SQL copy dropped that column. Do not use business dates such as `start_time`.

`pagination.max_pages` is a safety cap for first tests. Remove it before a real historical load, or the table will silently hold only the first pages.

### Step A4 — First table YAML

Create `configs/sources/<sor>_<table>.yml`. Every key below is required unless marked optional. Write the literal string `N/A` for a value that does not apply. Never leave a key out to mean "default".

```yaml
# One YAML = one source table.

source:
  name: <sor>_<table>
  connectivity: direct            # direct | uc_proxy
  connection: N/A                 # UC HTTP connection name when connectivity=uc_proxy
  api_host: https://<host>        # no trailing slash
  base_path: /<path>              # path to the table or collection
  secret_scope: <scope>
  extra_params:                   # optional: static query params added to every request
    <param>: <value>

auth:
  type: basic                     # basic | bearer | api_key | oauth2_client_credentials | none
  username_secret_key: <key>      # basic
  password_secret_key: <key>      # basic

pagination:
  type: offset_limit              # offset_limit | cursor | link_header
  page_size: 1000
  offset_param: <offset param>
  limit_param: <limit param>
  max_pages: 5                    # optional. Test cap only. Remove for the real load.

incrementality:
  mode: full_only                 # full_only | watermark
  watermark_field: N/A            # dotted path in a record, e.g. sys_updated_on
  watermark_param: N/A            # query param that receives the watermark, or N/A (ServiceNow rule)

primary_key: [<id_field>]

response:
  records_path: <key>             # JSON key that holds the list, or N/A when the body is the list

rate_limit:
  respect_retry_after: true
  max_retries: 3
  backoff: exponential

staging:
  volume: <catalog>.staging_<sor>.<table>

bronze:
  table: <catalog>.bronze_<sor>.<table>_raw
  snapshot_partition: true        # true when mode=full_only, false when mode=watermark
```

Auth keys for the other types:

| `auth.type` | Extra keys |
|---|---|
| `bearer` | `secret_key` (secret holding the token) |
| `api_key` | `secret_key`, `header_name` (default `X-Api-Key`) |
| `oauth2_client_credentials` | `token_url`, `client_id_secret`, `client_secret_secret`, optional `scope`, optional `client_id_header` |
| `none` | none |

Pagination keys for the other types:

| `pagination.type` | Keys |
|---|---|
| `cursor` | `cursor_param`, `cursor_json_path`, optional `page_size_param`, optional `page_size` |
| `link_header` | none |

Worked example, small full-reload ServiceNow table (this exact file was run and reconciled):

```yaml
source:
  name: servicenow_cmn_schedule
  connectivity: direct
  connection: N/A
  api_host: https://bofi.service-now.com
  base_path: /api/now/table/cmn_schedule
  secret_scope: servicenow
  extra_params:
    sysparm_query: ORDERBYsys_id

auth:
  type: basic
  username_secret_key: snow-username
  password_secret_key: snow-password

pagination:
  type: offset_limit
  page_size: 50
  offset_param: sysparm_offset
  limit_param: sysparm_limit
  max_pages: 5

incrementality:
  mode: full_only
  watermark_field: N/A
  watermark_param: N/A

primary_key: [sys_id]

response:
  records_path: result

rate_limit:
  respect_retry_after: true
  max_retries: 3
  backoff: exponential

staging:
  volume: enterprise_dev.staging_servicenow.cmn_schedule

bronze:
  table: enterprise_dev.bronze_servicenow.cmn_schedule_raw
  snapshot_partition: true
```

Worked example, incremental ServiceNow table: appendix file **`configs/sources/servicenow_change_request.yml`**.

Check: `python scripts/validate_config.py` prints `OK   configs/sources/<sor>_<table>.yml`.

### Step A5 — Job for the table

Create `resources/jobs/<sor>_<table>.yml`. Replace only the placeholders.

```yaml
resources:
  jobs:
    <sor>_<table>:
      name: api_loader_<sor>_<table>
      environments:
        - environment_key: serverless_default
          spec:
            environment_version: "2"
            dependencies:
              - pyyaml
              - requests
      tasks:
        - task_key: bootstrap
          environment_key: serverless_default
          spark_python_task:
            python_file: ../../src/python/run_bootstrap.py
            parameters:
              - "--config"
              - "../../configs/sources/<sor>_<table>.yml"
        - task_key: extract
          environment_key: serverless_default
          depends_on:
            - task_key: bootstrap
          spark_python_task:
            python_file: ../../src/python/run_extract.py
            parameters:
              - "--config"
              - "../../configs/sources/<sor>_<table>.yml"
        - task_key: promote
          environment_key: serverless_default
          depends_on:
            - task_key: extract
          spark_python_task:
            python_file: ../../src/python/run_promote.py
            parameters:
              - "--config"
              - "../../configs/sources/<sor>_<table>.yml"
              - "--batch-id"
              - "{{tasks.extract.values.batch_id}}"
```

Why the paths look like this:

- `python_file` is relative to the job YAML (`resources/jobs/`), so `../../src/python/...`.
- `parameters` paths are relative to the folder of the Python file when the task runs (`src/python/`), so `../../configs/...` also lands on the bundle root.
- `{{tasks.extract.values.batch_id}}` is filled at run time from the value `run_extract.py` sets. That is how promote reads only this run's files.
- `environments` without a cluster means Serverless.

### Step A6 — Check before handing to the user

From the bundle folder:

```powershell
python scripts/validate_config.py
databricks bundle validate -t dev_sandbox --profile <cli_profile>
```

Both must pass. If `bundle validate` fails with `Refresh token is invalid`, that is an expired login, not a bundle problem. Tell the user to run `databricks auth login --host <workspace_url> --profile <cli_profile>`.

Stop here. Tell the user the deploy and run commands from section 9.

### Step A7 — What the user should see after the first run

Give the user these queries for a SQL editor or notebook:

```sql
-- files landed
LIST '/Volumes/<catalog>/staging_<sor>/<table>/';

-- rows per batch
SELECT _batch_id, COUNT(*) AS rows, COUNT(DISTINCT _pk) AS keys
FROM <catalog>.bronze_<sor>.<table>_raw
GROUP BY _batch_id
ORDER BY _batch_id DESC;

-- sample fields out of the VARIANT
SELECT payload:<id_field>::string AS id, payload:<field>::string AS field
FROM <catalog>.bronze_<sor>.<table>_raw
LIMIT 10;
```

Compare `COUNT(DISTINCT _pk)` to the downstream system, not `COUNT(*)`. Full-reload tables hold one copy per run. API header totals (for ServiceNow `X-Total-Count`) can be higher than the readable rows because of access rules: the original run saw 43 in the header and 42 readable rows.

### Step A8 — Adding more tables

For each new table: copy the first table's YAML and job, rename every `<table>` occurrence, and set the mode.

| Table size | Settings |
|---|---|
| Small, no reliable change field | `mode: full_only`, `snapshot_partition: true`, `watermark_field: N/A` |
| Large, has a change timestamp | `mode: watermark`, `snapshot_partition: false`, `watermark_field: <field>`, `page_size: 1000` |
| Large first load (historical backfill) | Same as large, and **no `max_pages`**. Expect a long run. On the original workspace about 830 thousand rows took about 60 minutes. |

Run `python scripts/validate_config.py` after every new file.

API payloads can contain nested objects instead of plain values. In ServiceNow, reference fields are `{"link": ..., "value": ..., "display_value": ...}`. Bronze stores them as-is in `payload`, which is why the VARIANT design does not break when a field changes shape. The flatten step must read `payload:<field>:value` for those (see Step A9.6).

### Step A9 — Silver for one table

Do this only after the table's bronze passed Step A7. Do it for one table at a time.

#### A9.1 — Add silver settings to the same YAML

At the bottom of `configs/sources/<sor>_<table>.yml`, add these top-level keys (same indentation as `bronze:`):

```yaml
# Silver (SCD Type 2). Read by the fan-out and by silver.py.
source_table: <table>_flat
source_catalog: <catalog>
source_schema: bronze_<sor>
target_catalog_prefix: <catalog>
target_schema: silver_<sor>
scd_2_key_list:
  - <id_field>
history_timestamp_source: <change_timestamp_field>
history_timestamp_format: "yyyy-MM-dd HH:mm:ss"
scd_type: 2
scd_2_exclude_list:
  - <change_timestamp_field>
  - _batch_id
  - _load_ts
columns:
  - <id_field>
  - <business column 1>
  - <business column 2>
  - <change_timestamp_field>
```

What each key does:

| Key | Meaning |
|---|---|
| `source_table` | Name of the flat table the fan-out writes, and the name silver uses for its output table |
| `source_catalog`, `source_schema` | Where the flat table lives (the bronze schema) |
| `target_catalog_prefix`, `target_schema` | Where silver is written |
| `scd_2_key_list` | Business key. One row per key is current. |
| `history_timestamp_source` | Field that says which version is newer |
| `history_timestamp_format` | Format of that field as text. ServiceNow uses `yyyy-MM-dd HH:mm:ss`. |
| `scd_type` | `2` keeps history. `1` keeps only the latest. |
| `scd_2_exclude_list` | Columns whose change alone must not open a new version |
| `columns` | Fields the fan-out pulls out of `payload`. Use the downstream SQL table's column names plus the timestamp field. Leave out values the old system calculated (for example a `HashKey`). |

The API loader ignores these extra keys. `silver.py` and `fanout.py` ignore every YAML that has no `scd_2_key_list`, so tables without these keys stay bronze-only.

Check: `python scripts/validate_config.py` still prints `OK` for the file.

#### A9.2 — Fan-out and silver code

Create from the appendix:

- `src/pipelines/raw_to_flat/fanout.py`
- `src/pipelines/staging_to_silver/silver.py`
- `src/utilities/schema_parser.py` (imported by `silver.py`; with no `.schema` files it changes nothing)

`fanout.py` streams `bronze.table`, keeps rows with no corrupt record and a non-null key, and appends `columns` plus `_batch_id` and `_load_ts` to `source_catalog.source_schema.source_table`. The stream checkpoint means each run reads only rows added since the last run.

`silver.py` reads every YAML in the config folder, keeps those with `scd_2_key_list`, skips any whose flat table does not exist yet, and runs `apply_changes` (SCD Type 2) from the flat table into `target_catalog_prefix.target_schema.source_table`. Bronze and staging file-metadata columns are excluded from change tracking automatically.

#### A9.3 — Pipeline resources

Create `resources/pipelines/raw_to_flat.pipeline.yml`:

```yaml
resources:
  pipelines:
    <sor>_raw_to_flat:
      name: <sor>_raw_to_flat
      catalog: ${var.catalog}
      schema: bronze_<sor>
      serverless: true
      channel: CURRENT
      photon: true
      continuous: false
      configuration:
        pipeline.config_dir: ${workspace.root_path}/files/configs/sources
        pipeline.catalog: ${var.catalog}
      libraries:
        - file:
            path: ../../src/pipelines/raw_to_flat/fanout.py
      environment:
        dependencies:
          - pyyaml
```

Create `resources/pipelines/staging_to_silver.pipeline.yml`:

```yaml
resources:
  pipelines:
    <sor>_staging_to_silver:
      name: <sor>_staging_to_silver
      catalog: ${var.catalog}
      schema: silver_<sor>
      serverless: true
      channel: CURRENT
      photon: true
      edition: ADVANCED
      continuous: false
      configuration:
        "pipelines.pipelineType": "WORKSPACE"
        pipeline.catalog: ${var.catalog}
        pipeline.project_root: ${workspace.root_path}/files
        pipeline.config_dir: ${workspace.root_path}/files/configs/sources
      libraries:
        - file:
            path: ../../src/pipelines/staging_to_silver/silver.py
      environment:
        dependencies:
          - pyyaml
```

`edition: ADVANCED` is required for `apply_changes`. `${workspace.root_path}/files` is where the bundle uploads its files.

#### A9.4 — Include pipelines in the bundle

In `databricks.yml`:

```yaml
include:
  - resources/jobs/*.yml
  - resources/pipelines/*.yml
```

#### A9.5 — Add two tasks to that table's job

Append to `tasks` in `resources/jobs/<sor>_<table>.yml`:

```yaml
        - task_key: raw_to_flat
          depends_on:
            - task_key: promote
          pipeline_task:
            pipeline_id: ${resources.pipelines.<sor>_raw_to_flat.id}
            full_refresh: false
        - task_key: staging_to_silver
          depends_on:
            - task_key: raw_to_flat
          pipeline_task:
            pipeline_id: ${resources.pipelines.<sor>_staging_to_silver.id}
            full_refresh: false
```

`full_refresh: false` keeps the stream checkpoints and the silver history. A full refresh would rebuild them from bronze.

Both pipelines process every silver-enabled YAML in one update. When more than one table has silver, do not attach the pipelines to every table job, because two jobs starting the same pipeline at once makes the second fail. Instead create one parent job that runs the table jobs (`run_job_task`) and then the two pipeline tasks once.

The complete job file for the proven table is appendix file **`resources/jobs/servicenow_cmn_schedule.yml`**.

Check: `python scripts/validate_config.py` and `databricks bundle validate -t dev_sandbox --profile <cli_profile>` both pass. Stop and hand off to the user.

#### A9.6 — Nested reference fields

`fanout.py` accepts simple field names only and reads `payload:<name>` as text. If a silver column is a nested object (a ServiceNow reference), tell the user before you change anything. The change is in `fanout.py`:

1. Let a `columns` entry be either a name or a mapping `{name: <column>, path: <dotted.path>}`.
2. For a mapping, project `variant_get(payload, '$.<dotted.path>', 'string') AS <column>`.
3. Validate each part of the path with the same identifier rule.

Example YAML after that change:

```yaml
columns:
  - sys_id
  - name: assigned_to
    path: assigned_to.value
```

`value` is the key the old C# loader stored. Do not add this until a table needs it.

#### A9.7 — What the user should see

```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT <id_field>) AS keys
FROM <catalog>.bronze_<sor>.<table>_flat;

SELECT COUNT(*) AS current_rows
FROM <catalog>.silver_<sor>.<table>_flat
WHERE __END_AT IS NULL;

SELECT <id_field>, __START_AT, __END_AT
FROM <catalog>.silver_<sor>.<table>_flat
ORDER BY <id_field>, __START_AT
LIMIT 20;
```

`__START_AT` and `__END_AT` are added by SCD Type 2. `__END_AT IS NULL` is the current row. A second run with unchanged business columns must not add rows to silver.

---

## 8. Pattern B — CSV files on S3

### Step B0 — Choose how the workspace reads S3

| Option | Use when | Effect |
|---|---|---|
| 1. Unity Catalog external location | An admin created an AWS storage credential (IAM role) and an external location for the bucket, and granted READ FILES | Auto Loader reads `s3://...` directly. No copy task. |
| 2. Access keys in a secret scope | You only have an access key and secret key | `s3_copy` copies new files into a UC volume, then Auto Loader reads the volume. |

Do not pass AWS keys as Auto Loader options and do not set `fs.s3a` keys in Spark config. Serverless does not accept those settings, and the `cloudFiles.awsAccessKey` options are for file-notification setup, not for reading data. Option 2 is the way to use keys.

Ask the user which option applies. If they do not know, use option 2.

### Step B1 — Folder tree

```text
<bundle>/
├── .gitignore
├── README.md
├── databricks.yml
├── configs/
│   └── sources/
│       ├── _template.yml                        # skipped by the loaders (starts with _)
│       └── <sor>_<table>.yml                    # one CSV pattern = one table
├── resources/
│   ├── jobs/
│   │   └── <sor>_files_to_silver.yml
│   └── pipelines/
│       ├── file_loader.pipeline.yml
│       └── staging_to_silver.pipeline.yml
└── src/
    ├── file_copy/                               # option 2 only
    │   ├── __init__.py                          # empty file
    │   └── s3_copy.py
    ├── python/
    │   └── run_s3_copy.py                       # option 2 only
    ├── pipelines/
    │   ├── file_loader/
    │   │   ├── axos_framework.py
    │   │   └── acquisition_file_pipeline.py
    │   └── staging_to_silver/
    │       ├── bronze.py
    │       └── silver.py
    └── utilities/
        └── schema_parser.py
```

`databricks.yml` for this pattern includes both folders from the start:

```yaml
include:
  - resources/jobs/*.yml
  - resources/pipelines/*.yml
```

### Step B2 — Code files

Create from the appendix, same path and content:

- `src/pipelines/file_loader/axos_framework.py` — config scan, validation, Auto Loader option mapping
- `src/pipelines/file_loader/acquisition_file_pipeline.py` — one Auto Loader streaming table per YAML. Adds `_staging_ingested_at`, `_source_file_path`, `_source_file_name`, `_source_file_size`, `_source_file_modified`. Writes `<pipeline.catalog>.staging_<sor>.<source_table>`.
- `src/pipelines/staging_to_silver/bronze.py` — streams each staging table to `<catalog>.bronze_<sor>.<source_table>`, fixes column names Delta cannot store, drops rows where every business column is NULL, adds `_bronze_ingested_at`
- `src/pipelines/staging_to_silver/silver.py` — same file as Pattern A
- `src/utilities/schema_parser.py` — same file as Pattern A
- Option 2 only: `src/file_copy/__init__.py` (empty), `src/file_copy/s3_copy.py`, `src/python/run_s3_copy.py`

CSV columns stay text. `axos_framework.autoloader_options` turns column type inference off for CSV. Leave it off. Types are set in silver later.

`s3_copy.py` lists the bucket under `prefix`, keeps keys whose file name matches `file_name_pattern`, and copies each into `/Volumes/<catalog>/staging_<sor>/landing/<source_table>/`. A file already there with the same size is skipped. Auto Loader remembers which files it read, so new data must arrive as new file names. A changed file with an old name is not read again.

### Step B3 — One YAML per CSV table

Create `configs/sources/_template.yml` with the template below, and `configs/sources/<sor>_<table>.yml` filled in. Both files are YAML lists with one item.

```yaml
- source_table: <table>
  sor: <sor>
  description: <what the file holds and how often it arrives>

  # --- S3 copy (option 2 only; delete this block for option 1) ---
  s3_copy:
    bucket: <bucket>
    region: <region>
    prefix: <prefix>/
    file_name_pattern: "<pattern>.csv"
    secret_scope: <scope>
    access_key_secret: <key name for the access key id>
    secret_key_secret: <key name for the secret access key>
    volume: <catalog>.staging_<sor>.landing

  # --- Auto Loader ---
  # option 2: the volume folder s3_copy writes to (must match exactly)
  # option 1: s3://<bucket>/<prefix>/
  source_path: /Volumes/<catalog>/staging_<sor>/landing/<table>/
  file_format: csv
  bronze_options:
    include_existing_files: true
    file_name_pattern: "<pattern>.csv"
    schema_evolution_mode: addNewColumns
    csv_options:
      header: true
      delimiter: ","

  # --- Layers ---
  staging_schema: staging_<sor>
  source_schema: bronze_<sor>
  target_schema: silver_<sor>

  # --- Silver (empty key list = silver skips this table) ---
  scd_2_key_list: []
  history_timestamp_source: pipeline_timestamp
  scd_type: 2
  data_quality_rules: ~
```

Rules for this file:

- Do not add `staging_catalog`, `source_catalog`, or `target_catalog_prefix`. The pipelines set the catalog from `pipeline.catalog`.
- `file_format: csv` needs a non-empty `csv_options`.
- For option 2, `source_path` must equal `/Volumes/<catalog>/staging_<sor>/landing/<source_table>/`. `s3_copy` stops with an error otherwise.
- Leave `scd_2_key_list: []` until the first load shows the real columns. Do not guess the key.

After the user confirms the key from the staging table, replace the silver block:

```yaml
  scd_2_key_list:
    - <key_column>
  history_timestamp_source: <timestamp_column>      # or pipeline_timestamp when the file has none
  history_timestamp_format: "yyyy-MM-dd HH:mm:ss"   # only when a timestamp column is used; match the file
  scd_type: 2
  scd_2_exclude_list:
    - <timestamp_column>
  data_quality_rules:
    - description: key_not_null
      expr: "`<key_column>` IS NOT NULL"
```

With `pipeline_timestamp`, versions are ordered by `_bronze_ingested_at`. With data quality rules, silver also writes a `<table>_qtn` quarantine table for rows that fail.

### Step B4 — Pipeline resources

`resources/pipelines/file_loader.pipeline.yml`:

```yaml
resources:
  pipelines:
    <sor>_file_loader_to_staging:
      name: <sor>_file_loader_to_staging
      catalog: ${var.catalog}
      schema: staging_<sor>
      serverless: true
      channel: CURRENT
      photon: true
      continuous: false
      configuration:
        catalog_env: dev
        pipeline.catalog: ${var.catalog}
        pipeline_config_dir: ${workspace.root_path}/files/configs/sources
        pipeline_source_dir: ${workspace.root_path}/files/src/pipelines/file_loader
      libraries:
        - file:
            path: ../../src/pipelines/file_loader/acquisition_file_pipeline.py
      environment:
        dependencies:
          - pyyaml
```

`axos_framework.py` is not listed as a library. The acquisition script imports it from `pipeline_source_dir`.

`resources/pipelines/staging_to_silver.pipeline.yml`:

```yaml
resources:
  pipelines:
    <sor>_staging_to_silver:
      name: <sor>_staging_to_silver
      catalog: ${var.catalog}
      schema: silver_<sor>
      serverless: true
      channel: CURRENT
      photon: true
      edition: ADVANCED
      continuous: false
      configuration:
        "pipelines.pipelineType": "WORKSPACE"
        pipeline.catalog: ${var.catalog}
        pipeline.project_root: ${workspace.root_path}/files
        pipeline.config_dir: ${workspace.root_path}/files/configs/sources
      libraries:
        - file:
            path: ../../src/pipelines/staging_to_silver/bronze.py
        - file:
            path: ../../src/pipelines/staging_to_silver/silver.py
      environment:
        dependencies:
          - pyyaml
```

### Step B5 — Job

`resources/jobs/<sor>_files_to_silver.yml` for option 2:

```yaml
resources:
  jobs:
    <sor>_files_to_silver:
      name: <sor>_files_to_silver
      environments:
        - environment_key: serverless_default
          spec:
            environment_version: "2"
            dependencies:
              - pyyaml
              - boto3
      tasks:
        - task_key: s3_copy
          environment_key: serverless_default
          spark_python_task:
            python_file: ../../src/python/run_s3_copy.py
            parameters:
              - "--config-dir"
              - "../../configs/sources"
        - task_key: file_loader_to_staging
          depends_on:
            - task_key: s3_copy
          pipeline_task:
            pipeline_id: ${resources.pipelines.<sor>_file_loader_to_staging.id}
            full_refresh: false
        - task_key: staging_to_silver
          depends_on:
            - task_key: file_loader_to_staging
          pipeline_task:
            pipeline_id: ${resources.pipelines.<sor>_staging_to_silver.id}
            full_refresh: false
```

For option 1, delete the `environments` block and the `s3_copy` task, and remove `depends_on` from `file_loader_to_staging`.

One job runs every CSV table in `configs/sources`. A new CSV table is a new YAML only.

### Step B6 — Check before handing to the user

There is no local config validator for this pattern. Check by hand:

- every YAML in `configs/sources` is a one-item list, has `source_table`, `sor`, `source_path`, `file_format`, `bronze_options.csv_options`, `staging_schema`, `source_schema`, `target_schema`
- every `source_table` is unique
- option 2: `source_path` equals the volume folder rule in Step B3
- no file contains a key value (search the folder for `AKIA` and for the secret)

Then:

```powershell
databricks bundle validate -t dev_sandbox --profile <cli_profile>
```

Stop. Hand off to the user with the commands in section 9.

### Step B7 — What the user should see

```sql
LIST '/Volumes/<catalog>/staging_<sor>/landing/<table>/';        -- option 2

SELECT _source_file_name, COUNT(*) AS rows
FROM <catalog>.staging_<sor>.<table>
GROUP BY _source_file_name;

SELECT COUNT(*) FROM <catalog>.bronze_<sor>.<table>;
```

Rows per file must equal the file's line count minus the header. Silver does not exist until `scd_2_key_list` is filled in and the job runs again.

---

## 9. Commands the user runs

From the bundle folder:

```powershell
python scripts/validate_config.py                                   # Pattern A only
databricks bundle validate -t dev_sandbox --profile <cli_profile>
databricks bundle deploy   -t dev_sandbox --profile <cli_profile>
databricks bundle run <job_resource_key> -t dev_sandbox --profile <cli_profile>
```

`<job_resource_key>` is the key under `resources.jobs` (for example `servicenow_cmn_schedule` or `datathon_files_to_silver`), not the display name.

In the workspace: **Workflows** (or **Jobs & Pipelines**) → search the table or SOR name → open the job → **Runs** → open the latest run. Each task is a box. A pipeline task box links to that pipeline's update graph.

---

## 10. Done criteria

| Pattern | Done when |
|---|---|
| A, bronze | Job succeeds. Bronze `COUNT(DISTINCT _pk)` equals the downstream system's key count, or every difference is explained. An incremental table's second run reads only changed records (often 0 or 1 page). |
| A, silver | Flat and silver tables exist. Current silver rows equal distinct keys. A second run with no source change adds no silver versions. Business columns match the downstream SQL table for every key. |
| B | Staging rows per file equal file lines minus header. Bronze equals staging. After the key is set, current silver rows equal distinct keys. |

---

## 11. Known problems and fixes

| Symptom | Cause | Fix |
|---|---|---|
| `Refresh token is invalid` from any CLI command | Login expired | `databricks auth login --host <url> --profile <profile>` |
| `UNEXPECTED_EOF_WHILE_READING` calling the API | Classic cluster egress blocked the TLS handshake | Use Serverless (the jobs above already do) |
| `Config validation failed: ... watermark_param` | Old validator that required the param | Use the `config.py` in the appendix. `N/A` is allowed. |
| API receives a literal `N/A=` parameter | Paginator not using `napp()` | Use the `pagination/__init__.py` in the appendix |
| `ModuleNotFoundError: databricks.sdk` on a laptop | `uc_proxy.py` imports the Databricks SDK | Harmless for `validate_config.py`. Install `databricks-sdk` only if you need to import the extractor locally. |
| Job fails on a task file that does not exist | A job references a script that was removed | Remove the task from the job YAML |
| Silver pipeline logs `skipped (no scd_2_key_list)` | Expected for bronze-only tables | Nothing |
| Silver logs `skipped (source table not found)` | Fan-out or bronze has not created the input table yet | Run the job once more after the first pipeline succeeds |
| Second job fails with "update already running" | Two jobs started the same pipeline | Use one parent job (Step A9.5) |
| Header total higher than rows loaded | Source access rules hide some rows | Reconcile on distinct keys |
| Silver new version on every load | Load metadata or timestamp is being compared | Put the timestamp, `_batch_id`, and `_load_ts` in `scd_2_exclude_list` |
| Auto Loader does not pick up a changed CSV | Same file name as an already-read file | Deliver new data as a new file name |

---

## Appendix — file contents

Create each file at the path in its heading, with exactly this content.

Empty files to create (no content): `src/api_loader/__init__.py` (Pattern A), `src/file_copy/__init__.py` (Pattern B option 2).

### Common

#### `.gitignore`

````gitignore
# Databricks CLI/extension
.databricks/
.bundle/

# Secrets — never commit
.env
.env.*
!.env.example

# Python
__pycache__/
*.pyc
.pytest_cache/
.venv/
venv/
*.egg-info/

# OS / editors
.DS_Store
Thumbs.db

# Local dumps
*.ndjson
````

### Pattern A — engine

#### `src/api_loader/bootstrap.py`

````python
"""Sandbox bootstrap: create the UC objects a source needs before its first run.

This is a prototyping stand-in for the Terraform chain in spec §4/§5 (UC HTTP
connection, external storage credential/location, external volume). It uses
MANAGED schemas/volumes on the catalog's default storage instead of EXTERNAL
ones, since we're not running Terraform against the sandbox yet. Swap this
for the Terraform resources in the spec once the source is ready to leave
the sandbox.
"""

from pyspark.sql import SparkSession

from .config import load_config
from .utils import quote_identifier

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  payload          VARIANT,
  _load_ts         TIMESTAMP,
  _batch_id        STRING,
  _source          STRING,
  _pk              STRING,
  _watermark_val   STRING,
  _snapshot_date   DATE,
  _corrupt_record  STRING
)
USING DELTA
"""


def bootstrap_source(config_path):
    cfg = load_config(config_path)
    spark = SparkSession.builder.getOrCreate()

    staging_catalog, staging_schema, volume_name = cfg["staging"]["volume"].split(".")
    bronze_catalog, bronze_schema, _ = cfg["bronze"]["table"].split(".")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{staging_catalog}`.`{staging_schema}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{bronze_catalog}`.`{bronze_schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{staging_catalog}`.`{staging_schema}`.`{volume_name}`")
    spark.sql(BRONZE_DDL.format(table=quote_identifier(cfg["bronze"]["table"])))

    print(f"Bootstrapped {cfg['source']['name']}: volume={cfg['staging']['volume']} table={cfg['bronze']['table']}")
````

#### `src/api_loader/config.py`

````python
"""Load + validate the per-source config (build spec §3).

Validation is deliberately dependency-free (no jsonschema) so it can run
identically inside a Databricks job, in a CI step, or on a laptop with just
PyYAML installed.
"""

import yaml

from .utils import napp

REQUIRED_TOP_LEVEL = [
    "source",
    "auth",
    "pagination",
    "incrementality",
    "primary_key",
    "rate_limit",
    "staging",
    "bronze",
]


class ConfigError(Exception):
    pass


def load_config(path):
    with open(path) as f:
        cfg = yaml.safe_load(f)
    validate_config(cfg)
    return cfg


def validate_config(cfg):
    errors = []

    for key in REQUIRED_TOP_LEVEL:
        if key not in cfg:
            errors.append(f"missing top-level key: {key}")
    if errors:
        raise ConfigError("Config validation failed:\n  - " + "\n  - ".join(errors))

    _validate_source(cfg["source"], errors)
    _validate_auth(cfg["auth"], errors)
    mode = _validate_incrementality(cfg["incrementality"], errors)
    _validate_pagination(cfg["pagination"], cfg["incrementality"], errors)

    if not cfg.get("primary_key"):
        errors.append("primary_key is required (list of field names)")

    _validate_rate_limit(cfg["rate_limit"], errors)
    _validate_staging(cfg["staging"], errors)
    _validate_bronze(cfg["bronze"], mode, errors)
    if "silver" in cfg:
        _validate_silver(cfg["silver"], errors)

    if errors:
        raise ConfigError("Config validation failed:\n  - " + "\n  - ".join(errors))


def _validate_source(source, errors):
    for field in ("name", "base_path"):
        if not source.get(field):
            errors.append(f"source.{field} is required")

    connectivity = source.get("connectivity")
    if connectivity not in ("direct", "uc_proxy"):
        errors.append("source.connectivity must be 'direct' or 'uc_proxy' (template extension, see README)")
    elif connectivity == "direct" and not napp(source.get("api_host")):
        errors.append("source.api_host is required when source.connectivity=direct")
    elif connectivity == "uc_proxy" and not napp(source.get("connection")):
        errors.append("source.connection is required when source.connectivity=uc_proxy")

    extra_params = source.get("extra_params")
    if extra_params is not None and not isinstance(extra_params, dict):
        errors.append("source.extra_params must be a dict of static query params, if present")


def _validate_auth(auth, errors):
    atype = auth.get("type")
    if atype not in ("oauth2_client_credentials", "bearer", "api_key", "basic", "none"):
        errors.append("auth.type must be one of: oauth2_client_credentials | bearer | api_key | basic | none")
    elif atype == "oauth2_client_credentials" and not auth.get("token_url"):
        errors.append("auth.token_url is required when auth.type=oauth2_client_credentials")


def _validate_pagination(pagination, incrementality, errors):
    ptype = pagination.get("type")
    if ptype not in ("offset_limit", "cursor", "link_header"):
        errors.append("pagination.type must be one of: offset_limit | cursor | link_header")
        return

    max_pages = pagination.get("max_pages")
    if max_pages is not None and (not isinstance(max_pages, int) or max_pages < 1):
        errors.append("pagination.max_pages must be a positive integer, if present")

    if ptype == "offset_limit":
        for field in ("page_size", "offset_param", "limit_param"):
            if field not in pagination:
                errors.append(f"pagination.{field} is required when pagination.type=offset_limit")
    elif ptype == "cursor":
        for field in ("cursor_param", "cursor_json_path"):
            if field not in pagination:
                errors.append(f"pagination.{field} is required when pagination.type=cursor")
    # link_header needs no extra params (follows the Link response header)


def _validate_incrementality(incrementality, errors):
    mode = incrementality.get("mode")
    if mode not in ("watermark", "full_only"):
        errors.append("incrementality.mode must be 'watermark' or 'full_only'")
        return mode

    if mode == "watermark":
        if not napp(incrementality.get("watermark_field")):
            errors.append("incrementality.watermark_field is required when mode=watermark")
        # watermark_param is optional. N/A means ServiceNow sysparm_query in extractor
        # (not GitHub ?since=). See extractor.apply_servicenow_sysparm_query.
    return mode


def _validate_rate_limit(rate_limit, errors):
    for field in ("respect_retry_after", "max_retries", "backoff"):
        if field not in rate_limit:
            errors.append(f"rate_limit.{field} is required")
    if rate_limit.get("backoff") not in (None, "exponential"):
        errors.append("rate_limit.backoff must be 'exponential' (only strategy implemented)")


def _validate_staging(staging, errors):
    volume = staging.get("volume")
    if not volume or volume.count(".") != 2:
        errors.append("staging.volume must be 'catalog.schema.volume'")


def _validate_bronze(bronze, mode, errors):
    table = bronze.get("table")
    if not table or table.count(".") != 2:
        errors.append("bronze.table must be 'catalog.schema.table'")

    if "snapshot_partition" not in bronze:
        errors.append("bronze.snapshot_partition is required (true/false)")
    elif mode == "full_only" and not bronze["snapshot_partition"]:
        errors.append("bronze.snapshot_partition must be true when incrementality.mode=full_only")
    elif mode == "watermark" and bronze["snapshot_partition"]:
        errors.append("bronze.snapshot_partition must be false when incrementality.mode=watermark")


def _validate_silver(silver, errors):
    """Optional. Absent means bronze-only. Present means a current-state typed table."""
    table = silver.get("table")
    if not table or table.count(".") != 2:
        errors.append("silver.table must be 'catalog.schema.table'")
    columns = silver.get("columns")
    if not isinstance(columns, list) or not columns:
        errors.append("silver.columns is required (list of payload field names) when silver is set")
        return
    for name in columns:
        if not isinstance(name, str) or not name.replace("_", "").isalnum():
            errors.append(f"silver.columns entry must be a simple field name, got {name!r}")
````

#### `src/api_loader/extractor.py`

````python
"""Extractor: API -> NDJSON in the staging volume. Never touches bronze. (spec §8)"""

import datetime as dt
import json
import os
import uuid

from .auth import build_auth
from .config import load_config
from .pagination import build_paginator
from .retry import request_with_backoff
from .connectivity import build_transport
from .utils import get_json_path, napp


def volume_path(cfg):
    catalog, schema, volume = cfg["staging"]["volume"].split(".")
    return f"/Volumes/{catalog}/{schema}/{volume}"


def extract_records(response, records_path=None):
    body = response.json()
    if records_path:
        body = get_json_path(body, records_path)
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        return [body]
    return []


def write_ndjson(path, records):
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def apply_servicenow_sysparm_query(params, cfg, watermark):
    """Walkthrough 3C: Table API date filter is sysparm_query, not ?since=.

    When incrementality.mode is watermark and watermark_param is N/A:
    - watermark set  -> sysparm_query=sys_updated_on><wm>^ORDERBYsys_id
    - watermark empty -> no date filter (full backfill), keep ORDERBYsys_id
    Does not change full_only sources (e.g. cmn_schedule extra_params).
    """
    inc = cfg["incrementality"]
    if inc.get("mode") != "watermark":
        return
    if napp(inc.get("watermark_param")):
        return
    order = "ORDERBYsys_id"
    extra = cfg["source"].get("extra_params") or {}
    if watermark:
        params["sysparm_query"] = f"sys_updated_on>{watermark}^{order}"
    else:
        params["sysparm_query"] = extra.get("sysparm_query") or order


def extract(config_path, watermark=None):
    cfg = load_config(config_path)

    auth = build_auth(cfg)
    transport = build_transport(cfg, auth)
    paginator = build_paginator(cfg)

    records_path = napp(cfg.get("response", {}).get("records_path"))
    rate_cfg = cfg["rate_limit"]
    is_watermark_mode = cfg["incrementality"]["mode"] == "watermark"
    watermark_field = cfg["incrementality"].get("watermark_field") if is_watermark_mode else None

    batch_id = f"{dt.datetime.utcnow():%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:6]}"
    batch_dir = os.path.join(volume_path(cfg), f"batch={batch_id}")
    os.makedirs(batch_dir, exist_ok=True)

    params = paginator.initial_params(watermark)
    params.update(cfg["source"].get("extra_params") or {})
    apply_servicenow_sysparm_query(params, cfg, watermark)
    part = 0
    max_watermark = watermark
    base_path = cfg["source"]["base_path"]
    max_pages = cfg["pagination"].get("max_pages")
    pages_fetched = 0

    while True:
        next_url = params.get("__next_url__")
        target = next_url if next_url else base_path
        request_params = None if next_url else params

        resp = request_with_backoff(
            transport,
            "GET",
            target,
            request_params,
            max_retries=rate_cfg["max_retries"],
            respect_retry_after=rate_cfg["respect_retry_after"],
        )
        pages_fetched += 1

        records = extract_records(resp, records_path)
        if records:
            part += 1
            write_ndjson(os.path.join(batch_dir, f"part-{part:04d}.ndjson"), records)

            if is_watermark_mode:
                for record in records:
                    val = get_json_path(record, watermark_field)
                    if val is not None and (max_watermark is None or str(val) > str(max_watermark)):
                        max_watermark = val

        if max_pages and pages_fetched >= max_pages:
            break

        next_params = paginator.next_params(resp, params)
        if next_params is None:
            break
        params = next_params

    return {
        "batch_id": batch_id,
        "records_written": part,
        "max_watermark": max_watermark,
    }
````

#### `src/api_loader/promoter.py`

````python
"""Promoter: NDJSON in the staging volume -> bronze Delta table. Never calls the API. (spec §9)"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, expr, lit

from .config import load_config
from .extractor import volume_path
from .utils import quote_identifier


def promote(config_path, batch_id):
    cfg = load_config(config_path)
    spark = SparkSession.builder.getOrCreate()

    src = f"{volume_path(cfg)}/batch={batch_id}"  # scope to this batch only — a failed/cancelled
    ckpt = f"{src}/_checkpoints"                    # prior batch's leftover files never get read/promoted
    is_full = cfg["incrementality"]["mode"] == "full_only"
    pk_field = cfg["primary_key"][0]  # TODO: composite keys if a source needs them

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("singleVariantColumn", "payload")
        .option("cloudFiles.schemaLocation", ckpt)
        .option("cloudFiles.partitionColumns", "")  # batch=<id> in the path is a file-org convention, not a table partition
        .load(src)
    )

    watermark_col = (
        lit(None).cast("string")
        if is_full
        else expr(f"payload:{cfg['incrementality']['watermark_field']}").cast("string")
    )

    enriched = (
        stream.withColumn("_load_ts", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_source", lit(cfg["source"]["name"]))
        .withColumn("_pk", expr(f"payload:{pk_field}").cast("string"))
        .withColumn("_watermark_val", watermark_col)
        .withColumn("_snapshot_date", current_date() if is_full else lit(None).cast("date"))
        .withColumn("_corrupt_record", lit(None).cast("string"))
    )

    query = (
        enriched.writeStream.option("checkpointLocation", ckpt)
        .outputMode("append")  # append-only, always — no UPDATE/MERGE/overwrite into bronze
        .trigger(availableNow=True)  # batch-style: process what's there, then stop
        .toTable(quote_identifier(cfg["bronze"]["table"]))
    )
    query.awaitTermination()
    return query
````

#### `src/api_loader/retry.py`

````python
"""Rate limiting / backoff (build spec §8.1). Always on, even for sequential pulls."""

import random
import time


def request_with_backoff(transport, method, target, params, max_retries, respect_retry_after=True):
    attempt = 0
    while True:
        resp = transport.request(method, target, params=params)

        if resp.status_code == 429 or resp.status_code >= 500:
            attempt += 1
            if attempt > max_retries:
                resp.raise_for_status()

            delay = None
            if respect_retry_after and "Retry-After" in resp.headers:
                try:
                    delay = float(resp.headers["Retry-After"])
                except ValueError:
                    delay = None
            if delay is None:
                delay = (2 ** attempt) + random.uniform(0, 1)

            time.sleep(delay)
            continue

        resp.raise_for_status()
        return resp
````

#### `src/api_loader/secrets.py`

````python
"""Secret retrieval for Path A (direct transport).

Reads from a Databricks secret scope when running on a cluster/job. Falls
back to an environment variable when running locally (laptop) so pagination/
auth logic can be unit-tested without a Databricks runtime.

Naming standard note (spec §10): secret scope naming should follow the
project's standard once this leaves the sandbox. For now, the scope name
comes straight from the source config (source.secret_scope).
"""

import os


def get_secret(scope, key):
    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        env_key = f"{scope}__{key}".upper().replace("-", "_")
        val = os.environ.get(env_key)
        if val is None:
            raise RuntimeError(
                f"Secret {scope}/{key} not found via dbutils, and env var "
                f"{env_key} is not set for local fallback."
            )
        return val
````

#### `src/api_loader/utils.py`

````python
"""Small shared helpers used across the loader."""


def napp(value):
    """Convert the config convention of literal 'N/A' strings to None.

    The build spec requires every config field to be declared or explicitly
    'N/A' (no silent defaults). This turns that convention into a real
    optional value wherever the code reads it.
    """
    if isinstance(value, str) and value.strip().upper() == "N/A":
        return None
    return value


def get_json_path(obj, dotted_path):
    """Read a dotted path (e.g. 'meta.next_cursor') out of a nested dict."""
    if not dotted_path:
        return None
    cur = obj
    for part in dotted_path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def quote_identifier(dotted_name):
    """Backtick-quote each part of a catalog.schema.object name so hyphens
    (e.g. catalogs with hyphenated names) don't break SQL."""
    return ".".join(f"`{part}`" for part in dotted_name.split("."))
````

#### `src/api_loader/auth/__init__.py`

````python
from ..utils import napp
from .api_key import ApiKeyAuth
from .basic import BasicAuth
from .bearer import BearerAuth
from .oauth2_client_credentials import OAuth2ClientCredentialsAuth


def build_auth(cfg):
    """Factory: config -> AuthStrategy instance (or None for auth.type=none)."""
    auth_cfg = cfg["auth"]
    atype = auth_cfg["type"]

    if atype == "none":
        return None

    secret_scope = napp(cfg["source"].get("secret_scope"))

    if atype == "basic":
        return BasicAuth(
            secret_scope,
            auth_cfg.get("username_secret_key", "snow-username"),
            auth_cfg.get("password_secret_key", "snow-password"),
        )

    if atype == "bearer":
        return BearerAuth(secret_scope, auth_cfg.get("secret_key", "token"))

    if atype == "api_key":
        return ApiKeyAuth(
            secret_scope,
            auth_cfg.get("secret_key", "api_key"),
            header_name=auth_cfg.get("header_name", "X-Api-Key"),
        )

    if atype == "oauth2_client_credentials":
        return OAuth2ClientCredentialsAuth(
            token_url=auth_cfg["token_url"],
            secret_scope=secret_scope,
            client_id_secret=auth_cfg.get("client_id_secret", "client_id"),
            client_secret_secret=auth_cfg.get("client_secret_secret", "client_secret"),
            scope=napp(auth_cfg.get("scope")),
            client_id_header=napp(auth_cfg.get("client_id_header")),
        )

    raise ValueError(f"unsupported auth.type: {atype}")
````

#### `src/api_loader/auth/api_key.py`

````python
from ..secrets import get_secret
from .base import AuthStrategy


class ApiKeyAuth(AuthStrategy):
    """Static API key sent as a custom header."""

    def __init__(self, secret_scope, secret_key, header_name="X-Api-Key"):
        self._scope = secret_scope
        self._key = secret_key
        self._header_name = header_name

    def headers(self):
        return {self._header_name: get_secret(self._scope, self._key)}
````

#### `src/api_loader/auth/base.py`

````python
from abc import ABC, abstractmethod


class AuthStrategy(ABC):
    @abstractmethod
    def headers(self) -> dict:
        """Return the headers to attach to every request (token refresh happens here)."""
        raise NotImplementedError
````

#### `src/api_loader/auth/basic.py`

````python
import base64

from ..secrets import get_secret
from .base import AuthStrategy


class BasicAuth(AuthStrategy):
    """HTTP Basic auth. Documented exception for ServiceNow Table API.

    The api-loader template ships bearer / api_key / oauth2 only. ServiceNow
    uses Basic. Username and password stay in a secret scope.
    """

    def __init__(self, secret_scope, username_key, password_key):
        self._scope = secret_scope
        self._username_key = username_key
        self._password_key = password_key

    def headers(self):
        user = get_secret(self._scope, self._username_key)
        password = get_secret(self._scope, self._password_key)
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}", "Accept": "application/json"}
````

#### `src/api_loader/auth/bearer.py`

````python
from ..secrets import get_secret
from .base import AuthStrategy


class BearerAuth(AuthStrategy):
    """Static bearer token (e.g. a GitHub PAT) read from a secret scope."""

    def __init__(self, secret_scope, secret_key):
        self._scope = secret_scope
        self._key = secret_key

    def headers(self):
        token = get_secret(self._scope, self._key)
        return {"Authorization": f"Bearer {token}"}
````

#### `src/api_loader/auth/oauth2_client_credentials.py`

````python
import time

import requests

from ..secrets import get_secret
from .base import AuthStrategy


class OAuth2ClientCredentialsAuth(AuthStrategy):
    """OAuth2 client-credentials (M2M) flow, e.g. Twitch Helix app access tokens.

    Under Path B (uc_proxy) Databricks manages this token exchange for you via
    the UC connection. This implementation is for Path A (direct transport) —
    the extractor fetches and caches the token itself, refreshing shortly
    before expiry.
    """

    def __init__(
        self,
        token_url,
        secret_scope,
        client_id_secret="client_id",
        client_secret_secret="client_secret",
        scope=None,
        client_id_header=None,
    ):
        self._token_url = token_url
        self._client_id = get_secret(secret_scope, client_id_secret)
        self._client_secret = get_secret(secret_scope, client_secret_secret)
        self._scope = scope
        self._client_id_header = client_id_header
        self._token = None
        self._expires_at = 0

    def _refresh(self):
        data = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "grant_type": "client_credentials",
        }
        if self._scope:
            data["scope"] = self._scope

        resp = requests.post(self._token_url, data=data, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        self._token = payload["access_token"]
        # Refresh a minute early to avoid racing the expiry.
        self._expires_at = time.time() + payload.get("expires_in", 3600) - 60

    def headers(self):
        if self._token is None or time.time() >= self._expires_at:
            self._refresh()

        hdrs = {"Authorization": f"Bearer {self._token}"}
        if self._client_id_header:
            hdrs[self._client_id_header] = self._client_id
        return hdrs
````

#### `src/api_loader/connectivity/__init__.py`

````python
from ..utils import napp
from .direct import DirectTransport
from .uc_proxy import UCProxyTransport


def build_transport(cfg, auth):
    """Factory: config -> Transport instance, per source.connectivity."""
    source = cfg["source"]
    connectivity_type = source.get("connectivity")

    if connectivity_type == "direct":
        return DirectTransport(api_host=napp(source["api_host"]), auth=auth)

    if connectivity_type == "uc_proxy":
        return UCProxyTransport(connection_name=napp(source["connection"]))

    raise ValueError(f"unsupported source.connectivity: {connectivity_type}")
````

#### `src/api_loader/connectivity/base.py`

````python
from abc import ABC, abstractmethod


class Transport(ABC):
    @abstractmethod
    def request(self, method, target, params=None):
        """Issue a request and return a requests.Response.

        `target` is either a path (joined with the transport's base host) or,
        for link_header pagination, a full URL to call as-is.
        """
        raise NotImplementedError
````

#### `src/api_loader/connectivity/direct.py`

````python
import requests

from .base import Transport


class DirectTransport(Transport):
    """Path A: call the API directly, auth headers built from a secret scope.

    This is the sandbox-prototype transport, used while UC HTTP Connections
    (Path B / uc_proxy) haven't been validated in this workspace (see README
    and build spec §1 and §4).
    """

    def __init__(self, api_host, auth):
        self.api_host = api_host.rstrip("/")
        self.auth = auth

    def request(self, method, target, params=None):
        headers = self.auth.headers() if self.auth else {}
        url = target if target.startswith("http") else f"{self.api_host}{target}"
        return requests.request(method, url, params=params, headers=headers, timeout=30)
````

#### `src/api_loader/connectivity/uc_proxy.py`

````python
import requests
from databricks.sdk import WorkspaceClient

from .base import Transport


class UCProxyTransport(Transport):
    """Path B: call the API through the UC HTTP Connection proxy (spec §4/§8).

    Confirmed working in the sandbox against the REST proxy endpoint
    (f"{host}/api/2.0/unity-catalog/connections/{connection}/proxy{target}"),
    authenticated with the Databricks identity — source credentials are
    injected by the proxy and never seen by this code. Returns a real
    requests.Response, so response headers (e.g. GitHub's Link) pass through
    unlike the alternative `http_request()` SQL function, which does not
    expose them.
    """

    def __init__(self, connection_name):
        self.connection_name = connection_name
        self.w = WorkspaceClient()

    def request(self, method, target, params=None):
        url = f"{self.w.config.host}/api/2.0/unity-catalog/connections/{self.connection_name}/proxy{target}"
        headers = self.w.config.authenticate()
        return requests.request(method, url, params=params, headers=headers, timeout=30)
````

#### `src/api_loader/pagination/__init__.py`

````python
from ..utils import napp
from .cursor import CursorPaginator
from .link_header import LinkHeaderPaginator
from .offset_limit import OffsetLimitPaginator


def build_paginator(cfg):
    """Factory: config -> Paginator instance, per pagination.type (spec §8.2)."""
    pagination = cfg["pagination"]
    ptype = pagination["type"]
    watermark_param = napp(cfg["incrementality"].get("watermark_param"))

    if ptype == "offset_limit":
        return OffsetLimitPaginator(
            offset_param=pagination["offset_param"],
            limit_param=pagination["limit_param"],
            page_size=pagination["page_size"],
            watermark_param=watermark_param,
            records_path=napp(cfg.get("response", {}).get("records_path")),
        )

    if ptype == "cursor":
        return CursorPaginator(
            cursor_param=pagination["cursor_param"],
            cursor_json_path=pagination["cursor_json_path"],
            watermark_param=watermark_param,
            page_size_param=pagination.get("page_size_param"),
            page_size=pagination.get("page_size"),
        )

    if ptype == "link_header":
        return LinkHeaderPaginator(watermark_param=watermark_param)

    raise ValueError(f"unsupported pagination.type: {ptype}")
````

#### `src/api_loader/pagination/base.py`

````python
from abc import ABC, abstractmethod


class Paginator(ABC):
    @abstractmethod
    def initial_params(self, watermark=None) -> dict:
        """Query params for the first request."""
        raise NotImplementedError

    @abstractmethod
    def next_params(self, response, current_params):
        """Query params for the next request, or None to stop."""
        raise NotImplementedError
````

#### `src/api_loader/pagination/cursor.py`

````python
from ..utils import get_json_path
from .base import Paginator


class CursorPaginator(Paginator):
    """Read the next cursor from cursor_json_path; stop when absent (spec §8.2)."""

    def __init__(self, cursor_param, cursor_json_path, watermark_param=None, page_size_param=None, page_size=None):
        self.cursor_param = cursor_param
        self.cursor_json_path = cursor_json_path
        self.watermark_param = watermark_param
        self.page_size_param = page_size_param
        self.page_size = page_size

    def initial_params(self, watermark=None):
        params = {}
        if self.page_size_param and self.page_size:
            params[self.page_size_param] = self.page_size
        if watermark and self.watermark_param:
            params[self.watermark_param] = watermark
        return params

    def next_params(self, response, current_params):
        body = response.json()
        cursor = get_json_path(body, self.cursor_json_path)
        if not cursor:
            return None

        next_params = dict(current_params)
        next_params[self.cursor_param] = cursor
        return next_params
````

#### `src/api_loader/pagination/link_header.py`

````python
from .base import Paginator


class LinkHeaderPaginator(Paginator):
    """Follow rel="next" from the Link response header; stop when absent (spec §8.2).

    The extractor recognizes the special "__next_url__" key and, when present,
    calls that full URL directly instead of base_path + params.
    """

    def __init__(self, watermark_param=None):
        self.watermark_param = watermark_param

    def initial_params(self, watermark=None):
        params = {}
        if watermark and self.watermark_param:
            params[self.watermark_param] = watermark
        return params

    def next_params(self, response, current_params):
        next_url = response.links.get("next", {}).get("url")
        if not next_url:
            return None
        return {"__next_url__": next_url}
````

#### `src/api_loader/pagination/offset_limit.py`

````python
from .base import Paginator


class OffsetLimitPaginator(Paginator):
    """Increment offset_param by page_size until a short/empty page (spec §8.2)."""

    def __init__(self, offset_param, limit_param, page_size, watermark_param=None, records_path=None):
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.page_size = page_size
        self.watermark_param = watermark_param
        self.records_path = records_path

    def _records(self, response):
        body = response.json()
        if isinstance(body, list):
            return body
        if isinstance(body, dict) and self.records_path:
            value = body.get(self.records_path, [])
            return value if isinstance(value, list) else []
        return []

    def initial_params(self, watermark=None):
        params = {self.offset_param: 0, self.limit_param: self.page_size}
        if watermark and self.watermark_param:
            params[self.watermark_param] = watermark
        return params

    def next_params(self, response, current_params):
        records = self._records(response)
        if len(records) < self.page_size:
            return None

        next_params = dict(current_params)
        next_params[self.offset_param] = current_params[self.offset_param] + self.page_size
        return next_params
````

#### `src/python/run_bootstrap.py`

````python
"""DAB task entrypoint: create UC schema/volume/bronze table for a source."""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), ".."))

from api_loader.bootstrap import bootstrap_source  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    bootstrap_source(args.config)
````

#### `src/python/run_extract.py`

````python
"""DAB task entrypoint: run the extractor and pass batch_id/watermark downstream."""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), ".."))

from api_loader.config import load_config  # noqa: E402
from api_loader.extractor import extract  # noqa: E402
from api_loader.utils import quote_identifier  # noqa: E402


def set_task_value(key, value):
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        pass  # running outside a Databricks job (e.g. local test) — no-op


def get_last_watermark(cfg):
    """Read the highest _watermark_val already promoted to bronze (mode=watermark only).

    The bronze table always exists by the time extract runs (bootstrap creates
    it first), so an empty table just yields NULL -> None (full history).
    """
    if cfg["incrementality"]["mode"] != "watermark":
        return None
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        table = quote_identifier(cfg["bronze"]["table"])
        row = spark.sql(f"SELECT MAX(_watermark_val) AS wm FROM {table}").collect()[0]
        return row["wm"]
    except Exception:
        return None  # running outside a Databricks job (e.g. local test) — no-op


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--watermark", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    watermark = args.watermark or get_last_watermark(cfg)

    result = extract(args.config, watermark=watermark)
    print(f"Extract result: {json.dumps(result, default=str)}")

    set_task_value("batch_id", result["batch_id"])
    set_task_value("max_watermark", result["max_watermark"])
````

#### `src/python/run_promote.py`

````python
"""DAB task entrypoint: run the promoter for a given batch_id."""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), ".."))

from api_loader.promoter import promote  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()

    promote(args.config, args.batch_id)
    print(f"Promoted batch {args.batch_id} for config {args.config}")
````

#### `scripts/validate_config.py`

````python
#!/usr/bin/env python3
"""CI gate (spec §3): validate every source config, or specific ones given as args.

Usage:
    python scripts/validate_config.py                              # validate all configs/sources/*.yml
    python scripts/validate_config.py configs/sources/twitch_streams.yml
"""

import glob
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from api_loader.config import ConfigError, load_config  # noqa: E402


def main():
    paths = sys.argv[1:] or sorted(
        glob.glob(os.path.join(os.path.dirname(__file__), "..", "configs", "sources", "*.yml"))
    )

    failed = False
    for path in paths:
        try:
            load_config(path)
            print(f"OK   {path}")
        except ConfigError as e:
            failed = True
            print(f"FAIL {path}\n{e}")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
````

### Pattern A — examples

#### `configs/sources/servicenow_change_request.yml`

Incremental example. Remove `max_pages` before the real historical load.

````yaml
# Incremental change_request (walkthrough 3C). Not the C# rolling 5-day BETWEEN.
# watermark_param is N/A: extractor sets sysparm_query=sys_updated_on><wm>^ORDERBYsys_id
# when bronze has a watermark; empty watermark = full backfill (ORDERBYsys_id only).
#
# max_pages is a sandbox cap. REMOVE it before a real historical backfill.

source:
  name: servicenow_change_request
  connectivity: direct
  connection: N/A
  api_host: https://bofi.service-now.com
  base_path: /api/now/table/change_request
  secret_scope: servicenow
  extra_params:
    sysparm_query: ORDERBYsys_id

auth:
  type: basic
  username_secret_key: snow-username
  password_secret_key: snow-password

pagination:
  type: offset_limit
  page_size: 1000
  offset_param: sysparm_offset
  limit_param: sysparm_limit
  max_pages: 5

incrementality:
  mode: watermark
  watermark_field: sys_updated_on
  watermark_param: N/A

primary_key: [sys_id]

response:
  records_path: result

rate_limit:
  respect_retry_after: true
  max_retries: 5
  backoff: exponential

staging:
  volume: enterprise_dev.staging_servicenow.change_request

bronze:
  table: enterprise_dev.bronze_servicenow.change_request_raw
  snapshot_partition: false
````

#### `resources/jobs/servicenow_cmn_schedule.yml`

Proven job with silver attached. Pipeline keys here are `servicenow_raw_to_flat` and `servicenow_staging_to_silver`.

````yaml
resources:
  jobs:
    servicenow_cmn_schedule:
      name: api_loader_servicenow_cmn_schedule
      environments:
        - environment_key: serverless_default
          spec:
            environment_version: "2"
            dependencies:
              - pyyaml
              - requests
      tasks:
        - task_key: bootstrap
          environment_key: serverless_default
          spark_python_task:
            python_file: ../../src/python/run_bootstrap.py
            parameters:
              - "--config"
              - "../../configs/sources/servicenow_cmn_schedule.yml"
        - task_key: extract
          environment_key: serverless_default
          depends_on:
            - task_key: bootstrap
          spark_python_task:
            python_file: ../../src/python/run_extract.py
            parameters:
              - "--config"
              - "../../configs/sources/servicenow_cmn_schedule.yml"
        - task_key: promote
          environment_key: serverless_default
          depends_on:
            - task_key: extract
          spark_python_task:
            python_file: ../../src/python/run_promote.py
            parameters:
              - "--config"
              - "../../configs/sources/servicenow_cmn_schedule.yml"
              - "--batch-id"
              - "{{tasks.extract.values.batch_id}}"
        - task_key: raw_to_flat
          depends_on:
            - task_key: promote
          pipeline_task:
            pipeline_id: ${resources.pipelines.servicenow_raw_to_flat.id}
            full_refresh: false
        - task_key: staging_to_silver
          depends_on:
            - task_key: raw_to_flat
          pipeline_task:
            pipeline_id: ${resources.pipelines.servicenow_staging_to_silver.id}
            full_refresh: false
````

### Both patterns — flatten and silver

#### `src/pipelines/raw_to_flat/fanout.py`

Pattern A only.

````python
"""Fan-out: VARIANT bronze *_raw -> typed *_flat.

Reads configs/sources/*.yml. A file is included only when it has scd_2_key_list.
Projects each name in columns from payload, and copies _batch_id and _load_ts.
"""

import glob
import os
import re

import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F

_CONFIG_DIR = spark.conf.get("pipeline.config_dir", None)
if not _CONFIG_DIR or not os.path.isdir(_CONFIG_DIR):
    raise FileNotFoundError(
        f"pipeline.config_dir is not a directory: {_CONFIG_DIR!r}"
    )

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _load_source(path):
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if isinstance(data, list):
        return data[0]
    return data or {}


def _require_ident(value, label):
    if not isinstance(value, str) or not _IDENT.match(value):
        raise ValueError(f"{label} must be a simple identifier, got {value!r}")
    return value


def _flat_name(cfg):
    catalog = _require_ident(cfg.get("source_catalog"), "source_catalog")
    schema = _require_ident(cfg.get("source_schema"), "source_schema")
    table = _require_ident(cfg.get("source_table"), "source_table")
    return f"{catalog}.{schema}.{table}"


def _raw_name(cfg):
    raw = cfg.get("bronze", {}).get("table")
    if not isinstance(raw, str):
        raise ValueError("bronze.table is required")
    parts = raw.split(".")
    if len(parts) != 3:
        raise ValueError(f"bronze.table must be catalog.schema.table, got {raw!r}")
    return ".".join(_require_ident(part, "bronze.table") for part in parts)


def _columns(cfg):
    columns = cfg.get("columns")
    if not isinstance(columns, list) or not columns:
        raise ValueError("columns must be a non-empty list")
    return [_require_ident(name, "columns") for name in columns]


def _project(raw_df, columns, keys):
    if "_corrupt_record" in raw_df.columns:
        raw_df = raw_df.filter(F.col("_corrupt_record").isNull())
    projected = [
        F.expr(f"variant_get(payload, '$.{name}', 'string')").alias(name)
        for name in columns
    ]
    projected.append(F.col("_batch_id"))
    projected.append(F.col("_load_ts"))
    flat = raw_df.select(*projected)
    for key in keys:
        if key in columns:
            flat = flat.filter(F.col(key).isNotNull())
    return flat


_SOURCES = []
for _path in sorted(glob.glob(os.path.join(_CONFIG_DIR, "*.yml"))):
    _cfg = _load_source(_path)
    if _cfg.get("scd_2_key_list"):
        _SOURCES.append(_cfg)

if not _SOURCES:
    raise ValueError(f"No configs with scd_2_key_list in {_CONFIG_DIR}")


for _cfg in _SOURCES:
    _flat = _flat_name(_cfg)
    _raw = _raw_name(_cfg)
    _cols = _columns(_cfg)
    _keys = _cfg["scd_2_key_list"]
    if isinstance(_keys, str):
        _keys = [part.strip() for part in _keys.split(",") if part.strip()]
    _flow_name = f"{_cfg['source_table']}_fanout"

    dp.create_streaming_table(
        name=_flat,
        comment=f"Typed columns projected from {_raw}",
        table_properties={
            "quality": "bronze",
            "pipelines.autoOptimize.managed": "true",
            "ingestion.source_table": _raw,
        },
    )

    @dp.append_flow(target=_flat, name=_flow_name)
    def _flow(raw_table=_raw, columns=_cols, keys=_keys):
        return _project(spark.readStream.table(raw_table), columns, keys)
````

#### `src/pipelines/staging_to_silver/silver.py`

Used by both patterns. Adapted from the approved TCI / Jack Henry silver: API YAMLs without `source_table` are skipped by name.

````python
"""
silver.py
================================================================================
Config-Driven Silver Layer — Lakeflow Spark Declarative Pipelines (ServiceNow)
================================================================================

PURPOSE: Reads typed bronze tables, applies SCD Type 2 via apply_changes.
         Copied from the TCI bundle. Config folder is configs/sources.
         A YAML is used only when it has scd_2_key_list.

PIPELINE: staging_to_silver
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
        f"No .yml files found in '{_CONFIG_DIR}'."
    )

def _load_source(path: str) -> dict:
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if isinstance(data, list):
        return data[0]
    return data

_SOURCES_ALL = [_load_source(f) for f in _YAML_FILES]

# pipeline_group filter ─────────────────────────────────────────────────────
# If pipeline.table_group is set (e.g. "large", "xlarge"), only process
# configs whose `pipeline_group` field matches.
# If not set, process only UNTAGGED configs (excludes group-specific tables).
_TABLE_GROUP = spark.conf.get("pipeline.table_group", None)

if _TABLE_GROUP:
    _SOURCES  = [s for s in _SOURCES_ALL if s.get("pipeline_group") == _TABLE_GROUP]
    _YAML_FILES = [f for f, s in zip(_YAML_FILES, _SOURCES_ALL)
                   if s.get("pipeline_group") == _TABLE_GROUP]
    if not _SOURCES:
        raise ValueError(
            f"pipeline.table_group='{_TABLE_GROUP}' matched no configs in '{_CONFIG_DIR}'."
        )
else:
    _SOURCES = [s for s in _SOURCES_ALL if not s.get("pipeline_group")]
    _YAML_FILES = [f for f, s in zip(_YAML_FILES, _SOURCES_ALL)
                   if not s.get("pipeline_group")]

def _config_label(src: dict) -> str:
    """Name used in skip logs. API YAMLs have source.name until SCD keys are added."""
    if src.get("source_table"):
        return src["source_table"]
    source = src.get("source")
    if isinstance(source, dict) and source.get("name"):
        return source["name"]
    return "<unnamed>"


# Skip stub configs not yet configured for silver (empty scd_2_key_list) ─────
_pairs   = [(s, f) for s, f in zip(_SOURCES, _YAML_FILES) if s.get("scd_2_key_list")]
_skipped = [_config_label(s) for s in _SOURCES if not s.get("scd_2_key_list")]
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
        _s["source_catalog"] = _CATALOG
        _s["target_catalog_prefix"] = _CATALOG


# ─────────────────────────────────────────────────────────────────────────────
# Schema defaults — derive from sor field when explicit fields not present
# ─────────────────────────────────────────────────────────────────────────────

for _s in _SOURCES:
    _sor = _s.get("sor", "tci")
    _s.setdefault("source_catalog", _CATALOG)
    _s.setdefault("source_schema", f"bronze_{_sor}")
    _s.setdefault("target_catalog_prefix", _CATALOG)
    _s.setdefault("target_schema", f"silver_{_sor}")


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
# Filter out configs whose source (bronze) table does not exist yet
#
# Uses information_schema with fallback: if the query fails during graph
# loading (serverless SDP PREVIEW), all configs pass through unconditionally.
# ─────────────────────────────────────────────────────────────────────────────

def _get_existing_source_tables() -> set:
    """Fetch all table names in each distinct source schema in one pass."""
    schema_groups = {}
    for s in _SOURCES:
        key = (s["source_catalog"], s["source_schema"])
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

_EXISTING_SOURCES = _get_existing_source_tables()

if _EXISTING_SOURCES:
    def _source_table_exists(src: dict) -> bool:
        return (src["source_catalog"], src["source_schema"], src["source_table"]) in _EXISTING_SOURCES

    _pairs_exist = [(s, f) for s, f in zip(_SOURCES, _YAML_FILES) if _source_table_exists(s)]
    _skipped_missing = [s["source_table"] for s in _SOURCES if not _source_table_exists(s)]

    if _skipped_missing:
        _preview = _skipped_missing[:10]
        _suffix = f" ... +{len(_skipped_missing)-10} more" if len(_skipped_missing) > 10 else ""
        print(f"INFO: {len(_skipped_missing)} config(s) skipped (source table not found): {_preview}{_suffix}")

    if _pairs_exist:
        _SOURCES, _YAML_FILES = map(list, zip(*_pairs_exist))
    else:
        _SOURCES, _YAML_FILES = [], []
else:
    print("WARN: information_schema query returned empty; bypassing source table filter.")


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

    # Cast bit-origin columns to BOOLEAN
    bit_cols = get_bit_column_names(src["source_table"], schema_dir=_SCHEMA_DIR)
    for bit_col in bit_cols:
        silver_name = rename_map.get(bit_col, bit_col)
        if silver_name in df.columns:
            df = df.withColumn(silver_name, F.col(silver_name).cast("boolean"))

    # Stamp sequencing column
    if include_sequencing:
        scd_seq = src.get("history_timestamp_source") or "pipeline_timestamp"
        if scd_seq == "pipeline_timestamp":
            pass  # _bronze_ingested_at already exists in bronze; no synthetic column needed
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
# Dynamic Silver table registration
# ─────────────────────────────────────────────────────────────────────────────

for src in _SOURCES:

    table_name              = src["source_table"]
    silver_table            = _normalize_table_name(table_name)
    silver_stream_view_name = f"{table_name}_silver_stream"
    scd_type                = src.get("scd_type", 2)

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

    # ─────────────────────────────────────────────────────────────────────────
    # STREAMING MODE — apply_changes (all TCI sources are file-based)
    # ─────────────────────────────────────────────────────────────────────────

    @dp.temporary_view(name=silver_stream_view_name)
    def silver_stream(src=src, silver_cols_cfg=silver_cols_cfg, cdf_renames=cdf_renames):
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
        # Compute _identity_hash if used as SCD2 key
        if src.get("scd_2_key_list") == ["_identity_hash"]:
            hash_cols = sorted([c for c in df.columns if not c.startswith("_")])
            df = df.withColumn(
                "_identity_hash",
                F.md5(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in hash_cols]))
            )
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

    keys_normalized = [to_silver(k) for k in src["scd_2_key_list"]]

    scd_seq = src.get("history_timestamp_source") or "pipeline_timestamp"
    if scd_seq == "pipeline_timestamp":
        sequence_by_col = "_bronze_ingested_at"
        except_cols     = []
    elif isinstance(scd_seq, list):
        sequence_by_col = "_scd_sequence_ts"
        except_cols     = ["_scd_sequence_ts"]
    else:
        sequence_by_col = to_silver(scd_seq)
        except_cols     = []

    _SYSTEM_AUDIT_COLS = [
        "_bronze_ingested_at",
        "_staging_ingested_at",
        "_source_file_path",
        "_source_file_name",
        "_source_file_size",
        "_source_file_modified",
        "_rescued_data",
    ]
    if isinstance(scd_seq, list):
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
        scd_2_exclude_normalized = [
            to_silver(c) for c in (src.get("scd_2_exclude_list") or [])
        ]

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

    # Quarantine table (only created when DQ rules exist).
    # A streaming table with no flow raises NO_QUERY_DEFINED_FOR_DATASET in SDP,
    # so the table and its populating flow must be created together.
    if dq_expectations:

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
            StructField("config_file", StringType()),
            StructField("tags", StringType()),
        ])
        return spark.createDataFrame([], schema)
    return spark.createDataFrame(rows)
````

#### `src/utilities/schema_parser.py`

````python
"""
schema_parser.py
================================================================================
Parses SQL Server schema definition files (.schema) into structured Python objects.

Usage:
    from utilities.schema_parser import parse_schema_file, to_spark_schema_hints

    columns = parse_schema_file("ddmast.schema")
    # Returns list of dicts: [{"name": "RECID", "sql_type": "char(1)", "nullable": True, "spark_type": "STRING"}, ...]

    hints_str = to_spark_schema_hints(columns)
    # Returns Auto Loader schemaHints string: "`RECID` STRING, `BRANCH` DECIMAL(3,0), ..."
================================================================================
"""

import os
import re
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# SQL Server → Spark type mapping
# ─────────────────────────────────────────────────────────────────────────────

_SQL_SERVER_TO_SPARK = {
    "bigint": "LONG",
    "int": "INT",
    "smallint": "SHORT",
    "tinyint": "BYTE",
    "bit": "SHORT",
    "float": "DOUBLE",
    "real": "FLOAT",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "datetime2": "TIMESTAMP",
    "smalldatetime": "TIMESTAMP",
    "time": "STRING",
    "uniqueidentifier": "STRING",
    "xml": "STRING",
    "text": "STRING",
    "ntext": "STRING",
    "image": "BINARY",
    "binary": "BINARY",
    "varbinary": "BINARY",
    "money": "DECIMAL(19,4)",
    "smallmoney": "DECIMAL(10,4)",
}


def _map_sql_type_to_spark(sql_type: str) -> str:
    """Convert a SQL Server column type to the equivalent Spark SQL type string."""
    sql_lower = sql_type.lower().strip()

    # Direct match (simple types)
    if sql_lower in _SQL_SERVER_TO_SPARK:
        return _SQL_SERVER_TO_SPARK[sql_lower]

    # char / varchar / nchar / nvarchar → STRING
    if sql_lower.startswith(("char", "varchar", "nchar", "nvarchar")):
        return "STRING"

    # decimal / numeric with precision and scale
    match = re.match(r"(decimal|numeric)\((\d+),\s*(\d+)\)", sql_lower)
    if match:
        precision, scale = match.group(2), match.group(3)
        return f"DECIMAL({precision},{scale})"

    # decimal / numeric without parens
    if sql_lower in ("decimal", "numeric"):
        return "DECIMAL(38,0)"

    # datetime2 with precision, e.g. datetime2(7)
    match = re.match(r"datetime2\(\d+\)", sql_lower)
    if match:
        return "TIMESTAMP"

    # Fallback
    return "STRING"


# ─────────────────────────────────────────────────────────────────────────────
# Parser
# ─────────────────────────────────────────────────────────────────────────────

# Pattern matches lines like:  [COLUMN_NAME] [type](args) NULL/NOT NULL,
_LINE_PATTERN = re.compile(
    r"\[(?P<name>[^\]]+)\]\s+"       # Column name in brackets
    r"\[(?P<type>[^\]]+)\]"          # Base type in brackets
    r"(?:\((?P<params>[^)]*)\))?"    # Optional params in parens
    r"\s+(?P<nullable>NULL|NOT NULL)" # Nullability
)

# Characters that require backtick-quoting in Spark SQL identifiers
_NEEDS_QUOTING = re.compile(r"[^a-zA-Z0-9_]")


def parse_schema_file(
    filename: str,
    schema_dir: Optional[str] = None,
) -> list[dict]:
    """Parse a SQL Server schema definition file into a list of column definitions.

    Args:
        filename: Name of the schema file (e.g. "ddmast.schema").
        schema_dir: Directory containing the file. Defaults to the utilities/
                    folder alongside this script.

    Returns:
        List of dicts with keys:
            - name: column name (str)
            - sql_type: original SQL Server type string (str)
            - nullable: whether the column allows NULLs (bool)
            - spark_type: equivalent Spark SQL type string (str)
    """
    if schema_dir is None:
        schema_dir = os.path.dirname(os.path.abspath(__file__))

    filepath = os.path.join(schema_dir, filename)
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Schema file not found: {filepath}")

    columns = []
    with open(filepath, "r") as f:
        for line in f:
            # Strip line number prefix (e.g. "1: \t...")
            stripped = re.sub(r"^\d+:\s*", "", line).strip()
            if not stripped:
                continue

            match = _LINE_PATTERN.search(stripped)
            if not match:
                continue

            col_name = match.group("name")
            base_type = match.group("type")
            params = match.group("params")
            nullable = match.group("nullable") == "NULL"

            # Reconstruct full SQL type string
            sql_type = f"{base_type}({params})" if params else base_type
            spark_type = _map_sql_type_to_spark(sql_type)

            columns.append({
                "name": col_name,
                "sql_type": sql_type,
                "nullable": nullable,
                "spark_type": spark_type,
            })

    return columns


# ─────────────────────────────────────────────────────────────────────────────
# Output helpers
# ─────────────────────────────────────────────────────────────────────────────

def _quote_identifier(name: str) -> str:
    """Backtick-quote a column name if it contains special characters."""
    if _NEEDS_QUOTING.search(name):
        return f"`{name}`"
    return name


def to_spark_schema_hints(columns: list[dict]) -> str:
    """Convert parsed columns to an Auto Loader schemaHints string.

    Column names containing special characters (e.g. #, $, spaces) are
    backtick-quoted for safe use in Spark SQL.

    Returns a comma-separated string like:
        "`RECID` STRING, `BRANCH` DECIMAL(3,0), `ACHDR#` STRING, ..."
    """
    parts = [f"{_quote_identifier(col['name'])} {col['spark_type']}" for col in columns]
    return ", ".join(parts)


def to_column_names(columns: list[dict]) -> list[str]:
    """Return just the column names as a list."""
    return [col["name"] for col in columns]


def get_bit_column_names(
    source_table: str,
    schema_dir: Optional[str] = None,
) -> list[str]:
    """Return names of columns whose SQL Server type is 'bit'.

    These columns are loaded as SHORT (0/1) in staging/bronze and should be
    cast to BOOLEAN in Silver for proper true/false semantics.

    Returns an empty list if no schema file exists or has no bit columns.
    """
    if schema_dir is None:
        schema_dir = os.path.dirname(os.path.abspath(__file__))

    schema_file = f"{source_table}.schema"
    schema_path = os.path.join(schema_dir, schema_file)
    if not os.path.isfile(schema_path):
        return []

    columns = parse_schema_file(schema_file, schema_dir=schema_dir)
    return [col["name"] for col in columns if col["sql_type"].lower() == "bit"]


def to_struct_type(columns: list[dict]):
    """Convert parsed columns to a PySpark StructType schema object.

    Requires pyspark to be available in the environment.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, LongType,
        ShortType, ByteType, BooleanType, DoubleType, FloatType,
        DateType, TimestampType, BinaryType, DecimalType,
    )

    def _resolve_type(spark_type_str: str):
        s = spark_type_str.upper()
        if s == "STRING":
            return StringType()
        if s == "INT":
            return IntegerType()
        if s == "LONG":
            return LongType()
        if s == "SHORT":
            return ShortType()
        if s == "BYTE":
            return ByteType()
        if s == "BOOLEAN":
            return BooleanType()
        if s == "DOUBLE":
            return DoubleType()
        if s == "FLOAT":
            return FloatType()
        if s == "DATE":
            return DateType()
        if s == "TIMESTAMP":
            return TimestampType()
        if s == "BINARY":
            return BinaryType()
        m = re.match(r"DECIMAL\((\d+),(\d+)\)", s)
        if m:
            return DecimalType(int(m.group(1)), int(m.group(2)))
        return StringType()

    fields = [
        StructField(col["name"], _resolve_type(col["spark_type"]), col["nullable"])
        for col in columns
    ]
    return StructType(fields)


# ─────────────────────────────────────────────────────────────────────────────
# CLI / standalone usage
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    target = sys.argv[1] if len(sys.argv) > 1 else "ddmast.schema"
    cols = parse_schema_file(target)
    print(f"Parsed {len(cols)} columns from {target}\n")
    print("First 10 columns:")
    for c in cols[:10]:
        print(f"  {c['name']:20s} {c['sql_type']:20s} → {c['spark_type']}")
    print(f"\nSchema hints (first 200 chars):\n  {to_spark_schema_hints(cols)[:200]}...")
````

### Pattern B — files

#### `src/pipelines/file_loader/axos_framework.py`

````python
"""
axos_framework.py
================================================================================
Shared helpers for the TCI config-driven file-loader pipeline.
================================================================================

Adapted from the processibeam bundle's axos_framework. Provides:
  - Naming standard (catalog.schema.table construction)
  - Config folder scanning
  - Auto Loader option mapping
  - Validation

CURRENT AXOS STANDARD
    catalog.schema.table  =  {division}_{env}.{layer}_{sor}.{table}
    e.g.                     enterprise_dev.staging_tci.some_table
"""

import glob
import os
import re


# =============================================================================
# NAMING STANDARD
# =============================================================================

DEFAULT_DIVISION = "enterprise"

LAYER_STAGING = "staging"
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"


def division(src: dict) -> str:
    """Division token that prefixes the catalog."""
    return str(src.get("division") or DEFAULT_DIVISION)


def _catalog(src: dict, env: str, override_key: str) -> str:
    prefix = src.get(override_key) or division(src)
    return f"{prefix}_{env}"


def _schema(src: dict, layer: str, override_key: str) -> str:
    override = src.get(override_key)
    if override:
        return str(override)
    sor = src.get("sor")
    if not sor:
        raise ValueError(
            f"Table '{src.get('source_table', '<unnamed>')}' is missing 'sor', "
            f"which is required to build the {layer} schema name."
        )
    return f"{layer}_{sor}"


def staging_catalog(src: dict, env: str) -> str:
    return _catalog(src, env, "staging_catalog")


def staging_schema(src: dict) -> str:
    return _schema(src, LAYER_STAGING, "staging_schema")


def staging_fqn(src: dict, env: str) -> str:
    """Append-only Auto Loader landing table."""
    return f"{staging_catalog(src, env)}.{staging_schema(src)}.{src['source_table']}"


# =============================================================================
# ENVIRONMENT RESOLUTION
# =============================================================================

_ENV_CONF_KEYS = ("catalog_env", "target_env")


def resolve_env(spark) -> str:
    """Read the environment token from pipeline configuration."""
    for key in _ENV_CONF_KEYS:
        value = spark.conf.get(key, None)
        if value:
            return str(value).strip()
    raise ValueError(
        "No environment token found in the pipeline configuration. Set one of "
        f"{list(_ENV_CONF_KEYS)} in the pipeline's `configuration:` block."
    )


def resolve_config_dir(spark) -> str:
    """The config folder this pipeline owns."""
    config_dir = spark.conf.get("pipeline_config_dir", None)
    if not config_dir:
        raise ValueError(
            "Pipeline configuration key 'pipeline_config_dir' is not set."
        )
    return config_dir


# =============================================================================
# CONFIG FOLDER SCAN
# =============================================================================

def load_table_configs(config_dir: str) -> list:
    """
    Glob every *.yml in `config_dir` and load one table config per file.
    Returns [(filename, config_dict), ...].
    """
    if not os.path.isdir(config_dir):
        raise FileNotFoundError(
            f"Config directory not found: '{config_dir}'. Check the "
            "pipeline_config_dir value in databricks.yml."
        )

    # Reject .yaml files (only .yml is supported)
    stray = sorted(
        os.path.basename(p) for p in glob.glob(os.path.join(config_dir, "*.yaml"))
    )
    if stray:
        raise ValueError(
            f"Config directory '{config_dir}' contains .yaml files: {stray}. "
            "Table configs must use the .yml extension."
        )

    paths = sorted(glob.glob(os.path.join(config_dir, "*.yml")))

    # Skip template files (prefixed with _)
    paths = [p for p in paths if not os.path.basename(p).startswith("_")]

    if not paths:
        raise FileNotFoundError(
            f"No *.yml table configs found in '{config_dir}'. "
            "Populate the folder before deploying; one YAML = one table."
        )

    return [(os.path.basename(p), _load_one(p)) for p in paths]


def _load_one(path: str) -> dict:
    """Load a single table config YAML. Accepts a mapping or single-item list."""
    import yaml

    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if isinstance(data, list):
        if not data:
            raise ValueError(f"[{os.path.basename(path)}] File is empty.")
        return data[0]
    if not isinstance(data, dict):
        raise ValueError(
            f"[{os.path.basename(path)}] Expected a mapping or single-item list."
        )
    return data


# =============================================================================
# VALIDATION
# =============================================================================

def validate_identity(sources: list):
    """Check source_table uniqueness and sor presence."""
    seen = set()
    for filename, src in sources:
        table = src.get("source_table")
        if not table:
            raise ValueError(f"[{filename}] Missing required field 'source_table'.")
        if table in seen:
            raise ValueError(f"[{filename}] Duplicate source_table '{table}'.")
        seen.add(table)
        if not src.get("sor"):
            raise ValueError(
                f"[{filename}] Missing required field 'sor' for '{table}'."
            )


# Dev landing accounts — block non-dev environments from reading dev data.
DEV_ENV = "dev"
DEV_LANDING_ACCOUNTS = ("axdevdbxlanding",)
_ABFSS_HOST = re.compile(r"^abfss://[^@/]*@([^/]+)", re.IGNORECASE)


def storage_account(source_path: str):
    match = _ABFSS_HOST.match(str(source_path).strip())
    if not match:
        return None
    return match.group(1).split(".")[0].lower()


def validate_acquisition(sources: list, env: str):
    """Validate fields the acquisition layer needs."""
    validate_identity(sources)

    for filename, src in sources:
        table = src["source_table"]

        if not src.get("source_path"):
            raise ValueError(
                f"[{filename}] Missing 'source_path' for '{table}'."
            )

        # Block dev landing paths in non-dev envs
        source_path = str(src["source_path"])
        account = storage_account(source_path)
        if env != DEV_ENV and account in DEV_LANDING_ACCOUNTS:
            raise ValueError(
                f"[{filename}] source_path for '{table}' points at DEV landing "
                f"account '{account}' but environment is '{env}'."
            )

        file_format = str(src.get("file_format") or "").lower()
        if not file_format:
            raise ValueError(f"[{filename}] Missing 'file_format' for '{table}'.")
        if file_format not in FILE_FORMAT_ALIASES:
            raise ValueError(
                f"[{filename}] Unsupported file_format '{src['file_format']}' for "
                f"'{table}'. Valid: {sorted(FILE_FORMAT_ALIASES)}."
            )

        bronze_options = src.get("bronze_options") or {}
        csv_options = bronze_options.get("csv_options") or {}

        if file_format == "csv" and not csv_options:
            raise ValueError(
                f"[{filename}] file_format 'csv' for '{table}' requires a "
                "non-empty 'csv_options' block under bronze_options."
            )

        # XML requires explicit rowTag so the reader knows which element = one row
        xml_options = bronze_options.get("xml_options") or {}
        if file_format == "xml" and not xml_options.get("rowTag"):
            raise ValueError(
                f"[{filename}] file_format 'xml' for '{table}' requires an "
                "'xml_options' block under bronze_options with an explicit "
                "'rowTag' naming the repeating element (e.g. rowTag: Application)."
            )


# =============================================================================
# AUTO LOADER OPTIONS
# =============================================================================

FILE_FORMAT_ALIASES = {
    "json": "json",
    "parquet": "parquet",
    "csv": "csv",
    "avro": "avro",
    "text": "text",
    "excel": "excel",
    "xml": "xml",
}

BRONZE_OPTION_MAP = {
    "schema_hints": "cloudFiles.schemaHints",
    "schema_evolution_mode": "cloudFiles.schemaEvolutionMode",
    "include_existing_files": "cloudFiles.includeExistingFiles",
    "ignore_corrupt_files": "ignoreCorruptFiles",
    "file_name_pattern": "pathGlobFilter",
    "use_managed_file_events": "cloudFiles.useManagedFileEvents",
    "backfill_interval": "cloudFiles.backfillInterval",
}

INFER_COLUMN_TYPES_OPTION = "cloudFiles.inferColumnTypes"


def _option_value(value) -> str:
    """Convert Python bools to Spark option strings."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def autoloader_options(src: dict) -> dict:
    """Assemble the full reader option dict for one source's Auto Loader stream."""
    options = {
        "cloudFiles.format": FILE_FORMAT_ALIASES[str(src["file_format"]).lower()],
    }

    bronze_options = src.get("bronze_options") or {}

    for yaml_key, option_key in BRONZE_OPTION_MAP.items():
        value = bronze_options.get(yaml_key)
        if value is not None:
            options[option_key] = _option_value(value)

    # csv_options passthrough
    for key, value in (bronze_options.get("csv_options") or {}).items():
        options[key] = _option_value(value)

    # json_options passthrough
    for key, value in (bronze_options.get("json_options") or {}).items():
        options[key] = _option_value(value)

    # xml_options passthrough (rowTag, rootTag, etc.)
    for key, value in (bronze_options.get("xml_options") or {}).items():
        options[key] = _option_value(value)

    # Disable type inference for flat formats (Bronze stays STRING).
    # XML requires inference so nested elements become STRUCT/ARRAY, not STRING.
    file_fmt = str(src["file_format"]).lower()
    options[INFER_COLUMN_TYPES_OPTION] = "true" if file_fmt == "xml" else "false"

    return options


# =============================================================================
# TABLE PROPERTIES
# =============================================================================

def source_tags(src: dict) -> dict:
    """Per-table tags surfaced as table properties."""
    return {f"source.{k}": str(v) for k, v in (src.get("tags") or {}).items()}
````

#### `src/pipelines/file_loader/acquisition_file_pipeline.py`

The table name uses `pipeline.catalog` when that setting is present, so any catalog name works.

````python
# =============================================================================
# acquisition_file_pipeline.py
# =============================================================================
# File Loader — raw file acquisition into append-only staging tables.
#
# Auto Loader (cloudFiles) reads files from ADLS Gen2 (abfss://) and appends them
# to one streaming table per config YAML. No CDC, no dedup, no transforms — only
# file provenance columns are added.
#
# Config-driven: every *.yml in pipeline_config_dir becomes one staging table.
# Adapted from the processibeam bundle pattern.
# =============================================================================

import os
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Import the shared naming/config helpers
# ---------------------------------------------------------------------------
for _candidate in (spark.conf.get("pipeline_source_dir", None), os.getcwd()):
    if _candidate and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

try:
    import axos_framework as axos
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Could not import axos_framework.py. Ensure it sits next to this file "
        "in src/pipelines/file_loader/ and the pipeline's `configuration:` block "
        "sets 'pipeline_source_dir' to the synced workspace path of that folder. "
        f"sys.path was: {sys.path[:3]}"
    ) from exc

# ---------------------------------------------------------------------------
# Config folder scan
# ---------------------------------------------------------------------------
_ENV = axos.resolve_env(spark)
_CONFIG_DIR = axos.resolve_config_dir(spark)
_SOURCES = list(axos.load_table_configs(_CONFIG_DIR))
_PIPELINE_CATALOG = spark.conf.get("pipeline.catalog", None)

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
axos.validate_acquisition(_SOURCES, _ENV)

# ---------------------------------------------------------------------------
# Provenance columns
# ---------------------------------------------------------------------------

def _add_provenance(df):
    """Stamp file provenance metadata on every row."""
    return (
        df
        .withColumn("_staging_ingested_at", F.current_timestamp())
        .withColumn("_source_file_path", F.col("_metadata.file_path"))
        .withColumn("_source_file_name", F.col("_metadata.file_name"))
        .withColumn("_source_file_size", F.col("_metadata.file_size").cast("long"))
        .withColumn("_source_file_modified", F.col("_metadata.file_modification_time"))
    )

# ---------------------------------------------------------------------------
# Dynamic registration: one streaming table per config YAML
# ---------------------------------------------------------------------------
for _filename, _src in _SOURCES:

    staging_table = (
        f"{_PIPELINE_CATALOG}.{axos.staging_schema(_src)}.{_src['source_table']}"
        if _PIPELINE_CATALOG
        else axos.staging_fqn(_src, _ENV)
    )

    _table_properties = {
        "pipelines.autoOptimize.managed": "true",
        "ingestion.source_type": "file",
        "ingestion.source_path": str(_src["source_path"]),
        "ingestion.file_format": str(_src["file_format"]),
        "ingestion.config_file": _filename,
        "quality": "staging",
        **axos.source_tags(_src),
    }

    dp.create_streaming_table(
        name=staging_table,
        comment=_src.get("description")
        or f"Raw file acquisition (append-only): {_src['source_table']}",
        table_properties=_table_properties,
    )

    @dp.append_flow(
        target=staging_table,
        name=f"{_src['source_table']}_autoloader_flow",
    )
    def acquire_files(src=_src):
        reader = (
            spark.readStream
            .format("cloudFiles")
            .options(**axos.autoloader_options(src))
        )
        return reader.load(src["source_path"]).transform(_add_provenance)
````

#### `src/pipelines/staging_to_silver/bronze.py`

````python
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


def _drop_all_null_rows(df):
    """Drop rows where every business column (non-metadata) is NULL.

    TCI staging fan-out can produce completely null rows when an entity
    node is absent from the XML.  These carry no information and should
    not propagate to bronze.
    """
    business_cols = [c for c in df.columns if not c.startswith("_")]
    if not business_cols:
        return df
    # Keep row if at least one business column has a value
    condition = F.lit(False)
    for c in business_cols:
        condition = condition | F.col(f"`{c}`").isNotNull()
    return df.filter(condition)

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

def _table_exists(fqn: str) -> bool:
    """Check table existence via SQL (spark.catalog is blocked in DLT)."""
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}")
        return True
    except Exception:
        return False


for src in _SOURCES:

    _bronze_fqn = f"{src['source_catalog']}.{src['source_schema']}.{src['source_table']}"
    _staging_table = f"{src['staging_catalog']}.{src['staging_schema']}.{src['source_table']}"

    # Skip if staging table doesn't exist yet (e.g. Lakeflow Connect not configured)
    if not _table_exists(_staging_table):
        continue
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
            df = _drop_all_null_rows(df)
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
            df = _drop_all_null_rows(df)
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
````

#### `src/file_copy/s3_copy.py`

Pattern B option 2 only.

````python
"""Copy objects from S3 into a Unity Catalog volume. Never writes tables.

A config is used only when it has an s3_copy block. Files land under
/Volumes/<catalog>/<schema>/<volume>/<source_table>/ and Auto Loader reads
that folder. A file already in the volume with the same size is skipped.
"""

import fnmatch
import glob
import os
import shutil
import tempfile

import yaml


def get_secret(scope, key):
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark).secrets.get(scope=scope, key=key)
    except Exception:
        env_key = f"{scope}__{key}".upper().replace("-", "_")
        value = os.environ.get(env_key)
        if value is None:
            raise RuntimeError(
                f"Secret {scope}/{key} not found via dbutils, and env var "
                f"{env_key} is not set for local fallback."
            )
        return value


def load_copy_configs(config_dir):
    configs = []
    for path in sorted(glob.glob(os.path.join(config_dir, "*.yml"))):
        if os.path.basename(path).startswith("_"):
            continue
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        if isinstance(data, list):
            data = data[0] if data else {}
        if isinstance(data, dict) and data.get("s3_copy"):
            configs.append((os.path.basename(path), data))
    return configs


def _split_volume(name):
    parts = str(name).split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"s3_copy.volume must be catalog.schema.volume, got {name!r}")
    return parts


def target_dir(cfg):
    catalog, schema, volume = _split_volume(cfg["s3_copy"]["volume"])
    return f"/Volumes/{catalog}/{schema}/{volume}/{cfg['source_table']}"


def validate(filename, cfg):
    copy = cfg["s3_copy"]
    for field in ("bucket", "region", "volume", "secret_scope",
                  "access_key_secret", "secret_key_secret"):
        if not copy.get(field):
            raise ValueError(f"[{filename}] s3_copy.{field} is required")
    if not cfg.get("source_table"):
        raise ValueError(f"[{filename}] source_table is required")
    expected = target_dir(cfg).rstrip("/")
    actual = str(cfg.get("source_path") or "").rstrip("/")
    if actual != expected:
        raise ValueError(
            f"[{filename}] source_path must be {expected}/ so Auto Loader reads "
            f"the copied files, got {cfg.get('source_path')!r}"
        )


def build_s3_client(copy):
    import boto3

    return boto3.client(
        "s3",
        region_name=copy["region"],
        aws_access_key_id=get_secret(copy["secret_scope"], copy["access_key_secret"]),
        aws_secret_access_key=get_secret(copy["secret_scope"], copy["secret_key_secret"]),
    )


def copy_source(cfg, s3_client, spark):
    copy = cfg["s3_copy"]
    catalog, schema, volume = _split_volume(copy["volume"])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")

    destination = target_dir(cfg)
    os.makedirs(destination, exist_ok=True)

    prefix = copy.get("prefix") or ""
    pattern = copy.get("file_name_pattern") or "*"
    copied = 0
    skipped = 0

    pages = s3_client.get_paginator("list_objects_v2").paginate(
        Bucket=copy["bucket"], Prefix=prefix
    )
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if not fnmatch.fnmatch(key.rsplit("/", 1)[-1], pattern):
                continue
            relative = key[len(prefix):] if key.startswith(prefix) else key
            target = os.path.join(destination, relative.lstrip("/").replace("/", "__"))
            if os.path.exists(target) and os.path.getsize(target) == obj["Size"]:
                skipped += 1
                continue
            handle, local_path = tempfile.mkstemp()
            os.close(handle)
            try:
                s3_client.download_file(copy["bucket"], key, local_path)
                shutil.copyfile(local_path, target)
            finally:
                os.remove(local_path)
            copied += 1
    return copied, skipped


def run(config_dir):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    configs = load_copy_configs(config_dir)
    if not configs:
        raise ValueError(f"No configs with an s3_copy block in {config_dir}")
    for filename, cfg in configs:
        validate(filename, cfg)
    for filename, cfg in configs:
        client = build_s3_client(cfg["s3_copy"])
        copied, skipped = copy_source(cfg, client, spark)
        print(f"{filename}: copied={copied} skipped={skipped} into {target_dir(cfg)}")
````

#### `src/python/run_s3_copy.py`

Pattern B option 2 only.

````python
"""DAB task entrypoint: copy S3 objects into the staging volume."""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), ".."))

from file_copy.s3_copy import run  # noqa: E402

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-dir", required=True)
    args = parser.parse_args()
    run(args.config_dir)
````

