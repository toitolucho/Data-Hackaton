# Human setup guide — from an empty computer to a running pipeline

This is what **you** do by hand. The agent does the file work using `AGENT_BUILD_GUIDE.md`. You create accounts and credentials, install tools, connect the CLI, create the GitHub repository, hand the agent its inputs, and run deploy and run.

Both files are self-contained. You do not need the old sample folders on the new computer. Copy `AGENT_BUILD_GUIDE.md` and this file to the new machine.

---

## 0. What you will have at the end

- The Databricks CLI on your PC, logged in to your workspace under a named profile.
- A secret scope holding the source credentials. No password in any file.
- A GitHub repository with the bundle.
- A job in the workspace that loads one table into bronze, and later into silver.

| Pattern | Source | Job tasks |
|---|---|---|
| A | REST API (ServiceNow was the proven case) | `bootstrap → extract → promote → raw_to_flat → staging_to_silver` |
| B | CSV files on AWS S3 | `s3_copy → file_loader_to_staging → staging_to_silver` |

---

## 1. Accounts and access you need first

Get these before you install anything. Ask your admin for the ones you do not have.

| What | Why | How to check |
|---|---|---|
| A Databricks workspace user | Deploy and run jobs | You can open the workspace URL and sign in |
| Unity Catalog permission on one catalog: `USE CATALOG`, `CREATE SCHEMA` (or ownership) | The bundle creates schemas, a volume, and tables | Section 4 |
| Serverless enabled for jobs and pipelines | All tasks run on Serverless | Section 4 |
| A GitHub account (and an organization if your team uses one) | Source control | You can create a repository |
| Source credentials | The data source | ServiceNow: a service account user and password. S3: an access key id and secret access key, bucket name, region. |

Pattern B, option 1 only: an admin must create an AWS storage credential (IAM role) and an external location for the bucket, and grant you `READ FILES`. If that is not available, use option 2 (keys in a secret scope). Option 2 is what this guide assumes.

---

## 2. Install the tools (Windows)

Open **PowerShell**. Install with `winget`, or download the installers from the links below.

```powershell
winget install --id Git.Git -e
winget install --id Python.Python.3.11 -e
winget install --id Databricks.DatabricksCLI -e
winget install --id GitHub.cli -e        # optional, for creating repos from the terminal
```

Close PowerShell and open a new one so `PATH` updates. Then check each tool:

```powershell
git --version
python --version
databricks --version
gh --version          # only if installed
```

If `winget` is not available:

| Tool | Where |
|---|---|
| Git | [git-scm.com/download/win](https://git-scm.com/download/win) |
| Python 3.11 | [python.org/downloads](https://www.python.org/downloads/) (tick "Add python.exe to PATH") |
| Databricks CLI | [github.com/databricks/cli/releases](https://github.com/databricks/cli/releases). Download `databricks_cli_<version>_windows_amd64.zip`, unzip to `C:\Databricks\`, and add `C:\Databricks` to your user `PATH`. |

The CLI must be the new Go-based CLI (version 0.2xx or later). If `databricks --version` prints 0.17 or 0.18, that is the old Python CLI. Uninstall it (`pip uninstall databricks-cli`) and install the new one.

Python packages for local checks:

```powershell
python -m pip install --upgrade pip
python -m pip install pyyaml requests boto3
```

Set your Git identity once:

```powershell
git config --global user.name  "Your Name"
git config --global user.email "you@company.com"
```

Install an editor with an AI agent (Cursor) to run the agent guide.

---

## 3. Connect the CLI to your workspace

### 3.1 Find the workspace URL

Open the workspace in a browser. Copy the address up to the first `/` after the host. Examples:

- Azure: `https://adb-7405617824787548.8.azuredatabricks.net/`
- AWS: `https://<name>.cloud.databricks.com/`

### 3.2 Log in under a profile name

Pick a short profile name per workspace, for example `axos-dev` or `datathon-dev`. Use a different name for each workspace so they never mix.

```powershell
databricks auth login --host <workspace-url> --profile <profile>
```

A browser window opens. Sign in with the user that belongs to that workspace. When it says the login succeeded, return to PowerShell.

### 3.3 Check it

```powershell
databricks auth profiles
databricks current-user me --profile <profile>
```

`auth profiles` lists every profile and shows `YES` under Valid for working ones. `current-user me` prints your user name. The profiles are stored in `%USERPROFILE%\.databrickscfg`. Do not commit that file.

### 3.4 When the login expires

Any command can fail with:

```text
error getting token: token refresh: ... "Refresh token is invalid"
```

That only means the login expired. Run the same `databricks auth login --host <workspace-url> --profile <profile>` again.

---

## 4. Check the workspace

### 4.1 Serverless

In the workspace, open a new notebook. In the compute selector at the top right, **Serverless** must be available. If it is not, ask the workspace admin to enable serverless compute for notebooks, jobs, and pipelines (admin settings → **Feature enablement**).

### 4.2 Catalog permissions

Open **SQL Editor** (or a notebook on Serverless) and run, with your catalog name:

```sql
SHOW GRANTS ON CATALOG <catalog>;
```

You need `USE CATALOG` and `CREATE SCHEMA` (or `ALL PRIVILEGES`). Prove it:

```sql
CREATE SCHEMA IF NOT EXISTS <catalog>.setup_check;
DROP SCHEMA <catalog>.setup_check;
```

If it fails, send your admin this:

```sql
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <catalog> TO `<your user or group>`;
```

Write the catalog name down. On the original workspace it was `enterprise_dev`.

---

## 5. Store the credentials as secrets

Secrets are created with the CLI. Use names only in files. The values exist only in the scope.

### 5.1 Create the scope

```powershell
databricks secrets create-scope <scope> --profile <profile>
```

Examples: `servicenow` for the API, `aws-datathon` for S3.

### 5.2 Put each secret

```powershell
databricks secrets put-secret <scope> <key> --profile <profile>
```

The CLI asks for the value. Paste it and press Enter. Repeat for each key.

| Pattern | Keys (names are your choice; the YAML must use the same names) |
|---|---|
| A, ServiceNow | `snow-username`, `snow-password` |
| B, S3 | `aws-access-key-id`, `aws-secret-access-key` |

If the prompt does not appear, use `--string-value "<value>"`, then clear the PowerShell history line: `Clear-History` and delete `%APPDATA%\Microsoft\Windows\PowerShell\PSReadLine\ConsoleHost_history.txt` entries for that line.

### 5.3 Check the names (never prints values)

```powershell
databricks secrets list-scopes --profile <profile>
databricks secrets list-secrets <scope> --profile <profile>
```

### 5.4 Let other people or a service principal read it (only if needed)

```powershell
databricks secrets put-acl <scope> <user-or-group> READ --profile <profile>
```

Jobs you deploy in `dev_sandbox` run as you, so you do not need this for your own runs.

---

## 6. Test the connection before any bundle exists

Open a notebook on **Serverless**. Run the cell for your pattern. These cells print no secret.

### Pattern A, REST API

```python
import requests

user = dbutils.secrets.get("<scope>", "<username-key>")
password = dbutils.secrets.get("<scope>", "<password-key>")
response = requests.get(
    "https://<host>/<path>",
    params={"sysparm_limit": 1},
    auth=(user, password),
    headers={"Accept": "application/json"},
    timeout=30,
)
print(response.status_code)
print(list(response.json()["result"][0].keys())[:15])
```

`200` and a list of field names means the source, the account, and Serverless egress all work. On the original workspace a classic cluster failed this same call with `UNEXPECTED_EOF_WHILE_READING`. Serverless worked.

### Pattern B, S3

```python
%pip install boto3
```

```python
import boto3

s3 = boto3.client(
    "s3",
    region_name="<region>",
    aws_access_key_id=dbutils.secrets.get("<scope>", "aws-access-key-id"),
    aws_secret_access_key=dbutils.secrets.get("<scope>", "aws-secret-access-key"),
)
response = s3.list_objects_v2(Bucket="<bucket>", Prefix="<prefix>/", MaxKeys=20)
for obj in response.get("Contents", []):
    print(obj["Key"], obj["Size"])
```

Write down the file names. Pick **one** file pattern for the first table. Download one file and write down its header row, the column that identifies a row, and a timestamp column if there is one. The agent must not guess those.

Do not keep keys in a notebook or a script. If you run `boto3` on your laptop, set the keys only in the shell for that session:

```powershell
$env:AWS_ACCESS_KEY_ID = "<paste>"
$env:AWS_SECRET_ACCESS_KEY = "<paste>"
python -c "import boto3; s3=boto3.client('s3', region_name='<region>'); r=s3.list_objects_v2(Bucket='<bucket>', Prefix='<prefix>/', MaxKeys=20); [print(o['Key'], o['Size']) for o in r.get('Contents', [])]"
Remove-Item Env:AWS_ACCESS_KEY_ID, Env:AWS_SECRET_ACCESS_KEY
```

---

## 7. GitHub repository

### 7.1 Create an empty repository

In GitHub: **New repository**. Name it after the bundle (for example `servicenow-sor-databricks` or `s3-csv-databricks`). Private. Do **not** add a README, `.gitignore`, or license, so the first push has no conflict.

Or from the terminal:

```powershell
gh auth login
gh repo create <org-or-user>/<repo> --private
```

The original ServiceNow repository is `https://github.com/LYampa_axosEnt/servicenow-sor-databricks.git`. A new source type gets a new repository.

### 7.2 Create the local folder

```powershell
mkdir <path>\databricks\<bundle-name>
cd <path>\databricks\<bundle-name>
git init -b main
```

Point the agent at this folder in section 8.

### 7.3 First push (after the agent created the files and you reviewed them)

Before you commit, make sure no secret is in the folder:

```powershell
git status
Select-String -Path (Get-ChildItem -Recurse -File).FullName -Pattern 'AKIA|password\s*[:=]|secret\s*[:=]' -List
```

The search must find nothing except key **names** such as `password_secret_key: snow-password`.

```powershell
git add .
git commit -m "Initial ingestion bundle for <sor>."
git remote add origin https://github.com/<org-or-user>/<repo>.git
git push -u origin main
```

The first push opens a browser to sign in to GitHub (Git Credential Manager). Use a branch and a pull request for later changes if your team reviews code:

```powershell
git checkout -b <short-change-name>
git push -u origin <short-change-name>
```

### 7.4 Optional: see the repo inside Databricks

Deploying does not need this. It only lets you browse the repository in the workspace.

1. In the workspace, click your user icon → **Settings** → **Linked accounts**.
2. Under **Git integration**, choose **GitHub** and click **Link Git account** (or paste a GitHub personal access token).
3. Go to **Workspace** → your folder → **Create** → **Git folder**. Paste the repository URL.

---

## 8. Give the agent its instructions

Open the bundle folder in Cursor. Make sure `AGENT_BUILD_GUIDE.md` is in the folder above it or attach it. Fill in this message and send it:

```text
Follow AGENT_BUILD_GUIDE.md from section 0. Build pattern <A or B>.
Do not deploy, run, commit, or push.

Bundle name: <bundle-name>
Bundle folder: <full path>
Workspace URL: <url>
CLI profile: <profile>
Catalog: <catalog>
SOR token: <sor>
Secret scope: <scope>
Secret key names: <key1>, <key2>
First table: <table>

Pattern A:
API host: <https://host>
Table path: </path>
Auth type: <basic>
Paging: <offset/limit, param names>
Records key: <result>
Primary key: <field>
Mode: <full_only or watermark>, watermark field: <field or N/A>

Pattern B:
Bucket: <bucket>   Region: <region>   Prefix: <prefix>/
File pattern for this table: <pattern>.csv
S3 access: option 2 (keys in secret scope)
Header: <yes/no>, delimiter: <,>
```

When the agent finishes, it runs `bundle validate` and stops. Read the files it created. Check the YAML values yourself.

---

## 9. Validate, deploy, run

From the bundle folder:

```powershell
python scripts/validate_config.py                                   # Pattern A only
databricks bundle validate -t dev_sandbox --profile <profile>
databricks bundle deploy   -t dev_sandbox --profile <profile>
databricks bundle run <job-key> -t dev_sandbox --profile <profile>
```

- `validate` checks the YAML and the bundle. It changes nothing.
- `deploy` uploads files and creates or updates the job and pipelines under `/Users/<you>/.bundle/<bundle-name>/dev_sandbox`. It does not run anything.
- `run` starts the job and prints a run URL. `<job-key>` is the key under `resources.jobs` in the job YAML (for example `servicenow_cmn_schedule`), not the display name.

You can also start the job from the UI with **Run now**.

---

## 10. Watch the run in the UI

1. Open the workspace.
2. Left sidebar → **Jobs & Pipelines** (older UI: **Workflows**).
3. Search the table or SOR name. In development mode the name starts with `[dev <you>]`, for example `[dev lyampa] api_loader_servicenow_cmn_schedule`.
4. Open the job → **Runs** → the newest run.
5. Each task is a box. Green is done, red failed. Click a red box to read the error.
6. Click a pipeline task box (`raw_to_flat`, `file_loader_to_staging`, `staging_to_silver`) to open the pipeline update. It shows each table it built and how many rows went in.

---

## 11. Check the data

Open **SQL Editor** on Serverless.

### Pattern A

```sql
SELECT _batch_id, COUNT(*) AS rows, COUNT(DISTINCT _pk) AS keys
FROM <catalog>.bronze_<sor>.<table>_raw
GROUP BY _batch_id ORDER BY _batch_id DESC;
```

After silver is on:

```sql
SELECT COUNT(*) AS current_rows
FROM <catalog>.silver_<sor>.<table>_flat
WHERE __END_AT IS NULL;
```

### Pattern B

```sql
SELECT _source_file_name, COUNT(*) AS rows
FROM <catalog>.staging_<sor>.<table>
GROUP BY _source_file_name;

SELECT COUNT(*) FROM <catalog>.bronze_<sor>.<table>;
```

Rows per file must equal the file's line count minus the header.

---

## 12. Compare with the existing SQL Server copy (Pattern A)

On SQL Server:

```sql
SELECT COUNT(DISTINCT sys_id) AS keys FROM <db>.dbo.<table>;
SELECT sys_id, name, type, sys_class_name FROM <db>.dbo.<table> ORDER BY sys_id;
```

On Databricks:

```sql
SELECT sys_id, name, type, sys_class_name
FROM <catalog>.silver_<sor>.<table>_flat
WHERE __END_AT IS NULL
ORDER BY sys_id;
```

Compare key counts first, then the key lists, then column values. Use the production SQL copy if pre-production is stale. On the original run, PROD matched 42 of 42 and PREPROD had only 31 because it had not been refreshed.

Compare distinct keys, not `COUNT(*)`. A full-reload table has one copy per run in bronze.

---

## 13. Next tables and silver

| Step | Who |
|---|---|
| Add a table: new YAML + new job (A), or new YAML only (B) | Agent, using its guide |
| Validate, deploy, run, check counts | You |
| Large API table first load: make sure the YAML has **no** `max_pages` | You check, agent edits |
| Silver for a table: decide the key, timestamp, and the columns that match SQL Server | You |
| Add those settings to the same YAML and wire the pipelines | Agent |
| Check silver against SQL Server | You |

Turn on silver for one table at a time.

---

## 14. Day-to-day

| Task | How |
|---|---|
| Login expired | `databricks auth login --host <url> --profile <profile>` |
| Password or key changed | `databricks secrets put-secret <scope> <key> --profile <profile>`. No redeploy needed. |
| Change code or YAML | Edit, `bundle validate`, `bundle deploy`. Commit and push. |
| Schedule the job | Ask the agent to add a `schedule` block with `pause_status: PAUSED`, deploy, then unpause in the UI when ready |
| Remove the sandbox resources | `databricks bundle destroy -t dev_sandbox --profile <profile>` (deletes the job and pipelines it created; tables stay) |

---

## 15. Problems you may see

| Message | Meaning | What to do |
|---|---|---|
| `Refresh token is invalid` | Login expired | Section 3.4 |
| `PERMISSION_DENIED ... CREATE SCHEMA` | No rights on the catalog | Section 4.2 |
| `Secret does not exist with scope: ... and key: ...` | Name in YAML differs from the scope | `databricks secrets list-secrets <scope>`; fix the YAML or re-create the key |
| `401` from the API | Wrong user or password in the scope | Re-put the secret |
| `UNEXPECTED_EOF_WHILE_READING` | Classic cluster cannot reach the API | Use Serverless |
| `AccessDenied` from S3 | Key lacks `s3:ListBucket` / `s3:GetObject` on that bucket and prefix | Ask the bucket owner |
| Pipeline: `update already running` | Two jobs started the same pipeline | Run one job at a time, or ask the agent for a parent job |
| Silver table missing after first run | Expected while the key list is empty (Pattern B), or the flat table did not exist yet | Fill the key, or run the job again |

---

## 16. Your record sheet

Fill this in on the new computer. Keep it next to the bundle, not in the repository, if it has anything private.

| Item | Value |
|---|---|
| Workspace URL | |
| CLI profile | |
| Catalog | |
| SOR token | |
| Secret scope | |
| Secret key names | |
| GitHub repository | |
| Bundle folder on disk | |
| First table | |
| Job key | |
| Date first run succeeded | |

### Reference: the original ServiceNow setup

| Item | Value |
|---|---|
| Workspace URL | `https://adb-7405617824787548.8.azuredatabricks.net/` |
| CLI profile | `axos-dev` |
| Catalog | `enterprise_dev` |
| SOR token | `servicenow` |
| Secret scope | `servicenow` |
| Secret key names | `snow-username`, `snow-password` (also `snow-instance`) |
| API host | `https://bofi.service-now.com` |
| Service account | `SVC_AXP_EDW_SNOW` |
| GitHub | `https://github.com/LYampa_axosEnt/servicenow-sor-databricks.git` |
| Target | `dev_sandbox` |
| Tables in bronze | `cmn_schedule`, `sys_user`, `sys_user_group`, `contract_sla`, `u_product`, `change_request`, `problem`, `sysapproval_approver`, `task_sla`, `task` |
| Table with silver | `cmn_schedule` → `enterprise_dev.silver_servicenow.cmn_schedule_flat` |
| Still open | `max_pages: 5` on `change_request` and `problem`; key reconcile for `task`, `task_sla`, `sysapproval_approver`; a parent job for all tables |
| Confluence | [ServiceNow Documentation and QA for SOR Ingestion Pipeline](https://bofidev.atlassian.net/wiki/spaces/IO/pages/3225960448026/ServiceNow+Documentation+and+QA+for+SOR+Ingestion+Pipeline) |
