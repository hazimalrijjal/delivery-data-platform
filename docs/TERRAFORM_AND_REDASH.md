# Week 5: Terraform, Redash, GitHub (beginner walkthrough)

Status: written by Claude, then run by the author: `terraform apply` created 6 resources and the Redash dashboard loaded with BigQuery data. (The build sandbox could not download Terraform or Redash, so the author's run is the only real test.)

Terms
- **Terraform**: a tool where you describe cloud things (datasets, accounts) in text files and Terraform creates them.
- **Redash**: a web tool that runs SQL and draws charts on a dashboard.
- **Service account**: a robot login for a program (here: Redash) with only the permissions it needs.

## Part A: Terraform (BigQuery datasets as code)

### A1. Install Terraform
1. Open Command Prompt. Run: `winget install Hashicorp.Terraform`
2. Close the window, open a NEW Command Prompt, run: `terraform version`
   Expect a line like `Terraform v1.x.x`.
   If `winget` is not found: download the Windows AMD64 zip from developer.hashicorp.com/terraform/install, unzip `terraform.exe` into `C:\terraform`, add `C:\terraform` to the PATH (Start > "Edit the system environment variables" > Environment Variables > Path > Edit > New), open a new window and retry.

### A2. Check your Google login and settings
1. Run: `gcloud auth application-default login` and sign in (you did this in Week 3; Terraform uses the same login).
2. Run: `echo %GCP_PROJECT%` and `echo %BQ_LOCATION%`. Write both down. If they print the text back with percent signs, the variable is not set in this window: set them again (`set GCP_PROJECT=your-project-id`, `set BQ_LOCATION=US`, using YOUR values from Week 3).
3. Run: `gcloud auth application-default set-quota-project %GCP_PROJECT%`

### A3. Turn on two Google APIs (needed only for the read-only Redash account)
1. Run: `gcloud services enable iam.googleapis.com cloudresourcemanager.googleapis.com --project %GCP_PROJECT%`
2. If it says billing is required: do not set up billing. Open `terraform\terraform.tfvars` later and set `create_redash_user = false`; in Part B create the service account by hand (B2).

### A4. Delete the three old datasets (Terraform must create them itself)
The datasets `raw`, `staging`, `marts` already exist because the loader and dbt created them. Terraform would refuse ("already exists"), so delete them; step A6 reloads everything.
1. Open console.cloud.google.com/bigquery and select your project.
2. In the Explorer panel, for each of `raw`, `staging`, `marts`: click the three dots > Delete > type `delete` > confirm.

### A5. Fill in your settings
1. In the project folder, open the `terraform` folder.
2. Copy `terraform.tfvars.example` to `terraform.tfvars` (in cmd: `copy terraform\terraform.tfvars.example terraform\terraform.tfvars`).
3. Open `terraform.tfvars` in Notepad. Set `project_id` to your project id and `location` to the SAME value as BQ_LOCATION. Save.

### A6. Run Terraform
1. `cd terraform`
2. `terraform init`  (downloads the Google plugin; last line should say "Terraform has been successfully initialized")
3. `terraform plan`  (a preview, changes nothing). Expect `Plan: 6 to add, 0 to change, 0 to destroy.` (3 datasets, 1 service account, 2 permissions; 3 if you turned the Redash account off).
4. `terraform apply`, read the list, type `yes`. Expect `Apply complete! Resources: 6 added`.
5. Check the BigQuery console: `raw`, `staging`, `marts` are back (empty).
6. Reload the data, from the project folder with the venv active (same as Week 4, Part 2):
   - `python ingest\load_bigquery.py --batch-dir data\raw\batch_001 --mode replace`
   - `cd dbt_project`
   - `dbt build --target bq_sandbox --profiles-dir .`
   - run `scripts\bq_verify.sql` in the BigQuery console: all six must match, commission 709496.31.
7. Do NOT run `terraform destroy` unless you want the datasets and their data deleted.

Common errors
- `Error 409: Already Exists`: step A4 was skipped for a dataset. Delete it and apply again.
- `Error 403 ... API has not been used`: run the A3 command, wait a minute, apply again.
- `could not find default credentials`: repeat A2 step 1.

## Part B: Redash (dashboard)

Needs Docker Desktop running (you used it for Redpanda).

### B1. Create the secrets file
From the project folder: `python redash\make_env.py` (writes `redash\.env` with random secrets; it is ignored by git).

### B2. Get a key for the read-only account
1. console.cloud.google.com > IAM & Admin > Service Accounts. Click `redash-reader` (created by Terraform).
   (If you turned it off: Create service account, name `redash-reader`, give roles "BigQuery Job User" on the project and "BigQuery Data Viewer" on the dataset `marts`.)
2. Keys tab > Add key > Create new key > JSON > Create. A file downloads. Move it OUT of the project folder (for example Documents). Never put it on GitHub.

### B3. Start Redash
1. `docker compose -f redash/docker-compose.redash.yml run --rm server create_db` (first time only; creates Redash's own tables; the first run downloads images, a few minutes).
2. `docker compose -f redash/docker-compose.redash.yml up -d`
3. Wait about 1 minute, open http://localhost:5000 and create your admin account (any name, email, password).
Stop later with `docker compose -f redash/docker-compose.redash.yml down`.

### B4. Connect BigQuery
1. Click the gear icon > Data Sources > New Data Source > BigQuery.
2. Name: `delivery`. Project ID: your project id. Location: same as BQ_LOCATION. Upload the JSON key from B2. Create, then "Test Connection" (expect Success).

### B5. Create the 7 queries
For each file in `redash\queries` (01 to 07): Create > Query, choose data source `delivery`, paste the file's SQL, press Execute (expect rows), name it after the file, Save.
Then New Visualization on each:

| Query | Visualization | Settings |
|---|---|---|
| 01 | Chart, Line | X = day; Y = orders_placed, orders_7d_avg; gmv on right axis |
| 02 | Chart, Line | X = day; Y = avg_delivery_minutes, p90_delivery_minutes |
| 03 | Chart, Line | X = day; Y = cancel_rate |
| 04 | Chart, Pie | X = segment; Y = customers |
| 05 | Chart, Bar | X = merchant_name; Y = gmv |
| 06 | Table | default |
| 07 | Table | default |

### B6. Build the dashboard
1. Create > Dashboard, name it `Delivery platform`.
2. Edit > Add Widget > pick each query's chart > Add to Dashboard. Do it for all 7. Arrange, then Done Editing.
3. Take a screenshot, save as `docs\dashboard.png` in the project folder (the README can show it).

## Part C: Put the project on GitHub (account hazimalrijjal)
1. github.com > New repository. Name `delivery-data-platform`. Leave "add README" unchecked. Create.
2. In the project folder: `git init`, then:
   - `git add data_gen ingest/load_raw.py requirements.txt .gitignore`, `git commit -m "Add synthetic data generator and raw ingest"`
   - `git add dbt_project scripts tests`, `git commit -m "Add dbt staging and marts with tests"`
   - `git add pipeline dags docker-compose.airflow.yml requirements-airflow.txt`, `git commit -m "Add Airflow DAG with data-quality gate"`
   - `git add ingest/load_bigquery.py requirements-bq.txt`, `git commit -m "Add BigQuery loader"`
   - `git add spark_jobs streaming docker-compose.redpanda.yml requirements-spark.txt`, `git commit -m "Add PySpark batch job and Structured Streaming"`
   - `git add terraform redash docs README.md`, `git commit -m "Add Terraform, Redash, architecture, final README"`
   - `git status` should show nothing left except ignored files. If something is left, `git add .` and commit it.
3. `git branch -M main`
4. `git remote add origin https://github.com/hazimalrijjal/delivery-data-platform.git`
5. `git push -u origin main` (a browser window asks you to sign in).
6. Open the repository page and check the README shows the diagram.
