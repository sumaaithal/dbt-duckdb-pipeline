# 🧩 dbt-duckdb-pipeline

A dbt + DuckDB analytics pipeline for job market data. The project reads raw parquet files, stages them into clean job-posting models, and produces downstream tables for monthly trends, top locations, and remote-work analysis.

## 📊 Overview

This project answers practical business questions such as:

- 📈 How many job postings are created each month?
- 📉 Which roles are growing or declining over time?
- 🌍 Which cities or locations have the highest job demand?
- 🏠 How much of the job market supports remote work?

The pipeline follows a standard analytics engineering model:

- 🧱 raw parquet files are defined as dbt sources
- 🧹 a staging model cleans and filters the data
- 📊 aggregated models build business-ready metrics
- ✅ tests validate data quality and model integrity
- 🗃️ a generated DuckDB warehouse can be shared and queried externally

## 🗂️ Project structure

```text
dbt-duckdb-pipeline/
├── analytics/
│   └── models/
│       ├── job_postings.sql
│       ├── jobs_per_month.sql
│       ├── top_locations.sql
│       ├── monthly_summary.sql
│       ├── sources.yml
│       └── properties.yml
├── .gitignore
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── README.md
├── requirements.txt
└── target/
```

## 📦 Source data and parquet usage

The raw source is defined in `analytics/models/sources.yml`:

```yaml
sources:
  - name: raw
    description: "Job postings data from the source system."
    tables:
      - name: job_postings
        description: "Table containing job postings information."
        config:
          external_location: "read_parquet('../data/raw/raw_job_postings_*.parquet')"
```

This configuration tells DuckDB to treat all parquet files matching `raw_job_postings_*.parquet` as a dbt source table named `raw.job_postings`.

Why this matters:

- 📁 the raw layer stays lightweight and portable
- 🧰 no external database is required for local development
- ⚡ parquet files are efficient for analytics workloads
- 🔄 the raw data can be versioned and reused easily

## 🔄 Pipeline logic

### 1. 🏷️ job_postings

This is the staging model. It reads from the raw source and keeps only the fields needed for downstream analysis.

```sql
{{
  config(
    materialized='view'
  )
}}

select
  job_id,
  job_title,
  company_name,
  job_location,
  job_via,
  job_schedule_type,
  job_work_from_home,
  search_date,
  search_term
FROM {{ source('raw', 'job_postings') }}
WHERE error IS NULL
```

Key logic:

- selects relevant columns
- materializes as a view
- removes invalid records with `WHERE error IS NULL`

This is the first clean layer in the pipeline and ensures downstream models do not inherit broken data.

### 2. 📅 jobs_per_month

This model groups postings by month and role.

```sql
SELECT 
    DATE_TRUNC('month', search_date) AS month,
    search_term AS role,
    COUNT(*) AS postings
FROM {{ ref('job_postings') }}
GROUP BY month, role
ORDER BY month, role
```

Key logic:

- `DATE_TRUNC('month', search_date)` normalizes the date
- `search_term` acts as the role/category dimension
- `COUNT(*)` calculates the postings volume
- `GROUP BY month, role` creates a monthly role-level aggregate

This creates a trend table for understanding how demand changes over time.

### 3. 🌍 top_locations

This model ranks the top job locations by posting count.

```sql
SELECT 
    job_location,
    COUNT(*) AS postings
FROM {{ ref('job_postings') }}
GROUP BY job_location
ORDER BY postings DESC
LIMIT 10
```

Key logic:

- groups by `job_location`
- counts postings per location
- sorts descending
- limits to the top 10

This is useful for identifying the highest-volume hiring markets.

### 4. 📈 monthly_summary

This model combines totals and remote-job share.

```sql
WITH totals AS (
    SELECT month, SUM(postings) AS total_postings
    FROM {{ ref('jobs_per_month') }}
    GROUP BY month
),

remote AS (
   SELECT 
        DATE_TRUNC('month', search_date) AS month,
        COUNT(*) AS remote_postings
    FROM {{ ref('job_postings') }}
    WHERE job_work_from_home
    GROUP BY month
)

SELECT 
    month,
    total_postings,
    remote_postings,
    ROUND(100.0 * remote_postings / total_postings,1) AS pct_remote
FROM totals
JOIN remote USING (month)
ORDER BY month
```

Key logic:

- totals calculates total postings per month
- remote counts only jobs where `job_work_from_home` is true
- the final query combines both sets and calculates remote percentage
- `pct_remote` shows how much of the monthly market supports remote work

This is the most business-facing metric layer in the pipeline.

## ✅ dbt tests and quality checks

The project uses dbt tests to validate the quality of the transformed data. These checks are defined in `analytics/models/properties.yml`.

Example:

```yaml
models:
  - name: job_postings
    description: "Model that selects relevant columns from the raw job postings data."
    config:
      persist_docs:
        relation: true
        columns: true
    columns:
      - name: job_id
        description: "Unique identifier for each job posting."
      - name: job_title
        description: "Title of the job posting."
        tests:
          - not_null
      - name: job_location
        description: "Location of the job posting."

  - name: monthly_summary
    description: "Model that provides a monthly summary of job postings."
    columns:
      - name: month
        description: "Month when the job postings were searched or retrieved."
        tests:
          - unique
          - not_null
```

This ensures:

- ⚠️ important columns are not missing
- 📅 monthly outputs are unique and stable
- 🧠 the data is safe to use in downstream analysis

These tests help prevent broken transformations from entering the warehouse unnoticed.

## 🚀 GitHub Actions and generated warehouse

This project is structured to support automated warehouse generation and release-based sharing.

The generated DuckDB warehouse is published here:

```text
https://github.com/sumaaithal/dbt-duckdb-pipeline/releases/download/warehouse/prod.duckdb
```

This enables a workflow where:

1. 🤖 GitHub Actions runs the dbt pipeline
2. 🗃️ the DuckDB warehouse is regenerated
3. 📤 the final `prod.duckdb` file is uploaded to the release
4. 🔍 the warehouse can be downloaded and queried live

This is useful for:

- 👥 sharing the latest build with teammates
- 🧪 validating outputs outside the local environment
- 📊 querying the final dataset without rerunning dbt locally

Example query against the downloaded warehouse:

```bash
curl -L -o prod.duckdb https://github.com/sumaaithal/dbt-duckdb-pipeline/releases/download/warehouse/prod.duckdb
```

Then query it:

```sql
ATTACH 'prod.duckdb' AS warehouse (READ_ONLY);
SELECT *
FROM warehouse.main.monthly_summary
ORDER BY month DESC
LIMIT 10;
```

## ⚙️ Setup

### 🧰 Prerequisites

- Python 3.9+
- pip
- Git
- dbt Core
- DuckDB adapter for dbt

### 📥 Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install dbt-duckdb
```

### 🔎 Verify the environment

```bash
dbt --version
dbt debug
```

## 🏃 Run the project

```bash
dbt deps
dbt run
dbt test
dbt build
```

## 📚 Generate documentation

```bash
dbt docs generate
dbt docs serve
```

## 💡 Why this pipeline is useful

This project demonstrates a practical dbt workflow for local analytics:

- 🧱 raw parquet files become source tables
- 🧹 staging models normalize and clean data
- 📊 aggregation models create analytical metrics
- ✅ tests enforce quality checks
- 🗃️ the final warehouse can be published and queried live

It is small enough to understand quickly, but structured enough to reflect a real-world analytics engineering setup.

## 🧾 Summary

This repo is a lightweight analytics pipeline built with dbt and DuckDB. It turns raw job-posting data into meaningful reporting tables by:

- 🧹 staging and cleaning source data
- 📅 aggregating by time and role
- 🌍 ranking top locations
- 🏠 measuring remote-job share
- ✅ validating data through dbt tests
- 🚀 publishing the warehouse for external querying

This makes the project a solid example of a clean, maintainable, and testable dbt pipeline.

### 👩‍💻 Author
- Suma Aithal (Analytics Engineer)