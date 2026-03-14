# GlobalPartners Customer & Sales Analytics (AWS Glue + Athena + Streamlit)

## Overview
This project builds an end-to-end analytics pipeline for customer behavior and sales performance using AWS-native services. It ingests order and option-line data from **SQL Server (Amazon RDS)**, lands it in **S3 (raw)**, transforms it into a **curated analytics layer**, exposes it via **Athena**, and visualizes insights in a **local Streamlit dashboard**.

### Key outputs
- **Order-level revenue** including item revenue + option revenue (discounts handled via negative option price)
- **Customer daily snapshot** with evolving LTV/CLV and rolling windows (30/60/90 days)
- **RFM segmentation** and churn-risk indicators (rule-based)
- **Location performance** and discount effectiveness

---

## Architecture
**RDS SQL Server → AWS Glue (PySpark) → S3 Raw (Parquet) → AWS Glue (Curated) → S3 Curated (Parquet) → Athena → Streamlit**

- **Glue Job 1:** Extract SQL Server tables to S3 raw partitioned by `extract_date`
- **Glue Job 2:** Build curated tables partitioned by `as_of_date`
- **Athena:** External tables on curated Parquet
- **Streamlit:** Runs locally and queries Athena using `boto3`

See: `docs/architecture.md` and screenshots in `docs/screenshots/`.

---

## Datasets
Source tables in SQL Server:
- `dbo.order_items`
- `dbo.order_item_options`
- `dbo.date_dim`

Curated datasets in S3:
- `curated/facts/fact_order_revenue/`
- `curated/snapshots/customer_daily_snapshot/`
- `curated/snapshots/customer_rfm_daily/`
- `curated/facts/location_daily_perf/`

---

## AWS Resources
Region: `us-east-1`

S3 bucket (kept for curated output):
- `s3://globalpartners-niteesh-us-east-1-2026/`

Curated partition example:
- `s3://globalpartners-niteesh-us-east-1-2026/curated/facts/fact_order_revenue/as_of_date=2026-03-13/`

Athena database:
- `globalpartners_curated`

---

## How to Run (Pipeline)

### 1) Glue Job 1: RDS → S3 Raw
Script: `glue/gp-rds-to-s3-raw.py`

Writes:
- `s3://<bucket>/raw/order_items/extract_date=YYYY-MM-DD/`
- `s3://<bucket>/raw/order_item_options/extract_date=YYYY-MM-DD/`
- `s3://<bucket>/raw/date_dim/extract_date=YYYY-MM-DD/`

### 2) Glue Job 2: S3 Raw → S3 Curated
Script: `glue/gp-s3-raw-to-curated.py`

Parameters:
- `--BUCKET = globalpartners-niteesh-us-east-1-2026`
- `--EXTRACT_DATE = YYYY-MM-DD` (must match raw partition)

Writes:
- `curated/facts/fact_order_revenue/as_of_date=YYYY-MM-DD/`
- `curated/snapshots/customer_daily_snapshot/as_of_date=YYYY-MM-DD/`
- `curated/snapshots/customer_rfm_daily/as_of_date=YYYY-MM-DD/`
- `curated/facts/location_daily_perf/as_of_date=YYYY-MM-DD/`

---

## How to Run (Athena)
1. Create database:
   - `athena/ddl_create_tables.sql`
2. Repair partitions:
   - `athena/msck_repair.sql`
3. Validate:
   - `athena/validation_queries.sql`

---

## How to Run (Dashboard locally)

### Prerequisites
- Python 3.10+
- AWS credentials configured locally (`aws configure`)
- IAM permissions:
  - `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`
  - S3 access to Athena results path (example below)

### Install
```bash
pip install -r requirements.txt
