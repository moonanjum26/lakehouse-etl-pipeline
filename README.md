🏗️ AWS Lakehouse ETL Pipeline using Glue, Apache Iceberg, S3 & Redshift (Fully Automated with Terraform)

This repository contains a production-grade lakehouse data pipeline built on AWS.
It demonstrates how modern data engineering teams design incremental ETL pipelines using:

AWS Glue (PySpark) for transformations & merges

Apache Iceberg for ACID, schema evolution & upserts

Amazon S3 as Bronze, Silver & Gold layers

Amazon Redshift Serverless as the warehouse

Terraform for complete infrastructure automation

The pipeline ingests raw data from S3, performs SCD-1 upserts into Iceberg, creates a curated Gold layer, and loads it incrementally into Redshift using an idempotent COPY pattern.

This is a real-world lakehouse architecture similar to what large-scale companies deploy in production.

🚀 Key Capabilities
🔹 1. Automated Infrastructure-as-Code (Terraform)

Terraform provisions the entire pipeline:

Raw / Processed / Curated S3 buckets

AWS Glue Job, Roles, Triggers

Redshift Serverless Namespace & Workgroup

IAM roles & fine-grained permissions

Networking + Security policies

This ensures fully reproducible, version-controlled infrastructure.

🔹 2. Incremental Ingestion with Watermarking

Instead of scanning all S3 files, the Glue job:

Reads Iceberg table → extracts max(processed_date)

Loads only files newer than the watermark

Completely avoids reprocessing

💡 Result → Low cost, faster job runtime, scalable processing

🔹 3. SCD-1 Upserts via Apache Iceberg MERGE

The ETL job implements full SCD-1 merge logic:

Updates existing rows

Inserts new rows

Handles schema evolution dynamically (adds missing columns)

Leverages Iceberg’s ACID + metadata pruning

Exactly how enterprise pipelines maintain clean, consistent dimension/fact tables.

🔹 4. Curated (Gold) Layer Generation

A clean, analytics-ready dataset is written into the curated bucket:

Reduced & selected business columns

Optimized file layout

Ready for BI teams & downstream systems

🔹 5. Idempotent Redshift Incremental Loading

To avoid duplicating rows in the warehouse:

Each successfully loaded file is marked

COPY command loads only unprocessed curated files

The load is repeatable, safe, and fault-tolerant

This is a standard pattern used in most S3 → Redshift ETL pipelines.

🧠 Architecture Diagram
S3 (Raw Layer)
      ↓
AWS Glue (PySpark ETL)
      ↓  Incremental Load + SCD-1 Merge
Iceberg Table (Processed / Silver Layer)
      ↓
Curated S3 (Gold Layer)
      ↓
Amazon Redshift Serverless
(Incremental Idempotent COPY)

🧪 ETL Logic (Summary)
SCD-1 MERGE
MERGE INTO processed_db.orders t
USING incoming s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...

Iceberg Table Properties

format-version = 2

write.upsert.enabled = true

write.merge.mode = merge-on-read

Watermark Logic

Reads only:
folder_date > max(processed_date in Iceberg)

🛠️ How to Deploy
1. Deploy Infra
cd terraform/
terraform init
terraform apply

2. Upload Raw Files to S3

AWS Glue triggers automatically or manually.

3. Run Glue ETL

Incremental read

Transformations

SCD-1 merge

Write to Iceberg

Write curated layer

4. Load Redshift
COPY table FROM 's3://curated-bucket/gold/'
IAM_ROLE '<RoleARN>'
FORMAT AS PARQUET;

🎯 What This Project Demonstrates

✔ Modern Lakehouse architecture
✔ Incremental ingestion patterns
✔ SCD-1 merge using Iceberg
✔ Terraform IaC for end-to-end setup
✔ Glue ETL best practices
✔ Idempotent warehouse loading
✔ Enterprise-level folder organization

This is a high-quality E2E project suitable for portfolios, interviews, and LinkedIn.

📫 Contact

Made with ❤️ by **Mahwish Anjum**  
[Connect with me on LinkedIn](https://www.linkedin.com/in/mahwish-anjum-61a84347/)
