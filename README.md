# CoreTelecoms: Unified Customer Experience Data Platform

## Project Overview


**CoreTelecoms** is facing a customer retention crisis due to siloed data across Call Centers, Website Forms, and Social Media. This project implements a production-grade Modern Data Platform to unify these disparate sources into a Single Source of Truth, enabling analytics on Customer Churn and Agent Performance.

This platform automates the End-to-End lifecycle: **Ingestion** $\rightarrow$ **Data Lake** $\rightarrow$ **Warehouse** $\rightarrow$ **Transformation** $\rightarrow$ **Data Quality**.

## Architecture
The platform follows a Lakehouse Architecture using the ELT (Extract, Load, Transform) pattern.
(Place your architecture diagram here. You can draw one using draw.io exporting as png)ShutterstockExplore

## Data Flow

1. **Ingestion**: Python scripts running in **Docker and Airflow** extract data from AWS S3 (CSVs, JSON), Google Sheets, and Postgres.
2. **Data Lake**: Raw data is stored in **AWS S3** in Parquet format (partitioned and compressed).
3. **Warehousing**: Data is loaded into Snowflake using the `COPY INTO` command (Variant Pattern).
4. **Transformation**: dbt processes data through a Medallion Architecture (**Bronze** $\rightarrow$ **Silver** $\rightarrow$ **Gold**).
5. **Orchestration**: Apache Airflow schedules and manages dependencies between ingestion and transformation tasks.
6. **CI/CD**: GitHub Actions performs **linting** and auto-deploys the Airflow Docker image to **Docker Hub**. 

## Tech Stack
Category  Technology  Usage  
Cloud Provider AWS (Stockholm Region) S3 (Lake), IAM (Security), SSM (Secrets) 
Infrastructure as Code Terraform Provisioning S3, Snowflake DBs, IAM Roles 
OrchestrationApache Airflow Managing DAGs (Static Setup vs Daily Incremental)
Containerization Docker & Docker Compose encapsulating the Python runtime
Data Warehouse Snowflake Storage and Compute for Analytics 
Transformation dbt (Data Build Tool) Modeling, Testing, and Documentation 
CI/CD GitHub Actions Linting (Flake8) and Docker Hub Push LanguagesPython, SQL, HCLScripting, Querying, Infrastructure

Project Structure
```
    ├── dags/                        # Airflow DAGs (Static & Daily pipelines)
    ├── dbt/                         # dbt Project (Models, Tests, Seeds)
    │   ├── models/
    │   │   ├── staging/             # Silver Layer (Cleaning & Standardization)
    │   │   └── marts/               # Gold Layer (Dimensional Modeling)         
    │   └── macros/ 
    │       
    ├── scripts/                     # Python Extraction & Loading Logic
    │   ├── common/                  # Shared Utilities (AWS Client, Config)
    │   ├── extract/                 # Ingestion Scripts (S3, GSheets, Postgres)
    │   └── load/                    # Snowflake Loading Scripts
    │ 
    ├── terraform/ 
    │     ├── aws.tf                
    │     ├── gcp.tf           
    │     └── snowflake.tf   
    │
    ├── .github/workflows/           # CI/CD Pipeline Definitions
    ├── docker-compose.yaml          # Local Airflow Orchestration
    ├── Dockerfile                   
    ├── requirements.txt             
    └── README.md                    
```

### Key Features
1. Robust Data Ingestion
    - Incremental Loading: The pipeline checks existing files in S3 and only processes new data from Postgres and Call Logs.
    - Idempotency: Pipelines can be re-run multiple times without creating duplicate data in the Warehouse.
    - Variant Loading: Data is loaded into Snowflake as VARIANT (JSON) first, making the pipeline resilient to upstream schema changes

2. Medallion Architecture (dbt) 
    - Bronze (RAW): 1:1 copy of source data.
    - Silver (STAGING): Cleaned data. Fixes email typos (e.g., gmail.om), parses dates, and standardizes column names.
    - Gold (MARTS): Star Schema.
       - `FACT_CUSTOMER_COMPLAINTS`: Unions Call Logs, Web Forms, and Social Media into one view.
       - `DIM_CUSTOMERS`: Master customer profile with calculated tenure_days.
       - `DIM_AGENTS` : Agents data

3. Data Quality & Security
    - Data Contracts: dbt tests ensure unique IDs, not_null constraints, and referential integrity between Complaints and Customers.
    - Security: No hardcoded secrets. All credentials are managed via AWS SSM Parameter Store or injected via .env in Docker.
    
    
## How to Run Locally 
**Prerequisites**
- Docker & Docker Compose
- AWS CLI (Configured)
- Terraform

**Step 1: Infrastructure Setup**
Initialize the cloud resources
```bash
    cd terraform
    # create a terraform.tfvars file with your Snowflake/AWS credentials first

    terraform init
    terraform plan
    terraform apply
```
**Step 2: Configuration**
Create a .env file in the root directory

```Ini, TOML
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=eu-north-1
SNOWFLAKE_ACCOUNT=ORG-ACCOUNT
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SOURCE_AGENTS_SHEET_ID=your_sheet_id
```

**Step 3: Start Airflow**
Build the custom image and start the containers.

```Bash
    docker-compose up -d --build
``` 
**Step 4: Trigger Pipelines**
Access Airflow at http://localhost:8080 (User/Pass: `airflow`).
1. Run 01_setup_static_data (Once).
2. Enable 02_daily_ingestion (Scheduled).

#### Sample Insights (Gold Layer)

**Unified Complaint Analysis:**
```SQL
    SELECT
        source_channel, 
        COUNT(*) as total_complaints,
        AVG(handling_time_sec) as avg_resolution_time
    FROM CORE_TELECOMS.MARTS.FACT_UNIFIED_COMPLAINTS
    GROUP BY 1;
```

##### Future Improvements
- Dashboarding: Connect Tableau or PowerBI to the Gold Layer.
- Alerting: Implement Slack notifications in Airflow for failed tasks.
- Streaming: Move Social Media ingestion to Kinesis for real-time sentiment analysis.


Author: Temitope Bimbo Babatola: CDE Capstone Project