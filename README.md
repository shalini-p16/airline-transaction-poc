# Airline Transaction PoC
This project is PoC demonstrating how to load and analyze airline transactions data using an end-to-end data pipeline.
It showcases the integration of several modern data tools—Apache Airflow for orchestration, MinIO for object storage, ClickHouse as the OLAP database, DBT for data transformation and modeling, and Metabase for data visualization. The pipeline ingests JSON files stored in MinIO, transforms the raw data via DBT models into analytics-ready marts, and visualizes key insights through automatically generated dashboards in Metabase.

## 🔍 Key Highlights
- There is no official Airflow operator for loading data from MinIO to ClickHouse, so I developed a custom operator that:
    - Reads JSON files from MinIO
    - Automatically infers the schema
    - Creates or replaces ClickHouse tables
    - Loads data into ClickHouse
- I designed the setup to be easy to run and automated most prerequisites.
- This PoC gave me an opportunity to practice using Airflow 3, MinIO, and ClickHouse.

## 📌 Architecture Diagram

![Architecture Diagram](./docs/architecture-diagram.png) 

## 🛠️ Technologies Used
- Apache Airflow
- MinIO
- ClickHouse
- DBT
- Metabase
- Docker Compose
- Python
- Cosmos

## 🧱 Docker Compose Components

The `docker-compose.yml` file includes the following services:

| Service                   | Description                                                                             |
| ------------------------- | --------------------------------------------------------------------------------------- |
| **postgres**              | Backend database for Airflow metadata. Stores DAG runs, task instances, logs, and more. |
| **airflow-apiserver**     | REST API server for Apache Airflow (v3), exposes DAG and task management endpoints.    |
| **airflow-scheduler**     | Schedules Airflow DAGs and triggers task execution.                                     |
| **airflow-init**          | Initializes the Airflow environment (creates folders, user, and runs DB migrations).    |
| **airflow-cli**           | Debug container to run Airflow CLI commands manually.                                   |
| **airflow-dag-processor** | Separates DAG parsing and validation from the scheduler for performance.                |
| **minio**                 | S3-compatible object storage.          |
| **create-minio-bucket**   | Helper job that creates a bucket (`search-analytics`) in MinIO during initialization.   |
| **clickhouse**            | High-performance columnar OLAP database used as the analytics backend.                  |
| **dbt-docs-generator**    | Builds `dbt` documentation from models and metadata (ClickHouse-specific).              |
| **dbt-docs-server**       | Lightweight web server (nginx) to serve `dbt` documentation UI.                         |
| **metabase**              | Open-source BI tool used for building dashboards and exploring ClickHouse data.         |


## 🌐 Web Interfaces

| Component | URL                                            | Credentials                |
| --------- | ---------------------------------------------- | -------------------------- |
| Airflow   | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow`        |
| MinIO     | [http://localhost:9000](http://localhost:9000) | `minioadmin` / `minioadmin`  |
| DBT Docs  | [http://localhost:8085/index.html](http://localhost:8085/index.html) | *No login required*        |
| Metabase  | [http://localhost:3000](http://localhost:3000) | `test@test.com` / `test4567` |
| Clickhouse| [http://localhost:8123/play](http://localhost:8123/play) | `default` / `1234` |

## 🚀 Startup Steps
1. Start Docker Compose:

```bash
docker compose up -d
```

2. Upload JSON files to MinIO:
Visit [http://localhost:9000](http://localhost:9000) and upload your files to the `search-analytics` bucket.
![minio screenshot](./docs/minio_screenshot.png) 

3. Run the Airflow DAG `load_raw_json_files` to load data into ClickHouse.
http://localhost:8080/dags/load_raw_json_files
![dag1 screenshot](./docs/dag1_screenshot.png) 

4. Run the Airflow DAG `airline_transactions_dbt_dag` to trigger DBT transformations.
http://localhost:8080/dags/airline_transactions_dbt_dag
![dag2 screenshot](./docs/dag2_screenshot.png) 

## 📊 SQL Inights

### ERD Diagram



1. **From which Country are most transactions originating? How many transactions is this?.**

```sql
SELECT
  OriginCountry,
  COUNT(*) AS transaction_count
FROM
  int_transactions_enriched
GROUP BY
  OriginCountry
ORDER BY
  transaction_count DESC
```

2. **What's the split between domestic vs international transactions?**


3. **What's the distribution of number of segments included in transactions?.**

```sql
SELECT
  NumberOfSegments AS num_segments,
  count(*) AS transaction_count
FROM
  default.int_transactions_enriched
GROUP BY
  NumberOfSegments
ORDER BY
  NumberOfSegments;
```

## 🛠️ Data Modeling (DBT)
Since this is a PoC without access to real stakeholders, I assumed possible business needs and created 1 mart models:
![DBT Diagram](./docs/dbt_screenshot.png) 

## 📈 BI Dashboards
The dashboards in this project were automatically generated by Metabase based on the underlying ClickHouse tables and DBT models

Note: I’ve included the Metabase SQLite database in the repo for easier access and instant exploration. This is not a best practice for production but acceptable for a PoC.

![BI screenshot](./docs/bi1_screenshot.png) 
![BI screenshot](./docs/bi2_screenshot.png) 
![BI screenshot](./docs/bi3_screenshot.png)

## 📁Project Structure 
```
.
├── README.md                        # Project overview, setup instructions, and SQL insights
├── airflow                          # Contains all Airflow-related configurations and DAGs
│   ├── config
│   │   └── airflow.cfg              # Custom Airflow configuration (optional override)
│   ├── dags                         # Airflow DAG definitions and custom operators
│   │   ├── __init__.py
│   │   ├── operators                # Custom Airflow operators
│   │   │   ├── __init__.py
│   │   │   └── minio_to_clickhouse # Custom operator for loading data from MinIO to ClickHouse
│   │   │       ├── MinIOToClickHouseOperator.py  # Operator logic
│   │   │       └── __init__.py
│   │   ├── search_analytics_dbt.py          # DAG to run DBT models
│   │   └── search_analytics_load_raw_files.py  # DAG to load raw CSVs into ClickHouse
│   ├── data                        # (Optional) Place for temporary data files or uploads
│   ├── docker
│   │   ├── Dockerfile              # Airflow custom image Dockerfile
│   │   └── requirements.txt        # Python package dependencies for Airflow
│   ├── logs                        # Runtime logs generated by Airflow
│   └── plugins                     # Airflow plugin folder (if extended functionality is added)
├── clickhouse                      # ClickHouse configuration and logs
│   ├── config
│   │   ├── config.xml              # Main ClickHouse server config
│   │   ├── default-user.xml        # Default user settings
│   │   └── users.xml               # User roles and permissions
│   └── logs                        # Logs from the ClickHouse container
├── dbt                             # DBT project for data modeling
│   ├── dbt_packages                # Installed DBT packages (auto-generated)
│   ├── dbt_project.yml             # DBT project configuration
│   ├── logs                        # Logs from DBT runs
│   ├── models                      # DBT models organized by layer
│   │   ├── intermediate            # Intermediate transformation models
│   │   │   ├── int_all_events_union.sql
│   │   │   ├── int_search_sessions.sql
│   │   │   └── int_user_profiles.sql
│   │   ├── marts                   # Final, business-ready models
│   │   │   ├── mart_all_events.sql
│   │   │   ├── mart_daily_traffic.sql
│   │   │   ├── mart_keywords_summary.sql
│   │   │   ├── mart_search_activity.sql
│   │   │   └── mart_user_engagement.sql
│   │   ├── schema.yml              # Descriptions and tests for staging models
│   │   └── staging                 # Raw table models for initial loading
│   │       ├── stg_clicks.sql
│   │       ├── stg_custom_events.sql
│   │       ├── stg_groups.sql
│   │       ├── stg_keywords.sql
│   │       └── stg_searches.sql
│   ├── profiles.yml                # DBT profile config for connecting to ClickHouse
│   ├── target                      # Compiled DBT artifacts (auto-generated)
│   └── tests                       # Optional folder for DBT tests
├── dbt-docs
│   └── Dockerfile                  # Dockerfile for serving DBT docs with Nginx
├── docker-compose.yaml             # Docker Compose setup for all services
├── docs                            # Documentation assets (e.g., screenshots, diagrams)
├── metabase
│   └── data                        # Metabase SQLite file (for preloaded dashboards)
└── minio
    └── data                        # MinIO data directory (auto-mounted in container)

```

# 🔮 Future Work
In a real-world production setup, these improvements would be essential:
- Move infrastructure to Kubernetes
- Implement unit testing for Airflow and DBT
- Apply proper authentication and security practices
- Automate DBT doc generation in CI/CD
- Manage MinIO buckets via Terraform
- Move Airflow connection setup outside docker-compose into a secure, centralized solution
