# 🍫 ChocoFlow: End-to-End Chocolate Sales Data Pipeline

<div align="center">

<img src="https://img.shields.io/badge/Python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54"/>
<img src="https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white"/>
<img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0id2hpdGUiPjxwYXRoIGQ9Ik0yMS44IDQuMkwxOS44IDIuMkMxOS40IDEuOCAxOC44IDEuOCAxOC40IDIuMkwxMiA4LjZMNS42IDIuMkM1LjIgMS44IDQuNiAxLjggNC4yIDIuMkwyLjIgNC4yQzEuOCA0LjYgMS44IDUuMiAyLjIgNS42TDguNiAxMkwyLjIgMTguNEMxLjggMTguOCAxLjggMTkuNCAyLjIgMTkuOEw0LjIgMjEuOEM0LjYgMjIuMiA1LjIgMjIuMiA1LjYgMjEuOEwxMiAxNS40TDE4LjQgMjEuOEMxOC44IDIyLjIgMTkuNCAyMi4yIDE5LjggMjEuOEwyMS44IDE5LjhDMjIuMiAxOS40IDIyLjIgMTguOCAyMS44IDE4LjRMMTUuNCAxMkwyMS44IDUuNkMyMi4yIDUuMiAyMi4yIDQuNiAyMS44IDQuMloiLz48L3N2Zz4="/>
<img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
<img src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black"/>
<img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
<img src="https://img.shields.io/badge/Cube.dev-7B61FF?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0id2hpdGUiPjxwYXRoIGQ9Ik0xMiAyTDIgN3YxMGwxMCA1IDEwLTVWN0wxMiAyem0wIDIuMThMMjAgOC41djdMMTIgMTkuODIgNCAxNS41di03TDEyIDQuMTh6TTEyIDZMNiA5djZsNiAzIDYtM1Y5bC02LTN6Ii8+PC9zdmc+"/>
<img src="https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0iYmxhY2siPjxyZWN0IHg9IjIiIHk9IjE0IiB3aWR0aD0iNCIgaGVpZ2h0PSI4IiByeD0iMSIvPjxyZWN0IHg9IjgiIHk9IjkiIHdpZHRoPSI0IiBoZWlnaHQ9IjEzIiByeD0iMSIvPjxyZWN0IHg9IjE0IiB5PSI0IiB3aWR0aD0iNCIgaGVpZ2h0PSIxOCIgcng9IjEiLz48cmVjdCB4PSIyMCIgeT0iMTEiIHdpZHRoPSIyIiBoZWlnaHQ9IjExIiByeD0iMSIvPjwvc3ZnPg=="/>

</div>

<p align="center"><em>A production-style, event-driven data pipeline that turns raw chocolate sales CSVs into a fully governed star-schema warehouse and an interactive Power BI dashboard — all orchestrated, containerized, and reproducible.</em></p>

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [Architecture Diagram](#️-architecture-diagram)
3. [Tech Stack & Environment](#️-tech-stack--environment)
4. [Project Folder Structure](#-project-folder-structure)
5. [Data Flow Walkthrough](#-data-flow-walkthrough)
6. [Data Warehouse Design (Star Schema)](#-data-warehouse-design-star-schema)
7. [Semantic Model Layer (Cube.dev)](#-semantic-model-layer-cubedev)
8. [Dashboard & Visualization (Power BI)](#-dashboard--visualization-power-bi)
9. [Acknowledgments](#-acknowledgments)

---

## 🎯 Project Overview

**ChocoFlow** is a modern, end-to-end data engineering and analytics project that demonstrates a production-ready, event-driven pipeline for processing and analyzing chocolate sales data. It takes raw CSV files all the way to an interactive business dashboard — solving the common problem of siloed, manual reporting by building a fully automated, scalable system that ingests, models, and visualizes sales metrics.

**Key Features:**
- 📥 Raw data ingestion from CSV files using Python
- 🔄 Event-driven streaming architecture with Apache Kafka (KRaft mode)
- ⏱️ Pipeline orchestration and scheduling via Apache Airflow
- 🗄️ Centralized data storage using PostgreSQL
- 🧱 Reliable data transformation and testing with dbt (data build tool)
- 📐 Consistent metric definitions via a semantic layer with Cube.dev
- 📊 Interactive visualizations and business intelligence using Power BI
- 🐳 Fully containerized deployment for reproducibility with Docker

---

## 🗺️ Architecture Diagram

The pipeline is orchestrated using Apache Airflow and follows a multi-hop **medallion architecture** to incrementally process, refine, and model the chocolate sales data.

<div align="center">
  <img src="airflow-projects/Flow%20Diagrams/Data%20Pipeline%20Diagram.png" alt="Data Pipeline Architecture Diagram" width="90%"/>
</div>

**Data Flow & Pipeline Layers:**

| Layer | What happens |
|---|---|
| 🥉 **Ingestion (Bronze)** | Raw data is read from the CSV file by a Kafka Producer, streamed through Kafka topics in **KRaft mode**, and ingested into a **DuckDB** database by a Kafka Consumer. |
| 🥈 **Transformation (Silver)** | **dbt** executes models against the DuckDB database to clean and structure the raw data. |
| 🥇 **Warehousing (Gold)** | The staged tables are transferred from DuckDB into a **PostgreSQL** Data Warehouse. A second set of **dbt** transformations builds the final star schema (Fact and Dimension tables) and generates surrogate keys. |
| 🧠 **Semantic Layer & BI** | **Cube.dev** connects to the PostgreSQL warehouse to define a unified semantic model, queried by **Power BI** for the final interactive dashboards. |
| ⚙️ **Orchestration & Infra** | **Apache Airflow** manages dependencies, scheduling the ingestion DAG and triggering the warehousing DAG only upon successful ingestion. It also acts as the notification center for task success or failure. Everything runs inside **Docker**. |

---

## 🛠️ Tech Stack & Environment

This project leverages a modern, fully containerized data stack to ensure reproducibility, scalability, and easy local development.

### 🧰 The Technology Stack

| Technology | Role / Layer | Description |
| :--- | :--- | :--- |
| **Python** | Data Ingestion | Reads raw chocolate sales CSV files and acts as the producer sending batches to Kafka. |
| **Apache Kafka** | Streaming (Bronze) | A 3-node cluster running in **KRaft mode** (Zookeeper-less) for reliable, high-throughput event streaming. |
| **DuckDB** | Staging | Blazing-fast, in-process analytical database used by consumers to land and stage raw Kafka events. |
| **dbt** | Data Transformation | Executes SQL models to clean staging data (Silver) and build the final star schema (Gold). |
| **PostgreSQL** | Data Warehouse (Gold) | Persistent relational database storing the final Fact and Dimension tables. |
| **Cube.dev** | Semantic Layer | Headless BI server that centralizes metric definitions and serves them via a unified API. |
| **Power BI** | Visualization | Connects to the semantic model to deliver interactive, real-time chocolate sales dashboards. |
| **Apache Airflow** | Orchestration | Manages DAG scheduling, handles task dependencies, and manages alerting. |
| **Docker** | Infrastructure | Containerizes all services, ensuring consistent environments and network isolation. |

### ⚙️ Environment Setup & Architecture Notes

The infrastructure is divided into data processing services and pipeline orchestration, all running seamlessly via Docker.

- **Core Infrastructure (`docker-compose.yaml`):** A single compose file spins up the 3-node Kafka cluster, PostgreSQL data warehouse, pgAdmin, the DuckDB staging environment, the dbt execution container, and the Cube.dev server. All services communicate securely over a custom Docker bridge network (`de_network`).
- **Orchestration (Astro CLI):** Apache Airflow is deployed on Docker using the **Astronomer (Astro) CLI**. This runs alongside the core infrastructure, allowing Airflow to orchestrate the ingestion and warehousing DAGs while keeping orchestration dependencies cleanly separated from the data processing engines.
- **Data Persistence:** Local volume mapping is used across the stack (e.g., `./include/postgres_db` for PostgreSQL and `./kafka_data` for the brokers) to ensure no data is lost when containers are spun down.

<br/>
<div align="center">
  <a href="airflow-projects/docker-compose.yaml">
    <img src="https://img.shields.io/badge/🐳_View_docker--compose.yaml-2496ED?style=for-the-badge&logoColor=white" alt="View Docker Compose File" />
  </a>
</div>

---

## 📂 Project Folder Structure

```text
airflow-projects/
│
├── 📄 docker-compose.yaml          # Core infrastructure orchestration
├── 📄 PROJECT_DOCUMENTATION.md     # Detailed pipeline documentation
│
├── 📂 airflow/                     # Apache Airflow environment
│   ├── 📂 dags/                    # Pipeline schedules (Ingestion & Warehousing)
│   └── 📂 include/                 # Core pipeline logic
│       ├── 📂 consumers/           # Kafka Consumer scripts (to DuckDB)
│       ├── 📂 producers/           # Kafka Producer scripts (from CSV)
│       │
│       └── 📂 dbt/                 # Multi-database dbt projects
│           ├── 📂 chocolate_duckdb/      # SILVER LAYER: Staging & Cleaning
│           │   └── 📂 models/
│           │       ├── 📂 ods/
│           │       │   └── obt_chocolate_denormalized.sql
│           │       └── 📂 stg/
│           │           ├── stg_dim_customers.sql
│           │           ├── stg_dim_locations.sql
│           │           ├── stg_dim_products.sql
│           │           ├── stg_dim_stores.sql
│           │           └── stg_fact_sales.sql
│           │
│           └── 📂 chocolate_postgres/    # GOLD LAYER: Data Warehouse Schema
│               └── 📂 models/
│                   ├── dim_locations.sql
│                   ├── dim_products.sql
│                   ├── dim_stores.sql
│                   └── fact_sales.sql
│
├── 📂 cube_project/                # Headless Semantic Layer
│   └── 📂 model/
│       ├── 📂 cubes/               # Unified Metric Definitions
│       │   ├── dim_customers.yml
│       │   ├── dim_locations.yml
│       │   ├── dim_products.yml
│       │   ├── dim_stores.yml
│       │   ├── dwh_dim_calendar.yml
│       │   └── fact_sales.yml
│       │
│       └── 📂 views/               # Business-facing views
│           ├── example_view.yml
│           └── sales_analytics.yml
│
├── 📂 PowerBI Dashboard/           # Final visualization
│   └── Chocolate Sales Dashboard.pbix
│
└── 📂 My Datasets/                 # Raw source data files
    └── 📂 Chocolate Sales/         # Raw CSV extracts
```

---

## 🌊 Data Flow Walkthrough

The pipeline processes chocolate sales data spanning from **2023-01-01 to 2024-12-31**. The architecture is heavily focused on idempotency, fault tolerance, and strict sequential loading.

Original Kaggle data source:
<a href="https://www.kaggle.com/code/ssssws/chocolate-sales"><img src="https://img.shields.io/badge/Kaggle-20BEFF?style=for-the-badge&logo=Kaggle&logoColor=white" alt="Kaggle Dataset"/></a>

Alternatively, the raw CSV files are stored locally in the repository at:
`airflow-projects/My Datasets/Chocolate Sales`

### Phase 1 — Data Ingestion (The Bronze Layer)
*Managed by `chocolate_producer_dag` and Kafka Consumers*

<div align="center">
  <img src="airflow-projects/Dags%20images/Chocolate_Ingestion-graph.png" alt="Chocolate Ingestion DAG Graph" width="85%" />
  <p><em>Airflow graph view of the Ingestion DAG — a <code>BranchPythonOperator</code> checks for new data before deciding whether to consume into DuckDB or send a warning email, with success logging and email notification at the end.</em></p>
</div>
<br/>

1. **Kafka Topic Initialization** — the Kafka cluster is initialized with four distinct topics, populated by `produce_to_kafka`:
   - `chocolate_sales` (3 partitions, 3 replications)
   - `chocolate_products` (1 partition, 3 replications)
   - `chocolate_stores` (1 partition, 3 replications)
   - `chocolate_customers` (1 partition, 3 replications)

2. **Stateful Batch Producing** — instead of a simple dump, the Python producer streams data incrementally, month by month, using a local memory state file to track progress.

   State example:
   ```json
   {"target_month": "2023-01", "retry_count": 0, "max_retries": 3}
   ```
   **Fault Tolerance:** if a month's data is successfully loaded, the state advances to the next month. If data is missing or fails, it retries up to 3 consecutive runs — then gracefully moves on, leaving a gap rather than crashing the pipeline.

   <a href="airflow-projects/airflow/include/producers/producer_logic.py"><img src="https://img.shields.io/badge/View_Producer_Logic-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Producer Logic"/></a>

3. **Smart Consumption & Alerting** — `check_for_new_data` (a `BranchPythonOperator`) decides the path: if there's no new data, `send_warning_email` fires immediately. If new data exists, `consume_to_duckdb` reads from the topics by Kafka offset and lands it into raw Bronze tables in **DuckDB**, followed by `log_bronze_success` and `send_success_email`.

4. **Data-Aware Scheduling** — the populated DuckDB database is registered in Apache Airflow as an **Asset** (Dataset). A successful update of this Asset triggers the next phase automatically.

### Phase 2 — Transformation & Warehousing (Silver & Gold Layers)
*Triggered automatically by the DuckDB Asset via `chocolate_warehousing_dag`*

<div align="center">
  <img src="airflow-projects/Dags%20images/Chocolate_Staging_and_Transfer-graph.png" alt="Chocolate Staging and Transfer DAG Graph" width="95%" />
  <p><em>Airflow graph view of the Warehousing DAG — although the staging tasks (<code>run_stg_fact_sales</code>, <code>run_stg_dim_customers</code>, <code>run_stg_dim_locations</code>, <code>run_stg_dim_products</code>, <code>run_stg_dim_stores</code>) appear visually parallel, they actually run <strong>sequentially</strong>, gated by a single-slot Airflow pool — DuckDB cannot handle concurrent read/write connections, so each staging model waits its turn before the next one starts.</em></p>
</div>
<br/>

1. **Silver Layer (dbt + DuckDB)**
   - **OBT Creation:** dbt joins the raw tables into a single One Big Table (OBT) via `run_obt_model`.
   - **Staging:** dbt then breaks the OBT down into normalized staging tables aligned with the target star schema. Each staging model (`run_stg_fact_sales`, `run_stg_dim_customers`, `run_stg_dim_locations`, `run_stg_dim_products`, `run_stg_dim_stores`) is constrained to a **single-slot Airflow pool**, forcing strictly sequential execution — DuckDB is single-writer and cannot safely serve two simultaneous read/write transactions, so true parallelism here would corrupt or block the database.

2. **Data Transfer**
   - The cleaned staging tables are extracted from DuckDB and loaded into the `public` schema of the target **PostgreSQL Data Warehouse**.

3. **Gold Layer (dbt + PostgreSQL)**
   - **Dimension Loading:** data moves from the `public` schema into the final `dwh` schema. Dimensions load *first* and in parallel — `run_dwh_stores`, `build_calendar_dim`, `run_dbt_customers_snapshot`, `run_dwh_locations`, and `run_dwh_products` all fan out from `transfer_stg_to_postgres`. **Slowly Changing Dimension (SCD Type 2)** logic is applied via `run_dbt_customers_snapshot` to track historical changes in `dim_customers`.
   - **Fact Loading:** once all dimensions are loaded, `run_dwh_fact_sales` runs, performing lookup joins against the dimension tables to retrieve surrogate keys.

4. **Idempotency & Audit Trails**
   - **Success/Failure Alerting:** a final status email is dispatched via `send_success_email` to the admin account.
   - **Idempotent Cleanup:** `truncate_duckdb_staging` runs to clear the raw tables in DuckDB, guaranteeing no duplicate data on rerun.
   - **Auditing:** `log_gold_success` records the final DAG execution status in a dedicated audit table for pipeline observability.

---

## 🌟 Data Warehouse Design (Star Schema)

The PostgreSQL Data Warehouse (Gold Layer) is designed using a **Star Schema** to optimize analytical queries (OLAP) and ensure intuitive dashboard building in Power BI and Cube.dev.

<div align="center">
  <img src="Data%20models/Conceptual%20model.png" alt="Conceptual Data Model" width="48%" />
  <img src="Data%20models/Logical%20model.png" alt="Logical Data Model" width="48%" />
  <p><em>Left: high-level Conceptual Model. Right: detailed Logical Model with data types and keys.</em></p>
</div>

### 🏗️ Schema Breakdown

**1. The Fact Table — `Fact_Sales`**
The center of the star schema, storing core transactional events and calculated metrics.
- **Measures:** additive facts like `Quantity`, `Unit_Price`, `Discount`, `Revenue`, `Cost`, `Profit`, and `Margin_PCT`.
- **Foreign Keys (FK):** links to surrounding dimensions via surrogate keys (e.g., `Product_ID`, `Store_ID`, `Customer_ID`, `Order_Date_ID`).

**2. The Dimension Tables**
Descriptive context for the sales facts, fully denormalized for read performance:
- **`Dim_Product`** — category, brand, and cocoa percentage.
- **`Dim_Store`** — sales channel attributes, including physical vs. online stores.
- **`Dim_Location`** — geospatial attributes (City, Country, Lat, Lng) derived from external mapping data.
- **`Dim_Customer`** — demographic data (Age, Gender, Loyalty status). Implements **SCD Type 2** via dbt snapshots (`dbt_Valid_From`, `dbt_Valid_To`) to track historical changes in customer profiles.
- **`DWH_DIM_Calendar`** — a comprehensive date dimension with Quarter, Weekend flags, and Seasons, enabling complex time-series analysis.

**🔑 Surrogate Keys & Lookup Logic**
- **Surrogate Keys (SK):** primary keys (like `Sale_SK`, `Product_SK`) are generated during the dbt transformation phase, decoupling the warehouse from source-system changes and ensuring referential integrity.
- **Fact Lookups:** when loading `Fact_Sales`, dbt performs lookup joins against the pre-loaded dimension tables to fetch the correct surrogate keys from the original business keys.
- **Auditability:** every table includes audit attributes (`Created_At`, `Batch_ID`, `Source_System`) to trace data lineage back to the specific Kafka ingestion run.

---

## 🧠 Semantic Model Layer (Cube.dev)

Between the data warehouse and the visualization tool sits **Cube.dev**, acting as a headless BI server and unified semantic layer.

Instead of writing complex DAX calculations or SQL views directly inside the BI tool, all business logic is centralized in Cube — ensuring that a "Sale" or "Profit" means exactly the same thing regardless of which downstream tool is querying it.

- **Centralized Metrics (Measures):** the `fact_sales` cube defines the core aggregations — **Total Orders**, **Total Revenue**, **Total Cost**, **Total Profit**, and **Average Margin %** — pre-defined using standard SQL aggregations over the fact table.
- **Dimensions:** cubes for `dim_customers`, `dim_locations`, `dim_products`, and `dim_stores` expose descriptive attributes and handle join relationships to the fact table.
- **BI Integration:** Power BI connects to Cube.dev's SQL API using **Import Mode**, ingesting the clean, pre-calculated semantic model for lightning-fast dashboard performance without burdening the warehouse with repetitive analytical queries.

---

## 📊 Dashboard & Visualization (Power BI)

The final deliverable is an interactive, highly customized Power BI dashboard designed to answer critical business questions about chocolate sales behavior across 2023 and 2024.

The dashboard features a custom collapsible navigation pane, dynamic filtering, and a thematic UI that aligns with the artisanal chocolate data.

### 🏠 Home Page
A clean landing page that introduces the dashboard, its data source, and provides intuitive navigation to the analytical pages.

<div align="center">
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Home%20Page.png" alt="Power BI Home Page" width="80%" />
</div>
<br/>

### 📈 Overview Page
A high-level executive summary of sales performance. The UI features interactive pop-out panels for both navigation and dynamic filtering to keep the visual real estate clean and focused.

**Key KPIs:** Total Orders (990K+), Total Quantity, Total Revenue ($25M+), and the Cost vs. Profit split.

<div align="center">
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Overview%20Navigation%20view.png" alt="Overview Page Navigation" width="48%" />
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Overview%20Filters%20view.png" alt="Overview Page Filters" width="48%" />
  <p><em>Left: the expanded navigation pane (menu icon). Right: the expanded dynamic filters pane — Year, Month, Season, Store Type, Brand, Age Group, Gender (settings icon).</em></p>
</div>
<br/>

### 🍫 Products Analysis
A deep dive into the 197 unique products across 6 brands.

**Key KPIs:** Average Cocoa % per piece, Average piece weight.

**Business questions answered:** Which specific products drive the most volume? How does cocoa percentage impact total quantity sold? What is the profit breakdown by brand and category (Praline, White, Dark, Truffle, Milk)?

<div align="center">
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Products%20Analysis%20Page.png" alt="Products Analysis Page" width="80%" />
</div>
<br/>

### 🌍 Sales Analysis
Focuses on the geographical and operational aspects of the sales.

**Key KPIs:** Average Order Value ($25.49) and Average Margin (40%).

**Business questions answered:** Which countries generate the highest revenue? How do sales channels (Retail, Online, Mall, Airport) compare in profitability? Do loyalty members spend more than guests?

<div align="center">
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Sales%20Analysis%20pge.png" alt="Sales Analysis Page" width="80%" />
</div>
<br/>

<div align="center">
  <a href="airflow-projects/PowerBI%20Dashboard/Chocolate%20Sales%20Dashboard.pbix">
    <img src="https://img.shields.io/badge/📊_Download_Power_BI_Dashboard-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="Download Power BI Dashboard" />
  </a>
</div>

---

## 🙏 Acknowledgments

Thank you for taking the time to explore **ChocoFlow**! This project was built as a hands-on journey through modern data engineering — from streaming raw CSV records through Kafka, shaping them with dbt across two database engines, modeling a clean star schema, and finally surfacing it all through a semantic layer into a dashboard people can actually use.

Special thanks to the open-source communities behind **Apache Kafka, Apache Airflow, dbt, DuckDB, PostgreSQL, Cube.dev,** and **Docker** — this project wouldn't have been possible without the incredible tools they've built and shared freely.

If this repository helped you understand medallion architecture, dbt multi-database projects, or building a semantic layer with Cube.dev, consider leaving a ⭐ on the repo. Feedback, issues, and pull requests are always welcome!

<div align="center">
  <sub>Built with 🍫 and a lot of curiosity for data engineering.</sub>
</div>
