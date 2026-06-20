# 🍫  Chocolate Sales Analytics Project

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

---

## 🎯 Project Overview

This is a **modern data engineering and analytics project** that demonstrates a production-ready, event-driven data pipeline for processing and analyzing chocolate sales data. By transforming raw CSV files into actionable dashboards, this project solves the problem of siloed reporting. It combines multiple technologies to create a scalable, automated system that ingests, models, and visualizes sales metrics.

**Key Features:**
- Raw data ingestion from CSV files using Python
- Event-driven streaming architecture with Apache Kafka
- Pipeline orchestration and scheduling via Apache Airflow
- Centralized data storage using PostgreSQL
- Reliable data transformation and testing with dbt (data build tool)
- Consistent metric definitions via a semantic layer with Cube.dev
- Interactive visualizations and business intelligence using Power BI
- Fully containerized deployment for reproducibility with Docker

---


## 🗺️ Architecture Diagram

The pipeline is orchestrated using Apache Airflow and follows a multi-hop medallion architecture to incrementally process, refine, and model the chocolate sales data.

<div align="center">
  <img src="airflow-projects/Flow%20Diagrams/Data%20Pipeline%20Diagram.png" alt="Data Pipeline Architecture Diagram" />
</div>

**Data Flow & Pipeline Layers:**
*   **Data Ingestion (Bronze Layer):** Raw data is read from the CSV file by a Kafka Producer, streamed through Kafka topics in KRaft mode, and ingested into a **DuckDB** database by a Kafka Consumer.
*   **Transformation (Silver Layer):** **dbt** executes models against the DuckDB database to clean and structure the raw data. 
*   **Data Warehousing (Gold Layer):** The staged tables are transferred from DuckDB into a **PostgreSQL** Data Warehouse. A second set of **dbt** transformations is applied to build the final star schema (Fact and Dimension tables) and generate surrogate keys.
*   **Semantic Layer & BI:** **Cube.dev** connects to the PostgreSQL data warehouse to define a unified semantic model, which is then queried by **Power BI** for the final interactive dashboards.
*   **Orchestration & Infrastructure:** **Apache Airflow** manages the dependencies, scheduling the ingestion DAG and triggering the warehousing DAG only upon successful ingestion. It also acts as the notification center for task success or failure. The entire infrastructure is containerized using **Docker**.

---

## 🛠️ Tech Stack & Environment

This project leverages a modern, fully containerized data stack to ensure reproducibility, scalability, and easy local development.

### 🧰 The Technology Stack

| Technology | Role / Layer | Description |
| :--- | :--- | :--- |
| **Python** | Data Ingestion | Reads raw chocolate sales CSV files and acts as the producer sending batches to Kafka. |
| **Apache Kafka** | Streaming (Bronze) | A 3-node cluster running in **KRaft mode** (Zookeeper-less) for reliable, high-throughput event streaming. |
| **DuckDB** | Staging | Blazing fast, in-process analytical database used by consumers to land and stage raw Kafka events. |
| **dbt** | Data Transformation | Executes SQL models to clean staging data (Silver) and build the final star schema (Gold). |
| **PostgreSQL** | Data Warehouse (Gold) | Persistent relational database storing the final Fact and Dimension tables. |
| **Cube.dev** | Semantic Layer | Headless BI server that centralizes metric definitions and serves them via a unified API. |
| **Power BI** | Visualization | Connects to the semantic model to deliver interactive, real-time chocolate sales dashboards. |
| **Apache Airflow** | Orchestration | Manages DAG scheduling, handles task dependencies, and manages alerting. |
| **Docker** | Infrastructure | Containerizes all services, ensuring consistent environments and network isolation. |

### ⚙️ Environment Setup & Architecture Notes

The infrastructure is divided into data processing services and pipeline orchestration, all running seamlessly via Docker.

* **Core Infrastructure (`docker-compose.yaml`):** A single compose file spins up the 3-node Kafka cluster, PostgreSQL data warehouse, pgAdmin, the DuckDB staging environment, the dbt execution container, and the Cube.dev server. All services communicate securely over a custom Docker bridge network (`de_network`).
* **Orchestration (Astro CLI):** Apache Airflow is deployed on Docker using the **Astronomer (Astro) CLI**. This runs alongside the core infrastructure, allowing Airflow to orchestrate the ingestion and warehousing DAGs while keeping orchestration dependencies cleanly separated from the data processing engines.
* **Data Persistence:** Local volume mapping is utilized across the stack (e.g., `./include/postgres_db` for PostgreSQL and `./kafka_data` for the brokers) to ensure no data is lost when containers are spun down.

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

The pipeline processes Chocolate sales data spanning from **2023-01-01 to 2024-12-31**. The architecture is heavily focused on idempotency, fault tolerance, and strict sequential loading.

If you want to visit the original Kaggle data source, please click here: 
<a href="https://www.kaggle.com/code/ssssws/chocolate-sales"><img src="https://img.shields.io/badge/Kaggle-20BEFF?style=for-the-badge&logo=Kaggle&logoColor=white" alt="Kaggle Dataset"/></a>

Alternatively, the raw CSV files are stored locally in the repository at:
`airflow-projects/My Datasets/Choclate Sales`

### Phase 1: Data Ingestion (The Bronze Layer)
*Managed by `chocolate_producer_dag` and Kafka Consumers*

1. **Kafka Topic Initialization:** The Kafka cluster is initialized with four distinct topics:
   * `chocolate_sales` (3 partitions, 3 replications)
   * `chocolate_products` (1 partition, 3 replications)
   * `chocolate_stores` (1 partitions, 3 replications)
   * `chocolate_customers` (1 partitions, 3 replications)

2. **Stateful Batch Producing:** Instead of a simple dump, the Python producer streams data incrementally, month-by-month. It utilizes a local memory state file to track progress. 
   * The state tracks the current target month and retry attempts, looking like this:
     ```json
     {"target_month": "2023-01", "retry_count": 0, "max_retries": 3}
     ```
   * **Fault Tolerance:** If a month's data is successfully loaded, the memory increments to the next month. If data is missing or fails, it retries for up to 3 consecutive runs. If it still fails, it increments the month, gracefully leaving a gap rather than crashing the pipeline.
   * <a href="airflow-projects/airflow/include/producers/producer_logic.py"><img src="https://img.shields.io/badge/View_Producer_Logic-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Producer Logic"/></a>

3. **Smart Consumption & Alerting:** * If the ingestion DAG brings in no new data, an email notification is immediately dispatched to alert the team.
   * If new data exists, the consumer reads from the topics based on Kafka offsets and lands the data into raw Bronze tables within the **DuckDB** database.
   * A success report is emailed upon completion.

4. **Data-Aware Scheduling:** The populated DuckDB database is registered in Apache Airflow as an **Asset** (Dataset). The successful update of this Asset acts as the trigger for the next phase.

---

### Phase 2: Transformation & Warehousing (Silver & Gold Layers)
*Triggered automatically by the DuckDB Asset via `chocolate_warehousing_dag`*

1. **Silver Layer (dbt + DuckDB):**
   * **OBT Creation:** dbt initiates the transformation by joining the raw tables into a single One Big Table (OBT).
   * **Staging:** Utilizing a single-slot pool to manage concurrency, dbt breaks this OBT down into normalized staging tables aligned with the target star schema.

2. **Data Transfer:**
   * The cleaned staging tables are extracted from DuckDB and loaded into the `public` schema of the target **PostgreSQL Data Warehouse**.

3. **Gold Layer (dbt + PostgreSQL):**
   * **Dimension Loading:** Data is moved from the `public` schema into the final `dwh` schema. Dimensions are strictly loaded *first*. Notably, Slowly Changing Dimensions (SCD) logic is applied to the `dim_customers` table to track historical changes.
   * **Fact Loading:** Once dimensions are secure, the `fact_sales` table is loaded, performing necessary lookups against the dimension tables to retrieve surrogate keys.

4. **Idempotency & Audit Trails:**
   * **Success/Failure Alerting:** A final status email is dispatched to the admin account.
   * **Idempotent Cleanup:** A teardown script runs to truncate the raw tables in DuckDB. This guarantees that if the pipeline is rerun, no duplicate data will be processed.
   * **Auditing:** The final execution status of the DAG is recorded in a dedicated audit table for pipeline observability.

---


## 🌟 Data Warehouse Design (Star Schema)

The PostgreSQL Data Warehouse (Gold Layer) is designed using a **Star Schema** to optimize analytical queries (OLAP) and ensure intuitive dashboard building in Power BI and Cube.dev. 

<div align="center">
  <img src="Data%20models/Conceptual%20model.png" alt="Conceptual Data Model" width="48%" />
  <img src="Data%20models/Logical%20model.png" alt="Logical Data Model" width="48%" />
  <p><em>Left: High-level Conceptual Model. Right: Detailed Logical Model with data types and keys.</em></p>
</div>

### 🏗️ Schema Breakdown

#### 1. The Fact Table (`Fact_Sales`)
The center of the star schema stores the core transactional events and calculated metrics. 
* **Measures:** Includes additive facts like `Quantity`, `Unit_Price`, `Discount`, `Revenue`, `Cost`, `Profit`, and `Margin_PCT`.
* **Foreign Keys (FK):** Links to the surrounding dimensions using surrogate keys (e.g., `Product_ID`, `Store_ID`, `Customer_ID`, `Order_Date_ID`).

#### 2. The Dimension Tables
The descriptive context for the sales facts, fully denormalized for read performance:
* **`Dim_Product`:** Details like category, brand, and cocoa percentage.
* **`Dim_Store`:** Attributes for sales channels, tracking whether a store is physical or online.
* **`Dim_Location`:** Geospatial attributes (City, Country, Lat, Lng) derived from external mapping data.
* **`Dim_Customer`:** Demographic data (Age, Gender, Loyalty status). **Note:** This table implements **Slowly Changing Dimensions (SCD Type 2)** using dbt snapshots (`dbt_Valid_From`, `dbt_Valid_To`) to track historical changes in customer profiles.
* **`DWH_DIM_Calender`:** A comprehensive date dimension providing attributes like Quarter, Weekend flags, and Seasons to enable complex time-series analysis.

#### 🔑 Surrogate Keys & Lookup Logic
* **Surrogate Keys (SK):** Primary keys (like `Sale_SK`, `Product_SK`) are generated during the dbt transformation phase. This decouples the data warehouse from source system changes and ensures referential integrity.
* **Fact Lookups:** When loading the `Fact_Sales` table in the Gold layer, dbt executes lookup joins against the pre-loaded Dimension tables to fetch the correct Surrogate Keys based on the original business keys.
* **Auditability:** Every table includes audit attributes (`Created_At`, `Batch_ID`, `Source_System`) to track data lineage back to the specific Kafka ingestion run.

---


## 🧠 9. Semantic Model Layer (Cube.dev)

Between the data warehouse and the visualization tool sits **Cube.dev**, acting as a headless BI server and unified semantic layer. 

Instead of writing complex DAX calculations or SQL views directly inside the BI tool, all business logic is centralized in Cube. This ensures that a "Sale" or "Profit" means the exact same thing regardless of the downstream tool querying it.

* **Centralized Metrics (Measures):** The `fact_sales` cube defines the core aggregations. Basic calculations like **Total Orders**, **Total Revenue**, **Total Cost**, **Total Profit**, and **Average Margin %** are pre-defined here using standard SQL aggregations over the fact table.
* **Dimensions:** Cubes for `dim_customers`, `dim_locations`, `dim_products`, and `dim_stores` expose the descriptive attributes and handle the join relationships to the fact table.
* **BI Integration:** Power BI connects to Cube.dev's SQL API using **Import Mode**. This allows Power BI to ingest the clean, pre-calculated semantic model, resulting in lightning-fast dashboard performance without burdening the data warehouse with repetitive analytical queries.

---

## 📊 10. Dashboard & Visualization (Power BI)

The final deliverable is an interactive, highly customized Power BI dashboard designed to answer critical business questions about chocolate sales behavior across 2023 and 2024. 

The dashboard features a custom collapsible navigation pane, dynamic filtering, and a thematic UI that aligns perfectly with the artisanal chocolate data.

### 🏠 Home Page
A clean landing page that introduces the dashboard, its data source, and provides intuitive navigation to the analytical pages.

<div align="center">
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Home%20Page.png" alt="Power BI Home Page" width="80%" />
</div>
<br/>

### 📈 Overview Page
Provides a high-level executive summary of sales performance. The UI features interactive pop-out panels for both navigation and dynamic filtering to keep the visual real estate clean and focused.
* **Key KPIs:** Total Orders (990K+), Total Quantity, Total Revenue ($25M+), and the Cost vs. Profit split.

<div align="center">
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Overview%20Navigation%20view.png" alt="Overview Page Navigation" width="48%" />
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Overview%20Filters%20view.png" alt="Overview Page Filters" width="48%" />
  <p><em>Left: The expanded navigation pane triggered by the menu icon. Right: The expanded dynamic filters pane (Year, Month, Season, Store Type, Brand, Age Group, Gender) triggered by the settings icon.</em></p>
</div>
<br/>

### 🍫 Products Analysis
A deep dive into the 197 unique products across 6 brands.
* **Key KPIs:** Average Cocoa % per piece, Average piece weight.
* **Business Questions Answered:** Which specific products drive the most volume? How does cocoa percentage impact total quantity sold? What is the profit breakdown by brand and category (Praline, White, Dark, Truffle, Milk)?

<div align="center">
  <img src="airflow-projects/PowerBI%20Dashboard/Dashboard%20Images/Products%20Analysis%20Page.png" alt="Products Analysis Page" width="80%" />
</div>
<br/>

### 🌍 Sales Analysis
Focuses on the geographical and operational aspects of the sales.
* **Key KPIs:** Average Order Value ($25.49) and Average Margin (40%).
* **Business Questions Answered:** Which countries generate the highest revenue? How do sales channels (Retail, Online, Mall, Airport) compare in profitability? Do Loyalty Members spend more than Guests?

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
