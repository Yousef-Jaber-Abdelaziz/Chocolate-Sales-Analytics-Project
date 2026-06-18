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

Here is a high-level overview of the repository, highlighting the core components of the data pipeline:

```text
airflow-projects/
│
├── 📄 docker-compose.yaml          # Core infrastructure & services orchestration
├── 📄 README.md                    # Project documentation
│
├── 📂 airflow/                     # Apache Airflow orchestration setup
│   ├── 📂 dags/                    # Pipeline schedules (Ingestion & Warehousing DAGs)
│   ├── 📂 include/                 # Shared dependencies and utilities
│   └── 📄 Dockerfile               # Airflow Docker image definition
│
├── 📂 producers/                   # Python scripts for producing Kafka events from CSV
├── 📂 consumers/                   # Python scripts for consuming Kafka events to DuckDB
│
├── 📂 dbt/                         # Data Build Tool (dbt) project
│   ├── 📂 models/                  # SQL transformations (staging & marts)
│   ├── 📂 tests/                   # Data quality tests
│   └── dbt_project.yml             # dbt configuration
│
├── 📂 My Datasets/                 # Raw source data (CSV files for Chocolate Sales)
│
└── 🗄️ Persistent Data Volumes:     # Local storage mapped to Docker containers
    ├── 📂 duckdb_data/             # DuckDB staging database files
    ├── 📂 kafka_data/              # Kafka broker logs and KRaft metadata
    └── 📂 include/postgres_db/     # PostgreSQL Data Warehouse storage
