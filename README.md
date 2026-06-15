# Fintech Real-Time Medallion Lakehouse

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![Apache Flink](https://img.shields.io/badge/flink-1.20.4-orange)
![Apache Iceberg](https://img.shields.io/badge/iceberg-v2-blue)
![Trino](https://img.shields.io/badge/trino-481-vibrant)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-active%20sandbox%20%2F%20WIP-yellow)

A high-throughput, local sandbox for real-time Medallion Lakehouse architectures. This project ingests, validates, masks, and compacts transaction telemetry in real-time using Flink, Iceberg, Trino, and dbt.

> [!NOTE]  
> **Honest Disclosure & Project Status**  
> This repository is an **active educational sandbox and Work-in-Progress (WIP)** designed and maintained by a rising Year 2 computer engineering/science student. It was built using an AI-augmented workflow to explore enterprise-grade data streaming, distributed catalogs, and query optimization. 
> 
> Many components (such as input data feeds) are simulated locally. The codebase also documents identified production edge cases and state vulnerabilities under active study.

---

## ✨ Features

* **3-Broker Kafka (KRaft) Cluster:** Highly available local message broker stack utilizing Confluent Schema Registry to enforce `BACKWARD_TRANSITIVE` schema compatibility.
* **PyFlink Medallion Stream Processing:** Processes transactions in real-time, routing corrupted data to rejected tables, while deduplicating valid transactions using event-time `TUMBLE` windows to minimize RocksDB state footprint.
* **Salted SHA-256 PII Masking:** Masks sensitive customer data (`account_id`, `device_id`) directly at the streaming layer before writing to the Silver tables.
* **Iceberg v2 REST Catalog on MinIO:** Uses modern Iceberg v2 table layouts on local S3, employing object-store hashed file layouts (`object_store_layout_enabled`) to avoid S3 prefix performance bottlenecks.
* **Periodic Table Compaction:** Includes a maintenance engine executing `optimize` and `optimize_manifests` to prevent the "small file problem" caused by streaming checkpoints.
* **dbt-Trino Aggregations (Gold Layer):** Uses Trino as the query engine and dbt to build incremental fraud alerts and liquidity metrics with partition pruning macros.
* **Programmatic Test Coverage:** Complete unit testing testing Flink configuration behaviors, Avro compatibility checks, and Docker Compose boot order.

---

## 🛠️ Tech Stack

| Component | Technology | Version / Details |
|---|---|---|
| **Stream Processing** | [Apache Flink (PyFlink)](https://flink.apache.org/) | `1.20.4` (RocksDB Backend, managed memory) |
| **Message Queue** | [Apache Kafka](https://kafka.apache.org/) | `8.2.1-confluent` (KRaft cluster mode, Schema Registry) |
| **Table Catalog** | [Apache Iceberg](https://iceberg.apache.org/) | `v2` (REST Catalog impl, PostgreSQL backlink) |
| **Object Storage** | [MinIO](https://min.io/) | S3-Compatible Local Storage |
| **SQL Engine** | [Trino](https://trino.io/) | `481` (Direct query catalog over Iceberg) |
| **Data Transformation** | [dbt-trino](https://github.com/starburstdata/dbt-trino) | `1.10.2` (Incremental Gold layer models) |
| **Scripting Language** | [Python](https://www.python.org/) | `3.10+` (Faker for generator, Trino/Hadoop script handlers) |
| **Orchestration** | [Docker Compose](https://docs.docker.com/compose/) | Healthcheck dependency boot ordering |

---

## 🚀 Getting Started

### Prerequisites
* **Docker Desktop** (with Docker Compose v2)
* **Python 3.10+** installed locally on your host machine

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd fintech-realtime-lakehouse
   ```

2. **Sync the project virtual environment:**
   This will automatically create a `.venv` virtual environment and install all pinned dependencies from `pyproject.toml` and `uv.lock`:
   ```bash
   uv sync
   ```

3. **Activate the virtual environment:**
   ```bash
   # Windows PowerShell:
   .venv\Scripts\Activate.ps1
   ```

3. **Configure Environment Variables:**
   Create a `.env` file in the project root based on `.env.example`:
   ```bash
   cp .env.example .env
   ```
   Provide values for the following variables (do not commit this file to git):
   * `PII_HASH_SALT`: A secure 32+ character hex string used to salt SHA-256 hashes (e.g., `0123456789abcdef0123456789abcdef`).
   * `AWS_ACCESS_KEY_ID`: MinIO root access key (default: `admin`).
   * `AWS_SECRET_ACCESS_KEY`: MinIO secret password (default: `supersecretadmin`).

---

## 💻 Usage / How to Run (PowerShell Setup)

Follow these steps sequentially to spin up the local environment:

#### Step 1: Spin up the infrastructure
```powershell
docker compose -f docker\docker-compose.yml up -d
docker compose -f docker\docker-compose.yml ps
```

#### Step 2: Initialize Trino & Iceberg tables
```powershell
uv run python storage\iceberg_init.py
```

#### Step 3: Copy Flink job files and config
*(Note: Because of import paths, PyFlink needs both the main streaming job and the config module inside `/tmp/` of the container to import correctly)*
```powershell
docker cp .\streaming\jobs\kafka_to_iceberg.py fintech-flink-jobmanager:/tmp/kafka_to_iceberg.py
docker cp .\streaming\jobs\config.py fintech-flink-jobmanager:/tmp/config.py
```

#### Step 4: Submit Flink Ingestion
*(Note: We inject environment configurations and a valid 32-character hex `PII_HASH_SALT` so the streaming validation passes successfully)*
```powershell
docker exec `
  -e DEDUP_WINDOW_MINUTES=1 `
  -e WATERMARK_LATENESS_SECONDS=20 `
  -e PII_HASH_SALT=0123456789abcdef0123456789abcdef `
  -it fintech-flink-jobmanager flink run -d -py /tmp/kafka_to_iceberg.py
```

#### Step 5: Start the transaction simulator
*(In another terminal session)*
```powershell
$env:TARGET_EVENTS_PER_SECOND="20"
$env:ANOMALY_RATE="0.15"
uv run python simulator\main_generator.py
```
*(Let it generate traffic for 2–3 minutes, then hit `Ctrl+C` to stop)*

#### Step 6: Query Trino to verify Landing
```powershell
docker exec -it fintech-trino trino --execute "SELECT count(*) FROM iceberg.bronze.transactions"
docker exec -it fintech-trino trino --execute "SELECT count(*) FROM iceberg.silver.transactions"
docker exec -it fintech-trino trino --execute "SELECT account_id, device_id FROM iceberg.silver.transactions LIMIT 5"
```

#### Step 7: Run dbt validation models for Gold layer
```powershell
cd dbt_lakehouse
uv run dbt run --profiles-dir . --select gold
uv run dbt test --profiles-dir . --select gold
cd ..
```

---

## 📁 Project Structure

```text
.
├── dbt_lakehouse/          # dbt project for Gold layer models on Trino
│   ├── macros/             # Partition pruning and time calculation macros
│   └── models/             # SQL models for gold aggregates (liquidity, alerts)
├── docker/                 # Container files and Docker Compose definition
│   ├── dbt/                # Dockerfile and entrypoint for dbt runner
│   ├── flink/              # Flink image with PyFlink, S3/Hadoop JARs, and checksums
│   ├── iceberg-rest/       # Iceberg REST catalog image with PostgreSQL JDBC
│   ├── trino/              # Trino catalog configuration (iceberg.properties)
│   └── docker-compose.yml  # Local services orchestration
├── docs/                   # Architectural documentation & audit reports
│   └── security_audit_report.md # Security audit results and edge case analysis
├── simulator/              # Python generator producing simulated transaction streams
│   ├── schemas/            # Avro transaction schemas
│   └── main_generator.py   # Main Faker generator and schema registry publisher
├── storage/                # Lakehouse table creation and upkeep utilities
│   ├── iceberg_init.py     # Schema and Iceberg v2 table creator (via Trino REST)
│   ├── iceberg_maintenance.py # Small-file compaction and snapshot cleanup script
│   └── wait_for_silver.py  # Wait utility before running downstream dbt transformations
├── streaming/              # PyFlink stream processing
│   └── jobs/               # Flink streaming job and configuration parsing
├── tests/                  # Integration tests for docker, schemas, and Flink
├── LICENSE                 # Project license (MIT)
└── README.md               # Main project documentation
```

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
