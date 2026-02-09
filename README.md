# Real-time E-commerce Data Pipeline

## Architecture Overview

<img width="1026" height="265" alt="image" src="https://github.com/user-attachments/assets/d67a3f50-7c6f-4334-971d-ba97f7bfdcaf" />


## Data Flow

1. **Airflow DAG** crawls 3 e-commerce APIs in parallel every 3 minutes
2. **~200 products** per run sent to Kafka topic `ecommerce_products`
3. **Spark Streaming** consumes from Kafka and writes to ClickHouse
4. **ClickHouse** stores data with analytics views

## Quick Start

### Prerequisites
- Docker & Docker Compose
- At least 8GB RAM
- 10GB free disk space

### Step 1: Start the System

```bash
git clone https://github.com/minora2k4/ecommerce-etl-pipeline
cd ecommerce-pipeline
docker-compose up -d
```

### Step 2: Wait for Services to Initialize (~2-3 minutes)

```bash
# Check all services are healthy
docker-compose ps
```

### Step 3: Access the UIs

- **Airflow**: http://localhost:8083 (admin/admin)
- **Spark Master**: http://localhost:8080
- **Kafka UI**: http://localhost:8082
- **ClickHouse**: http://localhost:8123 (use DBeaver or clickhouse-client)

### Step 4: Enable the DAG

1. Go to Airflow UI (http://localhost:8083)
2. Login with `admin/admin`
3. Find `extract_ecommerce_api_to_kafka` DAG
4. Toggle the switch to enable it
5. It will run automatically every 3 minutes

## Verify Data Pipeline

### Check Kafka Messages

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce_products \
  --from-beginning \
  --max-messages 5
```

### Check Spark Streaming Logs

```bash
docker logs -f spark-submit
```

### Query ClickHouse Data

```bash
# Enter ClickHouse client
docker exec -it clickhouse clickhouse-client

# Run queries
SELECT count() FROM ecommerce.products;
SELECT * FROM ecommerce.products LIMIT 5 FORMAT Vertical;
```

##  Project Structure

```
ecommerce-pipeline/
├── airflow/
│   ├── Dockerfile              # Airflow image with dependencies
│   ├── requirements.txt        # Python packages (kafka-python, requests)
│   ├── dags/
│   │   └── extract_api.py      # Main DAG - parallel API crawling
│   └── logs/                   # Airflow logs
├── spark/
│   ├── Dockerfile              # Spark with ClickHouse JDBC & Kafka libs
│   ├── streaming_job.py        # Spark Streaming consumer
│   └── checkpoint/             # Streaming checkpoint
├── clickhouse/
│   └── init/
│       └── 01_schema.sql       # Database & table schema
├── env/
│   └── airflow.env             # Airflow environment variables
└── docker-compose.yml          # Complete stack definition
```

## Services Configuration

### Ports
- **Postgres**: 5432
- **Zookeeper**: 2181
- **Kafka**: 9092
- **Kafka UI**: 8082
- **Spark Master**: 7077, 8080 (UI)
- **Spark Worker**: 8081 (UI)
- **ClickHouse**: 8123 (HTTP), 9000 (Native)
- **Airflow**: 8083

### Resource Allocation
- **Spark Worker**: 2 cores, 2GB memory
- **Total cores for executors**: 2

##  Data Sources

### 1. DummyJSON (~80 products/run)
- **Endpoint**: https://dummyjson.com/products
- **Fields**: title, price, category, brand, rating, stock, thumbnail
- **Free**: No API key required

### 2. FakeStoreAPI (~100 products/run)
- **Endpoint**: https://fakestoreapi.com/products
- **Fields**: title, price, category, rating, description, image
- **Strategy**: Duplicates with price variations

### 3. Platzi Fake Store (~20 products/run)
- **Endpoint**: https://api.escuelajs.co/api/v1/products
- **Fields**: title, price, category, description, images
- **Free**: No API key required

**Total**: ~200 products every 3 minutes = ~4,000 products/hour



