# DataOps Pipeline Setup Guide

## Prerequisites

### System Requirements
- Docker Desktop 4.0+ or Docker Engine + Docker Compose
- 8GB RAM (minimum), 16GB recommended
- 20GB free disk space
- Git

### Installation

#### macOS
```bash
# Install Docker Desktop
brew install --cask docker

# Verify installation
docker --version
docker-compose --version
```

#### Linux (Ubuntu/Debian)
```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Add user to docker group (avoid sudo)
sudo usermod -aG docker $USER
newgrp docker
```

#### Windows
- Install [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
- Enable WSL2 backend

---

## 1. Project Setup (5 minutes)

### Step 1: Clone/Setup Repository
```bash
# Create project directory
mkdir -p ~/projects/dataops-pipeline
cd ~/projects/dataops-pipeline

# Create directory structure
mkdir -p \
  dags \
  data/{raw,bronze,silver,gold} \
  sql/stack_a \
  pyspark/stack_b \
  scripts \
  config/{great_expectations,monitoring} \
  tests/{unit,integration} \
  ci_cd/.github/workflows \
  notebooks

echo "✓ Project structure created"
```

### Step 2: Copy Configuration Files

Copy these files to your project root:
- `docker-compose.yml` → `./docker-compose.yml`
- `Dockerfile` → `./Dockerfile`
- `requirements.txt` → `./requirements.txt`
- `docker-compose.yml` → `./docker-compose.yml`

### Step 3: Create Environment File
```bash
# Create .env file with secrets
cat > .env << 'EOF'
# Database
DB_PASSWORD=postgres123
AIRFLOW_DB_PASSWORD=airflow123
AIRFLOW_SECRET_KEY=dev-secret-key-change-in-prod

# MinIO (S3-compatible storage for Delta Lake)
MINIO_PASSWORD=minioadmin123

# Monitoring
PROMETHEUS_RETENTION=15d

# System
PYTHONUNBUFFERED=1
AIRFLOW_HOME=/home/airflow
EOF

# Make sure .env is in .gitignore
echo ".env" >> .gitignore
```

---

## 2. Generate Mock Data (5 minutes)

### Copy Mock Data Generator
Copy `mock_data_generator.py` to your `scripts/` directory.

### Generate Data
```bash
# Generate 100K transactions (realistic dataset)
python scripts/mock_data_generator.py --records 100000 --output-dir data/raw

# Expected output:
# ✓ Generated 2,000 customers → data/raw/customers.csv
# ✓ Generated 1,000 products → data/raw/products.csv
# ✓ Generated 100,000 transactions → data/raw/transactions.csv

ls -lh data/raw/
# customers.csv    ~150 KB
# products.csv     ~40 KB
# transactions.csv ~25 MB
```

---

## 3. Start Docker Services (10 minutes)

### Option A: Start All Services
```bash
# Start all containers in detached mode
docker-compose up -d

# Watch the startup process
docker-compose logs -f

# Expected output:
# dataops-postgres                Started
# dataops-airflow-webserver       Started
# dataops-airflow-scheduler       Started
# dataops-spark-master            Started
# dataops-spark-worker-1          Started
# dataops-prometheus              Started
# dataops-grafana                 Started
# dataops-minio                   Started
```

### Option B: Start Services Gradually
```bash
# 1. Start infrastructure (PostgreSQL, MinIO)
docker-compose up -d postgres minio

# 2. Wait for health checks
docker-compose ps | grep postgres
docker-compose ps | grep minio

# 3. Start Airflow (will initialize DB)
docker-compose up -d airflow-webserver airflow-scheduler

# 4. Start Spark
docker-compose up -d spark-master spark-worker-1

# 5. Start monitoring
docker-compose up -d prometheus grafana
```

### Verify Services
```bash
# Check all containers are running
docker-compose ps

# Should show all 9 containers as "Up"

# Test connectivity
docker-compose exec postgres psql -U postgres -c "SELECT 1" 
# Should return: 1

# Test Airflow
curl -u airflow:airflow http://localhost:8080/api/v1/health
# Should return: {"status":"healthy"}
```

---

## 4. Initialize Databases & Schemas

### Create SQL Schema (Stack A)

```bash
# Copy stack A schema to database
docker-compose exec postgres psql -U postgres -d airflow -f < stack_a_schema.sql

# Verify
docker-compose exec postgres psql -U postgres -d airflow -c "
SELECT schemaname, COUNT(*) as table_count 
FROM pg_tables 
WHERE schemaname = 'stack_a' 
GROUP BY schemaname;
"

# Should show: stack_a | 13 (or similar)
```

### Create Delta Lake Structure (Stack B)

```bash
# Create directories for Delta Lake
docker-compose exec spark-master mkdir -p /data/delta/{bronze,silver,gold}

# Verify
docker-compose exec spark-master ls -la /data/delta/
```

### Initialize Airflow
```bash
# Airflow initializes automatically on first webserver start
# Verify by checking the log
docker-compose logs airflow-webserver | grep "initialized"

# Create default user (if not already created)
docker-compose exec airflow-webserver airflow users create \
  --username airflow \
  --password airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@airflow.com
```

---

## 5. Configure Airflow DAGs

### Copy DAG Files
```bash
# Copy Stack A DAG
cp stack_a_dag.py dags/ecommerce_dwh_stack_a.py

# Copy Stack B DAG (we'll create it next)
cp stack_b_dag.py dags/ecommerce_lakehouse_stack_b.py

# Verify DAGs are recognized
docker-compose exec airflow-webserver airflow dags list
```

---

## 6. Access Web Interfaces

### Airflow Web UI
```
URL: http://localhost:8080
User: airflow
Password: airflow

Steps:
1. Go to DAGs tab
2. Find "ecommerce_dwh_stack_a_pipeline"
3. Click on the DAG name
4. Click "Trigger DAG" button
5. Monitor execution
```

### Prometheus
```
URL: http://localhost:9090

Steps:
1. Go to Graph tab
2. Type metric name (e.g., "up")
3. Click "Execute"
4. View metrics
```

### Grafana
```
URL: http://localhost:3000
User: admin
Password: admin

Steps:
1. Add Prometheus data source (http://prometheus:9090)
2. Import dashboard JSON files
3. Create custom dashboards
```

### Jupyter Notebook
```
URL: http://localhost:8888

Steps:
1. Check startup logs for token
2. Use token to login
3. Create new PySpark notebook
4. Start exploring data
```

### MinIO Console (S3-compatible storage)
```
URL: http://localhost:9001
User: minioadmin
Password: minioadmin123

Steps:
1. Create bucket "delta-lake"
2. Create access keys for applications
```

---

## 7. First Pipeline Run

### Step 1: Verify Data Files
```bash
# Check CSV files are in data/raw/
ls -lh data/raw/
# Should show:
# customers.csv
# products.csv
# transactions.csv
```

### Step 2: Run Stack A (SQL/DWH) Pipeline
```bash
# Go to Airflow UI
# 1. Toggle DAG "ecommerce_dwh_stack_a_pipeline" to enabled
# 2. Click "Trigger DAG"
# 3. Monitor execution in the Graph tab

# Or via CLI:
docker-compose exec airflow-webserver \
  airflow dags trigger ecommerce_dwh_stack_a_pipeline

# Monitor logs
docker-compose exec airflow-webserver \
  airflow dags test ecommerce_dwh_stack_a_pipeline 2024-01-01
```

### Step 3: Verify Results
```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow -c "
SELECT COUNT(*) as customer_count FROM stack_a.silver_customers;
"

# Should return customer count

# Check gold layer
docker-compose exec postgres psql -U airflow -d airflow -c "
SELECT * FROM stack_a.gold_daily_sales_fact LIMIT 10;
"
```

### Step 4: Check Reconciliation
```bash
docker-compose exec postgres psql -U airflow -d airflow -c "
SELECT * FROM stack_a.reconciliation_results 
ORDER BY reconciliation_id DESC LIMIT 1;
"

# Should show status 'PASSED'
```

---

## 8. Run Stack B (Lakehouse/PySpark) Pipeline

### Step 1: Copy Stack B Scripts
```bash
cp stack_b_bronze_ingestion.py pyspark/
# (Create stack_b_silver_transformation.py and stack_b_gold_aggregation.py similarly)
```

### Step 2: Submit PySpark Job
```bash
# Run bronze ingestion
docker-compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --executor-memory 1g \
  --driver-memory 1g \
  /data/pyspark/stack_b_bronze_ingestion.py

# Monitor progress at http://localhost:4040
```

### Step 3: Verify Delta Tables
```bash
# Start Spark shell
docker-compose exec spark-master pyspark

# In pyspark shell:
# > df = spark.read.format("delta").load("/data/delta/bronze/customers")
# > df.count()
# 2000
# > df.show()
```

---

## 9. Setup Data Quality Testing (Great Expectations)

### Initialize Great Expectations
```bash
# Create GE project
docker-compose exec airflow-webserver \
  bash -c "cd /home/airflow && great_expectations init"

# Create expectations suite
docker-compose exec airflow-webserver \
  bash -c "cd /home/airflow && \
  great_expectations suite new --datasource postgres_datasource"
```

### Add Data Quality Checks
```yaml
# config/great_expectations/expectations/transactions.json
{
  "expectation_suite_name": "transactions_suite",
  "expectations": [
    {
      "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": {"min_value": 1000, "max_value": 1000000}
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {"column": "transaction_id"}
    },
    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {"column": "status", "value_set": ["completed", "pending", "refunded", "cancelled"]}
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {"column": "amount", "min_value": 0, "max_value": 100000}
    }
  ]
}
```

### Run Data Quality Checks
```bash
# Run validation
docker-compose exec airflow-webserver \
  bash -c "cd /home/airflow && great_expectations validate"

# View HTML report
open ge_validation_report.html
```

---

## 10. Setup Monitoring & Alerting

### Add Prometheus Scrape Config
```yaml
# config/monitoring/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-webserver:8080']
  
  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:8080']
  
  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']  # Requires postgres_exporter
```

### Add Grafana Dashboard
```bash
# Copy dashboard JSON
cp config/monitoring/grafana-dashboards/*.json \
  /var/lib/docker/volumes/dataops_grafana-data/_data/dashboards/

# Restart Grafana
docker-compose restart grafana
```

---

## 11. Troubleshooting

### Airflow Not Starting
```bash
# Check logs
docker-compose logs airflow-webserver

# Common issue: Database not initialized
docker-compose exec airflow-webserver airflow db init

# Reset everything
docker-compose down -v
docker-compose up -d postgres
sleep 10
docker-compose up -d
```

### Out of Memory
```bash
# Increase Docker memory allocation
# In Docker Desktop: Preferences → Resources → Memory → Increase to 8GB+

# Or reduce Spark executor memory in docker-compose.yml:
# environment:
#   SPARK_EXECUTOR_MEMORY: 512m  # was 2g
```

### PostgreSQL Connection Errors
```bash
# Check connectivity
docker-compose exec postgres \
  pg_isready -U postgres -h localhost

# Check credentials
docker-compose exec postgres \
  psql -U postgres -d airflow -c "SELECT 1"

# Check if airflow user exists
docker-compose exec postgres \
  psql -U postgres -d airflow -c "SELECT * FROM pg_user WHERE usename='airflow'"
```

### Spark Jobs Failing
```bash
# Check Spark UI
# URL: http://localhost:4040

# Check executor logs
docker-compose logs spark-worker-1

# Increase memory
docker-compose exec spark-master \
  spark-submit --executor-memory 2g script.py
```

### Great Expectations Issues
```bash
# Reinstall
docker-compose exec airflow-webserver \
  pip install --upgrade great-expectations

# Check context
docker-compose exec airflow-webserver \
  bash -c "cd /home/airflow && great_expectations --version"
```

---

## 12. Cleanup

### Stop Services
```bash
# Stop all services
docker-compose down

# Remove volumes (DELETE DATA!)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

### Reset Everything
```bash
# Complete reset
docker-compose down -v --rmi all
rm -rf data/{raw,bronze,silver,gold}
# Regenerate data
python scripts/mock_data_generator.py
```

---

## Next Steps

1. **Week 1**: Get infrastructure running, generate data, understand architecture
2. **Week 2**: Implement Stack A SQL pipeline, run first DAG
3. **Week 3**: Implement Stack B PySpark pipeline, compare results
4. **Week 4**: Add Great Expectations validation, setup monitoring
5. **Week 5**: Write integration tests, CI/CD pipeline, documentation

---

## Getting Help

### Common Errors

**"Address already in use"**
```bash
# Port conflict - change in docker-compose.yml
# Or kill the process
lsof -i :8080  # Find process on port 8080
kill -9 <PID>  # Kill it
```

**"Insufficient disk space"**
```bash
# Clean Docker
docker system prune -a

# Check disk
df -h /
```

**DAG not showing up**
```bash
# Restart scheduler
docker-compose restart airflow-scheduler

# Check DAG syntax
python dags/ecommerce_dwh_stack_a.py

# Check logs
docker-compose logs airflow-scheduler | grep "parsing"
```

---

## Resources

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Delta Lake Guide](https://docs.delta.io/)
- [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
- [Great Expectations](https://docs.greatexpectations.io/)
- [Prometheus Monitoring](https://prometheus.io/docs/prometheus/latest/getting_started/)
- [Grafana Dashboards](https://grafana.com/grafana/dashboards/)

---

**✅ You're all set! Proceed to the next section of the documentation.**
