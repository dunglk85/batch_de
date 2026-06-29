# DataOps Final Project: End-to-End Reliable Data Pipeline
## eCommerce/POS Transaction Processing

### 📁 Project Structure

```
dataops-pipeline/
├── README.md                          # This file
├── docker-compose.yml                 # All services (Airflow, Postgres, Spark, Prometheus, Grafana)
├── .env                               # Secrets & credentials
├── Dockerfile                         # Custom Airflow image
├── requirements.txt                   # Python dependencies
│
├── dags/
│   ├── __init__.py
│   ├── stack_a_dwh_pipeline.py       # SQL-based DWH pipeline (PostgreSQL)
│   └── stack_b_lakehouse_pipeline.py # PySpark + Delta Lake pipeline
│
├── data/
│   ├── raw/                          # Mock source data (CSV)
│   │   ├── transactions.csv
│   │   ├── customers.csv
│   │   └── products.csv
│   ├── bronze/                       # Raw ingestion layer (Stack B)
│   ├── silver/                       # Cleaned & validated (Stack B)
│   └── gold/                         # Analytics-ready (Stack B)
│
├── sql/
│   ├── stack_a/
│   │   ├── 01_create_bronze_tables.sql
│   │   ├── 02_create_silver_tables.sql
│   │   ├── 03_create_gold_tables.sql
│   │   └── dbt/                      # Optional: dbt models
│   │
│   └── reconciliation.sql            # Reconciliation queries for both stacks
│
├── pyspark/
│   ├── stack_b/
│   │   ├── bronze_ingestion.py       # Load raw data to Delta bronze
│   │   ├── silver_transformation.py  # Clean & validate (Silver layer)
│   │   └── gold_aggregation.py       # Analytics tables (Gold layer)
│   └── common/
│       ├── pii_masking.py            # PII encryption/masking
│       └── reconciliation.py         # Data reconciliation logic
│
├── config/
│   ├── great_expectations/
│   │   ├── checkpoints/
│   │   ├── expectations/
│   │   └── config.yml
│   └── monitoring/
│       ├── prometheus.yml            # Prometheus scrape configs
│       ├── grafana-dashboards/
│       │   ├── pipeline_health.json
│       │   ├── data_quality.json
│       │   └── reconciliation.json
│       └── alerts.yml
│
├── scripts/
│   ├── mock_data_generator.py        # Generate realistic eCommerce data
│   ├── init_databases.sh             # Initialize PostgreSQL & Delta Lake
│   └── health_check.py               # Pipeline health checks
│
├── tests/
│   ├── unit/
│   │   ├── test_transformations.py
│   │   └── test_pii_masking.py
│   └── integration/
│       └── test_pipeline_end_to_end.py
│
├── ci_cd/
│   ├── .github/workflows/
│   │   ├── unit-tests.yml            # Run on PR
│   │   ├── integration-tests.yml     # Run on merge
│   │   └── deploy.yml                # Docker build & push
│   │
│   └── gitlab-ci.yml                 # Alternative: GitLab CI
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── SETUP.md
│   ├── OPERATIONS.md
│   └── RUNBOOKS/
│       ├── incident_pipeline_failure.md
│       ├── data_quality_alert.md
│       ├── reconciliation_mismatch.md
│       └── pii_breach_response.md
│
└── .gitignore
```

---

## 🎯 What You'll Learn

### DataOps Fundamentals
- ✅ Design idempotent, re-runnable pipelines
- ✅ Implement medallion architecture (Bronze → Silver → Gold)
- ✅ Build reconciliation logic for data integrity
- ✅ Apply PII masking & encryption
- ✅ Monitor pipeline health & data quality

### Technology Stack
- **Orchestration**: Apache Airflow (task DAGs, scheduling, backfills)
- **Compute**: 
  - Stack A: PostgreSQL SQL
  - Stack B: PySpark on distributed storage
- **Storage**: 
  - Stack A: PostgreSQL tables
  - Stack B: Delta Lake (ACID transactions, time travel)
- **Data Quality**: Great Expectations (automated validation)
- **Monitoring**: Prometheus (metrics) + Grafana (visualization)
- **CI/CD**: GitHub Actions (or GitLab CI)
- **Deployment**: Docker Compose (local), Kubernetes-ready structure

### Operational Excellence
- ✅ Error handling & retry logic
- ✅ Observability: logs, metrics, traces
- ✅ Incident response runbooks
- ✅ Automated testing (unit + integration)
- ✅ Configuration management (secrets, env vars)

---

## 🚀 Quick Start (5 minutes)

### Prerequisites
```bash
# Install required tools
docker --version                   # Docker Desktop or Docker Engine
docker-compose --version          # Docker Compose v2+
```

### 1. Clone & Initialize
```bash
git clone <your-repo>
cd dataops-pipeline

# Create directories
mkdir -p data/{raw,bronze,silver,gold}
mkdir -p config/great_expectations

# Generate mock data
python scripts/mock_data_generator.py --records 100000

# Start all services
docker-compose up -d

# Initialize databases
docker-compose exec airflow bash scripts/init_databases.sh
```

### 2. Access Interfaces
```
Airflow Web UI:     http://localhost:8080 (user: airflow, pass: airflow)
Prometheus:         http://localhost:9090
Grafana:            http://localhost:3000 (user: admin, pass: admin)
PostgreSQL:         localhost:5432 (postgres user)
PySpark History:    http://localhost:4040
```

### 3. Trigger First Pipeline
```bash
# In Airflow UI:
# 1. Enable DAG: "ecommerce_dwh_pipeline" (Stack A) or "ecommerce_lakehouse_pipeline" (Stack B)
# 2. Click "Trigger DAG"
# 3. Monitor execution in Airflow
# 4. Check data quality in Great Expectations
# 5. View metrics in Grafana
```

---

## 📊 Architecture Comparison

### Stack A: Data Warehouse (SQL)
**Best for**: Structured SQL, familiar tools, team expertise in SQL
```
Raw Sources → Airflow → PostgreSQL (Bronze/Silver/Gold) → BI Tools
                          ↓
                    Great Expectations (validate)
                          ↓
                    Reconciliation checks
```

**Pros:**
- Simple, familiar SQL
- ACID transactions
- Easy to understand & maintain
- Great for structured data

**Cons:**
- Scaling challenges (need Postgres cluster/Redshift)
- Less flexible for semi-structured data
- Limited time-travel capabilities

---

### Stack B: Lakehouse (PySpark + Delta)
**Best for**: Large-scale data, semi-structured data, modern architecture
```
Raw Sources → Airflow → Delta Lake (Bronze/Silver/Gold) → BI Tools
                          ↓
                    Great Expectations (validate)
                          ↓
                    Reconciliation checks
```

**Pros:**
- Scales horizontally (Spark clusters)
- Supports unstructured data (JSON, Parquet, images)
- ACID transactions (Delta Lake)
- Time travel (query historical versions)
- Cost-effective storage (cloud object storage)

**Cons:**
- PySpark learning curve
- Slightly more complex operations
- Setup overhead

---

## 🔐 Security Features (Both Stacks)

1. **Secrets Management**
   - `.env` file for local dev (Docker secrets for prod)
   - Environment variable injection in Airflow
   - Encrypted database passwords

2. **PII Masking** (email, phone, SSN, credit card)
   - Hashing with salt for irreversible masking
   - Tokenization option for reversible masking
   - Audit logging of who accessed PII

3. **Access Control**
   - Airflow user roles (Admin, Viewer, Editor)
   - Database-level permissions
   - Row-level security (RLS) in PostgreSQL
   - Separate service accounts for each component

4. **Data Lineage & Audit**
   - Airflow task logs
   - Great Expectations validation reports
   - Database change logs
   - Reconciliation audit trail

---

## ✅ Data Quality & Testing

### Great Expectations Validations
```python
Expectations:
  - No NULL values in transaction_id (critical)
  - amount > 0 (business rule)
  - transaction_date ≤ TODAY (sanity check)
  - No duplicate transaction_ids (uniqueness)
  - customer_id exists in customers table (referential integrity)
```

### Test Scenarios
- **Unit Tests**: Transform logic, masking functions
- **Integration Tests**: End-to-end pipeline (mock data)
- **Reconciliation Tests**: Source ↔ Target row counts, sums

---

## 📈 Monitoring & Alerting

### Prometheus Metrics (both stacks)
- `pipeline_duration_seconds` → How long did the pipeline take?
- `records_processed_total` → How many rows were processed?
- `data_quality_failures` → How many validation failures?
- `reconciliation_mismatch` → Data integrity checks

### Grafana Dashboards
1. **Pipeline Health**: Success/failure rates, avg duration
2. **Data Quality**: Validation pass/fail trends
3. **Reconciliation**: Source vs target row counts

### Alerts (PagerDuty/Email)
- ⚠️ Pipeline fails → Immediate notification
- ⚠️ Data quality < 99% → Investigation required
- ⚠️ Reconciliation mismatch > 0.1% → Critical alert

---

## 🚨 Incident Response Runbooks

1. **Pipeline Failure**
   - Check Airflow logs → Identify task failure
   - Rerun failed task with `--force` flag
   - Verify data integrity with reconciliation

2. **Data Quality Alert**
   - Review Great Expectations report
   - Identify affected records
   - Fix source data → Re-run pipeline

3. **Reconciliation Mismatch**
   - Query source vs target counts
   - Check for partial loads
   - Identify missing/duplicate records

---

## 📚 Learning Path

**Week 1:**
- [ ] Setup local environment (Docker Compose)
- [ ] Understand medallion architecture (Bronze/Silver/Gold)
- [ ] Write first Airflow DAG with dummy tasks
- [ ] Generate mock data

**Week 2:**
- [ ] Choose Stack A or B
- [ ] Implement bronze layer (raw ingestion)
- [ ] Implement silver layer (cleaning, deduplication)
- [ ] Test with 10K rows

**Week 3:**
- [ ] Implement gold layer (aggregations)
- [ ] Add PII masking
- [ ] Write reconciliation queries
- [ ] Test with 100K rows

**Week 4:**
- [ ] Setup Great Expectations validation
- [ ] Add Prometheus metrics
- [ ] Create Grafana dashboards
- [ ] Write unit & integration tests

**Week 5:**
- [ ] Implement CI/CD pipeline
- [ ] Write incident runbooks
- [ ] Performance tuning
- [ ] Documentation & presentation

---

## 🔗 Next Steps

1. **Read** `SETUP.md` for detailed installation
2. **Explore** `scripts/mock_data_generator.py` to understand data model
3. **Choose** Stack A or B based on your preference
4. **Implement** following the DAG templates
5. **Monitor** using Grafana dashboards
6. **Iterate** based on learnings

---

## 📞 Troubleshooting

**Airflow can't connect to PostgreSQL?**
```bash
docker-compose logs airflow-scheduler | grep "postgres"
# Check AIRFLOW__DATABASE__SQLALCHEMY_CONN in .env
```

**PySpark running out of memory?**
```bash
# Increase in docker-compose.yml:
# environment:
#   SPARK_DRIVER_MEMORY: 2g
#   SPARK_EXECUTOR_MEMORY: 2g
```

**Great Expectations failing?**
```bash
# Check expectations config
docker-compose exec airflow great_expectations validate
```

---

## 📖 Additional Resources

- [Apache Airflow Docs](https://airflow.apache.org)
- [PySpark Guide](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io)
- [Great Expectations Docs](https://docs.greatexpectations.io)
- [Prometheus Monitoring](https://prometheus.io/docs/)

---

**Ready to start? Go to `SETUP.md` →**
