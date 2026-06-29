# DataOps Pipeline Architecture Guide

## Overview

This project implements an end-to-end reliable data pipeline following DataOps principles. Two architecture options are provided:

- **Stack A**: Data Warehouse approach (PostgreSQL + SQL)
- **Stack B**: Lakehouse approach (PySpark + Delta Lake)

Both stacks share the same medallion architecture (Bronze → Silver → Gold) and orchestration layer (Airflow).

---

## Medallion Architecture Explained

### Bronze Layer: Raw Data Ingestion
**Purpose**: Immutable log of all incoming data

```
├── Characteristics:
│   ├── Stores raw data exactly as received
│   ├── No transformations or quality checks
│   ├── Append-only (immutable)
│   ├── Tracks ingestion metadata
│   └── Full audit trail
│
├── Data:
│   ├── customers_bronze (2K records)
│   ├── products_bronze (1K records)
│   └── transactions_bronze (100K records)
│
└── Idempotency:
    ├── Deduplicates on primary key
    ├── Tracks load UUID
    └── Allows safe re-runs
```

### Silver Layer: Cleaned & Validated Data
**Purpose**: Deduplicated, validated, business-ready data

```
├── Transformations:
│   ├── Remove duplicates
│   ├── Mask PII (email, phone)
│   ├── Validate business rules
│   ├── Check data types
│   └── Enforce constraints
│
├── Data Quality:
│   ├── Flag invalid records
│   ├── Track validation errors
│   ├── NULL checks on critical columns
│   └── Referential integrity
│
└── Idempotency:
    ├── DELETE + INSERT pattern
    ├── Re-runnable without conflicts
    └── Tracked with load UUID
```

### Gold Layer: Analytics-Ready Aggregations
**Purpose**: Optimized for reporting and analysis

```
├── Fact Tables:
│   └── daily_sales_fact (aggregated by day/customer/product)
│
├── Dimension Tables:
│   ├── customer_metrics (LTV, avg transaction, risk score)
│   └── product_metrics (revenue, inventory, profitability)
│
├── Time Series:
│   └── category_trends (daily sales by category)
│
└── Optimization:
    ├── Pre-computed aggregations
    ├── Indexed for fast queries
    ├── Materialized for BI tools
    └── Updateable (re-computed daily)
```

---

## Stack A: Data Warehouse (PostgreSQL)

### Architecture Diagram
```
┌─────────────────────────────────────────────────────────┐
│                   Data Sources (CSV)                     │
├─────────────────────────────────────────────────────────┤
│  customers.csv | products.csv | transactions.csv        │
└────────────────┬────────────────┬──────────────────────┘
                 │                │
                 ▼                ▼
        ┌───────────────────────────────────┐
        │  Apache Airflow                   │
        │  - Orchestration                  │
        │  - Scheduling                     │
        │  - Error handling & retries       │
        └────────────┬──────────────────────┘
                     │
    ┌────────────────┼────────────────┐
    ▼                ▼                ▼
┌─────────────┐ ┌──────────────┐ ┌─────────────┐
│   Bronze    │ │   Silver     │ │    Gold     │
│   Tables    │ │   Tables     │ │   Tables    │
├─────────────┤ ├──────────────┤ ├─────────────┤
│  Raw data   │ │ Cleaned &    │ │Aggregated & │
│  Immutable  │ │ Validated    │ │ Optimized   │
│  Audit log  │ │ PII masked   │ │ BI-ready    │
└─────────────┘ └──────────────┘ └─────────────┘
    │                │                │
    └────────────────┼────────────────┘
                     │
        ┌────────────▼────────────┐
        │   PostgreSQL Database   │
        │  (Primary Storage)      │
        └────────────┬────────────┘
                     │
    ┌────────────────┼─────────────────┐
    ▼                ▼                  ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Great       │ │ Reconciliation│ │  Prometheus  │
│Expectations  │ │   Checks    │ │  Monitoring  │
│(Data Quality)│ │ (Integrity) │ │  (Metrics)   │
└──────────────┘ └──────────────┘ └──────────────┘
    │                │                  │
    └────────────────┼──────────────────┘
                     ▼
            ┌─────────────────┐
            │    Grafana      │
            │  (Dashboards)   │
            └─────────────────┘
```

### Technology Stack
- **Storage**: PostgreSQL (relational database)
- **Compute**: SQL (PL/pgSQL for stored procedures)
- **Orchestration**: Apache Airflow (DAG-based scheduling)
- **Data Quality**: Great Expectations
- **Monitoring**: Prometheus + Grafana

### Advantages
✅ Familiar SQL syntax  
✅ ACID transactions  
✅ Mature tools ecosystem  
✅ Easy to understand & maintain  
✅ Cost-effective for structured data  

### Disadvantages
❌ Vertical scaling only (limited to single server)  
❌ Less flexible for semi-structured data  
❌ Slower for very large datasets (>TB)  
❌ Limited time-travel capabilities  

### Data Flow Example
```
1. Load CSV → Bronze tables
   INSERT INTO stack_a.bronze_transactions (...)
   SELECT * FROM csv_load

2. Transform → Silver tables
   INSERT INTO stack_a.silver_transactions (...)
   SELECT ... FROM stack_a.bronze_transactions
   WHERE quantity > 0 AND amount > 0
   GROUP BY transaction_id (dedup)

3. Aggregate → Gold tables
   INSERT INTO stack_a.gold_daily_sales_fact (...)
   SELECT DATE(txn_date) as date, customer_id, SUM(amount) as total
   FROM stack_a.silver_transactions
   GROUP BY DATE(txn_date), customer_id

4. Validate
   ✓ Referential integrity (customer_id exists)
   ✓ Data quality (no NULLs, valid ranges)
   ✓ Reconciliation (count & sum matching)
```

---

## Stack B: Lakehouse (PySpark + Delta Lake)

### Architecture Diagram
```
┌─────────────────────────────────────────────────────────┐
│               Data Sources (CSV/Streaming)               │
├─────────────────────────────────────────────────────────┤
│  customers.csv | products.csv | transactions.csv        │
│  Real-time streams, APIs, databases, etc.               │
└────────────────┬────────────────┬──────────────────────┘
                 │                │
                 ▼                ▼
        ┌───────────────────────────────────┐
        │  Apache Airflow                   │
        │  - Job orchestration              │
        │  - Scheduling                     │
        │  - Dependency management          │
        └────────────┬──────────────────────┘
                     │
        ┌────────────▼────────────┐
        │    PySpark Cluster      │
        │ ┌──────────┐  ┌──────────┤
        │ │ Master   │  │ Workers  │
        │ ├──────────┘  └──────────┤
        │ └──────────────────────────┤
        └────────────┬────────────────┘
                     │
    ┌────────────────┼────────────────┐
    ▼                ▼                ▼
┌─────────────────┐┌──────────────┐┌──────────────┐
│ Bronze (Delta)  ││ Silver(Delta) ││  Gold(Delta) │
├─────────────────┤├──────────────┤├──────────────┤
│ Parquet files   ││ Parquet files ││Parquet files │
│ ACID enabled    ││ ACID enabled  ││ACID enabled  │
│ Time-travel ✓   ││ Time-travel ✓ ││Time-travel ✓ │
│ Immutable log   ││ Validated     ││Aggregated    │
└─────────────────┘└──────────────┘└──────────────┘
    │                │                │
    └────────────────┼────────────────┘
                     │
        ┌────────────▼───────────────┐
        │   Object Storage (MinIO)   │
        │   S3-compatible API        │
        │   Scalable & Cost-effective│
        └────────────┬───────────────┘
                     │
    ┌────────────────┼─────────────────┐
    ▼                ▼                  ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Great       │ │ Reconciliation│ │  Prometheus  │
│Expectations  │ │   Checks    │ │  Monitoring  │
│(Data Quality)│ │ (Integrity) │ │  (Metrics)   │
└──────────────┘ └──────────────┘ └──────────────┘
    │                │                  │
    └────────────────┼──────────────────┘
                     ▼
            ┌─────────────────┐
            │    Grafana      │
            │  (Dashboards)   │
            └─────────────────┘
```

### Technology Stack
- **Storage**: Delta Lake (over MinIO/S3)
- **Compute**: PySpark (distributed processing)
- **Orchestration**: Apache Airflow
- **Data Quality**: Great Expectations
- **Monitoring**: Prometheus + Grafana

### Advantages
✅ Horizontal scaling (add more workers)  
✅ ACID transactions on data lake  
✅ Time-travel (query historical versions)  
✅ Supports semi-structured data (JSON, Avro)  
✅ Cost-effective for large datasets  
✅ Cloud-native (S3, Azure Blob, GCS)  

### Disadvantages
❌ PySpark learning curve  
❌ Higher operational complexity  
❌ Requires distributed infrastructure  
❌ Cluster management overhead  
❌ Data locality considerations  

### Data Flow Example
```
1. Load CSV → Bronze (Delta)
   df = spark.read.csv("customers.csv", header=True, schema=customers_schema)
   df.write.format("delta").mode("append").save("/delta/bronze/customers")

2. Transform → Silver (Delta)
   df = spark.read.format("delta").load("/delta/bronze/customers")
   df_clean = df.dropDuplicates(["customer_id"]) \
               .withColumn("email_masked", mask_email(col("email"))) \
               .filter(col("first_name").isNotNull())
   df_clean.write.format("delta").mode("overwrite").save("/delta/silver/customers")

3. Aggregate → Gold (Delta)
   df_transactions = spark.read.format("delta").load("/delta/silver/transactions")
   df_gold = df_transactions.groupBy(to_date(col("transaction_date")), col("customer_id")) \
             .agg(sum("amount").alias("daily_total"), count("*").alias("txn_count"))
   df_gold.write.format("delta").mode("append").save("/delta/gold/daily_sales")

4. Validate & Reconcile
   ✓ Count matching (bronze vs silver)
   ✓ Sum matching (accounting)
   ✓ Schema validation
```

---

## Key Concepts

### Idempotency (Re-runnable Pipelines)
**Definition**: Pipeline can be safely re-run without causing errors or duplicates

**Stack A Implementation**:
```sql
-- DELETE + INSERT pattern (idempotent)
DELETE FROM stack_a.silver_transactions;
INSERT INTO stack_a.silver_transactions (...)
SELECT ... FROM stack_a.bronze_transactions
WHERE ... GROUP BY transaction_id;
```

**Stack B Implementation**:
```python
# Overwrite mode (idempotent)
df.write.format("delta").mode("overwrite").save("/delta/silver/transactions")
```

### Data Lineage Tracking
**Purpose**: Know where every piece of data came from

```
Transaction ID 12345
  ↓
  Source File: transactions.csv (load_uuid: abc-123)
  ↓
  Ingestion Time: 2024-01-15 02:30:00
  ↓
  Bronze Table: stack_a.bronze_transactions
  ↓
  Transformation: Cleaned, deduped
  ↓
  Silver Table: stack_a.silver_transactions
  ↓
  Aggregation: Summed by date/customer
  ↓
  Gold Table: stack_a.gold_daily_sales_fact
```

### PII Masking Strategy
**Methods**:
1. **Hashing** (irreversible): `SHA256(email)` - Best for analytics
2. **Tokenization** (reversible): Map original → token - Needed for joins
3. **Deletion**: Don't store sensitive data - Most secure

**Implementation**:
```sql
-- Stack A: Hash email
UPDATE stack_a.silver_customers
SET email_masked = 'SHA256:' || encode(digest(email, 'sha256'), 'hex');
```

```python
# Stack B: Hash email
from pyspark.sql.functions import md5, concat_ws
df = df.withColumn("email_masked", md5(col("email")))
```

### Reconciliation Checks
**Purpose**: Verify data integrity between layers

```
Reconciliation Layer:
├── Count Check
│   └── SELECT COUNT(*) FROM bronze_transactions (100,000)
│       SELECT COUNT(*) FROM silver_transactions (99,998 - 2 dupes removed)
│       Status: PASS (silver ≥ bronze is ok for dedupe)
│
├── Sum Check
│   └── SELECT SUM(amount) FROM bronze (where status='completed') = $1,234,567
│       SELECT SUM(amount) FROM silver (where status='completed') = $1,234,567
│       Status: PASS (within tolerance)
│
└── Referential Integrity Check
    └── SELECT COUNT(*) FROM silver_transactions
        WHERE customer_id NOT IN (SELECT customer_id FROM silver_customers) = 0
        Status: PASS (no orphaned records)
```

---

## Comparison Matrix

| Feature | Stack A (DWH) | Stack B (Lakehouse) |
|---------|---------------|-------------------|
| **Primary Tool** | PostgreSQL | PySpark + Delta |
| **Scalability** | Vertical | Horizontal |
| **Query Language** | SQL | SQL + Python |
| **ACID Transactions** | ✅ Built-in | ✅ Delta Lake |
| **Time Travel** | ❌ Limited | ✅ Full |
| **Data Types** | Structured | Structured + Semi |
| **Cost (Large Scale)** | High | Low |
| **Setup Complexity** | Low | Medium |
| **Team Skill Required** | SQL | Spark + Python |
| **Cloud Native** | ❌ | ✅ |
| **Best For** | <10TB, SQL-heavy | >10TB, Cloud, Flexible |

---

## When to Use Which Stack

### Choose Stack A if:
- ✅ Your data is primarily structured (tables)
- ✅ Team is comfortable with SQL
- ✅ Data volume < 10TB
- ✅ On-premises or limited cloud resources
- ✅ Simple operations preferred

### Choose Stack B if:
- ✅ Your data is semi-structured (JSON, logs)
- ✅ Data volume > 10TB
- ✅ Need horizontal scaling
- ✅ Running on cloud (AWS, GCP, Azure)
- ✅ Team has Spark/Python expertise
- ✅ Need time-travel capability

---

## Monitoring & Observability

### Key Metrics (Both Stacks)

```
Pipeline Health:
├── Pipeline Duration (seconds)
├── Success Rate (%)
├── Failed Task Count
└── Retry Count

Data Quality:
├── Validation Pass Rate (%)
├── Invalid Records
└── Null Count (by column)

Reconciliation:
├── Count Match Status
├── Sum Match Status
└── Discrepancy Rate (%)

System:
├── CPU Usage (%)
├── Memory Usage (%)
├── Disk I/O (MB/s)
└── Database Connections
```

### Alert Thresholds

```
Critical (page on-call):
├── Pipeline failed
├── Data quality < 95%
├── Reconciliation mismatch > 0.1%
└── Database connection lost

Warning (email notification):
├── Pipeline took > 2x normal time
├── Data quality 95-99%
├── Reconciliation mismatch 0.01-0.1%
└── Slow query detected
```

---

## Performance Considerations

### Stack A Optimization
```sql
-- Add indexes on frequently filtered columns
CREATE INDEX idx_transactions_customer ON stack_a.silver_transactions(customer_id);
CREATE INDEX idx_transactions_status ON stack_a.silver_transactions(status);

-- Partition large tables
CREATE TABLE gold_sales_2024 PARTITION OF gold_daily_sales_fact
  FOR VALUES FROM ('2024-01-01') TO ('2024-12-31');

-- Use materialized views for complex aggregations
CREATE MATERIALIZED VIEW monthly_sales AS
  SELECT DATE_TRUNC('month', transaction_date) as month, SUM(amount)
  FROM silver_transactions
  GROUP BY DATE_TRUNC('month', transaction_date);
```

### Stack B Optimization
```python
# Partition by date for faster queries
df.write.format("delta") \
  .mode("append") \
  .partitionBy("transaction_date") \
  .save("/delta/silver/transactions")

# Use Delta Z-order for multi-dimensional filtering
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/delta/silver/transactions")
dt.optimize().executeZOrderBy("customer_id", "product_id")

# Use caching for frequently accessed data
df = spark.read.format("delta").load("/delta/silver/transactions")
df.cache()
df.count()  # trigger materialization
```

---

## Next Steps

1. **Read**: OPERATIONS.md for day-to-day operations
2. **Read**: RUNBOOKS/ for incident response
3. **Review**: code in dags/ and pyspark/ directories
4. **Test**: Run both stacks in your environment
5. **Compare**: Results and performance between stacks

---

**This architecture is designed for reliability, scalability, and operational excellence.**
