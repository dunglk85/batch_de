# DataOps Pipeline Operations Guide

## Daily Operations Checklist

### Morning (Start of Day)
```
⏰ 6:00 AM
├─ [ ] Check Slack for overnight alerts
├─ [ ] Review PagerDuty incidents (if any)
├─ [ ] Check pipeline success dashboard
└─ [ ] Verify all services healthy (docker-compose ps)

⏰ 6:30 AM (After pipeline runs at 2 AM)
├─ [ ] Confirm ecommerce_dwh_stack_a_pipeline succeeded
├─ [ ] Check reconciliation_results table (status='PASSED')
├─ [ ] Verify data quality checks (> 99% pass rate)
└─ [ ] Confirm reporters accessing fresh data
```

### Mid-Day (Monitoring)
```
⏰ 12:00 PM
├─ [ ] Check resource utilization
│   ├─ CPU: Should be < 50% (outside pipeline window)
│   ├─ Memory: Should be < 60%
│   └─ Disk: Should have > 10% free space
├─ [ ] Review slow query logs
├─ [ ] Check PostgreSQL connection count (should be < 20)
└─ [ ] Scan Airflow logs for warnings
```

### End of Day (Sign-off)
```
⏰ 5:00 PM
├─ [ ] Review "Data Quality Alerts" dashboard in Grafana
├─ [ ] Check failed task count (should be 0)
├─ [ ] Document any issues encountered
├─ [ ] Update on-call schedule if needed
└─ [ ] Prepare escalation contact info for off-hours
```

---

## Monitoring Setup

### Prometheus Metrics Configuration

```yaml
# config/monitoring/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    environment: 'production'
    team: 'dataops'

scrape_configs:
  # Airflow metrics
  - job_name: 'airflow-webserver'
    static_configs:
      - targets: ['airflow-webserver:8080']
    metrics_path: '/admin/metrics'
    scrape_interval: 30s

  # PostgreSQL metrics (requires postgres_exporter)
  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']
    scrape_interval: 30s

  # Spark metrics
  - job_name: 'spark-master'
    static_configs:
      - targets: ['spark-master:8080']
    scrape_interval: 30s

  # Node exporter (system metrics)
  - job_name: 'node'
    static_configs:
      - targets: ['localhost:9100']
    scrape_interval: 30s

# Alert rules
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']

rule_files:
  - '/etc/prometheus/alert_rules.yml'
```

### Alert Rules Configuration

```yaml
# config/monitoring/alert_rules.yml
groups:
  - name: dataops_alerts
    interval: 15s
    rules:

      # Pipeline failures
      - alert: AirflowDAGFailed
        expr: airflow_dag_run_total{state="failed"} > 0
        for: 1m
        labels:
          severity: critical
          team: dataops
        annotations:
          summary: "Airflow DAG failed: {{ $labels.dag_id }}"
          description: "DAG {{ $labels.dag_id }} failed at {{ $value }}"

      # Resource alerts
      - alert: HighMemoryUsage
        expr: container_memory_usage_bytes / container_spec_memory_limit_bytes > 0.85
        for: 5m
        labels:
          severity: warning
          team: devops
        annotations:
          summary: "High memory usage: {{ $labels.name }}"
          description: "Container {{ $labels.name }} using {{ $value | humanizePercentage }}"

      - alert: LowDiskSpace
        expr: node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes < 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Low disk space on {{ $labels.instance }}"

      # Data quality alerts
      - alert: DataQualityCheckFailed
        expr: great_expectations_validation_failures > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Data quality check failed"
          description: "{{ $value }} validations failed"

      # Reconciliation alerts
      - alert: ReconciliationMismatch
        expr: reconciliation_mismatch_percentage > 0.001
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Reconciliation mismatch detected"
          description: "Mismatch: {{ $value | humanizePercentage }}"
```

### Grafana Dashboard Configuration

```json
{
  "dashboard": {
    "title": "DataOps Pipeline Health",
    "panels": [
      {
        "title": "Pipeline Status",
        "targets": [
          {
            "expr": "airflow_dag_run_total{state=\"success\"} / airflow_dag_run_total",
            "legendFormat": "Success Rate"
          }
        ],
        "type": "stat",
        "thresholds": {
          "mode": "absolute",
          "steps": [
            {"color": "red", "value": null, "comparison": "lt", "value": 0.95},
            {"color": "yellow", "value": 0.95, "comparison": "lt", "value": 0.99},
            {"color": "green", "value": 0.99}
          ]
        }
      },
      {
        "title": "Data Quality Score",
        "targets": [
          {"expr": "great_expectations_validation_pass_rate"}
        ],
        "type": "gauge",
        "unit": "percentunit"
      },
      {
        "title": "Pipeline Duration",
        "targets": [
          {"expr": "airflow_dag_run_duration_seconds"}
        ],
        "type": "graph"
      },
      {
        "title": "Reconciliation Status",
        "targets": [
          {"expr": "reconciliation_status{status=\"passed\"}"}
        ],
        "type": "stat"
      },
      {
        "title": "Resource Usage",
        "targets": [
          {"expr": "container_memory_usage_bytes"},
          {"expr": "rate(container_cpu_usage_seconds_total[5m])"}
        ],
        "type": "graph"
      }
    ]
  }
}
```

---

## Performance Tuning

### PostgreSQL Optimization (Stack A)

```sql
-- Connection pooling
-- In docker-compose.yml, add pgBouncer:
# pgbouncer:
#   image: edoburu/pgbouncer:latest
#   environment:
#     DATABASES_HOST: postgres
#     DATABASES_PORT: 5432
#     DATABASES_USER: postgres
#     DATABASES_PASSWORD: postgres123
#     PGBOUNCER_POOL_MODE: transaction
#     PGBOUNCER_MAX_CLIENT_CONN: 1000
#     PGBOUNCER_DEFAULT_POOL_SIZE: 25

-- Query optimization
-- Analyze slow queries
EXPLAIN ANALYZE
SELECT COUNT(*) FROM stack_a.silver_transactions
WHERE transaction_date > NOW() - INTERVAL '7 days'
AND status = 'completed';

-- Add indexes
CREATE INDEX CONCURRENTLY idx_txn_date_status 
  ON stack_a.silver_transactions(transaction_date DESC, status);

-- Vacuum & analyze
VACUUM ANALYZE stack_a.silver_transactions;

-- Check index bloat
SELECT schemaname, tablename, ROUND(100.0*OTTA*(CASE WHEN otta=0 OR sml.relpages=0 OR sml.relpages=otta THEN 1.0 ELSE sml.relpages/otta END)/(sml.relpages)::float, 2) AS table_bloat_ratio
FROM pg_class AS c
JOIN pg_stat_user_tables AS sml ON sml.relid = c.oid
WHERE sml.n_tup_ins + sml.n_tup_upd + sml.n_tup_del > 0;
```

### PySpark Optimization (Stack B)

```python
# Partition pruning
df = spark.read.format("delta").load("/delta/silver/transactions")
df_filtered = df.filter(col("transaction_date") >= "2024-01-01")  # Partition pruning!
df_filtered.explain()  # Check that partitions are pruned

# Z-order optimization
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/delta/silver/transactions")
dt.optimize().executeZOrderBy("customer_id", "product_id")

# Caching strategy
df = spark.read.format("delta").load("/delta/silver/transactions")
df.cache()
print(df.count())  # Materialize cache

# Broadcast small tables
customers = spark.read.format("delta").load("/delta/silver/customers")
transactions = spark.read.format("delta").load("/delta/silver/transactions")
result = transactions.join(broadcast(customers), "customer_id")

# Monitor execution
spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
```

---

## Maintenance Tasks

### Weekly Maintenance

```bash
# 1. Vacuum & analyze PostgreSQL
docker-compose exec postgres psql -U postgres -d airflow -c "
VACUUM ANALYZE;
REINDEX DATABASE airflow;
"

# 2. Check & clean logs
docker-compose logs --tail=0 > /tmp/logs_$(date +%Y%m%d).tar.gz
find /home/airflow/logs -mtime +30 -delete  # Remove logs > 30 days

# 3. Review disk usage
docker exec dataops-postgres du -sh /var/lib/postgresql/data

# 4. Test backup/restore
# (See backup section below)
```

### Monthly Maintenance

```bash
# 1. Upgrade dependencies
docker-compose exec airflow-webserver \
  pip list --outdated

# 2. Update Docker images
docker-compose pull
docker-compose up -d

# 3. Archive old data
docker-compose exec postgres psql -U postgres -d airflow << 'EOF'
-- Archive old transactions to separate table
CREATE TABLE stack_a.silver_transactions_archive_2023 AS
SELECT * FROM stack_a.silver_transactions
WHERE YEAR(transaction_date) = 2023;

-- Delete old data
DELETE FROM stack_a.silver_transactions
WHERE YEAR(transaction_date) = 2023;
EOF

# 4. Reorganize indexes
docker-compose exec postgres psql -U postgres -d airflow -c "
REINDEX TABLE CONCURRENTLY stack_a.silver_transactions;
REINDEX TABLE CONCURRENTLY stack_a.bronze_transactions;
"
```

---

## Backup & Disaster Recovery

### Automated Backups

```bash
#!/bin/bash
# scripts/backup.sh - Run daily via cron

BACKUP_DIR="/backups/dataops"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

# 1. PostgreSQL backup
docker-compose exec -T postgres pg_dump -U postgres airflow | \
  gzip > $BACKUP_DIR/postgres_$DATE.sql.gz

# 2. Data files backup
tar -czf $BACKUP_DIR/data_$DATE.tar.gz data/

# 3. Configuration backup
tar -czf $BACKUP_DIR/config_$DATE.tar.gz config/ dags/

# 4. Keep only last 30 days
find $BACKUP_DIR -mtime +30 -delete

echo "Backup complete: $BACKUP_DIR"
```

### Restore from Backup

```bash
#!/bin/bash
# scripts/restore.sh

BACKUP_FILE=$1
DATE=$(date +%Y%m%d_%H%M%S)

# 1. Restore PostgreSQL
gunzip -c $BACKUP_FILE | \
  docker-compose exec -T postgres psql -U postgres airflow

# 2. Restore data files
tar -xzf data_backup.tar.gz

# 3. Restart services
docker-compose restart airflow-webserver airflow-scheduler

echo "Restore complete"
```

---

## Scaling the Pipeline

### Horizontal Scaling (Stack B - PySpark)

```yaml
# Add more Spark workers in docker-compose.yml
spark-worker-2:
  image: spark:3.5.3-scala2.12-java11-python3-ubuntu
  command:
    - "/opt/spark/bin/spark-class"
    - "org.apache.spark.deploy.worker.Worker"
    - "spark://spark-master:7077"
  environment:
    SPARK_WORKER_MEMORY: 2g
    SPARK_WORKER_CORES: 4
  depends_on:
    - spark-master

spark-worker-3:
  image: spark:3.5.3-scala2.12-java11-python3-ubuntu
  command:
    - "/opt/spark/bin/spark-class"
    - "org.apache.spark.deploy.worker.Worker"
    - "spark://spark-master:7077"
  environment:
    SPARK_WORKER_MEMORY: 2g
    SPARK_WORKER_CORES: 4
  depends_on:
    - spark-master

# Then restart
docker-compose up -d
```

### Vertical Scaling (Stack A - PostgreSQL)

```yaml
# In docker-compose.yml, increase PostgreSQL resources
postgres:
  image: postgres:15-alpine
  environment:
    POSTGRES_SHARED_BUFFERS: 4GB  # Was 256MB
    POSTGRES_EFFECTIVE_CACHE_SIZE: 8GB  # Was 1GB
    POSTGRES_WORK_MEM: 20MB  # Was 5MB
    POSTGRES_MAX_CONNECTIONS: 200  # Was 100
```

---

## Security Best Practices

### 1. Secrets Management

```bash
# Use environment variables, never hardcode secrets
# In .env (git-ignored):
DB_PASSWORD=secure_postgres_password_123
AIRFLOW_DB_PASSWORD=secure_airflow_password_456

# Load in docker-compose.yml:
environment:
  AIRFLOW__DATABASE__SQLALCHEMY_CONN: postgresql://airflow:${AIRFLOW_DB_PASSWORD}@postgres:5432/airflow

# For production, use:
# - AWS Secrets Manager
# - Azure Key Vault
# - HashiCorp Vault
# - Kubernetes Secrets
```

### 2. Access Control

```sql
-- Create roles with least privilege
CREATE ROLE data_loader WITH LOGIN PASSWORD 'secure_password';
GRANT USAGE ON SCHEMA stack_a TO data_loader;
GRANT INSERT, UPDATE ON stack_a.bronze_* TO data_loader;

CREATE ROLE analyst WITH LOGIN PASSWORD 'secure_password';
GRANT USAGE ON SCHEMA stack_a TO analyst;
GRANT SELECT ON stack_a.gold_* TO analyst;
-- No access to silver/bronze (raw data)

-- Audit
CREATE TABLE audit_log (
  user_name TEXT,
  query TEXT,
  timestamp TIMESTAMP DEFAULT NOW()
);

-- Track all queries to sensitive tables
CREATE POLICY sensitive_data_audit
  ON stack_a.silver_customers
  USING (CURRENT_USER IN ('analyst', 'reporter'));
```

### 3. Data Encryption

```bash
# PostgreSQL SSL
# 1. Generate certificate
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 365 -nodes

# 2. Copy to PostgreSQL data directory
docker cp server.crt dataops-postgres:/var/lib/postgresql/data/
docker cp server.key dataops-postgres:/var/lib/postgresql/data/

# 3. Update postgresql.conf
docker-compose exec postgres \
  sed -i "s/#ssl = off/ssl = on/" /var/lib/postgresql/data/postgresql.conf

# 4. Restart
docker-compose restart postgres

# 5. Connect with SSL
psql "postgres://airflow@postgres:5432/airflow?sslmode=require"
```

### 4. Audit Logging

```python
# In Airflow tasks, log all data access
import logging
logger = logging.getLogger(__name__)

def load_csv_to_bronze(table_name: str, csv_path: str, **kwargs):
    # Log the action
    logger.info(f"User {kwargs['task'].owner} loading {csv_path} to {table_name}")
    
    # Store audit record
    cursor.execute("""
        INSERT INTO audit_log (user_name, action, table_name, timestamp)
        VALUES (%s, %s, %s, NOW())
    """, (kwargs['task'].owner, 'load', table_name))
    
    # Proceed with load
    ...
```

---

## Troubleshooting Common Issues

### Issue: Slow Pipeline
```bash
# 1. Check what's slow
docker-compose logs airflow-scheduler | grep "duration"

# 2. Analyze query plans
docker-compose exec postgres psql -U postgres -d airflow -c "
EXPLAIN ANALYZE SELECT ... FROM stack_a.silver_transactions;
"

# 3. Check resource usage
docker stats dataops-postgres

# 4. Add indexes if needed
CREATE INDEX idx_name ON table(column);

# 5. Increase memory if needed
# Edit docker-compose.yml and restart
```

### Issue: Data Quality Failures

```bash
# 1. Review Great Expectations report
open ge_validation_report.html

# 2. Identify failing records
docker-compose exec postgres psql -U postgres -d airflow -c "
SELECT * FROM stack_a.silver_transactions
WHERE dq_is_valid = FALSE
LIMIT 100;
"

# 3. Fix source data or update expectations
# Update config/great_expectations/expectations/

# 4. Re-run validation
docker-compose exec airflow-webserver \
  bash -c "cd /home/airflow && great_expectations validate"
```

### Issue: Reconciliation Mismatch

```bash
# 1. Review reconciliation results
docker-compose exec postgres psql -U postgres -d airflow -c "
SELECT * FROM stack_a.reconciliation_results
ORDER BY reconciliation_id DESC LIMIT 1;
"

# 2. Check specific discrepancies
SELECT 
  'bronze' as layer, COUNT(*) as row_count, SUM(amount) as total_amount
FROM stack_a.bronze_transactions
WHERE status = 'completed'

UNION ALL

SELECT 
  'silver' as layer, COUNT(*) as row_count, SUM(amount) as total_amount
FROM stack_a.silver_transactions
WHERE status = 'completed';

# 3. Investigate differences
# Are rows being deduped? Are there NULL values? Wrong transformations?

# 4. Update reconciliation logic if needed
# Edit dags/ecommerce_dwh_stack_a.py
```

---

## Contact & Escalation

### Team Directory
```
Role                     Name              Slack              Phone
─────────────────────────────────────────────────────────────────
Data Engineer (On-Call)  Alice             @alice             555-1001
DataOps Lead            Bob               @bob               555-1002
Database Admin          Carol             @carol             555-1003
Manager                 David             @david             555-1004
VP Engineering          Emma              @emma              555-1005
```

### Escalation Path
1. **On-Call Engineer** (you) → 15-min SLA
2. **DataOps Lead** → 5-min after escalation
3. **Database Admin** → 2-min after escalation
4. **Manager** → Inform of critical issues > 30 min

### Communication Channels
- Slack: #dataops-incidents
- PagerDuty: dataops-oncall
- Email: dataops@company.com
- War Room Zoom: https://zoom.us/my/dataops

---

## Further Learning

### Key Resources
- [Apache Airflow Documentation](https://airflow.apache.org)
- [PostgreSQL Performance Tips](https://wiki.postgresql.org/wiki/Performance_Optimization)
- [PySpark Best Practices](https://spark.apache.org/docs/latest/best-practices.html)
- [Delta Lake Architecture](https://docs.delta.io/)

### Recommended Reading
- "The Data Warehouse Toolkit" by Ralph Kimball
- "Fundamentals of Data Engineering" by Joe Reis & Matt Housley
- "Site Reliability Engineering" by Google

---

**Last Updated**: 2024-01-15  
**Next Review**: 2024-02-15  
**Maintained By**: DataOps Team
