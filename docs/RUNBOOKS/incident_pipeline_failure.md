# Runbook: Pipeline Failure

**Severity**: 🔴 Critical  
**Impact**: Data pipeline not running, reports unavailable  
**SLA**: 1 hour to restore  
**Channels**: PagerDuty → Slack → Email  

---

## 1. Alert Received

### Notification Details
```
Time: 2024-01-15 02:35:00 UTC
Alert: Airflow DAG "ecommerce_dwh_stack_a_pipeline" failed
Status: FAILED
Duration: 5 minutes
Task: bronze_ingestion.load_transactions
Error: "Connection timeout to /home/airflow/data/raw/transactions.csv"
```

### Immediate Actions (0-2 minutes)
- [ ] Acknowledge alert in PagerDuty
- [ ] Join war room (Zoom link)
- [ ] Post in #dataops-incidents Slack channel
- [ ] Start timer (1-hour SLA)

---

## 2. Initial Diagnosis (2-5 minutes)

### Check Pipeline Status
```bash
# SSH into Airflow server
ssh airflow-server

# Check DAG status
docker-compose ps | grep airflow

# Expected: All containers UP

# If not running:
docker-compose up -d

# Check DAG logs
docker-compose logs airflow-scheduler | tail -100

# Look for:
# - Connection refused errors
# - File not found
# - Permission denied
# - Out of memory
```

### Check Specific Task Failure
```bash
# Go to Airflow UI
# http://localhost:8080 → DAGs → ecommerce_dwh_stack_a_pipeline

# Click on failed task → Log tab
# Read error message carefully

# Common errors:
# ❌ "Connection refused" → PostgreSQL not running
# ❌ "File not found" → Missing CSV file
# ❌ "Permission denied" → File permission issue
# ❌ "Out of memory" → Increase heap size
# ❌ "Timeout" → Task taking too long
```

### Run Simple Health Check
```bash
# Can Airflow connect to database?
docker-compose exec airflow-webserver \
  airflow dags list

# Can we read the CSV file?
docker-compose exec airflow-webserver \
  ls -lh /home/airflow/data/raw/transactions.csv

# Can we connect to PostgreSQL?
docker-compose exec postgres \
  psql -U airflow -d airflow -c "SELECT COUNT(*) FROM stack_a.bronze_customers"
```

---

## 3. Diagnosis by Error Type

### Error: "File not found"
```bash
# ❌ Problem: CSV file missing
# ✓ Solution:

# 1. Check if file exists
ls -lh /home/airflow/data/raw/transactions.csv

# 2. If missing, regenerate mock data
python /home/airflow/scripts/mock_data_generator.py \
  --records 100000 \
  --output-dir /home/airflow/data/raw

# 3. Verify file created
ls -lh /home/airflow/data/raw/

# 4. Re-run failed task
# In Airflow UI:
# - Click task → Clear → Yes
# - Trigger DAG again
```

### Error: "Connection refused (PostgreSQL)"
```bash
# ❌ Problem: PostgreSQL container not running

# 1. Check if running
docker-compose ps postgres

# If "Exited":
docker-compose logs postgres

# 2. Restart PostgreSQL
docker-compose restart postgres

# 3. Wait for startup (check health)
docker-compose exec postgres \
  pg_isready -U postgres

# 4. Check if schema exists
docker-compose exec postgres \
  psql -U postgres -d airflow -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name='stack_a'"

# 5. If schema missing, recreate it
docker-compose exec postgres \
  psql -U postgres -d airflow -f /docker-entrypoint-initdb.d/stack_a_schema.sql

# 6. Re-run pipeline
# In Airflow UI: Clear task → Trigger DAG
```

### Error: "Out of memory"
```bash
# ❌ Problem: Insufficient memory

# 1. Check memory usage
free -h
docker stats

# 2. Increase Docker memory
# In Docker Desktop:
# Preferences → Resources → Memory → Set to 8GB+

# Or edit docker-compose.yml:
# environment:
#   SPARK_EXECUTOR_MEMORY: 1g  # reduce from 2g
#   SPARK_DRIVER_MEMORY: 1g    # reduce from 2g

# 3. Restart services
docker-compose restart

# 4. Re-run pipeline
```

### Error: "Timeout waiting for task"
```bash
# ❌ Problem: Task taking too long

# 1. Check task logs
# In Airflow UI: Task → Logs
# Look for query running slowly

# 2. Check database performance
docker-compose exec postgres \
  psql -U airflow -d airflow -c "
  SELECT * FROM pg_stat_statements 
  ORDER BY total_exec_time DESC LIMIT 5;
  "

# 3. Kill long-running query if necessary
docker-compose exec postgres \
  psql -U airflow -d airflow -c "
  SELECT pg_terminate_backend(pid) FROM pg_stat_activity 
  WHERE duration > interval '30 minutes';
  "

# 4. Increase task timeout in DAG
# Edit dags/ecommerce_dwh_stack_a.py:
# execution_timeout=timedelta(hours=2)

# 5. Re-run pipeline
```

### Error: "Duplicate key violation"
```bash
# ❌ Problem: Trying to insert duplicate records

# 1. Check if table has data
docker-compose exec postgres \
  psql -U airflow -d airflow -c "
  SELECT COUNT(*) FROM stack_a.bronze_transactions LIMIT 1;
  "

# 2. Clear bronze table (safe for re-run)
docker-compose exec postgres \
  psql -U airflow -d airflow -c "
  DELETE FROM stack_a.bronze_transactions WHERE _ingestion_date > NOW() - interval '1 day';
  "

# 3. Re-run pipeline
```

---

## 4. Root Cause Analysis (5-10 minutes)

### Decision Tree
```
Task failed?
├─ YES: Airflow + Scheduler running?
│   ├─ NO: Start Docker services
│   │   → Re-run
│   ├─ YES: PostgreSQL running?
│   │   ├─ NO: Restart PostgreSQL
│   │   │   → Re-run
│   │   ├─ YES: CSV files exist?
│   │   │   ├─ NO: Regenerate mock data
│   │   │   │   → Re-run
│   │   │   ├─ YES: File readable?
│   │   │   │   ├─ NO: Fix permissions
│   │   │   │   │   → Re-run
│   │   │   │   ├─ YES: Database schema exists?
│   │   │   │   │   ├─ NO: Create schema
│   │   │   │   │   │   → Re-run
│   │   │   │   │   ├─ YES: Check logs for specific error
│   │   │   │   │   │   → Debug by error type above
```

### Debug Commands Cheat Sheet
```bash
# Overall health check
docker-compose ps
docker-compose logs

# Database check
docker-compose exec postgres psql -U postgres -d airflow -c "\dt stack_a.*"
docker-compose exec postgres pg_isready

# Airflow check
docker-compose logs airflow-scheduler | grep -i "error"
curl -u airflow:airflow http://localhost:8080/api/v1/health

# File check
ls -lh /home/airflow/data/raw/
wc -l /home/airflow/data/raw/transactions.csv

# Memory check
docker stats
free -h
```

---

## 5. Remediation Steps

### Step 1: Fix the Issue
- [ ] Identify root cause from diagnosis above
- [ ] Apply appropriate fix from section 3
- [ ] Verify fix (run health check)

### Step 2: Clear & Retry
```bash
# In Airflow UI:
# 1. Click on failed task
# 2. Click "Clear"
# 3. Confirm "Downstream" option
# 4. Go back to DAG
# 5. Click "Trigger DAG"

# Or via CLI:
docker-compose exec airflow-webserver \
  airflow tasks clear ecommerce_dwh_stack_a_pipeline \
  --upstream --downstream --yes \
  -t bronze_ingestion.load_transactions

# Trigger retry
docker-compose exec airflow-webserver \
  airflow dags trigger ecommerce_dwh_stack_a_pipeline
```

### Step 3: Monitor Progress
```bash
# Watch real-time logs
docker-compose logs -f airflow-scheduler

# Check task status in UI every 30 seconds
# http://localhost:8080

# Expected timeline:
# 02:35 - Pipeline started
# 02:37 - Bronze ingestion (load_customers, load_products, load_transactions)
# 02:40 - Silver transformation
# 02:43 - Gold aggregation
# 02:45 - Reconciliation
# 02:48 - Data quality checks
# 02:50 - Pipeline SUCCESS ✓
```

### Step 4: Verify Data Integrity
```bash
# Check row counts
docker-compose exec postgres psql -U airflow -d airflow << 'EOF'
SELECT 
  (SELECT COUNT(*) FROM stack_a.bronze_customers) as bronze_customers,
  (SELECT COUNT(*) FROM stack_a.silver_customers) as silver_customers,
  (SELECT COUNT(*) FROM stack_a.bronze_products) as bronze_products,
  (SELECT COUNT(*) FROM stack_a.silver_products) as silver_products,
  (SELECT COUNT(*) FROM stack_a.bronze_transactions) as bronze_transactions,
  (SELECT COUNT(*) FROM stack_a.silver_transactions) as silver_transactions;
EOF

# Check reconciliation results
docker-compose exec postgres psql -U airflow -d airflow -c "
SELECT * FROM stack_a.reconciliation_results 
ORDER BY reconciliation_id DESC LIMIT 3;
"

# Should show status='PASSED' for most recent run
```

---

## 6. Post-Incident Actions (After Restoration)

### Immediate (0-5 minutes after success)
- [ ] Post "RESOLVED" in #dataops-incidents
- [ ] Update incident status in PagerDuty
- [ ] Confirm all reports regenerated correctly
- [ ] Stop timer and note RTO (Recovery Time Objective)

### Short-term (Next 1-2 hours)
- [ ] Write incident summary
  - What failed?
  - Root cause?
  - How was it fixed?
  - How long until detected? (MTTD)
  - How long to fix? (MTTR)

### Medium-term (Next 24 hours)
- [ ] Create ticket to prevent recurrence
  - E.g., "Add CSV validation step" or "Implement disk space monitoring"
- [ ] Update runbook with learnings
- [ ] Share findings in team standup

### Long-term (Next sprint)
- [ ] Implement preventive measures
  - Add automated health checks
  - Improve monitoring/alerting
  - Add circuit breakers
  - Implement auto-recovery

---

## 7. Escalation Path

### Escalation Levels
```
Level 1: On-call Engineer (you)
├─ Can: Restart services, Clear tasks, Run diagnostics
├─ SLA: 15 minutes to acknowledge
└─ If unresolved in 15 minutes → Escalate

Level 2: DataOps Lead
├─ Can: Modify code, Scale infrastructure, Database recovery
├─ SLA: 5 minutes after Level 1 escalation
└─ If unresolved in 20 minutes → Escalate

Level 3: Engineering Manager + Database Admin
├─ Can: Deep database investigation, Rollback changes, Infrastructure rebuild
├─ SLA: 2 minutes after Level 2 escalation
└─ Continue until resolved
```

### When to Escalate
- ❌ Error message not in this runbook
- ❌ Tried all fixes, issue persists > 20 minutes
- ❌ Data integrity compromised (reconciliation failed)
- ❌ Multiple components failing
- ❌ Uncertainty about root cause

### Escalation Process
```bash
# 1. In Slack #dataops-incidents:
# "@DataOps_Lead - Escalating failure of [component]. 
# Tried: [steps taken]. Status: [still failing].
# Please take over."

# 2. Add to PagerDuty incident
# Click "Escalate" → Select manager

# 3. Have diagnostics ready to share
docker-compose logs > /tmp/incidents/logs_$(date +%s).txt
docker-compose ps > /tmp/incidents/status_$(date +%s).txt
```

---

## 8. Prevention & Monitoring

### Recommended Monitoring Alerts
```yaml
Alerts to create:
- CPU usage > 80% for > 5 minutes
- Memory usage > 85% for > 5 minutes
- Disk space < 10% remaining
- PostgreSQL connections > 90%
- Task execution time > 2x baseline
- Failed task in last 24 hours
- Reconciliation mismatch > 0.1%
```

### Recommended Preventive Measures
1. **Automated Health Checks**
   - Add startup check to verify all dependencies ready
   - Run pre-pipeline validation (file exists, readable, valid schema)

2. **Better Error Handling**
   - Add try-catch blocks in Python tasks
   - Send detailed error messages, not just "failed"
   - Log context (rows processed, time elapsed, memory used)

3. **Resource Management**
   - Monitor disk space → alert at 20% used
   - Monitor memory → set limits in docker-compose.yml
   - Monitor CPU → consider scheduling tasks during off-peak hours

4. **Data Validation**
   - Add Great Expectations checks at every layer
   - Fail fast if data quality issues detected
   - Quarantine invalid data for review

---

## Quick Reference

### Critical Phone Numbers
```
On-Call: +1-XXX-XXX-XXXX
Manager: +1-XXX-XXX-XXXX
DBAdmin: +1-XXX-XXX-XXXX
```

### Critical Contacts
```
Slack: @dataops-team
Email: dataops@company.com
PagerDuty: https://company.pagerduty.com
```

### Critical Documentation
```
Runbooks: /docs/RUNBOOKS/
Logs: /var/log/docker/
Config: /etc/docker/docker-compose.yml
Database: postgres://airflow:***@postgres:5432/airflow
```

### Critical Commands (copy-paste)
```bash
# Full restart
docker-compose down -v && docker-compose up -d

# Check everything
docker-compose ps && docker-compose exec postgres pg_isready

# Regenerate data
python /home/airflow/scripts/mock_data_generator.py --records 100000

# Full diagnostics
docker-compose logs > /tmp/diagnostics.log && \
docker-compose ps > /tmp/status.txt && \
docker stats --no-stream > /tmp/resources.txt
```

---

## Success Criteria

Pipeline is considered **RESOLVED** when:
- ✅ Task status = SUCCESS in Airflow UI
- ✅ All rows loaded to silver layer
- ✅ Reconciliation status = PASSED
- ✅ Data quality validation = PASSED
- ✅ Reporters confirm they can access fresh data

---

**For additional help, see OPERATIONS.md or contact @dataops-team**
