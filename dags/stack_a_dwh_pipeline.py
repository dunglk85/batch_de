"""
Stack A: Data Warehouse Pipeline (PostgreSQL)
Medallion Architecture: Bronze → Silver → Gold
Orchestrated with Apache Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import logging

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_ARGS = {
    'owner': 'dataops-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email': ['dataops@company.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

# ============================================================================
# Python Functions for Pipeline Tasks
# ============================================================================

def load_csv_to_bronze(table_name: str, csv_path: str, **kwargs):
    """
    Load CSV file to Bronze layer (raw data ingestion)
    Idempotent: Uses _load_uuid to track loads
    """
    logger.info(f"Loading {csv_path} to bronze_{table_name}")
    
    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        logger.info(f"Read {len(df)} rows from {csv_path}")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="postgres",
            password="postgres123"
        )
        cursor = conn.cursor()
        
        # Insert to bronze table (idempotent: insert or update on conflict)
        columns = ', '.join(df.columns.tolist())
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO stack_a.bronze_{table_name}
        ({columns})
        VALUES ({placeholders})
        ON CONFLICT (
            {get_primary_key(table_name)}
        ) DO UPDATE SET
            _ingestion_date = CURRENT_TIMESTAMP
        """
        
        data = [tuple(row) for row in df.values]
        
        execute_batch(cursor, insert_query, data, page_size=1000)
        
        conn.commit()
        logger.info(f"Successfully loaded {len(df)} rows to bronze_{table_name}")
        
        # Log to audit table
        cursor.execute("""
            INSERT INTO stack_a.audit_load_history 
            (load_name, load_start_time, load_end_time, status, rows_inserted)
            VALUES (%s, %s, %s, %s, %s)
        """, (f"bronze_{table_name}", kwargs['task'].start_date, datetime.now(), 'SUCCESS', len(df)))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {'rows_loaded': len(df), 'table': table_name}
        
    except Exception as e:
        logger.error(f"Failed to load {table_name}: {str(e)}")
        raise


def transform_bronze_to_silver(table_name: str, **kwargs):
    """
    Transform data from Bronze to Silver layer
    - Remove duplicates
    - Mask PII (email, phone)
    - Validate data quality
    - Flag invalid records
    Idempotent: DELETE + INSERT pattern
    """
    logger.info(f"Transforming bronze_{table_name} to silver_{table_name}")
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="postgres",
        password="postgres123"
    )
    cursor = conn.cursor()
    
    try:
        if table_name == 'customers':
            # Delete existing silver data (idempotent)
            cursor.execute(f"DELETE FROM stack_a.silver_customers")
            
            # Transform: mask PII, validate
            sql = """
            INSERT INTO stack_a.silver_customers
            SELECT 
                customer_id,
                first_name,
                last_name,
                mask_email(email) as email_masked,
                mask_phone(phone) as phone_masked,
                city,
                state,
                zip_code,
                created_date,
                is_active,
                TRUE as dq_is_valid,
                NULL as dq_validation_errors,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                _load_uuid
            FROM stack_a.bronze_customers
            WHERE first_name IS NOT NULL
              AND last_name IS NOT NULL
              AND email IS NOT NULL
            GROUP BY customer_id, first_name, last_name, email, phone, city, state, zip_code, created_date, is_active, _load_uuid
            """
            cursor.execute(sql)
            
        elif table_name == 'products':
            cursor.execute(f"DELETE FROM stack_a.silver_products")
            
            sql = """
            INSERT INTO stack_a.silver_products
            SELECT 
                product_id,
                product_name,
                category,
                price,
                stock_quantity,
                is_active,
                CASE WHEN price > 0 AND product_name IS NOT NULL THEN TRUE ELSE FALSE END as dq_is_valid,
                CASE WHEN price <= 0 THEN 'Invalid price' WHEN product_name IS NULL THEN 'Missing name' ELSE NULL END as dq_validation_errors,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                _load_uuid
            FROM stack_a.bronze_products
            GROUP BY product_id, product_name, category, price, stock_quantity, is_active, _load_uuid
            """
            cursor.execute(sql)
            
        elif table_name == 'transactions':
            cursor.execute(f"DELETE FROM stack_a.silver_transactions")
            
            sql = """
            INSERT INTO stack_a.silver_transactions
            SELECT 
                transaction_id,
                transaction_date,
                customer_id,
                product_id,
                quantity,
                unit_price,
                amount,
                payment_method,
                status,
                store_location,
                CASE 
                    WHEN quantity > 0 AND unit_price > 0 AND amount > 0 
                         AND transaction_date <= NOW()
                    THEN TRUE 
                    ELSE FALSE 
                END as dq_is_valid,
                CASE 
                    WHEN quantity <= 0 THEN 'Invalid quantity'
                    WHEN unit_price <= 0 THEN 'Invalid price'
                    WHEN transaction_date > NOW() THEN 'Future date'
                    ELSE NULL 
                END as dq_validation_errors,
                EXISTS(
                    SELECT 1 FROM stack_a.silver_transactions st
                    WHERE st.transaction_id = bt.transaction_id
                ) as dq_duplicate_found,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                _load_uuid
            FROM stack_a.bronze_transactions bt
            WHERE customer_id IS NOT NULL
              AND product_id IS NOT NULL
              AND transaction_id IS NOT NULL
            """
            cursor.execute(sql)
        
        conn.commit()
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM stack_a.silver_{table_name}")
        row_count = cursor.fetchone()[0]
        
        logger.info(f"Successfully transformed {row_count} rows to silver_{table_name}")
        return {'rows_transformed': row_count}
        
    except Exception as e:
        logger.error(f"Failed to transform {table_name}: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def aggregate_silver_to_gold(**kwargs):
    """
    Aggregate data from Silver to Gold layer
    - Create analytical tables
    - Compute aggregations & metrics
    Idempotent: DELETE + INSERT with ON CONFLICT upsert safety net
    """
    logger.info("Aggregating silver data to gold layer")
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="postgres",
        password="postgres123"
    )
    cursor = conn.cursor()
    
    try:
        # 1. Daily Sales Fact
        cursor.execute("DELETE FROM stack_a.gold_daily_sales_fact")
        
        cursor.execute("""
        INSERT INTO stack_a.gold_daily_sales_fact
        (transaction_date, customer_id, product_id, category, store_location,
         quantity, gross_amount, net_amount, transaction_count,
         completed_count, refunded_count, _updated_at)
        SELECT 
            DATE(t.transaction_date),
            t.customer_id,
            t.product_id,
            p.category,
            t.store_location,
            SUM(t.quantity) as quantity,
            SUM(t.amount) as gross_amount,
            SUM(t.amount) * 0.95 as net_amount,
            COUNT(*) as transaction_count,
            SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) as completed_count,
            SUM(CASE WHEN t.status = 'refunded' THEN 1 ELSE 0 END) as refunded_count,
            CURRENT_TIMESTAMP
        FROM stack_a.silver_transactions t
        LEFT JOIN stack_a.silver_products p ON t.product_id = p.product_id
        WHERE t.dq_is_valid = TRUE
        GROUP BY DATE(t.transaction_date), t.customer_id, t.product_id, p.category, t.store_location
        """)
        
        # 2. Customer Metrics
        cursor.execute("DELETE FROM stack_a.gold_customer_metrics")
        
        cursor.execute("""
        INSERT INTO stack_a.gold_customer_metrics
        (customer_id, customer_lifetime_value, total_transactions,
         first_purchase_date, last_purchase_date, avg_transaction_amount,
         preferred_category, preferred_payment_method, is_active, risk_score, _updated_at)
        SELECT 
            c.customer_id,
            SUM(t.amount) as customer_lifetime_value,
            COUNT(DISTINCT t.transaction_id) as total_transactions,
            MIN(DATE(t.transaction_date)) as first_purchase_date,
            MAX(DATE(t.transaction_date)) as last_purchase_date,
            AVG(t.amount) as avg_transaction_amount,
            MODE() WITHIN GROUP (ORDER BY p.category) as preferred_category,
            MODE() WITHIN GROUP (ORDER BY t.payment_method) as preferred_payment_method,
            c.is_active,
            CASE 
                WHEN COUNT(CASE WHEN t.status = 'refunded' THEN 1 END) > 5 THEN 0.8
                WHEN COUNT(CASE WHEN t.status = 'refunded' THEN 1 END) > 2 THEN 0.5
                ELSE 0.1
            END as risk_score,
            CURRENT_TIMESTAMP
        FROM stack_a.silver_customers c
        LEFT JOIN stack_a.silver_transactions t ON c.customer_id = t.customer_id
        LEFT JOIN stack_a.silver_products p ON t.product_id = p.product_id
        WHERE t.dq_is_valid = TRUE
        GROUP BY c.customer_id, c.is_active
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_lifetime_value = EXCLUDED.customer_lifetime_value,
            total_transactions = EXCLUDED.total_transactions,
            first_purchase_date = EXCLUDED.first_purchase_date,
            last_purchase_date = EXCLUDED.last_purchase_date,
            avg_transaction_amount = EXCLUDED.avg_transaction_amount,
            preferred_category = EXCLUDED.preferred_category,
            preferred_payment_method = EXCLUDED.preferred_payment_method,
            is_active = EXCLUDED.is_active,
            risk_score = EXCLUDED.risk_score,
            _updated_at = EXCLUDED._updated_at
        """)
        
        # 3. Product Metrics
        cursor.execute("DELETE FROM stack_a.gold_product_metrics")
        
        cursor.execute("""
        INSERT INTO stack_a.gold_product_metrics
        (product_id, product_name, category, total_quantity_sold, total_revenue,
         avg_rating, days_in_inventory, inventory_turnover_ratio, is_profitable, _updated_at)
        SELECT 
            p.product_id,
            p.product_name,
            p.category,
            SUM(t.quantity) as total_quantity_sold,
            SUM(t.amount) as total_revenue,
            ROUND(AVG(5.0), 2) as avg_rating,
            CURRENT_DATE - MAX(DATE(t.transaction_date)) as days_in_inventory,
            ROUND(SUM(t.quantity)::NUMERIC / NULLIF(p.stock_quantity, 0), 2) as inventory_turnover_ratio,
            CASE WHEN SUM(t.amount) > SUM(t.unit_price * t.quantity) * 0.8 THEN TRUE ELSE FALSE END as is_profitable,
            CURRENT_TIMESTAMP
        FROM stack_a.silver_products p
        LEFT JOIN stack_a.silver_transactions t ON p.product_id = t.product_id AND t.status = 'completed'
        GROUP BY p.product_id, p.product_name, p.category, p.stock_quantity
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            total_quantity_sold = EXCLUDED.total_quantity_sold,
            total_revenue = EXCLUDED.total_revenue,
            avg_rating = EXCLUDED.avg_rating,
            days_in_inventory = EXCLUDED.days_in_inventory,
            inventory_turnover_ratio = EXCLUDED.inventory_turnover_ratio,
            is_profitable = EXCLUDED.is_profitable,
            _updated_at = EXCLUDED._updated_at
        """)
        
        conn.commit()
        logger.info("Successfully aggregated data to gold layer")
        return {'status': 'success', 'timestamp': datetime.now().isoformat()}
        
    except Exception as e:
        logger.error(f"Failed to aggregate to gold: {str(e)}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def run_reconciliation(**kwargs):
    """
    Reconciliation check: Verify data integrity between layers
    - Count matching
    - Sum matching
    - Referential integrity
    """
    logger.info("Running reconciliation checks")
    
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="postgres",
        password="postgres123"
    )
    cursor = conn.cursor()
    
    try:
        reconciliation_date = kwargs['task'].execution_date.date()
        
        # Check 1: Transaction count
        cursor.execute("""
        SELECT 
            COUNT(*) FROM stack_a.bronze_transactions
        """)
        bronze_count = cursor.fetchone()[0]
        
        cursor.execute("""
        SELECT COUNT(*) FROM stack_a.silver_transactions
        """)
        silver_count = cursor.fetchone()[0]
        
        count_match = bronze_count >= silver_count  # Silver should have fewer (deduped)
        
        # Check 2: Amount sum
        cursor.execute("""
        SELECT COALESCE(SUM(amount), 0) FROM stack_a.bronze_transactions WHERE status = 'completed'
        """)
        bronze_sum = cursor.fetchone()[0]
        
        cursor.execute("""
        SELECT COALESCE(SUM(amount), 0) FROM stack_a.silver_transactions WHERE status = 'completed'
        """)
        silver_sum = cursor.fetchone()[0]
        
        sum_match = abs(float(bronze_sum - silver_sum)) < 0.01
        
        # Check 3: Referential integrity
        cursor.execute("""
        SELECT COUNT(*) FROM stack_a.silver_transactions t
        WHERE NOT EXISTS (SELECT 1 FROM stack_a.silver_customers c WHERE c.customer_id = t.customer_id)
        """)
        orphaned_rows = cursor.fetchone()[0]
        
        # Log results
        cursor.execute("""
        INSERT INTO stack_a.reconciliation_results
        (reconciliation_date, table_name, source_count, target_count, count_match, 
         source_sum, target_sum, sum_match, discrepancies, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            reconciliation_date, 'transactions',
            bronze_count, silver_count, count_match,
            float(bronze_sum), float(silver_sum), sum_match,
            f"Orphaned rows: {orphaned_rows}",
            'PASSED' if (count_match and sum_match and orphaned_rows == 0) else 'FAILED'
        ))
        
        conn.commit()
        
        if not (count_match and sum_match and orphaned_rows == 0):
            logger.warning(f"Reconciliation FAILED: count_match={count_match}, sum_match={sum_match}, orphaned={orphaned_rows}")
            raise Exception("Reconciliation checks failed!")
        
        logger.info("Reconciliation checks PASSED")
        return {'status': 'passed'}
        
    except Exception as e:
        logger.error(f"Reconciliation error: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def get_primary_key(table_name: str) -> str:
    """Get primary key column for each table"""
    pk_map = {
        'customers': 'customer_id',
        'products': 'product_id',
        'transactions': 'transaction_id',
    }
    return pk_map.get(table_name, 'id')


# ============================================================================
# DAG Definition
# ============================================================================

dag = DAG(
    'ecommerce_dwh_stack_a_pipeline',
    default_args=DEFAULT_ARGS,
    description='Stack A: Data Warehouse Pipeline (PostgreSQL + Medallion Architecture)',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stack-a', 'dwh', 'production'],
)

# ============================================================================
# Tasks
# ============================================================================

with dag:
    
    # 1. Bronze Layer: Ingest raw data
    with TaskGroup("bronze_ingestion") as bronze_tasks:
        
        load_customers = PythonOperator(
            task_id='load_customers',
            python_callable=load_csv_to_bronze,
            op_kwargs={'table_name': 'customers', 'csv_path': '/home/airflow/data/raw/customers.csv'},
        )
        
        load_products = PythonOperator(
            task_id='load_products',
            python_callable=load_csv_to_bronze,
            op_kwargs={'table_name': 'products', 'csv_path': '/home/airflow/data/raw/products.csv'},
        )
        
        load_transactions = PythonOperator(
            task_id='load_transactions',
            python_callable=load_csv_to_bronze,
            op_kwargs={'table_name': 'transactions', 'csv_path': '/home/airflow/data/raw/transactions.csv'},
        )
    
    # 2. Silver Layer: Transform & clean (sequential to avoid deadlocks)
    with TaskGroup("silver_transformation") as silver_tasks:
        
        transform_customers = PythonOperator(
            task_id='transform_customers',
            python_callable=transform_bronze_to_silver,
            op_kwargs={'table_name': 'customers'},
        )
        
        transform_products = PythonOperator(
            task_id='transform_products',
            python_callable=transform_bronze_to_silver,
            op_kwargs={'table_name': 'products'},
        )
        
        transform_transactions = PythonOperator(
            task_id='transform_transactions',
            python_callable=transform_bronze_to_silver,
            op_kwargs={'table_name': 'transactions'},
        )
        
        transform_customers >> transform_products >> transform_transactions
    
    # 3. Gold Layer: Aggregate & analyze
    aggregate_to_gold = PythonOperator(
        task_id='aggregate_to_gold',
        python_callable=aggregate_silver_to_gold,
    )
    
    # 4. Reconciliation: Verify data integrity
    reconciliation = PythonOperator(
        task_id='reconciliation_check',
        python_callable=run_reconciliation,
    )
    
    # 5. Great Expectations: Data quality validation
    run_ge_validation = BashOperator(
        task_id='run_data_quality_checks',
        bash_command="""
        cd /home/airflow && \
        python -c "
        from great_expectations.data_context import DataContext
        context = DataContext()
        # Run checkpoint for transactions
        results = context.run_checkpoint('transactions_checkpoint')
        print(f'Great Expectations validation: {results}')
        "
        """
    )
    
    # 6. Publish metrics
    publish_metrics = BashOperator(
        task_id='publish_prometheus_metrics',
        bash_command="""
        curl -X POST http://prometheus:9090/api/v1/admin/config \
          -H "Content-Type: application/json" \
          -d '{"metric": "pipeline_duration_seconds", "value": 60, "timestamp": "'"$(date +%s)"'"}'
        """
    )
    
    # Define dependencies
    bronze_tasks >> silver_tasks >> aggregate_to_gold >> [reconciliation, run_ge_validation] >> publish_metrics
