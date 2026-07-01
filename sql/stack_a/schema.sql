-- ============================================================================
-- STACK A: Data Warehouse (PostgreSQL)
-- Medallion Architecture: Bronze → Silver → Gold
-- ============================================================================

-- Create schema for stack A
CREATE SCHEMA IF NOT EXISTS stack_a;

-- ============================================================================
-- BRONZE LAYER: Raw Data Ingestion (Immutable Log)
-- ============================================================================

CREATE TABLE IF NOT EXISTS stack_a.bronze_customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(10),
    created_date DATE,
    is_active BOOLEAN,
    -- Lineage & Audit
    _ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(50) DEFAULT 'ecommerce_api',
    _load_uuid UUID DEFAULT gen_random_uuid()
);

CREATE INDEX idx_bronze_customers_ingestion ON stack_a.bronze_customers(_ingestion_date);

---

CREATE TABLE IF NOT EXISTS stack_a.bronze_products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price NUMERIC(10, 2),
    stock_quantity INT,
    is_active BOOLEAN,
    -- Lineage & Audit
    _ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(50) DEFAULT 'product_catalog_api',
    _load_uuid UUID DEFAULT gen_random_uuid()
);

CREATE INDEX idx_bronze_products_ingestion ON stack_a.bronze_products(_ingestion_date);

---

CREATE TABLE IF NOT EXISTS stack_a.bronze_transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    transaction_date TIMESTAMP,
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    unit_price NUMERIC(10, 2),
    amount NUMERIC(10, 2),
    payment_method VARCHAR(50),
    status VARCHAR(50),
    store_location VARCHAR(100),
    -- Lineage & Audit
    _ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_system VARCHAR(50) DEFAULT 'pos_system',
    _load_uuid UUID DEFAULT gen_random_uuid(),
    _hash_row VARCHAR(64)  -- For deduplication
);

CREATE INDEX idx_bronze_transactions_date ON stack_a.bronze_transactions(transaction_date);
CREATE INDEX idx_bronze_transactions_ingestion ON stack_a.bronze_transactions(_ingestion_date);

---

-- ============================================================================
-- SILVER LAYER: Cleaned, Validated, Deduplicated Data
-- ============================================================================

CREATE TABLE IF NOT EXISTS stack_a.silver_customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email_masked VARCHAR(255),  -- PII: Email hashed
    phone_masked VARCHAR(255),   -- PII: Phone hashed
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(10),
    created_date DATE NOT NULL,
    is_active BOOLEAN NOT NULL,
    -- Data Quality Flags
    dq_is_valid BOOLEAN DEFAULT TRUE,
    dq_validation_errors TEXT,
    -- Lineage
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _bronze_load_uuid UUID
);

CREATE INDEX idx_silver_customers_active ON stack_a.silver_customers(is_active);
CREATE INDEX idx_silver_customers_state ON stack_a.silver_customers(state);

---

CREATE TABLE IF NOT EXISTS stack_a.silver_products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price > 0),
    stock_quantity INT NOT NULL CHECK (stock_quantity >= 0),
    is_active BOOLEAN NOT NULL,
    -- Data Quality Flags
    dq_is_valid BOOLEAN DEFAULT TRUE,
    dq_validation_errors TEXT,
    -- Lineage
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _bronze_load_uuid UUID
);

CREATE INDEX idx_silver_products_category ON stack_a.silver_products(category);
CREATE INDEX idx_silver_products_active ON stack_a.silver_products(is_active);

---

CREATE TABLE IF NOT EXISTS stack_a.silver_transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    transaction_date TIMESTAMP NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0),
    amount NUMERIC(10, 2) NOT NULL CHECK (amount > 0),
    payment_method VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    store_location VARCHAR(100),
    -- Data Quality Flags
    dq_is_valid BOOLEAN DEFAULT TRUE,
    dq_validation_errors TEXT,
    dq_duplicate_found BOOLEAN DEFAULT FALSE,
    -- Lineage
    _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _bronze_load_uuid UUID,
    FOREIGN KEY (customer_id) REFERENCES stack_a.silver_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES stack_a.silver_products(product_id)
);

CREATE INDEX idx_silver_transactions_date ON stack_a.silver_transactions(transaction_date);
CREATE INDEX idx_silver_transactions_customer ON stack_a.silver_transactions(customer_id);
CREATE INDEX idx_silver_transactions_product ON stack_a.silver_transactions(product_id);
CREATE INDEX idx_silver_transactions_status ON stack_a.silver_transactions(status);

---

-- ============================================================================
-- GOLD LAYER: Analytics-Ready, Aggregated Data
-- ============================================================================

-- Fact table: Daily Transaction Summary
CREATE TABLE IF NOT EXISTS stack_a.gold_daily_sales_fact (
    fact_id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    category VARCHAR(100),
    store_location VARCHAR(100),
    quantity INT NOT NULL,
    gross_amount NUMERIC(12, 2) NOT NULL,
    net_amount NUMERIC(12, 2) NOT NULL,
    transaction_count INT NOT NULL,
    completed_count INT NOT NULL,
    refunded_count INT NOT NULL,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gold_sales_date ON stack_a.gold_daily_sales_fact(transaction_date);
CREATE INDEX idx_gold_sales_customer ON stack_a.gold_daily_sales_fact(customer_id);
CREATE INDEX idx_gold_sales_product ON stack_a.gold_daily_sales_fact(product_id);
CREATE INDEX idx_gold_sales_category ON stack_a.gold_daily_sales_fact(category);

---

-- Dimension table: Customer Metrics
CREATE TABLE IF NOT EXISTS stack_a.gold_customer_metrics (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_lifetime_value NUMERIC(12, 2),
    total_transactions INT,
    first_purchase_date DATE,
    last_purchase_date DATE,
    avg_transaction_amount NUMERIC(10, 2),
    preferred_category VARCHAR(100),
    preferred_payment_method VARCHAR(50),
    is_active BOOLEAN,
    risk_score NUMERIC(3, 2),  -- 0.0 to 1.0 (fraud detection)
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gold_customer_metrics_value ON stack_a.gold_customer_metrics(customer_lifetime_value DESC);
CREATE INDEX idx_gold_customer_metrics_risk ON stack_a.gold_customer_metrics(risk_score DESC);

---

-- Dimension table: Product Performance
CREATE TABLE IF NOT EXISTS stack_a.gold_product_metrics (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    total_quantity_sold INT,
    total_revenue NUMERIC(12, 2),
    avg_rating NUMERIC(3, 2),
    days_in_inventory INT,
    inventory_turnover_ratio NUMERIC(6, 2),
    is_profitable BOOLEAN,
    _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_gold_product_metrics_revenue ON stack_a.gold_product_metrics(total_revenue DESC);
CREATE INDEX idx_gold_product_metrics_category ON stack_a.gold_product_metrics(category);

---

-- Time series: Sales by Category Over Time
CREATE TABLE IF NOT EXISTS stack_a.gold_category_trends (
    trend_date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    daily_sales NUMERIC(12, 2),
    daily_quantity INT,
    moving_avg_7_days NUMERIC(12, 2),
    yoy_growth NUMERIC(5, 2),  -- Year-over-year growth %
    PRIMARY KEY (trend_date, category)
);

CREATE INDEX idx_gold_category_trends_date ON stack_a.gold_category_trends(trend_date DESC);

---

-- ============================================================================
-- RECONCILIATION & AUDIT TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS stack_a.audit_load_history (
    load_id SERIAL PRIMARY KEY,
    load_name VARCHAR(255) NOT NULL,
    load_start_time TIMESTAMP NOT NULL,
    load_end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,  -- 'RUNNING', 'SUCCESS', 'FAILED'
    rows_inserted INT DEFAULT 0,
    rows_updated INT DEFAULT 0,
    rows_failed INT DEFAULT 0,
    error_message TEXT,
    dbt_test_passed BOOLEAN,
    dbt_test_results TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

---

CREATE TABLE IF NOT EXISTS stack_a.reconciliation_results (
    reconciliation_id SERIAL PRIMARY KEY,
    reconciliation_date DATE NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    source_count BIGINT,
    target_count BIGINT,
    count_match BOOLEAN,
    source_sum NUMERIC(15, 2),
    target_sum NUMERIC(15, 2),
    sum_match BOOLEAN,
    discrepancies TEXT,
    status VARCHAR(50),  -- 'PASSED', 'FAILED', 'INVESTIGATING'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

---

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Function to mask PII (email)
CREATE OR REPLACE FUNCTION mask_email(email VARCHAR) RETURNS VARCHAR AS $$
BEGIN
    IF email IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN 'SHA256:' || encode(digest(email::bytea, 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to mask PII (phone)
CREATE OR REPLACE FUNCTION mask_phone(phone VARCHAR) RETURNS VARCHAR AS $$
BEGIN
    IF phone IS NULL THEN
        RETURN NULL;
    END IF;
    RETURN 'SHA256:' || encode(digest(phone::bytea, 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to compute row hash (for deduplication)
CREATE OR REPLACE FUNCTION compute_row_hash(
    p_transaction_id VARCHAR,
    p_customer_id VARCHAR,
    p_product_id VARCHAR,
    p_transaction_date TIMESTAMP,
    p_amount NUMERIC
) RETURNS VARCHAR AS $$
BEGIN
    RETURN encode(
        digest(
            CONCAT(p_transaction_id, '|', p_customer_id, '|', p_product_id, '|', p_transaction_date, '|', p_amount),
            'sha256'
        ),
        'hex'
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

---

-- ============================================================================
-- GRANTS & PERMISSIONS (Security)
-- ============================================================================

-- Create service account for Airflow (IF NOT NOT EXISTS for PG < 16 compat)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow_user') THEN
    CREATE ROLE airflow_user WITH LOGIN PASSWORD 'airflow_secure_password';
  END IF;
END
$$;
GRANT USAGE ON SCHEMA stack_a TO airflow_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stack_a TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA stack_a TO airflow_user;

-- Create read-only role for analytics
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'analytics_reader') THEN
    CREATE ROLE analytics_reader;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA stack_a TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA stack_a TO analytics_reader;

-- Create analyst user
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'analyst') THEN
    CREATE ROLE analyst WITH LOGIN PASSWORD 'analyst_password';
  END IF;
END
$$;
GRANT analytics_reader TO analyst;

-- Notify completion
DO $$ BEGIN RAISE NOTICE 'Stack A (DWH) Schema created successfully!'; END $$;
