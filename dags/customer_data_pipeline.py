"""
Customer 360 Data Pipeline - Complete ETL with Data Vault
==========================================================
This DAG orchestrates the complete data pipeline:
1. Load staging tables (customers, products, transactions)
2. Load Data Vault Hubs (business keys)
3. Load Data Vault Satellites (attributes with history)
4. Load Data Vault Links (relationships)
5. Data quality validation

Author: Rushikesh Deshmukh
UPDATED: SQL embedded directly in DAG for reliability
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://dataeng:password123@postgres:5432/customer360"

default_args = {
    'owner': 'dataeng',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# ============================================
# STAGING LOAD FUNCTIONS
# ============================================

def load_customers():
    logger.info("Loading customers to staging...")
    try:
        engine = create_engine(DATABASE_URL)
        customers = pd.read_csv('/opt/airflow/data/customers.csv')
        customers.to_sql('customers', engine, schema='staging', if_exists='replace', index=False)
        logger.info(f"✅ Loaded {len(customers):,} customers")
        return len(customers)
    except Exception as e:
        logger.error(f"❌ Error loading customers: {str(e)}")
        raise


def load_products():
    logger.info("Loading products to staging...")
    try:
        engine = create_engine(DATABASE_URL)
        products = pd.read_csv('/opt/airflow/data/products.csv')
        products.to_sql('products', engine, schema='staging', if_exists='replace', index=False)
        logger.info(f"✅ Loaded {len(products):,} products")
        return len(products)
    except Exception as e:
        logger.error(f"❌ Error loading products: {str(e)}")
        raise


def load_transactions():
    logger.info("Loading transactions to staging...")
    try:
        engine = create_engine(DATABASE_URL)
        transactions = pd.read_csv('/opt/airflow/data/transactions.csv')
        transactions.to_sql('transactions', engine, schema='staging', if_exists='replace', index=False)
        logger.info(f"✅ Loaded {len(transactions):,} transactions")
        return len(transactions)
    except Exception as e:
        logger.error(f"❌ Error loading transactions: {str(e)}")
        raise


# ============================================
# DATA QUALITY VALIDATION
# ============================================

def validate_data_quality():
    logger.info("Validating data quality...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            customer_count = conn.execute(text("SELECT COUNT(*) FROM staging.customers")).fetchone()[0]
            product_count = conn.execute(text("SELECT COUNT(*) FROM staging.products")).fetchone()[0]
            transaction_count = conn.execute(text("SELECT COUNT(*) FROM staging.transactions")).fetchone()[0]

            logger.info(f"📊 Staging Data Quality Check:")
            logger.info(f"   Customers: {customer_count:,} rows")
            logger.info(f"   Products: {product_count:,} rows")
            logger.info(f"   Transactions: {transaction_count:,} rows")

            assert customer_count >= 1000, "Too few customers!"
            assert product_count >= 100, "Too few products!"
            assert transaction_count >= 1000, "Too few transactions!"

            logger.info("✅ Staging data quality validation passed!")
    except Exception as e:
        logger.error(f"❌ Data quality validation failed: {str(e)}")
        raise


def validate_data_vault():
    logger.info("Validating Data Vault...")
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            hub_customer = conn.execute(text("SELECT COUNT(*) FROM raw_vault.hub_customer")).fetchone()[0]
            hub_product = conn.execute(text("SELECT COUNT(*) FROM raw_vault.hub_product")).fetchone()[0]
            hub_order = conn.execute(text("SELECT COUNT(*) FROM raw_vault.hub_order")).fetchone()[0]
            sat_customer = conn.execute(text("SELECT COUNT(*) FROM raw_vault.sat_customer_details")).fetchone()[0]
            link_co = conn.execute(text("SELECT COUNT(*) FROM raw_vault.link_customer_order")).fetchone()[0]

            logger.info(f"📊 Data Vault Quality Check:")
            logger.info(f"   hub_customer: {hub_customer:,} rows")
            logger.info(f"   hub_product: {hub_product:,} rows")
            logger.info(f"   hub_order: {hub_order:,} rows")
            logger.info(f"   sat_customer_details: {sat_customer:,} rows")
            logger.info(f"   link_customer_order: {link_co:,} rows")

            assert hub_customer > 0, "No customers in Data Vault!"
            assert sat_customer > 0, "No customer details in Data Vault!"

            logger.info("✅ Data Vault validation passed!")
    except Exception as e:
        logger.error(f"❌ Data Vault validation failed: {str(e)}")
        raise


# ============================================
# SQL SCRIPTS EMBEDDED DIRECTLY
# ============================================

LOAD_HUBS_SQL = """
-- Load Hub Customer
INSERT INTO raw_vault.hub_customer (customer_hash_key, customer_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(customer_id) AS customer_hash_key,
    customer_id,
    CURRENT_TIMESTAMP AS load_date,
    'staging.customers' AS record_source
FROM staging.customers
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_hash_key) DO NOTHING;

-- Load Hub Product
INSERT INTO raw_vault.hub_product (product_hash_key, product_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(product_id) AS product_hash_key,
    product_id,
    CURRENT_TIMESTAMP AS load_date,
    'staging.products' AS record_source
FROM staging.products
WHERE product_id IS NOT NULL
ON CONFLICT (product_hash_key) DO NOTHING;

-- Load Hub Order
INSERT INTO raw_vault.hub_order (order_hash_key, transaction_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(transaction_id) AS order_hash_key,
    transaction_id,
    CURRENT_TIMESTAMP AS load_date,
    'staging.transactions' AS record_source
FROM staging.transactions
WHERE transaction_id IS NOT NULL
ON CONFLICT (order_hash_key) DO NOTHING;
"""

LOAD_SATELLITES_SQL = """
-- Load Satellite: Customer Details
UPDATE raw_vault.sat_customer_details sat
SET
    load_end_date = CURRENT_TIMESTAMP,
    is_current = FALSE
WHERE sat.customer_hash_key IN (
    SELECT DISTINCT generate_hash_key(s.customer_id)
    FROM staging.customers s
)
AND sat.is_current = TRUE
AND sat.hash_diff NOT IN (
    SELECT generate_hash_key(
        COALESCE(s.first_name, '') || '|' ||
        COALESCE(s.last_name, '') || '|' ||
        COALESCE(s.email, '') || '|' ||
        COALESCE(s.phone, '') || '|' ||
        COALESCE(s.customer_segment, '')
    )
    FROM staging.customers s
    WHERE generate_hash_key(s.customer_id) = sat.customer_hash_key
);

INSERT INTO raw_vault.sat_customer_details (
    customer_hash_key, load_date, first_name, last_name, email, phone,
    signup_date, country, state, city, customer_segment, hash_diff, record_source
)
SELECT
    generate_hash_key(customer_id) AS customer_hash_key,
    CURRENT_TIMESTAMP AS load_date,
    first_name,
    last_name,
    email,
    phone,
    signup_date::DATE,
    country,
    state,
    city,
    customer_segment,
    generate_hash_key(
        COALESCE(first_name, '') || '|' ||
        COALESCE(last_name, '') || '|' ||
        COALESCE(email, '') || '|' ||
        COALESCE(phone, '') || '|' ||
        COALESCE(customer_segment, '')
    ) AS hash_diff,
    'staging.customers' AS record_source
FROM staging.customers
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_hash_key, load_date) DO NOTHING;

-- Load Satellite: Product Details
INSERT INTO raw_vault.sat_product_details (
    product_hash_key, load_date, product_name, category, price, cost, supplier, hash_diff, record_source
)
SELECT
    generate_hash_key(product_id) AS product_hash_key,
    CURRENT_TIMESTAMP AS load_date,
    product_name,
    category,
    price,
    cost,
    supplier,
    generate_hash_key(
        COALESCE(product_name, '') || '|' ||
        COALESCE(category, '') || '|' ||
        COALESCE(price::TEXT, '') || '|' ||
        COALESCE(supplier, '')
    ) AS hash_diff,
    'staging.products' AS record_source
FROM staging.products
WHERE product_id IS NOT NULL
ON CONFLICT (product_hash_key, load_date) DO NOTHING;

-- Load Satellite: Order Details
INSERT INTO raw_vault.sat_order_details (
    order_hash_key, load_date, order_date, quantity, unit_price,
    total_amount, payment_method, order_status, hash_diff, record_source
)
SELECT
    generate_hash_key(transaction_id) AS order_hash_key,
    CURRENT_TIMESTAMP AS load_date,
    order_date::TIMESTAMP,
    quantity,
    unit_price,
    total_amount,
    payment_method,
    order_status,
    generate_hash_key(
        COALESCE(order_date::TEXT, '') || '|' ||
        COALESCE(quantity::TEXT, '') || '|' ||
        COALESCE(total_amount::TEXT, '') || '|' ||
        COALESCE(payment_method, '')
    ) AS hash_diff,
    'staging.transactions' AS record_source
FROM staging.transactions
WHERE transaction_id IS NOT NULL
ON CONFLICT (order_hash_key, load_date) DO NOTHING;
"""

LOAD_LINKS_SQL = """
-- Load Link: Customer-Order
INSERT INTO raw_vault.link_customer_order (link_hash_key, customer_hash_key, order_hash_key, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(customer_id || '|' || transaction_id) AS link_hash_key,
    generate_hash_key(customer_id) AS customer_hash_key,
    generate_hash_key(transaction_id) AS order_hash_key,
    CURRENT_TIMESTAMP AS load_date,
    'staging.transactions' AS record_source
FROM staging.transactions
WHERE customer_id IS NOT NULL AND transaction_id IS NOT NULL
ON CONFLICT (link_hash_key) DO NOTHING;

-- Load Link: Order-Product
INSERT INTO raw_vault.link_order_product (link_hash_key, order_hash_key, product_hash_key, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(transaction_id || '|' || product_id) AS link_hash_key,
    generate_hash_key(transaction_id) AS order_hash_key,
    generate_hash_key(product_id) AS product_hash_key,
    CURRENT_TIMESTAMP AS load_date,
    'staging.transactions' AS record_source
FROM staging.transactions
WHERE transaction_id IS NOT NULL AND product_id IS NOT NULL
ON CONFLICT (link_hash_key) DO NOTHING;
"""


# ============================================
# DEFINE THE DAG
# ============================================

with DAG(
    'customer_data_pipeline_complete',
    default_args=default_args,
    description='Complete ETL: Staging → Data Vault with quality checks',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'data-vault', 'customer360'],
) as dag:

    # ============================================
    # STAGE 1: Load Staging Tables
    # ============================================

    task_load_customers = PythonOperator(
        task_id='load_staging_customers',
        python_callable=load_customers,
    )

    task_load_products = PythonOperator(
        task_id='load_staging_products',
        python_callable=load_products,
    )

    task_load_transactions = PythonOperator(
        task_id='load_staging_transactions',
        python_callable=load_transactions,
    )

    task_validate_staging = PythonOperator(
        task_id='validate_staging_quality',
        python_callable=validate_data_quality,
    )

    # ============================================
    # STAGE 2: Load Data Vault Hubs
    # ============================================

    task_load_hubs = PostgresOperator(
        task_id='load_data_vault_hubs',
        postgres_conn_id='postgres_default',
        sql=LOAD_HUBS_SQL,
    )

    # ============================================
    # STAGE 3: Load Data Vault Satellites
    # ============================================

    task_load_satellites = PostgresOperator(
        task_id='load_data_vault_satellites',
        postgres_conn_id='postgres_default',
        sql=LOAD_SATELLITES_SQL,
    )

    # ============================================
    # STAGE 4: Load Data Vault Links
    # ============================================

    task_load_links = PostgresOperator(
        task_id='load_data_vault_links',
        postgres_conn_id='postgres_default',
        sql=LOAD_LINKS_SQL,
    )

    # ============================================
    # STAGE 5: Final Validation
    # ============================================

    task_validate_vault = PythonOperator(
        task_id='validate_data_vault_quality',
        python_callable=validate_data_vault,
    )

    # ============================================
    # DEFINE WORKFLOW
    # ============================================

    # Stage 1: Load staging (parallel)
    [task_load_customers, task_load_products] >> task_load_transactions >> task_validate_staging

    # Stage 2: Load Data Vault (sequential)
    task_validate_staging >> task_load_hubs >> task_load_satellites >> task_load_links

    # Stage 3: Final validation
    task_load_links >> task_validate_vault