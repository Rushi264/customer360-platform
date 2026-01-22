"""
Customer 360 Data Pipeline - ENHANCED
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'customer_data_pipeline_enhanced',
    default_args=default_args,
    description='Production-Grade ETL with quality checks',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'production'],
) as dag:

    def load_customers_with_logging():
        logger.info("=" * 50)
        logger.info("TASK: Load Customers")
        logger.info("=" * 50)
        try:
            engine = create_engine(DATABASE_URL)
            customers = pd.read_csv('/opt/airflow/data/customers.csv')
            logger.info(f"Read {len(customers):,} customer records")
            customers.to_sql('customers', engine, schema='staging', if_exists='replace', index=False)
            logger.info(f"Loaded {len(customers):,} customers to staging")
            return len(customers)
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            raise

    def load_products_with_logging():
        logger.info("=" * 50)
        logger.info("TASK: Load Products")
        logger.info("=" * 50)
        try:
            engine = create_engine(DATABASE_URL)
            products = pd.read_csv('/opt/airflow/data/products.csv')
            logger.info(f"Read {len(products):,} product records")
            products.to_sql('products', engine, schema='staging', if_exists='replace', index=False)
            logger.info(f"Loaded {len(products):,} products to staging")
            return len(products)
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            raise

    def load_transactions_with_logging():
        logger.info("=" * 50)
        logger.info("TASK: Load Transactions")
        logger.info("=" * 50)
        try:
            engine = create_engine(DATABASE_URL)
            transactions = pd.read_csv('/opt/airflow/data/transactions.csv')
            logger.info(f"Read {len(transactions):,} transaction records")
            transactions.to_sql('transactions', engine, schema='staging', if_exists='replace', index=False)
            logger.info(f"Loaded {len(transactions):,} transactions to staging")
            return len(transactions)
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            raise

    def quality_check_staging():
        logger.info("=" * 50)
        logger.info("QUALITY CHECK: Staging Data")
        logger.info("=" * 50)
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect() as conn:
                customer_count = conn.execute(text("SELECT COUNT(*) FROM staging.customers")).fetchone()[0]
                product_count = conn.execute(text("SELECT COUNT(*) FROM staging.products")).fetchone()[0]
                transaction_count = conn.execute(text("SELECT COUNT(*) FROM staging.transactions")).fetchone()[0]

                logger.info(f"Customers: {customer_count:,}")
                logger.info(f"Products: {product_count:,}")
                logger.info(f"Transactions: {transaction_count:,}")

                if customer_count < 1000:
                    raise ValueError("Too few customers!")
                if product_count < 100:
                    raise ValueError("Too few products!")
                if transaction_count < 1000:
                    raise ValueError("Too few transactions!")

                logger.info("ALL QUALITY CHECKS PASSED!")
                return True
        except Exception as e:
            logger.error(f"Quality check failed: {str(e)}")
            raise

    def quality_check_vault():
        logger.info("=" * 50)
        logger.info("QUALITY CHECK: Data Vault")
        logger.info("=" * 50)
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect() as conn:
                hub_customer = conn.execute(text("SELECT COUNT(*) FROM raw_vault.hub_customer")).fetchone()[0]
                hub_product = conn.execute(text("SELECT COUNT(*) FROM raw_vault.hub_product")).fetchone()[0]
                hub_order = conn.execute(text("SELECT COUNT(*) FROM raw_vault.hub_order")).fetchone()[0]

                logger.info(f"Hub Customer: {hub_customer:,}")
                logger.info(f"Hub Product: {hub_product:,}")
                logger.info(f"Hub Order: {hub_order:,}")

                assert hub_customer > 0, "No customers!"
                assert hub_product > 0, "No products!"
                assert hub_order > 0, "No orders!"

                logger.info("DATA VAULT VALIDATION PASSED!")
                return True
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            raise

    # Tasks
    task_load_customers = PythonOperator(
        task_id='load_staging_customers',
        python_callable=load_customers_with_logging,
    )

    task_load_products = PythonOperator(
        task_id='load_staging_products',
        python_callable=load_products_with_logging,
    )

    task_load_transactions = PythonOperator(
        task_id='load_staging_transactions',
        python_callable=load_transactions_with_logging,
    )

    task_quality_staging = PythonOperator(
        task_id='quality_check_staging',
        python_callable=quality_check_staging,
    )

    task_load_hubs = PostgresOperator(
        task_id='load_data_vault_hubs',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO raw_vault.hub_customer (customer_hash_key, customer_id, load_date, record_source)
            SELECT DISTINCT generate_hash_key(customer_id), customer_id, CURRENT_TIMESTAMP, 'staging.customers'
            FROM staging.customers WHERE customer_id IS NOT NULL ON CONFLICT (customer_hash_key) DO NOTHING;

            INSERT INTO raw_vault.hub_product (product_hash_key, product_id, load_date, record_source)
            SELECT DISTINCT generate_hash_key(product_id), product_id, CURRENT_TIMESTAMP, 'staging.products'
            FROM staging.products WHERE product_id IS NOT NULL ON CONFLICT (product_hash_key) DO NOTHING;

            INSERT INTO raw_vault.hub_order (order_hash_key, transaction_id, load_date, record_source)
            SELECT DISTINCT generate_hash_key(transaction_id), transaction_id, CURRENT_TIMESTAMP, 'staging.transactions'
            FROM staging.transactions WHERE transaction_id IS NOT NULL ON CONFLICT (order_hash_key) DO NOTHING;
        """,
    )

    task_load_satellites = PostgresOperator(
        task_id='load_data_vault_satellites',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO raw_vault.sat_customer_details (customer_hash_key, load_date, first_name, last_name, email, phone, signup_date, country, state, city, customer_segment, hash_diff, record_source)
            SELECT generate_hash_key(customer_id), CURRENT_TIMESTAMP, first_name, last_name, email, phone, signup_date::DATE, country, state, city, customer_segment,
                   generate_hash_key(COALESCE(first_name, '') || '|' || COALESCE(last_name, '') || '|' || COALESCE(email, '') || '|' || COALESCE(phone, '') || '|' || COALESCE(customer_segment, '')),
                   'staging.customers'
            FROM staging.customers WHERE customer_id IS NOT NULL ON CONFLICT (customer_hash_key, load_date) DO NOTHING;

            INSERT INTO raw_vault.sat_product_details (product_hash_key, load_date, product_name, category, price, cost, supplier, hash_diff, record_source)
            SELECT generate_hash_key(product_id), CURRENT_TIMESTAMP, product_name, category, price, cost, supplier,
                   generate_hash_key(COALESCE(product_name, '') || '|' || COALESCE(category, '') || '|' || COALESCE(price::TEXT, '') || '|' || COALESCE(supplier, '')),
                   'staging.products'
            FROM staging.products WHERE product_id IS NOT NULL ON CONFLICT (product_hash_key, load_date) DO NOTHING;

            INSERT INTO raw_vault.sat_order_details (order_hash_key, load_date, order_date, quantity, unit_price, total_amount, payment_method, order_status, hash_diff, record_source)
            SELECT generate_hash_key(transaction_id), CURRENT_TIMESTAMP, order_date::TIMESTAMP, quantity, unit_price, total_amount, payment_method, order_status,
                   generate_hash_key(COALESCE(order_date::TEXT, '') || '|' || COALESCE(quantity::TEXT, '') || '|' || COALESCE(total_amount::TEXT, '') || '|' || COALESCE(payment_method, '')),
                   'staging.transactions'
            FROM staging.transactions WHERE transaction_id IS NOT NULL ON CONFLICT (order_hash_key, load_date) DO NOTHING;
        """,
    )

    task_load_links = PostgresOperator(
        task_id='load_data_vault_links',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO raw_vault.link_customer_order (link_hash_key, customer_hash_key, order_hash_key, load_date, record_source)
            SELECT DISTINCT generate_hash_key(customer_id || '|' || transaction_id), generate_hash_key(customer_id), generate_hash_key(transaction_id), CURRENT_TIMESTAMP, 'staging.transactions'
            FROM staging.transactions WHERE customer_id IS NOT NULL AND transaction_id IS NOT NULL ON CONFLICT (link_hash_key) DO NOTHING;

            INSERT INTO raw_vault.link_order_product (link_hash_key, order_hash_key, product_hash_key, load_date, record_source)
            SELECT DISTINCT generate_hash_key(transaction_id || '|' || product_id), generate_hash_key(transaction_id), generate_hash_key(product_id), CURRENT_TIMESTAMP, 'staging.transactions'
            FROM staging.transactions WHERE transaction_id IS NOT NULL AND product_id IS NOT NULL ON CONFLICT (link_hash_key) DO NOTHING;
        """,
    )

    task_quality_vault = PythonOperator(
        task_id='quality_check_vault',
        python_callable=quality_check_vault,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dependencies
    [task_load_customers, task_load_products] >> task_load_transactions >> task_quality_staging
    task_quality_staging >> task_load_hubs >> task_load_satellites >> task_load_links >> task_quality_vault
