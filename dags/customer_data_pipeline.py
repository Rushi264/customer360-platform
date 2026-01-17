"""
Customer 360 Data Pipeline
==========================
This DAG loads customer, product, and transaction data from CSV files
into PostgreSQL staging tables on a daily schedule.

Author: Rushikesh Deshmukh
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Database connection
DATABASE_URL = "postgresql://dataeng:password123@postgres:5432/customer360"

# Default arguments for all tasks
default_args = {
    'owner': 'dataeng',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def load_customers():
    """Load customer data from CSV to PostgreSQL"""
    logger.info("Starting customer data load...")
    
    try:
        engine = create_engine(DATABASE_URL)
        customers = pd.read_csv('/opt/airflow/data/customers.csv')
        
        # Load to staging table
        customers.to_sql(
            'customers', 
            engine, 
            schema='staging', 
            if_exists='replace', 
            index=False
        )
        
        logger.info(f"✅ Successfully loaded {len(customers):,} customers")
        return len(customers)
        
    except Exception as e:
        logger.error(f"❌ Error loading customers: {str(e)}")
        raise


def load_products():
    """Load product data from CSV to PostgreSQL"""
    logger.info("Starting product data load...")
    
    try:
        engine = create_engine(DATABASE_URL)
        products = pd.read_csv('/opt/airflow/data/products.csv')
        
        # Load to staging table
        products.to_sql(
            'products', 
            engine, 
            schema='staging', 
            if_exists='replace', 
            index=False
        )
        
        logger.info(f"✅ Successfully loaded {len(products):,} products")
        return len(products)
        
    except Exception as e:
        logger.error(f"❌ Error loading products: {str(e)}")
        raise


def load_transactions():
    """Load transaction data from CSV to PostgreSQL"""
    logger.info("Starting transaction data load...")
    
    try:
        engine = create_engine(DATABASE_URL)
        transactions = pd.read_csv('/opt/airflow/data/transactions.csv')
        
        # Load to staging table
        transactions.to_sql(
            'transactions', 
            engine, 
            schema='staging', 
            if_exists='replace', 
            index=False
        )
        
        logger.info(f"✅ Successfully loaded {len(transactions):,} transactions")
        return len(transactions)
        
    except Exception as e:
        logger.error(f"❌ Error loading transactions: {str(e)}")
        raise


def validate_data_quality():
    """Validate that data was loaded correctly"""
    logger.info("Validating data quality...")
    
    try:
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Count records in each table
            customer_count = conn.execute(
                text("SELECT COUNT(*) FROM staging.customers")
            ).fetchone()[0]
            
            product_count = conn.execute(
                text("SELECT COUNT(*) FROM staging.products")
            ).fetchone()[0]
            
            transaction_count = conn.execute(
                text("SELECT COUNT(*) FROM staging.transactions")
            ).fetchone()[0]
            
            logger.info(f"📊 Data Quality Check:")
            logger.info(f"   Customers: {customer_count:,} rows")
            logger.info(f"   Products: {product_count:,} rows")
            logger.info(f"   Transactions: {transaction_count:,} rows")
            
            # Validate minimum record counts
            assert customer_count >= 1000, "Too few customers!"
            assert product_count >= 100, "Too few products!"
            assert transaction_count >= 1000, "Too few transactions!"
            
            logger.info("✅ Data quality validation passed!")
            
    except Exception as e:
        logger.error(f"❌ Data quality validation failed: {str(e)}")
        raise


# Define the DAG
with DAG(
    'customer_data_pipeline',
    default_args=default_args,
    description='Load customer, product, and transaction data to staging',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'staging', 'customer360'],
) as dag:

    # Task 1: Load customers
    task_load_customers = PythonOperator(
        task_id='load_customers',
        python_callable=load_customers,
    )

    # Task 2: Load products
    task_load_products = PythonOperator(
        task_id='load_products',
        python_callable=load_products,
    )

    # Task 3: Load transactions
    task_load_transactions = PythonOperator(
        task_id='load_transactions',
        python_callable=load_transactions,
    )
    
    # Task 4: Validate data quality
    task_validate_quality = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality,
    )

    # Define task dependencies
    [task_load_customers, task_load_products] >> task_load_transactions >> task_validate_quality

"""

**Save** (Ctrl+S) and **close Notepad**.

---

## ✅ **STEP 2: Wait for Airflow to Detect the DAG**

Airflow automatically scans the `dags/` folder every **30 seconds**.

**Wait 1 minute**, then:

1. Go to Airflow UI: **http://localhost:8080**
2. **Refresh** the page (F5)
3. Look for **`customer_data_pipeline`** in the DAG list

---

## ✅ **STEP 3: Activate the DAG**

Once you see the DAG:

1. Find the **toggle switch** on the left side of `customer_data_pipeline`
2. **Click** the toggle to turn it **ON** (it should turn blue/green)

---

## ✅ **STEP 4: Trigger the DAG Manually**

1. **Click** on the DAG name `customer_data_pipeline`
2. You'll see the DAG details page
3. **Click** the **"Play" button** (▶️) in the top-right corner
4. **Click** "Trigger DAG"

---

## ✅ **STEP 5: Watch It Execute**

1. Click on **"Graph"** view (top menu)
2. Watch the tasks change colors:
   - ⚪ **White/Grey** = Not started
   - 🟡 **Yellow** = Running
   - 🟢 **Green** = Success
   - 🔴 **Red** = Failed

The workflow should show:
```
    load_customers ─┐
                    ├─> load_transactions -> validate_data_quality
    load_products ──┘

"""