-- Customer 360 Platform - Database Initialization

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS raw_vault;
CREATE SCHEMA IF NOT EXISTS business_vault;
CREATE SCHEMA IF NOT EXISTS metadata;

-- Staging: Customers
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(50),
    signup_date DATE,
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    customer_segment VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging: Products
CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    supplier VARCHAR(200),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging: Transactions
CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date TIMESTAMP,
    product_id VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metadata: ETL Configuration
CREATE TABLE IF NOT EXISTS metadata.etl_config (
    config_id SERIAL PRIMARY KEY,
    source_name VARCHAR(100),
    target_table VARCHAR(100),
    load_type VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample metadata
INSERT INTO metadata.etl_config (source_name, target_table, load_type)
VALUES 
    ('customers.csv', 'staging.customers', 'FULL'),
    ('products.csv', 'staging.products', 'FULL'),
    ('transactions.csv', 'staging.transactions', 'INCREMENTAL')
ON CONFLICT DO NOTHING;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'Database initialization complete!';
END $$;