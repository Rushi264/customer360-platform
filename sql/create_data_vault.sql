/*
==============================================================================
DATA VAULT 2.0 MODEL - Customer 360 Platform
==============================================================================
This script creates the Data Vault structure:
- Hubs: Business keys (customers, products, orders)
- Links: Relationships between entities
- Satellites: Descriptive attributes with full history
==============================================================================
*/

-- ============================================
-- HUBS: Business Keys Only
-- ============================================

-- Hub: Customer (Unique customers)
CREATE TABLE IF NOT EXISTS raw_vault.hub_customer (
    customer_hash_key CHAR(64) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.customers'
);

CREATE INDEX idx_hub_customer_id ON raw_vault.hub_customer(customer_id);

-- Hub: Product (Unique products)
CREATE TABLE IF NOT EXISTS raw_vault.hub_product (
    product_hash_key CHAR(64) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.products'
);

CREATE INDEX idx_hub_product_id ON raw_vault.hub_product(product_id);

-- Hub: Order (Unique orders/transactions)
CREATE TABLE IF NOT EXISTS raw_vault.hub_order (
    order_hash_key CHAR(64) PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.transactions'
);

CREATE INDEX idx_hub_order_id ON raw_vault.hub_order(transaction_id);


-- ============================================
-- LINKS: Relationships Between Entities
-- ============================================

-- Link: Customer placed Order
CREATE TABLE IF NOT EXISTS raw_vault.link_customer_order (
    link_hash_key CHAR(64) PRIMARY KEY,
    customer_hash_key CHAR(64) NOT NULL,
    order_hash_key CHAR(64) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.transactions',
    FOREIGN KEY (customer_hash_key) REFERENCES raw_vault.hub_customer(customer_hash_key),
    FOREIGN KEY (order_hash_key) REFERENCES raw_vault.hub_order(order_hash_key)
);

CREATE INDEX idx_link_co_customer ON raw_vault.link_customer_order(customer_hash_key);
CREATE INDEX idx_link_co_order ON raw_vault.link_customer_order(order_hash_key);

-- Link: Order contains Product
CREATE TABLE IF NOT EXISTS raw_vault.link_order_product (
    link_hash_key CHAR(64) PRIMARY KEY,
    order_hash_key CHAR(64) NOT NULL,
    product_hash_key CHAR(64) NOT NULL,
    load_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.transactions',
    FOREIGN KEY (order_hash_key) REFERENCES raw_vault.hub_order(order_hash_key),
    FOREIGN KEY (product_hash_key) REFERENCES raw_vault.hub_product(product_hash_key)
);

CREATE INDEX idx_link_op_order ON raw_vault.link_order_product(order_hash_key);
CREATE INDEX idx_link_op_product ON raw_vault.link_order_product(product_hash_key);


-- ============================================
-- SATELLITES: Descriptive Attributes (History)
-- ============================================

-- Satellite: Customer Details (with history)
CREATE TABLE IF NOT EXISTS raw_vault.sat_customer_details (
    customer_hash_key CHAR(64) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    phone VARCHAR(50),
    signup_date DATE,
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    customer_segment VARCHAR(50),
    hash_diff CHAR(64) NOT NULL,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.customers',
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (customer_hash_key, load_date),
    FOREIGN KEY (customer_hash_key) REFERENCES raw_vault.hub_customer(customer_hash_key)
);

CREATE INDEX idx_sat_customer_hash ON raw_vault.sat_customer_details(customer_hash_key);
CREATE INDEX idx_sat_customer_current ON raw_vault.sat_customer_details(customer_hash_key, is_current);

-- Satellite: Product Details (with history)
CREATE TABLE IF NOT EXISTS raw_vault.sat_product_details (
    product_hash_key CHAR(64) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    supplier VARCHAR(200),
    hash_diff CHAR(64) NOT NULL,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.products',
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (product_hash_key, load_date),
    FOREIGN KEY (product_hash_key) REFERENCES raw_vault.hub_product(product_hash_key)
);

CREATE INDEX idx_sat_product_hash ON raw_vault.sat_product_details(product_hash_key);
CREATE INDEX idx_sat_product_current ON raw_vault.sat_product_details(product_hash_key, is_current);

-- Satellite: Order Details (with history)
CREATE TABLE IF NOT EXISTS raw_vault.sat_order_details (
    order_hash_key CHAR(64) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    load_end_date TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    order_date TIMESTAMP,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    hash_diff CHAR(64) NOT NULL,
    record_source VARCHAR(100) NOT NULL DEFAULT 'staging.transactions',
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (order_hash_key, load_date),
    FOREIGN KEY (order_hash_key) REFERENCES raw_vault.hub_order(order_hash_key)
);

CREATE INDEX idx_sat_order_hash ON raw_vault.sat_order_details(order_hash_key);
CREATE INDEX idx_sat_order_current ON raw_vault.sat_order_details(order_hash_key, is_current);


-- ============================================
-- HELPER FUNCTION: Generate SHA256 Hash Keys
-- ============================================

CREATE OR REPLACE FUNCTION generate_hash_key(input_text TEXT)
RETURNS CHAR(64) AS $$
BEGIN
    RETURN ENCODE(DIGEST(input_text, 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ============================================
-- SUCCESS MESSAGE
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Data Vault 2.0 schema created successfully!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Created:';
    RAISE NOTICE '  - 3 Hub tables (customer, product, order)';
    RAISE NOTICE '  - 2 Link tables (customer_order, order_product)';
    RAISE NOTICE '  - 3 Satellite tables (customer_details, product_details, order_details)';
    RAISE NOTICE '  - Hash key generation function';
    RAISE NOTICE '========================================';
END $$;