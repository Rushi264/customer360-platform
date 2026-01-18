/*
==============================================================================
LOAD HUBS - Business Keys from Staging to Data Vault
==============================================================================
This loads unique business keys (customers, products, orders) into Hub tables.
Hubs only store the business key, load date, and source - no attributes.
==============================================================================
*/

-- ============================================
-- Load Hub Customer
-- ============================================

INSERT INTO raw_vault.hub_customer (customer_hash_key, customer_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(customer_id) AS customer_hash_key,
    customer_id,
    CURRENT_TIMESTAMP AS load_date,
    'staging.customers' AS record_source
FROM staging.customers
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_hash_key) DO NOTHING;

-- Get count
SELECT 'hub_customer' AS table_name, COUNT(*) AS row_count FROM raw_vault.hub_customer;


-- ============================================
-- Load Hub Product
-- ============================================

INSERT INTO raw_vault.hub_product (product_hash_key, product_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(product_id) AS product_hash_key,
    product_id,
    CURRENT_TIMESTAMP AS load_date,
    'staging.products' AS record_source
FROM staging.products
WHERE product_id IS NOT NULL
ON CONFLICT (product_hash_key) DO NOTHING;

-- Get count
SELECT 'hub_product' AS table_name, COUNT(*) AS row_count FROM raw_vault.hub_product;


-- ============================================
-- Load Hub Order
-- ============================================

INSERT INTO raw_vault.hub_order (order_hash_key, transaction_id, load_date, record_source)
SELECT DISTINCT
    generate_hash_key(transaction_id) AS order_hash_key,
    transaction_id,
    CURRENT_TIMESTAMP AS load_date,
    'staging.transactions' AS record_source
FROM staging.transactions
WHERE transaction_id IS NOT NULL
ON CONFLICT (order_hash_key) DO NOTHING;

-- Get count
SELECT 'hub_order' AS table_name, COUNT(*) AS row_count FROM raw_vault.hub_order;