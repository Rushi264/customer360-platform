/*
==============================================================================
LOAD LINKS - Relationships Between Entities
==============================================================================
Links capture relationships: Customer placed Order, Order contains Product
==============================================================================
*/

-- ============================================
-- Load Link: Customer-Order
-- ============================================

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

-- Get count
SELECT 'link_customer_order' AS table_name, COUNT(*) AS row_count FROM raw_vault.link_customer_order;


-- ============================================
-- Load Link: Order-Product
-- ============================================

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

-- Get count
SELECT 'link_order_product' AS table_name, COUNT(*) AS row_count FROM raw_vault.link_order_product;