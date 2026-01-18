/*
LOAD SATELLITES - Descriptive Attributes with History
*/

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

SELECT 'sat_customer_details' AS table_name, COUNT(*) AS row_count FROM raw_vault.sat_customer_details;


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

SELECT 'sat_product_details' AS table_name, COUNT(*) AS row_count FROM raw_vault.sat_product_details;


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

SELECT 'sat_order_details' AS table_name, COUNT(*) AS row_count FROM raw_vault.sat_order_details;