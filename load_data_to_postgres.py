import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
DATABASE_URL = "postgresql://dataeng:password123@localhost:5432/customer360"
engine = create_engine(DATABASE_URL)

print("=" * 60)
print("LOADING DATA INTO POSTGRESQL")
print("=" * 60)

# Load customers
print("\n1. Loading customers...")
customers = pd.read_csv('data/customers.csv')
customers.to_sql('customers', engine, schema='staging', if_exists='replace', index=False)
print(f"   ✅ Loaded {len(customers):,} customers into staging.customers")

# Load products
print("\n2. Loading products...")
products = pd.read_csv('data/products.csv')
products.to_sql('products', engine, schema='staging', if_exists='replace', index=False)
print(f"   ✅ Loaded {len(products):,} products into staging.products")

# Load transactions
print("\n3. Loading transactions...")
transactions = pd.read_csv('data/transactions.csv')
transactions.to_sql('transactions', engine, schema='staging', if_exists='replace', index=False)
print(f"   ✅ Loaded {len(transactions):,} transactions into staging.transactions")

print("\n" + "=" * 60)
print("DATA LOAD COMPLETE!")
print("=" * 60)

# Verify data (FIXED VERSION)
print("\nVerifying data in database...")
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM staging.customers"))
    print(f"staging.customers: {result.fetchone()[0]:,} rows")
    
    result = conn.execute(text("SELECT COUNT(*) FROM staging.products"))
    print(f"staging.products: {result.fetchone()[0]:,} rows")
    
    result = conn.execute(text("SELECT COUNT(*) FROM staging.transactions"))
    print(f"staging.transactions: {result.fetchone()[0]:,} rows")

print("\n✅ All data loaded successfully!")