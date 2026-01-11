from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta
import os

fake = Faker()
Faker.seed(42)
random.seed(42)

print("=" * 60)
print("CUSTOMER 360 DATA GENERATOR")
print("=" * 60)

def generate_customers(n=10000):
    print(f"\nGenerating {n:,} customers...")
    customers = []
    for i in range(1, n+1):
        if i % 1000 == 0:
            print(f"   Progress: {i:,}/{n:,}")
        customers.append({
            "customer_id": f"CUST_{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "signup_date": fake.date_between(start_date='-2y', end_date='today').isoformat(),
            "country": "USA",
            "state": fake.state(),
            "city": fake.city(),
            "customer_segment": random.choices(['Bronze', 'Silver', 'Gold', 'Platinum'], weights=[40, 30, 20, 10])[0]
        })
    print(f"   Done: {len(customers):,} customers")
    return pd.DataFrame(customers)

def generate_products(n=5000):
    print(f"\nGenerating {n:,} products...")
    categories = {'Electronics': ['Laptops', 'Phones'], 'Apparel': ['Men', 'Women'], 'Home': ['Furniture', 'Decor']}
    products = []
    for i in range(1, n+1):
        if i % 1000 == 0:
            print(f"   Progress: {i:,}/{n:,}")
        category = random.choice(list(categories.keys()))
        price = round(random.uniform(10, 500), 2)
        products.append({
            "product_id": f"PROD_{i:05d}",
            "product_name": fake.catch_phrase(),
            "category": category,
            "price": price,
            "cost": round(price * 0.6, 2),
            "supplier": fake.company()
        })
    print(f"   Done: {len(products):,} products")
    return pd.DataFrame(products)

def generate_transactions(customers_df, products_df, n=100000):
    print(f"\nGenerating {n:,} transactions...")
    transactions = []
    customer_ids = customers_df['customer_id'].tolist()
    product_ids = products_df['product_id'].tolist()
    
    for i in range(1, n+1):
        if i % 10000 == 0:
            print(f"   Progress: {i:,}/{n:,}")
        days_ago = min(int(random.expovariate(1/180)), 730)
        order_date = datetime.now() - timedelta(days=days_ago)
        product_id = random.choice(product_ids)
        product_price = products_df[products_df['product_id'] == product_id]['price'].values[0]
        quantity = random.choice([1, 2, 3])
        
        transactions.append({
            "transaction_id": f"TXN_{i:06d}",
            "customer_id": random.choice(customer_ids),
            "order_date": order_date.strftime('%Y-%m-%d %H:%M:%S'),
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": product_price,
            "total_amount": round(product_price * quantity, 2),
            "payment_method": random.choice(['Credit Card', 'PayPal', 'Debit Card']),
            "order_status": random.choice(['Completed', 'Shipped', 'Processing'])
        })
    print(f"   Done: {len(transactions):,} transactions")
    return pd.DataFrame(transactions)

if __name__ == "__main__":
    os.makedirs('../data', exist_ok=True)
    
    print("\n" + "=" * 60)
    customers = generate_customers(10000)
    products = generate_products(5000)
    transactions = generate_transactions(customers, products, 100000)
    
    print("\n" + "=" * 60)
    print("SAVING FILES")
    print("=" * 60)
    
    customers.to_csv('../data/customers.csv', index=False)
    customers.to_json('../data/customers.json', orient='records', indent=2)
    print("Saved: customers.csv & customers.json")
    
    products.to_csv('../data/products.csv', index=False)
    print("Saved: products.csv")
    
    transactions.to_csv('../data/transactions.csv', index=False)
    print("Saved: transactions.csv")
    
    print("\n" + "=" * 60)
    print("DATA GENERATION COMPLETE!")
    print(f"Customers: {len(customers):,}")
    print(f"Products: {len(products):,}")
    print(f"Transactions: {len(transactions):,}")
    print("=" * 60)