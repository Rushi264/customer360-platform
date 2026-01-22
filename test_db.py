from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://dataeng:password123@localhost:5432/customer360"

try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM business_vault.vw_customer_360"))
        count = result.fetchone()[0]
        print(f"Connection successful!")
        print(f"Customer count: {count}")
except Exception as e:
    print(f"Error: {str(e)}")
    import traceback
    traceback.print_exc()
