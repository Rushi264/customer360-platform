"""
Customer 360 REST API - FIXED
Exposes data from Business Vault views via REST endpoints
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://dataeng:password123@localhost:5432/customer360"

try:
    engine = create_engine(DATABASE_URL)
    logger.info("Database connection pool created")
except Exception as e:
    logger.error(f"Failed to create connection pool: {e}")
    engine = None

app = FastAPI(
    title="Customer 360 API",
    description="REST API for Customer 360 Data Platform",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"status": "healthy", "message": "Customer 360 API is running!", "docs": "/docs"}

@app.get("/customers")
async def get_customers(skip: int = 0, limit: int = 100):
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Database not connected")
        if limit > 1000:
            limit = 1000
        with engine.connect() as conn:
            query = text("""
                SELECT customer_id, first_name, last_name, email, phone, country, customer_segment,
                       COALESCE(total_orders, 0)::int as total_orders,
                       COALESCE(lifetime_value, 0)::float as lifetime_value,
                       COALESCE(avg_order_value, 0)::float as avg_order_value,
                       COALESCE(last_order_date::text, 'N/A') as last_order_date
                FROM business_vault.vw_customer_360
                ORDER BY customer_id
                OFFSET :skip LIMIT :limit
            """)
            result = conn.execute(query, {"skip": skip, "limit": limit})
            rows = result.fetchall()
            customers = []
            for row in rows:
                customers.append({
                    "customer_id": row[0], "first_name": row[1], "last_name": row[2], "email": row[3],
                    "phone": row[4], "country": row[5], "customer_segment": row[6], "total_orders": row[7],
                    "lifetime_value": row[8], "avg_order_value": row[9], "last_order_date": row[10]
                })
            return customers
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/customers/{customer_id}")
async def get_customer(customer_id: int):
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Database not connected")
        with engine.connect() as conn:
            query = text("""
                SELECT customer_id, first_name, last_name, email, phone, country, customer_segment,
                       COALESCE(total_orders, 0)::int as total_orders,
                       COALESCE(lifetime_value, 0)::float as lifetime_value,
                       COALESCE(avg_order_value, 0)::float as avg_order_value,
                       COALESCE(last_order_date::text, 'N/A') as last_order_date
                FROM business_vault.vw_customer_360
                WHERE customer_id = :customer_id
            """)
            result = conn.execute(query, {"customer_id": customer_id})
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Customer not found")
            return {
                "customer_id": row[0], "first_name": row[1], "last_name": row[2], "email": row[3],
                "phone": row[4], "country": row[5], "customer_segment": row[6], "total_orders": row[7],
                "lifetime_value": row[8], "avg_order_value": row[9], "last_order_date": row[10]
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/products")
async def get_products(skip: int = 0, limit: int = 100):
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Database not connected")
        if limit > 1000:
            limit = 1000
        with engine.connect() as conn:
            query = text("""
                SELECT product_id, product_name, category, price, supplier,
                       COALESCE(total_sold, 0)::int as total_sold,
                       COALESCE(total_quantity, 0)::int as total_quantity,
                       COALESCE(revenue, 0)::float as revenue,
                       COALESCE(avg_price, 0)::float as avg_price
                FROM business_vault.vw_product_analytics
                ORDER BY product_id OFFSET :skip LIMIT :limit
            """)
            result = conn.execute(query, {"skip": skip, "limit": limit})
            return [{"product_id": r[0], "product_name": r[1], "category": r[2], "price": float(r[3]),
                     "supplier": r[4], "total_sold": r[5], "total_quantity": r[6], "revenue": r[7], "avg_price": r[8]}
                    for r in result.fetchall()]
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@app.get("/orders")
async def get_orders(skip: int = 0, limit: int = 100):
    try:
        if not engine:
            raise HTTPException(status_code=500, detail="Database not connected")
        if limit > 1000:
            limit = 1000
        with engine.connect() as conn:
            query = text("""
                SELECT transaction_id, order_date, quantity, unit_price, total_amount, payment_method, order_status
                FROM business_vault.vw_order_analytics
                ORDER BY order_date DESC OFFSET :skip LIMIT :limit
            """)
            result = conn.execute(query, {"skip": skip, "limit": limit})
            rows = result.fetchall()
            return {"total_records": len(rows), "skip": skip, "limit": limit,
                    "orders": [{"transaction_id": r[0], "order_date": str(r[1]), "quantity": r[2],
                               "unit_price": float(r[3]), "total_amount": float(r[4]), 
                               "payment_method": r[5], "order_status": r[6]} for r in rows]}
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
