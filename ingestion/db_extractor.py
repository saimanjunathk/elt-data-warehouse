import sqlite3
import sqlalchemy
import pandas as pd
import logging
import os
from faker import Faker
import random
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
fake = Faker()
Faker.seed(42)
random.seed(42)

class DBExtractor:
    def __init__(self, db_path: str = "data/raw/ecommerce.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")
        logger.info(f"Database connection created: {db_path}")
    def create_sample_database(self, n_customers=50, n_products=20, n_orders=200):
        logger.info("Creating sample ecommerce database...")
        customers = []
        for i in range(1, n_customers + 1):
            customers.append({
                "customer_id": i,
                "name": fake.name(),
                "email": fake.email(),
                "city": fake.city(),
                "state": fake.state(),
                "created_at": fake.date_between(
                    start_date="-2y",
                    end_date="today"
                ).strftime("%Y-%m-%d")
            })
        categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]

        products = []
        for i in range(1, n_products + 1):
            products.append({
                "product_id": i,
                "name": fake.bs().title(),
                "category": random.choice(categories),
                "price": round(random.uniform(9.99, 499.99), 2),
                "stock": random.randint(0, 500)
            })
        statuses = ["completed", "pending", "cancelled", "refunded"]
        orders = []
        for i in range(1, n_orders + 1):
            customer_id = random.randint(1, n_customers)
            product_id = random.randint(1, n_products)
            quantity = random.randint(1, 5)
            price = products[product_id - 1]["price"]
            orders.append({
                "order_id": i,
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "amount": round(price * quantity, 2),
                "status": random.choice(statuses),
                "order_date": fake.date_between(
                    start_date="-1y",
                    end_date="today"
                ).strftime("%Y-%m-%d")
            })
        customers_df = pd.DataFrame(customers)
        products_df = pd.DataFrame(products)
        orders_df = pd.DataFrame(orders)
        customers_df.to_sql("customers", self.engine, if_exists="replace", index=False)
        products_df.to_sql("products", self.engine, if_exists="replace", index=False)
        orders_df.to_sql("orders", self.engine, if_exists="replace", index=False)
        logger.info(f"Created customers table: {len(customers_df)} rows")
        logger.info(f"Created products table:  {len(products_df)} rows")
        logger.info(f"Created orders table:    {len(orders_df)} rows")
        return customers_df, products_df, orders_df

    def extract(self, query: str) -> pd.DataFrame:
        logger.info(f"Executing query: {query[:80]}...")
        df = pd.read_sql(query, self.engine)
        logger.info(f"Query returned {len(df)} rows")
        return df

    def extract_table(self, table_name: str) -> pd.DataFrame:
        query = f"SELECT * FROM {table_name}"
        return self.extract(query)

    def extract_incremental(self, table_name: str, date_column: str, since_date: str) -> pd.DataFrame:
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE {date_column} >= '{since_date}'
        """
        logger.info(f"Incremental extract from {table_name} since {since_date}")
        return self.extract(query)

if __name__ == "__main__":
    extractor = DBExtractor(db_path="data/raw/ecommerce.db")
    extractor.create_sample_database(
        n_customers=50,
        n_products=20,
        n_orders=200
    )
    customers = extractor.extract_table("customers")
    print(f"\nCustomers sample:")
    print(customers.head(3))
    products = extractor.extract_table("products")
    print(f"\nProducts sample:")
    print(products.head(3))
    completed_orders = extractor.extract("""
        SELECT
            o.order_id,
            o.order_date,
            o.amount,
            o.status,
            c.name as customer_name,
            c.city,
            p.name as product_name,
            p.category
        FROM orders o
        -- JOIN connects two tables using a shared column
        -- LEFT JOIN keeps all rows from orders even if no match in customers
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN products p  ON o.product_id  = p.product_id
        WHERE o.status = 'completed'
        ORDER BY o.order_date DESC
    """)
    print(f"\nCompleted Orders sample ({len(completed_orders)} total):")
    print(completed_orders.head(3))
    from datetime import datetime, timedelta
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    recent_orders = extractor.extract_incremental(
        table_name="orders",
        date_column="order_date",
        since_date=thirty_days_ago
    )
    print(f"\nRecent Orders (last 30 days): {len(recent_orders)} orders")