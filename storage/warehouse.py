import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import duckdb
import pandas as pd
import logging
import os
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class DataWarehouse:
    def __init__(self, db_path: str = "data/warehouse/ecommerce.duckdb"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB warehouse: {db_path}")
        self._setup_schemas()

    def _setup_schemas(self):
        schemas = ["raw", "staging", "marts"]
        for schema in schemas:
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            logger.info(f"Schema ready: {schema}")

    def load_dataframe(self, df: pd.DataFrame, table_name: str, mode: str = "replace"):
        logger.info(f"Loading {len(df)} rows into {table_name}...")
        if mode == "replace":
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        elif mode == "append":
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Successfully loaded {count} rows into {table_name}")

    def query(self, sql: str) -> pd.DataFrame:
        logger.info(f"Executing: {sql[:80]}...")
        return self.conn.execute(sql).fetchdf()
    def list_tables(self):
        result = self.conn.execute("""
            SELECT
                table_schema,
                table_name,
                -- We query row count for each table dynamically
                -- This is a subquery inside the main query
                (SELECT COUNT(*)
                 FROM information_schema.tables t2
                 WHERE t2.table_name = tables.table_name
                ) as approx_rows
            FROM information_schema.tables
            WHERE table_schema IN ('raw', 'staging', 'marts')
            ORDER BY table_schema, table_name
        """).fetchdf()
        return result
    def close(self):
        self.conn.close()
        logger.info("Warehouse connection closed")


class WarehouseLoader:

    def __init__(self):
        from ingestion.api_extractor import APIExtractor
        from ingestion.csv_loader import CSVLoader
        from ingestion.db_extractor import DBExtractor
        self.api_extractor = APIExtractor(
            base_url="https://jsonplaceholder.typicode.com"
        )
        self.csv_loader = CSVLoader(input_dir="data/raw")
        self.db_extractor = DBExtractor(db_path="data/raw/ecommerce.db")
        self.warehouse = DataWarehouse()
        logger.info("WarehouseLoader initialized")

    def load_api_data(self):
        logger.info("=== Loading API Data ===")
        users_df = self.api_extractor.extract_and_save("/users", "users.csv")
        posts_df = self.api_extractor.extract_and_save("/posts", "posts.csv")
        todos_df = self.api_extractor.extract_and_save("/todos", "todos.csv")
        self.warehouse.load_dataframe(users_df, "raw.api_users")
        self.warehouse.load_dataframe(posts_df, "raw.api_posts")
        self.warehouse.load_dataframe(todos_df, "raw.api_todos")

    def load_db_data(self):
        logger.info("=== Loading Database Data ===")
        customers_df = self.db_extractor.extract_table("customers")
        products_df  = self.db_extractor.extract_table("products")
        orders_df    = self.db_extractor.extract_table("orders")
        self.warehouse.load_dataframe(customers_df, "raw.customers")
        self.warehouse.load_dataframe(products_df,  "raw.products")
        self.warehouse.load_dataframe(orders_df,    "raw.orders")

    def run_full_load(self):
        logger.info("==============================")
        logger.info("  Starting Full Warehouse Load ")
        logger.info("==============================")
        self.load_api_data()
        self.load_db_data()
        logger.info("=== Warehouse Contents ===")
        tables = self.warehouse.list_tables()
        print("\nTables in Warehouse:")
        print(tables.to_string(index=False))
        logger.info("=== Sample Analytics Query ===")
        result = self.warehouse.query("""
            SELECT
                p.category,
                COUNT(o.order_id)        AS total_orders,
                SUM(o.amount)            AS total_revenue,
                AVG(o.amount)            AS avg_order_value,
                COUNT(DISTINCT o.customer_id) AS unique_customers
            FROM raw.orders o
            LEFT JOIN raw.products p
                ON o.product_id = p.product_id
            WHERE o.status = 'completed'
            GROUP BY p.category
            ORDER BY total_revenue DESC
        """)
        print("\nRevenue by Category (Completed Orders):")
        print(result.to_string(index=False))
        logger.info("Full load complete!")
        self.warehouse.close()

if __name__ == "__main__":
    loader = WarehouseLoader()
    loader.run_full_load()