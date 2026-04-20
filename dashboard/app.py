import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import random
from faker import Faker


# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ─────────────────────────────────────────────
# GENERATE DATA + BUILD WAREHOUSE IN MEMORY
# @st.cache_resource → runs once, reuses result
# We build the entire warehouse in memory
# No local files needed — works on Streamlit Cloud!
# ─────────────────────────────────────────────
@st.cache_resource
def build_warehouse():

    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    # Create in-memory DuckDB database
    # ":memory:" means no file, lives in RAM
    conn = duckdb.connect(":memory:")

    # Create schemas
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")

    # ── Generate customers ──
    categories  = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    statuses    = ["completed", "pending", "cancelled", "refunded"]

    customers = [{
        "customer_id": i,
        "name":       fake.name(),
        "email":      fake.email(),
        "city":       fake.city(),
        "state":      fake.state(),
        "created_at": fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d")
    } for i in range(1, 51)]

    # ── Generate products ──
    products = [{
        "product_id": i,
        "name":       fake.bs().title(),
        "category":   random.choice(categories),
        "price":      round(random.uniform(9.99, 499.99), 2),
        "stock":      random.randint(0, 500)
    } for i in range(1, 21)]

    # ── Generate orders ──
    orders = []
    for i in range(1, 201):
        cid = random.randint(1, 50)
        pid = random.randint(1, 20)
        qty = random.randint(1, 5)
        price = products[pid - 1]["price"]
        orders.append({
            "order_id":    i,
            "customer_id": cid,
            "product_id":  pid,
            "quantity":    qty,
            "amount":      round(price * qty, 2),
            "status":      random.choice(statuses),
            "order_date":  fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
        })

    # ── Load into DuckDB ──
    customers_df = pd.DataFrame(customers)
    products_df  = pd.DataFrame(products)
    orders_df    = pd.DataFrame(orders)

    conn.execute("CREATE TABLE raw.customers AS SELECT * FROM customers_df")
    conn.execute("CREATE TABLE raw.products  AS SELECT * FROM products_df")
    conn.execute("CREATE TABLE raw.orders    AS SELECT * FROM orders_df")

    # ── Staging transformations ──
    conn.execute("""
        CREATE VIEW staging.stg_orders AS
        SELECT
            CAST(order_id    AS INTEGER)        AS order_id,
            CAST(customer_id AS INTEGER)        AS customer_id,
            CAST(product_id  AS INTEGER)        AS product_id,
            CAST(quantity    AS INTEGER)        AS quantity,
            CAST(amount AS DECIMAL(10,2))       AS amount,
            LOWER(TRIM(status))                 AS status,
            CAST(order_date AS DATE)            AS order_date,
            EXTRACT(YEAR  FROM CAST(order_date AS DATE)) AS order_year,
            EXTRACT(MONTH FROM CAST(order_date AS DATE)) AS order_month,
            CASE WHEN LOWER(status) = 'completed' THEN true  ELSE false END AS is_completed,
            CASE WHEN LOWER(status) = 'cancelled' THEN true  ELSE false END AS is_cancelled,
            CASE WHEN LOWER(status) = 'completed'
                 THEN CAST(amount AS DECIMAL(10,2)) ELSE 0 END AS completed_revenue
        FROM raw.orders
        WHERE order_id IS NOT NULL AND amount > 0
    """)

    conn.execute("""
        CREATE VIEW staging.stg_customers AS
        SELECT
            CAST(customer_id AS INTEGER)        AS customer_id,
            TRIM(name)                          AS customer_name,
            LOWER(TRIM(email))                  AS email,
            TRIM(city)                          AS city,
            TRIM(state)                         AS state,
            CAST(created_at AS DATE)            AS created_at,
            DATEDIFF('day', CAST(created_at AS DATE), CURRENT_DATE) AS days_since_signup,
            CASE
                WHEN DATEDIFF('day', CAST(created_at AS DATE), CURRENT_DATE) <= 90  THEN 'New'
                WHEN DATEDIFF('day', CAST(created_at AS DATE), CURRENT_DATE) <= 365 THEN 'Regular'
                ELSE 'Veteran'
            END AS customer_segment
        FROM raw.customers
        WHERE customer_id IS NOT NULL
    """)

    conn.execute("""
        CREATE VIEW staging.stg_products AS
        SELECT
            CAST(product_id AS INTEGER)         AS product_id,
            TRIM(name)                          AS product_name,
            TRIM(category)                      AS category,
            CAST(price AS DECIMAL(10,2))        AS price,
            CAST(stock AS INTEGER)              AS stock,
            CASE
                WHEN CAST(price AS DECIMAL) < 50  THEN 'Budget'
                WHEN CAST(price AS DECIMAL) < 200 THEN 'Mid-Range'
                ELSE 'Premium'
            END AS price_tier
        FROM raw.products
        WHERE product_id IS NOT NULL AND price > 0
    """)

    # ── Mart transformations ──
    conn.execute("""
        CREATE TABLE marts.mart_sales AS
        SELECT
            o.order_id,
            o.order_date,
            o.order_year,
            o.order_month,
            o.quantity,
            o.amount,
            o.status,
            o.is_completed,
            o.is_cancelled,
            o.completed_revenue,
            c.customer_id,
            c.customer_name,
            c.city,
            c.state,
            c.customer_segment,
            p.product_id,
            p.product_name,
            p.category,
            p.price,
            p.price_tier
        FROM staging.stg_orders o
        LEFT JOIN staging.stg_customers c ON o.customer_id = c.customer_id
        LEFT JOIN staging.stg_products  p ON o.product_id  = p.product_id
    """)

    conn.execute("""
        CREATE TABLE marts.mart_customers AS
        SELECT
            c.customer_id,
            c.customer_name,
            c.email,
            c.city,
            c.state,
            c.created_at,
            c.customer_segment,
            c.days_since_signup,
            COALESCE(COUNT(o.order_id), 0)                  AS total_orders,
            COALESCE(SUM(o.completed_revenue), 0)           AS total_completed_spend,
            COALESCE(AVG(o.amount), 0)                      AS avg_order_value,
            CASE
                WHEN COALESCE(SUM(o.completed_revenue), 0) >= 2000 THEN 'High Value'
                WHEN COALESCE(SUM(o.completed_revenue), 0) >= 500  THEN 'Mid Value'
                WHEN COALESCE(SUM(o.completed_revenue), 0) > 0     THEN 'Low Value'
                ELSE 'No Orders'
            END AS customer_value_tier
        FROM staging.stg_customers c
        LEFT JOIN staging.stg_orders o ON c.customer_id = o.customer_id
        GROUP BY
            c.customer_id, c.customer_name, c.email,
            c.city, c.state, c.created_at,
            c.customer_segment, c.days_since_signup
    """)

    return conn


# ─────────────────────────────────────────────
# QUERY HELPERS
# ─────────────────────────────────────────────
def query(sql: str) -> pd.DataFrame:
    conn = build_warehouse()
    return conn.execute(sql).fetchdf()


# ─────────────────────────────────────────────
# DASHBOARD LAYOUT
# ─────────────────────────────────────────────
st.title("🛒 E-Commerce Analytics Dashboard")
st.markdown("**End-to-end ELT pipeline: Python → DuckDB → dbt-style transforms → Streamlit**")
st.divider()


# ── KPIs ──
st.subheader("📊 Key Performance Indicators")
kpis = query("""
    SELECT
        COUNT(DISTINCT order_id)                        AS total_orders,
        COUNT(DISTINCT customer_id)                     AS total_customers,
        ROUND(SUM(completed_revenue), 2)                AS total_revenue,
        ROUND(AVG(CASE WHEN is_completed THEN amount END), 2) AS avg_order_value,
        COUNT(DISTINCT CASE WHEN is_completed THEN order_id END) AS completed_orders,
        COUNT(DISTINCT CASE WHEN is_cancelled THEN order_id END) AS cancelled_orders
    FROM marts.mart_sales
""")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("💰 Total Revenue",    f"${kpis['total_revenue'].iloc[0]:,.2f}")
with col2:
    st.metric("📦 Total Orders",     f"{kpis['total_orders'].iloc[0]:,}")
with col3:
    st.metric("👥 Total Customers",  f"{kpis['total_customers'].iloc[0]:,}")
with col4:
    st.metric("🛍️ Avg Order Value",  f"${kpis['avg_order_value'].iloc[0]:,.2f}")

st.divider()


# ── Revenue Charts ──
st.subheader("📈 Revenue Analysis")
col1, col2 = st.columns(2)

with col1:
    st.markdown("**Revenue by Month**")
    monthly = query("""
        SELECT
            order_year || '-' || LPAD(CAST(order_month AS VARCHAR), 2, '0') AS month,
            ROUND(SUM(completed_revenue), 2) AS revenue
        FROM marts.mart_sales
        WHERE is_completed = true
        GROUP BY order_year, order_month
        ORDER BY order_year, order_month
    """)
    fig = px.line(monthly, x="month", y="revenue", markers=True,
                  labels={"month": "Month", "revenue": "Revenue ($)"})
    fig.update_traces(line_color="#00d4ff")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Revenue by Category**")
    cat = query("""
        SELECT category,
               ROUND(SUM(completed_revenue), 2) AS revenue
        FROM marts.mart_sales
        WHERE is_completed = true AND category IS NOT NULL
        GROUP BY category ORDER BY revenue DESC
    """)
    fig = px.bar(cat, x="category", y="revenue", color="category",
                 labels={"category": "Category", "revenue": "Revenue ($)"})
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# ── Customer Analysis ──
st.subheader("👥 Customer Analysis")
col1, col2 = st.columns(2)

with col1:
    st.markdown("**Customer Value Tiers**")
    segs = query("""
        SELECT customer_value_tier,
               COUNT(*) AS customers
        FROM marts.mart_customers
        GROUP BY customer_value_tier
    """)
    fig = px.pie(segs, values="customers", names="customer_value_tier", hole=0.4)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Top 10 Customers**")
    top = query("""
        SELECT customer_name, city, state,
               total_orders,
               ROUND(total_completed_spend, 2) AS total_spend,
               customer_value_tier
        FROM marts.mart_customers
        WHERE total_orders > 0
        ORDER BY total_completed_spend DESC
        LIMIT 10
    """)
    st.dataframe(top, hide_index=True, use_container_width=True)

st.divider()


# ── Sales by State ──
st.subheader("🗺️ Sales by State")
states = query("""
    SELECT state,
           COUNT(DISTINCT order_id)          AS orders,
           ROUND(SUM(completed_revenue), 2)  AS revenue
    FROM marts.mart_sales
    WHERE is_completed = true AND state IS NOT NULL
    GROUP BY state ORDER BY revenue DESC LIMIT 10
""")
fig = px.bar(states, x="revenue", y="state", orientation="h", color="revenue",
             labels={"revenue": "Revenue ($)", "state": "State"})
fig.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig, use_container_width=True)


# ── Sidebar ──
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    **ELT Data Warehouse Project**

    Built with:
    - 🦆 DuckDB
    - 🔧 dbt-style transforms
    - 🐍 Python
    - 📊 Streamlit
    - 📈 Plotly

    **Pipeline:**
    1. Generate realistic data with Faker
    2. Load into in-memory DuckDB
    3. Transform through raw → staging → marts
    4. Visualize with Streamlit

    **[View Source Code on GitHub](https://github.com/saimanjunathk/elt-data-warehouse)**
    """)
    st.divider()
    st.markdown("**Data Summary**")
    st.write(f"✅ Completed: {kpis['completed_orders'].iloc[0]}")
    st.write(f"❌ Cancelled: {kpis['cancelled_orders'].iloc[0]}")