import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",          # wide = full browser width
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_connection():
    db_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "warehouse", "ecommerce.duckdb"
    )
    return duckdb.connect(db_path, read_only=True)

@st.cache_data(ttl=300)
def load_kpis():
    # This query calculates our top-level KPI numbers
    conn = get_connection()
    return conn.execute("""
        SELECT
            COUNT(DISTINCT order_id)                    AS total_orders,
            COUNT(DISTINCT customer_id)                 AS total_customers,
            ROUND(SUM(completed_revenue), 2)            AS total_revenue,
            ROUND(AVG(CASE WHEN is_completed
                THEN amount END), 2)                    AS avg_order_value,
            COUNT(DISTINCT CASE WHEN is_completed
                THEN order_id END)                      AS completed_orders,
            COUNT(DISTINCT CASE WHEN is_cancelled
                THEN order_id END)                      AS cancelled_orders
        FROM main_marts.mart_sales
    """).fetchdf()


@st.cache_data(ttl=300)
def load_revenue_by_month():
    conn = get_connection()
    return conn.execute("""
        SELECT
            -- Format year-month as "2024-01" for the x axis
            order_year || '-' ||
            LPAD(CAST(order_month AS VARCHAR), 2, '0') AS month,
            -- LPAD pads with zeros: 1 → "01", 12 → "12"
            ROUND(SUM(completed_revenue), 2)           AS revenue,
            COUNT(DISTINCT order_id)                   AS orders
        FROM main_marts.mart_sales
        WHERE is_completed = true
        GROUP BY order_year, order_month
        ORDER BY order_year, order_month
    """).fetchdf()


@st.cache_data(ttl=300)
def load_revenue_by_category():
    conn = get_connection()
    return conn.execute("""
        SELECT
            category,
            ROUND(SUM(completed_revenue), 2)            AS revenue,
            COUNT(DISTINCT order_id)                    AS orders,
            ROUND(AVG(amount), 2)                       AS avg_order_value
        FROM main_marts.mart_sales
        WHERE is_completed = true
          AND category IS NOT NULL
        GROUP BY category
        ORDER BY revenue DESC
    """).fetchdf()


@st.cache_data(ttl=300)
def load_customer_segments():
    conn = get_connection()
    return conn.execute("""
        SELECT
            customer_value_tier,
            COUNT(*)                                    AS customers,
            ROUND(AVG(total_completed_spend), 2)        AS avg_spend
        FROM main_marts.mart_customers
        GROUP BY customer_value_tier
        ORDER BY avg_spend DESC
    """).fetchdf()


@st.cache_data(ttl=300)
def load_top_customers():
    conn = get_connection()
    return conn.execute("""
        SELECT
            customer_name,
            city,
            state,
            total_orders,
            ROUND(total_completed_spend, 2)             AS total_spend,
            customer_value_tier,
            customer_segment
        FROM main_marts.mart_customers
        WHERE total_orders > 0
        ORDER BY total_completed_spend DESC
        LIMIT 10
    """).fetchdf()


@st.cache_data(ttl=300)
def load_sales_by_state():
    conn = get_connection()
    return conn.execute("""
        SELECT
            state,
            COUNT(DISTINCT order_id)                    AS orders,
            ROUND(SUM(completed_revenue), 2)            AS revenue
        FROM main_marts.mart_sales
        WHERE is_completed = true
          AND state IS NOT NULL
        GROUP BY state
        ORDER BY revenue DESC
        LIMIT 10
    """).fetchdf()

st.title("🛒 E-Commerce Analytics Dashboard")
st.markdown("**Real-time analytics powered by DuckDB + dbt**")

st.divider()

st.subheader("📊 Key Performance Indicators")

kpis = load_kpis()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="💰 Total Revenue",
        value=f"${kpis['total_revenue'].iloc[0]:,.2f}"
    )

with col2:
    st.metric(
        label="📦 Total Orders",
        value=f"{kpis['total_orders'].iloc[0]:,}"
    )

with col3:
    st.metric(
        label="👥 Total Customers",
        value=f"{kpis['total_customers'].iloc[0]:,}"
    )

with col4:
    st.metric(
        label="🛍️ Avg Order Value",
        value=f"${kpis['avg_order_value'].iloc[0]:,.2f}"
    )

st.divider()

st.subheader("📈 Revenue Analysis")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Revenue by Month**")
    monthly = load_revenue_by_month()

    fig = px.line(
        monthly,
        x="month",
        y="revenue",
        markers=True,
        labels={"month": "Month", "revenue": "Revenue ($)"}
    )

    fig.update_traces(line_color="#00d4ff")

    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Revenue by Category**")
    category = load_revenue_by_category()


    fig = px.bar(
        category,
        x="category",
        y="revenue",
        color="category",  
        labels={"category": "Category", "revenue": "Revenue ($)"}
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# ─────────────────────────────────────────────
# ROW 3: CUSTOMER ANALYSIS
# ─────────────────────────────────────────────
st.subheader("👥 Customer Analysis")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Customer Value Tiers**")
    segments = load_customer_segments()

    # px.pie() creates a pie chart
    fig = px.pie(
        segments,
        values="customers",     # Size of each slice
        names="customer_value_tier",  # Label for each slice
        hole=0.4                # hole=0.4 makes it a donut chart
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Top 10 Customers by Revenue**")
    top_customers = load_top_customers()

    # st.dataframe() renders an interactive table
    # hide_index=True → don't show row numbers
    st.dataframe(
        top_customers,
        hide_index=True,
        use_container_width=True
    )

st.divider()


# ─────────────────────────────────────────────
# ROW 4: GEOGRAPHIC ANALYSIS
# ─────────────────────────────────────────────
st.subheader("🗺️ Sales by State")

state_data = load_sales_by_state()

# px.bar() with horizontal orientation
fig = px.bar(
    state_data,
    x="revenue",
    y="state",
    orientation="h",        # h = horizontal bar chart
    color="revenue",        # Color bars by revenue value
    labels={"revenue": "Revenue ($)", "state": "State"}
)
# Sort bars from highest to lowest
fig.update_layout(yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig, use_container_width=True)

st.divider()


# ─────────────────────────────────────────────
# SIDEBAR
# st.sidebar.* puts content in the left sidebar
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("ℹ️ About")
    st.markdown("""
    **ELT Data Warehouse Project**

    Built with:
    - 🦆 DuckDB
    - 🔧 dbt
    - 🐍 Python
    - 📊 Streamlit
    - 📈 Plotly

    **Pipeline:**
    1. Extract from APIs & DB
    2. Load into DuckDB
    3. Transform with dbt
    4. Visualize with Streamlit
    """)

    st.divider()
    st.markdown("**Data Summary**")

    # Show quick stats in sidebar
    kpis = load_kpis()
    st.write(f"✅ Completed: {kpis['completed_orders'].iloc[0]}")
    st.write(f"❌ Cancelled: {kpis['cancelled_orders'].iloc[0]}")