"""
Marketing Performance Dashboard - Streamlit
Simple dashboard to visualize ETL data
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
import psycopg
from psycopg import sql as psql
from psycopg.rows import dict_row
import streamlit as st

# Load environment
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', '')


def get_connection():
    """Get database connection"""
    return psycopg.connect(DATABASE_URL)


def query_to_df(query: str, params: tuple = None) -> pd.DataFrame:
    """Execute query and return DataFrame"""
    conn = get_connection()
    try:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(query, params or ())
        rows = cur.fetchall()
        return pd.DataFrame(rows)
    finally:
        conn.close()


def format_number(val):
    """Format number with K/M suffix"""
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    elif val >= 1_000:
        return f"{val / 1_000:.1f}K"
    return f"{val:.0f}"


def format_currency(val):
    """Format currency"""
    if val >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    elif val >= 1_000:
        return f"${val / 1_000:.1f}K"
    return f"${val:.2f}"


# Page config
st.set_page_config(
    page_title="Marketing Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
        .main-header { font-size: 2rem; font-weight: bold; margin-bottom: 1rem; }
        .metric-card { background: #f0f2f6; padding: 1rem; border-radius: 10px; }
        .stDataFrame { border-radius: 10px; }
    </style>
""", unsafe_allow_html=True)

# Header
st.markdown('<h1 class="main-header">📊 Marketing Performance Dashboard</h1>', unsafe_allow_html=True)

# Sidebar filters
with st.sidebar:
    st.header("Filters")

    # Date range filter
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT MIN(metric_date), MAX(metric_date) FROM daily_metrics")
        min_date, max_date = cur.fetchone()
    except:
        min_date, max_date = datetime(2024, 1, 1), datetime.now()
    finally:
        conn.close()

    if min_date:
        date_range = st.slider(
            "Date Range",
            value=(max_date - timedelta(days=30), max_date),
            min_value=min_date,
            max_value=max_date,
            format="YYYY-MM-DD"
        )
    else:
        date_range = (datetime.now() - timedelta(days=30), datetime.now())

    start_date, end_date = date_range

    st.divider()

    # Refresh button
    if st.button("🔄 Refresh Data"):
        st.rerun()

    # Run ETL button
    if st.button("▶️ Run ETL Now"):
        with st.spinner("Running ETL pipeline..."):
            try:
                from etl import run_etl
                result = run_etl(force=False)
                if result['success']:
                    st.success("ETL completed successfully!")
                else:
                    st.error(f"ETL failed: {result.get('error')}")
            except Exception as e:
                st.error(f"ETL error: {e}")

    st.divider()
    st.caption(f"Data from: **{start_date}** to **{end_date}**")

# ============================================
# KPI CARDS
# ============================================
st.subheader("Key Performance Indicators")

# Query daily metrics for the period
kpi_query = """
    SELECT
        SUM(sessions) as total_sessions,
        SUM(total_users) as total_users,
        SUM(page_views) as total_page_views,
        SUM(total_revenue) as total_revenue,
        SUM(transactions) as total_transactions,
        SUM(average_order_value * transactions) / NULLIF(SUM(transactions), 0) as avg_aov,
        SUM(conversion_rate * sessions) / NULLIF(SUM(sessions), 0) as avg_conversion_rate
    FROM daily_metrics
    WHERE metric_date BETWEEN %s AND %s
"""

kpi_data = query_to_df(kpi_query, (start_date, end_date))

if not kpi_data.empty:
    row = kpi_data.iloc[0]

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("Sessions", format_number(row['total_sessions'] or 0))

    with col2:
        st.metric("Users", format_number(row['total_users'] or 0))

    with col3:
        st.metric("Page Views", format_number(row['total_page_views'] or 0))

    with col4:
        st.metric("Revenue", format_currency(row['total_revenue'] or 0))

    with col5:
        st.metric("Transactions", format_number(row['total_transactions'] or 0))

    with col6:
        st.metric("Conv. Rate", f"{row['avg_conversion_rate'] or 0:.2f}%")
else:
    st.warning("No data available for selected period")

# ============================================
# CHARTS
# ============================================
st.divider()
col1, col2 = st.columns(2)

# Daily revenue & sessions chart
with col1:
    st.subheader("Daily Revenue & Sessions")
    daily_query = """
        SELECT metric_date, sessions, total_revenue, transactions
        FROM daily_metrics
        WHERE metric_date BETWEEN %s AND %s
        ORDER BY metric_date
    """
    daily_df = query_to_df(daily_query, (start_date, end_date))

    if not daily_df.empty:
        daily_df['metric_date'] = pd.to_datetime(daily_df['metric_date'])

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=daily_df['metric_date'],
            y=daily_df['total_revenue'],
            name='Revenue',
            line=dict(color='#00CC96'),
            yaxis='y',
            fill='tozeroy'
        ))

        fig.add_trace(go.Scatter(
            x=daily_df['metric_date'],
            y=daily_df['sessions'],
            name='Sessions',
            line=dict(color='#636EFA'),
            yaxis='y2'
        ))

        fig.update_layout(
            yaxis=dict(title="Revenue ($)", side="left"),
            yaxis2=dict(title="Sessions", side="right", overlaying="y"),
            hovermode='x unified',
            height=300,
            margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No data available")

# Traffic source breakdown
with col2:
    st.subheader("Traffic Sources")
    traffic_query = """
        SELECT
            COALESCE(source, '(none)') as source,
            COALESCE(medium, '(none)') as medium,
            SUM(sessions) as sessions,
            SUM(users) as users,
            SUM(revenue) as revenue,
            SUM(transactions) as transactions
        FROM traffic_source_metrics
        WHERE metric_date BETWEEN %s AND %s
        GROUP BY source, medium
        ORDER BY revenue DESC
        LIMIT 10
    """
    traffic_df = query_to_df(traffic_query, (start_date, end_date))

    if not traffic_df.empty:
        traffic_df['label'] = traffic_df['source'] + ' / ' + traffic_df['medium']

        fig = px.pie(
            traffic_df,
            values='revenue',
            names='label',
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No data available")

# ============================================
# DATA TABLES
# ============================================
st.divider()
st.subheader("Data Tables")

tab1, tab2, tab3 = st.tabs(["Daily Metrics", "Traffic Sources", "Products"])

with tab1:
    daily_table_query = """
        SELECT
            metric_date,
            sessions,
            total_users,
            page_views,
            total_revenue,
            transactions,
            average_order_value,
            conversion_rate,
            view_item_events,
            add_to_cart_events,
            begin_checkout_events
        FROM daily_metrics
        WHERE metric_date BETWEEN %s AND %s
        ORDER BY metric_date DESC
    """
    daily_table = query_to_df(daily_table_query, (start_date, end_date))

    if not daily_table.empty:
        # Rename columns for display
        daily_table.columns = [
            'Date', 'Sessions', 'Users', 'Page Views', 'Revenue',
            'Transactions', 'AOV', 'Conv Rate %', 'View Item',
            'Add to Cart', 'Checkout'
        ]

        # Format numeric columns
        for col in ['Revenue', 'AOV']:
            if col in daily_table.columns:
                daily_table[col] = daily_table[col].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "-")

        for col in ['Conv Rate %']:
            if col in daily_table.columns:
                daily_table[col] = daily_table[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")

        st.dataframe(daily_table, width='stretch', height=400)
    else:
        st.info("No data available")

with tab2:
    traffic_table_query = """
        SELECT
            metric_date,
            source,
            medium,
            sessions,
            users,
            page_views,
            transactions,
            revenue,
            conversion_rate
        FROM traffic_source_metrics
        WHERE metric_date BETWEEN %s AND %s
        ORDER BY metric_date DESC, revenue DESC
    """
    traffic_table = query_to_df(traffic_table_query, (start_date, end_date))

    if not traffic_table.empty:
        # Rename columns for display
        traffic_table.columns = [
            'Date', 'Source', 'Medium', 'Sessions', 'Users',
            'Page Views', 'Transactions', 'Revenue', 'Conv Rate %'
        ]

        for col in ['Revenue']:
            if col in traffic_table.columns:
                traffic_table[col] = traffic_table[col].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "-")

        for col in ['Conv Rate %']:
            if col in traffic_table.columns:
                traffic_table[col] = traffic_table[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")

        st.dataframe(traffic_table, width='stretch', height=400)
    else:
        st.info("No data available")

with tab3:
    product_table_query = """
        SELECT
            metric_date,
            item_id,
            item_name,
            item_category,
            views,
            add_to_carts,
            purchases,
            revenue,
            quantity_sold
        FROM product_metrics
        WHERE metric_date BETWEEN %s AND %s
        ORDER BY metric_date DESC, revenue DESC
        LIMIT 100
    """
    product_table = query_to_df(product_table_query, (start_date, end_date))

    if not product_table.empty:
        # Rename columns for display
        product_table.columns = [
            'Date', 'Item ID', 'Item Name', 'Category', 'Views',
            'Add to Cart', 'Purchases', 'Revenue', 'Qty Sold'
        ]

        for col in ['Revenue']:
            if col in product_table.columns:
                product_table[col] = product_table[col].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "-")

        st.dataframe(product_table, width='stretch', height=400)
    else:
        st.info("No data available")

# ============================================
# ETL RUNS
# ============================================
st.divider()
st.subheader("ETL Run History")

etl_query = """
    SELECT
        started_at,
        completed_at,
        status,
        rows_processed,
        error_message
    FROM etl_runs
    ORDER BY started_at DESC
    LIMIT 10
"""
etl_table = query_to_df(etl_query)

if not etl_table.empty:
    etl_table.columns = ['Started', 'Completed', 'Status', 'Rows Processed', 'Error']
    st.dataframe(etl_table, width='stretch')
else:
    st.info("No ETL runs yet")

# Footer
st.divider()
st.caption("Marketing Performance Dashboard | Powered by Streamlit + PostgreSQL Neon")
