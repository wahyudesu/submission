"""
Simple ETL Pipeline for Marketing Dashboard
Extract, Transform, Load GA4 data to PostgreSQL
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import kagglehub
from dotenv import load_dotenv
import pandas as pd
import psycopg
from psycopg.rows import dict_row

# Load environment
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', '')
DATA_DIR = Path(os.getenv('DATA_DIR', './data'))
RAW_DIR = DATA_DIR / 'raw'

NULL_VALUES = ['<Other>', 'NULL', 'null', '(not set)', 'not set', '', 'NaN']

# Dataset info
DATASET_HANDLE = "pdaasha/ga4-obfuscated-sample-ecommerce-jan2021"
# The downloaded file has a different name than expected
EXPECTED_CSV_FILE = "ga4_obfuscated_sample_ecommerce_Jan2021 - ga4_event_2021.csv"


def log(msg: str, level: str = 'INFO'):
    """Simple logger"""
    prefix = f"[{level}]"
    print(f"{prefix} {msg}")


def clean_val(val):
    """Clean NULL values"""
    if val is None or (isinstance(val, float) and (val != val)):  # NaN check
        return None
    val = str(val).strip()
    if val in NULL_VALUES or val == 'nan':
        return None
    return val


def clean_num(val):
    """Clean numeric values"""
    val = clean_val(val)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ============================================
# STEP 1: EXTRACT - Download from Kaggle
# ============================================
def download_from_kaggle(force: bool = False) -> Dict[str, Any]:
    """Download dataset using kagglehub.dataset_download"""
    log('Downloading from Kaggle using kagglehub...')

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    dataframes = {}

    try:
        # Download the dataset (returns path to downloaded files)
        log(f'  Downloading dataset: {DATASET_HANDLE}')
        download_path = kagglehub.dataset_download(DATASET_HANDLE, force_download=force)
        log(f'  Downloaded to: {download_path}')

        # List all CSV files in the download directory
        download_dir = Path(download_path)
        csv_files = list(download_dir.glob('*.csv'))
        log(f'  Found {len(csv_files)} CSV file(s)')

        for csv_path in csv_files:
            csv_name = csv_path.name
            log(f'  Loading {csv_name}...')
            df = pd.read_csv(csv_path)
            dataframes[csv_name] = df
            log(f'    Loaded {len(df)} rows from {csv_name}')

            # Save to local cache with a simpler name
            cache_name = 'events.csv'
            df.to_csv(RAW_DIR / cache_name, index=False)

    except Exception as e:
        log(f'  Error downloading dataset: {e}', 'ERROR')

        # Try loading from local cache as fallback
        log('  Attempting to load from local cache...')
        cached_path = RAW_DIR / 'events.csv'
        if cached_path.exists():
            log(f'  Loading events.csv from cache...')
            df = pd.read_csv(cached_path)
            dataframes['events.csv'] = df
            log(f'    Loaded {len(df)} rows from cache')

    log(f"Total files loaded: {len(dataframes)}")
    return dataframes


# ============================================
# STEP 2: TRANSFORM - Process data
# ============================================
def transform_data(dataframes: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw GA4 event data into metrics

    GA4 data is in "long format" where:
    - Each event has a base row with event_name, event_date, user info, etc.
    - Event parameters are in columns like event_params.key, event_params.value.*
    - Items are in columns like items.item_id, items.item_name, etc.
    - Some rows may be "parameter rows" with empty event_name but filled params
    """
    log('Transforming data...')

    # Get the first CSV file (whatever its name is)
    df = None
    for csv_name, data in dataframes.items():
        if data is not None and len(data) > 0:
            df = data
            log(f'Processing data from {csv_name}...')
            break

    if df is None:
        log('No data to process', 'ERROR')
        return {
            'events': [],
            'daily_metrics': [],
            'traffic_metrics': [],
            'product_metrics': []
        }

    log(f'Processing {len(df)} raw rows...')

    # Fill forward event data to parameter rows
    # In GA4 export, param rows have the same event_date/timestamp but empty event_name
    df['event_name'] = df['event_name'].ffill()
    df['user_pseudo_id'] = df['user_pseudo_id'].ffill()
    df['event_timestamp'] = df['event_timestamp'].ffill()

    # Group by unique events (event_date, user_pseudo_id, event_timestamp, event_name)
    # For now, we'll work with the main event rows (where event_name is not empty after fill)
    event_df = df[df['event_name'].notna()].copy()

    log(f'Found {len(event_df)} event rows after processing')

    all_events = []
    daily_metrics = {}
    traffic_metrics = {}
    product_metrics = {}

    for _, row in event_df.iterrows():
        # Extract event data
        event_name = clean_val(row.get('event_name')) or 'unknown'
        timestamp = row.get('event_timestamp', 0)
        date_val = row.get('event_date')
        user_id = clean_val(row.get('user_pseudo_id'))

        if not user_id:
            continue

        # Parse date (format: YYYYMMDD)
        try:
            if pd.notna(date_val):
                date_str = str(int(date_val)) if isinstance(date_val, (int, float)) else str(date_val)
                if len(date_str) == 8:
                    event_date = datetime.strptime(date_str, '%Y%m%d').date()
                else:
                    event_date = datetime.now().date()
            else:
                event_date = datetime.now().date()
        except:
            event_date = datetime.now().date()

        date_key = str(event_date)

        # Extract traffic source
        source = clean_val(row.get('traffic_source.source')) or '(none)'
        medium = clean_val(row.get('traffic_source.medium')) or '(none)'

        # Extract device info
        device_category = clean_val(row.get('device.category')) or '(none)'
        browser = clean_val(row.get('device.web_info.browser')) or '(none)'
        os = clean_val(row.get('device.operating_system')) or '(none)'

        # Extract geo info
        country = clean_val(row.get('geo.country')) or '(none)'
        city = clean_val(row.get('geo.city')) or '(none)'

        # Extract ecommerce data
        transaction_id = clean_val(row.get('ecommerce.transaction_id'))
        purchase_revenue = clean_num(row.get('ecommerce.purchase_revenue'))
        item_id = clean_val(row.get('items.item_id'))
        item_name = clean_val(row.get('items.item_name'))
        item_category = clean_val(row.get('items.item_category'))
        item_price = clean_num(row.get('items.price'))
        item_quantity = int(clean_val(row.get('items.quantity')) or '1')
        item_revenue = clean_num(row.get('items.item_revenue'))

        # Create session_id from user_id + timestamp (since it's not directly in the data)
        session_id = f"{user_id}_{str(timestamp)[:10]}"

        # Raw event
        event = {
            'event_name': event_name,
            'event_timestamp': int(timestamp) if pd.notna(timestamp) else 0,
            'event_date': date_key,
            'user_pseudo_id': user_id,
            'session_id': session_id,
            'country': country,
            'city': city,
            'device_category': device_category,
            'browser': browser,
            'os': os,
            'traffic_source': source,
            'traffic_medium': medium,
            'item_id': item_id,
            'item_name': item_name,
            'item_category': item_category,
            'item_revenue': item_revenue,
            'item_quantity': item_quantity,
            'transaction_id': transaction_id,
            'transaction_revenue': purchase_revenue,
            'page_location': None,
            'page_title': None
        }
        all_events.append(event)

        # Daily metrics aggregation
        if date_key not in daily_metrics:
            daily_metrics[date_key] = {
                'metric_date': date_key,
                'sessions': set(),
                'users': set(),
                'page_views': 0,
                'revenue': 0,
                'transactions': set(),
                'purchases': 0,
                'view_items': 0,
                'add_carts': 0,
                'checkouts': 0
            }

        dm = daily_metrics[date_key]
        dm['users'].add(user_id)
        if event_name == 'page_view':
            dm['page_views'] += 1
        dm['sessions'].add(session_id)
        if event_name == 'purchase':
            dm['purchases'] += 1
        if event_name == 'view_item':
            dm['view_items'] += 1
        if event_name == 'add_to_cart':
            dm['add_carts'] += 1
        if event_name == 'begin_checkout':
            dm['checkouts'] += 1
        if transaction_id:
            dm['transactions'].add(transaction_id)
        if purchase_revenue:
            dm['revenue'] += purchase_revenue

        # Traffic source metrics
        traffic_key = f"{date_key}|{source}|{medium}"

        if traffic_key not in traffic_metrics:
            traffic_metrics[traffic_key] = {
                'metric_date': date_key,
                'source': source,
                'medium': medium,
                'sessions': set(),
                'users': set(),
                'page_views': 0,
                'transactions': 0,
                'revenue': 0
            }

        tm = traffic_metrics[traffic_key]
        tm['users'].add(user_id)
        if event_name == 'page_view':
            tm['page_views'] += 1
        tm['sessions'].add(session_id)
        if event_name == 'purchase':
            tm['transactions'] += 1
        if purchase_revenue:
            tm['revenue'] += purchase_revenue

        # Product metrics
        if item_id:
            product_key = f"{date_key}|{item_id}"

            if product_key not in product_metrics:
                product_metrics[product_key] = {
                    'metric_date': date_key,
                    'item_id': item_id,
                    'item_name': item_name,
                    'item_category': item_category,
                    'views': 0,
                    'add_to_carts': 0,
                    'purchases': 0,
                    'revenue': 0,
                    'quantity_sold': 0
                }

            pm = product_metrics[product_key]
            if event_name == 'view_item':
                pm['views'] += 1
            if event_name == 'add_to_cart':
                pm['add_to_carts'] += 1
            if event_name == 'purchase':
                pm['purchases'] += 1
                if item_revenue:
                    pm['revenue'] += item_revenue
                pm['quantity_sold'] += item_quantity

    # Convert sets to counts
    daily_list = []
    for date_key, dm in daily_metrics.items():
        sessions = len(dm['sessions'])
        transactions = len(dm['transactions'])
        daily_list.append({
            'metric_date': date_key,
            'sessions': sessions,
            'total_users': len(dm['users']),
            'page_views': dm['page_views'],
            'total_revenue': dm['revenue'],
            'transactions': dm['purchases'],
            'items_sold': 0,
            'average_order_value': dm['revenue'] / transactions if transactions > 0 else 0,
            'conversion_rate': (dm['purchases'] / sessions * 100) if sessions > 0 else 0,
            'bounce_rate': 0,
            'unique_purchases': transactions,
            'view_item_events': dm['view_items'],
            'add_to_cart_events': dm['add_carts'],
            'begin_checkout_events': dm['checkouts']
        })

    traffic_list = []
    for key, tm in traffic_metrics.items():
        sessions = len(tm['sessions'])
        traffic_list.append({
            'metric_date': tm['metric_date'],
            'source': tm['source'],
            'medium': tm['medium'],
            'campaign': None,
            'sessions': sessions,
            'users': len(tm['users']),
            'page_views': tm['page_views'],
            'transactions': tm['transactions'],
            'revenue': tm['revenue'],
            'conversion_rate': (tm['transactions'] / sessions * 100) if sessions > 0 else 0
        })

    product_list = []
    for key, pm in product_metrics.items():
        product_list.append({
            'metric_date': pm['metric_date'],
            'item_id': pm['item_id'],
            'item_name': pm['item_name'],
            'item_category': pm['item_category'],
            'views': pm['views'],
            'add_to_carts': pm['add_to_carts'],
            'checkouts': 0,
            'purchases': pm['purchases'],
            'revenue': pm['revenue'],
            'quantity_sold': pm['quantity_sold'],
            'refund_amount': 0
        })

    return {
        'events': all_events,
        'daily_metrics': daily_list,
        'traffic_metrics': traffic_list,
        'product_metrics': product_list
    }


# ============================================
# STEP 3: LOAD - Insert to PostgreSQL
# ============================================
def get_connection():
    """Get database connection with dict row factory"""
    return psycopg.connect(DATABASE_URL)


def init_database():
    """Initialize database schema"""
    log('Initializing database schema...')

    conn = get_connection()
    cur = conn.cursor()

    schema = """
        DROP TABLE IF EXISTS product_metrics, traffic_source_metrics, daily_metrics, raw_events, etl_runs CASCADE;
        DROP MATERIALIZED VIEW IF EXISTS dashboard_summary CASCADE;

        CREATE TABLE raw_events (
            id SERIAL PRIMARY KEY,
            event_name VARCHAR(100),
            event_timestamp BIGINT,
            event_date DATE,
            user_pseudo_id VARCHAR(255),
            session_id VARCHAR(255),
            country VARCHAR(50),
            city VARCHAR(100),
            device_category VARCHAR(50),
            browser VARCHAR(100),
            os VARCHAR(100),
            traffic_source VARCHAR(255),
            traffic_medium VARCHAR(255),
            item_id VARCHAR(255),
            item_name VARCHAR(500),
            item_category VARCHAR(255),
            item_revenue DECIMAL(15,2),
            item_quantity INTEGER,
            transaction_id VARCHAR(255),
            transaction_revenue DECIMAL(15,2),
            page_location TEXT,
            page_title VARCHAR(500),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX idx_raw_events_date ON raw_events(event_date);

        CREATE TABLE daily_metrics (
            metric_date DATE PRIMARY KEY,
            sessions INTEGER,
            total_users INTEGER,
            page_views INTEGER,
            total_revenue DECIMAL(15,2),
            transactions INTEGER,
            items_sold INTEGER,
            average_order_value DECIMAL(15,2),
            conversion_rate DECIMAL(5,2),
            bounce_rate DECIMAL(5,2),
            unique_purchases INTEGER,
            view_item_events INTEGER,
            add_to_cart_events INTEGER,
            begin_checkout_events INTEGER,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE traffic_source_metrics (
            id SERIAL PRIMARY KEY,
            metric_date DATE,
            source VARCHAR(255),
            medium VARCHAR(255),
            campaign VARCHAR(255),
            sessions INTEGER,
            users INTEGER,
            page_views INTEGER,
            transactions INTEGER,
            revenue DECIMAL(15,2),
            conversion_rate DECIMAL(5,2),
            UNIQUE(metric_date, source, medium)
        );

        CREATE INDEX idx_traffic_date ON traffic_source_metrics(metric_date);

        CREATE TABLE product_metrics (
            id SERIAL PRIMARY KEY,
            metric_date DATE,
            item_id VARCHAR(255),
            item_name VARCHAR(500),
            item_category VARCHAR(255),
            views INTEGER,
            add_to_carts INTEGER,
            checkouts INTEGER DEFAULT 0,
            purchases INTEGER,
            revenue DECIMAL(15,2),
            quantity_sold INTEGER,
            refund_amount DECIMAL(15,2) DEFAULT 0,
            UNIQUE(metric_date, item_id)
        );

        CREATE INDEX idx_product_date ON product_metrics(metric_date);

        CREATE TABLE etl_runs (
            id SERIAL PRIMARY KEY,
            run_id UUID DEFAULT gen_random_uuid(),
            started_at TIMESTAMP DEFAULT NOW(),
            completed_at TIMESTAMP,
            status VARCHAR(50),
            rows_processed INTEGER DEFAULT 0,
            error_message TEXT
        );

        CREATE MATERIALIZED VIEW dashboard_summary AS
        SELECT
            dm.metric_date as date,
            dm.sessions,
            dm.total_users,
            dm.page_views,
            dm.total_revenue,
            dm.transactions,
            dm.items_sold,
            dm.average_order_value,
            dm.conversion_rate
        FROM daily_metrics dm
        ORDER BY dm.metric_date DESC;
    """

    cur.execute(schema)
    conn.commit()
    cur.close()
    conn.close()

    log('Database initialized')


def init_database_cli():
    """CLI entry point for database initialization"""
    init_database()


def load_data(data: Dict[str, Any]) -> bool:
    """Load transformed data to database"""
    log('Loading data to database...')

    conn = get_connection()
    cur = conn.cursor(row_factory=dict_row)

    # Create ETL run record
    cur.execute(
        "INSERT INTO etl_runs (status, rows_processed) VALUES ('running', 0) RETURNING run_id"
    )
    run_id = cur.fetchone()['run_id']

    try:
        # Insert raw events in batches
        events = data['events']
        if events:
            log(f'  Inserting {len(events)} raw events...')
            columns = [
                'event_name', 'event_timestamp', 'event_date', 'user_pseudo_id',
                'session_id', 'country', 'city', 'device_category', 'browser', 'os',
                'traffic_source', 'traffic_medium', 'item_id', 'item_name', 'item_category',
                'item_revenue', 'item_quantity', 'transaction_id', 'transaction_revenue',
                'page_location', 'page_title'
            ]
            batch_size = 500
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                with conn.cursor() as batch_cur:
                    batch_cur.executemany(
                        f"INSERT INTO raw_events ({', '.join(columns)}) VALUES "
                        f"({', '.join(['%s'] * len(columns))})",
                        [[event[col] for col in columns] for event in batch]
                    )
                if (i // batch_size) % 10 == 0:
                    log(f'    Progress: {min(i + batch_size, len(events))}/{len(events)}')
            conn.commit()
            log(f'  Inserted {len(events)} raw events')

        # Upsert daily metrics
        for dm in data['daily_metrics']:
            cur.execute("""
                INSERT INTO daily_metrics AS dm (
                    metric_date, sessions, total_users, page_views, total_revenue,
                    transactions, items_sold, average_order_value, conversion_rate,
                    bounce_rate, unique_purchases, view_item_events, add_to_cart_events,
                    begin_checkout_events
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (metric_date) DO UPDATE SET
                    sessions = EXCLUDED.sessions,
                    total_users = EXCLUDED.total_users,
                    page_views = EXCLUDED.page_views,
                    total_revenue = EXCLUDED.total_revenue,
                    transactions = EXCLUDED.transactions,
                    average_order_value = EXCLUDED.average_order_value,
                    conversion_rate = EXCLUDED.conversion_rate,
                    updated_at = NOW()
            """, (
                dm['metric_date'], dm['sessions'], dm['total_users'], dm['page_views'],
                dm['total_revenue'], dm['transactions'], dm['items_sold'],
                dm['average_order_value'], dm['conversion_rate'], dm['bounce_rate'],
                dm['unique_purchases'], dm['view_item_events'], dm['add_to_cart_events'],
                dm['begin_checkout_events']
            ))
        conn.commit()
        log(f"  Upserted {len(data['daily_metrics'])} daily metrics")

        # Upsert traffic metrics
        for tm in data['traffic_metrics']:
            cur.execute("""
                INSERT INTO traffic_source_metrics AS tsm (
                    metric_date, source, medium, campaign, sessions, users,
                    page_views, transactions, revenue, conversion_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (metric_date, source, medium) DO UPDATE SET
                    sessions = EXCLUDED.sessions,
                    users = EXCLUDED.users,
                    page_views = EXCLUDED.page_views,
                    transactions = EXCLUDED.transactions,
                    revenue = EXCLUDED.revenue,
                    conversion_rate = EXCLUDED.conversion_rate
            """, (
                tm['metric_date'], tm['source'], tm['medium'], tm['campaign'],
                tm['sessions'], tm['users'], tm['page_views'], tm['transactions'],
                tm['revenue'], tm['conversion_rate']
            ))
        conn.commit()
        log(f"  Upserted {len(data['traffic_metrics'])} traffic metrics")

        # Upsert product metrics
        for pm in data['product_metrics']:
            cur.execute("""
                INSERT INTO product_metrics AS pm (
                    metric_date, item_id, item_name, item_category, views,
                    add_to_carts, checkouts, purchases, revenue, quantity_sold, refund_amount
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (metric_date, item_id) DO UPDATE SET
                    views = EXCLUDED.views,
                    add_to_carts = EXCLUDED.add_to_carts,
                    purchases = EXCLUDED.purchases,
                    revenue = EXCLUDED.revenue,
                    quantity_sold = EXCLUDED.quantity_sold
            """, (
                pm['metric_date'], pm['item_id'], pm['item_name'], pm['item_category'],
                pm['views'], pm['add_to_carts'], pm['checkouts'], pm['purchases'],
                pm['revenue'], pm['quantity_sold'], pm['refund_amount']
            ))
        conn.commit()
        log(f"  Upserted {len(data['product_metrics'])} product metrics")

        # Refresh materialized view
        cur.execute("REFRESH MATERIALIZED VIEW dashboard_summary")
        conn.commit()

        # Update ETL run as completed
        cur.execute("""
            UPDATE etl_runs
            SET completed_at = NOW(), status = 'completed', rows_processed = %s
            WHERE run_id = %s
        """, (len(events), run_id))
        conn.commit()

        cur.close()
        conn.close()

        log('Data loaded successfully')
        return True

    except Exception as e:
        conn.rollback()
        cur.execute("""
            UPDATE etl_runs
            SET completed_at = NOW(), status = 'failed', error_message = %s
            WHERE run_id = %s
        """, (str(e), run_id))
        conn.commit()
        cur.close()
        conn.close()
        raise


# ============================================
# MAIN ETL FUNCTION
# ============================================
def run_etl(force: bool = False) -> Dict[str, Any]:
    """Run the complete ETL pipeline"""
    start_time = time.time()
    log('=' * 50)
    log('Starting ETL Pipeline...')

    try:
        # Step 1: Extract
        log('\n[1/3] EXTRACT - Downloading data...')
        dataframes = download_from_kaggle(force)

        # Step 2: Transform
        log('\n[2/3] TRANSFORM - Processing data...')
        data = transform_data(dataframes)
        log(f"Processed {len(data['events'])} events")

        # Step 3: Load
        log('\n[3/3] LOAD - Storing to database...')
        load_data(data)

        duration = time.time() - start_time
        log(f'\n ETL completed in {duration:.1f}s')
        log('=' * 50)

        return {'success': True, 'events': len(data['events'])}

    except Exception as e:
        duration = time.time() - start_time
        log(f'\n ETL failed: {e}', 'ERROR')
        log('=' * 50)
        return {'success': False, 'error': str(e)}


def main():
    """CLI entry point for uv run etl"""
    import sys
    force = '--force' in sys.argv or '-f' in sys.argv
    result = run_etl(force=force)
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
