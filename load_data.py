import os
import requests
import math
import time
import argparse
import logging
import duckdb
import shutil
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

load_dotenv()

APP_TOKEN = os.getenv('APPLICATION_TOKEN')
BASE_DIR = './json_data'
os.makedirs(BASE_DIR, exist_ok=True)

class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

logging.getLogger().setLevel(logging.INFO)
handler = TqdmLoggingHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger().handlers = [handler]

class AdaptiveRateLimiter:
    """Manages rate limiting and adapts based on response times"""
    def __init__(self, initial_delay=0.5, max_delay=5.0):  # 0.5s initial delay for sweet spot
        self.current_delay = initial_delay
        self.max_delay = max_delay
        self.min_delay = 0.2
        self.response_times = []
        self.window_size = 10
        self.slow_response_threshold = 3.0
        
    def wait(self):
        """Wait before next request"""
        time.sleep(self.current_delay)
        
    def record_response(self, response_time, status_code):
        """Adjust delay based on response performance"""
        self.response_times.append(response_time)
        if len(self.response_times) > self.window_size:
            self.response_times.pop(0)
        
        avg_time = sum(self.response_times) / len(self.response_times)
        
        if status_code == 429 or avg_time > self.slow_response_threshold:
            self.current_delay = min(self.current_delay * 1.5, self.max_delay)
            logging.debug(f"Increasing delay to {self.current_delay:.2f}s")
        elif avg_time < 1.0 and self.current_delay > self.min_delay:
            self.current_delay = max(self.current_delay * 0.9, self.min_delay)

class UpdateTracker:
    """Tracks download state for incremental updates"""
    
    def __init__(self, conn):
        self.conn = conn
        self.init_tracking_table()
    
    def init_tracking_table(self):
        """Create metadata table to track sync status"""
        self.conn.execute("""
            CREATE SCHEMA IF NOT EXISTS metadata;
            
            CREATE TABLE IF NOT EXISTS metadata.sync_status (
                dataset_id VARCHAR PRIMARY KEY,
                table_name VARCHAR,
                time_period VARCHAR,
                last_sync_time TIMESTAMP,
                last_record_id VARCHAR,
                total_records_synced BIGINT,
                sync_method VARCHAR
            );
        """)
    
    def get_last_sync(self, dataset_id):
        """Get last sync information for a dataset"""
        result = self.conn.execute("""
            SELECT last_record_id, total_records_synced, last_sync_time
            FROM metadata.sync_status
            WHERE dataset_id = ?
        """, [dataset_id]).fetchone()
        
        if result:
            return {
                'last_id': result[0],
                'total_synced': result[1],
                'last_sync_time': result[2]
            }
        return None
    
    def update_sync_status(self, dataset_id, table_name, time_period, 
                          last_id, total_records, method='incremental'):
        """Update sync tracking information"""
        self.conn.execute("""
            INSERT INTO metadata.sync_status 
            (dataset_id, table_name, time_period, last_sync_time, 
             last_record_id, total_records_synced, sync_method)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset_id) DO UPDATE SET
                last_sync_time = EXCLUDED.last_sync_time,
                last_record_id = EXCLUDED.last_record_id,
                total_records_synced = EXCLUDED.total_records_synced,
                sync_method = EXCLUDED.sync_method
        """, [dataset_id, table_name, time_period, datetime.now(), 
              last_id, total_records, method])
    
    def should_do_full_refresh(self, dataset_id, time_period):
        """Determine if we should do a full refresh"""
        # Current tables always get full refresh
        if time_period == 'current':
            return True
        
        # Historical tables get incremental updates if we have prior sync
        last_sync = self.get_last_sync(dataset_id)
        return last_sync is None

def create_session():
    """Create a requests session with connection pooling and compression"""
    session = requests.Session()
    session.headers.update({
        'Accept-Encoding': 'gzip, deflate',
        'X-App-Token': APP_TOKEN if APP_TOKEN else ''
    })
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=3,
        pool_maxsize=3,
        max_retries=3
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_total_records(dataset_identifier, session=None, where_clause=None):
    """Get total record count for a dataset with optional WHERE clause"""
    if session is None:
        session = create_session()
    
    try:
        logging.info(f"Getting record count for dataset '{dataset_identifier}'.")
        url = f"https://data.texas.gov/resource/{dataset_identifier}.json"
        params = {"$select": "COUNT(*) as count"}
        if where_clause:
            params["$where"] = where_clause
        
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return int(response.json()[0]['count'])
    except Exception as e:
        logging.error(f"Error getting total records for '{dataset_identifier}': {e}")
        return 0

def fetch_batch_incremental(dataset_identifier, name, time_period, 
                           limit, batch_num, last_id=None, 
                           rate_limiter=None, session=None):
    """Fetch a batch using ID-based pagination"""
    if session is None:
        session = create_session()
    
    if rate_limiter:
        rate_limiter.wait()
    
    start_time = time.time()
    retries = 0
    max_retries = 5
    base_sleep = 1
    
    url = f"https://data.texas.gov/resource/{dataset_identifier}.json"
    params = {
        "$order": ":id",
        "$limit": limit,
        "$select": ":*, *"
    }
    
    if last_id:
        params["$where"] = f":id > '{last_id}'"
    
    while retries <= max_retries:
        try:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            response_time = time.time() - start_time
            if rate_limiter:
                rate_limiter.record_response(response_time, response.status_code)
            
            data = response.json()
            
            if not data:
                return None, None
            
            dataset_dir = os.path.join(BASE_DIR, dataset_identifier)
            os.makedirs(dataset_dir, exist_ok=True)
            file_name = f"{name}_{time_period}_batch{batch_num:06d}.json"
            file_path = os.path.join(dataset_dir, file_name)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f)
            
            last_record_id = data[-1].get(':id') if data else None
            return file_path, last_record_id
            
        except requests.exceptions.RequestException as e:
            retries += 1
            if retries > max_retries:
                logging.error(f"Maximum retries exceeded for '{dataset_identifier}' batch {batch_num}")
                return None, None
            
            sleep_time = base_sleep * (2 ** retries)
            logging.warning(f"Error fetching batch {batch_num}: {e}. Retrying in {sleep_time}s...")
            time.sleep(sleep_time)
    
    return None, None

def fetch_data(dataset_identifier, name, time_period,
               tracker, limit_per_page=3000, max_pages=None, max_workers=None):
    """
    Smart fetch that does full refresh for current, incremental for historical
    """
    session = create_session()
    
    # Check if we should do incremental or full refresh
    should_full_refresh = tracker.should_do_full_refresh(dataset_identifier, time_period)
    
    if should_full_refresh:
        logging.info(f"Performing FULL refresh for {dataset_identifier} ({time_period})")
        starting_id = None
        starting_count = 0
        sync_method = 'full'
    else:
        # Get last sync point for incremental update
        last_sync = tracker.get_last_sync(dataset_identifier)
        starting_id = last_sync['last_id']
        starting_count = last_sync['total_synced']
        logging.info(f"Performing INCREMENTAL update for {dataset_identifier} ({time_period})")
        logging.info(f"  Starting from record ID: {starting_id}")
        logging.info(f"  Previously synced: {starting_count:,} records")
        sync_method = 'incremental'
    
    # Get count of records to fetch
    if starting_id:
        where_clause = f":id > '{starting_id}'"
        new_records = get_total_records(dataset_identifier, session, where_clause)
        logging.info(f"  Found {new_records:,} new records to sync")
        total_records = new_records
    else:
        total_records = get_total_records(dataset_identifier, session)
        logging.info(f"  Total records to sync: {total_records:,}")
    
    if total_records == 0:
        logging.info(f"No new records found for dataset '{dataset_identifier}'.")
        return [], starting_id, starting_count
    
    # Apply max_pages limit if specified
    if max_pages:
        max_records = max_pages * limit_per_page
        if total_records > max_records:
            logging.info(f"Limiting to {max_pages} pages ({max_records:,} records)")
            total_records = max_records
    
    # Adaptive worker count - practical sweet spot (3-4 workers)
    if max_workers is None:  # Auto-scale if not specified
        if total_records > 500000:
            max_workers = 3  # Large datasets: 3 workers
            logging.info(f"Using {max_workers} workers for large dataset")
        elif total_records > 100000:
            max_workers = 3  # Medium: 3 workers
            logging.info(f"Using {max_workers} workers for medium dataset")
        else:
            max_workers = 4  # Small datasets: 4 workers (safe since less data)
            logging.info(f"Using {max_workers} workers for small dataset")
    else:
        logging.info(f"Using user-specified {max_workers} workers")
    
    rate_limiter = AdaptiveRateLimiter()
    files = []
    last_id = starting_id
    batch_num = 0
    total_fetched = 0
    
    start_time = time.time()
    
    with tqdm(total=total_records, desc=f"{'Full' if should_full_refresh else 'Incremental'} - {dataset_identifier}") as pbar:
        while True:
            # Check if we've reached max_pages limit
            if max_pages and batch_num >= max_pages:
                logging.info(f"Reached max_pages limit ({max_pages})")
                break
            
            file_path, new_last_id = fetch_batch_incremental(
                dataset_identifier, name, time_period,
                limit_per_page, batch_num, last_id,
                rate_limiter, session
            )
            
            if file_path is None:
                break
            
            files.append(file_path)
            last_id = new_last_id
            batch_num += 1
            
            records_in_batch = min(limit_per_page, total_records - total_fetched)
            total_fetched += records_in_batch
            pbar.update(records_in_batch)
            
            if batch_num % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_fetched / elapsed if elapsed > 0 else 0
                logging.info(f"Progress: {total_fetched:,}/{total_records:,} records "
                           f"({rate:.0f} records/sec)")
    
    # Update tracking
    total_synced = starting_count + total_fetched
    tracker.update_sync_status(dataset_identifier, f"{name}_{time_period}", 
                              time_period, last_id, total_synced, sync_method)
    
    total_time = time.time() - start_time
    if total_time > 0 and total_fetched > 0:
        final_rate = total_fetched / total_time
        logging.info(f"Completed {total_fetched:,} records in {total_time:.1f}s "
                   f"({final_rate:.0f} records/sec average)")
    
    return files, last_id, total_synced

def process_datasets(datasets, conn, limit_per_page=3000, max_pages=None, max_workers=None):
    """Process datasets with smart update strategy"""
    dataset_files = {}
    tracker = UpdateTracker(conn)
    
    # Separate current and historical datasets
    current_datasets = {k: v for k, v in datasets.items() if v[1] == 'current'}
    historical_datasets = {k: v for k, v in datasets.items() if v[1] == 'historical'}
    
    # Process current tables (full refresh)
    if current_datasets:
        logging.info("=" * 60)
        logging.info("PROCESSING CURRENT TABLES (Full Refresh)")
        logging.info("=" * 60)
        
        for ds_id, (name, period) in tqdm(current_datasets.items(), 
                                         desc="Current tables"):
            jsons, last_id, total = fetch_data(
                ds_id, name, period, tracker, limit_per_page, max_pages, max_workers
            )
            if jsons:
                dataset_files[f"{name}_{period}"] = jsons
    
    # Process historical tables (incremental update)
    if historical_datasets:
        logging.info("=" * 60)
        logging.info("PROCESSING HISTORICAL TABLES (Incremental Update)")
        logging.info("=" * 60)
        
        for ds_id, (name, period) in tqdm(historical_datasets.items(), 
                                         desc="Historical tables"):
            jsons, last_id, total = fetch_data(
                ds_id, name, period, tracker, limit_per_page, max_pages, max_workers
            )
            if jsons:
                dataset_files[f"{name}_{period}"] = jsons
    
    return dataset_files

def load_to_database(conn, selected_datasets, dataset_files):
    """Load data into database with appropriate strategy"""
    
    for ds_id, (name, period) in tqdm(selected_datasets.items(), 
                                     desc="Loading into database"):
        tbl = f"{name}_{period}"
        json_files = dataset_files.get(tbl, [])
        if not json_files:
            continue
        
        json_dir = os.path.dirname(json_files[0])
        glob_name = f"{name}_{period}_*.json"
        json_path_glob = os.path.join(json_dir, glob_name)
        
        # For CURRENT tables: DROP and RECREATE
        if period == 'current':
            logging.info(f"Dropping and recreating current table: raw.{tbl}")
            conn.execute(f"DROP TABLE IF EXISTS raw.{tbl};")
            
            conn.execute(f"""
                CREATE TABLE raw.{tbl} AS 
                SELECT 
                    CAST(":id" AS VARCHAR) AS row_id,
                    CAST(":created_at" AS VARCHAR) AS created_at,
                    CAST(":updated_at" AS VARCHAR) AS updated_at,
                    CAST(":version" AS VARCHAR) AS version,
                    * EXCLUDE(":id", ":created_at", ":updated_at", ":version")
                FROM read_json_auto('{json_path_glob}', 
                                    auto_detect=True, 
                                    sample_size=-1, 
                                    maximum_depth=-1, 
                                    union_by_name=True);
            """)
            
            row_count = conn.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
            logging.info(f"  Loaded {row_count:,} records into raw.{tbl}")
        
        # For HISTORICAL tables: Handle schema evolution
        else:
            logging.info(f"Incrementally updating historical table: raw.{tbl}")
            
            # Check if table exists
            table_exists = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'raw' AND table_name = '{tbl}'
            """).fetchone()[0] > 0
            
            if not table_exists:
                # Create new table
                conn.execute(f"""
                    CREATE TABLE raw.{tbl} AS 
                    SELECT 
                        CAST(":id" AS VARCHAR) AS row_id,
                        CAST(":created_at" AS VARCHAR) AS created_at,
                        CAST(":updated_at" AS VARCHAR) AS updated_at,
                        CAST(":version" AS VARCHAR) AS version,
                        * EXCLUDE(":id", ":created_at", ":updated_at", ":version")
                    FROM read_json_auto('{json_path_glob}', 
                                        auto_detect=True, 
                                        sample_size=-1, 
                                        maximum_depth=-1, 
                                        union_by_name=True);
                """)
                new_count = conn.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
                logging.info(f"  Created table with {new_count:,} records")
            else:
                # Get existing count
                existing_count = conn.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
                
                # Create temp table with new data
                temp_table = f"temp_{tbl}_new"
                conn.execute(f"DROP TABLE IF EXISTS {temp_table};")
                
                conn.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} AS 
                    SELECT 
                        CAST(":id" AS VARCHAR) AS row_id,
                        CAST(":created_at" AS VARCHAR) AS created_at,
                        CAST(":updated_at" AS VARCHAR) AS updated_at,
                        CAST(":version" AS VARCHAR) AS version,
                        * EXCLUDE(":id", ":created_at", ":updated_at", ":version")
                    FROM read_json_auto('{json_path_glob}', 
                                        auto_detect=True, 
                                        sample_size=-1, 
                                        maximum_depth=-1, 
                                        union_by_name=True);
                """)
                
                # Insert from temp table (handles column mismatches)
                try:
                    conn.execute(f"""
                        INSERT INTO raw.{tbl}
                        SELECT * FROM {temp_table} new_data
                        WHERE NOT EXISTS (
                            SELECT 1 FROM raw.{tbl} existing
                            WHERE existing.row_id = new_data.row_id
                        );
                    """)
                except Exception as e:
                    if "has" in str(e) and "columns but" in str(e):
                        logging.warning(f"Schema mismatch detected for {tbl}. Rebuilding table...")
                        
                        # Save existing data
                        conn.execute(f"CREATE TEMPORARY TABLE {tbl}_backup AS SELECT * FROM raw.{tbl};")
                        
                        # Drop and recreate with union of both schemas
                        conn.execute(f"DROP TABLE raw.{tbl};")
                        conn.execute(f"""
                            CREATE TABLE raw.{tbl} AS 
                            SELECT * FROM {tbl}_backup
                            UNION ALL BY NAME
                            SELECT * FROM {temp_table}
                            WHERE NOT EXISTS (
                                SELECT 1 FROM {tbl}_backup b 
                                WHERE b.row_id = {temp_table}.row_id
                            );
                        """)
                        
                        conn.execute(f"DROP TABLE {tbl}_backup;")
                        logging.info(f"  Rebuilt table with merged schema")
                    else:
                        raise e
                
                # Clean up temp table
                conn.execute(f"DROP TABLE IF EXISTS {temp_table};")
                
                # Report what was added
                new_count = conn.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
                added = new_count - existing_count
                logging.info(f"  Added {added:,} new records to raw.{tbl} (total: {new_count:,})")

def generate_summary_report(conn):
    """Generate a summary report of the database status"""
    logging.info("=" * 60)
    logging.info("DATABASE SUMMARY REPORT")
    logging.info("=" * 60)
    
    # Get all tables in raw schema
    tables = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'raw'
        ORDER BY table_name
    """).fetchall()
    
    total_records = 0
    for table in tables:
        table_name = table[0]
        count = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
        total_records += count
        
        # Try to get date range if date columns exist
        try:
            date_info = conn.execute(f"""
                SELECT 
                    MIN(TRY_CAST(created_at AS DATE)) as earliest,
                    MAX(TRY_CAST(created_at AS DATE)) as latest
                FROM raw.{table_name}
                WHERE created_at IS NOT NULL
            """).fetchone()
            
            if date_info[0]:
                logging.info(f"  {table_name}: {count:,} records ({date_info[0]} to {date_info[1]})")
            else:
                logging.info(f"  {table_name}: {count:,} records")
        except:
            logging.info(f"  {table_name}: {count:,} records")
    
    logging.info(f"\nTotal records across all tables: {total_records:,}")
    
    # Show sync status
    logging.info("\nSync Status:")
    sync_status = conn.execute("""
        SELECT 
            table_name,
            sync_method,
            last_sync_time,
            total_records_synced
        FROM metadata.sync_status
        ORDER BY last_sync_time DESC
    """).fetchall()
    
    for status in sync_status:
        logging.info(f"  {status[0]}: {status[1]} sync at {status[2]} ({status[3]:,} total records)")

def filter_datasets(all_datasets, ds_type='all', time_period='all'):
    """Filter datasets by type and time period"""
    filtered = {}
    for ds_id, (name, period) in all_datasets.items():
        if ds_type != 'all' and ds_type.lower() not in name.lower():
            continue
        if time_period != 'all' and time_period.lower() != period.lower():
            continue
        filtered[ds_id] = (name, period)
    return filtered

def main():
    parser = argparse.ArgumentParser(
        description='Optimized TX Workers Comp data fetcher with differential sync')
    parser.add_argument('--dataset', type=str,
                        choices=['professional', 'institutional', 'pharmacy', 'all'], 
                        default='all',
                        help='Dataset type to download')
    parser.add_argument('--time_period', type=str,
                        choices=['current', 'historical', 'all'],
                        default='all',
                        help='Time period to download')
    parser.add_argument('--page_size', type=int, default=3000,
                        help='Records per page (default: 3000)')
    parser.add_argument('--max_pages', type=int, default=None,
                        help='Maximum pages to fetch per dataset')
    parser.add_argument('--max_workers', type=int, default=None,
                        help='Maximum concurrent workers (default: auto-scales 3-4)')
    parser.add_argument('--force_full', action='store_true',
                        help='Force full refresh for all tables (ignore incremental)')
    parser.add_argument('--report_only', action='store_true',
                        help='Only show database report without downloading')

    args = parser.parse_args()
    
    # Connect to database
    conn = duckdb.connect('tx_workers_comp.db')
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    
    # If report only, just show summary and exit
    if args.report_only:
        generate_summary_report(conn)
        conn.close()
        return
    
    # Define all datasets
    datasets = {
        # institutional
        "cmu9-4z9n": ("institutional_header", "current"),
        "tuuc-49gz": ("institutional_detail", "current"),
        "936m-z8wh": ("institutional_header", "historical"),
        "trqb-ssnn": ("institutional_detail", "historical"),

        # professional
        "pvi6-huub": ("professional_header", "current"),
        "c7b4-gune": ("professional_detail", "current"),
        "gh5j-28a7": ("professional_header", "historical"),
        "7au4-j7bg": ("professional_detail", "historical"),

        # pharmacy
        "mzi7-5ajk": ("pharmacy_header", "current"),
        "28cv-4t5q": ("pharmacy_detail", "current"),
        "jkpg-wdht": ("pharmacy_header", "historical"),
        "cmkf-edrp": ("pharmacy_detail", "historical")
    }

    # Filter datasets
    selected_datasets = filter_datasets(
        all_datasets=datasets,
        ds_type=args.dataset,
        time_period=args.time_period
    )

    if args.force_full:
        logging.warning("Force full refresh enabled - all tables will be completely refreshed")
        # Clear sync status to force full refresh
        tracker = UpdateTracker(conn)
        conn.execute("DELETE FROM metadata.sync_status")
    
    logging.info(f"Starting optimized update of {len(selected_datasets)} datasets")
    logging.info(f"Strategy: Current tables (full refresh), Historical tables (incremental)")
    logging.info(f"Page size: {args.page_size} records")
    if args.max_pages:
        logging.info(f"Max pages per dataset: {args.max_pages}")
    
    # Download with smart strategy
    dataset_files = process_datasets(
        selected_datasets,
        conn,
        limit_per_page=args.page_size,
        max_pages=args.max_pages,
        max_workers=args.max_workers
    )
    
    # Load into database with appropriate strategy
    load_to_database(conn, selected_datasets, dataset_files)
    
    # Generate summary report
    generate_summary_report(conn)
    
    # Cleanup temporary files
    if os.path.exists(BASE_DIR):
        shutil.rmtree(BASE_DIR)
        logging.info(f"Cleaned up temporary files in {BASE_DIR}")
    
    conn.close()
    logging.info("Optimized data fetch completed successfully!")

if __name__ == "__main__":
    main()