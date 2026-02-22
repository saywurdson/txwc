import os
import requests
import time
import argparse
import logging
import duckdb
import shutil
import json
import random
from datetime import datetime
from dotenv import load_dotenv
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
               tracker, limit_per_page=3000, max_pages=None):
    """
    Smart fetch that does full refresh for current, incremental for historical.
    Uses serial ID-based pagination (each batch depends on the last_id of the
    previous batch, so batches within a dataset cannot be parallelized).
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

def process_datasets(datasets, conn, limit_per_page=3000, max_pages=None):
    """Process datasets with smart update strategy.

    Datasets are fetched serially to stay within Socrata API rate limits
    (shared per app token). Within each dataset, ID-based pagination is
    inherently sequential.
    """
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
                ds_id, name, period, tracker, limit_per_page, max_pages
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
                ds_id, name, period, tracker, limit_per_page, max_pages
            )
            if jsons:
                dataset_files[f"{name}_{period}"] = jsons

    return dataset_files

def ensure_full_schema(conn, ds_id, tbl, session):
    """Fetch the full column list from Socrata metadata and add any missing columns."""
    try:
        url = f"https://data.texas.gov/api/views/{ds_id}/columns.json"
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        meta_cols = resp.json()

        # Socrata field names (lowercase, underscored) — skip internal columns
        api_columns = set()
        for col in meta_cols:
            fn = col.get('fieldName', '')
            if fn.startswith(':'):
                continue
            api_columns.add(fn)

        # Columns already in the DuckDB table
        existing = {r[0] for r in conn.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='raw' AND table_name='{tbl}'"
        ).fetchall()}

        missing = api_columns - existing
        if missing:
            for col_name in sorted(missing):
                conn.execute(f'ALTER TABLE raw.{tbl} ADD COLUMN "{col_name}" VARCHAR;')
            logging.info(f"  Added {len(missing)} missing columns to raw.{tbl}: "
                         f"{', '.join(sorted(missing))}")
    except Exception as e:
        logging.warning(f"Could not pad schema for raw.{tbl}: {e}")


def load_to_database(conn, selected_datasets, dataset_files, force_replace=False):
    """Load data into database with appropriate strategy"""
    session = create_session()

    for ds_id, (name, period) in tqdm(selected_datasets.items(),
                                     desc="Loading into database"):
        tbl = f"{name}_{period}"
        json_files = dataset_files.get(tbl, [])
        if not json_files:
            continue

        json_dir = os.path.dirname(json_files[0])
        glob_name = f"{name}_{period}_*.json"
        json_path_glob = os.path.join(json_dir, glob_name)

        # For CURRENT tables (or force_replace): DROP and RECREATE
        if period == 'current' or force_replace:
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
                                    sample_size=20,
                                    maximum_depth=-1,
                                    union_by_name=True);
            """)

            row_count = conn.execute(f"SELECT COUNT(*) FROM raw.{tbl}").fetchone()[0]
            logging.info(f"  Loaded {row_count:,} records into raw.{tbl}")
            ensure_full_schema(conn, ds_id, tbl, session)

        # For HISTORICAL tables: Handle schema evolution (sample_size=-1 scans all files for full schema)
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
                ensure_full_schema(conn, ds_id, tbl, session)
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
                ensure_full_schema(conn, ds_id, tbl, session)

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
        except Exception:
            logging.info(f"  {table_name}: {count:,} records")
    
    logging.info(f"\nTotal records across all tables: {total_records:,}")
    
    # Show sync status (may not exist in sampled mode)
    try:
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
    except duckdb.CatalogException:
        pass

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

def build_dataset_index(datasets):
    """Reorganize datasets dict into {claim_type: {period: {role: ds_id}}}

    Example output:
        {'professional': {'current': {'header': 'pvi6-huub', 'detail': 'c7b4-gune'}, ...}}
    """
    index = {}
    for ds_id, (name, period) in datasets.items():
        # name is like "professional_header" or "pharmacy_detail"
        parts = name.rsplit('_', 1)
        if len(parts) != 2:
            continue
        claim_type, role = parts  # e.g. ("professional", "header")
        index.setdefault(claim_type, {}).setdefault(period, {})[role] = ds_id
    return index


def build_where_in(column, values, batch_size=400):
    """Split values into batched WHERE IN clauses.

    Returns a list of SoQL $where strings like:
        "patient_account_number IN ('A','B','C')"
    Each clause contains at most *batch_size* values to stay under URL limits.
    """
    clauses = []
    values = list(values)
    for i in range(0, len(values), batch_size):
        batch = values[i:i + batch_size]
        escaped = [v.replace("'", "''") for v in batch]
        in_list = ",".join(f"'{v}'" for v in escaped)
        clauses.append(f"{column} IN ({in_list})")
    return clauses


def discover_patients(datasets, session, rate_limiter):
    """Discover patient_account_numbers from all header datasets.

    Uses SoQL aggregation to get distinct patients (non-null only) from each
    header table in the selected datasets. Returns a set of patient_account_numbers.
    """
    index = build_dataset_index(datasets)
    all_patients = set()

    for claim_type, periods in index.items():
        for period, roles in periods.items():
            ds_id = roles.get('header')
            if not ds_id:
                continue

            url = f"https://data.texas.gov/resource/{ds_id}.json"
            params = {
                "$select": "patient_account_number",
                "$group": "patient_account_number",
                "$where": "patient_account_number IS NOT NULL",
                "$limit": 50000,
            }

            rate_limiter.wait()
            start = time.time()
            try:
                resp = session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                rate_limiter.record_response(time.time() - start, resp.status_code)
                rows = resp.json()
                pans = {r['patient_account_number'] for r in rows
                        if r.get('patient_account_number')}
                logging.info(f"  {claim_type} {period} header: {len(pans):,} distinct patients")
                all_patients.update(pans)
            except Exception as e:
                logging.warning(f"Failed to discover patients from {ds_id}: {e}")

    return all_patients


def discover_complex_patients(datasets, session, rate_limiter):
    """Discover patients with bill counts per claim type for complexity scoring.

    Queries all header tables in the selected datasets (non-null patients only).
    Returns dict: {patient_account_number: {claim_type: bill_count, ...}}
    """
    index = build_dataset_index(datasets)
    patient_scores = {}  # pan -> {claim_type: count}

    for claim_type, periods in index.items():
        for period, roles in periods.items():
            ds_id = roles.get('header')
            if not ds_id:
                continue

            url = f"https://data.texas.gov/resource/{ds_id}.json"
            params = {
                "$select": "patient_account_number, count(*) as bill_count",
                "$group": "patient_account_number",
                "$where": "patient_account_number IS NOT NULL",
                "$order": "bill_count DESC",
                "$limit": 50000,
            }

            rate_limiter.wait()
            start = time.time()
            try:
                resp = session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                rate_limiter.record_response(time.time() - start, resp.status_code)
                rows = resp.json()
                for r in rows:
                    pan = r.get('patient_account_number')
                    if not pan:
                        continue
                    count = int(r.get('bill_count', 0))
                    patient_scores.setdefault(pan, {})
                    patient_scores[pan][claim_type] = (
                        patient_scores[pan].get(claim_type, 0) + count
                    )
                logging.info(f"  {claim_type} {period} header: {len(rows):,} patients with counts")
            except Exception as e:
                logging.warning(f"Failed to discover complex patients from {ds_id}: {e}")

    return patient_scores


def select_patients(all_patients, n):
    """Randomly sample N patients from the pool."""
    pool = list(all_patients)
    if len(pool) <= n:
        logging.info(f"Patient pool ({len(pool)}) <= requested sample ({n}), using all")
        return set(pool)
    selected = set(random.sample(pool, n))
    logging.info(f"Randomly selected {len(selected)} patients from pool of {len(pool):,}")
    return selected


def select_complex_patients(patient_scores, n):
    """Select top N patients by complexity score.

    Score = total_bill_count * number_of_distinct_claim_types
    """
    scored = []
    for pan, type_counts in patient_scores.items():
        total_bills = sum(type_counts.values())
        num_types = len(type_counts)
        score = total_bills * num_types
        scored.append((score, total_bills, num_types, pan))

    scored.sort(reverse=True)
    selected = {pan for _, _, _, pan in scored[:n]}

    if scored[:n]:
        top = scored[0]
        bottom = scored[min(n - 1, len(scored) - 1)]
        logging.info(
            f"Selected {len(selected)} complex patients "
            f"(top score: {top[0]}, bills={top[1]}, types={top[2]}; "
            f"cutoff score: {bottom[0]}, bills={bottom[1]}, types={bottom[2]})"
        )
    return selected


def fetch_filtered(ds_id, name, period, where_clause, session, rate_limiter,
                   limit_per_page=3000):
    """Fetch all records from a dataset matching a $where clause.

    Uses offset-based pagination. Returns list of saved JSON file paths.
    """
    url = f"https://data.texas.gov/resource/{ds_id}.json"
    files = []
    offset = 0
    batch_num = 0

    while True:
        params = {
            "$where": where_clause,
            "$limit": limit_per_page,
            "$offset": offset,
            "$select": ":*, *",
            "$order": ":id",
        }

        rate_limiter.wait()
        start = time.time()
        retries = 0
        max_retries = 5
        data = None

        while retries <= max_retries:
            try:
                resp = session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                rate_limiter.record_response(time.time() - start, resp.status_code)
                data = resp.json()
                break
            except requests.exceptions.RequestException as e:
                retries += 1
                if retries > max_retries:
                    logging.error(f"Max retries for {ds_id} batch {batch_num}: {e}")
                    return files
                sleep_time = 1 * (2 ** retries)
                logging.warning(f"Retry {retries} for {ds_id} batch {batch_num}: {e}")
                time.sleep(sleep_time)

        if not data:
            break

        dataset_dir = os.path.join(BASE_DIR, ds_id)
        os.makedirs(dataset_dir, exist_ok=True)
        file_name = f"{name}_{period}_batch{batch_num:06d}.json"
        file_path = os.path.join(dataset_dir, file_name)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)

        files.append(file_path)
        batch_num += 1
        offset += limit_per_page

        if len(data) < limit_per_page:
            break

    return files


def process_datasets_sampled(datasets, conn, n_patients, complex_mode=False,
                              page_size=3000):
    """Orchestrator for patient-cohort sampling mode.

    1. Discover patients from header tables
    2. Select N patients (random or complex)
    3. Fetch headers filtered by patient_account_number
    4. Extract bill_ids from fetched headers
    5. Fetch details filtered by bill_id
    6. Load all into DuckDB
    """
    session = create_session()
    rate_limiter = AdaptiveRateLimiter()
    index = build_dataset_index(datasets)

    # --- Step 1 & 2: Discover and select patients ---
    logging.info("=" * 60)
    if complex_mode:
        logging.info(f"DISCOVERING COMPLEX PATIENTS (target: {n_patients})")
    else:
        logging.info(f"DISCOVERING PATIENTS (target: {n_patients})")
    logging.info("=" * 60)

    if complex_mode:
        patient_scores = discover_complex_patients(datasets, session, rate_limiter)
        selected_pans = select_complex_patients(patient_scores, n_patients)
    else:
        all_patients = discover_patients(datasets, session, rate_limiter)
        selected_pans = select_patients(all_patients, n_patients)

    if not selected_pans:
        logging.error("No patients discovered — nothing to fetch.")
        return {}

    logging.info(f"Selected {len(selected_pans)} patients for cohort")

    # --- Step 3: Fetch headers filtered by patient_account_number ---
    logging.info("=" * 60)
    logging.info("FETCHING HEADER RECORDS FOR SELECTED PATIENTS")
    logging.info("=" * 60)

    dataset_files = {}
    all_bill_ids = set()
    pan_clauses = build_where_in("patient_account_number", selected_pans)

    for claim_type, periods in index.items():
        for period, roles in periods.items():
            ds_id = roles.get('header')
            if not ds_id:
                continue

            name = f"{claim_type}_header"
            tbl = f"{name}_{period}"
            logging.info(f"Fetching {tbl} ({len(pan_clauses)} batches)...")

            all_files = []
            for clause in pan_clauses:
                batch_files = fetch_filtered(
                    ds_id, name, period, clause, session, rate_limiter, page_size
                )
                all_files.extend(batch_files)

            if all_files:
                dataset_files[tbl] = all_files
                # Extract bill_ids from fetched header JSON files
                for fp in all_files:
                    with open(fp, 'r') as f:
                        records = json.load(f)
                    for rec in records:
                        bid = rec.get('bill_id')
                        if bid:
                            all_bill_ids.add(bid)

                logging.info(f"  {tbl}: {len(all_files)} files fetched")

    logging.info(f"Extracted {len(all_bill_ids):,} bill_ids from headers")

    # --- Step 4: Fetch details filtered by bill_id ---
    if all_bill_ids:
        logging.info("=" * 60)
        logging.info("FETCHING DETAIL RECORDS FOR MATCHING BILL_IDS")
        logging.info("=" * 60)

        bid_clauses = build_where_in("bill_id", all_bill_ids)

        for claim_type, periods in index.items():
            for period, roles in periods.items():
                ds_id = roles.get('detail')
                if not ds_id:
                    continue

                name = f"{claim_type}_detail"
                tbl = f"{name}_{period}"
                logging.info(f"Fetching {tbl} ({len(bid_clauses)} batches)...")

                all_files = []
                for clause in bid_clauses:
                    batch_files = fetch_filtered(
                        ds_id, name, period, clause, session, rate_limiter, page_size
                    )
                    all_files.extend(batch_files)

                if all_files:
                    dataset_files[tbl] = all_files
                    logging.info(f"  {tbl}: {len(all_files)} files fetched")

    # --- Step 5: Drop stale raw tables and load into DuckDB ---
    # Remove any raw tables from previous runs that aren't part of this load.
    # This prevents dbt from querying leftover tables with incompatible schemas.
    expected_tables = {f"{name}_{period}" for _, (name, period) in datasets.items()}
    existing_tables = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall()}
    stale = existing_tables - expected_tables
    if stale:
        for tbl in sorted(stale):
            conn.execute(f"DROP TABLE IF EXISTS raw.{tbl};")
        logging.info(f"Dropped {len(stale)} stale raw tables: {', '.join(sorted(stale))}")

    logging.info("=" * 60)
    logging.info("LOADING SAMPLED DATA INTO DATABASE")
    logging.info("=" * 60)

    load_to_database(conn, datasets, dataset_files, force_replace=True)

    return dataset_files


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
    parser.add_argument('--force_full', action='store_true',
                        help='Force full refresh for all tables (ignore incremental)')
    parser.add_argument('--report_only', action='store_true',
                        help='Only show database report without downloading')
    parser.add_argument('--sample_patients', type=int, default=None,
                        help='Fetch only N complete patients (all headers + details)')
    parser.add_argument('--complex', action='store_true',
                        help='With --sample_patients, pick patients with most claims across types')

    args = parser.parse_args()
    
    # Connect to database
    conn = duckdb.connect('/workspaces/txwc/tx_workers_comp.db')
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

    # --- Patient-cohort sampling mode ---
    if args.sample_patients:
        logging.info(f"PATIENT-COHORT SAMPLING MODE: {args.sample_patients} patients"
                     f"{' (complex)' if args.complex else ''}")
        logging.info(f"Datasets: {len(selected_datasets)}")

        dataset_files = process_datasets_sampled(
            selected_datasets, conn,
            n_patients=args.sample_patients,
            complex_mode=args.complex,
            page_size=args.page_size,
        )

        generate_summary_report(conn)

        if os.path.exists(BASE_DIR):
            shutil.rmtree(BASE_DIR)
            logging.info(f"Cleaned up temporary files in {BASE_DIR}")

        conn.close()
        logging.info("Patient-cohort sampling completed successfully!")
        return

    # --- Standard full/incremental mode ---
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
        max_pages=args.max_pages
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