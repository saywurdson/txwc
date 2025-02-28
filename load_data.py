import os
import requests
import math
import time
import argparse
import logging
import duckdb
import shutil
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

load_dotenv()

APP_TOKEN = os.getenv('APPLICATION_TOKEN')
BASE_DIR = './csv_data'
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

def get_total_records(dataset_identifier):
    try:
        logging.info(f"Getting total records for dataset '{dataset_identifier}'.")
        url = f"https://data.texas.gov/resource/{dataset_identifier}.json"
        headers = {"X-App-Token": APP_TOKEN}
        params = {"$select": "COUNT(*) as count"}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return int(response.json()[0]['count'])
    except Exception as e:
        logging.error(f"Error getting total records for '{dataset_identifier}': {e}")
        return 0

def fetch_batch(dataset_identifier, name, time_period, order_by, limit, offset):
    retries = 0
    max_retries = 5
    base_sleep = 1
    url = f"https://data.texas.gov/resource/{dataset_identifier}.csv"
    headers = {"X-App-Token": APP_TOKEN}
    params = {
        "$order": order_by,
        "$limit": limit,
        "$offset": offset,
        "$select": ":*, *"
    }

    while retries <= max_retries:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            dataset_dir = os.path.join(BASE_DIR, dataset_identifier)
            os.makedirs(dataset_dir, exist_ok=True)
            file_name = f"{name}_{time_period}_{offset}.csv"
            file_path = os.path.join(dataset_dir, file_name)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            return file_path
        except Exception as e:
            retries += 1
            sleep_time = base_sleep * (2 ** retries)
            logging.warning(
                f"Error fetching data from '{dataset_identifier}' at offset {offset}: {e}. "
                f"Retrying in {sleep_time} seconds..."
            )
            time.sleep(sleep_time)

    logging.error(f"Maximum retries exceeded for '{dataset_identifier}' at offset {offset}.")
    return None

def fetch_data(dataset_identifier, name, time_period,
               order_by=":id",
               limit_per_page=5000,
               max_pages=None,
               offset=0):
    total_records = get_total_records(dataset_identifier)
    if total_records == 0:
        logging.warning(f"No records found for dataset '{dataset_identifier}'. Skipping.")
        return []

    total_pages = math.ceil((total_records - offset) / limit_per_page)
    if max_pages is not None:
        total_pages = min(total_pages, max_pages)
        logging.info(f"Limiting to {max_pages} pages for dataset '{dataset_identifier}'.")

    offsets = [i * limit_per_page + offset for i in range(total_pages)]
    files = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_offset = {
            executor.submit(fetch_batch, dataset_identifier, name, time_period, order_by, limit_per_page, off): off 
            for off in offsets
        }
        for future in tqdm(as_completed(future_to_offset), total=len(future_to_offset),
                           desc=f"Fetching '{dataset_identifier}'"):
            off = future_to_offset[future]
            try:
                file_path = future.result()
                if file_path:
                    files.append(file_path)
                else:
                    logging.warning(f"No data returned for '{dataset_identifier}' at offset {off}.")
            except Exception as e:
                logging.error(f"Failed to fetch data for '{dataset_identifier}' at offset {off}: {e}")

    return files

def process_datasets(datasets, order_by=":id",
                     limit_per_page=5000, max_pages=None, offset=0):
    dataset_files = {}
    for ds_id, (name, period) in tqdm(datasets.items(), total=len(datasets), desc="Downloading datasets"):
        csvs = fetch_data(ds_id, name, period, order_by, limit_per_page, max_pages, offset)
        if csvs:
            dataset_files[f"{name}_{period}"] = csvs
    return dataset_files

def filter_datasets(all_datasets, ds_type='all', time_period='all'):
    filtered = {}
    for ds_id, (name, period) in all_datasets.items():
        # Filter by dataset type
        if ds_type != 'all' and ds_type.lower() not in name.lower():
            continue
        # Filter by time period
        if time_period != 'all' and time_period.lower() != period.lower():
            continue
        filtered[ds_id] = (name, period)
    return filtered

def main():
    parser = argparse.ArgumentParser(description='Process TX Workers Comp data with pagination.')
    parser.add_argument('--max_pages', type=int, default=None, 
                       help='Maximum pages to fetch. If omitted, fetch all pages.')
    parser.add_argument('--offset', type=int, default=0,
                       help='Starting offset for data fetch.')
    parser.add_argument('--dataset', type=str,
                        choices=['professional', 'institutional', 'pharmacy', 'all'], 
                        default='all',
                        help='Which dataset to download: professional, institutional, pharmacy, or all.')
    parser.add_argument('--time_period', type=str,
                        choices=['current', 'historical', 'all'],
                        default='all',
                        help='Which period to download: current, historical, or all (default).')

    args = parser.parse_args()

    # Define all datasets (name, period)
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

    # Filter datasets based on BOTH --dataset and --time_period
    selected_datasets = filter_datasets(
        all_datasets=datasets,
        ds_type=args.dataset,
        time_period=args.time_period
    )

    # Download and process the filtered datasets
    dataset_files = process_datasets(
        selected_datasets,
        max_pages=args.max_pages,
        offset=args.offset
    )

    conn = duckdb.connect('tx_workers_comp.db')
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # Create table if not exists, then bulk-insert from a glob pattern
    for ds_id, (name, period) in selected_datasets.items():
        tbl = f"{name}_{period}"
        csv_files = dataset_files.get(tbl, [])
        if not csv_files:
            continue

        # 1) Create an empty table if it doesn't exist (use the first CSV to define schema)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS raw.{tbl} AS 
            SELECT 
                CAST(":id" AS VARCHAR) AS row_id,
                CAST(":created_at" AS VARCHAR) AS created_at,
                CAST(":updated_at" AS VARCHAR) AS updated_at,
                CAST(":version" AS VARCHAR) AS version,
                * EXCLUDE(":id", ":created_at", ":updated_at", ":version")
            FROM read_csv_auto('{csv_files[0]}', ALL_VARCHAR=TRUE)
            WHERE 1=0;
        """)

        # 2) Build a glob pattern for all CSVs in the dataset
        csv_dir = os.path.dirname(csv_files[0])
        glob_name = f"{name}_{period}_*.csv"
        csv_path_glob = os.path.join(csv_dir, glob_name)

        # 3) Insert only new rows from all matching CSV files at once
        conn.execute(f"""
            INSERT INTO raw.{tbl}
            SELECT 
                CAST(":id" AS VARCHAR) AS row_id,
                CAST(":created_at" AS VARCHAR) AS created_at,
                CAST(":updated_at" AS VARCHAR) AS updated_at,
                CAST(":version" AS VARCHAR) AS version,
                * EXCLUDE(":id", ":created_at", ":updated_at", ":version")
            FROM read_csv_auto('{csv_path_glob}', ALL_VARCHAR=TRUE) AS multi_file_insert
            WHERE NOT EXISTS (
                SELECT 1 FROM raw.{tbl} old_data
                WHERE old_data.row_id = multi_file_insert.":id"
            );
        """)

    conn.close()

    if os.path.exists(BASE_DIR):
        shutil.rmtree(BASE_DIR)
        logging.info(f"Cleaned up downloaded files in {BASE_DIR}")

    logging.info("Done.")

if __name__ == "__main__":
    main()