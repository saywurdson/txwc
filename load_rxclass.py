import os
from urllib.request import urlopen
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from tqdm import tqdm
import time
from urllib.error import HTTPError
import threading
import logging
import duckdb

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self, f):
        def wrapped(*args, **kwargs):
            with self.lock:
                now = time.time()
                # Keep only calls within the defined period
                self.calls = [c for c in self.calls if now - c < self.period]
                if len(self.calls) >= self.max_calls:
                    time.sleep(self.period - (now - self.calls[0]))
                self.calls.append(time.time())
            return f(*args, **kwargs)
        return wrapped

# Apply rate limiter to fetch_json to enforce API limits (20 calls per second)
@RateLimiter(max_calls=20, period=1)
def fetch_json(url):
    with urlopen(url) as response:
        return json.loads(response.read())

def process_concept(class_base_url, concept, max_retries=3, initial_delay=1):
    url = class_base_url + concept['rxcui']
    for attempt in range(max_retries):
        try:
            cur_json = fetch_json(url)
            # Access the list of drug info records
            class_data = cur_json['rxclassDrugInfoList']['rxclassDrugInfo']
            # Merge the original concept with:
            # - 'rela' and 'rela_source' (from the current record), and
            # - all classification details from 'rxclassMinConceptItem' as top-level columns.
            return [
                dict(
                    concept,
                    rela=k.get('rela', ''),
                    rela_source=k.get('relaSource', ''),
                    **k['rxclassMinConceptItem']
                )
                for k in class_data
            ]
        except HTTPError as e:
            if e.code == 429:
                delay = initial_delay * (2 ** attempt)  # Exponential backoff for 429 errors
                logging.warning(f"Rate limit hit for {concept['rxcui']}. Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(delay)
            else:
                # For non-429 HTTP errors, log minimal info and skip the concept.
                # logging.error(f"HTTP error {e.code} for {concept['rxcui']}. Skipping concept.")
                return None
        except Exception as e:
            # For any other errors, log minimal info and skip the concept.
            # logging.error(f"Error processing {concept['rxcui']}: {str(e)}. Skipping concept.")
            return None
    logging.error(f"Max retries reached for {concept['rxcui']}. Skipping concept.")
    return None

def main():
    # Define the base URLs
    base_url = "https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty=SBD+SCD+GPCK+BPCK"
    class_base_url = "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui="

    # Fetch the list of concepts
    cui_json = fetch_json(base_url)
    concepts = cui_json['minConceptGroup']['minConcept']

    # Process each concept concurrently
    with ThreadPoolExecutor(max_workers=20) as executor:
        process_func = partial(process_concept, class_base_url)
        results = list(tqdm(executor.map(process_func, concepts), total=len(concepts), desc="Processing concepts"))

    # Flatten the results and filter out failed concepts
    successful_results = [item for sublist in results if sublist is not None for item in sublist]
    failed_concepts = [concept for concept, result in zip(concepts, results) if result is None]

    full_df = pd.DataFrame(successful_results).drop_duplicates().reset_index(drop=True)

    # Log and print a summary of the processing
    total_concepts = len(concepts)
    processed_concepts = len(set(full_df['rxcui']))
    failed_count = len(failed_concepts)
    logging.info(f"Processing complete. Total concepts: {total_concepts}, Processed: {processed_concepts}, Failed: {failed_count}")

    # Connect to DuckDB database and create table in the specified schema
    db_path = os.environ.get(
        'TXWC_DB_PATH',
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tx_workers_comp.db')
    )
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS reference_data")
    conn.register('temp_df', full_df)
    conn.execute("CREATE OR REPLACE TABLE reference_data.rxclass AS SELECT * FROM temp_df")
    print("Table 'reference_data.rxclass' created successfully in /workspaces/txwc/tx_workers_comp.db")

if __name__ == "__main__":
    main()