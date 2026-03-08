import os
import json
import time
import logging
from urllib.request import urlopen
from urllib.error import HTTPError
from tqdm import tqdm
import dlt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_json(url):
    """Fetch JSON from a URL."""
    with urlopen(url) as response:
        return json.loads(response.read())


def process_concept(class_base_url, concept, max_retries=3, initial_delay=1):
    """Look up RxClass classifications for a single concept with retries."""
    url = class_base_url + concept['rxcui']
    for attempt in range(max_retries):
        try:
            cur_json = fetch_json(url)
            class_data = cur_json['rxclassDrugInfoList']['rxclassDrugInfo']
            return [
                dict(
                    concept,
                    rela=k.get('rela', ''),
                    rela_source=k.get('relaSource', ''),
                    **k['rxclassMinConceptItem'],
                )
                for k in class_data
            ]
        except HTTPError as e:
            if e.code == 429:
                delay = initial_delay * (2 ** attempt)
                logging.warning(
                    f"Rate limit hit for {concept['rxcui']}. "
                    f"Retrying in {delay} seconds... (Attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(delay)
            else:
                return None
        except Exception:
            return None
    logging.error(f"Max retries reached for {concept['rxcui']}. Skipping concept.")
    return None


@dlt.resource(write_disposition="replace", name="rxclass")
def rxclass_resource():
    """Yield deduplicated RxClass drug classification records."""
    base_url = "https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty=SBD+SCD+GPCK+BPCK"
    class_base_url = "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui="

    cui_json = fetch_json(base_url)
    concepts = cui_json['minConceptGroup']['minConcept']

    seen = set()
    failed_count = 0
    processed_count = 0

    for concept in tqdm(concepts, desc="Processing concepts"):
        results = process_concept(class_base_url, concept)
        if results is None:
            failed_count += 1
            continue
        processed_count += 1
        for record in results:
            record_key = tuple(sorted(record.items()))
            if record_key not in seen:
                seen.add(record_key)
                yield record

    total_concepts = len(concepts)
    logging.info(
        f"Processing complete. Total concepts: {total_concepts}, "
        f"Processed: {processed_count}, Failed: {failed_count}"
    )


def main():
    db_path = os.environ.get(
        'TXWC_DB_PATH',
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tx_workers_comp.db'),
    )
    pipeline = dlt.pipeline(
        pipeline_name="rxclass",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="reference_data",
    )
    load_info = pipeline.run(rxclass_resource())
    print(load_info)
    print("Table 'reference_data.rxclass' loaded successfully.")


if __name__ == "__main__":
    main()
