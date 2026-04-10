import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import dlt
from dlt.sources.helpers.requests import Session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

_WORKER_COUNT = 6


def _create_session():
    """Create a dlt Session with retry/backoff for RxNav API."""
    return Session(timeout=30)


def fetch_json(url, session):
    """Fetch JSON from a URL using the dlt session."""
    resp = session.get(url)
    resp.raise_for_status()
    return resp.json()


_thread_local = threading.local()


def _get_thread_session():
    """Get or create a thread-local dlt Session (requests.Session is not thread-safe)."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = _create_session()
    return _thread_local.session


def process_concept(class_base_url, concept):
    """Look up RxClass classifications for a single concept."""
    session = _get_thread_session()
    url = class_base_url + concept['rxcui']
    try:
        cur_json = fetch_json(url, session)
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
    except Exception as e:
        status = getattr(getattr(e, 'response', None), 'status_code', None)
        if status and status != 404:
            logging.warning(f"HTTP {status} for rxcui {concept['rxcui']}: {e}")
        return None


@dlt.resource(write_disposition="replace", name="rxclass")
def rxclass_resource():
    """Yield deduplicated RxClass drug classification records."""
    session = _create_session()
    base_url = "https://rxnav.nlm.nih.gov/REST/allconcepts.json?tty=SBD+SCD+GPCK+BPCK"
    class_base_url = "https://rxnav.nlm.nih.gov/REST/rxclass/class/byRxcui.json?rxcui="

    cui_json = fetch_json(base_url, session)
    concepts = cui_json['minConceptGroup']['minConcept']

    seen = set()
    failed_count = 0
    processed_count = 0

    logging.info(f"Fetching RxClass for {len(concepts):,} concepts with {_WORKER_COUNT} workers")

    with ThreadPoolExecutor(max_workers=_WORKER_COUNT) as executor:
        futures = {
            executor.submit(process_concept, class_base_url, c): c
            for c in concepts
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing concepts"):
            results = future.result()
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
    pipeline = dlt.pipeline(
        pipeline_name="rxclass",
        destination="duckdb",
        dataset_name="reference_data",
    )
    load_info = pipeline.run(rxclass_resource())
    print(load_info)
    print("Table 'reference_data.rxclass' loaded successfully.")


if __name__ == "__main__":
    main()
