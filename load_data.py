import os
import argparse
import logging
import duckdb
import random

import dlt
from dlt.sources.helpers.requests import Session
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

APP_TOKEN = os.getenv('APPLICATION_TOKEN')

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

for _name in ("dlt", "alembic"):
    logging.getLogger(_name).setLevel(logging.WARNING)


ALL_DATASETS = {
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
    "cmkf-edrp": ("pharmacy_detail", "historical"),
}


SOCRATA_RENAME = {
    ":id": "row_id",
    ":created_at": "created_at",
    ":updated_at": "updated_at",
    ":version": "version",
}


def _create_session():
    """Create a dlt requests Session with built-in retry/backoff."""
    session = Session(timeout=60)
    session.headers.update({
        "Accept-Encoding": "gzip, deflate",
        "X-App-Token": APP_TOKEN if APP_TOKEN else "",
    })
    return session


def _rename_socrata_cols(row: dict) -> dict:
    """Rename Socrata system columns and coerce values to strings."""
    return {
        SOCRATA_RENAME.get(k, k): str(v) if v is not None else None
        for k, v in row.items()
    }


def _get_socrata_columns(ds_id, session):
    """Fetch column names from Socrata metadata API for a dataset."""
    import requests as _requests
    try:
        url = f"https://data.texas.gov/api/views/{ds_id}/columns.json"
        resp = _requests.get(url, timeout=30)
        resp.raise_for_status()
        return {
            col.get("fieldName", "")
            for col in resp.json()
            if not col.get("fieldName", "").startswith(":")
        }
    except Exception as e:
        logging.warning(f"Could not fetch metadata for {ds_id}: {e}")
        return set()


def _make_text_columns(column_names):
    """Build dlt column hints forcing all columns to text type."""
    renamed = {SOCRATA_RENAME.get(c, c) for c in column_names}
    return {col: {"data_type": "text"} for col in renamed}


def _ensure_full_schema(ds_id, table_name, db_path, session):
    """Add missing Socrata columns to the DuckDB table after load."""
    api_columns = _get_socrata_columns(ds_id, session)
    if not api_columns:
        return

    try:
        conn = duckdb.connect(db_path, read_only=False)
        existing = {
            r[0]
            for r in conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema='raw' AND table_name='{table_name}'"
            ).fetchall()
        }

        missing = api_columns - existing
        if missing:
            for col_name in sorted(missing):
                conn.execute(
                    f'ALTER TABLE raw.{table_name} ADD COLUMN "{col_name}" VARCHAR;'
                )
            logging.info(
                f"  Added {len(missing)} missing columns to raw.{table_name}"
            )
        conn.close()
    except Exception as e:
        logging.warning(f"Could not pad schema for raw.{table_name}: {e}")


def _get_total_records(ds_id, session, where_clause=None):
    """Get total record count for a dataset with optional WHERE clause."""
    url = f"https://data.texas.gov/resource/{ds_id}.json"
    params = {"$select": "COUNT(*) as count"}
    if where_clause:
        params["$where"] = where_clause
    try:
        resp = session.get(url, params=params)
        return int(resp.json()[0]["count"])
    except Exception as e:
        logging.error(f"Error getting total records for '{ds_id}': {e}")
        return 0



def _paginate_socrata(ds_id, session, page_size, max_pages=None,
                      starting_id=None):
    """Yield rows from Socrata using :id-based pagination."""
    last_id = starting_id
    page = 0

    while True:
        if max_pages is not None and page >= max_pages:
            break

        url = f"https://data.texas.gov/resource/{ds_id}.json"
        params = {
            "$order": ":id",
            "$limit": page_size,
            "$select": ":*, *",
        }
        if last_id:
            params["$where"] = f":id > '{last_id}'"

        resp = session.get(url, params=params)
        data = resp.json()

        if not data:
            break

        for row in data:
            yield _rename_socrata_cols(row)

        last_id = data[-1].get(":id")
        page += 1

        if len(data) < page_size:
            break


def _make_resource(ds_id, name, period, session, page_size, max_pages=None,
                   force_full=False):
    """Build a dlt resource for one Socrata dataset."""
    table_name = f"{name}_{period}"
    text_columns = _make_text_columns(_get_socrata_columns(ds_id, session))

    if period == "current" or force_full:
        disposition = "replace"
        merge_key = None
    else:
        disposition = "merge"
        merge_key = "row_id"

    @dlt.resource(
        name=table_name,
        write_disposition=disposition,
        merge_key=merge_key,
        max_table_nesting=0,
        columns=text_columns,
    )
    def socrata_resource():
        total = _get_total_records(ds_id, session)
        if total == 0:
            logging.info(f"No records for {table_name}")
            return

        effective_total = total
        if max_pages is not None:
            effective_total = min(total, max_pages * page_size)

        logging.info(f"{'FULL' if disposition == 'replace' else 'MERGE'} "
                     f"- {table_name}: {effective_total:,} records")

        count = 0
        with tqdm(total=effective_total, desc=table_name) as pbar:
            for row in _paginate_socrata(ds_id, session, page_size,
                                         max_pages=max_pages):
                yield row
                count += 1
                if count % page_size == 0:
                    pbar.update(page_size)
            # update remainder
            remainder = count % page_size
            if remainder:
                pbar.update(remainder)

        logging.info(f"  Yielded {count:,} records for {table_name}")

    return socrata_resource



def _paginate_socrata_filtered(ds_id, session, where_clause, page_size):
    """Paginate a Socrata dataset with a $where filter."""
    offset = 0
    while True:
        url = f"https://data.texas.gov/resource/{ds_id}.json"
        params = {
            "$where": where_clause,
            "$limit": page_size,
            "$offset": offset,
            "$select": ":*, *",
            "$order": ":id",
        }
        resp = session.get(url, params=params)
        data = resp.json()
        if not data:
            break

        for row in data:
            yield _rename_socrata_cols(row)

        offset += page_size
        if len(data) < page_size:
            break


def build_dataset_index(datasets):
    """Reorganize datasets dict into {claim_type: {period: {role: ds_id}}}"""
    index = {}
    for ds_id, (name, period) in datasets.items():
        parts = name.rsplit("_", 1)
        if len(parts) != 2:
            continue
        claim_type, role = parts
        index.setdefault(claim_type, {}).setdefault(period, {})[role] = ds_id
    return index


def build_where_in(column, values, max_url_chars=2000):
    """Split values into WHERE IN clauses that fit within URL length limits.

    Socrata URL-encodes the $where param (~3x expansion), so max_url_chars
    limits the pre-encoded clause length to stay well under typical URL limits.
    """
    clauses = []
    current_batch = []
    current_len = len(column) + 5  # "column IN ()"
    for v in values:
        escaped = v.replace("'", "''")
        entry_len = len(escaped) + 3  # quotes + comma
        if current_batch and current_len + entry_len > max_url_chars:
            in_list = ",".join(f"'{x.replace(chr(39), chr(39)+chr(39))}'" for x in current_batch)
            clauses.append(f"{column} IN ({in_list})")
            current_batch = []
            current_len = len(column) + 5
        current_batch.append(v)
        current_len += entry_len
    if current_batch:
        in_list = ",".join(f"'{v.replace(chr(39), chr(39)+chr(39))}'" for v in current_batch)
        clauses.append(f"{column} IN ({in_list})")
    return clauses


def discover_patients(datasets, session):
    """Discover patient_account_numbers from header tables."""
    index = build_dataset_index(datasets)
    all_patients = set()

    for claim_type, periods in index.items():
        for period, roles in periods.items():
            ds_id = roles.get("header")
            if not ds_id:
                continue

            url = f"https://data.texas.gov/resource/{ds_id}.json"
            params = {
                "$select": "patient_account_number",
                "$group": "patient_account_number",
                "$where": "patient_account_number IS NOT NULL",
                "$limit": 50000,
            }
            try:
                resp = session.get(url, params=params)
                rows = resp.json()
                pans = {
                    r["patient_account_number"]
                    for r in rows
                    if r.get("patient_account_number")
                }
                logging.info(
                    f"  {claim_type} {period} header: {len(pans):,} distinct patients"
                )
                all_patients.update(pans)
            except Exception as e:
                logging.warning(f"Failed to discover patients from {ds_id}: {e}")

    return all_patients


def discover_complex_patients(datasets, session):
    """Discover patients with bill counts per claim type for complexity scoring."""
    index = build_dataset_index(datasets)
    patient_scores = {}

    for claim_type, periods in index.items():
        for period, roles in periods.items():
            ds_id = roles.get("header")
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
            try:
                resp = session.get(url, params=params)
                rows = resp.json()
                for r in rows:
                    pan = r.get("patient_account_number")
                    if not pan:
                        continue
                    count = int(r.get("bill_count", 0))
                    patient_scores.setdefault(pan, {})
                    patient_scores[pan][claim_type] = (
                        patient_scores[pan].get(claim_type, 0) + count
                    )
                logging.info(
                    f"  {claim_type} {period} header: "
                    f"{len(rows):,} patients with counts"
                )
            except Exception as e:
                logging.warning(
                    f"Failed to discover complex patients from {ds_id}: {e}"
                )

    return patient_scores


def select_patients(all_patients, n):
    """Randomly sample N patients from the pool."""
    pool = list(all_patients)
    if len(pool) <= n:
        logging.info(
            f"Patient pool ({len(pool)}) <= requested sample ({n}), using all"
        )
        return set(pool)
    selected = set(random.sample(pool, n))
    logging.info(f"Randomly selected {len(selected)} patients from pool of {len(pool):,}")
    return selected


def select_complex_patients(patient_scores, n):
    """Select top N patients by score (total_bills * num_claim_types)."""
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



def _yield_rows(rows):
    """Simple generator that yields a list of rows. Used with dlt.resource()."""
    yield from rows


def _get_pipeline(db_path):
    """Create a dlt pipeline targeting the raw schema in DuckDB."""
    return dlt.pipeline(
        pipeline_name="txwc_load",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="raw",
    )



def process_datasets(selected_datasets, pipeline, db_path, page_size=3000,
                     max_pages=None, force_full=False):
    """Fetch and load datasets via dlt pipeline."""
    session = _create_session()

    current = {k: v for k, v in selected_datasets.items() if v[1] == "current"}
    historical = {k: v for k, v in selected_datasets.items() if v[1] == "historical"}

    if current:
        logging.info("=" * 60)
        logging.info("PROCESSING CURRENT TABLES (Full Refresh)")
        logging.info("=" * 60)

        for ds_id, (name, period) in current.items():
            table_name = f"{name}_{period}"
            resource_fn = _make_resource(
                ds_id, name, period, session, page_size, max_pages, force_full
            )
            info = pipeline.run(resource_fn(), loader_file_format="parquet")
            logging.info(f"  dlt load: {info}")
            _ensure_full_schema(ds_id, table_name, db_path, session)

    if historical:
        logging.info("=" * 60)
        logging.info("PROCESSING HISTORICAL TABLES (Merge Update)")
        logging.info("=" * 60)

        for ds_id, (name, period) in historical.items():
            table_name = f"{name}_{period}"
            resource_fn = _make_resource(
                ds_id, name, period, session, page_size, max_pages, force_full
            )
            info = pipeline.run(resource_fn(), loader_file_format="parquet")
            _ensure_full_schema(ds_id, table_name, db_path, session)
            logging.info(f"  dlt load: {info}")



def process_datasets_sampled(selected_datasets, pipeline, db_path,
                              n_patients, complex_mode=False, page_size=3000):
    """Patient-cohort sampling: discover patients, fetch their data, load via dlt."""
    session = _create_session()
    index = build_dataset_index(selected_datasets)

    ds_text_columns = {}
    for ds_id in selected_datasets:
        ds_text_columns[ds_id] = _make_text_columns(
            _get_socrata_columns(ds_id, session)
        )

    logging.info("=" * 60)
    if complex_mode:
        logging.info(f"DISCOVERING COMPLEX PATIENTS (target: {n_patients})")
    else:
        logging.info(f"DISCOVERING PATIENTS (target: {n_patients})")
    logging.info("=" * 60)

    if complex_mode:
        patient_scores = discover_complex_patients(selected_datasets, session)
        selected_pans = select_complex_patients(patient_scores, n_patients)
    else:
        all_patients = discover_patients(selected_datasets, session)
        selected_pans = select_patients(all_patients, n_patients)

    if not selected_pans:
        logging.error("No patients discovered — nothing to fetch.")
        return

    logging.info(f"Selected {len(selected_pans)} patients for cohort")

    logging.info("=" * 60)
    logging.info("FETCHING HEADER RECORDS FOR SELECTED PATIENTS")
    logging.info("=" * 60)

    all_bill_ids = set()
    pan_clauses = build_where_in("patient_account_number", selected_pans)

    for claim_type, periods in index.items():
        for period, roles in periods.items():
            ds_id = roles.get("header")
            if not ds_id:
                continue

            name = f"{claim_type}_header"
            table_name = f"{name}_{period}"
            logging.info(f"Fetching {table_name} ({len(pan_clauses)} batches)...")

            header_rows = []
            for clause in pan_clauses:
                for row in _paginate_socrata_filtered(
                    ds_id, session, clause, page_size
                ):
                    header_rows.append(row)
                    bid = row.get("bill_id")
                    if bid:
                        all_bill_ids.add(bid)

            logging.info(f"  {table_name}: {len(header_rows):,} rows fetched")

            if header_rows:
                resource = dlt.resource(
                    _yield_rows(header_rows),
                    name=table_name,
                    write_disposition="replace",
                    max_table_nesting=0,
                    columns=ds_text_columns.get(ds_id, {}),
                )
                info = pipeline.run(resource, loader_file_format="parquet")
                logging.info(f"  dlt load: {info}")
                _ensure_full_schema(ds_id, table_name, db_path, session)

    logging.info(f"Extracted {len(all_bill_ids):,} bill_ids from headers")

    if all_bill_ids:
        logging.info("=" * 60)
        logging.info("FETCHING DETAIL RECORDS FOR MATCHING BILL_IDS")
        logging.info("=" * 60)

        bid_clauses = build_where_in("bill_id", all_bill_ids)

        for claim_type, periods in index.items():
            for period, roles in periods.items():
                ds_id = roles.get("detail")
                if not ds_id:
                    continue

                name = f"{claim_type}_detail"
                table_name = f"{name}_{period}"
                logging.info(
                    f"Fetching {table_name} ({len(bid_clauses)} batches)..."
                )

                detail_rows = []
                for clause in bid_clauses:
                    for row in _paginate_socrata_filtered(
                        ds_id, session, clause, page_size
                    ):
                        detail_rows.append(row)

                logging.info(f"  {table_name}: {len(detail_rows):,} rows fetched")

                if detail_rows:
                    resource = dlt.resource(
                        _yield_rows(detail_rows),
                        name=table_name,
                        write_disposition="replace",
                        max_table_nesting=0,
                        columns=ds_text_columns.get(ds_id, {}),
                    )
                    info = pipeline.run(resource, loader_file_format="parquet")
                    logging.info(f"  dlt load: {info}")
                    _ensure_full_schema(ds_id, table_name, db_path, session)

    expected_tables = {
        f"{name}_{period}" for _, (name, period) in selected_datasets.items()
    }
    try:
        conn = duckdb.connect(db_path, read_only=False)
        existing_tables = {
            r[0]
            for r in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'raw'"
            ).fetchall()
        }
        stale = {
            t for t in (existing_tables - expected_tables)
            if not t.startswith("_dlt_")
        }
        if stale:
            for tbl in sorted(stale):
                conn.execute(f"DROP TABLE IF EXISTS raw.{tbl};")
            logging.info(
                f"Dropped {len(stale)} stale raw tables: {', '.join(sorted(stale))}"
            )
        conn.close()
    except Exception as e:
        logging.warning(f"Could not clean stale tables: {e}")



def generate_summary_report(db_path):
    """Generate a summary report of the database status."""
    conn = duckdb.connect(db_path, read_only=True)

    logging.info("=" * 60)
    logging.info("DATABASE SUMMARY REPORT")
    logging.info("=" * 60)

    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'raw' AND table_name NOT LIKE '_dlt_%' "
        "ORDER BY table_name"
    ).fetchall()

    total_records = 0
    for (table_name,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
        total_records += count

        try:
            date_info = conn.execute(f"""
                SELECT
                    MIN(TRY_CAST(created_at AS DATE)) as earliest,
                    MAX(TRY_CAST(created_at AS DATE)) as latest
                FROM raw.{table_name}
                WHERE created_at IS NOT NULL
            """).fetchone()

            if date_info and date_info[0]:
                logging.info(
                    f"  {table_name}: {count:,} records "
                    f"({date_info[0]} to {date_info[1]})"
                )
            else:
                logging.info(f"  {table_name}: {count:,} records")
        except Exception:
            logging.info(f"  {table_name}: {count:,} records")

    logging.info(f"\nTotal records across all tables: {total_records:,}")
    conn.close()



def filter_datasets(all_datasets, ds_type="all", time_period="all"):
    """Filter datasets by type and time period."""
    filtered = {}
    for ds_id, (name, period) in all_datasets.items():
        if ds_type != "all" and ds_type.lower() not in name.lower():
            continue
        if time_period != "all" and time_period.lower() != period.lower():
            continue
        filtered[ds_id] = (name, period)
    return filtered



def main():
    parser = argparse.ArgumentParser(
        description="TX Workers Comp data loader (dlt-powered)"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        choices=["professional", "institutional", "pharmacy", "all"],
        default="all",
        help="Dataset type to download",
    )
    parser.add_argument(
        "--time_period",
        type=str,
        choices=["current", "historical", "all"],
        default="all",
        help="Time period to download",
    )
    parser.add_argument(
        "--page_size",
        type=int,
        default=3000,
        help="Records per page (default: 3000)",
    )
    parser.add_argument(
        "--max_pages",
        type=int,
        default=None,
        help="Maximum pages to fetch per dataset",
    )
    parser.add_argument(
        "--force_full",
        action="store_true",
        help="Force full refresh for all tables (ignore incremental)",
    )
    parser.add_argument(
        "--report_only",
        action="store_true",
        help="Only show database report without downloading",
    )
    parser.add_argument(
        "--sample_patients",
        type=int,
        default=None,
        help="Fetch only N complete patients (all headers + details)",
    )
    parser.add_argument(
        "--complex",
        action="store_true",
        help="With --sample_patients, pick patients with most claims across types",
    )

    args = parser.parse_args()

    db_path = os.environ.get(
        "TXWC_DB_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "tx_workers_comp.db"),
    )

    if args.report_only:
        generate_summary_report(db_path)
        return

    selected_datasets = filter_datasets(
        all_datasets=ALL_DATASETS,
        ds_type=args.dataset,
        time_period=args.time_period,
    )

    if not selected_datasets:
        logging.error("No datasets matched the given filters.")
        return

    pipeline = _get_pipeline(db_path)

    if args.sample_patients:
        logging.info(
            f"PATIENT-COHORT SAMPLING MODE: {args.sample_patients} patients"
            f"{'  (complex)' if args.complex else ''}"
        )
        logging.info(f"Datasets: {len(selected_datasets)}")

        process_datasets_sampled(
            selected_datasets,
            pipeline,
            db_path,
            n_patients=args.sample_patients,
            complex_mode=args.complex,
            page_size=args.page_size,
        )

        generate_summary_report(db_path)
        logging.info("Patient-cohort sampling completed successfully!")
        return

    if args.force_full:
        logging.warning(
            "Force full refresh enabled - all tables will be completely refreshed"
        )

    logging.info(f"Starting update of {len(selected_datasets)} datasets")
    logging.info("Strategy: Current tables (full refresh), Historical tables (merge)")
    logging.info(f"Page size: {args.page_size} records")
    if args.max_pages:
        logging.info(f"Max pages per dataset: {args.max_pages}")

    process_datasets(
        selected_datasets,
        pipeline,
        db_path,
        page_size=args.page_size,
        max_pages=args.max_pages,
        force_full=args.force_full,
    )

    generate_summary_report(db_path)
    logging.info("Data load completed successfully!")


if __name__ == "__main__":
    main()
