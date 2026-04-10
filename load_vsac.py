import base64
import logging
import os
from tqdm import tqdm
import dlt
from dlt.sources.helpers.requests import Session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    API_KEY = dlt.secrets["sources.vsac.api_key"]
except KeyError:
    API_KEY = ""

BASE_URL_SVS = "https://cts.nlm.nih.gov/fhir"


def _create_session():
    """Create a dlt Session with auth headers for VSAC FHIR API."""
    credentials = f"apikey:{API_KEY}".encode('utf-8')
    b64 = base64.b64encode(credentials).decode('utf-8')
    session = Session(timeout=30)
    session.headers.update({
        "Authorization": f"Basic {b64}",
        "Accept": "application/fhir+json",
    })
    return session


def _create_umls_session():
    """Create a plain dlt Session for UMLS API calls."""
    return Session(timeout=30)


def get_all_value_set_ids(session):
    """Retrieve all value set OIDs from VSAC using FHIR API pagination."""
    all_oids = []
    url = f"{BASE_URL_SVS}/ValueSet?_count=1000"

    while url:
        try:
            response = session.get(url)
            response.raise_for_status()
            bundle = response.json()

            for entry in bundle.get('entry', []):
                oid = entry.get('resource', {}).get('id')
                if oid:
                    all_oids.append(oid)

            url = None
            for link in bundle.get('link', []):
                if link.get('relation') == 'next':
                    url = link.get('url')
                    break

            tqdm.write(f"Retrieved {len(all_oids)} value sets so far...")

        except Exception as e:
            tqdm.write(f"Error fetching value sets: {e}")
            break

    return all_oids


class UMLSFetcher:
    def __init__(self, api_key, session):
        self.API_KEY = api_key
        self.SERVICE = "https://uts-ws.nlm.nih.gov"
        self.session = session
        self.TICKET_GRANTING_TICKET = self.get_ticket_granting_ticket()

    def get_ticket_granting_ticket(self):
        params = {'apikey': self.API_KEY}
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent": "python"}
        response = self.session.post("https://utslogin.nlm.nih.gov/cas/v1/api-key", headers=headers, data=params)
        response.raise_for_status()
        return response.url.split('/')[-1]

    def get_service_ticket(self):
        params = {'service': self.SERVICE}
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent": "python"}
        response = self.session.post(f"https://utslogin.nlm.nih.gov/cas/v1/tickets/{self.TICKET_GRANTING_TICKET}", headers=headers, data=params)
        response.raise_for_status()
        return response.text

    def get_descendants(self, source, code):
        ticket = self.get_service_ticket()
        url = f"{self.SERVICE}/rest/content/current/source/{source}/{code}/descendants?ticket={ticket}"
        response = self.session.get(url, headers={"User-Agent": "python"})
        if response.status_code != 200:
            tqdm.write(f"Error fetching descendants: {response.status_code} - {response.text}")
            return []
        return [result['ui'] for result in tqdm(response.json()['result'], desc=f"Retrieving descendants for {code}")]


SYSTEM_MAP = {
    "http://snomed.info/sct": "SNOMED",
    "http://hl7.org/fhir/sid/icd-10-cm": "ICD10CM",
    "http://hl7.org/fhir/sid/icd-9-cm": "ICD9CM",
    "http://loinc.org": "LOINC",
    "http://www.ama-assn.org/go/cpt": "CPT",
    "http://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets": "HCPCS",
    "http://www2a.cdc.gov/vaccines/iis/iisstandards/vaccines.asp?rpt=cvx": "CVX",
    "http://www.nlm.nih.gov/research/umls/rxnorm": "RXNORM",
    "http://terminology.hl7.org/CodeSystem/v3-AdministrativeGender": "HL7 AdministrativeGender",
    "http://www.cms.gov/Medicare/Coding/ICD10": "ICD10PCS",
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/HospitalAcqCond/Coding": "CMS Present of Admission (POA) Indicator",
    "http://www.nlm.nih.gov/research/umls/hcpcs": "HCPCS",
    "https://www.cdc.gov/nhsn/cdaportal/terminology/codesystem/hsloc.html": "HSLOC",
    "http://hl7.org/fhir/sid/cvx": "CVX",
    "http://www.ada.org/cdt": "CDT",
    "https://nahdo.org/sopt": "Source of Payment Typology (SOPT)",
    "urn:oid:2.16.840.1.113883.6.238": "CDC Race and Ethnicity",
}

_MAX_RECURSION_DEPTH = 10


def retrieve_value_set(oid, session):
    """Retrieve a single value set from VSAC by OID."""
    try:
        response = session.get(f"{BASE_URL_SVS}/ValueSet/{oid}")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.warning(f"Failed to retrieve value set {oid}: {e}")
        return {}


def flatten_value_set(response_json, umls_fetcher, processed_oids, session,
                      current_oid=None, parent_oid=None, depth=0):
    """Flatten a VSAC value set JSON into records, handling descendantOf filters and recursive references."""
    if depth > _MAX_RECURSION_DEPTH:
        logging.warning(f"Max recursion depth reached for OID {current_oid}")
        return []

    data = []
    concepts = response_json.get('compose', {}).get('include', [])
    for concept in concepts:
        system_name = SYSTEM_MAP.get(concept.get('system', ''), "Unknown System")
        concept_codes = concept.get('concept', [])
        for code in concept_codes:
            data.append({
                "valueSetName": response_json.get("name", ""),
                "code": code.get("code", ""),
                "display": code.get("display", ""),
                "system": system_name,
                "status": response_json.get("status", ""),
                "version": response_json.get("version", ""),
                "lastUpdated": response_json.get("meta", {}).get("lastUpdated", ""),
                "oid": current_oid if current_oid else response_json.get("id", ""),
                "parent_oid": parent_oid if parent_oid else None,
            })

        # Process filters for THIS concept's include entry (not just [0])
        for filter_ in concept.get('filter', []):
            if filter_.get("op") == "descendantOf":
                descendants = umls_fetcher.get_descendants(filter_.get("system", ""), filter_.get("value", ""))
                for descendant_code in descendants:
                    data.append({
                        "valueSetName": response_json.get("name", ""),
                        "code": descendant_code,
                        "display": "",
                        "system": filter_.get("system", ""),
                        "status": response_json.get("status", ""),
                        "oid": current_oid if current_oid else response_json.get("id", ""),
                        "parent_oid": parent_oid if parent_oid else None,
                        "version": response_json.get("version", ""),
                        "lastUpdated": response_json.get("meta", {}).get("lastUpdated", ""),
                    })

        referenced_value_sets = concept.get('valueSet', [])
        for ref_vs in referenced_value_sets:
            oid = ref_vs.split('/')[-1]
            if oid not in processed_oids:
                processed_oids.add(oid)
                ref_json = retrieve_value_set(oid, session)
                if ref_json:
                    data.extend(flatten_value_set(
                        ref_json, umls_fetcher, processed_oids, session,
                        current_oid=oid, parent_oid=current_oid,
                        depth=depth + 1,
                    ))

    return data


@dlt.resource(write_disposition="replace", name="vsac")
def vsac_resource():
    """Yield flattened, deduplicated VSAC value set records."""
    session = _create_session()
    umls_session = _create_umls_session()

    print("Retrieving all value set IDs from VSAC...")
    value_set_ids = get_all_value_set_ids(session)
    print(f"Found {len(value_set_ids)} value sets to download")

    umls_fetcher = UMLSFetcher(API_KEY, umls_session)
    processed_oids = set()
    seen = set()

    for oid in tqdm(value_set_ids, desc="Processing OIDs"):
        if oid in processed_oids:
            continue
        processed_oids.add(oid)
        response_json = retrieve_value_set(oid, session)
        if not response_json:
            continue
        records = flatten_value_set(
            response_json, umls_fetcher, processed_oids, session,
            current_oid=oid,
        )
        for record in records:
            dedup_key = (record["valueSetName"], record["code"], record["display"])
            if dedup_key not in seen:
                seen.add(dedup_key)
                yield record


def main():
    pipeline = dlt.pipeline(
        pipeline_name="vsac",
        destination="duckdb",
        dataset_name="reference_data",
    )
    load_info = pipeline.run(vsac_resource())
    print(load_info)
    print("Table 'reference_data.vsac' loaded successfully.")


if __name__ == "__main__":
    main()
