import requests
import base64
import os
from dotenv import load_dotenv
from tqdm import tqdm
import dlt

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
API_KEY = os.getenv('API_KEY')

BASE_URL_SVS = "https://cts.nlm.nih.gov/fhir"


def _get_auth_headers():
    """Build FHIR Basic auth headers from API_KEY."""
    credentials = f"apikey:{API_KEY}".encode('utf-8')
    b64 = base64.b64encode(credentials).decode('utf-8')
    return {
        "Authorization": f"Basic {b64}",
        "Accept": "application/fhir+json",
    }


def get_all_value_set_ids():
    """Retrieve all value set OIDs from VSAC using FHIR API pagination."""
    headers = _get_auth_headers()
    all_oids = []
    url = f"{BASE_URL_SVS}/ValueSet?_count=1000"

    while url:
        try:
            response = requests.get(url, headers=headers)
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

        except requests.exceptions.RequestException as e:
            tqdm.write(f"Error fetching value sets: {e}")
            break

    return all_oids


class UMLSFetcher:
    def __init__(self, api_key):
        self.API_KEY = api_key
        self.SERVICE = "https://uts-ws.nlm.nih.gov"
        self.TICKET_GRANTING_TICKET = self.get_ticket_granting_ticket()

    def get_ticket_granting_ticket(self):
        params = {'apikey': self.API_KEY}
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent": "python"}
        response = requests.post("https://utslogin.nlm.nih.gov/cas/v1/api-key", headers=headers, data=params)
        response.raise_for_status()
        return response.url.split('/')[-1]

    def get_service_ticket(self):
        params = {'service': self.SERVICE}
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain", "User-Agent": "python"}
        response = requests.post(f"https://utslogin.nlm.nih.gov/cas/v1/tickets/{self.TICKET_GRANTING_TICKET}", headers=headers, data=params)
        response.raise_for_status()
        return response.text

    def get_descendants(self, source, code):
        ticket = self.get_service_ticket()
        url = f"{self.SERVICE}/rest/content/current/source/{source}/{code}/descendants?ticket={ticket}"
        response = requests.get(url, headers={"User-Agent": "python"})
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


def retrieve_value_set(oid):
    """Retrieve a single value set from VSAC by OID."""
    headers = _get_auth_headers()
    response = requests.get(f"{BASE_URL_SVS}/ValueSet/{oid}", headers=headers)
    return response.json()


def flatten_value_set(response_json, umls_fetcher, processed_oids, current_oid=None, parent_oid=None):
    """Flatten a VSAC value set JSON into records, handling descendantOf filters and recursive references."""
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

        filters = response_json.get('compose', {}).get('include', [{}])[0].get('filter', [])
        for filter_ in filters:
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
                data.extend(flatten_value_set(
                    retrieve_value_set(oid), umls_fetcher, processed_oids,
                    current_oid=oid, parent_oid=current_oid,
                ))

    return data


@dlt.resource(write_disposition="replace", name="vsac")
def vsac_resource():
    """Yield flattened, deduplicated VSAC value set records."""
    print("Retrieving all value set IDs from VSAC...")
    value_set_ids = get_all_value_set_ids()
    print(f"Found {len(value_set_ids)} value sets to download")

    umls_fetcher = UMLSFetcher(API_KEY)
    processed_oids = set()
    seen = set()

    for oid in tqdm(value_set_ids, desc="Processing OIDs"):
        if oid in processed_oids:
            continue
        processed_oids.add(oid)
        response_json = retrieve_value_set(oid)
        records = flatten_value_set(response_json, umls_fetcher, processed_oids, current_oid=oid)
        for record in records:
            dedup_key = (record["valueSetName"], record["code"], record["display"])
            if dedup_key not in seen:
                seen.add(dedup_key)
                yield record


def main():
    db_path = os.environ.get(
        'TXWC_DB_PATH',
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tx_workers_comp.db'),
    )
    pipeline = dlt.pipeline(
        pipeline_name="vsac",
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="reference_data",
    )
    load_info = pipeline.run(vsac_resource())
    print(load_info)
    print("Table 'reference_data.vsac' loaded successfully.")


if __name__ == "__main__":
    main()
