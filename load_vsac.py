import requests
import pandas as pd
import base64
import concurrent.futures
from tqdm import tqdm
import duckdb
import os
from dotenv import load_dotenv

load_dotenv('/workspaces/txwc/.env')
API_KEY = os.getenv('API_KEY')

# Constants
BASE_URL_SVS = "https://cts.nlm.nih.gov/fhir"

def get_all_value_set_ids():
    """Retrieve all value set OIDs from VSAC using FHIR API pagination."""
    credentials = f"apikey:{API_KEY}".encode('utf-8')
    base64_encoded_credentials = base64.b64encode(credentials).decode('utf-8')
    headers = {
        "Authorization": f"Basic {base64_encoded_credentials}",
        "Accept": "application/fhir+json"
    }

    all_oids = []
    url = f"{BASE_URL_SVS}/ValueSet?_count=1000"

    while url:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            bundle = response.json()

            # Extract OIDs from this page
            for entry in bundle.get('entry', []):
                oid = entry.get('resource', {}).get('id')
                if oid:
                    all_oids.append(oid)

            # Find next page link
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

def main():
    # Get all value set OIDs
    print("Retrieving all value set IDs from VSAC...")
    described_value_set_ids = get_all_value_set_ids()
    print(f"Found {len(described_value_set_ids)} value sets to download")

    processed_oids = set()  # To keep track of processed OIDs and avoid infinite loops

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

        # Use to get descendant codes for value sets that only provide references instead of codes
        def get_descendants(self, source, code):
            ticket = self.get_service_ticket()
            url = f"{self.SERVICE}/rest/content/current/source/{source}/{code}/descendants?ticket={ticket}"
            response = requests.get(url, headers={"User-Agent": "python"})
            if response.status_code != 200:
                tqdm.write(f"Error fetching descendants: {response.status_code} - {response.text}")
                return []
            return [result['ui'] for result in tqdm(response.json()['result'], desc=f"Retrieving descendants for {code}")]

    umls_fetcher = UMLSFetcher(API_KEY)

    # Use to retrieve a value set from VSAC
    def retrieve_value_set(oid):
        credentials = f"apikey:{API_KEY}".encode('utf-8')
        base64_encoded_credentials = base64.b64encode(credentials).decode('utf-8')
        headers = {
            "Authorization": f"Basic {base64_encoded_credentials}",
            "Accept": "application/fhir+json"
        }
        response = requests.get(f"{BASE_URL_SVS}/ValueSet/{oid}", headers=headers)
        return response.json()

    # Convert the JSON response to a DataFrame
    def json_to_dataframe(response_json, current_oid=None, parent_oid=None):
        data = []
        # Mapping of system URIs to recognizable names
        system_map = {
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
            "urn:oid:2.16.840.1.113883.6.238": "CDC Race and Ethnicity"
        }
        concepts = response_json.get('compose', {}).get('include', [])
        for concept in concepts:
            system_name = system_map.get(concept.get('system', ''), "Unknown System")
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
                    "parent_oid": parent_oid if parent_oid else None
                })

            # Handle the 'descendantOf' filter for value sets that only provide references instead of codes
            filters = response_json.get('compose', {}).get('include', [{}])[0].get('filter', [])
            for filter_ in filters:
                if filter_.get("op") == "descendantOf":
                    descendants = umls_fetcher.get_descendants(filter_.get("system", ""), filter_.get("value", ""))
                    for descendant_code in descendants:
                        data.append({
                            "valueSetName": response_json.get("name", ""),
                            "code": descendant_code,
                            "display": "",  # Display is empty because the UMLS API doesn't return it
                            "system": filter_.get("system", ""),
                            "status": response_json.get("status", ""),
                            "oid": current_oid if current_oid else response_json.get("id", ""),
                            "parent_oid": parent_oid if parent_oid else None,
                            "version": response_json.get("version", ""),
                            "lastUpdated": response_json.get("meta", {}).get("lastUpdated", "")
                        })

            # If the value set references other value sets, retrieve those as well
            referenced_value_sets = concept.get('valueSet', [])
            for ref_vs in referenced_value_sets:
                # Extract OID from the reference URL
                oid = ref_vs.split('/')[-1]
                if oid not in processed_oids:  # Avoid re-processing already processed OIDs
                    data.extend(json_to_dataframe(retrieve_value_set(oid), current_oid=oid, parent_oid=current_oid))

        return data

    # Concurrent function that retrieves and processes OIDs
    def retrieve_and_process(oid):
        processed_oids.add(oid)
        response_json = retrieve_value_set(oid)
        return json_to_dataframe(response_json, current_oid=oid)

    # Use ThreadPoolExecutor for concurrent requests with tqdm progress bar
    all_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(retrieve_and_process, oid) for oid in described_value_set_ids]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(described_value_set_ids), desc="Processing OIDs"):
            all_data.extend(future.result())

    result_df = pd.DataFrame(all_data)

    # Reset the index of the final dataframe and remove duplicates
    result_df = result_df.drop_duplicates(subset=['valueSetName', 'code', 'display'])

    # Connect to DuckDB database and create table in the specified schema
    conn = duckdb.connect('/workspaces/txwc/tx_workers_comp.db')
    conn.execute("CREATE SCHEMA IF NOT EXISTS reference_data")
    conn.register('temp_df', result_df)
    conn.execute("CREATE OR REPLACE TABLE reference_data.vsac AS SELECT * FROM temp_df")
    print("Table 'reference_data.vsac' created successfully in /workspaces/txwc/tx_workers_comp.db")

if __name__ == "__main__":
    main()
