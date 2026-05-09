from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import time

@dag(
    dag_id="prospectus_ingestion",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["edgar", "ipo", "424b4"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def prospectus_ingestion():

    @task
    def fetch_424b4_filings(**context) -> list[dict]:
        """Pull 424B4 filings filed on the execution date."""
        from edgar import set_identity, get_filings
        set_identity(os.environ["EDGAR_IDENTITY"])

        date = context["data_interval_start"].strftime("%Y-%m-%d")
        filings = get_filings(form="424B4", filing_date=date)

        out = []
        for f in filings:
            doc = f.document
            if doc is None:
                continue
            out.append({
                "cik": str(f.cik),
                "company_name": f.company,
                "filing_date": str(f.filing_date),
                "accession_number": f.accession_no,
                "form_type": "424B4",
                "filing_url": doc.url,
            })
        return out

    @task
    def extract_terms_batch(filings: list[dict]) -> list[dict]:
        """Single task that loops, respecting the 10 req/s EDGAR rate limit.
        
        We process sequentially in one task rather than using .expand() because
        parallel tasks would each open their own EDGAR session and easily breach
        the rate limit. 0.15s sleep keeps us well under 10/s.
        """
        import requests
        from dataclasses import asdict
        from extractor import extract_offering_terms

        headers = {"User-Agent": os.environ["EDGAR_IDENTITY"]}
        results = []

        for f in filings:
            try:
                resp = requests.get(f["filing_url"], headers=headers, timeout=30)
                resp.raise_for_status()
                terms = extract_offering_terms(resp.text)
                results.append({
                    "cik": f["cik"],
                    "accession_number": f["accession_number"],
                    **asdict(terms),
                })
                time.sleep(0.15)
            except Exception as e:
                # Log and continue — don't kill the whole DAG for one bad filing
                print(f"Extraction failed for {f['company_name']}: {e}")
                results.append({
                    "cik": f["cik"],
                    "accession_number": f["accession_number"],
                    "offering_type": "unknown",
                    "shares_offered": None,
                    "price_low": None,
                    "price_high": None,
                    "extraction_method": "error",
                })
        return results

    @task
    def load_filings(filings: list[dict]):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        if not filings:
            return
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = [
            (f["cik"], f["company_name"], f["filing_date"],
             f["accession_number"], f["form_type"], f["filing_url"])
            for f in filings
        ]
        hook.insert_rows(
            table="RAW.PROSPECTUS_FILINGS",
            rows=rows,
            target_fields=["cik", "company_name", "filing_date",
                           "accession_number", "form_type", "filing_url"],
        )

    @task
    def load_terms(terms: list[dict]):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        if not terms:
            return
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = [
            (t["cik"], t["accession_number"], t.get("offering_type"),
             t.get("shares_offered"), t.get("price_low"), t.get("price_high"),
             t.get("extraction_method", "regex"))
            for t in terms
        ]
        hook.insert_rows(
            table="RAW.OFFERING_TERMS",
            rows=rows,
            target_fields=["cik", "accession_number", "offering_type",
                           "shares_offered", "price_low", "price_high",
                           "extraction_method"],
        )

    filings = fetch_424b4_filings()
    load_filings(filings)
    terms = extract_terms_batch(filings)
    load_terms(terms)

prospectus_ingestion()