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
    def fetch_company_metadata(filings: list[dict]) -> list[dict]:
        """For each unique CIK, fetch metadata from the submissions API.

        We hit data.sec.gov/submissions/CIK{padded}.json which returns
        sic, sicDescription, stateOfIncorporation, fiscalYearEnd, etc.
        """
        import requests
        if not filings:
            return []

        unique_ciks = {f["cik"] for f in filings}
        headers = {"User-Agent": os.environ["EDGAR_IDENTITY"]}
        companies = []

        for cik in unique_ciks:
            padded = cik.zfill(10)
            url = f"https://data.sec.gov/submissions/CIK{padded}.json"
            try:
                r = requests.get(url, headers=headers, timeout=30)
                if r.status_code == 404:
                    continue
                r.raise_for_status()
                data = r.json()
                companies.append({
                    "cik": cik,
                    "company_name": data.get("name"),
                    "sic_code": data.get("sic"),
                    "sic_description": data.get("sicDescription"),
                    "state_of_incorp": data.get("stateOfIncorporation"),
                    "fiscal_year_end": data.get("fiscalYearEnd"),
                })
                time.sleep(0.15)  # EDGAR rate limit
            except Exception as e:
                print(f"Company metadata fetch failed for CIK {cik}: {e}")

        return companies

    @task
    def extract_terms_batch(filings: list[dict]) -> list[dict]:
        """Sequential extraction; respects EDGAR's 10 req/s limit."""
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
    def upsert_companies(companies: list[dict]):
        """Insert new companies; update existing ones via MERGE."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        if not companies:
            return
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        conn = hook.get_conn()
        cur = conn.cursor()

        # MERGE = upsert. Snowflake's idiomatic pattern.
        for c in companies:
            cur.execute("""
                MERGE INTO RAW.COMPANIES tgt
                USING (SELECT %s AS cik, %s AS company_name, %s AS sic_code,
                              %s AS sic_description, %s AS state_of_incorp,
                              %s AS fiscal_year_end) src
                ON tgt.cik = src.cik
                WHEN MATCHED THEN UPDATE SET
                    company_name = src.company_name,
                    sic_code = src.sic_code,
                    sic_description = src.sic_description,
                    state_of_incorp = src.state_of_incorp,
                    fiscal_year_end = src.fiscal_year_end,
                    ingested_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (cik, company_name, sic_code, sic_description,
                     state_of_incorp, fiscal_year_end)
                    VALUES (src.cik, src.company_name, src.sic_code,
                            src.sic_description, src.state_of_incorp,
                            src.fiscal_year_end)
            """, (c["cik"], c.get("company_name"), c.get("sic_code"),
                  c.get("sic_description"), c.get("state_of_incorp"),
                  c.get("fiscal_year_end")))
        conn.commit()
        cur.close()

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
    companies = fetch_company_metadata(filings)
    terms = extract_terms_batch(filings)

    upsert_companies(companies)
    load_filings(filings)
    load_terms(terms)

prospectus_ingestion()