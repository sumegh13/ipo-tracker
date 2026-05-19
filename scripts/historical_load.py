"""One-shot historical backfill — populates COMPANIES, PROSPECTUS_FILINGS,
and OFFERING_TERMS in one pass."""
import os, sys, time, requests
import snowflake.connector
from datetime import date, timedelta
from edgar import set_identity, get_filings
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "include"))
from extractor import extract_offering_terms

load_dotenv()
set_identity(os.environ["EDGAR_IDENTITY"])

# Load private key
with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH_LOCAL"], "rb") as f:
    p_key = serialization.load_pem_private_key(f.read(), password=None)
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

START, END = date(2024, 1, 1), date(2024, 12, 31)

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    private_key=pkb,
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="RAW",
)
cur = conn.cursor()
headers = {"User-Agent": os.environ["EDGAR_IDENTITY"]}

# Cache CIK → company metadata across the whole run.
# Most CIKs appear only once, but some companies file multiple 424B4s in a year.
company_cache: dict[str, dict] = {}

def get_company_metadata(cik: str) -> dict | None:
    """Fetch from EDGAR submissions API; cached per run."""
    if cik in company_cache:
        return company_cache[cik]
    padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{padded}.json"
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 404:
            company_cache[cik] = None
            return None
        r.raise_for_status()
        data = r.json()
        meta = {
            "cik": cik,
            "company_name": data.get("name"),
            "sic_code": data.get("sic"),
            "sic_description": data.get("sicDescription"),
            "state_of_incorp": data.get("stateOfIncorporation"),
            "fiscal_year_end": data.get("fiscalYearEnd"),
        }
        company_cache[cik] = meta
        time.sleep(0.15)
        return meta
    except Exception as e:
        print(f"  Company metadata failed for CIK {cik}: {e}")
        company_cache[cik] = None
        return None


def upsert_company(meta: dict):
    cur.execute("""
        MERGE INTO COMPANIES tgt
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
    """, (meta["cik"], meta.get("company_name"), meta.get("sic_code"),
          meta.get("sic_description"), meta.get("state_of_incorp"),
          meta.get("fiscal_year_end")))


current = START
total_filings = total_extracted = 0

while current <= END:
    print(f"\n=== {current} ===")
    try:
        filings = get_filings(form="424B4", filing_date=str(current))
    except Exception as e:
        print(f"  Skipped (fetch failed): {e}")
        current += timedelta(days=1)
        continue

    for f in filings:
        doc_url = None
        for attempt in range(3):
            try:
                doc = f.document
                doc_url = doc.url if doc else None
                break
            except Exception as e:
                print(f"  Document fetch attempt {attempt+1} failed for {f.company}: {e}")
                time.sleep(2 ** attempt)
        if not doc_url:
            print(f"  Skipping {f.company} — no document URL after retries")
            continue
        total_filings += 1
        cik = str(f.cik)

        # 1. Upsert company dimension first
        meta = get_company_metadata(cik)
        if meta:
            try:
                upsert_company(meta)
            except Exception as e:
                print(f"  Company upsert failed for {f.company}: {e}")

        # 2. Insert filing
        try:
            cur.execute("""
                INSERT INTO PROSPECTUS_FILINGS
                (cik, company_name, filing_date, accession_number, form_type, filing_url)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (cik, f.company, str(f.filing_date),
                  f.accession_no, "424B4", doc_url))
        except Exception as e:
            print(f"  Filing insert failed for {f.company}: {e}")
            continue

        # 3. Extract and insert offering terms
        try:
            resp = requests.get(f.document.url, headers=headers, timeout=30)
            resp.raise_for_status()
            terms = extract_offering_terms(resp.text)
            cur.execute("""
                INSERT INTO OFFERING_TERMS
                (cik, accession_number, offering_type, shares_offered,
                 price_low, price_high, extraction_method)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (cik, f.accession_no, terms.offering_type,
                  terms.shares_offered, terms.price_low, terms.price_high,
                  terms.extraction_method))
            total_extracted += 1
            print(f"  ✓ {f.company[:40]:40s} | {terms.offering_type:10s} | "
                  f"sic={meta.get('sic_code') if meta else 'NONE':5s}")
        except Exception as e:
            print(f"  Extract failed for {f.company}: {e}")

        time.sleep(0.15)

    conn.commit()
    current += timedelta(days=1)

print(f"\n{'='*60}")
print(f"Done. {total_filings} filings, {total_extracted} extracted, "
      f"{len([v for v in company_cache.values() if v])} companies cached.")
cur.close()
conn.close()