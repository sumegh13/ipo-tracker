# sample_covers.py — fetch cover pages for a large sample and save for analysis
import os
import re
import json
import time
import requests
from edgar import set_identity, get_filings
from include.extractor import get_cover_page_text
from dotenv import load_dotenv

load_dotenv()
set_identity(os.environ["EDGAR_IDENTITY"])

SAMPLE_SIZE = 100
OUTPUT_FILE = "cover_samples.jsonl"
HEADERS = {"User-Agent": os.environ["EDGAR_IDENTITY"]}

filings = get_filings(form="424B4", filing_date="2024-01-01:2024-06-30")
print(f"Total 424B4 filings available: {len(filings)}")

results = []
errors = []

for i, filing in enumerate(filings.head(SAMPLE_SIZE)):
    try:
        resp = requests.get(filing.document.url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        cover = get_cover_page_text(resp.text)

        dollar_matches = re.findall(r'\$\s*[\d,\.]+[^\n]{0,60}', cover)

        record = {
            "index":        i,
            "company":      filing.company,
            "cik":          str(filing.cik),
            "filing_date":  str(filing.filing_date),
            "url":          filing.document.url,
            "cover_3000":   cover[:3000],
            "dollar_matches": dollar_matches,
        }
        results.append(record)

        status = "✓" if dollar_matches else "–"
        print(f"[{i+1:>3}/{SAMPLE_SIZE}] {status}  {filing.company[:55]}")

        # Be polite to EDGAR — stay well under the 10 req/sec limit
        time.sleep(0.15)

    except Exception as e:
        errors.append({"index": i, "company": filing.company, "error": str(e)})
        print(f"[{i+1:>3}/{SAMPLE_SIZE}] ERR  {filing.company[:55]} — {e}")

# Save as newline-delimited JSON — one record per line, easy to grep
with open(OUTPUT_FILE, "w") as f:
    for record in results:
        f.write(json.dumps(record) + "\n")

print(f"\nSaved {len(results)} records to {OUTPUT_FILE}")
print(f"Errors: {len(errors)}")

# Quick summary of patterns seen in dollar matches
print("\n--- Unique dollar-amount contexts (first 80 chars) ---")
all_matches = [m for r in results for m in r["dollar_matches"]]
# Deduplicate by prefix to see distinct phrasings
seen = set()
for m in all_matches:
    key = re.sub(r'\d', '#', m)[:60]   # replace digits so "$16.00" and "$19.00" collapse
    if key not in seen:
        seen.add(key)
        print(f"  {m[:80]}")