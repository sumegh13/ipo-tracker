import os, requests, time, pandas as pd
from dataclasses import asdict
from edgar import set_identity, get_filings
from extractor import extract_offering_terms
from dotenv import load_dotenv

load_dotenv()
set_identity(os.environ["EDGAR_IDENTITY"])

filings = get_filings(form="424B4", filing_date="2024-01-01:2024-06-30")

results = []
for i, filing in enumerate(filings.head(100)):
    try:
        resp = requests.get(
            filing.document.url,
            headers={"User-Agent": os.environ["EDGAR_IDENTITY"]},
            timeout=15,
        )
        resp.raise_for_status()
        terms = extract_offering_terms(resp.text)
        results.append({
            "company": filing.company,
            "cik": filing.cik,
            "filing_date": str(filing.filing_date),
            **asdict(terms),
        })
        time.sleep(0.15)
    except Exception as e:
        print(f"Error [{i}] {filing.company}: {e}")

df = pd.DataFrame(results)

# Show breakdown by type
print("\n=== Offering type breakdown ===")
print(df["offering_type"].value_counts())

# IPO-only stats
ipos = df[df["offering_type"] == "ipo"]
print(f"\n=== IPOs only (n={len(ipos)}) ===")
print(ipos[["company", "filing_date", "shares_offered", "price_low"]].to_string())
print(f"\nIPO price extraction rate: {ipos['price_low'].notna().mean():.1%}")