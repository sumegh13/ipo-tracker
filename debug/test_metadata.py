# test_metadata.py
import requests
import time

EDGAR_IDENTITY = "Sumegh sumegh@gmail.com"
headers = {"User-Agent": EDGAR_IDENTITY}

# Test with a few known CIKs from your dataset
test_ciks = ["1847367", "2002473", "1901440"]

for cik in test_ciks:
    padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{padded}.json"
    r = requests.get(url, headers=headers, timeout=30)
    data = r.json()
    print(f"\nCIK: {cik}")
    print(f"  name:          {data.get('name')}")
    print(f"  sic_code:      {data.get('sic')}")
    print(f"  sic_desc:      {data.get('sicDescription')}")
    print(f"  state:         {data.get('stateOfIncorporation')}")
    print(f"  fiscal_year:   {data.get('fiscalYearEnd')}")
    time.sleep(0.15)