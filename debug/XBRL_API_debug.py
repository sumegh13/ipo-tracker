# Run this locally before deploying — just verify the XBRL API responds
import requests
r = requests.get(
    "https://data.sec.gov/api/xbrl/companyfacts/CIK0001847367.json",  # Alumis
    headers={"User-Agent": "Sumegh sumegh@gmail.com"}
)
print(r.status_code)
print(list(r.json()["facts"]["us-gaap"].keys())[:10])