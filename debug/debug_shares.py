# debug_shares.py — reads cover_samples.jsonl, shows cover text for NaN-shares IPOs
import json, re

NAN_SHARES_COMPANIES = [
    "Rectitude Holdings",
    "Armlogi Holding",
    "Viking Holdings",
    "CleanCore Solutions",
    "Loar Holdings",
    "Marex Group",
    "Rubrik",
    "mF International",
    "Mingteng International",
]

with open("cover_samples.jsonl") as f:
    records = [json.loads(line) for line in f]

for r in records:
    if any(name.lower() in r["company"].lower() for name in NAN_SHARES_COMPANIES):
        cover = r["cover_3000"]
        print(f"\n{'='*60}")
        print(f"COMPANY: {r['company']}")
        print(f"--- First 800 chars ---")
        print(cover[:800])
        print(f"--- Number patterns found ---")
        # Show every large number (potential share counts)
        print(re.findall(r'[\d,]{5,}\s+(?:ordinary\s+)?(?:class\s+[a-z]\s+)?shares?', 
                         cover, re.IGNORECASE)[:10])