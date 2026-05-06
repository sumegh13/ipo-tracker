# analyze_samples.py — inspect the saved file without hitting EDGAR again
import json, re

with open("cover_samples.jsonl") as f:
    records = [json.loads(line) for line in f]

print(f"Total records: {len(records)}")

# Find all records where NO dollar amounts were found — these need attention
no_dollars = [r for r in records if not r["dollar_matches"]]
print(f"\nRecords with no dollar matches: {len(no_dollars)}")
for r in no_dollars[:5]:
    print(f"  {r['company']} — cover starts: {r['cover_3000'][:200]}")

# Show every distinct dollar-amount phrasing pattern
print("\n--- All unique $ phrasings (digits replaced with #) ---")
all_matches = [m for r in records for m in r["dollar_matches"]]
patterns = {}
for m in all_matches:
    key = re.sub(r'[\d,]+', '#', m)[:70]
    patterns[key] = patterns.get(key, 0) + 1

for phrasing, count in sorted(patterns.items(), key=lambda x: -x[1])[:30]:
    print(f"  {count:>4}x  {phrasing}")