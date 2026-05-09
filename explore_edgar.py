import os
from dotenv import load_dotenv
from edgar import set_identity, get_filings

load_dotenv()
set_identity(os.environ["EDGAR_IDENTITY"])

# Fetch S-1 filings from a specific date range
# This is the form most companies file before going public
filings = get_filings(form="S-1", filing_date="2024-01-01:2024-03-31")

print(f"Found {len(filings)} S-1 filings in Q1 2024")
print("\nFirst 5 filings:")
for filing in filings.head(5):
    print(f"  {filing.filing_date} | CIK {filing.cik} | {filing.company}")

# Pick the first one
sample_filing = filings[0]

# What documents does it contain?
print(f"\nDocuments in {sample_filing.company}'s S-1:")
for doc in sample_filing.attachments:
    print(f"  {doc.document_type} | {doc.description}")

# Get the primary document
primary = sample_filing.document
print(f"\nPrimary document URL: {primary.url}")
print(f"First 500 characters of text:")
print(primary.text()[:500])