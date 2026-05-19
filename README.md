# SEC IPO Data Pipeline

Automated data pipeline extracting and analyzing initial public offering (IPO) data from SEC EDGAR filings. Processes 400+ filings to produce analytics-ready datasets with valuation metrics and sector benchmarking.

## Overview

**What it does:**
- Extracts offering terms (shares, price ranges) from SEC 424B4 prospectus filings
- Enriches with post-IPO financials (revenue, shares outstanding) from XBRL API
- Calculates market cap, price-to-sales ratios, and valuation flags
- Delivers clean, tested datasets for IPO performance analysis

**Key Achievement:** 100% extraction accuracy on 80+ 2024 IPOs with automated daily ingestion.

---

## Architecture

SEC EDGAR API
↓
Apache Airflow (orchestration)
├─ prospectus_ingestion (daily) → extracts offering terms
└─ post_ipo_enrichment (weekly) → fetches financials
↓
Snowflake Data Warehouse
├─ RAW layer (Airflow loads)
├─ STAGING layer (dbt deduplication)
└─ MARTS layer (analytics models)
↓
dbt Transformations
└─ FCT_IPO_OFFERINGS (fact table with metrics)

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Orchestration** | Apache Airflow (Astronomer) |
| **Extraction** | Python, edgartools, BeautifulSoup |
| **LLM Fallback** | Anthropic Claude Haiku API |
| **Data Warehouse** | Snowflake |
| **Transformation** | dbt Core 1.11 |
| **Authentication** | RSA key-pair (Snowflake) |

**Key Python Libraries:** `edgartools`, `anthropic`, `snowflake-connector-python`, `apache-airflow`, `dbt-snowflake`

---

## Features

### Extraction
- **Regex-based parsing** for standard prospectus formats
- **LLM fallback** (Claude Haiku) for non-standard filings (~$0.002 per 10 calls)
- **Smart classification** (IPO vs SPAC vs follow-on) using signal detection
- **Company metadata** (SIC codes, fiscal year end) from SEC submissions API

### Data Quality
- 12 automated dbt tests (uniqueness, nulls, ranges, business logic)
- Deduplication on accession numbers
- Validation: price ≤ $500, shares 10k-1B, gross proceeds > 0

### Analytics
- Implied market capitalization (shares × final price)
- Price-to-sales ratio (market cap / trailing revenue)
- Valuation flags: Overvalued | Fairly valued | Undervalued | No revenue data
- Sector-level benchmarking against median P/S ratios

---

## Quick Start

### Prerequisites
- Snowflake account
- Anthropic API key
- Python 3.11+
- Docker Desktop
- Astronomer CLI (`brew install astro`)

### Setup

1. **Clone and configure:**
```bash
git clone <repo-url>
cd ipo-tracker

# Create .env with required variables
EDGAR_IDENTITY=Your Name your.email@example.com
ANTHROPIC_API_KEY=sk-ant-api03-...
SNOWFLAKE_USER=airflow_svc
SNOWFLAKE_ACCOUNT=YOUR-ACCOUNT-ID
SNOWFLAKE_PRIVATE_KEY_PATH_LOCAL=/path/to/rsa_key.p8
```

2. **Generate Snowflake key pair:**
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

3. **Setup Snowflake** (create database, schemas, tables, service user with key-pair auth)

4. **Install dependencies:**
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install edgartools anthropic snowflake-connector-python dbt-snowflake
```

5. **Start Airflow:**
```bash
astro dev start
# UI: http://localhost:8080 (admin/admin)
```

---

## Usage

### Historical Backfill
```bash
# Via Airflow
astro dev run backfill create \
    --dag-id prospectus_ingestion \
    --from-date 2024-01-01 \
    --to-date 2024-12-31 \
    --max-active-runs 1

# Or run Python script directly (more reliable)
python scripts/historical_load.py
```

### Daily Operations
- **prospectus_ingestion** runs daily at midnight UTC (automatic)
- **post_ipo_enrichment** runs weekly on Sundays (automatic)
- Manual triggers available via Airflow UI

### Run dbt Transformations
```bash
cd ipo_tracker_dbt
dbt run              # Build all models
dbt test             # Run data quality tests
dbt docs generate    # Generate documentation
dbt docs serve       # View lineage at localhost:8080
```

### Query Results
```sql
-- Most overvalued IPOs by P/S ratio
SELECT company_name, sector, price_to_sales_ratio, valuation_flag
FROM IPO_TRACKER.MARTS.FCT_IPO_OFFERINGS
WHERE price_to_sales_ratio IS NOT NULL
ORDER BY price_to_sales_ratio DESC
LIMIT 10;

-- IPO count by sector
SELECT sector, COUNT(*) as ipo_count, AVG(implied_market_cap) as avg_market_cap
FROM IPO_TRACKER.MARTS.FCT_IPO_OFFERINGS
GROUP BY sector;
```

---

## Project Structure

ipo-tracker/
├── dags/
│   ├── prospectus_ingestion.py      # Daily filing ingestion
│   └── post_ipo_enrichment.py       # Weekly financial enrichment
├── include/
│   └── extractor.py                 # Extraction logic (regex + LLM)
├── scripts/
│   └── historical_load.py           # One-time backfill script
├── ipo_tracker_dbt/
│   ├── models/
│   │   ├── staging/                 # Deduplication views
│   │   └── marts/
│   │       └── fct_ipo_offerings.sql  # Final fact table
│   └── macros/
│       └── generate_schema_name.sql   # Schema naming override
└── .env                              # Environment variables

---

## Data Model

**RAW Layer** (Airflow loads)
- `FILINGS_424B4` — All prospectus filings
- `OFFERING_TERMS_424B4` — Extracted IPO terms
- `POST_IPO_FINANCIALS` — Revenue and shares from XBRL

**STAGING Layer** (dbt views)
- `STG_PROSPECTUS_FILINGS`, `STG_OFFERING_TERMS` — Deduped sources

**MARTS Layer** (dbt tables)
- `FCT_IPO_OFFERINGS` — Analytics fact table with:
  - Filing metadata (company, date, accession number)
  - Offering terms (shares, price)
  - Post-IPO financials (revenue, shares outstanding)
  - Calculated metrics (market cap, P/S ratio, valuation flag)
  - Sector benchmarks (median P/S by SIC code)

---

## Troubleshooting

**Network timeouts during backfill?**  
Clear EDGAR cache: `astro dev bash` → `rm -rf /home/astro/.edgar/_tcache` → `exit`  
Or run backfill in quarterly batches instead of full year

**SIC_CODE/SECTOR are NULL?**  
Run `python scripts/historical_load.py` to fetch company metadata, then rebuild dbt models

**dbt creates `RAW_MARTS` instead of `MARTS`?**  
Add custom schema macro in `macros/generate_schema_name.sql` to override dbt's default behavior

---

## Future Enhancements

- [ ] Integrate dbt run as Airflow downstream task
- [ ] Extend backfill to 2020-2023
- [ ] Build Streamlit dashboard for interactive exploration
- [ ] Add ML model for sector classification
- [ ] Implement S-1/A amendment tracking
- [ ] Export to Metabase/Lightdash for visualization
