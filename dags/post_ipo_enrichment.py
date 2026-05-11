from airflow.decorators import dag, task
from datetime import datetime
import os
import time

@dag(
    dag_id="post_ipo_enrichment",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["edgar", "xbrl"],
)
def post_ipo_enrichment():

    @task
    def get_iposed_ciks() -> list[str]:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = hook.get_records("""
            SELECT DISTINCT cik
            FROM IPO_TRACKER.MARTS.FCT_IPO_OFFERINGS
            WHERE filing_date <= DATEADD(month, -6, CURRENT_DATE())
        """)
        return [r[0] for r in rows]

    @task
    def fetch_xbrl_facts(ciks: list[str]) -> list[dict]:
        """Hit data.sec.gov/api/xbrl/companyfacts for each CIK."""
        import requests
        headers = {"User-Agent": os.environ["EDGAR_IDENTITY"]}
        out = []
        for cik in ciks:
            padded = cik.zfill(10)
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded}.json"
            try:
                r = requests.get(url, headers=headers, timeout=30)
                if r.status_code == 404:
                    continue   # too new for a 10-K
                r.raise_for_status()
                facts = r.json()
                rev_tag, revenue = extract_trailing_revenue(facts)
                shares_out = extract_shares_outstanding(facts)
                fy_end = extract_fiscal_year_end(facts)
                if revenue is not None:
                    out.append({
                        "cik": cik,
                        "fiscal_year_end": fy_end,
                        "trailing_revenue": revenue,
                        "shares_outstanding": shares_out,
                        "revenue_tag": rev_tag,
                    })
                time.sleep(0.15)
            except Exception as e:
                print(f"XBRL fetch failed for CIK {cik}: {e}")
        return out

    @task
    def load_financials(records: list[dict]):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        if not records:
            return
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        rows = [
            (r["cik"], r["fiscal_year_end"], r["trailing_revenue"],
             r.get("shares_outstanding"), r["revenue_tag"])
            for r in records
        ]
        hook.insert_rows(
            table="RAW.POST_IPO_FINANCIALS",
            rows=rows,
            target_fields=["cik", "fiscal_year_end", "trailing_revenue",
                           "shares_outstanding", "revenue_tag"],
        )

    ciks = get_iposed_ciks()
    facts = fetch_xbrl_facts(ciks)
    load_financials(facts)


def extract_trailing_revenue(facts: dict) -> tuple[str | None, float | None]:
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    revenue_tags = [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
    ]
    for tag in revenue_tags:
        if tag not in us_gaap:
            continue
        usd_units = us_gaap[tag].get("units", {}).get("USD", [])
        annual = [u for u in usd_units
                  if u.get("form", "").startswith("10-K") and u.get("fp") == "FY"]
        if annual:
            most_recent = max(annual, key=lambda x: x.get("end", ""))
            return tag, float(most_recent["val"])
    return None, None


def extract_shares_outstanding(facts: dict) -> int | None:
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    dei = facts.get("facts", {}).get("dei", {})
    tags = ["CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"]
    for tag in tags:
        for source in [us_gaap, dei]:
            if tag in source:
                units = source[tag].get("units", {}).get("shares", [])
                if units:
                    most_recent = max(units, key=lambda x: x.get("end", ""))
                    return int(most_recent["val"])
    return None


def extract_fiscal_year_end(facts: dict) -> str | None:
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    if "Revenues" in us_gaap:
        units = us_gaap["Revenues"].get("units", {}).get("USD", [])
        annual = [u for u in units if u.get("fp") == "FY"]
        if annual:
            return max(annual, key=lambda x: x.get("end", ""))["end"]
    return None


post_ipo_enrichment()