{{ config(materialized='table', schema='MARTS') }}

WITH filings AS (SELECT * FROM {{ ref('stg_prospectus_filings') }}),

terms AS (
    SELECT * FROM {{ ref('stg_offering_terms') }}
    WHERE offering_type = 'ipo'
      AND final_price IS NOT NULL
      AND shares_offered IS NOT NULL
),

financials AS (
    SELECT
        cik,
        trailing_revenue,
        shares_outstanding,
        ROW_NUMBER() OVER (PARTITION BY cik ORDER BY fiscal_year_end DESC) AS rn
    FROM {{ source('raw', 'post_ipo_financials') }}
    WHERE trailing_revenue > 0
    QUALIFY rn = 1
),

sectors AS (
    SELECT sic_code,
        CASE
            WHEN sic_code BETWEEN '7370' AND '7379' THEN 'Software & IT Services'
            WHEN sic_code BETWEEN '2830' AND '2839' THEN 'Pharmaceuticals'
            WHEN sic_code BETWEEN '3840' AND '3849' THEN 'Medical Devices'
            WHEN sic_code BETWEEN '6020' AND '6029' THEN 'Banks'
            WHEN sic_code BETWEEN '6311' AND '6411' THEN 'Insurance'
            WHEN sic_code BETWEEN '3670' AND '3679' THEN 'Semiconductors'
            WHEN sic_code BETWEEN '5800' AND '5899' THEN 'Restaurants & Retail'
            WHEN sic_code BETWEEN '1311' AND '1389' THEN 'Oil & Gas'
            WHEN sic_code BETWEEN '4812' AND '4899' THEN 'Telecom'
            ELSE 'Other'
        END AS sector
    FROM filings GROUP BY sic_code
),

base AS (
    SELECT
        f.cik, f.company_name, f.filing_date, f.accession_number,
        f.sic_code, s.sector,
        t.shares_offered, t.final_price, t.gross_proceeds,
        t.extraction_method,
        t.offering_type,
        fin.trailing_revenue, fin.shares_outstanding,
        -- True implied market cap requires post-IPO shares outstanding,
        -- not just shares offered. Fall back to shares_offered when missing.
        COALESCE(fin.shares_outstanding, t.shares_offered) * t.final_price
            AS implied_market_cap,
        CASE
            WHEN fin.trailing_revenue > 0
            THEN COALESCE(fin.shares_outstanding, t.shares_offered) * t.final_price
                 / fin.trailing_revenue
        END AS price_to_sales_ratio,
        DATE_TRUNC('quarter', f.filing_date) AS filing_quarter,
        DATE_TRUNC('year', f.filing_date) AS filing_year
    FROM filings f
    INNER JOIN terms t USING (cik, accession_number)
    LEFT JOIN sectors s ON f.sic_code = s.sic_code
    LEFT JOIN financials fin ON f.cik = fin.cik
)

SELECT *,
    AVG(price_to_sales_ratio) OVER (PARTITION BY sector) AS sector_avg_ps,
    MEDIAN(price_to_sales_ratio) OVER (PARTITION BY sector) AS sector_median_ps,
    CASE
        WHEN price_to_sales_ratio IS NULL THEN 'No revenue data'
        WHEN price_to_sales_ratio > MEDIAN(price_to_sales_ratio)
            OVER (PARTITION BY sector) * 1.5 THEN 'Richly valued'
        WHEN price_to_sales_ratio < MEDIAN(price_to_sales_ratio)
            OVER (PARTITION BY sector) * 0.7 THEN 'Cheaply valued'
        ELSE 'In line with sector'
    END AS valuation_flag
FROM base