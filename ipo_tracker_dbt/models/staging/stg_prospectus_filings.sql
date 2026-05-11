{{ config(materialized='view') }}

WITH dedup AS (
    SELECT
        cik,
        company_name,
        filing_date,
        accession_number,
        form_type,
        filing_url,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY accession_number
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ source('raw', 'prospectus_filings') }}
)

SELECT
    cik, company_name, filing_date, accession_number,
    form_type, filing_url
FROM dedup
WHERE rn = 1