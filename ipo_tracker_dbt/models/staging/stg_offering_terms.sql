{{ config(materialized='view') }}

WITH dedup AS (
    SELECT
        cik,
        accession_number,
        offering_type,
        shares_offered,
        price_low,
        price_high,
        extraction_method,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY accession_number
            ORDER BY extracted_at DESC
        ) AS rn
    FROM {{ source('raw', 'offering_terms') }}
)

SELECT
    cik,
    accession_number,
    offering_type,
    shares_offered,
    price_low,
    price_high,
    -- For 424B4, price_low and price_high are usually equal (final price)
    COALESCE((price_low + price_high) / 2, price_low, price_high) AS final_price,
    shares_offered * COALESCE((price_low + price_high) / 2, price_low, price_high)
        AS gross_proceeds,
    extraction_method
FROM dedup
WHERE rn = 1