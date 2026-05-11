{{ config(materialized='view') }}

SELECT
    cik,
    company_name,
    sic_code,
    sic_description,
    state_of_incorp,
    fiscal_year_end
FROM {{ source('raw', 'companies') }}
WHERE sic_code IS NOT NULL