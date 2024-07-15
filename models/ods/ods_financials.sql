{{
  config(
    materialized = 'table',
  )
}}

WITH cleaned AS (
  SELECT
    id::NUMBER                   as customer_id
    , device_id::NUMBER          as device_id
    , "5_year_ltv":: NUMBER      as five_year_lifetime_value_eur
    , cac::NUMBER                as customer_acquisition_cost_eur
    , channel::VARCHAR           as customer_acquisition_channel
  FROM {{ source('schema_name', 'raw_financials') }}
  WHERE id IS NOT NULL
)

SELECT * FROM cleaned
