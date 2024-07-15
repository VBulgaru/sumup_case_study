{{
  config(
    materialized = 'table',
  )
}}

WITH cleaned AS (
  SELECT
    customer_id::NUMBER          AS customer_id
    , channel::VARCHAR           AS customer_acquisition_channel
    , MONTH(acquisition_month)   AS customer_acquisition_month
    , YEAR(acquisition_month)    AS customer_acquisition_year
  FROM {{ source('schema_name', 'raw_sales') }}
  WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned
