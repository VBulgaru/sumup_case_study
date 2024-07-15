{{
  config(
    materialized = 'table',
  )
}}

WITH ods_financials AS (
  (
  SELECT *
  FROM {{ ref('ods_financials') }}
)
  
, final AS (
  SELECT
    customer_id
    , sum(five_year_lifetime_value_eur) as five_year_lifetime_value_eur
    , sum(customer_acquisition_cost_eur) as customer_acquisition_cost_eur
    , COUNT(distinct device_id) as count_devices
FROM ods_financials
GROUP BY ALL
)

SELECT * FROM final
