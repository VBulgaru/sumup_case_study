{{
  config(
    materialized = 'table',
  )
}}

WITH dim AS (
  SELECT
  *
  FROM {{ ref('dim_customer') }}
)

, fct as (
  SELECT
  *
  FROM {{ ref('fct_customer') }}
)
  
, channel_performance AS (
  SELECT
    dim.customer_country_name,
    dim.customer_acquisition_channel,
    sum(fct.five_year_lifetime_value_eur) / sum (fct.customer_acquisition_cost_eur) as ltv_over_cac
  FROM dim
  INNER JOIN fct 
    ON dim.customer_id = fct.customer_id
  WHERE
    dim.first_customer_acquisition_year = 2023
  GROUP BY 1,2
  )
final AS (
  SELECT
      'Top' as rank_type, *
  FROM channel_performance
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_country_name ORDER BY ltv_over_cac DESC) = 1
  
  UNION ALL
  
  SELECT
      'Bottom' as rank_type, *
  FROM channel_performance
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_country_name ORDER BY ltv_over_cac ASC) = 1
)

SELECT * FROM final
