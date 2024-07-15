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

, fct AS (
  SELECT
  *
  FROM {{ ref('fct_customer') }}
)

, aggregated AS (
  SELECT
    dim.customer_id
    , dim.customer_typology
    , fct.five_year_lifetime_value_eur
    , DATEDIFF(day,dim.lead_created_date, dim.customer_live_date) AS amount_days_lead_to_live
  FROM dim
  INNER JOIN fct 
    ON dim.customer_id = fct.customer_id
  WHERE dim.customer_live_date IS NOT NULL
)

, final AS (
  SELECT
    'Overall' AS calculation_type
    , NULL AS customer_typology
    , sum(amount_days_lead_to_live) / count(distinct customer_id) AS avg_days_lead_to_live
    , sum(five_year_lifetime_value_eur) AS five_year_lifetime_value_eur
  FROM aggregated

  UNION ALL 

  SELECT
    'By Typology' AS calculation_type
    , customer_typology 
    , sum(amount_days_lead_to_live) / count(distinct customer_id) as avg_days_lead_to_live
    , sum(five_year_lifetime_value_eur) as five_year_lifetime_value_eur
  FROM aggregated
  GROUP BY 1,2
  ORDER BY five_year_lifetime_value_eur DESC
  LIMIT 6 -- for top 5 typologies incl. Others + the overall
)

SELECT * FROM final
  
