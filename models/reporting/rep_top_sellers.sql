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

final AS (
  SELECT
    dim.seller_email
    , COUNT(DISTINCT fct.customer_id) as amount_customers
    , SUM(fct.five_year_lifetime_value_eur) as revenue
    , SUM(fct.customer_acquisition_cost_eur) as costs
    , revenue / amount_customers as rev_per_customer
  FROM dim
  INNER JOIN fct
    ON dim.customer_id = fct.customer_id
  WHERE dim.first_customer_acquisition_year = 2023
    AND dim.seller_email is not null
  GROUP BY 1
  ORDER BY revenue DESC
  LIMIT 3 -- for top 3
)

SELECT * FROM final
