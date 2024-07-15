{{
  config(
    materialized = 'table'
  )
}}

WITH financials_agg AS (
  SELECT 
    customer_id
    , customer_acquisition_channel
    , sum(five_year_lifetime_value_eur) as five_year_lifetime_value_eur
    , sum(customer_acquisition_cost_eur) as customer_acquisition_cost_eur
  FROM {{ ref('ods_financials') }}
  GROUP BY ALL
)

, sales_agg AS (
  SELECT
    customer_id
    , min(customer_acquisition_year) as first_customer_acquisition_year -- taking the first year a customer record was created
FROM {{ ref('ods_sales') }}
GROUP BY ALL
)

, store_agg AS (
  SELECT 
    *
FROM {{ ref('ods_store') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_created_at_timestamp_cet ASC) = 1 -- in case of duplicates, using only the first entry based on customer_created_timestamp
)

, funnel_agg AS (
  SELECT
    *
FROM {{ ref('ods_funnel') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY lead_created_date ASC) = 1 -- taking the first entry from the table in case there are duplicates (in this case customer_id = 177)
)
-- assumption: financials are the source of truth based on auditing and market practices. 
, final AS (
  SELECT
    sales_agg.first_customer_acquisition_year
    , financials_agg.customer_id
    , financials_agg.customer_acquisition_channel
    , COALESCE(funnel_agg.customer_country_name,store_agg.customer_country_name) as customer_country_name -- backfilling from ods_store in case of missing records in ods_funnel
    , funnel_agg.seller_email
    , funnel_agg.lead_created_date
    , funnel_agg.lead_qualified_date
    , funnel_agg.customer_live_date
    , COALESCE(funnel_agg.customer_typology, store_agg.customer_typology) as customer_typology -- backfilling from ods_store in case of missing records in ods_funnel
FROM financials_agg
LEFT JOIN funnel_agg 
  on financials_agg.customer_id = funnel_agg.customer_id
LEFT JOIN store_agg 
  on financials_agg.customer_id = store_agg.customer_id
LEFT JOIN sales_agg 
  on financials_agg.customer_id  = sales_agg.customer_id
)

SELECT * FROM final
