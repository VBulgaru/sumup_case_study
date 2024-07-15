{{
  config(
    materialized = 'table',
  )
}}

WITH cleaned AS (
  SELECT
    sales_email::VARCHAR         AS seller_email
    , typology::VARCHAR          AS customer_typology
    , country::VARCHAR           AS customer_country_name
    , customer_id::NUMBER        AS customer_id
    , order_paid_date::DATE      AS order_paid_date
    , live_date::DATE            AS customer_live_date
    , qualified_date::DATE       AS lead_qualified_date
    , lead_creation_date::DATE   AS lead_created_date
  FROM {{ source('schema_name', 'raw_funnel') }}
  WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned
