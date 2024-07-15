{{
  config(
    materialized = 'table',
  )
}}

WITH cleaned AS (
  SELECT
    customer_id::NUMBER                                                                                  AS customer_id
    , country::VARCHAR                                                                                   AS customer_country_name
    , CONVERT_TIMEZONE('UTC', 'Europe/Berlin',try_to_timestamp_ntz(created_at,'MM/DD/YYYY HH12:MI:SS'))  AS customer_created_at_timestamp_cet
    , typology::VARCHAR                                                                                  AS customer_typology
  FROM {{ source('schema_name', 'raw_store') }}
  WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned
