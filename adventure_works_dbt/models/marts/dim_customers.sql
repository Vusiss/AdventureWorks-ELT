{{ config(materialized='table') }}

SELECT
    customer_id,
    person_id,
    store_id,
    territory_id,
    account_number
FROM {{ ref('stg_customers') }}