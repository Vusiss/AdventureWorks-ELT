{{ config(materialized='view') }}

SELECT
    CustomerID        AS customer_id,
    PersonID          AS person_id,
    StoreID           AS store_id,
    TerritoryID       AS territory_id,
    AccountNumber     AS account_number
FROM {{ source('adventureworks', 'Customer') }}