{{
    config(
        materialized='view'
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_staging__transaction') }}
),


SELECT 
    device_id
    ,SUM(amount) AS total_trans_amount
    ,AVG(amount) AS avg_trans_amount
FROM 
    transactions
GROUP BY
    device_id
