{{
    config(
        materialized='view'
    )
}}

-- after several tries, when i add materialized='table' in config, it always says
--   000606 (57P03): No active warehouse selected in the current session.  Select an active warehouse with the 'use warehouse' command.
-- well look into ^that probably later

WITH transaction AS (
    SELECT * FROM {{ ref('stg_staging__transaction') }}
),


transactions_per_item AS (
    SELECT 
        product_sku
        ,COUNT(1) as num_transactions
        ,ROW_NUMBER() OVER(ORDER BY COUNT(1)) AS row_num
    FROM 
        transaction
    GROUP BY
        product_sku
    ORDER BY
        row_num
)

SELECT 
    product_sku
    ,num_transactions 

FROM 
    transactions_per_item

WHERE 
    row_num <= 10

ORDER BY num_transactions DESC