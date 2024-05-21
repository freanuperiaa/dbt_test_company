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

-- we can probably just use ROW_NUMBER() and
-- filter ROW_NUMBER() res <=10
-- and order by row number?
transactions_per_item AS (
    SELECT 
        product_sku
        ,count(1) as num_transactions
    FROM 
        transaction
    GROUP BY
        product_sku
    ORDER BY
        num_transactions DESC
)

SELECT * FROM transactions_per_item LIMIT 10