{{
    config(
        materialized='view'
    )
}}

-- after several tries, when i add materialized='table' in config, it always says
--   000606 (57P03): No active warehouse selected in the current session.  Select an active warehouse with the 'use warehouse' command.
-- well look into ^that probably later


WITH transactions AS (
    SELECT * FROM {{ ref('stg_staging__transaction') }}
),

devices AS (
    SELECT * FROM {{ ref("stg_staging__device") }}
),


transactions_with_device AS (

    -- we need to know how long it takes on average for a store to make five transactions.
    SELECT
        A.id
        ,A.device_id
        ,B.store_id
        ,A.happened_at
        ,ROW_NUMBER() OVER(PARTITION BY B.store_id ORDER BY A.happened_at) AS row_num

    FROM transactions A LEFT JOIN devices B
        ON A.device_id = B.id
),

per_store_first_five_trans AS (
    SELECT
        store_id
        ,happened_at
        ,row_num
    
    FROM 
        transactions_with_device
    
    WHERE row_num <= 5
),

per_store_to_first_five_trans AS (
    SELECT    
        store_id
        ,MAX(row_num) AS max_row_num
        ,DATEDIFF(
            day,
            MIN(happened_at),
            MAX(happened_at)
        ) AS days_to_five_transactions

    FROM
        per_store_first_five_trans
    
    GROUP BY
        store_id
    
    HAVING
        max_row_num = 5
    

)

SELECT AVG(days_to_five_transactions) AS average_time_to_five_transactions FROM per_store_to_first_five_trans

