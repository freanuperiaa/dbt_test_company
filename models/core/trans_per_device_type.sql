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

stores AS (
    SELECT *FROM {{ ref("stg_staging__store") }}
),

devices AS (
    SELECT * FROM {{ ref("stg_staging__device") }}
),


transations_device AS (
    SELECT
        B.type AS device_type
        ,COUNT(A.id) AS count_trans
    
    FROM 
        transactions A LEFT JOIN devices B
            ON A.device_id = B.id

    GROUP BY B.type
),

total_trans AS (
    -- this should return a single row
    SELECT COUNT(1) AS total_transactions FROM transactions
)

SELECT
    A.device_type
    ,(A.count_trans / B.total_transactions) AS pct

FROM
    -- cross join to the single rowed table
    transations_device A,
    total_trans B

ORDER BY pct DESC

