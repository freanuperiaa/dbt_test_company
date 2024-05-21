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

-- TODO: maybe we could create a model for this, to be used on top_stores_per_trans_amt and this model. it can have SUM(amount) and AVG(amount) on the columns
-- we cant have per transaction as it wont scale well
total_trans_amt_by_device AS (
    SELECT 
        device_id, AVG(amount) AS avg_trans_amount
    FROM 
        transactions
    GROUP BY
        device_id
)

SELECT
    C.typology AS typology 
    ,C.country AS country
    ,SUM(A.avg_trans_amount) AS avg_amount

FROM
    total_trans_amt_by_device A LEFT JOIN devices B
        ON A.device_id = B.id
    LEFT JOIN stores C 
        ON B.store_id = C.id

GROUP BY
    C.typology
    ,C.country

ORDER BY avg_amount DESC

