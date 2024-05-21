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

total_trans_amt_by_device AS (
SELECT 
    device_id
    ,total_trans_amount 
    ,ROW_NUMBER() OVER(ORDER BY total_trans_amount DESC) AS row_num -- top 10
FROM 
    {{ ref("trans_amt_by_device") }}

)

SELECT
    B.store_id
    ,C.name AS store_name -- might not be needed
    ,A.total_trans_amount AS transacted_amount

FROM
    total_trans_amt_by_device A LEFT JOIN devices B
        ON A.device_id = B.id
    LEFT JOIN stores C 
    -- NOTE: not sure if I have to have the store name for this model. but If it were me, I would normalize fact tables, hence I would just simply remove the store names, removing the need to join the stores table.
        ON B.store_id = C.id

WHERE
    A.row_num <= 10
    

ORDER BY A.total_trans_amount DESC

