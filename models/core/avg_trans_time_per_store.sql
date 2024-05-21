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


ranked_transactions_device AS (
    SELECT
        A.id AS transaction_id
        ,A.device_id AS device_id
        ,B.store_id AS store_id
        ,A.happened_at 
        ,ROW_NUMBER() OVER(PARTITION BY B.store_id ORDER BY A.happened_at) AS row_num

    FROM
        transactions A LEFT JOIN devices B
            ON A.device_id = B.id

    ORDER BY row_num
),

ranked_transactions_device_next_trans AS (
    SELECT 
        A.transaction_id
        ,A.device_id
        ,A.store_id
        ,A.happened_at
        ,B.happened_at AS next_trans_happened_at
        ,A.row_num

    FROM
        ranked_transactions_device A LEFT JOIN ranked_transactions_device B
            ON A.store_id = B.store_id
            AND (A.row_num + 1) = B.row_num

),

top_trans_duration_rank AS (
    SELECT 
        transaction_id
        ,device_id
        ,store_id
        ,happened_at
        ,next_trans_happened_at
        ,DATEDIFF(day, happened_at, next_trans_happened_at) AS trans_duration
        ,row_num
    
    FROM 
        ranked_transactions_device_next_trans

    WHERE
        row_num <= 5
        AND trans_duration IS NOT NULL
        -- NULL trans_duration will be caused by transactions with no next_trans_happened_at. let's not account for it since there's no other better way to *count* the duration of a transaction
)



SELECT
    store_id
    ,AVG(trans_duration) AS avg_trans_duration
    -- weird behavior, won't run if the AVG is not given alias 
    -- https://stackoverflow.com/questions/56743577/snowflake-sql-compilation-error-missing-column-specification
    -- https://community.snowflake.com/s/question/0D50Z00008Zg12qSAB/how-do-i-add-currenttimestamp-to-a-view-i-keep-getting-the-error-sql-compilation-error-missing-column-specification

FROM 
    top_trans_duration_rank

GROUP BY
    store_id

ORDER BY avg_trans_duration
