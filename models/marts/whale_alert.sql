{{
    config(
        materialized='table'
    )
}}

WITH whales AS (
    SELECT
        OUTPUT_ADDRESS,
        SUM(OUTPUT_VALUE) AS TOTAL_SENT,
        COUNT(*) AS TRANSACTION_COUNT
    FROM {{ ref('int_btc_non_coinbase_outputs') }}
    WHERE OUTPUT_VALUE > 10
    GROUP BY OUTPUT_ADDRESS
)

SELECT
    w.OUTPUT_ADDRESS,
    w.TOTAL_SENT,
    w.TRANSACTION_COUNT,
    {{ convert_to_usd('w.TOTAL_SENT') }} AS TOTAL_SENT_USD
FROM whales w
ORDER BY TOTAL_SENT_USD DESC