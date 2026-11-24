
-- ============================================================
-- MODEL: WHALE ALERT V2
-- ============================================================
-- PURPOSE:
--     Identifies Bitcoin addresses receiving large outputs
--     and aggregates their total Bitcoin received.
--
-- VERSION 2 CHANGE:
--     TOTAL_SENT_USD has been removed.
--     Consumers are responsible for their own USD conversion.
--
-- MATERIALIZATION:
--     TABLE
-- ============================================================


-- ============================================================
-- MODEL CONFIGURATION
-- ============================================================

{{
    config(
        materialized='table'
    )
}}


-- ============================================================
-- COMMON TABLE EXPRESSION (CTE): IDENTIFY WHALES
-- ============================================================

WITH whales AS (

    -- Aggregate qualifying Bitcoin outputs by wallet address.
    SELECT
        OUTPUT_ADDRESS,

        -- Total Bitcoin received by this address.
        SUM(OUTPUT_VALUE) AS TOTAL_SENT,

        -- Number of qualifying output records.
        -- This is not necessarily a distinct transaction count.
        COUNT(*) AS TRANSACTION_COUNT

    FROM {{ ref('int_btc_non_coinbase_outputs') }}

    -- Only include individual outputs greater than 10 BTC.
    WHERE OUTPUT_VALUE > 10

    -- Produce one aggregated record per Bitcoin address.
    GROUP BY OUTPUT_ADDRESS

)


-- ============================================================
-- FINAL SELECT
-- ============================================================

SELECT

    -- Bitcoin wallet address.
    w.OUTPUT_ADDRESS,

    -- Total qualifying Bitcoin received.
    w.TOTAL_SENT,

    -- Number of qualifying output records.
    w.TRANSACTION_COUNT

FROM whales w


-- ============================================================
-- SORT RESULTS
-- ============================================================

-- Sort addresses by total Bitcoin received,
-- from highest to lowest.
ORDER BY w.TOTAL_SENT DESC
