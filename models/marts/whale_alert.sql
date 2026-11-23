-- ============================================================
-- MODEL CONFIGURATION
-- ============================================================

{{
    config(
        materialized='table'
    )
}}

-- config():
--     A dbt function used to configure this model.
--
-- materialized='table':
--     Tells dbt to create a physical table in Snowflake.
--
--     Every time this model runs, dbt rebuilds the table
--     using the latest results from its upstream models.
--
--     Unlike an incremental model, this model does not
--     use an incremental filter.
--
-- Target table in our dev2 environment:
--     BTC_DEV.DBT_MARK.WHALE_ALERT
--
-- This model belongs to our MARTS layer because it produces
-- aggregated, business-oriented information.


-- ============================================================
-- COMMON TABLE EXPRESSION (CTE): IDENTIFY WHALES
-- ============================================================

WITH whales AS (

    -- SELECT:
    --     Defines the columns we want in our aggregation.
    --
    -- OUTPUT_ADDRESS:
    --     The Bitcoin wallet address receiving the output.
    --
    -- SUM(OUTPUT_VALUE):
    --     Adds together the qualifying output values
    --     associated with each address.
    --
    -- AS TOTAL_SENT:
    --     Gives the calculated sum a meaningful column name.
    --
    -- COUNT(*):
    --     Counts how many qualifying output rows exist
    --     for each address.
    --
    -- AS TRANSACTION_COUNT:
    --     Gives the count a meaningful column name.
    --
    -- IMPORTANT:
    --     COUNT(*) counts output records, not necessarily
    --     distinct Bitcoin transactions.

    SELECT
        OUTPUT_ADDRESS,
        SUM(OUTPUT_VALUE) AS TOTAL_SENT,
        COUNT(*) AS TRANSACTION_COUNT


    -- ========================================================
    -- UPSTREAM MODEL
    -- ========================================================

    FROM {{ ref('int_btc_non_coinbase_outputs') }}

    -- ref():
    --     References another dbt model.
    --
    -- Here, we are reading from:
    --     int_btc_non_coinbase_outputs
    --
    -- That upstream model already removes Coinbase transactions.
    --
    -- dbt uses ref() to understand dependencies and determine
    -- the correct execution order.


    -- ========================================================
    -- FILTER LARGE OUTPUTS
    -- ========================================================

    WHERE OUTPUT_VALUE > 10

    -- WHERE:
    --     Filters records before aggregation.
    --
    -- OUTPUT_VALUE > 10:
    --     Keeps only individual output records with a value
    --     greater than 10.
    --
    -- In our BTC data, OUTPUT_VALUE is measured in Bitcoin.
    --
    -- IMPORTANT:
    --     This filters individual output records BEFORE SUM().
    --     It does not simply filter addresses whose total
    --     received Bitcoin is greater than 10.


    -- ========================================================
    -- GROUP BY BITCOIN ADDRESS
    -- ========================================================

    GROUP BY OUTPUT_ADDRESS

    -- GROUP BY:
    --     Groups all qualifying output records that belong
    --     to the same Bitcoin address.
    --
    -- Example:
    --
    --     ADDRESS_A | 12 BTC
    --     ADDRESS_A | 15 BTC
    --     ADDRESS_A | 20 BTC
    --
    -- After GROUP BY:
    --
    --     ADDRESS_A | 47 BTC
    --
    -- Each address appears once in the aggregated result.

)


-- ============================================================
-- FINAL SELECT
-- ============================================================

SELECT

    -- The Bitcoin wallet address.
    w.OUTPUT_ADDRESS,


    -- The sum of qualifying output values received
    -- by this address.
    w.TOTAL_SENT,


    -- The number of qualifying output rows associated
    -- with this address.
    w.TRANSACTION_COUNT,


    -- ========================================================
    -- CONVERT BITCOIN VALUE INTO US DOLLARS
    -- ========================================================

    {{ convert_to_usd('w.TOTAL_SENT') }} AS TOTAL_SENT_USD

    -- convert_to_usd():
    --     A custom dbt macro that we created in:
    --
    --     macros/btc_utils.sql
    --
    -- 'w.TOTAL_SENT':
    --     Passes the aggregated Bitcoin amount to the macro.
    --
    -- The macro:
    --     1. Finds the latest available non-null USD closing price
    --        in the btc_usd_max seed.
    --
    --     2. Multiplies TOTAL_SENT by that USD price.
    --
    -- AS TOTAL_SENT_USD:
    --     Names the calculated result TOTAL_SENT_USD.
    --
    -- dbt expands the macro into SQL when compiling the model.
    --
    -- This uses the latest available seed price, not a separate
    -- historical Bitcoin price for each transaction date.


-- ============================================================
-- SOURCE FOR FINAL RESULTS
-- ============================================================

FROM whales w

-- FROM whales:
--     Reads the aggregated results from our CTE.
--
-- w:
--     Gives the CTE the SQL alias "w".
--
-- We use w. before column names to make it clear
-- that those columns come from the whales CTE.


-- ============================================================
-- SORT RESULTS
-- ============================================================

ORDER BY TOTAL_SENT_USD DESC

-- ORDER BY:
--     Sorts the final results.
--
-- TOTAL_SENT_USD:
--     Sorts using the calculated USD value.
--
-- DESC:
--     Descending order, from highest to lowest.
--
-- Result:
--     Addresses with the largest calculated USD totals
--     appear first.