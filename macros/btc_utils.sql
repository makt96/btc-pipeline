-- ============================================================
-- MACRO: CONVERT BITCOIN TO USD
-- ============================================================

{% macro convert_to_usd(column_name) %}

-- PURPOSE:
--     This reusable dbt macro converts a Bitcoin amount
--     into its estimated US Dollar value.
--
-- HOW IT WORKS:
--     1. Accepts a SQL column or expression as a parameter.
--     2. Retrieves the latest available non-null BTC closing price.
--     3. Multiplies the supplied Bitcoin amount by that price.
--     4. Returns the calculated USD value.
--
-- LOCATION:
--     macros/btc_utils.sql
--
-- EXAMPLE USAGE:
--     convert_to_usd('w.TOTAL_SENT')
--
--     The macro can be reused in multiple dbt models without
--     rewriting the USD conversion logic.


-- ============================================================
-- MAIN CALCULATION
-- ============================================================

(
    -- Multiply the supplied Bitcoin amount by the latest
    -- available Bitcoin closing price in USD.

    {{ column_name }} * (

        -- ====================================================
        -- RETRIEVE THE LATEST AVAILABLE BITCOIN USD PRICE
        -- ====================================================

        SELECT CLOSE_PRICE_USD

        -- SELECT:
        --     Retrieves the Bitcoin closing price in US dollars.


        -- ====================================================
        -- REFERENCE THE DBT SEED
        -- ====================================================

        FROM {{ ref('btc_usd_max') }}

        -- FROM:
        --     Reads the reference data from our btc_usd_max seed.
        --
        -- The seed is a CSV file located in:
        --     seeds/btc_usd_max.csv
        --
        -- Its columns include:
        --     EVENT_DATE
        --     CLOSE_PRICE_USD
        --     MARKET_CAP_USD
        --     VOLUME_USD
        --
        -- dbt uses this reference to establish a dependency
        -- between the seed and the model using this macro.


        -- ====================================================
        -- EXCLUDE RECORDS WITHOUT A CLOSING PRICE
        -- ====================================================

        WHERE CLOSE_PRICE_USD IS NOT NULL

        -- WHERE:
        --     Filters the seed records.
        --
        -- IS NOT NULL:
        --     Keeps only records that contain an actual
        --     Bitcoin closing price.
        --
        -- This prevents a missing price from being selected
        -- for the USD conversion.


        -- ====================================================
        -- SORT BY DATE: NEWEST FIRST
        -- ====================================================

        ORDER BY

            TRY_TO_DATE(
                REPLACE(
                    TO_VARCHAR(EVENT_DATE),
                    ' UTC',
                    ''
                )
            ) DESC

        -- EVENT_DATE:
        --     Contains the date associated with the closing price.
        --
        -- TO_VARCHAR():
        --     Converts the EVENT_DATE value into a string.
        --
        -- REPLACE():
        --     Removes the UTC suffix from the date string.
        --
        --     Example:
        --     2026-11-21 UTC
        --
        --     After REPLACE:
        --     2026-11-21
        --
        -- TRY_TO_DATE():
        --     Converts the cleaned string into a Snowflake DATE.
        --
        --     Returns NULL if conversion is unsuccessful.
        --
        -- DESC:
        --     Sorts the dates in descending order.
        --
        --     Newest date first.
        --     Oldest date last.
        --
        -- Our objective is to retrieve the latest available
        -- closing price.


        -- ====================================================
        -- RETURN ONLY THE LATEST PRICE
        -- ====================================================

        LIMIT 1

        -- LIMIT 1:
        --     Returns only one record from the sorted results.
        --
        -- Since the results are sorted newest first,
        -- this selects the latest available closing price.

    )

)

-- ============================================================
-- END OF MACRO
-- ============================================================

{% endmacro %}