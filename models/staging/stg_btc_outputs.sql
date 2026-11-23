-- ============================================================
-- MODEL CONFIGURATION
-- ============================================================

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

-- config():
--     A dbt function used to configure this model.
--
-- materialized='incremental':
--     Tells dbt to create and maintain a physical table
--     in Snowflake and load additional data during future runs.
--
--     First run:
--         Loads the complete result into the target table.
--
--     Subsequent runs:
--         Loads only records selected by the incremental filter.
--
-- incremental_strategy='append':
--     Tells dbt to INSERT new records into the existing table.
--
--     It does not update existing records or merge duplicates.
--
-- In our project, the target table is:
--     BTC_DEV.DBT_MARK.STG_BTC_OUTPUTS


-- ============================================================
-- COMMON TABLE EXPRESSION (CTE)
-- ============================================================

WITH flattened_outputs AS (

    -- SELECT:
    --     Defines the columns we want in our transformed result.
    --
    -- t.TRANSACTION_HASH:
    --     The unique identifier of the Bitcoin transaction.
    --
    -- t.BLOCK_TIMESTAMP:
    --     The timestamp associated with the transaction.
    --
    -- t.IS_COINBASE:
    --     Indicates whether the transaction is a Coinbase transaction.
    --
    -- These three columns come from the upstream transaction model.

    SELECT
        t.TRANSACTION_HASH,
        t.BLOCK_TIMESTAMP,
        t.IS_COINBASE,


        -- ====================================================
        -- EXTRACT FIELDS FROM THE JSON OUTPUT OBJECTS
        -- ====================================================

        -- f.VALUE:address:
        --     Accesses the address field inside each JSON output object.
        --
        -- ::STRING:
        --     Converts the extracted value into a Snowflake STRING.
        --
        -- AS OUTPUT_ADDRESS:
        --     Gives the extracted column a meaningful column name.
        --
        -- Example:
        --     JSON address -> OUTPUT_ADDRESS

        f.VALUE:address::STRING AS OUTPUT_ADDRESS,


        -- f.VALUE:value:
        --     Accesses the value field inside each JSON output object.
        --
        -- ::FLOAT:
        --     Converts the extracted value into a FLOAT data type.
        --
        -- AS OUTPUT_VALUE:
        --     Gives the extracted column a meaningful column name.
        --
        -- Example:
        --     JSON value -> OUTPUT_VALUE

        f.VALUE:value::FLOAT AS OUTPUT_VALUE


    -- ========================================================
    -- SOURCE MODEL
    -- ========================================================

    FROM {{ ref('stg_btc_transactions') }} t,

    -- ref('stg_btc_transactions'):
    --     References the upstream dbt model.
    --
    -- t:
    --     Gives that upstream model the SQL alias "t".
    --
    -- dbt uses ref() to understand model dependencies
    -- and determine the correct execution order.


    -- ========================================================
    -- FLATTEN THE OUTPUTS ARRAY
    -- ========================================================

    LATERAL FLATTEN(
        INPUT => t.OUTPUTS
    ) f

    -- FLATTEN():
    --     A Snowflake table function that expands semi-structured data
    --     such as JSON arrays into individual rows.
    --
    -- INPUT => t.OUTPUTS:
    --     Tells Snowflake which JSON array should be expanded.
    --
    -- LATERAL:
    --     Allows FLATTEN to access the OUTPUTS column from
    --     the current transaction row.
    --
    -- f:
    --     Gives the flattened result the alias "f".
    --
    -- f.VALUE:
    --     Contains the individual JSON output object.
    --
    -- Example:
    --
    -- Original transaction:
    --     OUTPUTS = [
    --         {"address": "address_A", "value": 0.5},
    --         {"address": "address_B", "value": 0.3},
    --         {"address": "address_C", "value": 0.2}
    --     ]
    --
    -- After FLATTEN:
    --
    --     address_A | 0.5
    --     address_B | 0.3
    --     address_C | 0.2
    --
    -- One transaction with three outputs becomes three rows.


    -- ========================================================
    -- REMOVE OUTPUTS WITHOUT AN ADDRESS
    -- ========================================================

    WHERE f.VALUE:address IS NOT NULL

    -- f.VALUE:address:
    --     Retrieves the address field from each JSON object.
    --
    -- IS NOT NULL:
    --     Keeps only output records that have an address.
    --
    -- This prevents records with missing addresses from
    -- entering our flattened output data.


    -- ========================================================
    -- INCREMENTAL FILTER
    -- ========================================================

    {% if is_incremental() %}

        -- is_incremental():
        --     A dbt macro that checks whether this model is
        --     running as an incremental update.
        --
        --     TRUE:
        --         The target table already exists and dbt is
        --         performing an incremental run.
        --
        --     FALSE:
        --         This is the initial build, or a full refresh.
        --
        -- During the initial build, this entire IF block
        -- is excluded from the compiled SQL.

        AND t.BLOCK_TIMESTAMP > (

            -- SELECT MAX(BLOCK_TIMESTAMP):
            --     Finds the newest transaction timestamp
            --     already stored in our target table.

            SELECT MAX(BLOCK_TIMESTAMP)

            -- {{ this }}:
            --     A dbt variable representing the current model's
            --     target relation in Snowflake.
            --
            --     In our dev2 target, it points to:
            --     BTC_DEV.DBT_MARK.STG_BTC_OUTPUTS
            --
            -- This lets dbt check the latest timestamp already loaded.

            FROM {{ this }}

        )

        -- Comparison:
        --
        -- t.BLOCK_TIMESTAMP > MAX(BLOCK_TIMESTAMP)
        --
        -- Only selects source transactions whose timestamp
        -- is newer than the latest timestamp in the target table.
        --
        -- This avoids scanning and appending all previously
        -- loaded transaction timestamps on every incremental run.

    {% endif %}

)


-- ============================================================
-- FINAL OUTPUT
-- ============================================================

-- Select the columns we want to store in the final table.
--
-- We are selecting from the CTE called flattened_outputs.
--
-- The final table contains one row per Bitcoin transaction output.

SELECT
    TRANSACTION_HASH,
    BLOCK_TIMESTAMP,
    IS_COINBASE,
    OUTPUT_ADDRESS,
    OUTPUT_VALUE

FROM flattened_outputs