-- ============================================================
-- MODEL CONFIGURATION
-- ============================================================

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='TRANSACTION_HASH'
    )
}}

-- config():
--     A dbt function used to configure this model.
--
-- materialized='incremental':
--     Tells dbt to create and maintain a physical table
--     in Snowflake.
--
--     First run:
--         Loads the complete source dataset.
--
--     Subsequent runs:
--         Processes only records selected by the
--         incremental filter below.
--
-- incremental_strategy='merge':
--     Tells dbt to use Snowflake MERGE functionality.
--
--     It compares incoming records with existing records
--     using the unique key.
--
--     If a matching record exists:
--         UPDATE the existing record.
--
--     If no matching record exists:
--         INSERT a new record.
--
-- unique_key='TRANSACTION_HASH':
--     Identifies the column dbt uses to match records
--     between the incoming data and the existing target table.
--
--     In our BTC pipeline, TRANSACTION_HASH identifies
--     a Bitcoin transaction.
--
--     This helps prevent duplicate transaction records
--     when matching keys are processed again.
--
-- Target table in our dev2 environment:
--     BTC_DEV.DBT_MARK.STG_BTC_TRANSACTIONS


-- ============================================================
-- SELECT SOURCE DATA
-- ============================================================

SELECT *

-- SELECT *:
--     Selects every column from the source table.
--
--     In our project, these include:
--
--     TRANSACTION_HASH
--     BLOCK_HASH
--     BLOCK_TIMESTAMP
--     FEE
--     INPUT_VALUE
--     OUTPUT_VALUE
--     FEE_PER_BYTE
--     IS_COINBASE
--     OUTPUTS
--
--     OUTPUTS is particularly important because it contains
--     the nested JSON array that we flatten in the next model.


-- ============================================================
-- SOURCE DEFINITION
-- ============================================================

FROM {{ source('btc', 'BTC_TRANSACTIONS') }}

-- source():
--     A dbt function used to reference a raw source table.
--
--     Unlike ref(), source() references a table defined
--     in the project's sources.yml file.
--
-- 'btc':
--     The source name declared in sources.yml.
--
-- 'BTC_TRANSACTIONS':
--     The table name declared under that source.
--
-- In our project, the source points to:
--
-- Database: BTC_DEV
-- Schema:   RAW
-- Table:    BTC_TRANSACTIONS
--
-- dbt also uses source() to understand the dependency
-- between our staging model and the raw source table.


-- ============================================================
-- INCREMENTAL FILTER
-- ============================================================

{% if is_incremental() %}

    -- is_incremental():
    --     A dbt macro that determines whether this model
    --     is running as an incremental update.
    --
    --     TRUE:
    --         The target table already exists and this is
    --         an incremental run.
    --
    --     FALSE:
    --         This is the initial build or a full refresh.
    --
    -- During the initial build, this entire WHERE block
    -- is excluded from the compiled SQL.

    WHERE BLOCK_TIMESTAMP > (

        -- Find the latest transaction timestamp
        -- already stored in our target table.

        SELECT MAX(BLOCK_TIMESTAMP)

        -- {{ this }}:
        --     A dbt variable representing the current model's
        --     target relation in Snowflake.
        --
        --     In our dev2 environment, it points to:
        --
        --     BTC_DEV.DBT_MARK.STG_BTC_TRANSACTIONS
        --
        -- This allows us to compare incoming source data
        -- with the data already stored in the target table.

        FROM {{ this }}

    )

    -- BLOCK_TIMESTAMP > MAX(BLOCK_TIMESTAMP):
    --
    --     Only selects source transactions with timestamps
    --     newer than the latest timestamp already loaded.
    --
    --     This reduces the amount of data processed
    --     during subsequent incremental runs.

{% endif %}