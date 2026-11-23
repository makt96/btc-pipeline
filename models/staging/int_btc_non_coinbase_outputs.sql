-- JINJA CONFIGURATION
-- -------------------

{{
    config(
        materialized='ephemeral'
    )
}}

-- config():
--     This is a dbt function used to configure the model.
--
-- materialized='ephemeral':
--     Tells dbt NOT to create a physical table or view
--     in Snowflake for this model.
--
--     Instead, dbt injects this model's SQL into the
--     downstream model that references it, as a CTE.
--
-- Why use ephemeral?
--     It allows us to create reusable transformation logic
--     without creating an additional physical Snowflake object.


-- SQL QUERY
-- ---------

-- SELECT *:
--     Selects every column from the upstream model.
--
--     In our project, these columns include:
--     TRANSACTION_HASH
--     BLOCK_TIMESTAMP
--     IS_COINBASE
--     OUTPUT_ADDRESS
--     OUTPUT_VALUE
--
-- FROM {{ ref('stg_btc_outputs') }}:
--     ref() is a dbt function.
--
--     It identifies another dbt model by its name.
--
--     Here, we are reading from stg_btc_outputs.
--
--     dbt also uses ref() to understand dependencies
--     and determine the correct execution order.
--
-- WHERE IS_COINBASE = FALSE:
--     Filters the records.
--
--     IS_COINBASE = TRUE:
--         Coinbase transactions.
--
--     IS_COINBASE = FALSE:
--         Non-coinbase transactions.
--
--     We only want non-coinbase transaction outputs
--     in this model.

SELECT *

FROM {{ ref('stg_btc_outputs') }}

WHERE IS_COINBASE = FALSE