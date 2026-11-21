
{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

WITH flattened_outputs AS (

    SELECT
        t.TRANSACTION_HASH,
        t.BLOCK_TIMESTAMP,
        t.IS_COINBASE,

        f.VALUE:address::STRING AS OUTPUT_ADDRESS,
        f.VALUE:value::FLOAT AS OUTPUT_VALUE

    FROM {{ ref('stg_btc_transactions') }} t,

    LATERAL FLATTEN(
        INPUT => t.OUTPUTS
    ) f

    WHERE f.VALUE:address IS NOT NULL

    {% if is_incremental() %}

        AND t.BLOCK_TIMESTAMP > (
            SELECT MAX(BLOCK_TIMESTAMP)
            FROM {{ this }}
        )

    {% endif %}

)

SELECT
    TRANSACTION_HASH,
    BLOCK_TIMESTAMP,
    IS_COINBASE,
    OUTPUT_ADDRESS,
    OUTPUT_VALUE

FROM flattened_outputs
