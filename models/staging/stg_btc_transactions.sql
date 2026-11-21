
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='TRANSACTION_HASH'
    )
}}

SELECT *
FROM {{ source('btc', 'BTC_TRANSACTIONS') }}

{% if is_incremental() %}

    WHERE BLOCK_TIMESTAMP > (
        SELECT MAX(BLOCK_TIMESTAMP)
        FROM {{ this }}
    )

{% endif %}
