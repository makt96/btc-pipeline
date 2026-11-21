{% macro convert_to_usd(column_name) %}
(
    {{ column_name }} * (
        SELECT CLOSE_PRICE_USD
        FROM {{ ref('btc_usd_max') }}
        WHERE CLOSE_PRICE_USD IS NOT NULL
        ORDER BY
            TRY_TO_DATE(REPLACE(TO_VARCHAR(EVENT_DATE), ' UTC', '')) DESC
        LIMIT 1
    )
)
{% endmacro %}