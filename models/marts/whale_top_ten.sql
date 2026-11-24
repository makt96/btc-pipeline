
-- ============================================================
-- MODEL: WHALE TOP TEN
-- ============================================================
-- PURPOSE:
--     Returns the top 10 Bitcoin addresses by total BTC received.
--
-- DEPENDENCY:
--     whale_alert
--
-- VERSION BEHAVIOR:
--     Uses an unpinned ref(), so dbt resolves the configured
--     latest version of whale_alert.
-- ============================================================

SELECT
    *

FROM {{ ref('whale_alert') }}

ORDER BY TOTAL_SENT DESC

LIMIT 10
