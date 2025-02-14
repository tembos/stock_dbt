WITH expected_nvda AS (
    SELECT DATE('2025-02-06 09:30:00') AS trade_date_est, CAST(128 AS FLOAT64) AS daily_open, CAST(130 AS FLOAT64) AS daily_high, CAST(126 AS FLOAT64) AS daily_low, CAST(126 AS FLOAT64) AS daily_close, 20000 AS total_volume
)

SELECT * FROM {{ ref('ohlc_nvda') }}
    EXCEPT DISTINCT

SELECT * FROM expected_nvda
