{{ config(materialized='table') }}

WITH base AS (
    SELECT
        DATE(datetime_est) AS trade_date_est,
        open,
        high,
        low,
        close,
        volume,
        ROW_NUMBER() OVER (PARTITION BY DATE(datetime_est) ORDER BY datetime_est ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY DATE(datetime_est) ORDER BY datetime_est DESC) AS rn_desc
    FROM {{ ref('stg_nvda') }}
)

SELECT DISTINCT
    trade_date_est,
    FIRST_VALUE(open) OVER (PARTITION BY trade_date_est ORDER BY rn_asc) AS daily_open,
    MAX(high) OVER (PARTITION BY trade_date_est) AS daily_high,
    MIN(low) OVER (PARTITION BY trade_date_est) AS daily_low,
    FIRST_VALUE(close) OVER (PARTITION BY trade_date_est ORDER BY rn_desc) AS daily_close,
    SUM(volume) OVER (PARTITION BY trade_date_est) AS total_volume
FROM base
GROUP BY trade_date_est, rn_asc, rn_desc, open, close, high, low, volume
