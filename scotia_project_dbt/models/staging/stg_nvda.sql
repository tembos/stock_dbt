{{ config(materialized='view') }}

SELECT
    SAFE_CAST(`datetime` AS DATETIME) as datetime_est,
    SAFE_CAST(open AS FLOAT64) as open,
    SAFE_CAST(high AS FLOAT64) as high,
    SAFE_CAST(low AS FLOAT64) as low,
    SAFE_CAST(close AS FLOAT64) as close,
    SAFE_CAST(volume AS INT64) as volume,
    JSON_EXTRACT_SCALAR(meta, '$') AS meta,
    TIMESTAMP(DATETIME(TIMESTAMP(ingestion_datetime_utc), "America/New_York")) AS ingestion_datetime_est,
    batch_id

FROM {{ ref('source_nvda') }}
