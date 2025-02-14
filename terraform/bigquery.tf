provider "google" {
  project = "bigquerysheets-404104"
}

resource "google_bigquery_dataset" "stocks_bronze" {
  dataset_id = "stocks_raw"
  location   = "US"
}

resource "google_bigquery_table" "my_table" {
  dataset_id = google_bigquery_dataset.stocks_bronze.dataset_id
  table_id   = "source_nvda"

  time_partitioning {
        type  = "DAY"
        field = "ingestion_datetime_utc"
        expiration_ms = 2592000000
      }

  schema = <<EOF
[
  {"name": "datetime", "type": "STRING", "mode": "NULLABLE"},
  {"name": "open", "type": "STRING", "mode": "NULLABLE"},
  {"name": "high", "type": "STRING", "mode": "NULLABLE"},
  {"name": "low", "type": "STRING", "mode": "NULLABLE"},
  {"name": "close", "type": "STRING", "mode": "NULLABLE"},
  {"name": "volume", "type": "STRING", "mode": "NULLABLE"},
  {"name": "meta", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ingestion_datetime_utc", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "batch_id", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}
