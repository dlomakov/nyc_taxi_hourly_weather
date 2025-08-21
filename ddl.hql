CREATE DATABASE IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.fact_taxi_hourly (
  zone_id               INT,
  borough               STRING,
  pickup_hour           TIMESTAMP,
  trips_cnt             BIGINT,
  avg_duration_min      DECIMAL(18,4),
  p50_duration_min      DECIMAL(18,4),
  p95_duration_min      DECIMAL(18,4),
  avg_speed_kmh         DECIMAL(18,4),
  revenue_usd           DECIMAL(18,4),
  cash_share            DECIMAL(18,4),
  temp_c                DECIMAL(18,4),
  precip_mm             DECIMAL(18,4)
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;
