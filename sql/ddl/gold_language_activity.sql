-- =====================================================================
-- gold_language_activity — F4 언어 활동 히트맵 마트
-- =====================================================================
-- 입자도: hour × language (UTC 0~23)
-- silver_events.repo_language (정규화) 기준.
-- F4 화면: 시간(가로) × 언어(세로) 히트맵 + Top 10 언어 막대.
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS oi.gold_language_activity (
  event_date    DATE,
  hour          INT,
  language      STRING,
  event_count   BIGINT,
  unique_repos  BIGINT,
  unique_actors BIGINT
)
PARTITIONED BY (
  year   STRING,
  month  STRING,
  day    STRING
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/language_activity/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
