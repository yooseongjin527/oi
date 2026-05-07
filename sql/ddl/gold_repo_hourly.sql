-- F4: 시간대별 활동 히트맵 마트
-- 입자도: repo × hour (UTC, 0~23)
-- silver_events의 hour partition을 그대로 활용
-- repo×hour×day 단위 집계 → repo의 timezone/contributor base 추정 신호
-- F6 repo 프로필 (Day 6)에서 그대로 재사용 예정

CREATE EXTERNAL TABLE IF NOT EXISTS oi.gold_repo_hourly (
  event_date DATE,
  hour INT,
  repo_id BIGINT,
  repo_name STRING,
  event_count BIGINT,
  dominant_event_type STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/repo_hourly/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
