-- F2: 어제 대비 활동량 가속도 마트
-- partition: year, month, day STRING (Gold 표준)
-- prev_event_count는 LAG로 직전 일자에서 가져옴
-- acceleration_ratio = event_count / NULLIF(prev_event_count, 0)
-- 04-27은 prev 없음 → NULL 처리됨

CREATE EXTERNAL TABLE IF NOT EXISTS oi.gold_repo_acceleration (
  event_date DATE,
  repo_id BIGINT,
  repo_name STRING,
  event_count BIGINT,
  prev_event_count BIGINT,
  event_delta BIGINT,
  acceleration_ratio DOUBLE,
  dominant_event_type STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/repo_acceleration/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
