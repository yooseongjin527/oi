-- F3: 이상 탐지 마트 — z-score + 이벤트 타입 비율 결합
-- activity_zscore: 일별 평균 대비 활동량 표준화
-- watch_ratio, fork_ratio: 이벤트 타입 분포
-- watch_zscore, fork_zscore: 비율 자체의 z-score (전체 repo 분포 기준)
-- anomaly_score: 세 z-score의 유클리드 거리 (sqrt of sum of squares)
-- 분모 0 방지: NULLIF로 가드 (DAG INSERT에서 처리)

CREATE EXTERNAL TABLE IF NOT EXISTS oi.gold_repo_anomaly (
  event_date DATE,
  repo_id BIGINT,
  repo_name STRING,
  event_count BIGINT,
  activity_zscore DOUBLE,
  watch_count BIGINT,
  watch_ratio DOUBLE,
  watch_zscore DOUBLE,
  fork_ratio DOUBLE,
  fork_zscore DOUBLE,
  anomaly_score DOUBLE
)
PARTITIONED BY (year STRING, month STRING, day STRING)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/repo_anomaly/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
