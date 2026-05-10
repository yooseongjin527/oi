-- =====================================================================
-- gold_hourly_recent — 매시간 적재되는 hourly 활동 마트
-- =====================================================================
-- 목적: "오늘 진행 중인 시간대별 트렌드" 를 daily batch lag 없이 노출
-- 적재: silver_to_gold_hourly DAG 가 매시 05분 trigger 되어 직전 hour 만 적재
-- 입자도: repo × hour
-- 일별 마트(gold_repo_hourly)와 별도 — 일별 분석은 daily 갱신, 이건 hourly 갱신
-- 활용: dashboard 의 "오늘의 시간대별 진행" 차트
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS oi.gold_hourly_recent (
  event_date           DATE,
  hour                 INT,
  hour_ts              TIMESTAMP,         -- 해당 hour 의 정확한 시작 시각 (UTC)
  repo_id              BIGINT,
  repo_name            STRING,
  event_count          BIGINT,
  unique_actors        BIGINT,
  push_count           BIGINT,
  pr_count             BIGINT,
  issue_count          BIGINT,
  watch_count          BIGINT,
  fork_count           BIGINT,
  dominant_event_type  STRING
)
PARTITIONED BY (
  year   STRING,
  month  STRING,
  day    STRING,
  hour_p STRING                            -- partition 컬럼명 충돌 회피용
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/hourly_recent/'
TBLPROPERTIES (
  'parquet.compression'    = 'SNAPPY',
  'projection.enabled'     = 'true',
  'projection.year.type'   = 'integer',
  'projection.year.range'  = '2026,2030',
  'projection.year.digits' = '4',
  'projection.month.type'  = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits'= '2',
  'projection.day.type'    = 'integer',
  'projection.day.range'   = '1,31',
  'projection.day.digits'  = '2',
  'projection.hour_p.type' = 'integer',
  'projection.hour_p.range'  = '0,23',
  'projection.hour_p.digits' = '2',
  'storage.location.template' =
    's3://${BUCKET}/gold/hourly_recent/year=${year}/month=${month}/day=${day}/hour=${hour_p}/'
);
