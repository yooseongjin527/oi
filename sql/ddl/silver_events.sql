-- =====================================================================
-- silver_events — 통합 정규화 이벤트 (Live + Archive)
-- =====================================================================
-- bronze_to_silver DAG 가 매일 INSERT.
-- 출처: oi.bronze_archive (GHArchive) UNION oi.bronze_live (Redpanda)
-- dedup: event_id 동일 시 source='live' 우선 (실시간 데이터 우선)
-- 정규화:
--   - created_at: STRING ('2026-04-29T12:34:56Z') → TIMESTAMP
--   - repo_language: payload 에서 추출 + 매핑 (JavaScript/TypeScript/Python/...)
-- 파티션: year/month/day/hour (UTC 기준)
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS oi.silver_events (
  event_id        STRING,
  type            STRING,
  actor_login     STRING,
  actor_id        BIGINT,
  repo_name       STRING,
  repo_id         BIGINT,
  payload         STRING,
  payload_action  STRING,
  public          BOOLEAN,
  created_at      TIMESTAMP,
  org_login       STRING,
  repo_language   STRING,
  source          STRING        -- 'archive' | 'live'
)
PARTITIONED BY (
  year   STRING,
  month  STRING,
  day    STRING,
  hour   STRING
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/silver/events/'
TBLPROPERTIES (
  'parquet.compression' = 'SNAPPY',
  'projection.enabled'  = 'true',
  'projection.year.type'   = 'integer',
  'projection.year.range'  = '2026,2030',
  'projection.year.digits' = '4',
  'projection.month.type'  = 'integer',
  'projection.month.range' = '1,12',
  'projection.month.digits'= '2',
  'projection.day.type'    = 'integer',
  'projection.day.range'   = '1,31',
  'projection.day.digits'  = '2',
  'projection.hour.type'   = 'integer',
  'projection.hour.range'  = '0,23',
  'projection.hour.digits' = '2',
  'storage.location.template' = 's3://${BUCKET}/silver/events/year=${year}/month=${month}/day=${day}/hour=${hour}/'
);
