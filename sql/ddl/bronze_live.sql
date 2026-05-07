-- =====================================================================
-- bronze_live — Redpanda 에서 흘러온 실시간 이벤트 (jsonl.gz)
-- =====================================================================
-- bronze_writer 컨테이너가 S3 에 쓰는 prefix 를 Athena 가 읽도록 매핑.
--   s3://<bucket>/bronze/live/year=YYYY/month=MM/day=DD/hour=HH/batch_*.jsonl.gz
-- partition projection 으로 Glue partition 등록 없이 자동 인식.
-- bronze_archive 와 동일한 GitHub Events API 스키마 (collector main.py 참조).
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS oi.bronze_live (
  id            STRING,
  type          STRING,
  actor         STRUCT<
                  id: BIGINT,
                  login: STRING,
                  display_login: STRING,
                  gravatar_id: STRING,
                  url: STRING,
                  avatar_url: STRING
                >,
  repo          STRUCT<
                  id: BIGINT,
                  name: STRING,
                  url: STRING
                >,
  payload       STRING,
  public        BOOLEAN,
  created_at    STRING,
  org           STRUCT<
                  id: BIGINT,
                  login: STRING,
                  gravatar_id: STRING,
                  url: STRING,
                  avatar_url: STRING
                >,
  ingested_at   STRING
)
PARTITIONED BY (
  year   STRING,
  month  STRING,
  day    STRING,
  hour   STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'true',
  -- collector 가 JSON 에는 '_ingested_at' 키로 쓰지만 Hive 컬럼명에
  -- underscore prefix 가 안 되므로 매핑.
  'mapping.ingested_at'   = '_ingested_at'
)
LOCATION 's3://${BUCKET}/bronze/live/'
TBLPROPERTIES (
  'projection.enabled'        = 'true',
  'projection.year.type'      = 'integer',
  'projection.year.range'     = '2026,2030',
  'projection.year.digits'    = '4',
  'projection.month.type'     = 'integer',
  'projection.month.range'    = '1,12',
  'projection.month.digits'   = '2',
  'projection.day.type'       = 'integer',
  'projection.day.range'      = '1,31',
  'projection.day.digits'     = '2',
  'projection.hour.type'      = 'integer',
  'projection.hour.range'     = '0,23',
  'projection.hour.digits'    = '2',
  'storage.location.template' = 's3://${BUCKET}/bronze/live/year=${year}/month=${month}/day=${day}/hour=${hour}/',
  'classification'            = 'json',
  'compressionType'           = 'gzip'
);
