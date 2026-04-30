-- =====================================================================
-- bronze_archive — GHArchive hourly snapshots
-- =====================================================================
-- Reads .json.gz files written by the gharchive_to_bronze Airflow DAG.
-- One file per hour. Partition columns are part of the S3 key path.
--
-- Run with workgroup `oi-workgroup`.
-- Replace ${BUCKET} below with the actual bucket name before execution.
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS oi.bronze_archive (
  id          string,
  type        string,
  actor       struct<
                id:         bigint,
                login:      string,
                display_login: string,
                gravatar_id: string,
                url:        string,
                avatar_url: string
              >,
  repo        struct<
                id:   bigint,
                name: string,
                url:  string
              >,
  payload     string,
  public      boolean,
  created_at  string,
  org         struct<
                id:         bigint,
                login:      string,
                gravatar_id: string,
                url:        string,
                avatar_url: string
              >
)
PARTITIONED BY (
  year   string,
  month  string,
  day    string,
  hour   string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json'      = 'true',
  'dots.in.keys'               = 'false',
  'case.insensitive'           = 'true',
  'mapping'                    = 'true'
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
          OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://${BUCKET}/bronze/archive/'
TBLPROPERTIES (
  'has_encrypted_data' = 'false',
  'classification'     = 'json',
  'compressionType'    = 'gzip'
);
