CREATE EXTERNAL TABLE oi.bronze_archive (
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
                  >
)
PARTITIONED BY (
    year   STRING,
    month  STRING,
    day    STRING,
    hour   STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
LOCATION 's3://oi-data-lake/bronze/archive/'
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
    'storage.location.template' = 's3://oi-data-lake/bronze/archive/year=${year}/month=${month}/day=${day}/hour=${hour}/'
);
