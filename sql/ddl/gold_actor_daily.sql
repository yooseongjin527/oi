CREATE EXTERNAL TABLE if not exists oi.gold_actor_daily (
    event_date          date,
    actor_id            bigint,
    actor_login         string,
    event_count         bigint,
    unique_repos        bigint,
    dominant_event_type string,
    push_count          bigint,
    pr_count            bigint,
    issue_count         bigint,
    watch_count         bigint,
    fork_count          bigint
)
PARTITIONED BY (
    year  STRING,
    month STRING,
    day   STRING
)
STORED AS PARQUET
LOCATION 's3://oi-data-lake/gold/actor_daily/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
