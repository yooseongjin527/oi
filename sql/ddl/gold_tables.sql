-- gold_repo_daily: per-day repo activity summary
CREATE EXTERNAL TABLE if not exists oi.gold_repo_daily (
    event_date          date,
    repo_id             bigint,
    repo_name           string,
    event_count         bigint,
    unique_actors       bigint,
    dominant_event_type string,
    push_count          bigint,
    pr_count            bigint,
    issue_count         bigint,
    watch_count         bigint,
    fork_count          bigint,
    first_event_at      timestamp,
    last_event_at       timestamp
)
PARTITIONED BY (
    year  STRING,
    month STRING,
    day   STRING
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/repo_daily/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
