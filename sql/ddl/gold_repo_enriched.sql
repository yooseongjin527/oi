-- =====================================================================
-- gold_repo_enriched — Bedrock 인사이트/카테고리 캐시 (M4)
-- =====================================================================
-- 매 인사이트 호출마다 Bedrock 을 다시 부르면 비용·latency 폭증 → 캐시.
-- 핵심 키: (event_date, repo_id) — 일자별 1회 분석 후 보관.
-- 실제 hot-path 캐시는 OpenSearch oi-repo-daily 가 담당.
-- 이 테이블은 Athena 쪽에서 LLM 결과를 컬럼으로 join 가능하게 두는 백업.
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS oi.gold_repo_enriched (
  event_date            DATE,
  repo_id               BIGINT,
  repo_name             STRING,
  category              STRING,        -- AI/ML | DevTools | Web | Infra | Game | Other
  category_confidence   DOUBLE,
  category_reasoning    STRING,
  insight_markdown      STRING,
  bedrock_model_id      STRING,
  generated_at          TIMESTAMP
)
PARTITIONED BY (
  year   STRING,
  month  STRING,
  day    STRING
)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/repo_enriched/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');
