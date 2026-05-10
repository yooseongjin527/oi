# Opensource Insights (OI)

GitHub 의 실시간 이벤트 스트림과 시간별 아카이브를 통합 분석해, 트렌딩 repo 와 그 부상 원인을 한국어로 제공하는 데이터 분석 서비스.

> "GitHub Trending 이 무엇을 보여준다면, OI 는 왜를 알려준다."

**개발 기간**: 2026.04.28 ~ 2026.05.10
**배포**: AWS ap-northeast-2, EC2 t3.medium + Docker Compose

---

## Stack

| 영역 | 도구 |
|---|---|
| Streaming | Redpanda (Kafka API) |
| Batch | Apache Airflow (LocalExecutor) |
| Lake | S3 (Bronze / Silver / Gold) + Athena + Glue |
| Search | OpenSearch (oi-repo-daily 인덱스) |
| LLM | Amazon Bedrock (Claude Haiku 4.5) |
| Backend | FastAPI + SQLAlchemy + Jinja2 + aiokafka |
| Admin | Streamlit (IP 화이트리스트) |
| DB | PostgreSQL |
| Container | Docker Compose |

---

## 데이터 시간 해상도 — 3-layer 구조

OI 는 **세 가지 시점의 데이터를 layer 로 분리**해서 보여줍니다. 사용자는 한 화면에서 셋 다 봅니다.

```
시점          소스                                커버리지   가시 lag    용도
─────────────────────────────────────────────────────────────────────────────
분 단위      collector → Redpanda → 메모리       ~5~30%     ~1분        지금 분위기 (참고용)
             (live_aggregator FastAPI bg task)   sample
────────────────────────────────────────────────────────────────────────────
시간 단위    bronze_live + bronze_archive UNION  ~95~100%   ~2~3시간    오늘 시간대별 진행 (정확)
             → gold_hourly_recent                풀
─────────────────────────────────────────────────────────────────────────────
일 단위      bronze_archive(GHArchive) + live    ~95~100%   ~10시간     어제 종합 분석
             → silver_events → 6개 daily gold    풀                   (가속도/이상치/카테고리/AI 요약)
─────────────────────────────────────────────────────────────────────────────
```

---

## Architecture

```
[GitHub Events API] ──60s polling, ETag──> [collector]
                                                │
                                                ▼
                                  [Redpanda gh.events.live]
                                  │           │
                                  │           ▼
                                  │   [bronze_writer (consumer → S3)]
                                  │           │
                                  ▼           ▼
                       [live_aggregator]  [S3 bronze/live/]
                       (FastAPI bg task,
                        60min sliding win)
                                  │
                                  ▼
                          /api/live/pulse,top
                          (대시보드 LIVE PULSE)

[GHArchive] ──hourly + 90min lag──> [Airflow gharchive_to_bronze]
                                                │
                                                ▼
                                       [S3 bronze/archive/]
                                                │
                ┌───────────────────────────────┼───────────────────────┐
                ▼                               ▼                       ▼
   [silver_to_gold_hourly]          [bronze_to_silver]          (사용 안 함)
   schedule "5 * * * *"             schedule "30 0 * * *"
   처리 = -2h hour                  KST 09:30 daily
   bronze_live + archive UNION             │
                │                          ▼
                ▼                   [S3 silver/events/]
   [S3 gold/hourly_recent/]                │
                │                          ▼
                ▼                  [silver_to_gold]
   /api/hourly/today               schedule "0 1 * * *"
   (대시보드                       KST 10:00 daily
    오늘 시간대별 진행)            gate_silver_ready → 6 INSERT → verify
                                          │
                                          ▼
                                  [S3 gold/{6 마트}/]
                                          │
                                          ▼
                                  /api/insights/daily
                                  ├─ Athena top 10 + 가중치 정렬
                                  ├─ Bedrock 인사이트 (3 repo)
                                  ├─ inline batch 카테고리 분류 (10 repo 1회 호출)
                                  └─ OpenSearch 색인 (카테고리 포함)

[categorize_daily]   "0 2 * * *" KST 11:00 — 사용자 안 들어간 날짜의 backup 분류
[Repo 상세 페이지]   /api/repo/.../profile — repo 단독 Bedrock 인사이트 + OpenSearch 캐시
```

설계 결정 상세는 [OI_PROJECT_HANDOFF.md](OI_PROJECT_HANDOFF.md) 참조.

---

## 핵심 기능

### 사용자 대시보드
| ID | 기능 | 데이터 소스 | Layer |
|---|---|---|---|
| L1 | LIVE PULSE (지금 GitHub에서) | `live_aggregator` (Redpanda 메모리 sliding window) | 분 단위 |
| L2 | 오늘 시간대별 진행 | `gold_hourly_recent` | 시간 단위 |
| F1 | AI 인사이트 카드 (Top 3) | Bedrock + Athena Gold rank_score | 일 단위 |
| F2 | 전일 대비 급상승 Top 10 | `gold_repo_acceleration` | 일 단위 |
| F3 | 갑작스런 인기 Top 5 | `gold_repo_anomaly` (z-score) | 일 단위 |
| F4 | 시간대별 활동 패턴 (KST × 활동 종류) | `silver_events` 직접 집계 | 일 단위 |
| F5 | 분야별 트렌딩 (6개) | OpenSearch + Bedrock inline batch 분류 | 일 단위 |
| F6 | Repo 프로필 (단독 인사이트 + 24h 시계열) | Athena + Bedrock + OpenSearch 캐시 | 일 단위 |

### 메인 페이지
- 분 단위 / 시간 단위 / 일 단위 데이터 layer 소개 (시각)
- "어떻게 동작하나요?" 3단계 (24h 추적 → AI 정리 → 한 화면)
- 6개 기능 카드

### 추가
- 인증 (회원가입 → admin 승인 → 로그인 → 대시보드)
- 비밀번호 재설정 / 탈퇴
- OpenSearch 풀텍스트 검색
- 운영자 콘솔 (Streamlit, IP 화이트리스트)
- 다크 / 라이트 테마 토글 (헤더 우측)

---

## Quick Start (로컬 개발 — WSL Ubuntu 권장)

전제: Docker Desktop, AWS CLI v2 (`~/.aws/credentials`).

```bash
# 1. 환경 변수
cp .env.example .env
# .env 의 모든 값을 채우기
#   특히: JWT_SECRET_KEY, AIRFLOW_FERNET_KEY,
#         AWS_S3_BUCKET, ADMIN_PASSWORD, GITHUB_TOKEN

# 2. AWS 인프라 (1회만)
aws s3 mb s3://${AWS_S3_BUCKET} --region ${AWS_REGION}
python3 scripts/bootstrap_athena.py    # Glue DB + workgroup + 모든 DDL

# 3. 컨테이너 빌드 & 기동
docker compose up -d --build

# 4. 초기 admin 계정 (1회만, 멱등)
docker compose exec fastapi python init_admin.py

# 5. 상태 확인
docker compose ps
curl -fsS http://localhost:8000/health

# 6. 모든 DAG unpause
docker compose exec airflow-scheduler bash -c '
  airflow dags unpause gharchive_to_bronze
  airflow dags unpause bronze_to_silver
  airflow dags unpause silver_to_gold
  airflow dags unpause silver_to_gold_hourly
  airflow dags unpause categorize_daily
'
```

서비스 엔드포인트:

| 서비스 | URL | 용도 |
|---|---|---|
| 사용자 대시보드 | http://localhost:8000 | FastAPI |
| 운영자 콘솔 | http://localhost:8501 | Streamlit |
| Redpanda Console | http://localhost:8088 | Kafka 토픽 모니터링 |
| Airflow UI | http://localhost:8090 | DAG 관리 (admin/CHANGEME) |
| OpenSearch Dashboards | http://localhost:5601 | 인덱스 디버깅 |
| PostgreSQL | localhost:5432 | DB |

---

## Schedule 정리 — 한국시간 (KST = UTC+9)

| DAG | cron (UTC) | KST 시각 | 처리 대상 |
|---|---|---|---|
| `gharchive_to_bronze` | `0 * * * *` | 매시 정각 | 직전 hour (publish lag 90min, retry backoff) |
| `bronze_to_silver` | `30 0 * * *` | 매일 09:30 | 어제 (UTC) silver 빌드 |
| `silver_to_gold` | `0 1 * * *` | 매일 10:00 | 어제 (UTC) 6 daily mart |
| `silver_to_gold_hourly` | `5 * * * *` | 매시 5분 | -2h hour (KST 09시 데이터를 KST 11시에 적재) |
| `categorize_daily` | `0 2 * * *` | 매일 11:00 | 어제 OpenSearch 색인 (사용자 안 들어간 날짜 backup) |

**일별 분석 가시 시점**: KST 5/10 10:00 → 5/9 분석 결과 (어제 데이터). 
**시간대별 가시 시점**: 매 hour 종료 후 약 2~3시간. 
**실시간 ticker**: 1분 lag.

---

## 데이터 파이프라인 운영

### 1) 실시간 수집 확인
```bash
docker compose logs -f collector | head -20
# "📦 cycle: published=N modified_pages=K/3 took=Xs" 가 60초마다

docker compose logs -f bronze_writer | head -20
# "Uploaded N events -> s3://..../bronze/live/year=.../batch_*.jsonl.gz"

# live_aggregator 헬스 (FastAPI 안에 background task 로 도는 컨슈머)
curl -fsS http://localhost:8000/api/live/health | jq .
# {"connected": true, "messages_consumed": N, ...}
```

### 2) GHArchive 백필
```bash
# 특정 날짜 24시간 backfill
docker compose exec airflow-scheduler airflow dags backfill gharchive_to_bronze \
    --start-date 2026-04-29T00:00:00+00:00 \
    --end-date   2026-04-29T23:00:00+00:00 \
    --reset-dagruns --yes
```

### 3) Silver / Gold 수동 빌드
```bash
# Schedule cron 시각에 정확히 맞춰 logical_date 지정 필요
docker compose exec airflow-scheduler airflow dags backfill bronze_to_silver \
    --start-date 2026-04-29T00:30:00 --end-date 2026-04-29T00:30:00 \
    --reset-dagruns --yes

docker compose exec airflow-scheduler airflow dags backfill silver_to_gold \
    --start-date 2026-04-29T01:00:00 --end-date 2026-04-29T01:00:00 \
    --reset-dagruns --yes

# Hourly mart 의 logical_date = 처리 hour + 2h (코드가 -2h 보정)
docker compose exec airflow-scheduler airflow dags backfill silver_to_gold_hourly \
    --start-date 2026-05-10T02:05:00 --end-date 2026-05-10T05:05:00 \
    --reset-dagruns --yes
```

### 4) F1 인사이트 + F5 카테고리 (자동)
사용자가 `/dashboard?date=YYYY-MM-DD` 접근 시:
1. `/api/insights/daily` → Athena Gold top-10 → Bedrock 인사이트
2. **inline batch 카테고리 분류** (단일 Bedrock 호출, ~3-5초)
3. OpenSearch 인덱싱 (카테고리 포함)
4. 같은 날짜 두 번째 진입은 OpenSearch 캐시 hit

`categorize_daily` DAG 는 사용자가 한 번도 안 본 날짜의 backup 분류 용도.

### 5) Repo 단독 인사이트
사용자가 `/repo/{owner}/{name}?date=YYYY-MM-DD` 접근 시:
1. Athena hourly + daily 메트릭 조회
2. OpenSearch 캐시 (`repo_insight_markdown`) 조회 → hit 면 즉시 반환
3. miss 면 단일 repo Bedrock 호출 (~2-3초) → OpenSearch 캐시 → 반환
4. 다음 사용자는 캐시 hit

### 6) 데이터 검증
```bash
bash scripts/run_validation_queries.sh 2026-04-29
```

---

## EC2 배포

```bash
# EC2 SSH 접속 후 (1회만)
git clone https://github.com/<owner>/oi.git ~/oi
cd ~/oi
cp .env.example .env && nano .env

# 일반 배포
~/oi/scripts/deploy.sh

# 최초 배포 시: Athena DDL 도 같이 적용
~/oi/scripts/deploy.sh --bootstrap

# 코드만 변경 (이미지 재빌드 스킵)
~/oi/scripts/deploy.sh --no-build
```

상세는 본 README 의 [EC2 배포 가이드](#ec2-배포-가이드-처음부터-끝까지) 섹션 참조.

---

## Project Structure

```
.
├── .env.example                   # 모든 환경 변수 템플릿
├── docker-compose.yml             # 12개 서비스
├── README.md
├── OI_PROJECT_HANDOFF.md          # 설계 문서 원본
├── backend/                       # FastAPI 사용자 대시보드 + 인증
│   ├── main.py                    # lifespan 으로 live_aggregator 시작
│   ├── auth.py
│   ├── models.py
│   ├── routers/
│   │   ├── pages.py               # GET 페이지
│   │   ├── auth_router.py         # signup/login/forgot/reset/settings/delete
│   │   ├── admin_router.py        # 사용자 관리
│   │   ├── insights.py            # F1 (Bedrock + inline 카테고리 분류)
│   │   ├── repo.py                # F6 (repo 프로필)
│   │   ├── language.py            # F4 (시간대별 활동 패턴)
│   │   ├── search.py              # OpenSearch 검색
│   │   ├── category.py            # F5 카테고리 수동 트리거
│   │   ├── live.py                # LIVE PULSE API (/api/live/*)
│   │   └── hourly.py              # 오늘 시간대별 진행 API (/api/hourly/today)
│   ├── services/
│   │   ├── athena_client.py
│   │   ├── bedrock_client.py
│   │   ├── opensearch_client.py   # 인덱싱 + repo 캐시 helpers
│   │   ├── insights_service.py    # E2E (Athena → Bedrock → 카테고리 → 색인)
│   │   ├── category_service.py    # 단일/batch 분류
│   │   ├── repo_service.py        # 프로필 + 단독 인사이트 + 캐시
│   │   └── live_aggregator.py     # Redpanda 컨슈머 + 60min sliding window
│   ├── prompts/
│   │   ├── repo_insight_v1.md         # daily Top 3 인사이트
│   │   ├── repo_category_v1.md        # 단일 repo 분류
│   │   ├── repo_category_batch_v1.md  # Top 10 batch 분류
│   │   └── repo_single_insight_v1.md  # repo 상세 페이지 단독 인사이트
│   ├── templates/                 # Jinja2 (다크/라이트 테마 토글)
│   └── static/css/                # base/home/repo/admin/auth/settings
├── collector/                     # GitHub Events API → Redpanda
├── bronze_writer/                 # Redpanda → S3 Bronze
├── streamlit_admin/               # 운영자 콘솔
├── airflow/dags/
│   ├── gharchive_to_bronze.py     # 시간별 GHArchive → S3 (IAM 403 fallback 포함)
│   ├── bronze_to_silver.py        # KST 09:30 daily
│   ├── silver_to_gold.py          # KST 10:00 daily, gate_silver_ready 보호
│   ├── silver_to_gold_hourly.py   # 매시 5분, -2h hour 처리, gate_sources_ready
│   └── categorize_daily.py        # KST 11:00 backup 분류
├── sql/ddl/
│   ├── bronze_archive.sql         # GHArchive raw
│   ├── bronze_live.sql            # collector → bronze_writer
│   ├── silver_events.sql          # UNION + dedup + lang 정규화
│   ├── gold_tables.sql            # gold_repo_daily
│   ├── gold_actor_daily.sql
│   ├── gold_repo_acceleration.sql
│   ├── gold_repo_anomaly.sql
│   ├── gold_repo_hourly.sql
│   ├── gold_language_activity.sql
│   ├── gold_repo_enriched.sql
│   └── gold_hourly_recent.sql     # 매시 적재 hourly mart
└── scripts/
    ├── bootstrap_athena.py        # Glue DB + Workgroup + 모든 DDL 자동 적용
    ├── deploy.sh                  # EC2 배포
    └── run_validation_queries.sh  # 데이터 품질 6개 룰
```

---

## 트러블슈팅

| 증상 | 원인 / 해결 |
|---|---|
| `/dashboard` 빈 화면, 401 | admin 승인 필요. `/admin` 에서 승인 후 다시 로그인 |
| 인사이트가 "분석 결과가 아직 준비되지 않았습니다" | 그 날짜 silver_to_gold 가 아직 안 돌았거나 미완료. 매일 KST 10:00 갱신. 어제 데이터를 그 전에 보면 비어있음 |
| Athena `Table not found` | `python3 scripts/bootstrap_athena.py` 미실행. WSL 본체에 새 DDL 파일 동기화 후 재실행 |
| 카테고리 모두 "Other" | OpenSearch 색인에 카테고리 없음. `/api/insights/daily` 첫 호출이 inline batch 분류로 채워야 정상. fastapi 로그에 `insights.batch_classify filled=N/M` 확인 |
| `gate_silver_ready` retry → fail | bronze_to_silver 의 같은 날짜 run 이 안 끝났거나 실패. 의존성 우선 확인 |
| `silver_to_gold_hourly` archive=0 | `gharchive_to_bronze` 의 해당 hour 가 publish lag 안 지났거나 미실행. 시간 지나면 자동, 또는 backfill |
| `gharchive_to_bronze` HeadObject 403 | EC2 IAM Role 에 `s3:ListBucket` 미부여. 코드는 fallback 으로 객체 없음 처리 → 다운로드 진행. 정석은 IAM 정책 추가 |
| `categorize_daily` skip | OpenSearch 에 해당 날짜 색인 없음. 사용자가 `/api/insights/daily?date=YYYY-MM-DD` 한 번 호출 필요 |
| Bedrock `AccessDenied` | IAM Role 에 `BedrockFullAccess` + 모델 액세스 콘솔에서 Haiku 사용 신청 |
| LIVE PULSE 빈 상태 / "연결 대기 중" | `docker compose logs fastapi \| grep LiveAggregator`. Redpanda healthy 확인 |
| OpenSearch 호스트 connection reset | 컨테이너 안에서 호출은 OK (`docker compose exec fastapi curl http://opensearch:9200/...`). 호스트 외부 노출만 막혔을 가능성 |

---

## EC2 배포 가이드 (처음부터 끝까지)

(다음 섹션의 "EC2 빠른 배포" 가이드 참조)

---

## License

Private project (개발 중)
