# Opensource Insights (OI)

GitHub 실시간 이벤트 스트림과 시간별 아카이브를 통합 분석해 트렌딩 repo와 그 부상 원인을 자연어로 제공하는 데이터 분석 서비스.

> "GitHub Trending이 무엇을 보여준다면, OI는 왜를 알려준다."

**개발 기간**: 2026.04.28 ~ 2026.05.10
**배포**: AWS ap-northeast-2, EC2 t3.medium + Docker Compose

---

## Stack

| 영역 | 도구 |
|---|---|
| Streaming | Redpanda (Kafka API) |
| Batch | Apache Airflow (LocalExecutor) |
| Lake | S3 (Bronze/Silver/Gold) + Athena + Glue |
| Search | OpenSearch (oi-repo-daily 인덱스) |
| LLM | Amazon Bedrock (Claude Haiku 4.5) |
| Backend | FastAPI + SQLAlchemy + Jinja2 |
| Admin | Streamlit (IP 화이트리스트) |
| DB | PostgreSQL |
| Container | Docker Compose |

---

## Architecture

```
[GitHub Events API] ──60s polling, ETag──> [collector]
                                                │
                                                ▼
                                  [Redpanda gh.events.live]
                                                │
                                                ▼
                                  [bronze_writer (consumer)]
                                                │
                                                ▼
[Airflow gharchive_to_bronze] ──> [S3 bronze/{archive,live}/]
                                                │
                                  [bronze_to_silver: UNION + dedup + lang norm]
                                                ▼
                                       [S3 silver/events/]
                                                │
                          [silver_to_gold: 6 marts in parallel]
                                                ▼
   gold_repo_daily · gold_actor_daily · gold_repo_acceleration
   gold_repo_anomaly · gold_repo_hourly · gold_language_activity
                                                │
                  ┌─────────────────────────────┼──────────────────────────┐
                  ▼                             ▼                          ▼
          [FastAPI 사용자]              [Bedrock Haiku]           [Streamlit 운영]
                                              │
                                  [OpenSearch oi-repo-daily]
                                              │
                                  [categorize_daily DAG: F5]
```

상세 설계는 [OI_PROJECT_HANDOFF.md](OI_PROJECT_HANDOFF.md) 참조.

---

## 핵심 기능

| ID | 기능 | 데이터 소스 |
|---|---|---|
| F1 | AI 인사이트 카드 | Bedrock + Athena Gold rank_score |
| F2 | 가속도 점수 (전일 대비) | gold_repo_acceleration |
| F3 | 이상 급등 탐지 (z-score) | gold_repo_anomaly |
| F4 | 언어 활동 히트맵 | gold_language_activity |
| F5 | 카테고리별 트렌딩 | OpenSearch + Bedrock 자동 분류 |
| F6 | Repo 프로필 페이지 | gold_repo_hourly + acceleration + anomaly |

추가:
- 인증/회원가입/비밀번호 재설정/탈퇴 (사용자/관리자)
- 검색 (OpenSearch full-text)
- 운영자 콘솔 (Gold 직접 조회, 검색, 인사이트 히스토리)

---

## Quick Start (로컬 개발)

전제: Docker Desktop, AWS CLI v2 (`~/.aws/credentials`).

```bash
# 1. 환경 변수
cp .env.example .env
# .env 의 모든 값을 채우기 (특히 JWT_SECRET_KEY, AIRFLOW_FERNET_KEY,
#  AWS_S3_BUCKET, ADMIN_PASSWORD)

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

## 데이터 파이프라인 운영

### 1) 실시간 수집이 도는지 확인

```bash
docker compose logs -f collector | head -20
# "📦 cycle: published=N modified_pages=K/3 took=Xs" 로그가 60초마다 떠야 함

docker compose logs -f bronze_writer | head -20
# "Uploaded N events -> s3://bucket/bronze/live/year=.../batch_*.jsonl.gz"
```

S3 확인:
```bash
aws s3 ls s3://${AWS_S3_BUCKET}/bronze/live/ --recursive | tail -5
```

### 2) Bronze archive 백필 (GHArchive)

Airflow UI 에서 `gharchive_to_bronze` DAG 활성화. 시간당 자동 실행. 과거 백필:

```bash
docker compose exec airflow-scheduler airflow dags backfill \
    gharchive_to_bronze \
    --start-date 2026-04-29T00:00:00+00:00 \
    --end-date   2026-04-29T23:00:00+00:00
```

### 3) Silver/Gold 빌드

`bronze_to_silver` 와 `silver_to_gold` 는 daily schedule. 즉시 빌드 필요 시:

```bash
# 어제 데이터를 다시 만들기
docker compose exec airflow-scheduler airflow dags trigger \
    bronze_to_silver --conf '{"data_interval_start": "2026-04-29T00:00:00+00:00"}'
```

`silver_to_gold` 는 silver 가 갱신되면 Dataset 트리거로 자동 실행됨.

### 4) F1 인사이트 + F5 카테고리

사용자가 `/dashboard?date=YYYY-MM-DD` 에 접근하면:

1. `/api/insights/daily` 호출 → Athena Gold top-10 조회 → Bedrock 호출 → OpenSearch 인덱싱
2. 인덱싱 완료 후 `categorize_daily` DAG 가 Dataset 트리거로 자동 실행 → 카테고리 보강

수동 트리거:
```bash
# admin JWT 가 필요. 운영 콘솔에서 호출하거나 직접:
curl -X POST 'http://localhost:8000/api/admin/category/run?date=2026-04-29' \
     -H "Cookie: oi_token=..."
```

### 5) 데이터 검증

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

`deploy.sh` 가 하는 일:
1. `git fetch && git reset --hard origin/main`
2. `docker compose down --remove-orphans`
3. (옵션) `docker compose build --pull`
4. `docker compose up -d`
5. fastapi healthcheck 대기
6. (옵션) `bootstrap_athena.py` 실행
7. `init_admin.py` 멱등 실행

---

## Project Structure

```
.
├── .env.example                   # 모든 환경 변수 템플릿
├── docker-compose.yml             # 12개 서비스 (Redpanda, FastAPI, Airflow, OpenSearch, ...)
├── README.md
├── OI_PROJECT_HANDOFF.md          # 설계 문서 (이 README 의 원본)
├── backend/                       # FastAPI 사용자 대시보드 + 인증
│   ├── main.py
│   ├── auth.py
│   ├── models.py
│   ├── routers/
│   │   ├── pages.py               # GET 페이지
│   │   ├── auth_router.py         # signup/login/forgot/reset/settings/delete
│   │   ├── admin_router.py        # 사용자 관리 (승인/거부/삭제)
│   │   ├── insights.py            # F1 (Bedrock)
│   │   ├── repo.py                # F6 (repo 프로필)
│   │   ├── language.py            # F4 (히트맵)
│   │   ├── search.py              # OpenSearch 검색
│   │   └── category.py            # F5 카테고리 수동 트리거
│   ├── services/                  # athena/bedrock/opensearch/insights/repo/category
│   ├── prompts/                   # repo_insight_v1.md, repo_category_v1.md
│   ├── templates/                 # Jinja2 (다크 톤)
│   └── static/css/                # base/home/repo/admin/auth/settings
├── collector/                     # GitHub Events API → Redpanda
├── bronze_writer/                 # Redpanda → S3 Bronze
├── streamlit_admin/               # 운영자 콘솔
├── airflow/dags/
│   ├── gharchive_to_bronze.py     # 시간별 GHArchive → S3
│   ├── bronze_to_silver.py        # Live + Archive 통합 + dedup + 언어 정규화
│   ├── silver_to_gold.py          # 6개 Gold 마트 동시 빌드
│   └── categorize_daily.py        # F5 카테고리 자동 분류 (Dataset 트리거)
├── sql/ddl/                       # ${BUCKET} 변수화된 모든 EXTERNAL TABLE DDL
└── scripts/
    ├── bootstrap_athena.py        # Glue DB + Workgroup + DDL 자동 적용
    ├── deploy.sh                  # EC2 배포
    └── run_validation_queries.sh  # 데이터 품질 6개 룰
```

---

## 트러블슈팅

| 증상 | 원인 / 해결 |
|---|---|
| `/dashboard` 빈 화면, 401 | admin 승인 필요. `/admin` 에서 승인 후 다시 로그인 |
| Athena `Table not found: oi.bronze_archive` | `python3 scripts/bootstrap_athena.py` 미실행 |
| `bronze_to_silver` 0행 | bronze_archive 또는 bronze_live S3 prefix 비어있음. collector/gharchive DAG 확인 |
| F4 히트맵 안 보임 | `silver_to_gold` 의 `insert_language_activity` 태스크 확인. silver 의 repo_language 가 모두 'Unknown' 이면 히트맵 한 행만 나옴 |
| `categorize_daily` skip | OpenSearch 에 해당 날짜 색인 없음. 사용자가 `/api/insights/daily?date=YYYY-MM-DD` 한 번 호출해야 함 |
| Bedrock `AccessDenied` | IAM Role 에 `BedrockFullAccess` 필요. 모델 액세스 콘솔에서 Haiku 사용 신청 |
| `kafka.errors.NoBrokersAvailable` | `redpanda` 컨테이너 healthcheck 실패. `docker compose logs redpanda` |

---

## License

Private project (개발 중)
