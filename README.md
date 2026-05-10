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

빠른 명령 요약 (이미 인프라 구축 완료 후):
```bash
# EC2 SSH 접속 후 일반 배포
~/oi/scripts/deploy.sh

# 최초 배포 또는 새 DDL 추가 시
~/oi/scripts/deploy.sh --bootstrap

# 코드만 변경 (이미지 재빌드 스킵)
~/oi/scripts/deploy.sh --no-build
```

처음부터 셋업하는 풀 가이드는 본 README 의 [EC2 + DuckDNS 배포 가이드](#ec2--duckdns-배포-가이드-처음부터-끝까지) 섹션 참조.

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

## EC2 + DuckDNS 배포 가이드 (처음부터 끝까지)

> 운영 형태: **단일 EC2 t3.medium + Nginx reverse proxy + DuckDNS 무료 도메인 + Let's Encrypt HTTPS**
> 비용 합산 (17일 운영 기준): 약 **$30** (도메인·CDN·LB 모두 $0)

### 아키텍처

```
[인터넷]
   │
   ▼  HTTPS 443 (Let's Encrypt 인증서, 90일 자동 갱신)
[EC2 oi-prod]  (Public subnet, Elastic IP)
   │
   ├─ Nginx 80/443  ← reverse proxy + HTTPS termination
   │     │
   │     ▼
   ├─ FastAPI 8000        (사용자 대시보드, SG 차단)
   ├─ Streamlit 8501      ┐
   ├─ Airflow 8090        ├─ My IP 만 직접 접근 (운영자)
   └─ Redpanda Console 8088 ┘

DuckDNS DNS  ─────────┐
                      ▼
oi-prod.duckdns.org → <EC2_Elastic_IP>
```

### 사전 준비 (AWS Console 한 번 셋업)

핸드오프 §3.3 의 인프라 그대로:
- VPC `oi-vpc` (10.0.0.0/16, public subnet 2-AZ, S3 Gateway Endpoint)
- IAM Role `oi-ec2-role` (`S3FullAccess`, `AthenaFullAccess`, `GlueFullAccess`, `BedrockFullAccess`, **+ `s3:ListBucket` on bucket**)
- Security Group `oi-sg` 인바운드:
  - 22 (SSH): My IP
  - **80, 443 (HTTP/HTTPS): 0.0.0.0/0**  ← 외부 공개
  - 8090 (Airflow), 8501 (Streamlit), 8088 (Redpanda): My IP
  - 8000 은 **열지 않음** (Nginx 만 localhost 로 접근)
- S3 버킷 `oi-data-lake-{suffix}`
- Bedrock Console → Model access → Claude Haiku 4.5 활성화

---

### Step 1 — 코드 GitHub push

Local WSL 에서:
```bash
cd ~/projects/oi
git add -A
git commit -m "Ready for production deployment"
git push
```

---

### Step 2 — EC2 인스턴스 생성

AWS Console → EC2 → Launch instance:

| 항목 | 값 |
|---|---|
| Name | `oi-prod` |
| AMI | Ubuntu Server 24.04 LTS (x86_64) |
| Instance type | **t3.medium** (4GB RAM 필수) |
| Key pair | 새로 생성 → `oi-key.pem` 다운로드 |
| VPC | `oi-vpc` |
| Subnet | `oi-public-2a` |
| Auto-assign public IP | Enable |
| Security group | `oi-sg` (기존) |
| Storage | 30 GB gp3 |
| IAM instance profile | `oi-ec2-role` |

Launch → Running 확인.

**Elastic IP** 할당 (재부팅해도 IP 안 바뀌게):
- EC2 → Elastic IPs → Allocate → Associate to `oi-prod`
- 그 IP 가 `<EC2_IP>` (이후 `dig` / DuckDNS 입력에 사용)

---

### Step 3 — EC2 시스템 셋업

WSL Ubuntu 에서:
```bash
chmod 400 ~/Downloads/oi-key.pem
ssh -i ~/Downloads/oi-key.pem ubuntu@<EC2_IP>
```

EC2 안에서:
```bash
# Docker + Compose v2 + AWS CLI v2
sudo apt update && sudo apt upgrade -y
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
sudo apt install -y unzip nginx certbot python3-certbot-nginx
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install && rm -rf aws awscliv2.zip

# Swap 2GB (t3.medium 메모리 보호)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Group 적용 위해 재로그인
exit
ssh -i ~/Downloads/oi-key.pem ubuntu@<EC2_IP>

# 검증
docker --version
aws --version
nginx -v
certbot --version
free -h     # swap 2GB 보여야 함
```

---

### Step 4 — GitHub 코드 EC2 로

```bash
# EC2 에 SSH key 생성
ssh-keygen -t ed25519 -C "oi-ec2" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub
# 출력 복사
```

GitHub:
- Repo `oi` → Settings → Deploy keys → **Add deploy key**
- Title: `oi-prod-ec2`, Key: 위 복사한 공개키, **Allow write access OFF** (read-only)

EC2:
```bash
cd ~
git clone git@github.com:<owner>/oi.git
cd ~/oi
```

---

### Step 5 — `.env` 작성

```bash
cd ~/oi
cp .env.example .env
nano .env
```

핵심 값 (강력한 비밀번호 / 키 생성):
```bash
# AWS — IAM Role 사용하므로 Access Key 비워둠 (boto3 IMDS 자동)
AWS_REGION=ap-northeast-2
AWS_DEFAULT_REGION=ap-northeast-2
AWS_S3_BUCKET=oi-data-lake-XXXX           # 본인 버킷명
AWS_ACCESS_KEY_ID=                         # 비워둠
AWS_SECRET_ACCESS_KEY=

# 보안 (반드시 새로 생성)
JWT_SECRET_KEY=$(openssl rand -hex 32)
ADMIN_PASSWORD=<강력한 비밀번호>
AIRFLOW_FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AIRFLOW_ADMIN_PASSWORD=<강력한 비밀번호>

# GitHub PAT (collector rate limit 5000/h 위해)
GITHUB_TOKEN=ghp_xxxxx

# Bedrock
OI_BEDROCK_REGION=ap-northeast-2
OI_BEDROCK_MODEL_ID=global.anthropic.claude-haiku-4-5-20251001-v1:0

# 나머지 기본값 그대로
POSTGRES_USER=oi
POSTGRES_PASSWORD=<강력한 비밀번호>
POSTGRES_DB=oi
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
OI_ATHENA_WORKGROUP=oi-workgroup
OI_ATHENA_OUTPUT=s3://${AWS_S3_BUCKET}/athena-results/
OI_GLUE_DATABASE=oi
OI_OPENSEARCH_HOST=http://opensearch:9200
KAFKA_BOOTSTRAP_SERVERS=redpanda:9092
KAFKA_TOPIC_LIVE=gh.events.live

# 운영자 콘솔 IP 화이트리스트 (My IP)
ADMIN_IP_WHITELIST=<your.public.ip.addr>
```

`openssl rand` / `Fernet.generate_key()` 출력을 직접 복사해서 채우세요.

---

### Step 6 — Athena bootstrap + 컨테이너 기동

```bash
cd ~/oi
set -a && source .env && set +a

# Athena 인프라 (Glue DB + Workgroup + 모든 DDL — 멱등)
python3 scripts/bootstrap_athena.py

# Docker 컨테이너 빌드 + 기동
docker compose up -d --build

# 헬스체크 (~1-2분 대기)
sleep 90
docker compose ps
curl -fsS http://localhost:8000/health
# {"status":"ok"}

# 초기 admin 계정 (1회만, 멱등)
docker compose exec fastapi python init_admin.py
```

---

### Step 7 — 모든 DAG unpause + 첫 데이터 적재

```bash
docker compose exec airflow-scheduler bash -c '
  airflow dags unpause gharchive_to_bronze
  airflow dags unpause bronze_to_silver
  airflow dags unpause silver_to_gold
  airflow dags unpause silver_to_gold_hourly
  airflow dags unpause categorize_daily
'

# 어제 분 silver/gold 즉시 빌드 (사용자한테 보여주려고)
YESTERDAY=$(date -u -d '1 day ago' +%Y-%m-%d)
docker compose exec airflow-scheduler airflow dags backfill bronze_to_silver \
    --start-date ${YESTERDAY}T00:30:00 --end-date ${YESTERDAY}T00:30:00 \
    --reset-dagruns --yes

# bronze_to_silver 끝나면 (~3-5분):
docker compose exec airflow-scheduler airflow dags backfill silver_to_gold \
    --start-date ${YESTERDAY}T01:00:00 --end-date ${YESTERDAY}T01:00:00 \
    --reset-dagruns --yes
```

---

### Step 8 — DuckDNS 무료 도메인 발급

1. https://www.duckdns.org 접속 → **GitHub 또는 Google 로그인**
2. 페이지 상단의 `domain` 입력란에 원하는 이름 입력 (예: `oi-prod`) → **add domain**
3. **token** 메모: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
4. 추가된 `oi-prod.duckdns.org` 옆 **current ip** 칸에 EC2 Elastic IP 입력 → **update ip**

검증 (전파 ~1분):
```bash
dig oi-prod.duckdns.org +short
# 출력: <EC2_IP> 가 떠야 정상
```

#### (선택) IP 자동 갱신 cron — Elastic IP 사용하면 불필요
EIP 가 고정이라 안 해도 됨. 만약 향후 IP 바뀔 가능성 대비:
```bash
mkdir -p ~/duckdns
cat > ~/duckdns/duck.sh <<'EOF'
#!/bin/bash
TOKEN="여기에_토큰"
DOMAIN="oi-prod"
echo url="https://www.duckdns.org/update?domains=${DOMAIN}&token=${TOKEN}&ip=" \
  | curl -k -o ~/duckdns/duck.log -K -
EOF
chmod +x ~/duckdns/duck.sh
(crontab -l 2>/dev/null; echo "*/5 * * * * ~/duckdns/duck.sh >/dev/null 2>&1") | crontab -
```

---

### Step 9 — Nginx reverse proxy 설정

```bash
sudo tee /etc/nginx/sites-available/oi <<'EOF'
server {
    listen 80;
    server_name oi-prod.duckdns.org;

    # Let's Encrypt HTTP-01 challenge 가 사용 (Step 10)
    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Bedrock 호출 등 긴 응답 (~10s) 안전장치
        proxy_read_timeout 60s;
        proxy_send_timeout 60s;

        # 큰 파일 업로드 안 쓰지만 안전 마진
        client_max_body_size 5m;
    }
}
EOF

sudo ln -sf /etc/nginx/sites-available/oi /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx

# HTTP 검증 (HTTPS 전)
curl -I http://oi-prod.duckdns.org/health
# HTTP/1.1 200 OK
```

---

### Step 10 — Let's Encrypt HTTPS 인증서 발급

```bash
sudo certbot --nginx -d oi-prod.duckdns.org \
    --non-interactive --agree-tos -m your_email@example.com \
    --redirect
```
- `--redirect`: 80 → 443 자동 redirect 룰 추가
- 90일마다 자동 갱신 (`systemd timer` 등록됨)

검증:
```bash
curl -I https://oi-prod.duckdns.org/health
# HTTP/2 200
# server: nginx/...

# HTTP → HTTPS redirect 확인
curl -I http://oi-prod.duckdns.org
# HTTP/1.1 301 Moved Permanently
# Location: https://oi-prod.duckdns.org/
```

자동 갱신 동작 확인:
```bash
sudo systemctl status certbot.timer
sudo certbot renew --dry-run
# Congratulations, all simulated renewals succeeded
```

---

### Step 11 — Security Group 정리 (8000 직접 접근 차단)

EC2 → Security Groups → `oi-sg` → Edit inbound rules:

| Type | Protocol | Port | Source | 용도 |
|---|---|---|---|---|
| SSH | TCP | 22 | My IP | 관리자 SSH |
| HTTP | TCP | 80 | 0.0.0.0/0 | Nginx (Let's Encrypt + redirect) |
| HTTPS | TCP | 443 | 0.0.0.0/0 | Nginx HTTPS |
| Custom TCP | TCP | 8090 | My IP | Airflow UI |
| Custom TCP | TCP | 8501 | My IP | Streamlit |
| Custom TCP | TCP | 8088 | My IP | Redpanda Console |

**8000 은 인바운드 룰에 없어야 함** (Nginx 가 localhost 로 접근하니 외부에 노출 X).

다른 IP 에서 직접 접근 차단 검증 (Mobile network 같은 다른 네트워크):
```
http://<EC2_IP>:8000   → timeout (정상)
https://oi-prod.duckdns.org → 200 OK (정상)
```

---

### Step 12 — 외부 접속 + 동작 검증

브라우저:
```
https://oi-prod.duckdns.org
```
- 자물쇠 ✅
- 메인페이지 정상 렌더링
- 회원가입 → 다른 브라우저로 admin 로그인 (`admin` / `ADMIN_PASSWORD`) → /admin 에서 승인 → 일반 로그인 → /dashboard

---

### Step 13 — `deploy.sh` 자동 배포 흐름

코드 수정 → push → EC2 에서:
```bash
cd ~/oi
~/oi/scripts/deploy.sh           # 일반 배포 (git pull + docker rebuild + healthcheck)
~/oi/scripts/deploy.sh --no-build # 코드만 (이미지 재빌드 X, 빠름)
~/oi/scripts/deploy.sh --bootstrap # 새 DDL 추가 시
```

---

### Step 14 — 비용 모니터링 + 절약

**비용 모니터링**:
- AWS Cost Explorer 매일 확인 (EC2, Athena, Bedrock, S3)
- Billing → Budgets → **$40 cap 알림** 설정

**프로젝트 종료 후 정리**:
```bash
# EC2 terminate
aws ec2 terminate-instances --instance-ids <i-xxxxx>

# Elastic IP release
aws ec2 release-address --allocation-id <eipalloc-xxxxx>

# S3 데이터 정리 (필요 시)
aws s3 rm s3://${AWS_S3_BUCKET} --recursive
aws s3 rb s3://${AWS_S3_BUCKET}

# DuckDNS 도메인은 자동 무료 유지 (정리 불필요, 또는 duckdns.org 에서 delete)
```

---

### 종합 체크리스트 — 배포 후 1시간 내

- [ ] `https://oi-prod.duckdns.org` 자물쇠 ✅ + 메인페이지
- [ ] `http://oi-prod.duckdns.org` → 301 redirect → HTTPS
- [ ] 회원가입 → admin 승인 → /dashboard 접근
- [ ] LIVE PULSE 카드에 events/min 차오름 (collector 동작)
- [ ] `aws s3 ls s3://${AWS_S3_BUCKET}/bronze/live/year=$(date -u +%Y)/` 채워짐
- [ ] `aws s3 ls s3://${AWS_S3_BUCKET}/bronze/archive/year=$(date -u +%Y)/` 채워짐
- [ ] 어제 silver/gold partition 채워짐 (Step 7 의 backfill 결과)
- [ ] 대시보드에 어제 인사이트 + 가속도 Top 10 + 분야별 카테고리 정상 표시
- [ ] Repo 카드 클릭 → 상세 페이지 → 그날 단독 인사이트 + 24h 시계열
- [ ] EC2 8000 직접 접근 (다른 IP) → timeout (SG 차단 정상)
- [ ] Airflow UI (`http://<EC2_IP>:8090`) → My IP 만 접근

---

### 비용 정리 (실측)

t3.medium 24/7 + DuckDNS 무료 + Let's Encrypt 무료:

| 운영 일수 | EC2+EBS | Athena | S3 | Bedrock | 도메인 | **합계** |
|---|---|---|---|---|---|---|
| 11일 (개발만) | $11.9 | $4.7 | $1.5 | $0.2 | **$0** | **$18.3** |
| 14일 (+발표 3일) | $15.1 | $6.0 | $2.5 | $0.3 | **$0** | **$23.9** |
| 17일 (+발표 1주) | $18.4 | $7.3 | $3.8 | $0.4 | **$0** | **$29.9** |
| 21일 | $22.7 | $9.0 | $5.0 | $0.4 | **$0** | **$37.1** |

→ **23일 운영까지 $40 cap 안에서 가능** ✅

---

### 흔한 함정

| 증상 | 해결 |
|---|---|
| `certbot` 가 challenge 실패 | DNS 전파 미완. `dig oi-prod.duckdns.org` 가 EC2 IP 가리키는지 + SG 의 80 포트 0.0.0.0/0 확인 |
| `502 Bad Gateway` | docker compose 가 healthy 아님. `docker compose ps` + `curl http://localhost:8000/health` |
| `400 Bad Request` 또는 무한 redirect | uvicorn `--proxy-headers` 옵션 누락. backend/Dockerfile 확인 후 rebuild |
| Nginx reload 후도 옛 설정 | `sudo nginx -t` 로 syntax check, `sudo systemctl restart nginx` |
| Bedrock `AccessDenied` | IAM Role 에 `BedrockFullAccess` + Bedrock Console 모델 액세스 활성화 |
| `gharchive_to_bronze` 403 | EC2 IAM Role 에 `s3:ListBucket` 추가 (또는 코드 fallback 자동 처리됨) |
| 메모리 부족 OOM | `free -h` 로 swap 확인. opensearch heap 줄이기 (`OPENSEARCH_JAVA_OPTS=-Xms256m -Xmx256m`) |
| HTTPS 인증서 만료 알림 | certbot timer 확인: `sudo systemctl list-timers \| grep certbot` |

---

## License

Private project (개발 중)
