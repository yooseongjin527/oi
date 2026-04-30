# Opensource Insights (OI) — 프로젝트 핸드오프

> 새 Claude 채팅으로 작업 이어가기 위한 컨텍스트 문서.
> 이전 채팅에서 설계 완료된 내용 요약. 코드 구현 단계부터 시작.

---

## 1. 프로젝트 한 줄 정의

**OI**는 GitHub 실시간 이벤트 스트림과 시간별 아카이브 데이터를 통합 분석해 트렌딩 repo와 그 부상 원인을 자연어로 제공하는 데이터 분석 서비스.

> "GitHub Trending이 무엇을 보여준다면, OI는 왜를 알려준다."

**개발 기간**: 2026.04.28 ~ 2026.05.08 (11일)
**예산**: $40 이내
**개발 환경**: Windows + WSL2 Ubuntu 24.04, VSCode (Remote-WSL)
**배포**: AWS ap-northeast-2, EC2 t3.medium 1대 + Docker Compose

---

## 2. 핵심 기능 (사용자 대시보드)

| ID | 기능 | 설명 |
|---|---|---|
| F1 | AI 인사이트 카드 | 트렌딩 repo의 부상 원인을 LLM(Bedrock Claude Haiku)이 자연어로 요약 |
| F2 | 가속도 점수 | 5분 단위 변화율 기반 트렌딩 Top 10 라이브 차트 |
| F3 | 이상 급등 탐지 | z-score 3 이상 급등 감지, 봇/바이럴 unique_actors 비율로 구분 |
| F4 | 언어 활동 히트맵 | 시간대별 언어별 이벤트 활동량 시각화 |
| F5 | 카테고리별 트렌딩 | AI/ML, Web, Infra, Game, CLI/Tool 등 LLM 자동 분류 |
| F6 | Repo 프로필 페이지 | 24h/7d 활동 추이, contributor, 관련 repo |

**운영자 콘솔 (Streamlit, 별도)**: 데이터 품질 모니터링, DAG 실행 현황, 비용 트래커, 6개 검증 룰. IP 화이트리스트 통제.

---

## 3. 아키텍처

### 3.1 데이터 파이프라인 (Kappa 변형 + 메달리온)

```
[GitHub Events API] ──(60s polling, ETag)──> [Python Collector]
                                                      │
                                                      ▼
                                              [Redpanda gh.events.live]
                                                      │
                            ┌─────────────────────────┼─────────────────────────┐
                            ▼                         ▼                         ▼
                    [Kafka Consumer]          [Anomaly Detector]        [Live Counter]
                            │                         │                         │
                            ▼                         ▼                         ▼
                       [S3 Bronze] ◄──── [Airflow GHArchive DAG (hourly)]
                            │
                       (Athena CTAS, hourly)
                            ▼
                       [S3 Silver] (Parquet, dedup, 정규화)
                            │
                       (Athena CTAS, hourly)
                            ▼
                       [S3 Gold]
                            │
              ┌─────────────┼─────────────┬──────────────┐
              ▼             ▼             ▼              ▼
        repo_metrics_5min  repo_velocity  language_activity  repo_enriched
                                                              │
                                                  [Bedrock Claude Haiku]
                                                  (Top 100 repo, 일 1~2회)
                            │
                            ▼
                  [FastAPI 사용자 대시보드]   [Streamlit 운영자 콘솔]
```

### 3.2 메달리온 계층 정책

**Bronze**: raw JSON (.jsonl.gz). `s3://bucket/bronze/{source}/year=/month=/day=/hour=/`. 14일 후 라이프사이클 삭제.

**Silver**: Parquet (Snappy). 통일 스키마. **Live 우선 dedup** (event_id 동일 시 source='live' 우선). 언어명 정규화. 파티션: `event_date / event_hour` (created_at 기준).

**Gold (4개 테이블)**:
- `repo_metrics_5min`: repo × 5분 단위 집계 (star/fork/PR/commit/unique_actors)
- `repo_velocity`: M1에서 윈도우 함수로 derive (z_score, anomaly_flag)
- `language_activity`: 시간 × 언어 매트릭스
- `repo_enriched`: Top 100 repo의 카테고리 + AI 인사이트 (Bedrock 캐시)

### 3.3 인프라

**VPC**: `oi-vpc` (10.0.0.0/16)
- Public subnet × 2 AZ (oi-public-2a, 2c) — EC2 위치
- Private subnet × 2 AZ (예약, 미래 RDS)
- Internet Gateway
- **S3 Gateway Endpoint (무료)** — 데이터 전송비 0
- NAT Gateway 없음 (비용 회피)

**EC2**: t3.medium, Ubuntu 24.04, 30GB gp3, Elastic IP, IAM Role `oi-ec2-role`

**Security Group `oi-sg` 인바운드**:
- 22 (SSH): My IP
- 80, 443 (HTTP/HTTPS): 0.0.0.0/0
- 8000 (FastAPI), 8080 (Airflow), 8088 (Redpanda Console), 8501 (Streamlit): My IP

**IAM Role 정책**: S3FullAccess, AthenaFullAccess, GlueConsoleFullAccess, BedrockFullAccess

---

## 4. 기술 스택

| 영역 | 도구 |
|---|---|
| 스트리밍 | Redpanda (Kafka API) |
| 배치 오케스트레이션 | Apache Airflow (LocalExecutor) |
| 데이터 레이크 | Amazon S3 |
| 쿼리 엔진 | Amazon Athena |
| 스키마 카탈로그 | AWS Glue Crawler |
| LLM 추론 | Amazon Bedrock (Claude Haiku) |
| 사용자 백엔드 | FastAPI + SQLAlchemy + Jinja2 |
| 운영자 콘솔 | Streamlit |
| 데이터베이스 | PostgreSQL (사용자 + Airflow metadata) |
| 시각화 (사용자) | Plotly.js, Chart.js |
| 컨테이너 | Docker Compose (단일 호스트) |

**제외한 도구 (의도적 트레이드오프)**:
- Kinesis/Firehose (시간당 과금 → Redpanda로 대체)
- Flink (KPU 비용 → Lambda + Athena로 충분)
- OpenSearch/ELK (풀텍스트 불필요 → Athena 적합)
- EKS/ArgoCD (5일 내 운영 부담)
- Streamlit을 사용자 대시보드에는 미사용 (JWT 통합 부담)

---

## 5. 보안 및 인증

**일반 사용자 (FastAPI)**:
- 회원가입 → status='pending' → admin 승인 → status='approved'
- JWT (httpOnly + SameSite=Lax 쿠키)
- bcrypt 패스워드 해싱
- `/dashboard/*` 라우트는 FastAPI Depends로 토큰 검증 강제
- 미인증 직접 접근 → 401

**운영자 (Streamlit, 8501 포트)**:
- IP 화이트리스트만 (Streamlit 자체 인증 미사용)
- 읽기 전용 (메트릭 조회만)

---

## 6. 디자인 톤

**메인 사이트 (다크)**:
- 배경 #0A0A0F, 서피스 #15151D, 보더 rgba(255,255,255,0.08)
- 텍스트 primary #F5F5F7, secondary #A0A0AB
- 액센트 (보라) #7F77DD, hover #9A93E8
- 폰트: Pretendard (한국어), JetBrains Mono (모노)

**운영자 콘솔 (Streamlit 기본 라이트)**: 다크 톤 흉내 안 내고 Streamlit 기본 톤으로 갈 것. 의도적 시각 분리.

---

## 7. 일정 (Day별 마일스톤)

| Day | 기간 | 목표 |
|---|---|---|
| Day 1 | 04.28 | 인프라 + 인증 + 메인 페이지 + Streamlit placeholder |
| Day 2 | 04.29 | Bronze 적재 (Kafka consumer) + GHArchive DAG + Silver CTAS |
| Day 3 | 04.30 | Gold 4개 마트 구축 |
| Day 4 | 05.01 | F2 가속도, F3 이상 탐지, F4 히트맵 화면 |
| Day 5 | 05.02 | Bedrock 통합, F1 인사이트, F5 카테고리, M4 enrichment |
| Day 6 | 05.05 | F6 repo 프로필 페이지 |
| Day 7 | 05.06 | 데이터 품질 검증 DAG + Streamlit 운영 콘솔 실 콘텐츠 |
| Day 8 | 05.07 | 폴리싱, Nginx + HTTPS, 에러 핸들링 |
| Day 9 | 05.08 | README, 발표 자료, 리허설, 최종 배포 |

---

## 8. 개발 워크플로우

```
[로컬 WSL Ubuntu] ──push──> [GitHub] <──pull── [EC2]
        │                                          │
   docker compose up                          docker compose up
   (개발 + 검증)                              (배포 운영)
```

- **로컬 (WSL Ubuntu)**: docker-compose로 6개 컨테이너 동작 확인
- **GitHub**: single source of truth, Deploy Key (read-only)로 EC2 연결
- **EC2**: `~/oi/deploy.sh` (`git pull && docker compose up -d --build`)
- **수정 흐름**: 로컬 코드 수정 → 검증 → push → EC2에서 deploy.sh 실행

---

## 9. Day 1 작업 상세 (현재 진행 단계)

### Phase 0 — WSL2 + 개발 환경 (60분, 1회만)

1. PowerShell 관리자 권한: `wsl --install -d Ubuntu-24.04`
2. Docker Desktop 설치 + Settings → Resources → WSL Integration → Ubuntu-24.04 토글
3. Docker Desktop 메모리 6~8GB 할당
4. VSCode + Remote-WSL 확장 설치
5. WSL Ubuntu에 git, gh, python3, build-essential 설치

**중요**: 모든 작업은 WSL Ubuntu 터미널에서. PowerShell 안 씀. 작업 디렉토리는 `~/projects/oi` (WSL 홈, 절대 `/mnt/c/...` 쓰지 말 것 — 도커 볼륨 성능 + 권한 문제).

### Phase 1 — 로컬 개발 (5~6시간)

**디렉토리 구조**:
```
~/projects/oi/
├── .env (gitignore, 로컬 시크릿)
├── .env.example
├── .gitignore
├── docker-compose.yml
├── README.md
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── database.py
│   ├── models.py
│   ├── auth.py
│   ├── init_admin.py
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── pages.py
│   │   ├── auth_router.py
│   │   └── admin_router.py
│   ├── templates/
│   │   ├── base.html
│   │   ├── home.html
│   │   ├── login.html
│   │   ├── signup.html
│   │   ├── pending.html
│   │   ├── admin.html
│   │   └── dashboard.html (placeholder)
│   └── static/
│       ├── css/{base,home,auth,admin}.css
│       └── js/home.js
├── collector/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── streamlit_admin/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py (placeholder)
└── airflow/
    └── dags/ (Day 2부터 사용)
```

**docker-compose 서비스 6개**:
- `redpanda` (9092, 9644)
- `redpanda-console` (8088)
- `postgres` (5432)
- `fastapi` (8000) — 메인 사이트
- `collector` — GitHub Events API → Redpanda
- `streamlit_admin` (8501) — 운영자 콘솔

> **Airflow는 Day 1엔 docker-compose에서 제외. Day 2부터 추가.** 메모리 절약.

**Day 1 검증 항목**:
- [ ] http://localhost:8000 메인 페이지 다크 톤 렌더링
- [ ] /signup → /pending → admin 승인 → 일반 로그인 → /dashboard 흐름
- [ ] /dashboard 미인증 직접 접근 시 401
- [ ] http://localhost:8501 Streamlit placeholder 동작
- [ ] http://localhost:8088 Redpanda Console에 gh.events.live 토픽 + 메시지 흐름
- [ ] collector가 60초마다 GitHub 이벤트 수집 → Redpanda 적재

### Phase 2 — GitHub 연결 (30분)

```bash
gh auth login
git add . && git commit -m "Day 1: ..."
gh repo create oi --private --source=. --remote=origin --push
```

### Phase 3 — AWS + EC2 배포 (4~5시간)

1. IAM Role `oi-ec2-role` 생성 (4개 정책)
2. VPC 생성 (Console "VPC and more", 10.0.0.0/16, 2 AZ, NAT Gateway 없음, S3 Gateway Endpoint 포함)
3. Security Group `oi-sg` 생성 (위 인바운드 규칙)
4. EC2 인스턴스 생성 (t3.medium, Ubuntu 24.04, 30GB gp3, oi-public-2a, oi-ec2-role attach, oi-sg)
5. Elastic IP attach
6. S3 버킷 `oi-data-lake-{suffix}` 생성
7. EC2 SSH 접속 (`~/.ssh/oi-key.pem`, chmod 400)
8. EC2에 Docker, AWS CLI v2, swap 2GB 설치
9. EC2에서 SSH key 생성 → GitHub Deploy Key 등록 (read-only)
10. `git clone` → `.env` 채우기 → `docker compose up -d --build` → admin 계정 생성
11. 외부에서 http://<EC2_IP>:8000 검증
12. `~/oi/deploy.sh` 작성 (`git pull && docker compose up -d --build`)

---

## 10. 핵심 코드 명세

### 10.1 DB 스키마 (FastAPI 사용자)

```python
class UserStatus(str, Enum): pending, approved, rejected
class UserRole(str, Enum): user, admin

class User(Base):
    id, email (unique), username (unique), password_hash,
    status (default pending), role (default user),
    created_at, approved_at
```

### 10.2 인증 흐름

- `/signup` → User(status=pending) 생성 → /pending 리다이렉트
- `/login` → status=approved만 통과 → JWT를 `oi_token` httpOnly 쿠키로 발급
- `/logout` → 쿠키 삭제
- `/admin` → require_admin 데코레이터 (role==admin만)
- `/dashboard/*` → get_current_user 데코레이터 (status==approved만)

### 10.3 Collector (GitHub Events API)

- `https://api.github.com/events?per_page=100`
- ETag + If-None-Match 헤더로 rate limit 보호
- X-Poll-Interval 헤더 준수 (보통 60초)
- 메시지 key = `repo_id` (같은 repo 같은 파티션 → 순서 보장)
- Producer config: `acks='all'`, `linger_ms=100`

### 10.4 Bronze 적재 (Day 2)

Kafka consumer가 5초 poll, 1분 또는 500건 단위로 buffered write:
```
s3://bucket/bronze/live_events/year=YYYY/month=MM/day=DD/hour=HH/batch_*.jsonl.gz
```

### 10.5 Silver CTAS (Day 2, Live 우선 dedup)

```sql
INSERT INTO silver.events
SELECT ..., language_normalized AS repo_language, ...
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id 
      ORDER BY CASE source WHEN 'live' THEN 0 ELSE 1 END, ingested_at ASC
    ) AS rn
  FROM bronze.events_unified
  WHERE event_date = date '{{ ds }}'
    AND event_hour = {{ logical_date.hour }}
) WHERE rn = 1;
```

언어 정규화 매핑: javascript→JavaScript, js→JavaScript, typescript→TypeScript, python→Python, rust→Rust, go/golang→Go, c++/cpp→C++, ''→Unknown.

### 10.6 Gold 마트 SQL 골격 (Day 3)

**M1 repo_metrics_5min**:
```sql
SELECT repo_id, max(repo_name) AS repo_name,
  date_trunc('minute', created_at) - interval '1' minute * (minute(created_at) % 5) AS ts_5min,
  count_if(event_type = 'WatchEvent') AS star_count,
  count_if(event_type = 'ForkEvent') AS fork_count,
  count_if(event_type = 'PullRequestEvent') AS pr_count,
  count_if(event_type = 'PushEvent') AS commit_count,
  approx_distinct(actor_id) AS unique_actors,
  max(repo_language) AS primary_language
FROM silver.events
WHERE event_date = date '{{ ds }}'
GROUP BY 1, 3;
```

**M2 repo_velocity** (M1 derive, 24시간 baseline):
```
z_score = (star_count - star_avg_24h) / star_std_24h
anomaly_flag = z_score > 3
delta_stars_per_hour = (star_count - star_1h_ago) * 12
```

**M3 language_activity**: silver에서 시간 × 언어 집계.

**M4 repo_enriched**: Day 5에 Bedrock 호출, repo_id+date 캐싱.

### 10.7 Bedrock 인사이트 프롬프트 (Day 5)

```
[repo: {repo_name}]
[지난 24h 지표: stars +{delta}, forks +{delta}, PRs +{delta}]
[평소 평균 stars/h: {baseline}]
[최근 commit messages 5개: {commits}]

위 데이터를 바탕으로 이 repo가 왜 트렌딩 중인지 한국어로 2~3문장 요약해줘.
원인이 명확하지 않으면 "활동량 증가가 뚜렷하지만 명확한 트리거는 보이지 않음"으로 답해.
```

모델: `anthropic.claude-3-5-haiku-20241022-v1:0`. max_tokens=300.

---

## 11. 비용 통제 핵심 전략

- Redpanda를 EC2에 직접 (MSK 시간당 과금 회피)
- S3 Gateway Endpoint (전송비 0)
- NAT Gateway 미사용
- Athena Parquet + 파티셔닝 (스캔량 최소화)
- Bedrock Top 100 repo만 호출, repo_id+date 캐싱
- 개발 비활용 시간 EC2 stop

**예산**: $11 EC2 + $5 Athena + $5 Bedrock + $2 데이터 전송 + 기타 = $26 예상, $14 버퍼 = $40 cap.

---

## 12. 위험 요소 및 대응

| 위험 | 대응 |
|---|---|
| GitHub API rate limit | ETag 캐싱, X-Poll-Interval 준수, 60초 polling |
| EC2 메모리 부족 | 2GB swap, t3.medium, Day 1엔 Airflow 제외 |
| Bedrock 비용 폭증 | Top 100 제한, repo_id+date 캐싱 |
| Athena 스캔 과다 | Parquet, 파티션 프루닝, LIMIT |
| 5일 내 6개 기능 미완성 | F6 stretch goal, 사전 컷 우선순위 |
| GHArchive 누락 | Live 우선 dedup으로 보완, backfill DAG |

---

## 13. Memory 관련 주의사항 (개인 컨텍스트)

- **Windows + WSL2 Ubuntu** 환경 (이전 raffle-app 프로젝트에서 PowerShell heredoc/문법 충돌 빈번 → WSL로 회피 결정)
- 작업 디렉토리는 `~/projects/oi` (WSL 홈), `/mnt/c/...` 절대 사용 금지 (도커 볼륨 성능 + 권한 문제)
- MacBook Pro M5는 별도 환경 (이 프로젝트는 Windows에서 진행)
- 한국어로 소통

---

## 14. 다음 채팅 시작 시 할 일

1. 이 문서 첨부
2. "Day 1 Phase 1부터 시작할게요. 먼저 Block 1.4의 docker-compose.yml부터 작성해주세요." 같은 식으로 진행 단계 명시
3. 코드 구현 단계라 디자인/아키텍처 논의는 이미 끝났음을 명시
4. 막히는 부분만 핀포인트로 질문

각 Day 끝날 때마다 진행 상황을 이 문서에 추가 업데이트하면 다음 Day로 넘어갈 때 좋음.
