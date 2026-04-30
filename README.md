# Opensource Insights (OI)

GitHub 실시간 이벤트 스트림과 시간별 아카이브를 통합 분석해 트렌딩 repo와 그 부상 원인을 자연어로 제공하는 데이터 분석 서비스.

> "GitHub Trending이 무엇을 보여준다면, OI는 왜를 알려준다."

**개발 기간**: 2026.04.28 ~ 2026.05.08
**배포**: AWS ap-northeast-2, EC2 t3.medium + Docker Compose

---

## Stack

| 영역 | 도구 |
|---|---|
| Streaming | Redpanda (Kafka API) |
| Batch | Apache Airflow (LocalExecutor) |
| Lake | S3 (Bronze/Silver/Gold) + Athena + Glue |
| LLM | Amazon Bedrock (Claude Haiku) |
| Backend | FastAPI + SQLAlchemy + Jinja2 |
| Admin | Streamlit (IP 화이트리스트) |
| DB | PostgreSQL |
| Container | Docker Compose |

---

## Quick Start (로컬 개발)

전제: WSL2 Ubuntu 24.04, Docker Desktop (WSL Integration ON).

```bash
# 1. 환경 변수 설정
cp .env.example .env
# .env의 JWT_SECRET_KEY, POSTGRES_PASSWORD, ADMIN_PASSWORD 채울 것
# JWT_SECRET_KEY는: openssl rand -hex 32 결과 사용

# 2. 컨테이너 빌드 & 기동
docker compose up -d --build

# 3. 초기 admin 계정 생성 (최초 1회)
docker compose exec fastapi python init_admin.py

# 4. 상태 확인
docker compose ps
```

서비스 엔드포인트:

| 서비스 | URL | 용도 |
|---|---|---|
| 사용자 대시보드 | http://localhost:8000 | FastAPI |
| 운영자 콘솔 | http://localhost:8501 | Streamlit |
| Redpanda Console | http://localhost:8088 | Kafka 토픽 모니터링 |
| PostgreSQL | localhost:5432 | DB (외부 접속 시) |

---

## Architecture

```
[GitHub Events API] ─(60s polling, ETag)─> [Collector]
                                                │
                                                ▼
                                        [Redpanda gh.events.live]
                                                │
                          ┌─────────────────────┼─────────────────────┐
                          ▼                     ▼                     ▼
                    [Kafka Consumer]    [Anomaly Detector]    [Live Counter]
                          │
                          ▼
                     [S3 Bronze] ◄── [Airflow GHArchive DAG]
                          │
                     [Athena CTAS]
                          ▼
                     [S3 Silver]
                          │
                     [Athena CTAS]
                          ▼
                     [S3 Gold] ──> [Bedrock Claude Haiku]
                          │
                          ▼
                     [FastAPI 대시보드]   [Streamlit 운영 콘솔]
```

상세 설계는 `OI_PROJECT_HANDOFF.md` 참조.

---

## Development Workflow

```
[로컬 WSL Ubuntu] ──push──> [GitHub] <──pull── [EC2]
        │                                          │
   docker compose up                          docker compose up
   (개발 + 검증)                              (배포 운영)
```

수정 흐름: 로컬 코드 수정 → `docker compose up -d --build` 검증 → push → EC2에서 `~/oi/deploy.sh` 실행.

---

## Project Structure

```
.
├── .env.example
├── docker-compose.yml
├── backend/              # FastAPI (사용자 대시보드 + 인증)
│   ├── main.py
│   ├── routers/
│   ├── templates/
│   └── static/
├── collector/            # GitHub Events API → Redpanda
├── streamlit_admin/      # 운영자 콘솔 (IP 화이트리스트)
└── airflow/
    └── dags/             # Day 2부터 사용
```

---

## Day-by-Day Milestones

| Day | 목표 |
|---|---|
| Day 1 | 인프라 + 인증 + 메인 페이지 + Streamlit placeholder |
| Day 2 | Bronze 적재 + GHArchive DAG + Silver CTAS |
| Day 3 | Gold 4개 마트 |
| Day 4 | F2 가속도, F3 이상 탐지, F4 히트맵 |
| Day 5 | Bedrock + F1 인사이트 + F5 카테고리 |
| Day 6 | F6 repo 프로필 |
| Day 7 | 데이터 품질 검증 + 운영 콘솔 |
| Day 8 | 폴리싱, Nginx + HTTPS |
| Day 9 | 문서화, 발표, 최종 배포 |

---

## License

Private project (개발 중)
