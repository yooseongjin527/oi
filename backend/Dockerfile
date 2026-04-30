FROM python:3.12-slim

# 빌드 종속성 (psycopg2-binary가 컴파일 필요한 경우 대비)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# 시스템 패키지 (postgres 클라이언트, curl for healthcheck)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        curl \
        libpq5 \
 && rm -rf /var/lib/apt/lists/*

# requirements 먼저 복사해서 layer caching
COPY requirements.txt .
RUN pip install -r requirements.txt

# 앱 코드 복사 (변경 잦음)
COPY . .

EXPOSE 8000

# uvicorn 실행 (개발 모드: --reload)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
