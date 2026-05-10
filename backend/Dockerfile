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

# uvicorn 실행
# - --reload: 개발 편의 (운영에서도 부담 적어서 그대로). 부하 더 줄이고 싶으면 빼세요.
# - --proxy-headers --forwarded-allow-ips=*: ALB 같은 reverse proxy 뒤에서
#   X-Forwarded-Proto / X-Forwarded-For 를 신뢰. 이게 있어야 request.url.scheme 이
#   "https" 로 잡혀 쿠키 secure 판단/리다이렉트가 정상 동작.
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--proxy-headers", "--forwarded-allow-ips=*", \
     "--reload"]
