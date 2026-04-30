FROM python:3.12-slim

WORKDIR /app

# 시스템 의존성 (boto3 SSL 등)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 의존성 먼저 복사 → 레이어 캐싱
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 코드
COPY main.py .

# Python 출력 버퍼링 끔 (docker logs 즉시 보이게)
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
