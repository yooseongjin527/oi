#!/usr/bin/env bash
# ============================================
# OI — EC2 deploy 스크립트
# ============================================
# 사용 (EC2 SSH 접속 후):
#   ~/oi/scripts/deploy.sh                  # 일반 배포
#   ~/oi/scripts/deploy.sh --bootstrap      # 최초 1회: Athena DDL 도 적용
#   ~/oi/scripts/deploy.sh --no-build       # 이미지 빌드 스킵 (코드만 변경)
#
# HANDOFF §8 의 deploy.sh 명세 + 부트스트랩 옵션 추가.
# ============================================

set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/oi}"
COMPOSE_FILE="${COMPOSE_FILE:-$REPO_DIR/docker-compose.yml}"
DO_BOOTSTRAP=0
DO_BUILD=1

for arg in "$@"; do
  case "$arg" in
    --bootstrap)  DO_BOOTSTRAP=1 ;;
    --no-build)   DO_BUILD=0 ;;
    -h|--help)
      sed -n '1,15p' "$0"
      exit 0
      ;;
  esac
done

cd "$REPO_DIR"

echo "[deploy] pulling latest from origin/main ..."
git fetch --quiet origin
git reset --hard origin/main

if [[ ! -f .env ]]; then
  echo "[deploy] ❌ .env 파일이 없습니다. cp .env.example .env 후 채우세요."
  exit 1
fi

echo "[deploy] docker compose down (graceful) ..."
docker compose -f "$COMPOSE_FILE" down --remove-orphans || true

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "[deploy] docker compose build (--pull) ..."
  docker compose -f "$COMPOSE_FILE" build --pull
fi

echo "[deploy] docker compose up -d ..."
docker compose -f "$COMPOSE_FILE" up -d

# 컨테이너 안정화 대기 — fastapi healthcheck 가 가장 늦게 올라옴
echo "[deploy] waiting for fastapi healthcheck ..."
for i in $(seq 1 30); do
  if curl -fsS http://localhost:8000/health >/dev/null 2>&1; then
    echo "[deploy] fastapi up after ${i}s"
    break
  fi
  sleep 2
done

if [[ "$DO_BOOTSTRAP" -eq 1 ]]; then
  echo "[deploy] running Athena/Glue bootstrap (idempotent) ..."
  docker compose -f "$COMPOSE_FILE" exec -T fastapi \
    python /app/../scripts/bootstrap_athena.py 2>/dev/null \
    || python3 "$REPO_DIR/scripts/bootstrap_athena.py"
fi

# admin 계정이 없으면 생성 (init_admin.py 멱등)
echo "[deploy] ensuring admin account ..."
docker compose -f "$COMPOSE_FILE" exec -T fastapi python init_admin.py || true

echo "[deploy] ✓ done."
docker compose -f "$COMPOSE_FILE" ps
