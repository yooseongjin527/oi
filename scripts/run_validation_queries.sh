#!/usr/bin/env bash
# Day 4 Gold 마트 검증 쿼리 4개를 Athena CLI로 실행, 결과를 표 형태로 종합 출력.
# 발표 슬라이드용 수치 (행 수, 스캔 사이즈) 자동 기록.
#
# 전제: AWS_DEFAULT_REGION, BUCKET 환경변수 세팅됨, oi-workgroup 사용

set -uo pipefail

REGION="${AWS_DEFAULT_REGION:-ap-northeast-2}"
BUCKET="${BUCKET:-oi-data-lake}"
WORKGROUP="oi-workgroup"
OUTPUT_LOCATION="s3://${BUCKET}/athena-results/"
TARGET_DAY="2026-04-29"  # 발표 데모 기준일 (가속도/이상탐지 둘 다 의미 있는 첫 날)

# ──────────────────────────────────────────
# 쿼리 4개 정의 (heredoc로 SQL 그대로)
# ──────────────────────────────────────────

read -r -d '' Q_A <<'SQL' || true
SELECT repo_name, event_count, prev_event_count,
       acceleration_ratio, dominant_event_type
FROM oi.gold_repo_acceleration
WHERE year='2026' AND month='04' AND day='29'
  AND prev_event_count >= 5
ORDER BY acceleration_ratio DESC NULLS LAST
LIMIT 10
SQL

read -r -d '' Q_B <<'SQL' || true
SELECT repo_name, event_count, watch_ratio, watch_zscore,
       activity_zscore, anomaly_score
FROM oi.gold_repo_anomaly
WHERE year='2026' AND month='04' AND day='29'
ORDER BY anomaly_score DESC NULLS LAST
LIMIT 10
SQL

read -r -d '' Q_C_TEMPLATE <<'SQL' || true
SELECT hour, event_count
FROM oi.gold_repo_hourly
WHERE year='2026' AND month='04' AND day='29'
  AND repo_name = '__TOP_REPO__'
ORDER BY hour
SQL

read -r -d '' Q_D <<'SQL' || true
SELECT hour, SUM(event_count) AS total_events,
       COUNT(DISTINCT repo_id) AS active_repos
FROM oi.gold_repo_hourly
WHERE year='2026' AND month='04' AND day='29'
GROUP BY hour
ORDER BY hour
SQL

# ──────────────────────────────────────────
# 헬퍼: 쿼리 실행 + 완료 대기 + 결과/메타 반환
# ──────────────────────────────────────────

run_query() {
    # $1: query string
    # echo: qid (다른 함수가 받음)
    local sql="$1"
    local qid
    qid=$(aws athena start-query-execution \
        --query-string "$sql" \
        --result-configuration "OutputLocation=${OUTPUT_LOCATION}" \
        --work-group "$WORKGROUP" \
        --region "$REGION" \
        --query 'QueryExecutionId' \
        --output text)
    echo "$qid"
}

wait_query() {
    # $1: qid
    # 종료 상태(SUCCEEDED/FAILED/CANCELLED) 출력
    local qid="$1"
    local state
    for i in {1..60}; do
        state=$(aws athena get-query-execution \
            --query-execution-id "$qid" \
            --region "$REGION" \
            --query 'QueryExecution.Status.State' \
            --output text 2>/dev/null)
        case "$state" in
            SUCCEEDED|FAILED|CANCELLED) echo "$state"; return ;;
        esac
        sleep 1
    done
    echo "TIMEOUT"
}

get_meta() {
    # $1: qid
    # echo: "<scanned_bytes>|<row_count>|<elapsed_ms>"
    local qid="$1"
    aws athena get-query-execution \
        --query-execution-id "$qid" \
        --region "$REGION" \
        --query 'QueryExecution.Statistics.[DataScannedInBytes,EngineExecutionTimeInMillis]' \
        --output text 2>/dev/null
}

get_results() {
    # $1: qid
    # raw rows (header 포함, tab-separated)
    local qid="$1"
    aws athena get-query-results \
        --query-execution-id "$qid" \
        --region "$REGION" \
        --query 'ResultSet.Rows[*].Data[*].VarCharValue' \
        --output text 2>/dev/null
}

format_bytes() {
    # MB 단위로 깔끔하게
    local bytes="$1"
    awk -v b="$bytes" 'BEGIN { printf "%.2f", b/1024/1024 }'
}

run_and_report() {
    # $1: label, $2: sql
    # returns via globals: LAST_QID, LAST_STATE, LAST_SCAN_MB, LAST_TIME_MS, LAST_ROW_COUNT
    local label="$1"
    local sql="$2"

    echo ""
    echo "=========================================="
    echo " 쿼리 ${label}"
    echo "=========================================="

    LAST_QID=$(run_query "$sql")
    echo "QID: $LAST_QID"

    LAST_STATE=$(wait_query "$LAST_QID")
    echo "State: $LAST_STATE"

    if [ "$LAST_STATE" != "SUCCEEDED" ]; then
        echo "❌ 실패. reason 확인:"
        aws athena get-query-execution \
            --query-execution-id "$LAST_QID" \
            --region "$REGION" \
            --query 'QueryExecution.Status.StateChangeReason' \
            --output text
        return 1
    fi

    local meta
    meta=$(get_meta "$LAST_QID")
    LAST_SCAN_BYTES=$(echo "$meta" | awk '{print $1}')
    LAST_TIME_MS=$(echo "$meta" | awk '{print $2}')
    LAST_SCAN_MB=$(format_bytes "$LAST_SCAN_BYTES")

    # 결과 미리보기
    local raw
    raw=$(get_results "$LAST_QID")
    LAST_ROW_COUNT=$(echo "$raw" | wc -l)
    LAST_ROW_COUNT=$((LAST_ROW_COUNT - 1))  # header 제외

    echo "Scanned: ${LAST_SCAN_MB} MB  |  Time: ${LAST_TIME_MS} ms  |  Rows: ${LAST_ROW_COUNT}"
    echo ""
    echo "--- 결과 ---"
    echo "$raw" | head -15
}

# ──────────────────────────────────────────
# 실행
# ──────────────────────────────────────────

declare -A SUMMARY

run_and_report "A (가속도 top 10)" "$Q_A"
SUMMARY[A_rows]=$LAST_ROW_COUNT
SUMMARY[A_mb]=$LAST_SCAN_MB
SUMMARY[A_ms]=$LAST_TIME_MS

# 쿼리 A의 1순위 repo 추출 (쿼리 C 입력)
TOP_REPO=$(get_results "$LAST_QID" | sed -n '2p' | awk '{print $1}')
echo ""
echo ">> Q_C에 사용할 top repo: $TOP_REPO"

run_and_report "B (이상 repo top 10)" "$Q_B"
SUMMARY[B_rows]=$LAST_ROW_COUNT
SUMMARY[B_mb]=$LAST_SCAN_MB
SUMMARY[B_ms]=$LAST_TIME_MS

if [ -n "$TOP_REPO" ]; then
    Q_C="${Q_C_TEMPLATE/__TOP_REPO__/$TOP_REPO}"
    run_and_report "C ($TOP_REPO 시간대 분포)" "$Q_C"
    SUMMARY[C_rows]=$LAST_ROW_COUNT
    SUMMARY[C_mb]=$LAST_SCAN_MB
    SUMMARY[C_ms]=$LAST_TIME_MS
else
    echo "⚠️  top repo 추출 실패 → Q_C skip"
    SUMMARY[C_rows]="-"; SUMMARY[C_mb]="-"; SUMMARY[C_ms]="-"
fi

run_and_report "D (전체 시간대 활동 분포)" "$Q_D"
SUMMARY[D_rows]=$LAST_ROW_COUNT
SUMMARY[D_mb]=$LAST_SCAN_MB
SUMMARY[D_ms]=$LAST_TIME_MS

# ──────────────────────────────────────────
# 최종 종합 표 (발표 슬라이드용)
# ──────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║       Day 4 Athena 검증 쿼리 종합 (2026-04-29)   ║"
echo "╠══════════════════════════════════════════════════╣"
printf "║ %-30s │ %5s │ %7s │ %6s ║\n" "Query" "Rows" "Scan(MB)" "Time(ms)"
echo "╠──────────────────────────────────────────────────╣"
printf "║ %-30s │ %5s │ %7s │ %6s ║\n" "A 가속도 top"          "${SUMMARY[A_rows]}" "${SUMMARY[A_mb]}" "${SUMMARY[A_ms]}"
printf "║ %-30s │ %5s │ %7s │ %6s ║\n" "B 이상 repo top"       "${SUMMARY[B_rows]}" "${SUMMARY[B_mb]}" "${SUMMARY[B_ms]}"
printf "║ %-30s │ %5s │ %7s │ %6s ║\n" "C 시간대 분포 (1 repo)" "${SUMMARY[C_rows]}" "${SUMMARY[C_mb]}" "${SUMMARY[C_ms]}"
printf "║ %-30s │ %5s │ %7s │ %6s ║\n" "D 전체 시간대 활동"     "${SUMMARY[D_rows]}" "${SUMMARY[D_mb]}" "${SUMMARY[D_ms]}"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "발표 인사이트 (쿼리 A 1위): $TOP_REPO"
