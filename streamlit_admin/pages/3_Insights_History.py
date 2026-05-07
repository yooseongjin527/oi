"""인사이트 히스토리 — 날짜별 인덱싱 결과 조회.

Day 6 운영자 콘솔 페이지 #3.
- OpenSearch date 필드 distinct aggregation 으로 날짜 목록
- 선택 날짜의 top-N + 통합 인사이트 마크다운 표시
"""
import pandas as pd
import streamlit as st

from lib.auth import check_ip_whitelist
from lib import opensearch as os_client


st.set_page_config(
    page_title="Insights History · OI Admin",
    page_icon="📜",
    layout="wide",
)
check_ip_whitelist()

st.markdown(
    """
    <style>
    /* ─── 베이스 ──────────────────────────────────── */
    .stApp { background-color: #0A0A0F; color: #E8E8F0; }
    h1, h2, h3, h4 { letter-spacing: -0.02em; color: #F4F4F8 !important; }
    p, li, span, div, label { color: #D8D8E0; }

    /* Streamlit 기본 텍스트 강제 명도 */
    .stMarkdown, .stMarkdown p, .stMarkdown li {
        color: #D8D8E0 !important;
    }

    /* caption (회색톤 강제 명도 상향) */
    .stCaption, [data-testid="stCaptionContainer"], small {
        color: #9A9AA8 !important;
    }

    /* ─── 메트릭 (st.metric) ────────────────────── */
    [data-testid="stMetricLabel"] {
        color: #B8B8C8 !important;
        font-weight: 500;
    }
    [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
        font-weight: 700;
    }
    [data-testid="stMetricDelta"] {
        color: #9A9AA8 !important;
    }

    /* ─── 카드 ──────────────────────────────────── */
    .placeholder-card {
        background: #1A1A24;
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
        height: 100%;
    }
    .placeholder-card h3 {
        color: #FFFFFF !important;
        margin-top: 4px;
        margin-bottom: 12px;
    }
    .placeholder-card p {
        color: #C8C8D4 !important;
        line-height: 1.55;
    }
    .placeholder-card code {
        background: rgba(154,147,232,0.15);
        color: #B8B0FF;
        padding: 2px 6px;
        border-radius: 4px;
        font-size: 12px;
    }
    .placeholder-card a { color: #B8B0FF !important; }

    /* ─── 뱃지 ──────────────────────────────────── */
    .badge {
        display: inline-block;
        padding: 4px 11px;
        border-radius: 999px;
        background: rgba(154,147,232,0.18);
        color: #B8B0FF !important;
        font-size: 11px;
        font-family: 'JetBrains Mono', monospace;
        font-weight: 600;
        letter-spacing: 0.4px;
        margin-bottom: 8px;
    }
    .day-tag {
        background: rgba(229,177,76,0.18) !important;
        color: #F0C868 !important;
    }
    .live-tag {
        background: rgba(76,229,144,0.18) !important;
        color: #6FE8A8 !important;
    }

    /* ─── 검색 결과 카드 (search 페이지) ─────────── */
    .result-card {
        background: #1A1A24;
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 8px;
        padding: 14px 18px;
        margin-bottom: 10px;
    }
    .result-meta {
        color: #9A9AA8 !important;
        font-family: 'JetBrains Mono', monospace;
        font-size: 12px;
        margin-top: 6px;
    }
    .result-name {
        font-size: 16px;
        font-weight: 600;
        color: #FFFFFF !important;
    }
    .result-name a { color: #FFFFFF !important; text-decoration: none; }
    .result-name a:hover { color: #B8B0FF !important; }

    /* ─── 인사이트 프레임 (history 페이지) ────────── */
    .insight-frame {
        background: #1A1A24;
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 12px;
        padding: 24px 28px;
        margin-top: 8px;
    }

    /* ─── 입력 위젯 (selectbox, text_input 등) ────── */
    .stSelectbox label, .stTextInput label, .stNumberInput label,
    .stSlider label, .stRadio label {
        color: #D8D8E0 !important;
        font-weight: 500;
    }

    /* ─── 사이드바 ──────────────────────────────── */
    [data-testid="stSidebar"] {
        background-color: #0F0F18;
        border-right: 1px solid rgba(255,255,255,0.06);
    }
    [data-testid="stSidebar"] .stMarkdown { color: #D8D8E0; }

    /* ─── DataFrame ──────────────────────────────── */
    [data-testid="stDataFrame"] {
        background: #1A1A24;
        border-radius: 8px;
    }

    /* ─── alert (info, warning, error) 명도 ──────── */
    [data-testid="stAlert"] {
        background: rgba(154,147,232,0.08);
        border: 1px solid rgba(154,147,232,0.25);
    }
    [data-testid="stAlert"] p { color: #E8E8F0 !important; }

    /* ─── expander ──────────────────────────────── */
    [data-testid="stExpander"] {
        background: #1A1A24;
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 8px;
    }
    [data-testid="stExpander"] summary { color: #E8E8F0 !important; }

    /* ─── divider 더 진하게 ─────────────────────── */
    hr { border-color: rgba(255,255,255,0.1) !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<span class="badge">Insights History</span>', unsafe_allow_html=True)
st.title("📜 Insights History")
st.caption("OpenSearch 인덱싱된 일별 인사이트 + 근거 데이터")
st.divider()


# ─── 사이드바: 날짜 선택 ────────────────────────────────
with st.sidebar:
    st.markdown("### 📅 날짜 선택")

    indexed_dates = os_client.get_indexed_dates()
    if not indexed_dates:
        st.warning("인덱싱된 날짜 없음")
        st.caption("메인 페이지에서 인사이트를 먼저 생성하세요.")
        st.stop()

    # 데모일이 있으면 디폴트
    default_idx = 0
    if "2026-04-29" in indexed_dates:
        default_idx = indexed_dates.index("2026-04-29")

    selected_date = st.selectbox(
        "조회 날짜",
        options=indexed_dates,
        index=default_idx,
    )

    st.divider()
    if st.button("🔄 캐시 초기화", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─── 데이터 로드 ─────────────────────────────────────────
with st.spinner(f"히스토리 로딩... ({selected_date})"):
    try:
        result = os_client.get_by_date(selected_date, size=50)
    except Exception as e:
        st.error(f"조회 실패: {e}")
        st.stop()

hits = result["hits"]
if not hits:
    st.warning(f"`{selected_date}` 인덱스에 데이터 없음")
    st.stop()


# ─── 헤더 메트릭 ─────────────────────────────────────────
total_events = sum((h.get("event_count") or 0) for h in hits)
avg_rank = sum((h.get("rank_score") or 0) for h in hits) / len(hits)
max_zscore = max((h.get("watch_zscore") or 0) for h in hits)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Repo 수", len(hits))
col2.metric("총 이벤트", f"{int(total_events):,}")
col3.metric("평균 rank", f"{avg_rank:.2f}")
col4.metric("최고 watch_z", f"{max_zscore:.2f}")

st.divider()


# ─── 통합 인사이트 마크다운 (모든 hit 동일값이라 첫 번째꺼만) ──
insight_md = hits[0].get("insight_markdown")

if insight_md:
    st.subheader(f"💡 {selected_date} 인사이트")
    # st.container + border + 직접 markdown 렌더 (HTML wrap 우회)
    with st.container(border=True):
        st.markdown(insight_md)
else:
    st.info("인사이트 마크다운이 인덱싱되지 않은 날짜입니다.")

st.divider()


# ─── 근거 데이터 테이블 ──────────────────────────────────
st.subheader("📊 근거 데이터")

df = pd.DataFrame(hits)
display_cols = [
    "repo_name", "event_count", "dominant_event_type",
    "acceleration_ratio", "anomaly_score", "watch_zscore", "rank_score",
]
display_cols = [c for c in display_cols if c in df.columns]

# 숫자 컬럼 변환 — OpenSearch 가 None 으로 넣은 값 처리
for col in ["event_count", "acceleration_ratio", "anomaly_score",
            "watch_zscore", "rank_score"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.sort_values("rank_score", ascending=False)

st.dataframe(
    df[display_cols].style.format({
        "acceleration_ratio": "{:.2f}",
        "anomaly_score": "{:.2f}",
        "watch_zscore": "{:.2f}",
        "rank_score": "{:.2f}",
    }),
    use_container_width=True,
    hide_index=True,
)

# CSV 다운로드
csv_bytes = df[display_cols].to_csv(index=False).encode("utf-8")
st.download_button(
    label="📥 CSV 다운로드",
    data=csv_bytes,
    file_name=f"insights_history_{selected_date}.csv",
    mime="text/csv",
)