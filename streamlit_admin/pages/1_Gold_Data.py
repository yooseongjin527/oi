"""Gold 마트 직접 조회 — 일별 top-N repo + 차트.

Day 6 운영자 콘솔 페이지 #1.
- Athena 직접 호출 (boto3) — fastapi 우회
- @st.cache_data 1시간 캐싱 — 같은 날짜 반복 조회 비용 0
- pandas DataFrame + Plotly 막대 차트
"""
import streamlit as st
import plotly.express as px

from lib.auth import check_ip_whitelist
from lib import athena


# ─── 페이지 설정 + IP 가드 ───────────────────────────────
st.set_page_config(
    page_title="Gold Data · OI Admin",
    page_icon="📊",
    layout="wide",
)
check_ip_whitelist()


# ─── 공통 톤 (app.py 에서 정의된 것과 동일) ──────────────
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

st.markdown('<span class="badge">Gold Mart</span>', unsafe_allow_html=True)
st.title("📊 Gold Data")
st.caption("Athena 직접 조회 · 일별 top-N repo · rank_score 기반 정렬")
st.divider()


# ─── 사이드바: 날짜 + limit 선택 ────────────────────────
with st.sidebar:
    st.markdown("### 🔍 필터")

    # 사용 가능한 날짜 목록 (Gold 마트 distinct)
    with st.spinner("날짜 목록 로딩..."):
        try:
            dates = athena.get_available_dates()
        except Exception as e:
            st.error(f"Athena 연결 실패: {e}")
            dates = []

    # 데모일 (2026-04-29) 이 있으면 디폴트로
    default_idx = 0
    if "2026-04-29" in dates:
        default_idx = dates.index("2026-04-29")

    if dates:
        selected_date = st.selectbox(
            "조회 날짜",
            options=dates,
            index=default_idx,
            help="Gold 마트에 데이터가 존재하는 날짜만 표시",
        )
    else:
        selected_date = st.text_input("조회 날짜 (수동)", value="2026-04-29")

    limit = st.slider("결과 개수", min_value=5, max_value=50, value=10, step=5)

    st.divider()
    if st.button("🔄 캐시 초기화", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─── 본문 ────────────────────────────────────────────────
try:
    with st.spinner(f"Athena 쿼리 실행 중... ({selected_date})"):
        df = athena.get_top_repos(selected_date, limit=limit)
except Exception as e:
    st.error(f"쿼리 실패: {e}")
    st.stop()

if df.empty:
    st.warning(f"`{selected_date}` 의 Gold 데이터가 없습니다.")
    st.stop()


# ─── 헤더 메트릭 ─────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Top Repo 개수", len(df))
col2.metric("총 이벤트", f"{int(df['event_count'].sum()):,}")
col3.metric("평균 rank_score", f"{df['rank_score'].mean():.2f}")
col4.metric("최고 watch_zscore", f"{df['watch_zscore'].max():.2f}")

st.divider()


# ─── 차트: rank_score top-N ──────────────────────────────
st.subheader("Rank Score Top-N")

chart_df = df.sort_values("rank_score", ascending=True).tail(limit)
fig = px.bar(
    chart_df,
    x="rank_score",
    y="repo_name",
    orientation="h",
    color="rank_score",
    color_continuous_scale="Viridis",
    hover_data={
        "event_count": True,
        "acceleration_ratio": ":.2f",
        "anomaly_score": ":.2f",
        "watch_zscore": ":.2f",
        "rank_score": ":.2f",
    },
)
fig.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#E0E0E8"),
    margin=dict(l=10, r=10, t=10, b=10),
    height=max(400, 30 * limit),
    yaxis=dict(title="", tickfont=dict(family="monospace", size=11)),
    xaxis=dict(title="rank_score", gridcolor="rgba(255,255,255,0.05)"),
    coloraxis_showscale=False,
)
st.plotly_chart(fig, use_container_width=True)

st.divider()


# ─── 데이터 테이블 + 다운로드 ────────────────────────────
st.subheader("Raw Data")

# 표시용 컬럼만 선별 (긴 컬럼 가독성 위해)
display_cols = [
    "repo_name", "event_count", "dominant_event_type",
    "acceleration_ratio", "anomaly_score", "watch_zscore",
    "rank_score",
]
display_cols = [c for c in display_cols if c in df.columns]

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

# CSV 다운로드 버튼
csv_bytes = df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="📥 CSV 다운로드",
    data=csv_bytes,
    file_name=f"gold_top_{selected_date}.csv",
    mime="text/csv",
)