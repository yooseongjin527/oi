"""OpenSearch full-text 검색 페이지.

Day 6 운영자 콘솔 페이지 #2.
- repo_name + insight_markdown multi-match
- 5분 캐싱 (반복 검색 비용 0)
- 검색 결과 → 인사이트 마크다운 미리보기 expander
"""
import streamlit as st

from lib.auth import check_ip_whitelist
from lib import opensearch as os_client


st.set_page_config(
    page_title="Search · OI Admin",
    page_icon="🔍",
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

st.markdown('<span class="badge">Full-text Search</span>', unsafe_allow_html=True)
st.title("🔍 Search")
st.caption("OpenSearch · repo 이름 + 인사이트 본문 매칭")
st.divider()


# ─── 인덱스 통계 ─────────────────────────────────────────
stats = os_client.get_index_stats()
col1, col2, col3 = st.columns(3)
col1.metric("색인 문서 수", f"{stats['doc_count']:,}")
col2.metric("색인 크기", f"{stats['size_bytes'] / 1024:.1f} KB")
col3.metric("인덱스", "oi-repo-daily")

st.divider()


# ─── 검색 입력 ───────────────────────────────────────────
col_q, col_size = st.columns([4, 1])

with col_q:
    query = st.text_input(
        "검색어",
        placeholder="예: AI agent, rust terminal, warpdotdev",
        label_visibility="collapsed",
    )

with col_size:
    size = st.number_input("결과 수", min_value=1, max_value=50, value=10, step=5)


# ─── 검색 실행 ───────────────────────────────────────────
if not query:
    st.info("검색어를 입력하세요. 예: `AI agent`, `terminal`, `rust`")
    st.stop()

with st.spinner("검색 중..."):
    try:
        result = os_client.search(query=query, size=int(size))
    except Exception as e:
        st.error(f"검색 실패: {e}")
        st.stop()

total = result["total"]
hits = result["hits"]

st.markdown(f"**{total}건** 검색됨 · `{query}`")
st.divider()

if not hits:
    st.warning(
        "결과가 없습니다.\n\n"
        "- 영문 검색어가 한국어 인사이트 본문에 매칭이 잘 안 될 수 있음\n"
        "- repo 이름 일부로 검색해보기 (예: `warp`, `hermes`)"
    )
    st.stop()


# ─── 결과 카드 렌더링 ────────────────────────────────────
for hit in hits:
    repo_name = hit.get("repo_name", "—")
    date = hit.get("date", "—")
    score = hit.get("_score", 0)
    rank_score = hit.get("rank_score", 0) or 0
    event_count = hit.get("event_count", 0) or 0
    dominant = hit.get("dominant_event_type", "—")
    insight_md = hit.get("insight_markdown", "")

    with st.container():
        st.markdown(
            f"""
            <div class="result-card">
                <div class="result-name">
                    <a href="https://github.com/{repo_name}" target="_blank" 
                       style="color:#E0E0E8; text-decoration:none;">
                        {repo_name} ↗
                    </a>
                </div>
                <div class="result-meta">
                    📅 {date} · 🎯 score={score:.2f} · 
                    ⚡ rank={rank_score:.2f} · 📈 events={event_count:,} · 
                    🏷️ {dominant}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 인사이트 미리보기 (expander)
        if insight_md:
            with st.expander("💡 그날의 인사이트 보기"):
                st.markdown(insight_md)