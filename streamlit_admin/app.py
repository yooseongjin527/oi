"""OI 운영자 콘솔.

Day 6 시점 — Multi-page 진입 페이지 (랜딩).
- IP 화이트리스트 가드 (모든 페이지 공통, lib/auth.py 와 동일 로직)
- pages/ 디렉터리의 페이지들이 사이드바에 자동 노출됨
- 본문은 페이지 안내 카드만 표시 (실제 기능은 각 페이지에서)

IP 화이트리스트:
- 환경변수 ADMIN_IP_WHITELIST (콤마 구분, 예: "127.0.0.1,1.2.3.4")
- 빈 문자열이면 모든 IP 허용 (로컬 개발 모드)
- Streamlit 자체엔 미들웨어가 없어서 진입 시점에 체크
"""
import os
from datetime import datetime

import streamlit as st


# ─── 페이지 기본 설정 ─────────────────────────────────────
st.set_page_config(
    page_title="OI Admin Console",
    page_icon="🛠️",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─── IP 화이트리스트 가드 ────────────────────────────────
# Note: lib/auth.py 에도 동일 로직이 있지만, app.py 는 lib 도입 이전부터
# 존재했던 진입점이라 의존성 최소화를 위해 인라인 유지.
# pages/ 의 새 페이지들은 lib.auth.check_ip_whitelist() 를 import 해서 사용.
def get_client_ip() -> str:
    """Streamlit 1.30+ public API로 클라이언트 IP 추출.

    Streamlit Cloud나 ALB 뒤에서는 X-Forwarded-For 헤더가 필요한데
    Day 1 로컬 단계에서는 단순화. EC2 배포 시 nginx → uvicorn 구간에서
    X-Real-IP를 받도록 보강 예정.
    """
    try:
        ctx = st.runtime.scriptrunner.get_script_run_ctx()
        if ctx and hasattr(ctx, "session_id"):
            session_info = st.runtime.get_instance().get_client(ctx.session_id)
            if session_info and hasattr(session_info, "request"):
                return session_info.request.remote_ip
    except Exception:
        pass
    return "unknown"


def check_ip_whitelist() -> None:
    raw = os.getenv("ADMIN_IP_WHITELIST", "").strip()
    if not raw:
        return  # 빈 화이트리스트 = 모두 허용 (로컬 개발 모드)

    allowed = {ip.strip() for ip in raw.split(",") if ip.strip()}
    client_ip = get_client_ip()

    # IPv6 ::1 = IPv4 127.0.0.1 동일 취급
    if client_ip in ("::1",):
        client_ip = "127.0.0.1"

    if client_ip not in allowed:
        st.error(
            f"🚫 접근이 거부되었습니다.\n\n"
            f"클라이언트 IP `{client_ip}` 는 화이트리스트에 없습니다."
        )
        st.stop()


check_ip_whitelist()


# ─── 사이드바 ────────────────────────────────────────────
# Day 6: 기존 라디오 메뉴 제거. pages/ 디렉터리의 페이지들이
# Streamlit multi-page 기능으로 사이드바에 자동 노출됨.
with st.sidebar:
    st.markdown("### 🛠️ OI Admin")
    st.caption("Operations Console")
    st.divider()
    st.caption("👈 사이드바의 페이지 목록에서 메뉴를 선택하세요.")
    st.divider()
    st.caption(f"Build · `dev`")
    st.caption(f"Time · `{datetime.now().strftime('%Y-%m-%d %H:%M')}`")


# ─── 공통 톤 (페이지 전반 동일 스타일) ───────────────────
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


# ─── 본문 (랜딩) ─────────────────────────────────────────
st.markdown('<span class="badge">Admin Console</span>', unsafe_allow_html=True)
st.title("🛠️ OI Operations Console")
st.caption("Day 6 운영자 콘솔 · OpenSearch + Streamlit 통합")

st.divider()


# ─── Day 6 활성 페이지 안내 ──────────────────────────────
st.subheader("📍 활성 페이지")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge live-tag">Day 6 · LIVE</span>
            <h3>📊 Gold Data</h3>
            <p style="color:#A0A0AB;">
                Athena Gold 마트 직접 조회. 일별 top-N repo,
                rank_score 기반 차트, CSV 다운로드.
            </p>
            <p style="color:#6E6E78; font-size:13px;">
                데이터 소스: Athena · <code>oi.gold_repo_*</code>
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge live-tag">Day 6 · LIVE</span>
            <h3>🔍 Search</h3>
            <p style="color:#A0A0AB;">
                OpenSearch full-text 검색. repo 이름 + 인사이트
                본문 매칭으로 과거 트렌드 검색.
            </p>
            <p style="color:#6E6E78; font-size:13px;">
                데이터 소스: OpenSearch · <code>oi-repo-daily</code>
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col3:
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge live-tag">Day 6 · LIVE</span>
            <h3>📜 Insights History</h3>
            <p style="color:#A0A0AB;">
                날짜별 인덱싱된 인사이트 + 근거 데이터.
                과거 분석 결과 일관성 비교.
            </p>
            <p style="color:#6E6E78; font-size:13px;">
                데이터 소스: OpenSearch · <code>oi-repo-daily</code>
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.divider()


# ─── Day 7 예정 페이지 안내 ──────────────────────────────
st.subheader("🚧 Day 7 예정")

col1, col2 = st.columns(2)

with col1:
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>운영 대시보드</h3>
            <p style="color:#A0A0AB;">
                전체 시스템 건강 상태를 한눈에. 사용자 통계,
                Redpanda 토픽 lag, S3 적재 상태, Bedrock 호출량.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>스트림 모니터링</h3>
            <p style="color:#A0A0AB;">
                Redpanda 토픽 처리율, consumer lag,
                Airflow DAG 실행 결과 및 실패 알림.
            </p>
            <p style="color:#6E6E78; font-size:13px;">
                Quick Link · 
                <a href="http://localhost:8088" target="_blank" style="color:#9A93E8;">
                    Redpanda Console ↗
                </a>
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>비용 & 사용량 추적</h3>
            <p style="color:#A0A0AB;">
                S3 스토리지/요청, Athena 스캔, Bedrock 토큰,
                EC2 시간당 비용. 일일/월간 예산 알람.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>사용자 통계 · 감사 로그</h3>
            <p style="color:#A0A0AB;">
                가입/승인/이탈 추이, 일일 활성 사용자.
                admin 액션 이력 + AWS API 호출 기록.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─── 푸터 ────────────────────────────────────────────────
st.divider()
st.caption("© 2026 Opensource Insights · Operations Console")