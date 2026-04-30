"""OI 운영자 콘솔 (placeholder).

Day 1 단계에서는 placeholder 화면 + IP 화이트리스트 가드만 구현.
실제 메트릭(사용자 통계, 토픽 lag, 비용 알람 등)은 Day 7에서 추가.

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
def get_client_ip() -> str:
    """Streamlit에서 클라이언트 IP 추출.

    Streamlit Cloud나 ALB 뒤에서는 X-Forwarded-For 헤더가 필요한데
    Day 1 로컬 단계에서는 단순화. EC2 배포 시 nginx → uvicorn 구간에서
    X-Real-IP를 받도록 보강 예정.
    """
    try:
        # 1.30+ public API
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
        st.error(f"🚫 접근이 거부되었습니다.\n\n클라이언트 IP `{client_ip}` 는 화이트리스트에 없습니다.")
        st.stop()


check_ip_whitelist()


# ─── 사이드바 ────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🛠️ OI Admin")
    st.caption("Operations Console")
    st.divider()

    section = st.radio(
        "Sections",
        ["대시보드", "사용자 통계", "스트림 모니터링", "비용 & 운영", "감사 로그"],
        label_visibility="collapsed",
    )

    st.divider()
    st.caption(f"Build · `dev`")
    st.caption(f"Time · `{datetime.now().strftime('%Y-%m-%d %H:%M')}`")


# ─── 본문 ────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* fastapi와 톤 맞춤 */
    .stApp { background-color: #0A0A0F; }
    h1, h2, h3 { letter-spacing: -0.02em; }
    .placeholder-card {
        background: #15151D;
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 16px;
    }
    .badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 999px;
        background: rgba(127,119,221,0.12);
        color: #9A93E8;
        font-size: 11px;
        font-family: 'JetBrains Mono', monospace;
        letter-spacing: 0.4px;
        margin-bottom: 8px;
    }
    .day-tag {
        background: rgba(229,177,76,0.12);
        color: #E5B14C;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<span class="badge">Admin Console</span>', unsafe_allow_html=True)
st.title(f"{section}")
st.caption("Day 1 placeholder · 실제 메트릭은 Day 7에서 추가됩니다.")

st.divider()


# ─── 섹션별 placeholder ─────────────────────────────────
if section == "대시보드":
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("승인 대기", "—", "")
    col2.metric("DAU (24h)", "—", "")
    col3.metric("토픽 메시지/분", "—", "")
    col4.metric("Bedrock 호출/일", "—", "")

    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>운영 대시보드</h3>
            <p style="color:#A0A0AB;">
                전체 시스템 건강 상태를 한눈에 보여주는 종합 대시보드. 
                사용자 통계, Redpanda 토픽 lag, S3 적재 상태, Bedrock 호출량, 
                비용 알람을 단일 화면에서 모니터링.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

elif section == "사용자 통계":
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>사용자 분석</h3>
            <p style="color:#A0A0AB;">
                가입/승인/이탈 추이, 일일 활성 사용자, 페이지 조회 분포, 
                인기 검색어 등 사용자 활동 메트릭.
            </p>
            <p style="color:#6E6E78; font-size:13px;">
                데이터 소스: PostgreSQL <code>users</code> 테이블 + 추가 이벤트 로깅
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

elif section == "스트림 모니터링":
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>Redpanda / Airflow 모니터링</h3>
            <p style="color:#A0A0AB;">
                토픽별 메시지 처리율, consumer lag, 파티션 분포, Airflow DAG 실행 결과 
                및 실패한 태스크 알림. Redpanda Console 링크 제공.
            </p>
            <p style="color:#6E6E78; font-size:13px;">
                Quick Link · <a href="http://localhost:8088" target="_blank" style="color:#9A93E8;">Redpanda Console ↗</a>
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

elif section == "비용 & 운영":
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>AWS 비용 추적</h3>
            <p style="color:#A0A0AB;">
                S3 스토리지/요청 비용, Athena 스캔 비용, Bedrock 토큰 사용량, 
                EC2 시간당 비용. 일일/월간 예산 알람.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

elif section == "감사 로그":
    st.markdown(
        """
        <div class="placeholder-card">
            <span class="badge day-tag">Day 7</span>
            <h3>감사 로그</h3>
            <p style="color:#A0A0AB;">
                admin 콘솔 접근 이력, 사용자 승인/거부 액션, 권한 변경, 
                AWS API 호출 기록. 보안 감사용.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─── 푸터 ────────────────────────────────────────────────
st.divider()
st.caption("© 2026 Opensource Insights · Operations Console")
