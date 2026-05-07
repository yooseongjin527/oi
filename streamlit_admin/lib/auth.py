"""IP 화이트리스트 가드 — app.py에서 분리해서 페이지마다 import"""
import os
import streamlit as st


def get_client_ip() -> str:
    """Streamlit 1.30+ public API로 클라이언트 IP 추출."""
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
    """ADMIN_IP_WHITELIST 환경변수 기반 진입 차단.
    빈 문자열이면 모두 허용 (로컬 개발 모드).
    각 페이지 최상단에서 호출 — 우회 방지를 위해 직접 URL 접근 시도해도 차단됨.
    """
    raw = os.getenv("ADMIN_IP_WHITELIST", "").strip()
    if not raw:
        return

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