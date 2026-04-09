from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import streamlit as st


def _get_session_timeout_minutes() -> int:
    raw = os.getenv("SESSION_TIMEOUT_MINUTES", "45").strip()
    try:
        value = int(raw)
    except Exception:
        value = 45
    return max(5, min(value, 240))


def _is_session_expired() -> bool:
    if not st.session_state.get("logged_in"):
        return False

    timeout_minutes = _get_session_timeout_minutes()
    now = datetime.now(timezone.utc)
    last_activity_iso = st.session_state.get("last_activity_utc")

    if not last_activity_iso:
        st.session_state.last_activity_utc = now.isoformat()
        return False

    try:
        last_activity = datetime.fromisoformat(str(last_activity_iso))
    except Exception:
        st.session_state.last_activity_utc = now.isoformat()
        return False

    return now - last_activity > timedelta(minutes=timeout_minutes)


def _touch_activity() -> None:
    if st.session_state.get("logged_in"):
        st.session_state.last_activity_utc = datetime.now(timezone.utc).isoformat()


def _clear_login_state() -> None:
    st.session_state.logged_in = False
    st.session_state.logged_user = None
    st.session_state.last_activity_utc = None

def render_sidebar():
    if _is_session_expired():
        _clear_login_state()
        st.warning("Sessao expirada por inatividade. Faça login novamente.")
        st.switch_page("main.py")

    _touch_activity()

    with st.sidebar:
        st.image("app/assets/logo.png", use_container_width=True)

        st.markdown("### Navegação")

        st.markdown(
            f"""
            <div style="
                background:#cfe0cb;
                border-radius:12px;
                padding:12px 14px;
                margin-bottom:14px;
            ">
                <div style="font-size:0.95rem; color:#2f3a1f; margin-bottom:4px;">
                    Usuário:
                </div>
                <div style="font-size:1rem; font-weight:600; color:#2f3a1f;">
                    {st.session_state.get('logged_user', 'logado')}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            "<div style='font-size:0.82rem; font-weight:700; color:#6a744f; margin:12px 0 8px 2px;'>DADOS</div>",
            unsafe_allow_html=True,
        )
        st.page_link("main.py", label="🏠 Início")
        st.page_link("pages/00_Base_Mestre.py", label="📚 Base Mestre")
        st.page_link("pages/01_Importacao.py", label="📥 Importação")
        st.page_link("pages/02_Consolidacao.py", label="⚙️ Consolidação")
        st.page_link("pages/03_Analises.py", label="📊 Análises")
        st.page_link("pages/6_Geoprocessamento.py", label="🗺️ Geoambiental")

        st.markdown(
            "<div style='font-size:0.82rem; font-weight:700; color:#6a744f; margin:16px 0 8px 2px;'>SAÍDA</div>",
            unsafe_allow_html=True,
        )
        st.page_link("pages/04_Exportacao.py", label="📤 Exportação")

        st.divider()

        if st.button("Sair", use_container_width=True):
            _clear_login_state()
            st.rerun()