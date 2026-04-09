import base64
import hashlib
import hmac
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from dotenv import load_dotenv

from core.sidebar import render_sidebar

load_dotenv()

st.set_page_config(
    page_title="OPYTA DATA",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if "failed_login_attempts" not in st.session_state:
    st.session_state.failed_login_attempts = 0

if "login_lock_until" not in st.session_state:
    st.session_state.login_lock_until = None

st.markdown("""
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

[data-testid="stAppViewContainer"] {
    background: #f6f7f2;
}

[data-testid="stSidebar"] {
    background: #eef1e6;
    border-right: 1px solid #dfe5d2;
    width: 240px !important;
}

.block-container {
    padding-top: 1.4rem;
    padding-bottom: 1.5rem;
}

h1, h2, h3 {
    color: #2f3a1f;
}

div.stButton > button {
    background-color: #d4a81e;
    color: #1e1e1e;
    border: none;
    border-radius: 12px;
    font-weight: 600;
    height: 3rem;
}

div.stButton > button:hover {
    background-color: #bf9719;
    color: #1e1e1e;
}

div[data-baseweb="input"] > div {
    border: 1px solid #b8c1a3 !important;
    border-radius: 12px !important;
    background-color: #ffffff !important;
}

input {
    background-color: #ffffff !important;
}

[data-testid="stAlert"] {
    border-radius: 12px;
}

.opyta-card {
    background: #ffffff;
    border: 1px solid #dfe5d2;
    border-radius: 18px;
    padding: 1.2rem 1.2rem 1rem 1.2rem;
    box-shadow: 0 4px 14px rgba(0,0,0,0.04);
    min-height: 210px;
}

.opyta-card h3 {
    margin-top: 0;
    margin-bottom: 0.5rem;
    color: #2f3a1f;
    font-size: 1.35rem;
}

.opyta-card p {
    color: #55613d;
    margin-bottom: 1rem;
}

.opyta-tag {
    display: inline-block;
    padding: 0.25rem 0.6rem;
    border-radius: 999px;
    background: #eef1e6;
    color: #5f6b46;
    font-size: 0.82rem;
    margin-bottom: 0.8rem;
}

.opyta-hero {
    background: linear-gradient(135deg, #ffffff 0%, #f4f7ef 100%);
    border: 1px solid #dfe5d2;
    border-radius: 22px;
    padding: 1rem 1.4rem;
    margin-bottom: 1.2rem;
    box-shadow: 0 4px 14px rgba(0,0,0,0.04);
}

.opyta-sub {
    color: #5f6b46;
    margin-top: 0.25rem;
    margin-bottom: 0;
}

div[data-testid="stSidebar"] div.stButton > button {
    background: #ffffff !important;
    color: #6f4b2a !important;
    border: 1px solid #d8d8cf !important;
    border-radius: 12px !important;
    font-weight: 500 !important;
}

div[data-testid="stSidebar"] div.stButton > button:hover {
    background: #f7f7f2 !important;
    color: #6f4b2a !important;
}
</style>
""", unsafe_allow_html=True)

if not st.session_state.logged_in:
    st.markdown("""
    <style>
    [data-testid="stSidebar"] {display: none;}
    [data-testid="stSidebarNav"] {display: none;}
    </style>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
    <style>
    [data-testid="stSidebarNav"] {display: none;}
    </style>
    """, unsafe_allow_html=True)


def require_login():
    def _parse_pbkdf2_record(record: str):
        # Format: pbkdf2_sha256$<iterations>$<salt_b64>$<hash_b64>
        try:
            scheme, iterations, salt_b64, hash_b64 = record.split("$", 3)
            if scheme != "pbkdf2_sha256":
                return None
            return int(iterations), salt_b64, hash_b64
        except Exception:
            return None

    def _verify_password(password: str, stored_record: str) -> bool:
        parsed = _parse_pbkdf2_record(stored_record)
        if not parsed:
            return False

        iterations, salt_b64, expected_hash_b64 = parsed
        try:
            salt = base64.b64decode(salt_b64.encode("utf-8"))
        except Exception:
            return False

        derived = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            iterations,
        )
        derived_b64 = base64.b64encode(derived).decode("utf-8")
        return hmac.compare_digest(derived_b64, expected_hash_b64)

    def _load_auth_users() -> dict[str, str]:
        users: dict[str, str] = {}

        # Prioridade 1: Streamlit secrets (AUTH_USERS_JSON)
        try:
            auth_json = st.secrets.get("AUTH_USERS_JSON")
            if auth_json:
                loaded = json.loads(str(auth_json))
                if isinstance(loaded, dict):
                    for user, record in loaded.items():
                        users[str(user).strip().lower()] = str(record).strip()
        except Exception:
            pass

        # Prioridade 2: Variável de ambiente
        if not users:
            try:
                env_auth = os.getenv("AUTH_USERS_JSON", "").strip()
                if env_auth:
                    loaded = json.loads(env_auth)
                    if isinstance(loaded, dict):
                        for user, record in loaded.items():
                            users[str(user).strip().lower()] = str(record).strip()
            except Exception:
                pass

        return users

    def _is_locked() -> tuple[bool, str]:
        lock_until = st.session_state.get("login_lock_until")
        if not lock_until:
            return False, ""

        now = datetime.now(timezone.utc)
        if now >= lock_until:
            st.session_state.login_lock_until = None
            st.session_state.failed_login_attempts = 0
            return False, ""

        remaining = lock_until - now
        minutes = int(remaining.total_seconds() // 60)
        seconds = int(remaining.total_seconds() % 60)
        return True, f"Muitas tentativas inválidas. Tente novamente em {minutes:02d}:{seconds:02d}."

    def _register_failed_attempt():
        attempts = int(st.session_state.get("failed_login_attempts", 0)) + 1
        st.session_state.failed_login_attempts = attempts

        max_attempts = 5
        lock_minutes = 15
        if attempts >= max_attempts:
            st.session_state.login_lock_until = datetime.now(timezone.utc) + timedelta(minutes=lock_minutes)

    def _reset_login_attempts():
        st.session_state.failed_login_attempts = 0
        st.session_state.login_lock_until = None

    if st.session_state.logged_in:
        return True

    _, center, _ = st.columns([1.1, 1.4, 1.1])

    with center:
        st.image("app/assets/logo.png", width=260)
        st.markdown("### Acesso à plataforma")
        st.write("Entre com seu usuário e senha para continuar.")

        locked, lock_msg = _is_locked()
        if locked:
            st.error(lock_msg)
            return False

        user_input = st.text_input("Usuário")
        pwd_input = st.text_input("Senha", type="password")

        if st.button("Entrar", use_container_width=True):
            users = _load_auth_users()
            if not users:
                st.error(
                    "Autenticação não configurada. Defina AUTH_USERS_JSON em secrets/ambiente com hashes PBKDF2."
                )
                return False

            user_key = user_input.strip().lower()
            stored_record = users.get(user_key, "")

            # Delay mínimo para reduzir brute force por tentativa.
            time.sleep(0.35)

            if stored_record and _verify_password(pwd_input, stored_record):
                st.session_state.logged_in = True
                st.session_state.logged_user = user_key
                _reset_login_attempts()
                st.rerun()
            else:
                _register_failed_attempt()
                remaining = max(0, 5 - int(st.session_state.get("failed_login_attempts", 0)))
                st.error("Usuário ou senha incorretos.")
                if remaining > 0:
                    st.caption(f"Tentativas restantes antes de bloqueio temporário: {remaining}")

    return False


if not require_login():
    st.stop()

render_sidebar()

st.markdown("""
<div class="opyta-hero">
    <h1 style="margin-bottom:0;">OPYTA DATA</h1>
    <p class="opyta-sub">Centralize importação, consolidação e análises ambientais em um único fluxo.</p>
</div>
""", unsafe_allow_html=True)

st.markdown(
    "📌 **Fluxo completo:** Base Mestre → Importação → Consolidação → Análises → Exportação\n"
    "Cada etapa prepara dados para a próxima. Comece cadastrando sua referência, depois importe e analise."
)

col1, col2 = st.columns(2, gap="large")

with col1:
    st.markdown("""
    <div class="opyta-card">
        <div class="opyta-tag">Cadastro</div>
        <h3>📚 Base Mestre</h3>
        <p>Gerencie espécies, parâmetros e estruturas de referência do sistema.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Abrir Base Mestre", use_container_width=True, key="go_base"):
        st.switch_page("pages/00_Base_Mestre.py")

with col2:
    st.markdown("""
    <div class="opyta-card">
        <div class="opyta-tag">Entrada de dados</div>
        <h3>📥 Importação</h3>
        <p>Envie planilhas, valide automaticamente e prepare os dados para migração.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Abrir Importação", use_container_width=True, key="go_import"):
        st.switch_page("pages/01_Importacao.py")

col3, col4 = st.columns(2, gap="large")

with col3:
    st.markdown("""
    <div class="opyta-card">
        <div class="opyta-tag">Processamento</div>
        <h3>⚙️ Consolidação</h3>
        <p>Reúna as tabelas processadas e atualize a base consolidada para análise.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Abrir Consolidação", use_container_width=True, key="go_cons"):
        st.switch_page("pages/02_Consolidacao.py")

with col4:
    st.markdown("""
    <div class="opyta-card">
        <div class="opyta-tag">Análises</div>
        <h3>📊 Análises Ecológicas</h3>
        <p>Execute análises taxonômicas, diversidade e índices específicos por grupo.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Abrir Análises", use_container_width=True, key="go_analises"):
        st.switch_page("pages/03_Analises.py")
st.write("")
col5, col6 = st.columns(2, gap="large")

with col5:
    st.markdown("""
    <div class="opyta-card">
        <div class="opyta-tag">Saída</div>
        <h3>📤 Exportação</h3>
        <p>Exporte dados consolidados para CSV ou Excel com filtros de projeto e grupo.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Abrir Exportação", use_container_width=True, key="go_export"):
        st.switch_page("pages/04_Exportacao.py")

with col6:
    st.markdown("""
    <div class="opyta-card">
        <div class="opyta-tag">Em breve</div>
        <h3>🔜 Relatórios</h3>
        <p>Geração automática de relatórios técnicos e apresentações.</p>
    </div>
    """, unsafe_allow_html=True)
st.markdown("---")
st.caption(f"Usuário conectado: {st.session_state.get('logged_user', '—')}")