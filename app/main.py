import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="OPYTA DATA",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
}

.block-container {
    padding-top: 2rem;
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
    border-radius: 12px;
}

[data-testid="stAlert"] {
    border-radius: 12px;
}

.login-card {
    background: white;
    padding: 2rem 2rem 1.5rem 2rem;
    border-radius: 20px;
    box-shadow: 0 6px 24px rgba(0,0,0,0.08);
    border: 1px solid #e8ecdf;
}

.login-title {
    color: #2f3a1f;
    font-weight: 700;
    margin-top: 0.5rem;
    margin-bottom: 0.2rem;
}

.login-subtitle {
    color: #5f6b46;
    margin-bottom: 1.2rem;
}
</style>
""", unsafe_allow_html=True)


def require_login():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if st.session_state.logged_in:
        return True

    left, center, right = st.columns([1.2, 1.5, 1.2])

    with center:
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        st.image("app/assets/logo.png", width=260)
        st.markdown('<div class="login-title">Acesso à plataforma</div>', unsafe_allow_html=True)
        st.markdown('<div class="login-subtitle">Entre com seu usuário e senha para continuar.</div>', unsafe_allow_html=True)

        user_input = st.text_input("Usuário")
        pwd_input = st.text_input("Senha", type="password")

        if st.button("Entrar", use_container_width=True):
            valid_user = os.getenv("OPTA_LOGIN_EMAIL") or st.secrets.get("OPTA_LOGIN_EMAIL")
            valid_pwd = os.getenv("OPTA_LOGIN_PASSWORD") or st.secrets.get("OPTA_LOGIN_PASSWORD")

            if not valid_user or not valid_pwd:
                st.error("Credenciais não configuradas.")
                st.markdown("</div>", unsafe_allow_html=True)
                return False

            if user_input == valid_user and pwd_input == valid_pwd:
                st.session_state.logged_in = True
                st.session_state.logged_user = user_input
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos.")

        st.markdown('</div>', unsafe_allow_html=True)

    return False


if not require_login():
    st.stop()


with st.sidebar:
    st.image("app/assets/logo.png", use_container_width=True)
    st.markdown("### Navegação")
    st.success(f"Usuário: {st.session_state.get('logged_user', 'logado')}")

    st.page_link("main.py", label="🏠 Início")
    st.page_link("pages/00_Base_Mestre.py", label="📚 Base Mestre")
    st.page_link("pages/01_Importacao.py", label="📥 Importação")
    st.page_link("pages/02_Consolidacao.py", label="⚙ Consolidação")
    st.page_link("pages/03_Analises.py", label="📊 Análises")

    st.divider()

    if st.button("🚪 Sair", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.logged_user = None
        st.rerun()


st.image("app/assets/logo.png", width=320)
st.subheader("Plataforma de dados e análises ambientais")
st.write("Use o menu à esquerda para navegar pelo sistema.")