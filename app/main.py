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
    layout="wide"
)

# pequeno ajuste visual
st.markdown(
    "<style>img {margin-bottom:20px;}</style>",
    unsafe_allow_html=True
)


def require_login():

    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if st.session_state.logged_in:
        return True

    # centraliza login
    col_left, col_center, col_right = st.columns([1, 2, 1])

    with col_center:

        st.image("app/assets/logo.png", width=260)

        st.markdown("### Acesso à plataforma")
        st.info("Informe usuário e senha para acessar o sistema.")

        user_input = st.text_input("Usuário")
        pwd_input = st.text_input("Senha", type="password")

        if st.button("Entrar", use_container_width=True):

            valid_user = os.getenv("OPTA_LOGIN_EMAIL") or st.secrets.get("OPTA_LOGIN_EMAIL")
            valid_pwd = os.getenv("OPTA_LOGIN_PASSWORD") or st.secrets.get("OPTA_LOGIN_PASSWORD")

            if not valid_user or not valid_pwd:
                st.error("Credenciais não configuradas.")
                return False

            if user_input == valid_user and pwd_input == valid_pwd:
                st.session_state.logged_in = True
                st.session_state.logged_user = user_input
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos.")

    return False


if not require_login():
    st.stop()


# sidebar
with st.sidebar:

    st.image("app/assets/logo.png", use_container_width=True)

    st.markdown("### Navegação")

    st.success(f"Usuário: {st.session_state.get('logged_user', 'logado')}")

    st.page_link("pages/00_Base_Mestre.py", label="📚 Base Mestre")
    st.page_link("pages/01_Importacao.py", label="📥 Importação")
    st.page_link("pages/02_Consolidacao.py", label="⚙ Consolidação")
    st.page_link("pages/03_Analises.py", label="📊 Análises")

    st.divider()

    if st.button("🚪 Sair", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.logged_user = None
        st.rerun()


# tela inicial
st.image("app/assets/logo.png", width=320)

st.write("Use o menu à esquerda para navegar pelo sistema.")