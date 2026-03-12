import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="OPYTA DATA", layout="wide")


def require_login():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if st.session_state.logged_in:
        return True

    st.title("🔐 OPYTA DATA — Login")
    st.info("Informe usuário e senha para acessar o sistema.")

    user_input = st.text_input("Usuário")
    pwd_input = st.text_input("Senha", type="password")

    if st.button("Entrar", use_container_width=True):
        valid_user = os.getenv("OPTA_LOGIN_EMAIL")
        valid_pwd = os.getenv("OPTA_LOGIN_PASSWORD")

        if not valid_user or not valid_pwd:
            st.error("OPTA_LOGIN_EMAIL / OPTA_LOGIN_PASSWORD não configurados no .env")
            return False

        if user_input == valid_user and pwd_input == valid_pwd:
            st.session_state.logged_in = True
            st.session_state.logged_user = user_input
            st.success("Login realizado com sucesso.")
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos.")

    return False


if not require_login():
    st.stop()


with st.sidebar:
    st.title("Navegação")
    st.success(f"Usuário: {st.session_state.get('logged_user', 'logado')}")
    st.page_link("pages/00_Base_Mestre.py", label="00 — Base Mestre")
    st.page_link("pages/01_Importacao.py", label="01 — Importação")
    st.page_link("pages/02_Consolidacao.py", label="02 — Consolidação")

    if st.button("Sair", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.logged_user = None
        st.rerun()


st.title("OPYTA DATA — Fase 1")
st.write("Use o menu à esquerda.")