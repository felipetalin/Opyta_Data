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
    # Fase 1: login operacional simples (não expõe credenciais em tela)
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    if st.session_state.logged_in:
        return True

    st.title("OPYTA DATA — Login (Fase 1)")
    st.info("Login operacional da Fase 1 (credenciais via .env).")

    if st.button("Entrar"):
        email = os.getenv("OPTA_LOGIN_EMAIL")
        pwd = os.getenv("OPTA_LOGIN_PASSWORD")
        if not email or not pwd:
            st.error("OPTA_LOGIN_EMAIL / OPTA_LOGIN_PASSWORD não configurados no .env")
            return False
        st.session_state.logged_in = True
        st.success("Login OK.")
        st.rerun()

    return False

if not require_login():
    st.stop()

st.sidebar.title("Navegação")
st.sidebar.page_link("pages/00_Base_Mestre.py", label="00 — Base Mestre")
st.sidebar.page_link("pages/01_Importacao.py", label="01 — Importação")
st.sidebar.page_link("pages/02_Consolidacao.py", label="02 — Consolidação")

st.title("OPYTA DATA — Fase 1")
st.write("Use o menu à esquerda.")