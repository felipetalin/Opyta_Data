import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="OPYTA DATA",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

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
    if st.session_state.logged_in:
        return True

    _, center, _ = st.columns([1.2, 1.5, 1.2])

    with center:
        st.image("app/assets/logo.png", width=260)
        st.markdown("### Acesso à plataforma")
        st.write("Entre com seu usuário e senha para continuar.")

        user_input = st.text_input("Usuário")
        pwd_input = st.text_input("Senha", type="password")

        if st.button("Entrar", use_container_width=True):
            users = {
                "ismayllen@opyta.com.br": "123456",
                "anamoreira@opyta.com.br": "123456",
                "yurisimoes@opyta.com.br": "123456",
                "wilder@opyta.com.br": "123456",
                "felipetalin@opyta.com.br": "FTNblind19!",
            }

            if user_input in users and pwd_input == users[user_input]:
                st.session_state.logged_in = True
                st.session_state.logged_user = user_input
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos.")

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
    st.page_link("pages/02_Consolidacao.py", label="⚙️ Consolidação")
    st.page_link("pages/03_Analises.py", label="📊 Análises")

    st.divider()

    if st.button("🚪 Sair", use_container_width=True):
        st.session_state.logged_in = False
        st.session_state.logged_user = None
        st.rerun()

st.image("app/assets/logo.png", width=320)
st.subheader("Plataforma de dados e análises ambientais")
st.write("Use o menu à esquerda para navegar pelo sistema.")