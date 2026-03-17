import sys
from pathlib import Path

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
    if st.session_state.logged_in:
        return True

    _, center, _ = st.columns([1.1, 1.4, 1.1])

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

render_sidebar()

st.markdown("""
<div class="opyta-hero">
    <h1 style="margin-bottom:0;">OPYTA DATA</h1>
    <p class="opyta-sub">Centralize importação, consolidação e análises ambientais em um único fluxo.</p>
</div>
""", unsafe_allow_html=True)

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
        <div class="opyta-tag">Resultados</div>
        <h3>📊 Análises</h3>
        <p>Execute análises ecológicas, visualize gráficos e prepare saídas técnicas.</p>
    </div>
    """, unsafe_allow_html=True)
    if st.button("Abrir Análises", use_container_width=True, key="go_analises"):
        st.switch_page("pages/03_Analises.py")

st.markdown("---")
st.caption(f"Usuário conectado: {st.session_state.get('logged_user', '—')}")