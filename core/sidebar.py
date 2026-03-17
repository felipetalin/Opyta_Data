import streamlit as st

def render_sidebar():

    with st.sidebar:

        st.markdown("## 🌿 OPYTA DATA")

        st.markdown("---")

        st.page_link("app/main.py", label="🏠 Início")
        st.page_link("app/pages/01_dashboard.py", label="📊 Dashboard")
        st.page_link("app/pages/02_importacao.py", label="📥 Importação")
        st.page_link("app/pages/03_analises.py", label="📈 Análises")

        st.markdown("---")

        if st.button("Sair"):
            st.session_state.logged_in = False
            st.rerun()