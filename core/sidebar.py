import streamlit as st

def render_sidebar():
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