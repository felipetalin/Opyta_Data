import streamlit as st

def render_sidebar():
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

        st.markdown(
            "<div style='font-size:0.82rem; font-weight:700; color:#6a744f; margin:16px 0 8px 2px;'>SAÍDA</div>",
            unsafe_allow_html=True,
        )
        st.page_link("pages/04_Exportacao.py", label="📤 Exportação")

        st.divider()

        if st.button("Sair", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.logged_user = None
            st.rerun()