import streamlit as st

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.title("🔐 Acesso restrito")
    password = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if password == st.secrets["APP_PASSWORD"]:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Senha incorreta")

    return False