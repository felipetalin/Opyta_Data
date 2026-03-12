from auth import check_password
import streamlit as st

if not check_password():
    st.stop()