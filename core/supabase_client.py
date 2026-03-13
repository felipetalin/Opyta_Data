# core/supabase_client.py
from __future__ import annotations

import os

import streamlit as st
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()


@st.cache_resource
def get_supabase() -> Client:
    """
    Prioridade:
    1) st.secrets
    2) variáveis de ambiente / .env
    """
    url = None
    key = None

    # 1) Streamlit secrets
    try:
        url = st.secrets.get("SUPABASE_URL", None)
        key = st.secrets.get("SUPABASE_ANON_KEY", None)
    except Exception:
        pass

    # 2) fallback local
    if not url:
        url = os.getenv("SUPABASE_URL")
    if not key:
        key = os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        raise RuntimeError("SUPABASE_URL e SUPABASE_ANON_KEY não configurados.")

    return create_client(url, key)