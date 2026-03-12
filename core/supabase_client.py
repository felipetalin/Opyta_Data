import os
from dotenv import load_dotenv
from supabase import create_client, Client
import streamlit as st

load_dotenv()

@st.cache_resource
def get_supabase() -> Client:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL e SUPABASE_ANON_KEY não configurados no .env")
    return create_client(url, key)