import streamlit as st
from pathlib import Path
from datetime import datetime

from core.supabase_client import get_supabase
from runners.script_runner import run_python_script
from runners.registry import ACTIONS

st.title("02 — Consolidação")

PROJECT_ROOT = Path(__file__).resolve().parents[2]

supabase = get_supabase()
RUNTIME_DIR = Path("runtime/consolidacao")
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def write_log(grupo: str, status: str, mensagem: str):
    payload = {
        "grupo": grupo,
        "projeto": None,
        "campanha": None,
        "usuario": None,
        "data_execucao": datetime.now().isoformat(),
        "status": status,
        "mensagem": (mensagem or "")[:5000],
    }
    try:
        supabase.table("import_logs").insert(payload).execute()
    except Exception as e:
        st.warning(f"Falha ao gravar log no banco (import_logs): {e}")


spec = ACTIONS["CONSOLIDAR"]

st.info("Executa o script existente de consolidação (processar_dados.py).")

if st.button("Rodar Consolidação"):
    script_abs = PROJECT_ROOT / spec.script
    res = run_python_script(str(script_abs), cwd=RUNTIME_DIR)

    st.code(res.stdout or "(sem saída)")
    write_log(spec.key, res.status, res.stdout or "")

    if res.status == "success":
        st.success("Consolidação concluída ✅")
    else:
        st.error("Consolidação falhou ❌")