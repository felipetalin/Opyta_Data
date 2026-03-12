import streamlit as st

# 🔐 Proteção de login
if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

from pathlib import Path
from datetime import datetime
import re

from core.supabase_client import get_supabase
from runners.script_runner import run_python_script
from runners.registry import ACTIONS


st.title("00 — Base Mestre (Espécies e Parâmetros)")

# Raiz do projeto: .../Opyta_Data
PROJECT_ROOT = Path(__file__).resolve().parents[2]

supabase = get_supabase()

RUNTIME_DIR = Path("runtime/base_mestre")
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


# ----------------------------
# Helpers: logs
# ----------------------------
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


# ----------------------------
# Helpers: parse stdout (Espécies)
# ----------------------------
def parse_species_stdout(stdout: str) -> dict:
    """Extrai métricas do stdout do cadastrar_especies.py."""
    data = {
        "bacias_processadas": None,
        "especies_processadas": None,
        "endemismo_nao_mapeado": None,
        "success": False,
        "warnings": [],
    }

    if not stdout:
        return data

    # sucesso
    if "CADASTRO MESTRE DE ESPÉCIES CONCLUÍDO COM SUCESSO" in stdout.upper():
        data["success"] = True
    elif "CONCLUÍDO COM SUCESSO" in stdout.upper():
        data["success"] = True

    # bacias
    m = re.search(r"(\d+)\s+registros\s+de\s+bacias\s+processados", stdout, re.IGNORECASE)
    if m:
        data["bacias_processadas"] = int(m.group(1))

    # espécies
    m = re.search(r"Tabela\s+de\s+Espécies.*?(\d+)\s+registros\s+processados", stdout, re.IGNORECASE | re.DOTALL)
    if m:
        data["especies_processadas"] = int(m.group(1))

    # endemismo aviso
    m = re.search(r"Aviso:\s+(\d+)\s+linha\(s\).*?endemismo.*?não\s+puderam\s+ser\s+mapeadas", stdout, re.IGNORECASE)
    if m:
        data["endemismo_nao_mapeado"] = int(m.group(1))

    # warnings
    for line in stdout.splitlines():
        if "Warning" in line or "WARNING" in line:
            data["warnings"].append(line.strip())

    return data


# ----------------------------
# Helpers: parse stdout (Parâmetros) - genérico (ajustamos amanhã se quiser detalhar)
# ----------------------------
def parse_parametros_stdout(stdout: str) -> dict:
    """Parser simples para cadastrar_parametros.py (sem saber ainda o padrão exato do stdout)."""
    data = {
        "success": False,
        "warnings": [],
    }
    if not stdout:
        return data
    if "CONCLUÍDO COM SUCESSO" in stdout.upper() or "SUCESSO" in stdout.upper():
        data["success"] = True
    for line in stdout.splitlines():
        if "Warning" in line or "WARNING" in line:
            data["warnings"].append(line.strip())
    return data


# ----------------------------
# Helpers: render
# ----------------------------
def render_run_summary(title: str, status: str, stdout: str, parsed: dict | None = None):
    ok = (status == "success")
    if ok:
        st.success(f"{title}: sucesso ✅")
    else:
        st.error(f"{title}: falhou ❌")

    cols = st.columns(4)
    cols[0].metric("Status", "Sucesso" if ok else "Erro")

    if parsed:
        # Métricas específicas de espécies
        if "especies_processadas" in parsed:
            cols[1].metric("Espécies", parsed.get("especies_processadas") or 0)
            cols[2].metric("Bacias", parsed.get("bacias_processadas") or 0)
            cols[3].metric("Avisos (endemismo)", parsed.get("endemismo_nao_mapeado") or 0)
        else:
            # parâmetros (genérico)
            cols[1].metric("Avisos", len(parsed.get("warnings", [])))
            cols[2].metric("Detalhes", "-")
            cols[3].metric("—", "—")

    with st.expander("Ver log completo"):
        st.code(stdout or "(sem saída)")

    # mostrar warnings destacados (opcional)
    if parsed and parsed.get("warnings"):
        with st.expander("Warnings detectados"):
            for w in parsed["warnings"]:
                st.write("-", w)


# ----------------------------
# Runner da Base Mestre
# ----------------------------
def run_base_action(action_key: str, uploaded_bytes: bytes):
    spec = ACTIONS[action_key]
    if not spec.expected_excel_name:
        st.error("Base action sem expected_excel_name no registry.")
        st.stop()

    # 1) salva com nome fixo que o script espera
    target = RUNTIME_DIR / spec.expected_excel_name
    target.write_bytes(uploaded_bytes)

    # 2) executa script por caminho absoluto, com cwd apontando para o runtime
    script_abs = PROJECT_ROOT / spec.script

    with st.spinner("Executando script..."):
        res = run_python_script(str(script_abs), cwd=RUNTIME_DIR)

    # 3) log no banco
    write_log(spec.key, res.status, res.stdout or "")

    return res


# ----------------------------
# UI
# ----------------------------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Cadastrar Espécies")
    up = st.file_uploader("Upload cadastro_especies_opyta.xlsx", type=["xlsx"], key="up_especies")

    if st.button("Rodar cadastro de espécies", disabled=(up is None)):
        res = run_base_action("BASE_ESPECIES", up.getvalue())
        parsed = parse_species_stdout(res.stdout or "")
        render_run_summary("Cadastro de Espécies", res.status, res.stdout or "", parsed)

with col2:
    st.subheader("Cadastrar Parâmetros")
    up = st.file_uploader("Upload cadastro_parametros_opyta.xlsx", type=["xlsx"], key="up_parametros")

    if st.button("Rodar cadastro de parâmetros", disabled=(up is None)):
        res = run_base_action("BASE_PARAMETROS", up.getvalue())
        parsed = parse_parametros_stdout(res.stdout or "")
        render_run_summary("Cadastro de Parâmetros", res.status, res.stdout or "", parsed)