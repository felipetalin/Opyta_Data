from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

from core.engine import get_engine
from core.sidebar import render_sidebar

if not st.session_state.get("logged_in"):
    st.switch_page("main.py")

st.set_page_config(page_title="04 - Exportação", layout="wide")

render_sidebar()

if "logged_in" not in st.session_state or not st.session_state.logged_in:
    st.warning("Faça login para acessar esta página.")
    st.stop()

st.title("04 — Exportação")

st.markdown(
    "📌 **Etapa final:** Exporte dados consolidados em dois formatos: Dados Brutos ou Darwin Core (padrão internacional).\n"
    "✅ Selecione projeto, grupo e filtros, depois clique em Carregar e Exportar."
)

EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


@st.cache_data(show_spinner=False)
def listar_projetos() -> list[str]:
    engine = get_engine()
    sql = """
        SELECT DISTINCT nome_projeto
        FROM public.biota_analise_consolidada
        WHERE nome_projeto IS NOT NULL
        ORDER BY 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df["nome_projeto"].astype(str).tolist()


@st.cache_data(show_spinner=False)
def listar_grupos() -> list[str]:
    engine = get_engine()
    sql = """
        SELECT DISTINCT grupo_biologico
        FROM public.biota_analise_consolidada
        WHERE grupo_biologico IS NOT NULL
        ORDER BY 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df["grupo_biologico"].astype(str).tolist()


@st.cache_data(show_spinner=False)
def listar_campanhas(projeto: str, grupo: str) -> list[str]:
    engine = get_engine()
    sql = """
        SELECT DISTINCT nome_campanha
        FROM public.biota_analise_consolidada
        WHERE nome_projeto = %(projeto)s
          AND grupo_biologico = %(grupo)s
          AND nome_campanha IS NOT NULL
        ORDER BY 1
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"projeto": projeto, "grupo": grupo})
    return df["nome_campanha"].astype(str).tolist()


@st.cache_data(show_spinner=False)
def carregar_dados(projeto: str, grupo: str, campanha: str) -> pd.DataFrame:
    engine = get_engine()
    clauses: list[str] = []
    params: dict[str, str] = {}

    if projeto:
        clauses.append("nome_projeto = %(projeto)s")
        params["projeto"] = projeto
    if grupo:
        clauses.append("grupo_biologico = %(grupo)s")
        params["grupo"] = grupo
    if campanha and campanha != "(todas)":
        clauses.append("nome_campanha = %(campanha)s")
        params["campanha"] = campanha

    query = "SELECT * FROM public.biota_analise_consolidada"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY nome_projeto, grupo_biologico, nome_campanha"

    with engine.connect() as conn:
        return pd.read_sql(query, conn, params=params)


def criar_arquivo_csv(df: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def criar_arquivo_excel(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="dados")
    return buffer.getvalue()


def normalizar_taxon_rank(nome_cientifico):
    if pd.isna(nome_cientifico):
        return None
    nome = str(nome_cientifico).strip()
    if " sp." in nome.lower() or nome.lower().endswith(" sp"):
        return "Genus"
    if len(nome.split()) >= 2:
        return "Species"
    return None


def montar_event_id(row):
    """Monta eventID a partir de id_resultado_pk, campanha, ponto e protocolo."""
    partes = []
    
    if "id_resultado_pk" in row.index and pd.notna(row["id_resultado_pk"]):
        partes.append(str(row["id_resultado_pk"]).strip())
    
    if "nome_campanha" in row.index and pd.notna(row["nome_campanha"]):
        partes.append(str(row["nome_campanha"]).strip().replace(" ", "_"))
    
    if "nome_ponto" in row.index and pd.notna(row["nome_ponto"]):
        partes.append(str(row["nome_ponto"]).strip().replace(" ", "_"))
    
    if "metodo_de_captura" in row.index and pd.notna(row["metodo_de_captura"]):
        partes.append(str(row["metodo_de_captura"]).strip().replace(" ", "_"))
    
    return "-".join(partes) if partes else "EVENT_UNKNOWN"


def montar_occurrence_id(eventID: str, nome_cientifico: str, idx: int) -> str:
    """Monta occurrenceID a partir de eventID, espécie e índice."""
    nome_clean = str(nome_cientifico).strip().replace(" ", "_") if pd.notna(nome_cientifico) else "sp"
    return f"{eventID}-{nome_clean}-{idx+1}"


def montar_sampling_effort(row):
    """Monta samplingEffort a partir de esforço e unidade."""
    esforco = row.get("esforco")
    unidade = row.get("unidade_esforco")
    
    esforco_txt = "" if pd.isna(esforco) or esforco is None else str(esforco).strip()
    unidade_txt = "" if pd.isna(unidade) or unidade is None else str(unidade).strip()
    
    texto = f"{esforco_txt} {unidade_txt}".strip()
    return texto if texto else None


def gerar_darwin_core_excel(df: pd.DataFrame, projeto: str, grupo: str) -> bytes:
    """Gera arquivo Excel com estrutura Darwin Core (Sampling Events, Occurrences, Biometric data)."""
    
    # Campos padrão Darwin Core
    event_cols = [
        'eventID', 'samplingProtocol', 'samplingEffort', 'sampleSizeValue',
        'sampleSizeUnit', 'eventDate', '@eventRemarks', 'county', 'municipality',
        'waterBody', 'locality', 'decimalLatitude', 'decimalLongitude', 'geodeticDatum'
    ]
    
    occ_cols = [
        'eventID', 'occurrenceID', 'basisOfRecord', 'scientificName', 'kingdom',
        'phylum', 'class', 'order', 'family', 'taxonRank', 'identificationQualifier',
        'recordedBy', 'individualCount', 'sex', 'lifeStage', 'reproductiveCondition',
        'preparations', '@occurrenceRemarks'
    ]
    
    bio_cols = [
        'eventID', 'occurrenceID', 'scientificName', 'individualCount',
        'Weight', 'StandardLength', 'TotalLength', 'Sex', 'GonadalStage', 'GonadWeight'
    ]
    
    # Monta campos derivados
    df['eventID'] = df.apply(montar_event_id, axis=1)
    df['occurrenceID'] = [
        montar_occurrence_id(df.iloc[i]['eventID'], df.iloc[i].get('nome_cientifico'), i)
        for i in range(len(df))
    ]
    df['taxonRank_DWC'] = df['nome_cientifico'].apply(normalizar_taxon_rank)
    
    # 1. SAMPLING EVENTS
    eventos = pd.DataFrame({
        'eventID': df['eventID'],
        'samplingProtocol': df.get('metodo_de_captura') if 'metodo_de_captura' in df.columns else None,
        'samplingEffort': [montar_sampling_effort(row) for _, row in df.iterrows()],
        'sampleSizeValue': df.get('unidade_esforco') if 'unidade_esforco' in df.columns else None,
        'sampleSizeUnit': df.get('esforco') if 'esforco' in df.columns else None,
        'eventDate': df.get('nome_campanha') if 'nome_campanha' in df.columns else None,
        '@eventRemarks': None,
        'county': df.get('municipio', 'N/A') if 'municipio' in df.columns else 'N/A',
        'municipality': df.get('municipio', 'N/A') if 'municipio' in df.columns else 'N/A',
        'waterBody': df.get('bacia_hidrografica') if 'bacia_hidrografica' in df.columns else None,
        'locality': df.get('nome_ponto') if 'nome_ponto' in df.columns else None,
        'decimalLatitude': df.get('latitude') if 'latitude' in df.columns else None,
        'decimalLongitude': df.get('longitude') if 'longitude' in df.columns else None,
        'geodeticDatum': 'WGS84'
    })
    df_sampling = eventos[event_cols].drop_duplicates(subset=['eventID']).reset_index(drop=True)
    
    # 2. ASSOCIATED OCCURRENCES
    df_occ = pd.DataFrame({
        'eventID': df['eventID'],
        'occurrenceID': df['occurrenceID'],
        'basisOfRecord': 'HumanObservation',
        'scientificName': df['nome_cientifico'],
        'kingdom': df.get('reino') if 'reino' in df.columns else None,
        'phylum': df.get('filo') if 'filo' in df.columns else None,
        'class': df.get('classe') if 'classe' in df.columns else None,
        'order': df.get('ordem') if 'ordem' in df.columns else None,
        'family': df.get('familia') if 'familia' in df.columns else None,
        'taxonRank': df['taxonRank_DWC'],
        'identificationQualifier': None,
        'recordedBy': None,
        'individualCount': df.get('contagem') if 'contagem' in df.columns else None,
        'sex': None,
        'lifeStage': None,
        'reproductiveCondition': None,
        'preparations': None,
        '@occurrenceRemarks': None,
    })[occ_cols]
    
    # 3. FISH BIOMETRIC DATA
    df_bio = pd.DataFrame({
        'eventID': df['eventID'],
        'occurrenceID': df['occurrenceID'],
        'scientificName': df['nome_cientifico'],
        'individualCount': df.get('contagem') if 'contagem' in df.columns else None,
        'Weight': df.get('biomassa') if 'biomassa' in df.columns else None,
        'StandardLength': df.get('medida_1') if 'medida_1' in df.columns else None,
        'TotalLength': df.get('medida_2') if 'medida_2' in df.columns else None,
        'Sex': None,
        'GonadalStage': None,
        'GonadWeight': None,
    })[bio_cols]
    
    colunas_medidas = ['Weight', 'StandardLength', 'TotalLength', 'Sex', 'GonadalStage', 'GonadWeight']
    df_bio = df_bio[df_bio[colunas_medidas].notna().any(axis=1)].reset_index(drop=True)
    
    # Cria Excel com múltiplas abas
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_sampling.to_excel(writer, index=False, sheet_name="Sampling Events")
        df_occ.to_excel(writer, index=False, sheet_name="Associated Occurrences")
        df_bio.to_excel(writer, index=False, sheet_name="Fish Biometric data")
    
    return buffer.getvalue()


with st.expander("Filtros de exportação", expanded=True):
    projetos = listar_projetos()
    grupos = listar_grupos()

    col1, col2 = st.columns(2)
    
    with col1:
        projeto = st.selectbox("Projeto", options=[""] + projetos, index=0)
    
    with col2:
        grupo = st.selectbox("Grupo biológico", options=[""] + grupos, index=0)

    campanhas: list[str] = []
    campanha = ""
    if projeto and grupo:
        try:
            campanhas = listar_campanhas(projeto, grupo)
        except Exception:
            campanhas = []

        campanha = st.selectbox(
            "Campanha",
            options=["(todas)"] + campanhas,
            index=0,
        )
    else:
        st.selectbox("Campanha", options=["(preencha projeto e grupo)"], index=0, disabled=True)

    st.markdown("---")
    modo_export = st.radio(
        "Formato de exportação",
        options=["📊 Dados Brutos", "🌍 Darwin Core"],
        index=0,
        horizontal=True,
    )
    
    st.write(
        "**Dados Brutos**: Todas as colunas do banco consolidado. | **Darwin Core**: Estrutura padrão internacional (Sampling Events, Occurrences, Biometric)."
    )

carregar = st.button("Carregar dados para exportação")

if carregar:
    if not projeto and not grupo:
        st.warning("Selecione ao menos Projeto ou Grupo para evitar consultas muito grandes.")
    else:
        with st.spinner("Consultando dados..."):
            try:
                df = carregar_dados(projeto, grupo, campanha)
            except Exception as exc:
                st.error(f"Erro ao consultar dados: {exc}")
                df = pd.DataFrame()

        if df.empty:
            st.warning("Nenhum registro encontrado com os filtros selecionados.")
        else:
            st.success(f"{len(df):,} registros carregados.")
            
            if "Dados Brutos" in modo_export:
                with st.expander("Prévia dos dados", expanded=False):
                    st.dataframe(df.head(100), use_container_width=True)

                nome_base = projeto or grupo or "export"
                nome_campanha = campanha if campanha and campanha != "(todas)" else "todas"
                nome_base = nome_base.replace(" ", "_").lower()

                csv_bytes = criar_arquivo_csv(df)
                xlsx_bytes = criar_arquivo_excel(df)

                csv_name = f"export_{nome_base}_{nome_campanha}.csv"
                xlsx_name = f"export_{nome_base}_{nome_campanha}.xlsx"

                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        label="📥 Baixar CSV",
                        data=csv_bytes,
                        file_name=csv_name,
                        mime="text/csv",
                    )
                with col2:
                    st.download_button(
                        label="📥 Baixar Excel",
                        data=xlsx_bytes,
                        file_name=xlsx_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

                Path(EXPORT_DIR / csv_name).write_bytes(csv_bytes)
                Path(EXPORT_DIR / xlsx_name).write_bytes(xlsx_bytes)

                st.info(f"✅ Arquivos salvos em `{EXPORT_DIR.resolve()}`")
            
            else:  # Darwin Core
                try:
                    with st.spinner("Gerando estrutura Darwin Core..."):
                        dc_bytes = gerar_darwin_core_excel(df, projeto, grupo)
                    
                    nome_base = projeto or grupo or "export"
                    nome_campanha = campanha if campanha and campanha != "(todas)" else "todas"
                    nome_base = nome_base.replace(" ", "_").lower()
                    dc_name = f"DarwinCore_{nome_base}_{nome_campanha}.xlsx"
                    
                    st.download_button(
                        label="📥 Baixar Darwin Core (Excel)",
                        data=dc_bytes,
                        file_name=dc_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                    
                    Path(EXPORT_DIR / dc_name).write_bytes(dc_bytes)
                    st.success(f"✅ Darwin Core gerado com sucesso! (3 abas: Sampling Events, Occurrences, Biometric)")
                    st.info(f"📂 Arquivo também salvo em `{EXPORT_DIR.resolve()}`")
                
                except Exception as exc:
                    st.error(f"❌ Erro ao gerar Darwin Core: {exc}")

