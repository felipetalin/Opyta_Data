from __future__ import annotations

import sys
from pathlib import Path
import os
import traceback
import logging

import pandas as pd
from sqlalchemy import text

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Garante que a pasta raiz do projeto esteja no path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.engine import get_engine
from validators.especies.pipeline import validate_especies_file
from validators.importacao.pipeline import validate_importacao_file

# --- CONFIGURAÃ‡Ã•ES ---
ARQUIVO_EXCEL = sys.argv[1] if len(sys.argv) > 1 else "projeto_mastofauna_real.xlsx"
GRUPO_BIOLOGICO_ALVO = sys.argv[2] if len(sys.argv) > 2 else "Mastofauna"
NOME_TABELA_RESULTADOS = f"resultados_{GRUPO_BIOLOGICO_ALVO.lower().replace(' ', '_')}"
NOME_ABA_RESULTADOS = f"Resultados_{GRUPO_BIOLOGICO_ALVO.replace(' ', '_')}"

ABAS_OBRIGATORIAS = [
    "Capa_Projeto",
    "Pontos_e_Campanhas",
    "Metadados_Esforco",
    "Especies",
    NOME_ABA_RESULTADOS,
]


def validar_abas_excel(xls: pd.ExcelFile, abas_obrigatorias: list[str]) -> None:
    abas_ausentes = [aba for aba in abas_obrigatorias if aba not in xls.sheet_names]
    if abas_ausentes:
        raise ValueError(
            f"Abas obrigatÃ³rias ausentes no Excel: {abas_ausentes}. "
            f"Abas disponÃ­veis: {xls.sheet_names}"
        )
    logger.info(f"ValidaÃ§Ã£o de abas OK: {abas_obrigatorias}")


def normalizar_texto(valor):
    if pd.isna(valor):
        return None
    valor = str(valor).replace("\ufeff", "").strip()
    # Colapsa quebras de linha e espaços duplicados para padronizar chaves de mapeamento.
    valor = " ".join(valor.split())
    return valor if valor else None


def normalizar_chave(valor):
    txt = normalizar_texto(valor)
    return txt.lower() if txt else None


def normalizar_grupo(valor):
    if pd.isna(valor):
        return None
    return str(valor).strip().lower()


def normalizar_numero(valor):
    if pd.isna(valor):
        return None
    return valor


def normalizar_data(valor):
    if pd.isna(valor):
        return None
    try:
        dt = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.tz_localize(None)
    except Exception:
        return None


def reportar_validacao(titulo: str, report) -> bool:
    logger.info(
        "%s: %s bloqueio(s), %s aviso(s).",
        titulo,
        len(report.blocks),
        len(report.warnings),
    )
    for issue in report.blocks[:10]:
        logger.error("%s [%s] %s", titulo, issue.code, issue.message)
    for issue in report.warnings[:5]:
        logger.warning("%s [%s] %s", titulo, issue.code, issue.message)
    return not report.blocks


def limpar_dados_da_campanha(connection, id_projeto, df_pontos_da_planilha, tabela_resultados: str):
    nomes_campanhas_na_planilha = (
        df_pontos_da_planilha["Campanha"].dropna().astype(str).str.strip().unique().tolist()
    )

    if not nomes_campanhas_na_planilha:
        logger.info("Nenhuma campanha encontrada na planilha para limpar.")
        return

    logger.info(
        f"Verificando e limpando dados ANTIGOS de {GRUPO_BIOLOGICO_ALVO} "
        f"para as campanhas: {nomes_campanhas_na_planilha}..."
    )

    query_ids_campanha = text(
        "SELECT id_campanha FROM public.campanhas WHERE nome_campanha = ANY(:nomes)"
    )
    ids_campanha_para_limpar = connection.execute(
        query_ids_campanha, {"nomes": nomes_campanhas_na_planilha}
    ).scalars().all()

    if not ids_campanha_para_limpar:
        logger.info("Campanhas sÃ£o novas no banco, nenhuma limpeza necessÃ¡ria.")
        return

    params = {
        "id_proj": id_projeto,
        "ids_camp": tuple(ids_campanha_para_limpar),
        "grupo": GRUPO_BIOLOGICO_ALVO,
    }

    logger.info(f"Removendo resultados antigos de {tabela_resultados}...")
    query_delete_res = text(f"""
        DELETE FROM {tabela_resultados}
        WHERE id_esforco IN (
            SELECT id_esforco
            FROM esforcos_amostragem
            WHERE grupo_biologico = :grupo
              AND id_ponto_coleta IN (
                  SELECT id_ponto_coleta
                  FROM pontos_coleta
                  WHERE id_projeto = :id_proj
                    AND id_campanha IN :ids_camp
              )
        )
    """)
    connection.execute(query_delete_res, params)

    logger.info(f"Removendo esforÃ§os antigos de {GRUPO_BIOLOGICO_ALVO}...")
    query_delete_esforcos = text("""
        DELETE FROM esforcos_amostragem
        WHERE grupo_biologico = :grupo
          AND id_ponto_coleta IN (
              SELECT id_ponto_coleta
              FROM pontos_coleta
              WHERE id_projeto = :id_proj
                AND id_campanha IN :ids_camp
          )
    """)
    connection.execute(query_delete_esforcos, params)

    logger.info("Pontos de coleta preservados (infraestrutura compartilhada).")
    logger.info(f"Limpeza de {GRUPO_BIOLOGICO_ALVO} concluÃ­da com seguranÃ§a.")


def obter_mapas_de_ids(connection):
    logger.info("Mapeando IDs existentes do banco de dados...")

    especies_map = (
        pd.read_sql(
            text("SELECT id_especie, nome_cientifico FROM especies"),
            connection,
        )
        .assign(
            nome_cientifico=lambda df: df["nome_cientifico"]
            .astype(str)
            .str.replace("\ufeff", "", regex=False)
            .str.strip()
            .str.lower()
        )
        .set_index("nome_cientifico")["id_especie"]
        .to_dict()
    )

    campanhas_map = (
        pd.read_sql(
            text("SELECT id_campanha, nome_campanha FROM campanhas"),
            connection,
        )
        .assign(nome_campanha=lambda df: df["nome_campanha"].astype(str).str.strip())
        .set_index("nome_campanha")["id_campanha"]
        .to_dict()
    )

    return especies_map, campanhas_map


def obter_observacoes_existentes(
    connection, id_projeto: int, tabela_resultados: str
) -> dict:
    """
    Salva as observações manuais antes da limpeza da campanha, indexadas por
    chave natural (campanha, ponto, metodo, id_especie).
    Permite restaurá-las após a reinserção dos resultados.
    """
    try:
        rows = connection.execute(
            text(f"""
                SELECT
                    c.nome_campanha,
                    pc.nome_ponto,
                    ea.metodo_de_captura,
                    rm.id_especie,
                    rm.observacoes
                FROM {tabela_resultados} rm
                JOIN esforcos_amostragem ea ON rm.id_esforco = ea.id_esforco
                JOIN pontos_coleta pc ON ea.id_ponto_coleta = pc.id_ponto_coleta
                JOIN campanhas c ON pc.id_campanha = c.id_campanha
                WHERE pc.id_projeto = :id_proj
                  AND ea.grupo_biologico = :grupo
                  AND rm.observacoes IS NOT NULL
                  AND TRIM(rm.observacoes) != ''
            """),
            {"id_proj": id_projeto, "grupo": GRUPO_BIOLOGICO_ALVO},
        ).fetchall()
        obs_map = {
            (
                str(row[0]).strip(),
                str(row[1]).strip(),
                str(row[2]).strip(),
                int(row[3]),
            ): row[4]
            for row in rows
        }
        if obs_map:
            logger.info(
                f"Preservadas {len(obs_map)} observação(ões) manual(is) antes da limpeza."
            )
        return obs_map
    except Exception as exc:
        logger.warning(f"Não foi possível preservar observações: {exc}")
        return {}


def atualizar_especies_da_planilha(connection, df_especies: pd.DataFrame | None):
    """Atualiza cadastro de espécies a partir da aba `Especies` da planilha de campo."""
    if df_especies is None or df_especies.empty:
        logger.info("Aba 'Especies' ausente ou vazia. Cadastro de espécies não será atualizado.")
        return

    logger.info("Atualizando cadastro de espécies com dados ecológicos da aba 'Especies'...")

    rename_map = {
        "Nome_Cientifico": "nome_cientifico",
        "Nome_Popular": "nome_popular",
        "Grupo_Biologico": "grupo_biologico",
        "Reino": "reino",
        "Filo": "filo",
        "Classe": "classe",
        "Ordem": "ordem",
        "Familia": "familia",
        "Genero": "genero",
        "Autor_e_Ano": "autor_e_ano",
        "Status_IUCN": "status_ameaca_global",
        "Status_MMA": "status_ameaca_nacional",
        "Status_COPAM": "status_copam",
        "Status_Copam": "status_copam",
        "Status_Estadual": "status_estadual",
        "CITES": "cites",
        "Cites": "cites",
        "Habito_Alimentar": "habito_alimentar",
        "Guilda_Alimentar": "guilda_alimentar",
        "Dependencia_Florestal": "dependencia_florestal",
        "Endemismo": "endemismo",
        "Sensibilidade_Ambiental": "sensibilidade_ambiental",
        "Migratorio": "migratorio",
        "Raridade": "raridade",
        "Observacoes": "observacoes",
    }

    expected_cols = [
        "nome_cientifico",
        "nome_popular",
        "grupo_biologico",
        "reino",
        "filo",
        "classe",
        "ordem",
        "familia",
        "genero",
        "autor_e_ano",
        "status_ameaca_nacional",
        "status_ameaca_global",
        "status_copam",
        "status_estadual",
        "cites",
        "habito_alimentar",
        "guilda_alimentar",
        "dependencia_florestal",
        "endemismo",
        "sensibilidade_ambiental",
        "migratorio",
        "raridade",
        "observacoes",
    ]

    df = df_especies.rename(columns=rename_map).copy()
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    df = df[expected_cols]
    for col in df.columns:
        df[col] = df[col].apply(normalizar_texto)

    df = df[df["nome_cientifico"].notna()].copy()
    if df.empty:
        logger.info("Nenhuma espécie válida encontrada para atualização.")
        return

    records = df.to_dict("records")

    cols_db = pd.read_sql(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'especies'
            """
        ),
        connection,
    )["column_name"].tolist()
    cols_db_set = set(cols_db)
    available_cols = [c for c in expected_cols if c in cols_db_set]

    rows = [{k: rec.get(k) for k in available_cols} for rec in records]
    if not rows or "nome_cientifico" not in available_cols:
        logger.warning("Não foi possível atualizar espécies: colunas mínimas indisponíveis.")
        return

    insert_cols_sql = ",\n                ".join(available_cols)
    values_cols_sql = ", ".join(f":{c}" for c in available_cols)
    update_cols = [c for c in available_cols if c != "nome_cientifico"]
    update_sql = ",\n                ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    connection.execute(
        text(
            f"""
            INSERT INTO especies (
                {insert_cols_sql}
            )
            VALUES (
                {values_cols_sql}
            )
            ON CONFLICT (nome_cientifico)
            DO UPDATE SET
                {update_sql}
            """
        ),
        rows,
    )

    logger.info("Cadastro de espécies atualizado com %s registro(s) da planilha.", len(rows))


def migrar_dados(
    connection,
    df_capa,
    df_pontos,
    df_esforco,
    df_resultados,
    tabela_resultados: str,
    obs_map: dict | None = None,
    df_especies: pd.DataFrame | None = None,
):
    colunas_obrigatorias_pontos = ["Campanha", "Ponto"]
    for coluna in colunas_obrigatorias_pontos:
        if coluna not in df_pontos.columns:
            raise ValueError(
                f"A aba 'Pontos_e_Campanhas' nÃ£o possui a coluna obrigatÃ³ria '{coluna}'."
            )

    colunas_obrigatorias_esforco = ["Grupo_Biologico", "Campanha", "Ponto", "Metodo_de_Captura"]
    for coluna in colunas_obrigatorias_esforco:
        if coluna not in df_esforco.columns:
            raise ValueError(
                f"A aba 'Metadados_Esforco' nÃ£o possui a coluna obrigatÃ³ria '{coluna}'."
            )

    colunas_obrigatorias_resultados = [
        "Campanha",
        "Ponto",
        "Metodo_de_Captura",
        "Nome_Cientifico",
        "Numero_de_Individuos",
        "Tipo_de_Amostragem",
    ]
    for coluna in colunas_obrigatorias_resultados:
        if coluna not in df_resultados.columns:
            raise ValueError(
                f"A aba '{NOME_ABA_RESULTADOS}' nÃ£o possui a coluna obrigatÃ³ria '{coluna}'."
            )

    logger.info("Colunas obrigatÃ³rias validadas em todas as abas.")

    atualizar_especies_da_planilha(connection, df_especies)
    especies_map, campanhas_map_inicial = obter_mapas_de_ids(connection)

    logger.info("Processando Campanhas e Pontos de Coleta...")
    novas_campanhas = [
        {"nome": c}
        for c in df_pontos["Campanha"].dropna().astype(str).str.strip().unique()
        if c not in campanhas_map_inicial
    ]

    if novas_campanhas:
        connection.execute(
            text(
                """
                INSERT INTO campanhas (nome_campanha)
                VALUES (:nome)
                ON CONFLICT (nome_campanha) DO NOTHING
                """
            ),
            novas_campanhas,
        )

        campanhas_map_atualizado = (
            pd.read_sql(
                text("SELECT id_campanha, nome_campanha FROM campanhas"),
                connection,
            )
            .assign(nome_campanha=lambda df: df["nome_campanha"].astype(str).str.strip())
            .set_index("nome_campanha")["id_campanha"]
            .to_dict()
        )
    else:
        campanhas_map_atualizado = campanhas_map_inicial

    codigo_opyta = df_capa.iloc[0]["Codigo_Opyta"]

    id_projeto = connection.execute(
        text(
            """
            SELECT id_projeto
            FROM projetos
            WHERE codigo_interno_opyta = :codigo
            """
        ),
        {"codigo": codigo_opyta},
    ).scalar_one()

    pontos_records = []
    for _, row in df_pontos.iterrows():
        campanha = normalizar_texto(row.get("Campanha"))
        ponto = normalizar_texto(row.get("Ponto"))

        if not campanha or not ponto:
            continue

        data_coleta = normalizar_data(row.get("Data"))

        pontos_records.append(
            {
                "id_projeto": id_projeto,
                "id_campanha": campanhas_map_atualizado.get(campanha),
                "nome_ponto": ponto,
                "data_hora_coleta": data_coleta,
                "latitude": normalizar_numero(row.get("Latitude")),
                "longitude": normalizar_numero(row.get("Longitude")),
                "bacia_hidrografica": normalizar_texto(row.get("Bacia_Hidrografica")),
            }
        )

    # Carrega pontos já existentes no banco (com ou sem id_empreendimento)
    # para evitar criar duplicatas quando o ponto já existe com empreendimento preenchido.
    pontos_existentes_keys: set[tuple] = set()
    try:
        rows_exist = connection.execute(
            text(
                """
                SELECT ca.nome_campanha, pc.nome_ponto
                FROM pontos_coleta pc
                JOIN campanhas ca ON pc.id_campanha = ca.id_campanha
                WHERE pc.id_projeto = :id_projeto
                """
            ),
            {"id_projeto": id_projeto},
        ).fetchall()
        pontos_existentes_keys = {
            (str(r[0]).strip(), str(r[1]).strip()) for r in rows_exist
        }
        logger.info(
            f"{len(pontos_existentes_keys)} ponto(s) já existem no banco — serão ignorados."
        )
    except Exception as exc:
        logger.warning(f"Não foi possível verificar pontos existentes: {exc}")

    id_to_campanha = {v: k for k, v in campanhas_map_atualizado.items()}
    pontos_novos = [
        rec
        for rec in pontos_records
        if (id_to_campanha.get(rec["id_campanha"]), rec["nome_ponto"])
        not in pontos_existentes_keys
    ]

    if pontos_novos:
        logger.info(f"Inserindo {len(pontos_novos)} novo(s) ponto(s) de coleta.")
        query_pontos = text(
            """
            INSERT INTO pontos_coleta (
                id_projeto, id_campanha, nome_ponto, data_hora_coleta,
                latitude, longitude, bacia_hidrografica
            )
            VALUES (
                :id_projeto, :id_campanha, :nome_ponto, :data_hora_coleta,
                :latitude, :longitude, :bacia_hidrografica
            )
            ON CONFLICT (id_projeto, id_campanha, nome_ponto)
                WHERE id_empreendimento IS NULL
            DO NOTHING
            """
        )
        connection.execute(query_pontos, pontos_novos)
    elif pontos_records:
        logger.info("Todos os pontos já existem no banco — nenhuma inserção necessária.")

    logger.info("Pontos de coleta inseridos/verificados.")

    logger.info(f"Processando EsforÃ§os de Amostragem de {GRUPO_BIOLOGICO_ALVO}...")
    pontos_db_map = (
        pd.read_sql(
            text(
                """
                SELECT pc.id_ponto_coleta, ca.nome_campanha, pc.nome_ponto
                FROM pontos_coleta pc
                JOIN campanhas ca ON pc.id_campanha = ca.id_campanha
                WHERE pc.id_projeto = :id_projeto
                """
            ),
            connection,
            params={"id_projeto": id_projeto},
        )
        .assign(
            nome_campanha=lambda df: df["nome_campanha"].apply(normalizar_chave),
            nome_ponto=lambda df: df["nome_ponto"].apply(normalizar_chave),
        )
        .set_index(["nome_campanha", "nome_ponto"])["id_ponto_coleta"]
        .to_dict()
    )

    esforcos_records = []
    grupo_alvo = normalizar_grupo(GRUPO_BIOLOGICO_ALVO)
    if grupo_alvo == "mastofauna":
        grupos_aceitos = {"mastofauna", "primatas", "primates", "primatologia"}
        df_esforco_filtrado = df_esforco[
            df_esforco["Grupo_Biologico"].apply(normalizar_grupo).isin(grupos_aceitos)
        ].copy()
    else:
        df_esforco_filtrado = df_esforco[
            df_esforco["Grupo_Biologico"].apply(normalizar_grupo) == grupo_alvo
        ].copy()

    for _, row in df_esforco_filtrado.iterrows():
        campanha = normalizar_chave(row.get("Campanha"))
        ponto = normalizar_chave(row.get("Ponto"))
        metodo = normalizar_chave(row.get("Metodo_de_Captura"))

        if not campanha or not ponto or not metodo:
            continue

        chave_ponto = (campanha, ponto)
        id_ponto = pontos_db_map.get(chave_ponto)

        if id_ponto:
            esforcos_records.append(
                {
                    "id_ponto_coleta": id_ponto,
                    "grupo_biologico": GRUPO_BIOLOGICO_ALVO,
                    "metodo_de_captura": metodo,
                    "esforco": normalizar_numero(row.get("Esforco")),
                    "unidade_esforco": normalizar_texto(row.get("Unidade_Esforco")),
                    "tipo_amostragem": normalizar_texto(row.get("Tipo_de_Amostragem")),
                }
            )

    if esforcos_records:
        query_esforcos = text(
            """
            INSERT INTO esforcos_amostragem (
                id_ponto_coleta, grupo_biologico, metodo_de_captura,
                esforco, unidade_esforco, tipo_amostragem
            )
            VALUES (
                :id_ponto_coleta, :grupo_biologico, :metodo_de_captura,
                :esforco, :unidade_esforco, :tipo_amostragem
            )
            ON CONFLICT (id_ponto_coleta, grupo_biologico, metodo_de_captura)
            DO UPDATE SET
                esforco = EXCLUDED.esforco,
                unidade_esforco = EXCLUDED.unidade_esforco,
                tipo_amostragem = EXCLUDED.tipo_amostragem
            """
        )
        connection.execute(query_esforcos, esforcos_records)

    logger.info(
        f"{len(esforcos_records)} esforÃ§os de {GRUPO_BIOLOGICO_ALVO} "
        "inseridos/verificados."
    )

    logger.info(f"Processando e agregando Resultados de {GRUPO_BIOLOGICO_ALVO}...")
    esforcos_db_map = (
        pd.read_sql(
            text(
                """
                SELECT
                    e.id_esforco,
                    c.nome_campanha,
                    p.nome_ponto,
                    e.metodo_de_captura
                FROM esforcos_amostragem e
                JOIN pontos_coleta p ON e.id_ponto_coleta = p.id_ponto_coleta
                JOIN campanhas c ON p.id_campanha = c.id_campanha
                WHERE p.id_projeto = :id_projeto
                  AND e.grupo_biologico = :grupo
                """
            ),
            connection,
            params={
                "id_projeto": id_projeto,
                "grupo": GRUPO_BIOLOGICO_ALVO,
            },
        )
        .assign(
            nome_campanha=lambda df: df["nome_campanha"].apply(normalizar_chave),
            nome_ponto=lambda df: df["nome_ponto"].apply(normalizar_chave),
            metodo_de_captura=lambda df: df["metodo_de_captura"].apply(normalizar_chave),
        )
        .set_index(["nome_campanha", "nome_ponto", "metodo_de_captura"])["id_esforco"]
        .to_dict()
    )

    df_resultados = df_resultados.copy()
    df_resultados["Campanha"] = df_resultados["Campanha"].apply(normalizar_chave)
    df_resultados["Ponto"] = df_resultados["Ponto"].apply(normalizar_chave)
    df_resultados["Metodo_de_Captura"] = df_resultados["Metodo_de_Captura"].apply(normalizar_chave)
    df_resultados["Nome_Cientifico"] = (
        df_resultados["Nome_Cientifico"].apply(normalizar_texto).str.lower()
    )
    df_resultados["Tipo_de_Amostragem"] = df_resultados["Tipo_de_Amostragem"].apply(normalizar_texto)
    df_resultados["Numero_de_Individuos"] = pd.to_numeric(
        df_resultados["Numero_de_Individuos"], errors="coerce"
    )

    df_resultados_agregado = (
        df_resultados.groupby(
            ["Campanha", "Ponto", "Metodo_de_Captura", "Nome_Cientifico", "Tipo_de_Amostragem"],
            dropna=False,
        )
        .agg(Numero_de_Individuos=("Numero_de_Individuos", "sum"))
        .reset_index()
    )

    logger.info(
        "Resultados: %s linha(s) no Excel, %s combinação(ões) após agregação.",
        len(df_resultados),
        len(df_resultados_agregado),
    )

    # Fallback: cria esforço a partir dos resultados quando a combinação
    # campanha+ponto+metodo existir na planilha e não existir no banco.
    chaves_resultados = set(
        zip(
            df_resultados_agregado["Campanha"],
            df_resultados_agregado["Ponto"],
            df_resultados_agregado["Metodo_de_Captura"],
        )
    )
    chaves_faltantes = [k for k in chaves_resultados if k not in esforcos_db_map]
    esforcos_fallback = []
    for campanha, ponto, metodo in chaves_faltantes:
        id_ponto = pontos_db_map.get((campanha, ponto))
        if not id_ponto or not metodo:
            continue

        tipo_candidates = (
            df_resultados_agregado[
                (df_resultados_agregado["Campanha"] == campanha)
                & (df_resultados_agregado["Ponto"] == ponto)
                & (df_resultados_agregado["Metodo_de_Captura"] == metodo)
            ]["Tipo_de_Amostragem"]
            .dropna()
            .tolist()
        )
        tipo_fallback = normalizar_texto(tipo_candidates[0]) if tipo_candidates else None

        esforcos_fallback.append(
            {
                "id_ponto_coleta": id_ponto,
                "grupo_biologico": GRUPO_BIOLOGICO_ALVO,
                "metodo_de_captura": metodo,
                "esforco": None,
                "unidade_esforco": None,
                "tipo_amostragem": tipo_fallback,
            }
        )

    if esforcos_fallback:
        connection.execute(
            text(
                """
                INSERT INTO esforcos_amostragem (
                    id_ponto_coleta, grupo_biologico, metodo_de_captura,
                    esforco, unidade_esforco, tipo_amostragem
                )
                VALUES (
                    :id_ponto_coleta, :grupo_biologico, :metodo_de_captura,
                    :esforco, :unidade_esforco, :tipo_amostragem
                )
                ON CONFLICT (id_ponto_coleta, grupo_biologico, metodo_de_captura)
                DO UPDATE SET
                    tipo_amostragem = COALESCE(esforcos_amostragem.tipo_amostragem, EXCLUDED.tipo_amostragem)
                """
            ),
            esforcos_fallback,
        )
        logger.info(
            "Criados/atualizados %s esforço(s) via fallback de resultados.",
            len(esforcos_fallback),
        )

        # Recarrega o mapa de esforços após fallback.
        esforcos_db_map = (
            pd.read_sql(
                text(
                    """
                    SELECT
                        e.id_esforco,
                        c.nome_campanha,
                        p.nome_ponto,
                        e.metodo_de_captura
                    FROM esforcos_amostragem e
                    JOIN pontos_coleta p ON e.id_ponto_coleta = p.id_ponto_coleta
                    JOIN campanhas c ON p.id_campanha = c.id_campanha
                    WHERE p.id_projeto = :id_projeto
                      AND e.grupo_biologico = :grupo
                    """
                ),
                connection,
                params={
                    "id_projeto": id_projeto,
                    "grupo": GRUPO_BIOLOGICO_ALVO,
                },
            )
            .assign(
                nome_campanha=lambda df: df["nome_campanha"].apply(normalizar_chave),
                nome_ponto=lambda df: df["nome_ponto"].apply(normalizar_chave),
                metodo_de_captura=lambda df: df["metodo_de_captura"].apply(normalizar_chave),
            )
            .set_index(["nome_campanha", "nome_ponto", "metodo_de_captura"])["id_esforco"]
            .to_dict()
        )

    resultados_records = []
    warnings_especies = set()
    warnings_esforcos = 0
    warnings_detalhe = []

    for _, row in df_resultados_agregado.iterrows():
        campanha = normalizar_texto(row.get("Campanha"))
        ponto = normalizar_texto(row.get("Ponto"))
        metodo = normalizar_texto(row.get("Metodo_de_Captura"))
        nome_cientifico_clean = normalizar_texto(row.get("Nome_Cientifico"))

        if not campanha or not ponto or not metodo or not nome_cientifico_clean:
            warnings_esforcos += 1
            continue

        chave_esforco = (campanha, ponto, metodo)
        id_esforco = esforcos_db_map.get(chave_esforco)
        id_especie = especies_map.get(nome_cientifico_clean)

        if id_esforco and id_especie:
            resultados_records.append(
                {
                    "id_esforco": id_esforco,
                    "id_especie": id_especie,
                    "numero_de_individuos": (
                        int(row["Numero_de_Individuos"])
                        if pd.notna(row["Numero_de_Individuos"])
                        else None
                    ),
                    "tipo_amostragem": normalizar_texto(row.get("Tipo_de_Amostragem")),
                    "observacoes": None,
                }
            )
        elif not id_especie:
            warnings_especies.add(nome_cientifico_clean)
            warnings_detalhe.append(
                {
                    "campanha": campanha,
                    "ponto": ponto,
                    "metodo": metodo,
                    "especie": nome_cientifico_clean,
                    "motivo": "especie_nao_cadastrada",
                }
            )
        else:
            warnings_esforcos += 1
            warnings_detalhe.append(
                {
                    "campanha": campanha,
                    "ponto": ponto,
                    "metodo": metodo,
                    "especie": nome_cientifico_clean,
                    "motivo": "esforco_nao_mapeado",
                }
            )

    if resultados_records:
        query_resultados = text(f"""
            INSERT INTO {tabela_resultados} (
                id_esforco, id_especie, numero_de_individuos,
                tipo_amostragem, observacoes
            )
            VALUES (
                :id_esforco, :id_especie, :numero_de_individuos,
                :tipo_amostragem, :observacoes
            )
            ON CONFLICT (id_esforco, id_especie)
            DO UPDATE SET
                numero_de_individuos = EXCLUDED.numero_de_individuos,
                tipo_amostragem = EXCLUDED.tipo_amostragem
                -- observacoes preservadas intencionalmente (não sobrescrever dados manuais)
        """)
        connection.execute(query_resultados, resultados_records)

    logger.info(
        "Resultados mapeados: %s; descartados por mapeamento/campos: %s; táxons ausentes no cadastro: %s",
        len(resultados_records),
        warnings_esforcos,
        len(warnings_especies),
    )

    logger.info(f"{len(resultados_records)} registros de resultados inseridos/atualizados.")

    # Restaurar observações manuais preservadas antes da limpeza
    if obs_map:
        obs_updates = []
        for (campanha, ponto, metodo, id_especie), observacao in obs_map.items():
            id_esforco = esforcos_db_map.get((campanha, ponto, metodo))
            if id_esforco:
                obs_updates.append(
                    {
                        "id_esforco": id_esforco,
                        "id_especie": id_especie,
                        "observacoes": observacao,
                    }
                )
        if obs_updates:
            connection.execute(
                text(f"""
                    UPDATE {tabela_resultados}
                    SET observacoes = :observacoes
                    WHERE id_esforco = :id_esforco
                      AND id_especie = :id_especie
                """),
                obs_updates,
            )
            logger.info(
                f"Restauradas {len(obs_updates)} observação(ões) manual(is)."
            )

    if warnings_especies:
        logger.warning(
            f"Os seguintes tÃ¡xons nÃ£o foram encontrados no cadastro mestre: "
            f"{sorted(list(warnings_especies))}"
        )

    if warnings_esforcos:
        logger.warning(
            f"{warnings_esforcos} linha(s) de resultado nÃ£o puderam ser "
            "mapeadas para esforÃ§o/ponto/mÃ©todo."
        )
        for item in warnings_detalhe[:20]:
            logger.warning(
                "Nao mapeado -> campanha='%s' | ponto='%s' | metodo='%s' | especie='%s' | motivo='%s'",
                item["campanha"],
                item["ponto"],
                item["metodo"],
                item["especie"],
                item["motivo"],
            )


def main():
    if not os.path.exists(ARQUIVO_EXCEL):
        logger.error(f"Arquivo '{ARQUIVO_EXCEL}' nÃ£o foi encontrado.")
        sys.exit(1)

    engine = None

    try:
        logger.info(f"Iniciando migraÃ§Ã£o de {GRUPO_BIOLOGICO_ALVO}: {ARQUIVO_EXCEL}")
        logger.info(f"Tabela de resultados: {NOME_TABELA_RESULTADOS}")

        engine = get_engine()
        logger.info("ConexÃ£o com banco de dados estabelecida.")

        xls = pd.ExcelFile(ARQUIVO_EXCEL)
        validar_abas_excel(xls, ABAS_OBRIGATORIAS)

        df_capa = pd.read_excel(xls, "Capa_Projeto")
        df_pontos = pd.read_excel(xls, "Pontos_e_Campanhas").dropna(how="all")
        df_esforco = pd.read_excel(xls, "Metadados_Esforco").dropna(how="all")
        df_especies = (
            pd.read_excel(xls, "Especies").dropna(how="all")
            if "Especies" in xls.sheet_names
            else None
        )
        df_resultados = pd.read_excel(xls, NOME_ABA_RESULTADOS).dropna(how="all")

        logger.info(f"Excel carregado: {len(df_pontos)} pontos, {len(df_resultados)} resultados")

        species_report = validate_especies_file(ARQUIVO_EXCEL, engine)
        if not reportar_validacao("Validação de cadastro de espécies", species_report):
            sys.exit(1)

        if species_report.cleaned_df is not None:
            df_especies = species_report.cleaned_df

        allowed_species = set()
        if species_report.cleaned_df is not None and "Nome_Cientifico" in species_report.cleaned_df.columns:
            allowed_species = {
                str(value).strip().lower()
                for value in species_report.cleaned_df["Nome_Cientifico"].dropna().tolist()
                if str(value).strip()
            }

        import_report = validate_importacao_file(
            ARQUIVO_EXCEL,
            GRUPO_BIOLOGICO_ALVO,
            engine,
            allowed_species=allowed_species,
            strict_unknown_species=True,
        )
        if not reportar_validacao("Validação de resultados", import_report):
            sys.exit(1)

        if "Data" in df_pontos.columns:
            df_pontos["Data"] = df_pontos["Data"].apply(normalizar_data)

        with engine.begin() as connection:
            dados_projeto = df_capa.iloc[0].to_dict()

            connection.execute(
                text(
                    """
                    INSERT INTO clientes (nome_empresa, cnpj)
                    VALUES (:Cliente, :CNPJ)
                    ON CONFLICT (cnpj) DO NOTHING
                    """
                ),
                dados_projeto,
            )

            id_cliente = connection.execute(
                text("SELECT id_cliente FROM clientes WHERE cnpj = :CNPJ"),
                dados_projeto,
            ).scalar_one()

            params_projeto = {**dados_projeto, "id_cliente": id_cliente}

            connection.execute(
                text(
                    """
                    INSERT INTO projetos (id_cliente, nome_projeto, codigo_interno_opyta)
                    VALUES (:id_cliente, :Nome_do_Projeto, :Codigo_Opyta)
                    ON CONFLICT (codigo_interno_opyta) DO NOTHING
                    """
                ),
                params_projeto,
            )

            id_projeto = connection.execute(
                text(
                    "SELECT id_projeto FROM projetos WHERE codigo_interno_opyta = :Codigo_Opyta"
                ),
                params_projeto,
            ).scalar_one()

            obs_map = obter_observacoes_existentes(connection, id_projeto, NOME_TABELA_RESULTADOS)
            limpar_dados_da_campanha(connection, id_projeto, df_pontos, NOME_TABELA_RESULTADOS)
            migrar_dados(
                connection,
                df_capa,
                df_pontos,
                df_esforco,
                df_resultados,
                NOME_TABELA_RESULTADOS,
                obs_map=obs_map,
                df_especies=df_especies,
            )

            logger.info(f"âœ“ MIGRAÃ‡ÃƒO DE {GRUPO_BIOLOGICO_ALVO.upper()} CONCLUÃDA COM SUCESSO")

    except Exception as e:
        logger.error("ERRO DURANTE A MIGRAÃ‡ÃƒO. A TRANSAÃ‡ÃƒO FOI REVERTIDA (ROLLBACK)")
        logger.error(f"Detalhes: {str(e)}")
        logger.error("Stack trace:")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if engine is not None:
            engine.dispose()
            logger.info("Recursos de conexÃ£o liberados.")


if __name__ == "__main__":
    main()
