# --- SCRIPT DE CADASTRO MESTRE (VERSÃO PADRONIZADA COM ENGINE CENTRAL) ---

from __future__ import annotations

import sys
from pathlib import Path
import os
import re
import unicodedata

import numpy as np
import pandas as pd
from sqlalchemy import text

# Garante que a pasta raiz do projeto esteja no path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.engine import get_engine
from validators.especies.pipeline import validate_especies_file

# --- CONFIGURAÇÕES ---
ARQUIVO_EXCEL_ESPECIES = sys.argv[1] if len(sys.argv) > 1 else "cadastro_especies_opyta.xlsx"


def log_progress(percent: int, etapa: str):
    """Imprime marcador de progresso para consumo pela UI."""
    p = max(0, min(100, int(percent)))
    print(f"[{p}%] {etapa}")


def explain_error(exc: Exception) -> tuple[str, str]:
    """Retorna causa provável e ação sugerida para erros comuns."""
    msg = str(exc)

    bind_match = re.search(r"bind parameter '([^']+)'", msg)
    if bind_match:
        col = bind_match.group(1)
        return (
            f"Coluna ausente na aba 'Especies' ou nome de coluna divergente: {col}.",
            "Revise os cabeçalhos da planilha e confirme que os nomes estão no padrão esperado. "
            "Se a coluna for opcional, verifique se o script em deploy está atualizado.",
        )

    if "undefinedcolumn" in msg.lower() or (
        "column" in msg.lower() and "does not exist" in msg.lower()
    ):
        return (
            "A estrutura da tabela 'especies' no banco está desatualizada em relação ao script.",
            "Aplique a migration 002 ou mantenha o script em modo compatível com colunas existentes.",
        )

    if "relation" in msg.lower() and "does not exist" in msg.lower():
        return (
            "Tabela/visão não encontrada no banco.",
            "Verifique se as migrations necessárias foram aplicadas no ambiente alvo.",
        )

    if "permission denied" in msg.lower():
        return (
            "Permissão insuficiente no banco para executar INSERT/UPDATE.",
            "Valide as credenciais e permissões da role usada pela aplicação.",
        )

    return (
        "Falha durante leitura da planilha ou gravação no banco.",
        "Confira o log completo e valide formato das abas/colunas e conectividade com banco.",
    )


def _report_validation(report, title: str) -> bool:
    print(f"\n-> {title}")
    if report.blocks:
        print(f"  Bloqueios: {len(report.blocks)}")
        for issue in report.blocks[:10]:
            print(f"  - [{issue.code}] {issue.message}")
        return False

    if report.warnings:
        print(f"  Avisos: {len(report.warnings)}")
        for issue in report.warnings[:5]:
            print(f"  - [{issue.code}] {issue.message}")
    else:
        print("  Sem bloqueios.")

    return True


def _to_bool_or_none(value):
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    txt = unicodedata.normalize("NFKD", str(value).strip().lower())
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"\s+", " ", txt)
    if txt in {"sim", "s", "yes", "y", "true", "1", "x"}:
        return True
    if txt in {"nao", "n", "no", "false", "0", ""}:
        return False
    return None


def cadastrar_dicionarios(connection, df_bacias, df_biomas):
    """Insere bacias e biomas em lote."""
    print("\n-> Cadastrando Bacias Hidrográficas e Biomas...")

    if df_bacias is not None and not df_bacias.empty:
        bacias_records = [
            {"nome": str(nome).strip()}
            for nome in df_bacias["Nome_Bacia"].dropna().unique()
        ]
        if bacias_records:
            query_bacias = text(
                """
                INSERT INTO bacias_hidrograficas (nome_bacia)
                VALUES (:nome)
                ON CONFLICT (nome_bacia) DO NOTHING
                """
            )
            connection.execute(query_bacias, bacias_records)
            print(f"  {len(bacias_records)} registros de bacias processados.")

    if df_biomas is not None and not df_biomas.empty:
        biomas_records = [
            {"nome": str(nome).strip()}
            for nome in df_biomas["Nome_Bioma"].dropna().unique()
        ]
        if biomas_records:
            query_biomas = text(
                """
                INSERT INTO biomas (nome_bioma)
                VALUES (:nome)
                ON CONFLICT (nome_bioma) DO NOTHING
                """
            )
            connection.execute(query_biomas, biomas_records)
            print(f"  {len(biomas_records)} registros de biomas processados.")

    print("Dicionários de Bacias e Biomas atualizados.")


def cadastrar_especies_principal(connection, df_especies):
    """Insere e atualiza espécies em lote."""
    print("\n-> Cadastrando/Atualizando informações principais das espécies...")

    if df_especies.empty:
        print("  Aviso: Nenhuma espécie para processar na planilha.")
        return

    # Colunas obrigatórias (já existiam na tabela)
    _rename_base = {
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
        # Aliases usados em planilhas de cadastro geral.
        "Status_IUCN": "status_ameaca_global",
        "Status_MMA": "status_ameaca_nacional",
        "Status_Ameaca_Nacional": "status_ameaca_nacional",
        "Status_Ameaca_Global": "status_ameaca_global",
        "Origem": "origem",
        "Habito_Alimentar": "habito_alimentar",
        "Estrategia_Reprodutiva": "estrategia_reprodutiva",
        "Valor_Economico": "valor_economico",
        "Cinegetica": "cinegetica",
        "Cinegéticas": "cinegetica",
        "Cinegeticas": "cinegetica",
        "Xerimbabo": "xerimbabo",
        "Xerimbabos": "xerimbabo",
        "Observacoes": "observacoes",
        "BMWP_Score": "bmwp_score",
    }

    # Colunas opcionais para fauna terrestre (migration 002)
    # Aceita tanto "Status_Estadual" quanto "Status_Ameaca_Estadual" (alias do usuário)
    _rename_terrestre = {
        "Status_Estadual": "status_estadual",
        "Status_Ameaca_Estadual": "status_estadual",
        "Status_Copam": "status_copam",
        "Status_COPAM": "status_copam",
        "Cites": "cites",
        "CITES": "cites",
        "Guilda_Alimentar": "guilda_alimentar",
        "Dependencia_Florestal": "dependencia_florestal",
        "Endemismo": "endemismo",
        "Sensibilidade_Ambiental": "sensibilidade_ambiental",
        "Migratorio": "migratorio",
        "Raridade": "raridade",
    }

    df_renamed = df_especies.rename(columns={**_rename_base, **_rename_terrestre})

    # Garante retrocompatibilidade: qualquer coluna ausente vira NULL.
    expected_db_cols = [
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
        "origem",
        "habito_alimentar",
        "estrategia_reprodutiva",
        "valor_economico",
        "cinegetica",
        "xerimbabo",
        "observacoes",
        "bmwp_score",
        "status_estadual",
        "status_copam",
        "cites",
        "guilda_alimentar",
        "dependencia_florestal",
        "endemismo",
        "sensibilidade_ambiental",
        "migratorio",
        "raridade",
    ]

    for col in expected_db_cols:
        if col not in df_renamed.columns:
            df_renamed[col] = None

    # Remove colunas extras da planilha para evitar binds inesperados.
    df_renamed = df_renamed[expected_db_cols]

    df_renamed = df_renamed.mask(df_renamed.isin(["N.A.", "n.a.", "NA"]), np.nan)
    for bool_col in ["cinegetica", "xerimbabo"]:
        if bool_col in df_renamed.columns:
            df_renamed[bool_col] = df_renamed[bool_col].map(_to_bool_or_none)

    records_to_insert = df_renamed.to_dict("records")

    for record in records_to_insert:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, str):
                record[key] = value.strip()

    # Remove identificacoes genericas (sp./spp.) que violam a constraint do banco.
    def _is_generic_name(name: str | None) -> bool:
        if not name:
            return False
        txt = str(name).strip().lower()
        return (
            " spp." in txt
            or txt.endswith(" spp")
            or " sp." in txt
            or txt.endswith(" sp")
        )

    total_before_filter = len(records_to_insert)
    records_to_insert = [
        rec for rec in records_to_insert if not _is_generic_name(rec.get("nome_cientifico"))
    ]
    skipped_generic = total_before_filter - len(records_to_insert)
    if skipped_generic > 0:
        print(
            "  Aviso: "
            f"{skipped_generic} registro(s) com nome cientifico generico (sp./spp.) foram ignorados."
        )

    if records_to_insert:
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

        available_cols = [c for c in expected_db_cols if c in cols_db_set]
        missing_cols = [c for c in expected_db_cols if c not in cols_db_set]

        if "nome_cientifico" not in available_cols:
            raise RuntimeError(
                "A tabela 'especies' não possui a coluna obrigatória 'nome_cientifico'."
            )

        if missing_cols:
            print(
                "  Aviso: colunas não encontradas na tabela 'especies' "
                f"(serão ignoradas neste ambiente): {', '.join(missing_cols)}"
            )

        rows = [{k: rec.get(k) for k in available_cols} for rec in records_to_insert]

        insert_cols_sql = ",\n                ".join(available_cols)
        values_cols_sql = ", ".join(f":{c}" for c in available_cols)
        update_cols = [c for c in available_cols if c != "nome_cientifico"]

        if update_cols:
            update_sql = ",\n                ".join(
                f"{c} = EXCLUDED.{c}" for c in update_cols
            )
            query_sql = f"""
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
        else:
            query_sql = f"""
            INSERT INTO especies (
                {insert_cols_sql}
            )
            VALUES (
                {values_cols_sql}
            )
            ON CONFLICT (nome_cientifico) DO NOTHING
            """

        connection.execute(text(query_sql), rows)

    print(f"Tabela de Espécies principal atualizada. {len(records_to_insert)} registros processados.")


def cadastrar_endemismo(connection, df_endemismo):
    if df_endemismo is None or df_endemismo.empty:
        print("\n-> Aba de Endemismo não encontrada ou vazia. Pulando esta etapa.")
        return

    print("\n-> Cadastrando informações de Endemismo...")

    bacias_map = (
        pd.read_sql(
            text("SELECT id_bacia, nome_bacia FROM bacias_hidrograficas"),
            connection,
        )
        .set_index("nome_bacia")["id_bacia"]
        .to_dict()
    )

    biomas_map = (
        pd.read_sql(
            text("SELECT id_bioma, nome_bioma FROM biomas"),
            connection,
        )
        .set_index("nome_bioma")["id_bioma"]
        .to_dict()
    )

    especies_map = (
        pd.read_sql(
            text("SELECT id_especie, nome_cientifico FROM especies"),
            connection,
        )
        .set_index("nome_cientifico")["id_especie"]
        .to_dict()
    )

    records_to_insert = []
    warnings = 0

    for _, linha in df_endemismo.iterrows():
        nome_cientifico = str(linha["Nome_Cientifico"]).strip()
        tipo_regiao = str(linha["Tipo_de_Regiao"]).strip()
        nome_regiao = str(linha["Nome_da_Regiao"]).strip()

        id_especie = especies_map.get(nome_cientifico)
        id_bacia, id_bioma = None, None

        if tipo_regiao == "Bacia Hidrográfica":
            id_bacia = bacias_map.get(nome_regiao)
        elif tipo_regiao == "Bioma":
            id_bioma = biomas_map.get(nome_regiao)

        if id_especie and (id_bacia or id_bioma):
            records_to_insert.append(
                {
                    "id_sp": id_especie,
                    "id_ba": id_bacia,
                    "id_bi": id_bioma,
                }
            )
        else:
            warnings += 1

    if records_to_insert:
        query = text(
            """
            INSERT INTO endemismo_especies (id_especie, id_bacia, id_bioma)
            VALUES (:id_sp, :id_ba, :id_bi)
            ON CONFLICT DO NOTHING
            """
        )
        connection.execute(query, records_to_insert)
        print(f"Relações de Endemismo atualizadas. {len(records_to_insert)} registros processados.")

    if warnings > 0:
        print(f"  Aviso: {warnings} linha(s) da aba de endemismo não puderam ser mapeadas.")


def main():
    if not os.path.exists(ARQUIVO_EXCEL_ESPECIES):
        sys.exit(f"Erro: O arquivo '{ARQUIVO_EXCEL_ESPECIES}' não foi encontrado.")

    engine = None

    try:
        log_progress(5, "Iniciando cadastro mestre")
        engine = get_engine()
        print(f"--- INICIANDO CADASTRO MESTRE: {ARQUIVO_EXCEL_ESPECIES} ---")

        log_progress(15, "Lendo arquivo Excel")
        xls = pd.ExcelFile(ARQUIVO_EXCEL_ESPECIES)

        log_progress(25, "Validando aba Especies")
        species_report = validate_especies_file(ARQUIVO_EXCEL_ESPECIES, engine)
        if not _report_validation(species_report, "Validação da aba Especies"):
            sys.exit(1)

        df_especies = species_report.cleaned_df if species_report.cleaned_df is not None else pd.read_excel(xls, "Especies").dropna(how="all")

        df_bacias = pd.read_excel(xls, "Bacias_Hidrograficas").dropna(how="all") if "Bacias_Hidrograficas" in xls.sheet_names else None
        df_biomas = pd.read_excel(xls, "Biomas").dropna(how="all") if "Biomas" in xls.sheet_names else None
        _aba_endemismo = next((n for n in xls.sheet_names if n.lower().startswith("endemismo")), None)
        df_endemismo = pd.read_excel(xls, _aba_endemismo).dropna(how="all") if _aba_endemismo else None

        with engine.begin() as connection:
            log_progress(35, "Cadastrando dicionários (bacias/biomas)")
            cadastrar_dicionarios(connection, df_bacias, df_biomas)
            log_progress(65, "Cadastrando espécies")
            cadastrar_especies_principal(connection, df_especies)
            log_progress(85, "Cadastrando endemismo")
            cadastrar_endemismo(connection, df_endemismo)

        log_progress(100, "Cadastro mestre concluído")
        print("\n--- CADASTRO MESTRE CONCLUÍDO COM SUCESSO ---")

    except Exception as e:
        print("\n--- ERRO DURANTE O CADASTRO MESTRE ---")
        print(e)
        causa, acao = explain_error(e)
        print(f"CAUSA_PROVAVEL: {causa}")
        print(f"ACAO_SUGERIDA: {acao}")
        sys.exit(1)
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()
