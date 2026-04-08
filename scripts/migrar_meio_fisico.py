# --- migrar_meio_fisico.py (V7.5 - Conexão Centralizada com get_engine) ---

import sys
from pathlib import Path
import os
import re

import pandas as pd
from sqlalchemy import text
from dotenv import load_dotenv

# Garante que a pasta raiz do projeto esteja no path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.engine import get_engine


def extrair_sinal_e_valor(resultado_str):
    if pd.isna(resultado_str):
        return None, None

    s = str(resultado_str).replace(",", ".").strip()
    match = re.search(r"([<>])?\s*([0-9.]+)", s)

    if match:
        sinal = match.group(1) if match.group(1) else None
        try:
            return sinal, float(match.group(2))
        except Exception:
            return None, None

    return None, None


def norm_text(value):
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s else None


def main():
    load_dotenv()
    engine = None

    try:
        engine = get_engine()

        # PEGA O NOME DO ARQUIVO PASSADO NO TERMINAL OU USA O PADRÃO
        ARQUIVO_EXCEL = sys.argv[1] if len(sys.argv) > 1 else "Resultados_Meio_Fisico.xlsx"

        if not os.path.exists(ARQUIVO_EXCEL):
            sys.exit(f"❌ Erro: Arquivo {ARQUIVO_EXCEL} não encontrado.")

        print(f"--- INICIANDO MIGRAÇÃO: {ARQUIVO_EXCEL} ---")

        xls = pd.ExcelFile(ARQUIVO_EXCEL)
        df_capa = pd.read_excel(xls, "Capa_Projeto")
        df_pts_camp = pd.read_excel(xls, "Pontos_e_Campanhas").dropna(subset=["Ponto"])
        df_res = pd.read_excel(xls, "Resultados_Meio_Fisico").dropna(subset=["Ponto", "Parametro"])

        # Padronização de cabeçalhos e detecção de Unidade
        df_res.columns = [c.strip().lower() for c in df_res.columns]
        col_unid_list = [c for c in df_res.columns if "unid" in c]

        if not col_unid_list:
            sys.exit("❌ Coluna de Unidade não encontrada.")

        nome_col_unid = col_unid_list[0]

        codigo_opyta = str(df_capa.iloc[0]["Codigo_Opyta"]).strip()
        nome_projeto = str(df_capa.iloc[0]["Nome_do_Projeto"]).strip()
        nome_cliente = str(df_capa.iloc[0]["Cliente"]).strip()

        with engine.begin() as conn:
            # 1. SINCRONIZAR CLIENTE
            id_cliente = conn.execute(
                text("SELECT id_cliente FROM clientes WHERE nome_empresa = :n"),
                {"n": nome_cliente},
            ).scalar()

            if not id_cliente:
                id_cliente = conn.execute(
                    text("INSERT INTO clientes (nome_empresa) VALUES (:n) RETURNING id_cliente"),
                    {"n": nome_cliente},
                ).scalar()

            # 2. SINCRONIZAR PROJETO
            conn.execute(
                text("""
                    INSERT INTO projetos (id_cliente, nome_projeto, codigo_interno_opyta)
                    VALUES (:id_c, :nom, :cod)
                    ON CONFLICT (codigo_interno_opyta) DO NOTHING
                """),
                {"id_c": id_cliente, "nom": nome_projeto, "cod": codigo_opyta},
            )

            id_projeto = conn.execute(
                text("SELECT id_projeto FROM projetos WHERE codigo_interno_opyta = :c"),
                {"c": codigo_opyta},
            ).scalar()

            # 3. SINCRONIZAR CAMPANHAS
            for camp in df_pts_camp["Campanha"].unique():
                conn.execute(
                    text("""
                        INSERT INTO campanhas (nome_campanha)
                        VALUES (:n)
                        ON CONFLICT (nome_campanha) DO NOTHING
                    """),
                    {"n": str(camp).strip()},
                )

            camp_map = pd.read_sql(
                text("SELECT id_campanha, nome_campanha FROM campanhas"),
                conn,
            )
            camp_map = {
                norm_text(r["nome_campanha"]): r["id_campanha"]
                for _, r in camp_map.iterrows()
            }

            # 4. SINCRONIZAR PONTOS
            print("-> Sincronizando pontos...")
            for _, row in df_pts_camp.iterrows():
                id_c = camp_map.get(norm_text(row.get("Campanha")))
                data_c = pd.to_datetime(row["Data"], errors="coerce")

                conn.execute(
                    text("""
                        INSERT INTO pontos_coleta (
                            id_projeto,
                            id_campanha,
                            nome_ponto,
                            latitude,
                            longitude,
                            bacia_hidrografica,
                            data_hora_coleta
                        )
                        VALUES (:id_p, :id_c, :nom, :lat, :lon, :bac, :dat)
                        ON CONFLICT (id_projeto, id_campanha, nome_ponto)
                            WHERE id_empreendimento IS NULL
                        DO NOTHING
                    """),
                    {
                        "id_p": id_projeto,
                        "id_c": id_c,
                        "nom": norm_text(row.get("Ponto")),
                        "lat": row.get("Latitude"),
                        "lon": row.get("Longitude"),
                        "bac": row.get("Bacia_Hidrografica"),
                        "dat": None if pd.isna(data_c) else data_c,
                    },
                )

            # 5. MAPEAMENTOS PARA RESULTADOS
            param_dict = pd.read_sql(
                text("SELECT id_parametro, nome_parametro, matriz FROM parametros_analise"),
                conn,
            ).set_index(["nome_parametro", "matriz"])["id_parametro"].to_dict()

            pts_db_df = pd.read_sql(
                text("""
                    SELECT pc.id_ponto_coleta, pc.nome_ponto, ca.nome_campanha
                    FROM pontos_coleta pc
                    JOIN campanhas ca ON ca.id_campanha = pc.id_campanha
                    WHERE pc.id_projeto = :p
                """),
                conn,
                params={"p": id_projeto},
            )

            pts_db_map: dict[tuple[str, str], int] = {}
            pts_db_by_ponto: dict[str, list[int]] = {}
            for _, r in pts_db_df.iterrows():
                camp = norm_text(r.get("nome_campanha"))
                ponto = norm_text(r.get("nome_ponto"))
                if camp and ponto:
                    pts_db_map[(camp, ponto)] = int(r["id_ponto_coleta"])
                if ponto:
                    pts_db_by_ponto.setdefault(ponto, []).append(int(r["id_ponto_coleta"]))

            # 6. PROCESSAR E INSERIR RESULTADOS (UPSERT)
            print(f"-> Migrando {len(df_res)} resultados...")

            res_records = []
            warn_params = set()
            warn_points = set()
            warn_points_ambiguous = set()
            has_campanha_in_result = "campanha" in df_res.columns

            for _, row in df_res.iterrows():
                ponto = norm_text(row.get("ponto"))
                campanha = norm_text(row.get("campanha")) if has_campanha_in_result else None

                id_p = None
                if campanha and ponto:
                    id_p = pts_db_map.get((campanha, ponto))
                elif ponto:
                    candidates = pts_db_by_ponto.get(ponto, [])
                    if len(candidates) == 1:
                        id_p = candidates[0]
                    elif len(candidates) > 1:
                        warn_points_ambiguous.add(ponto)

                id_pr = param_dict.get((str(row["parametro"]).strip(), str(row["matriz"]).strip()))

                if id_p and id_pr:
                    sinal, valor = extrair_sinal_e_valor(row["resultado"])

                    if valor is not None:
                        res_records.append(
                            {
                                "id_p": id_p,
                                "id_pr": id_pr,
                                "val": valor,
                                "sin": sinal,
                                "mat": row["matriz"],
                                "unid": row[nome_col_unid],
                                "lab": row.get("laboratorio"),
                                "obs": row.get("observacoes"),
                            }
                        )
                else:
                    if not id_pr:
                        warn_params.add(f"{row['parametro']} ({row['matriz']})")
                    if not id_p and ponto:
                        if campanha:
                            warn_points.add(f"{campanha} / {ponto}")
                        elif ponto not in warn_points_ambiguous:
                            warn_points.add(ponto)

            if res_records:
                conn.execute(
                    text("""
                        INSERT INTO resultados_analise (
                            id_ponto_coleta,
                            id_parametro,
                            valor_medido,
                            sinal_limite,
                            matriz,
                            unidade_medida,
                            laboratorio_responsavel,
                            observacoes_resultado
                        )
                        VALUES (:id_p, :id_pr, :val, :sin, :mat, :unid, :lab, :obs)
                        ON CONFLICT (id_ponto_coleta, id_parametro, matriz)
                        DO UPDATE SET
                            valor_medido = EXCLUDED.valor_medido,
                            sinal_limite = EXCLUDED.sinal_limite,
                            unidade_medida = EXCLUDED.unidade_medida
                    """),
                    res_records,
                )
                print(f"✅ SUCESSO! {len(res_records)} resultados processados.")

            if warn_params:
                print(f"⚠️ Parâmetros não cadastrados no mestre: {list(warn_params)}")

            if warn_points_ambiguous:
                print(
                    "⚠️ Pontos ambíguos na aba de resultados (mesmo nome em múltiplas campanhas). "
                    f"Inclua a coluna 'Campanha' em Resultados_Meio_Fisico: {sorted(list(warn_points_ambiguous))}"
                )

            if warn_points:
                print(f"⚠️ Pontos não mapeados para id_ponto_coleta: {sorted(list(warn_points))}")

    except Exception as e:
        print("\n--- ERRO DURANTE A MIGRAÇÃO DO MEIO FÍSICO ---")
        print(e)
        sys.exit(1)
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()