# --- cadastrar_parametros.py (V2.0 - Engine Central Padronizado) ---

from __future__ import annotations

import sys
from pathlib import Path
import os

import pandas as pd
from sqlalchemy import text

# Garante que a pasta raiz do projeto esteja no path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.engine import get_engine


def limpar(v):
    if pd.isna(v) or str(v).strip() in ["-", "N.A.", "", "None", "Ausente"]:
        return None

    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return None


def main():
    arquivo = "cadastro_parametros_opyta.xlsx"
    abas = {
        "Aguas_Superficiais": "Água Superficial",
        "Aguas_Subterraneas": "Água Subterrânea",
        "Sedimento": "Sedimento",
        "Efluentes": "Efluente",
    }

    if not os.path.exists(arquivo):
        sys.exit(f"Erro: O arquivo '{arquivo}' não foi encontrado.")

    engine = None

    try:
        engine = get_engine()
        print(f"--- INICIANDO CADASTRO DE PARÂMETROS: {arquivo} ---")

        for nome_aba, matriz_db in abas.items():
            try:
                df = pd.read_excel(arquivo, sheet_name=nome_aba).dropna(subset=["Parametro"])
                df.columns = [c.lower().strip() for c in df.columns]

                records = []
                for _, row in df.iterrows():
                    records.append(
                        {
                            "n": str(row["parametro"]).strip(),
                            "m": matriz_db,
                            "u": None if pd.isna(row.get("unidade_medida")) else str(row.get("unidade_medida")).strip(),
                            "c1min": limpar(row.get("vmp_357_cl1_min")),
                            "c1max": limpar(row.get("vmp_357_cl1_max")),
                            "c2min": limpar(row.get("vmp_357_cl2_min")),
                            "c2max": limpar(row.get("vmp_357_cl2_max")),
                            "ch": limpar(row.get("vmp_396_consumo_humano")),
                            "an": limpar(row.get("vmp_396_dessedentacao_animal")),
                            "ir": limpar(row.get("vmp_396_irrigacao")),
                            "re": limpar(row.get("vmp_396_recreacao")),
                            "n1": limpar(row.get("vmp_454_n1")),
                            "n2": limpar(row.get("vmp_454_n2")),
                            "p430": limpar(row.get("vmp_430_padrao")),
                        }
                    )

                if not records:
                    print(f"⚠️ {matriz_db}: nenhuma linha válida para processar.")
                    continue

                query = text(
                    """
                    INSERT INTO public.parametros_analise (
                        nome_parametro,
                        matriz,
                        unidade_medida,
                        vmp_357_cl1_min,
                        vmp_357_cl1_max,
                        vmp_357_cl2_min,
                        vmp_357_cl2_max,
                        vmp_396_consumo_humano,
                        vmp_396_dessedentacao_animal,
                        vmp_396_irrigacao,
                        vmp_396_recreacao,
                        vmp_454_n1,
                        vmp_454_n2,
                        vmp_430_padrao
                    )
                    VALUES (
                        :n, :m, :u, :c1min, :c1max, :c2min, :c2max,
                        :ch, :an, :ir, :re, :n1, :n2, :p430
                    )
                    ON CONFLICT (nome_parametro, matriz)
                    DO UPDATE SET
                        unidade_medida = EXCLUDED.unidade_medida,
                        vmp_357_cl1_min = EXCLUDED.vmp_357_cl1_min,
                        vmp_357_cl1_max = EXCLUDED.vmp_357_cl1_max,
                        vmp_357_cl2_min = EXCLUDED.vmp_357_cl2_min,
                        vmp_357_cl2_max = EXCLUDED.vmp_357_cl2_max,
                        vmp_396_consumo_humano = EXCLUDED.vmp_396_consumo_humano,
                        vmp_396_dessedentacao_animal = EXCLUDED.vmp_396_dessedentacao_animal,
                        vmp_396_irrigacao = EXCLUDED.vmp_396_irrigacao,
                        vmp_396_recreacao = EXCLUDED.vmp_396_recreacao,
                        vmp_454_n1 = EXCLUDED.vmp_454_n1,
                        vmp_454_n2 = EXCLUDED.vmp_454_n2,
                        vmp_430_padrao = EXCLUDED.vmp_430_padrao
                    """
                )

                with engine.begin() as conn:
                    conn.execute(query, records)

                print(f"✅ {matriz_db} OK. {len(records)} registros processados.")

            except Exception as e:
                print(f"❌ Erro na aba '{nome_aba}': {e}")

        print("\n--- CADASTRO DE PARÂMETROS CONCLUÍDO ---")

    except Exception as e:
        print("\n--- ERRO GERAL NO CADASTRO DE PARÂMETROS ---")
        print(e)
        sys.exit(1)
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()