# --- SCRIPT DE PROCESSAMENTO E CONSOLIDAÇÃO (V2.2 - Conexão Centralizada) ---

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

# Garante que a pasta raiz do projeto esteja no path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.engine import get_engine

TABELA_ANALISE = "biota_analise_consolidada"


def processar_e_consolidar(engine):
    print(f"\n-> Iniciando o processo de consolidação para a tabela '{TABELA_ANALISE}'...")

    query = text("""
        WITH todos_resultados AS (
            SELECT
                id_esforco,
                id_especie,
                numero_de_individuos AS contagem,
                ct_cm AS medida_1,
                pc_g AS medida_2,
                tipo_amostragem
            FROM resultados_ictiofauna

            UNION ALL

            SELECT
                id_esforco,
                id_especie,
                numero_de_individuos AS contagem,
                NULL AS medida_1,
                NULL AS medida_2,
                tipo_amostragem
            FROM resultados_zooplancton

            UNION ALL

            SELECT
                id_esforco,
                id_especie,
                abundancia AS contagem,
                NULL AS medida_1,
                NULL AS medida_2,
                tipo_amostragem
            FROM resultados_zoobentos

            UNION ALL

            SELECT
                id_esforco,
                id_especie,
                densidade_cel_ml AS contagem,
                biovolume_mm3_L AS medida_1,
                NULL AS medida_2,
                tipo_amostragem
            FROM resultados_fitoplancton
        )
        SELECT
            cli.nome_empresa,
            proj.nome_projeto,
            proj.codigo_interno_opyta,
            camp.nome_campanha,
            pts.nome_ponto,
            pts.data_hora_coleta,
            pts.latitude,
            pts.longitude,
            pts.bacia_hidrografica,
            esf.grupo_biologico,
            esf.metodo_de_captura,
            esf.esforco,
            esf.unidade_esforco,
            res.tipo_amostragem,
            sp.nome_cientifico,
            sp.nome_popular,
            sp.reino,
            sp.filo,
            sp.classe,
            sp.ordem,
            sp.familia,
            sp.genero,
            sp.origem,
            sp.bmwp_score,
            res.contagem,
            res.medida_1,
            res.medida_2 AS biomassa
        FROM todos_resultados res
        JOIN esforcos_amostragem esf
            ON res.id_esforco = esf.id_esforco
        JOIN pontos_coleta pts
            ON esf.id_ponto_coleta = pts.id_ponto_coleta
        JOIN campanhas camp
            ON pts.id_campanha = camp.id_campanha
        JOIN projetos proj
            ON pts.id_projeto = proj.id_projeto
        JOIN clientes cli
            ON proj.id_cliente = cli.id_cliente
        JOIN especies sp
            ON res.id_especie = sp.id_especie;
    """)

    print("   -> Lendo e processando dados de todas as tabelas...")
    df_consolidado = pd.read_sql(query, engine)
    print(f"   -> {len(df_consolidado)} registros consolidados foram processados.")

    if df_consolidado.empty:
        print("   Aviso: Nenhum dado foi encontrado para consolidar.")
        return

    print("   -> Garantindo estrutura mínima da tabela de destino...")
    with engine.begin() as connection:
        connection.execute(
            text("""
                ALTER TABLE biota_analise_consolidada
                ADD COLUMN IF NOT EXISTS tipo_amostragem VARCHAR(50);
            """)
        )

    print(f"   -> Limpando a tabela de análise '{TABELA_ANALISE}' (TRUNCATE)...")
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {TABELA_ANALISE} RESTART IDENTITY;"))

    print(f"   -> Carregando os novos dados na tabela '{TABELA_ANALISE}'...")
    df_consolidado.to_sql(
        TABELA_ANALISE,
        engine,
        if_exists="append",
        index=False,
        chunksize=1000,
        method="multi"
    )
    print("   -> Carga de dados concluída.")


def main():
    engine = None

    try:
        engine = get_engine()
        processar_e_consolidar(engine)
        print("\n--- PROCESSO DE CONSOLIDAÇÃO CONCLUÍDO COM SUCESSO ---")
    except Exception as e:
        print("\n--- ERRO DURANTE O PROCESSAMENTO ---")
        print(e)
        sys.exit(1)
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()