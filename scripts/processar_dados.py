# --- SCRIPT DE PROCESSAMENTO E CONSOLIDAÇÃO (V2.1 - Ajuste ao Nome da Coluna Existente) ---

import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

TABELA_ANALISE = 'biota_analise_consolidada'

def processar_e_consolidar(engine):
    print(f"\n-> Iniciando o processo de consolidação para a tabela '{TABELA_ANALISE}'...")
    
    # Query corrigida: busca 'tipo_amostragem' de CADA tabela de resultado
    query = text(f"""
    WITH todos_resultados AS (
        SELECT id_esforco, id_especie, numero_de_individuos AS contagem, ct_cm AS medida_1, pc_g AS medida_2, tipo_amostragem FROM resultados_ictiofauna UNION ALL
        SELECT id_esforco, id_especie, numero_de_individuos, NULL, NULL, tipo_amostragem FROM resultados_zooplancton UNION ALL
        SELECT id_esforco, id_especie, abundancia, NULL, NULL, tipo_amostragem FROM resultados_zoobentos UNION ALL
        SELECT id_esforco, id_especie, densidade_cel_ml, biovolume_mm3_L, NULL, tipo_amostragem FROM resultados_fitoplancton
    )
    SELECT
        cli.nome_empresa, proj.nome_projeto, proj.codigo_interno_opyta, camp.nome_campanha,
        pts.nome_ponto, pts.data_hora_coleta, pts.latitude, pts.longitude, pts.bacia_hidrografica,
        esf.grupo_biologico, esf.metodo_de_captura, esf.esforco, esf.unidade_esforco,
        res.tipo_amostragem, 
        sp.nome_cientifico, sp.nome_popular, sp.reino, sp.filo, sp.classe, sp.ordem, sp.familia, sp.genero, sp.origem, sp.bmwp_score,
        res.contagem, res.medida_1, res.medida_2 AS biomassa -- AGORA MEDIDA_2 É ALIAS PARA A COLUNA 'biomassa'
    FROM todos_resultados res
    JOIN esforcos_amostragem esf ON res.id_esforco = esf.id_esforco
    JOIN pontos_coleta pts ON esf.id_ponto_coleta = pts.id_ponto_coleta
    JOIN campanhas camp ON pts.id_campanha = camp.id_campanha
    JOIN projetos proj ON pts.id_projeto = proj.id_projeto
    JOIN clientes cli ON proj.id_cliente = cli.id_cliente
    JOIN especies sp ON res.id_especie = sp.id_especie;
    """)

    print("   -> Lendo e processando dados de todas as tabelas...")
    df_consolidado = pd.read_sql(query, engine)
    print(f"   -> {len(df_consolidado)} registros consolidados foram processados.")
    if df_consolidado.empty: print("   Aviso: Nenhum dado foi encontrado para consolidar."); return

    # Garante que a coluna 'tipo_amostragem' existe na tabela de destino
    # (Não é necessário criar a coluna 'biomassa' pois ela já existe, conforme informado)
    with engine.connect() as connection:
        connection.execute(text("ALTER TABLE biota_analise_consolidada ADD COLUMN IF NOT EXISTS tipo_amostragem VARCHAR(50);"))

    print(f"   -> Limpando a tabela de análise '{TABELA_ANALISE}' (TRUNCATE)...")
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {TABELA_ANALISE} RESTART IDENTITY;"))

    print(f"   -> Carregando os novos dados na tabela '{TABELA_ANALISE}'...")
    df_consolidado.to_sql(TABELA_ANALISE, engine, if_exists='append', index=False, chunksize=1000)
    print("   -> Carga de dados concluída.")

def main():
    try:
        db_user, db_password, db_host, db_name = os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'), os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"
        engine = create_engine(db_url)
    except Exception as e: sys.exit(f"Erro ao conectar ao banco de dados: {e}")
    try:
        processar_e_consolidar(engine)
        print(f"\n--- PROCESSO DE CONSOLIDAÇÃO CONCLUÍDO COM SUCESSO ---")
    except Exception as e: print(f"\n--- ERRO DURANTE O PROCESSAMENTO ---"); print(e)
    engine.dispose()

if __name__ == "__main__":
    main()