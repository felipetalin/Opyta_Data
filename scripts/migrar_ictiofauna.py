# --- SCRIPT DE MIGRAÇÃO - ICTIOFAUNA (V6.0 - Limpeza Segura por Grupo) ---

import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from dotenv import load_dotenv

# --- CONFIGURAÇÕES ---
ARQUIVO_EXCEL = sys.argv[1] if len(sys.argv) > 1 else 'projeto_ictio_real.xlsx'
GRUPO_BIOLOGICO_ALVO = 'Ictiofauna'
NOME_TABELA_RESULTADOS = 'resultados_ictiofauna'
NOME_ABA_RESULTADOS = 'Resultados_Ictiofauna'

def limpar_dados_da_campanha(connection, id_projeto, df_pontos_da_planilha):
    """
    Limpa APENAS os dados de Ictiofauna das campanhas presentes na planilha.
    NÃO apaga pontos de coleta, pois eles podem ser usados por Bentos, Fito, etc.
    """
    nomes_campanhas_na_planilha = df_pontos_da_planilha['Campanha'].unique().tolist()
    if not nomes_campanhas_na_planilha:
        print("   -> Nenhuma campanha encontrada na planilha para limpar."); return
    
    print(f"\n-> Verificando e limpando dados ANTIGOS de {GRUPO_BIOLOGICO_ALVO} para as campanhas: {nomes_campanhas_na_planilha}...")

    query_ids_campanha = text("SELECT id_campanha FROM public.campanhas WHERE nome_campanha = ANY(:nomes)")
    ids_campanha_para_limpar = connection.execute(query_ids_campanha, {'nomes': nomes_campanhas_na_planilha}).scalars().all()
    
    if not ids_campanha_para_limpar:
        print("   -> Campanhas são novas no banco, nenhuma limpeza necessária."); return

    params = {"id_proj": id_projeto, "ids_camp": tuple(ids_campanha_para_limpar), "grupo": GRUPO_BIOLOGICO_ALVO}
    
    # --- ORDEM DE DELEÇÃO SEGURA ---
    
    # 1. Apaga APENAS os resultados da tabela de ICTIOFAUNA ligados a essas campanhas
    # Não toca nas tabelas de Bentos, Zoo ou Fito.
    print(f"   -> Removendo resultados antigos de {NOME_TABELA_RESULTADOS}...")
    query_delete_res = text(f"""
        DELETE FROM {NOME_TABELA_RESULTADOS} 
        WHERE id_esforco IN (
            SELECT id_esforco FROM esforcos_amostragem 
            WHERE grupo_biologico = :grupo -- Garante que só apaga esforços de Ictio
            AND id_ponto_coleta IN (
                SELECT id_ponto_coleta FROM pontos_coleta 
                WHERE id_projeto = :id_proj AND id_campanha IN :ids_camp
            )
        )
    """)
    connection.execute(query_delete_res, params)
    
    # 2. Apaga APENAS os esforços de ICTIOFAUNA
    # Mantém esforços de Bentos, Fito, etc. intactos.
    print(f"   -> Removendo esforços antigos de {GRUPO_BIOLOGICO_ALVO}...")
    query_delete_esforcos = text("""
        DELETE FROM esforcos_amostragem 
        WHERE grupo_biologico = :grupo -- O pulo do gato: Filtra pelo grupo
        AND id_ponto_coleta IN (
            SELECT id_ponto_coleta FROM pontos_coleta 
            WHERE id_projeto = :id_proj AND id_campanha IN :ids_camp
        )
    """)
    connection.execute(query_delete_esforcos, params)

    # 3. PONTOS DE COLETA: NÃO APAGAR!
    # Motivo: Pontos são compartilhados. Se apagarmos o Ponto 1 aqui, 
    # os dados de Bentos do Ponto 1 ficariam órfãos e seriam corrompidos.
    print("   -> Pontos de coleta preservados (infraestrutura compartilhada).")
    print(f"   -> Limpeza de {GRUPO_BIOLOGICO_ALVO} concluída com segurança.")


def obter_mapas_de_ids(connection):
    print("\n-> Mapeando IDs existentes do banco de dados...")
    especies_map = pd.read_sql("SELECT id_especie, nome_cientifico FROM especies", connection).set_index('nome_cientifico')['id_especie'].to_dict()
    campanhas_map = pd.read_sql("SELECT id_campanha, nome_campanha FROM campanhas", connection).set_index('nome_campanha')['id_campanha'].to_dict()
    return especies_map, campanhas_map

def migrar_dados(connection, df_capa, df_pontos, df_esforco, df_resultados):
    especies_map, campanhas_map_inicial = obter_mapas_de_ids(connection)
    
    print("\n-> Processando Campanhas e Pontos de Coleta...")
    novas_campanhas = [{'nome': c} for c in df_pontos['Campanha'].unique() if c not in campanhas_map_inicial]
    if novas_campanhas:
        connection.execute(text("INSERT INTO campanhas (nome_campanha) VALUES (:nome) ON CONFLICT (nome_campanha) DO NOTHING"), novas_campanhas)
        campanhas_map_atualizado = pd.read_sql("SELECT id_campanha, nome_campanha FROM campanhas", connection).set_index('nome_campanha')['id_campanha'].to_dict()
    else: campanhas_map_atualizado = campanhas_map_inicial
    
    codigo_opyta = df_capa.iloc[0]['Codigo_Opyta']
    id_projeto = connection.execute(text("SELECT id_projeto FROM projetos WHERE codigo_interno_opyta = :codigo"), {"codigo": codigo_opyta}).scalar_one()

    pontos_records = []
    for _, row in df_pontos.iterrows(): 
        # Tratamento para NaT (Data Vazia)
        data_coleta = row['Data']
        if pd.isna(data_coleta):
            data_coleta = None
            
        pontos_records.append({
            "id_projeto": id_projeto, 
            "id_campanha": campanhas_map_atualizado.get(row['Campanha']), 
            "nome_ponto": row['Ponto'], 
            "data_hora_coleta": data_coleta, 
            "latitude": row.get('Latitude'), 
            "longitude": row.get('Longitude'), 
            "bacia_hidrografica": row.get('Bacia_Hidrografica')
        })
        
    if pontos_records:
        # ON CONFLICT DO NOTHING garante que não duplicamos, mas também não apagamos os existentes
        query_pontos = text("INSERT INTO pontos_coleta (id_projeto, id_campanha, nome_ponto, data_hora_coleta, latitude, longitude, bacia_hidrografica) VALUES (:id_projeto, :id_campanha, :nome_ponto, :data_hora_coleta, :latitude, :longitude, :bacia_hidrografica) ON CONFLICT (id_projeto, id_campanha, nome_ponto) DO NOTHING")
        connection.execute(query_pontos, pontos_records)
    print("   Pontos de coleta inseridos/verificados.")

    print("\n-> Processando Esforços de Amostragem...")
    pontos_db_map = pd.read_sql(f"SELECT pc.id_ponto_coleta, ca.nome_campanha, pc.nome_ponto FROM pontos_coleta pc JOIN campanhas ca ON pc.id_campanha = ca.id_campanha WHERE pc.id_projeto = {id_projeto}", connection).set_index(['nome_campanha', 'nome_ponto'])['id_ponto_coleta'].to_dict()
    
    esforcos_records = []
    df_esforco_filtrado = df_esforco[df_esforco['Grupo_Biologico'] == GRUPO_BIOLOGICO_ALVO]
    
    for _, row in df_esforco_filtrado.iterrows():
        chave_ponto = (row['Campanha'], row['Ponto'])
        id_ponto = pontos_db_map.get(chave_ponto)
        if id_ponto: 
            esforcos_records.append({
                "id_ponto_coleta": id_ponto, 
                "grupo_biologico": GRUPO_BIOLOGICO_ALVO, 
                "metodo_de_captura": row['Metodo_de_Captura'], 
                "esforco": row.get('Esforco'), 
                "unidade_esforco": row.get('Unidade_Esforco'), 
                "tipo_amostragem": row.get('Tipo_de_Amostragem')
            })
            
    if esforcos_records:
        query_esforcos = text("INSERT INTO esforcos_amostragem (id_ponto_coleta, grupo_biologico, metodo_de_captura, esforco, unidade_esforco, tipo_amostragem) VALUES (:id_ponto_coleta, :grupo_biologico, :metodo_de_captura, :esforco, :unidade_esforco, :tipo_amostragem) ON CONFLICT (id_ponto_coleta, grupo_biologico, metodo_de_captura) DO UPDATE SET esforco = EXCLUDED.esforco, unidade_esforco = EXCLUDED.unidade_esforco, tipo_amostragem = EXCLUDED.tipo_amostragem")
        connection.execute(query_esforcos, esforcos_records)
    print(f"   {len(esforcos_records)} esforços de {GRUPO_BIOLOGICO_ALVO} inseridos/verificados.")
    
    print(f"\n-> Processando e agregando Resultados de {GRUPO_BIOLOGICO_ALVO}...")
    esforcos_db_map = pd.read_sql(f"SELECT e.id_esforco, c.nome_campanha, p.nome_ponto, e.metodo_de_captura FROM esforcos_amostragem e JOIN pontos_coleta p ON e.id_ponto_coleta = p.id_ponto_coleta JOIN campanhas c ON p.id_campanha = c.id_campanha WHERE p.id_projeto = {id_projeto} AND e.grupo_biologico = '{GRUPO_BIOLOGICO_ALVO}'", connection).set_index(['nome_campanha', 'nome_ponto', 'metodo_de_captura'])['id_esforco'].to_dict()
    
    df_resultados_agregado = df_resultados.groupby(['Campanha', 'Ponto', 'Metodo_de_Captura', 'Nome_Cientifico', 'Tipo_de_Amostragem']).agg(Numero_de_Individuos=('Numero_de_Individuos', 'sum'), CT_cm=('CT_cm', 'mean'), PC_g=('PC_g', 'mean')).reset_index()

    resultados_records, warnings_especies = [], set()
    for _, row in df_resultados_agregado.iterrows():
        chave_esforco = (row['Campanha'], row['Ponto'], row['Metodo_de_Captura'])
        id_esforco = esforcos_db_map.get(chave_esforco)
        nome_cientifico_clean = str(row['Nome_Cientifico']).strip()
        id_especie = especies_map.get(nome_cientifico_clean)
        
        if id_esforco and id_especie:
            resultados_records.append({
                "id_esforco": id_esforco, 
                "id_especie": id_especie, 
                "numero_de_individuos": row.get('Numero_de_Individuos'), 
                "ct_cm": row.get('CT_cm'), 
                "pc_g": row.get('PC_g'), 
                "tipo_amostragem": row.get('Tipo_de_Amostragem')
            })
        elif not id_especie: 
            warnings_especies.add(nome_cientifico_clean)
    
    if resultados_records:
        query_resultados = text(f"INSERT INTO {NOME_TABELA_RESULTADOS} (id_esforco, id_especie, numero_de_individuos, ct_cm, pc_g, tipo_amostragem) VALUES (:id_esforco, :id_especie, :numero_de_individuos, :ct_cm, :pc_g, :tipo_amostragem) ON CONFLICT (id_esforco, id_especie) DO UPDATE SET numero_de_individuos = EXCLUDED.numero_de_individuos, ct_cm = EXCLUDED.ct_cm, pc_g = EXCLUDED.pc_g, tipo_amostragem = EXCLUDED.tipo_amostragem")
        connection.execute(query_resultados, resultados_records)
    print(f"   {len(resultados_records)} registros de resultados inseridos/atualizados.")
    if warnings_especies: print(f"   Aviso: Os seguintes táxons não foram encontrados no cadastro mestre: {list(warnings_especies)}")

def main():
    if not os.path.exists(ARQUIVO_EXCEL): sys.exit(f"Erro: O arquivo '{ARQUIVO_EXCEL}' não foi encontrado.")
    try:
        load_dotenv()
        db_user, db_password, db_host, db_name = os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'), os.getenv('DB_NAME')
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}"
        engine = create_engine(db_url)
    except Exception as e: sys.exit(f"Erro ao conectar ao banco de dados: {e}")
    
    print(f"--- INICIANDO MIGRAÇÃO DE {GRUPO_BIOLOGICO_ALVO.upper()}: {ARQUIVO_EXCEL} ---")
    try:
        xls = pd.ExcelFile(ARQUIVO_EXCEL)
        df_capa = pd.read_excel(xls, 'Capa_Projeto'); df_pontos = pd.read_excel(xls, 'Pontos_e_Campanhas').dropna(how='all'); df_esforco = pd.read_excel(xls, 'Metadados_Esforco').dropna(how='all'); df_resultados = pd.read_excel(xls, NOME_ABA_RESULTADOS).dropna(how='all')
        
        df_pontos['Data'] = pd.to_datetime(df_pontos['Data'], dayfirst=True, errors='coerce').dt.tz_localize(None)
        df_pontos['Data'] = df_pontos['Data'].astype(object).where(df_pontos['Data'].notnull(), None)

        with engine.begin() as connection:
            dados_projeto = df_capa.iloc[0].to_dict()
            connection.execute(text("INSERT INTO clientes (nome_empresa, cnpj) VALUES (:Cliente, :CNPJ) ON CONFLICT (cnpj) DO NOTHING"), dados_projeto)
            id_cliente = connection.execute(text("SELECT id_cliente FROM clientes WHERE cnpj = :CNPJ"), dados_projeto).scalar_one()
            params_projeto = {**dados_projeto, 'id_cliente': id_cliente}; connection.execute(text("INSERT INTO projetos (id_cliente, nome_projeto, codigo_interno_opyta) VALUES (:id_cliente, :Nome_do_Projeto, :Codigo_Opyta) ON CONFLICT (codigo_interno_opyta) DO NOTHING"), params_projeto)
            id_projeto = connection.execute(text("SELECT id_projeto FROM projetos WHERE codigo_interno_opyta = :Codigo_Opyta"), params_projeto).scalar_one()
            
            # CHAMA A LIMPEZA SEGURA
            limpar_dados_da_campanha(connection, id_projeto, df_pontos)
            
            migrar_dados(connection, df_capa, df_pontos, df_esforco, df_resultados)
            print(f"\n--- MIGRAÇÃO DE {GRUPO_BIOLOGICO_ALVO.upper()} CONCLUÍDA COM SUCESSO ---")
    except Exception as e:
        print(f"\n--- ERRO DURANTE A MIGRAÇÃO. A TRANSAÇÃO FOI REVERTIDA (ROLLBACK) ---")
        print(f"   Detalhe do erro: {e}"); raise

if __name__ == "__main__":
    main()