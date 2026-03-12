# --- SCRIPT DE CADASTRO MESTRE (VERSÃO DE TESTE COM CREDENCIAIS FIXAS) ---

import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
import numpy as np

# --- CONFIGURAÇÕES ---
ARQUIVO_EXCEL_ESPECIES = 'cadastro_especies_opyta.xlsx'

# --- FUNÇÕES DE CADASTRO ---
# (As funções cadastrar_dicionarios, cadastrar_especies_principal, cadastrar_endemismo permanecem as mesmas)
def cadastrar_dicionarios(connection, df_bacias, df_biomas):
    """Insere bacias e biomas em lote."""
    print("\n-> Cadastrando Bacias Hidrográficas e Biomas...")
    if df_bacias is not None and not df_bacias.empty:
        bacias_records = [{"nome": str(nome).strip()} for nome in df_bacias['Nome_Bacia'].dropna().unique()]
        if bacias_records:
            query_bacias = text("INSERT INTO bacias_hidrograficas (nome_bacia) VALUES (:nome) ON CONFLICT (nome_bacia) DO NOTHING")
            connection.execute(query_bacias, bacias_records)
            print(f"  {len(bacias_records)} registros de bacias processados.")
    if df_biomas is not None and not df_biomas.empty:
        biomas_records = [{"nome": str(nome).strip()} for nome in df_biomas['Nome_Bioma'].dropna().unique()]
        if biomas_records:
            query_biomas = text("INSERT INTO biomas (nome_bioma) VALUES (:nome) ON CONFLICT (nome_bioma) DO NOTHING")
            connection.execute(query_biomas, biomas_records)
            print(f"  {len(biomas_records)} registros de biomas processados.")
    print("Dicionários de Bacias e Biomas atualizados.")

def cadastrar_especies_principal(connection, df_especies):
    """Insere e atualiza espécies em lote."""
    print("\n-> Cadastrando/Atualizando informações principais das espécies...")
    if df_especies.empty:
        print("  Aviso: Nenhuma espécie para processar na planilha.")
        return
    df_renamed = df_especies.rename(columns={'Nome_Cientifico': 'nome_cientifico', 'Nome_Popular': 'nome_popular', 'Grupo_Biologico': 'grupo_biologico','Reino': 'reino', 'Filo': 'filo', 'Classe': 'classe', 'Ordem': 'ordem', 'Familia': 'familia', 'Genero': 'genero','Autor_e_Ano': 'autor_e_ano', 'Status_Ameaca_Nacional': 'status_ameaca_nacional','Status_Ameaca_Global': 'status_ameaca_global', 'Origem': 'origem', 'Habito_Alimentar': 'habito_alimentar','Estrategia_Reprodutiva': 'estrategia_reprodutiva', 'Valor_Economico': 'valor_economico','Observacoes': 'observacoes', 'BMWP_Score': 'bmwp_score'})
    df_renamed.replace(['N.A.', 'n.a.', 'NA'], np.nan, inplace=True)
    records_to_insert = df_renamed.to_dict('records')
    for record in records_to_insert:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, str):
                record[key] = value.strip()
    if records_to_insert:
        query = text("""INSERT INTO especies (nome_cientifico, nome_popular, grupo_biologico, reino, filo, classe, ordem, familia, genero, autor_e_ano, status_ameaca_nacional, status_ameaca_global, origem, habito_alimentar, estrategia_reprodutiva, valor_economico, observacoes, bmwp_score) VALUES (:nome_cientifico, :nome_popular, :grupo_biologico, :reino, :filo, :classe, :ordem, :familia, :genero, :autor_e_ano, :status_ameaca_nacional, :status_ameaca_global, :origem, :habito_alimentar, :estrategia_reprodutiva, :valor_economico, :observacoes, :bmwp_score) ON CONFLICT (nome_cientifico) DO UPDATE SET nome_popular = EXCLUDED.nome_popular, grupo_biologico = EXCLUDED.grupo_biologico, reino = EXCLUDED.reino, filo = EXCLUDED.filo, classe = EXCLUDED.classe, ordem = EXCLUDED.ordem, familia = EXCLUDED.familia, genero = EXCLUDED.genero, autor_e_ano = EXCLUDED.autor_e_ano, status_ameaca_nacional = EXCLUDED.status_ameaca_nacional, status_ameaca_global = EXCLUDED.status_ameaca_global, origem = EXCLUDED.origem, habito_alimentar = EXCLUDED.habito_alimentar, estrategia_reprodutiva = EXCLUDED.estrategia_reprodutiva, valor_economico = EXCLUDED.valor_economico, observacoes = EXCLUDED.observacoes, bmwp_score = EXCLUDED.bmwp_score;""")
        connection.execute(query, records_to_insert)
    num_records = len(records_to_insert)
    print(f"Tabela de Espécies principal atualizada. {num_records} registros processados.")

def cadastrar_endemismo(connection, df_endemismo):
    if df_endemismo is None or df_endemismo.empty:
        print("\n-> Aba de Endemismo não encontrada ou vazia. Pulando esta etapa.")
        return
    print("\n-> Cadastrando informações de Endemismo...")
    bacias_map = pd.read_sql("SELECT id_bacia, nome_bacia FROM bacias_hidrograficas", connection).set_index('nome_bacia')['id_bacia'].to_dict()
    biomas_map = pd.read_sql("SELECT id_bioma, nome_bioma FROM biomas", connection).set_index('nome_bioma')['id_bioma'].to_dict()
    especies_map = pd.read_sql("SELECT id_especie, nome_cientifico FROM especies", connection).set_index('nome_cientifico')['id_especie'].to_dict()
    records_to_insert, warnings = [], 0
    for _, linha in df_endemismo.iterrows():
        nome_cientifico, tipo_regiao, nome_regiao = str(linha['Nome_Cientifico']).strip(), str(linha['Tipo_de_Regiao']).strip(), str(linha['Nome_da_Regiao']).strip()
        id_especie = especies_map.get(nome_cientifico)
        id_bacia, id_bioma = None, None
        if tipo_regiao == 'Bacia Hidrográfica': id_bacia = bacias_map.get(nome_regiao)
        elif tipo_regiao == 'Bioma': id_bioma = biomas_map.get(nome_regiao)
        if id_especie and (id_bacia or id_bioma):
            records_to_insert.append({"id_sp": id_especie, "id_ba": id_bacia, "id_bi": id_bioma})
        else: warnings += 1
    if records_to_insert:
        query = text("INSERT INTO endemismo_especies (id_especie, id_bacia, id_bioma) VALUES (:id_sp, :id_ba, :id_bi) ON CONFLICT DO NOTHING")
        connection.execute(query, records_to_insert)
        print(f"Relações de Endemismo atualizadas. {len(records_to_insert)} registros processados.")
    if warnings > 0: print(f"  Aviso: {warnings} linha(s) da aba de endemismo não puderam ser mapeadas.")


def main():
    try:
        # --- BLOCO DE CONEXÃO MODIFICADO PARA TESTE ---
        db_user = "postgres.zmmylgtdorzdkdxpmnvj"
        db_password = "FTNblind19"
        db_host = "aws-1-sa-east-1.pooler.supabase.com"
        db_name = "postgres"
        db_port = "5432" # Porta da conexão direta

        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(db_url)
        print("--- TENTANDO CONECTAR COM CREDENCIAIS FIXAS NO CÓDIGO ---")
        # --- FIM DO BLOCO MODIFICADO ---
        
    except Exception as e: 
        sys.exit(f"Erro ao criar a engine de conexão: {e}")

    print(f"--- INICIANDO CADASTRO DE ESPÉCIES DO ARQUIVO: {ARQUIVO_EXCEL_ESPECIES} ---")

    try:
        xls = pd.ExcelFile(ARQUIVO_EXCEL_ESPECIES)
        df_especies = pd.read_excel(xls, sheet_name='Especies').dropna(subset=['Nome_Cientifico'])
        df_endemismo = pd.read_excel(xls, sheet_name='Endemismo_Especies') if 'Endemismo_Especies' in xls.sheet_names else pd.DataFrame()
        df_bacias = pd.read_excel(xls, sheet_name='Bacias_Hidrograficas') if 'Bacias_Hidrograficas' in xls.sheet_names else pd.DataFrame()
        df_biomas = pd.read_excel(xls, sheet_name='Biomas') if 'Biomas' in xls.sheet_names else pd.DataFrame()
        if not df_endemismo.empty: df_endemismo.dropna(subset=['Nome_Cientifico'], inplace=True)
        print("Abas da planilha mestre lidas com sucesso.")
    except Exception as e: 
        sys.exit(f"Erro ao ler as abas do Excel: {e}")

    try:
        with engine.begin() as connection:
            cadastrar_dicionarios(connection, df_bacias, df_biomas)
            cadastrar_especies_principal(connection, df_especies)
            cadastrar_endemismo(connection, df_endemismo)
            print("\n--- CADASTRO MESTRE DE ESPÉCIES CONCLUÍDO COM SUCESSO ---")
    except Exception as e:
        print(f"\n--- ERRO DURANTE O CADASTRO ---"); print(e); print("A transação foi revertida (rollback).")

    engine.dispose()

if __name__ == "__main__":
    main()