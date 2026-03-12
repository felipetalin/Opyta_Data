#!/usr/bin/env python
# coding: utf-8

# In[1]:


# CÉLULA 1: INSTALAÇÃO E IMPORTAÇÕES 

# Já instalado: # !pip install xlsxwriter

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import re
from sqlalchemy import create_engine, text

# Configurações globais de estilo
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_theme(style="whitegrid")

print("✅ Célula 1 finalizada: Bibliotecas carregadas.")


# In[2]:


# CÉLULA 2: CONFIGURAÇÃO E CONEXÃO

# --- CREDENCIAIS ---
DB_URL = "postgresql://postgres.zmmylgtdorzdkdxpmnvj:FTNblind19@aws-1-sa-east-1.pooler.supabase.com:5432/postgres"
engine = create_engine(DB_URL)

# --- CONFIGURAÇÕES DE PROJETO E CAMINHO MESTRE ---
CODIGO_PROJETO = 'FERSAM001' 
CAMINHO_RAIZ = r'G:\Meu Drive\Opyta\Clientes\Clientes\Clientes\Ferreira Rocha\SAM Metais\Produtos\Planilhas e análises\Meio Fisico'

if not os.path.exists(CAMINHO_RAIZ):
    os.makedirs(CAMINHO_RAIZ)

print(f"✅ Célula 2: Conectado ao projeto {CODIGO_PROJETO}.")
print(f"📁 Pasta Raiz: {CAMINHO_RAIZ}")


# In[3]:


# CÉLULA 3: DEFINIÇÃO DAS FUNÇÕES (V8.0 - VERSÃO INTEGRADA)

def limpar_pasta(caminho):
    """Remove arquivos antigos para evitar acúmulo de versões."""
    if os.path.exists(caminho):
        for arquivo in os.listdir(caminho):
            if arquivo.endswith(".png") or arquivo.endswith(".xlsx"):
                try: os.remove(os.path.join(caminho, arquivo))
                except: pass

def obter_dados_projeto(engine, codigo_projeto):
    """Busca dados e ordena campanhas cronologicamente."""
    query = text("SELECT * FROM fisico_analise_consolidada WHERE codigo_interno_opyta = :cod")
    df = pd.read_sql(query, engine, params={"cod": codigo_projeto})
    meses_map = {'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4, 'maio': 5, 'junho': 6,
                 'julho': 7, 'agosto': 8, 'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12}
    def sort_key(camp):
        try:
            p = str(camp).lower().split('-')
            return int(p[1]) * 100 + meses_map.get(p[0], 0)
        except: return 0
    df['ordem_cron'] = df['nome_campanha'].apply(sort_key)
    return df.sort_values(['nome_ponto', 'ordem_cron'])

def gerar_tabela_conformidade(df_matriz, matriz_alvo, caminho_pasta):
    """RESULTADO 01: Super Tabela de Conformidade com Cores."""
    if 'Superficial' in matriz_alvo:
        cols_vmp = ['vmp_357_cl1_min', 'vmp_357_cl1_max', 'vmp_357_cl2_min', 'vmp_357_cl2_max', 'vmp_amonia_dinamico']
    elif 'Sedimento' in matriz_alvo:
        cols_vmp = ['vmp_454_n1', 'vmp_454_n2']
    elif 'Subterrânea' in matriz_alvo:
        cols_vmp = ['vmp_396_consumo_humano', 'vmp_396_dessedentacao_animal', 'vmp_396_irrigacao', 'vmp_396_recreacao']
    else: cols_vmp = ['vmp_430_padrao']

    nome_sub = matriz_alvo.replace(" ", "_")
    path_final = os.path.join(caminho_pasta, f"01_Conformidade_{nome_sub}.xlsx")
    
    with pd.ExcelWriter(path_final, engine='xlsxwriter') as writer:
        for camp in df_matriz['nome_campanha'].unique():
            df_c = df_matriz[df_matriz['nome_campanha'] == camp]
            tabela_pivot = df_c.pivot_table(index=['nome_parametro', 'unidade_medida'], columns='nome_ponto', values='valor_medido', aggfunc='first').reset_index()
            df_vmp_map = df_c[['nome_parametro', 'unidade_medida'] + cols_vmp].drop_duplicates()
            tabela_final = pd.merge(df_vmp_map, tabela_pivot, on=['nome_parametro', 'unidade_medida'], how='left')

            sheet_name = re.sub(r'[\\/*?:"<>|]', "", str(camp))[:31]
            tabela_final.to_excel(writer, sheet_name=sheet_name, index=False)
            
            workbook, worksheet = writer.book, writer.sheets[sheet_name]
            fmt_vermelho = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            inicio_dados = 2 + len(cols_vmp)

            for i in range(len(tabela_final)):
                for col_idx in range(inicio_dados, len(tabela_final.columns)):
                    val = tabela_final.iloc[i, col_idx]
                    if pd.isna(val): continue
                    violou = False
                    if 'Superficial' in matriz_alvo:
                        v_min, v_max, v_amo = tabela_final.iloc[i, 4], tabela_final.iloc[i, 5], tabela_final.iloc[i, 6]
                        if (pd.notna(v_max) and val > v_max) or (pd.notna(v_min) and val < v_min) or (pd.notna(v_amo) and val > v_amo): violou = True
                    elif 'Sedimento' in matriz_alvo:
                        v_n1 = tabela_final.iloc[i, 2]
                        if pd.notna(v_n1) and val > v_n1: violou = True
                    elif 'Subterrânea' in matriz_alvo:
                        v_ch = tabela_final.iloc[i, 2]
                        if pd.notna(v_ch) and val > v_ch: violou = True
                    if violou: worksheet.write(i + 1, col_idx, val, fmt_vermelho)

def gerar_grafico_percentual_violacao(df_matriz, matriz_alvo, caminho_pasta):
    """RESULTADO 02: Gráfico de Barras com % de Violação."""
    df = df_matriz.copy()
    if 'Superficial' in matriz_alvo:
        df['violou'] = ((pd.notna(df['vmp_357_cl2_max']) & (df['valor_medido'] > df['vmp_357_cl2_max'])) |
                        (pd.notna(df['vmp_357_cl2_min']) & (df['valor_medido'] < df['vmp_357_cl2_min'])) |
                        (pd.notna(df['vmp_amonia_dinamico']) & (df['valor_medido'] > df['vmp_amonia_dinamico'])))
    elif 'Sedimento' in matriz_alvo:
        df['violou'] = (pd.notna(df['vmp_454_n1']) & (df['valor_medido'] > df['vmp_454_n1']))
    elif 'Subterrânea' in matriz_alvo:
        df['violou'] = (pd.notna(df['vmp_396_consumo_humano']) & (df['valor_medido'] > df['vmp_396_consumo_humano']))
    else: df['violou'] = False

    resumo = df.groupby(['nome_parametro', 'nome_campanha'])['violou'].agg(['sum', 'count']).reset_index()
    resumo['percentual'] = (resumo['sum'] / resumo['count']) * 100
    resumo = resumo[resumo['percentual'] > 0]
    if resumo.empty: return

    plt.figure(figsize=(10, len(resumo['nome_parametro'].unique()) * 0.5 + 2))
    sns.barplot(data=resumo, y='nome_parametro', x='percentual', hue='nome_campanha', palette='Reds_r')
    plt.title(f"Distribuição de Violação - {matriz_alvo}", fontsize=14, fontweight='bold')
    plt.xlabel("Percentual de Pontos com Violação (%)")
    plt.xlim(0, 110)
    plt.tight_layout()
    plt.savefig(os.path.join(caminho_pasta, f"02_Percentual_Violacao.png"), dpi=300)
    plt.close()

def gerar_grafico_final(df_param, nome_parametro, caminho_pasta):
    """RESULTADO 03: Gráficos individuais PNG."""
    matriz = df_param['matriz'].iloc[0]
    unidade = df_param['unidade_medida'].iloc[0] if pd.notna(df_param['unidade_medida'].iloc[0]) else "unid."
    fig, ax = plt.subplots(figsize=(15, 8))
    sns.scatterplot(data=df_param, x='nome_ponto', y='valor_medido', hue='nome_campanha', style='nome_campanha', s=160, ax=ax, zorder=10)
    ymin, ymax = ax.get_ylim()
    x_lims = [-0.5, len(df_param['nome_ponto'].unique()) - 0.5]

    if 'Superficial' in matriz:
        vmax = df_param['vmp_amonia_dinamico'].iloc[0] if pd.notna(df_param['vmp_amonia_dinamico'].iloc[0]) else df_param['vmp_357_cl2_max'].iloc[0]
        vmin = df_param['vmp_357_cl2_min'].iloc[0]
        if pd.notna(vmax):
            ax.axhline(vmax, color='red', linestyle='-', label=f'VMP Cl2 ({vmax})')
            if df_param['valor_medido'].max() > vmax: ax.fill_between(x_lims, vmax, ymax*1.2, color='red', alpha=0.07)
        if pd.notna(vmin):
            ax.axhline(vmin, color='red', linestyle='-', label=f'Mínimo Cl2 ({vmin})')
            if df_param['valor_medido'].min() < vmin: ax.fill_between(x_lims, ymin*0.8, vmin, color='red', alpha=0.07)
    elif 'Sedimento' in matriz:
        n1, n2 = df_param['vmp_454_n1'].iloc[0], df_param['vmp_454_n2'].iloc[0]
        if pd.notna(n1): ax.axhline(n1, color='orange', linestyle='--', label=f'Nível 1 ({n1})')
        if pd.notna(n2): ax.axhline(n2, color='red', linestyle='-', label=f'Nível 2 ({n2})')
    elif 'Subterrânea' in matriz:
        usos = {'vmp_396_consumo_humano': ('red', 'Consumo'), 'vmp_396_dessedentacao_animal': ('brown', 'Dessedentação'), 'vmp_396_irrigacao': ('green', 'Irrigação'), 'vmp_396_recreacao': ('blue', 'Recreação')}
        for col, (cor, lab) in usos.items():
            val = df_param[col].iloc[0]
            if pd.notna(val): ax.axhline(val, color=cor, linestyle='--', label=f'{lab}: {val}')

    ax.set_title(f"{nome_parametro} ({matriz})", fontsize=16, fontweight='bold')
    ax.set_ylabel(f"{nome_parametro} ({unidade})")
    ax.legend(title='Campanhas e VMPs', bbox_to_anchor=(1.02, 1), loc='upper left')
    plt.xticks(rotation=45); plt.tight_layout()
    nome_seguro = re.sub(r'[\\/*?:"<>|]', "", nome_parametro).replace(" ", "_")
    plt.savefig(os.path.join(caminho_pasta, f"{nome_seguro}.png"), dpi=300)
    plt.close()

print("✅ Célula 3 finalizada: Todas as funções integradas com sucesso!")


# In[4]:


# CÉLULA 4: EXECUÇÃO DA ANÁLISE (VERSÃO FINAL V6.0)

# 1. Carregar dados consolidados do Supabase
df_total = obter_dados_projeto(engine, CODIGO_PROJETO)
matrizes = df_total['matriz'].unique()

print(f"🚀 Iniciando Processamento Completo para o projeto {CODIGO_PROJETO}...")

for matriz in matrizes:
    # 2. Configuração de Pastas
    nome_sub = matriz.replace(" ", "_")
    caminho_matriz = os.path.join(CAMINHO_RAIZ, nome_sub)
    
    if not os.path.exists(caminho_matriz):
        os.makedirs(caminho_matriz)
    
    print(f"\n--- 📁 PROCESSANDO MATRIZ: {matriz} ---")
    
    # 3. Limpeza da pasta (evita arquivos duplicados ou antigos)
    limpar_pasta(caminho_matriz)
    
    # Filtra os dados apenas desta matriz
    df_m = df_total[df_total['matriz'] == matriz]
    
    # --- RESULTADO 01: TABELA DE CONFORMIDADE (EXCEL) ---
    # Agora gera com todos os parâmetros e colunas de VMP completas
    gerar_tabela_conformidade(df_m, matriz, caminho_matriz)
    
    # --- RESULTADO 02: GRÁFICO DE PERCENTUAL DE VIOLAÇÃO (BARRAS) ---
    # Agora com lógica dual (Água) e conservadora (Sedimento N1)
    gerar_grafico_percentual_violacao(df_m, matriz, caminho_matriz)
    
    # --- RESULTADO 03: GRÁFICOS DE TENDÊNCIA ESPACIAL (PNG) ---
    # Gera um gráfico individual para cada parâmetro da matriz
    lista_params = df_m['nome_parametro'].unique()
    for p in lista_params:
        df_p = df_m[df_m['nome_parametro'] == p]
        gerar_grafico_final(df_p, p, caminho_matriz)
    
    print(f"   📈 {len(lista_params)} parâmetros processados com sucesso.")

print(f"\n✨ PROCESSO FINALIZADO! Todos os resultados estão em: {CAMINHO_RAIZ}")


# In[5]:


# CÉLULA: RESULTADO 01 - TABELA DE CONFORMIDADE DEFINITIVA (VERSÃO FINAL)

def gerar_super_tabela_conformidade(df_total, matriz_alvo, caminho_raiz):
    print(f"🚀 Gerando Tabela de Conformidade Final: {matriz_alvo}")
    
    # 1. Filtra dados da matriz
    df_m = df_total[df_total['matriz'] == matriz_alvo].copy()
    
    # 2. Define colunas de VMP para exibir na tabela (de acordo com a matriz)
    if 'Superficial' in matriz_alvo:
        cols_vmp = ['vmp_357_cl1_min', 'vmp_357_cl1_max', 'vmp_357_cl2_min', 'vmp_357_cl2_max', 'vmp_amonia_dinamico']
    elif 'Sedimento' in matriz_alvo:
        cols_vmp = ['vmp_454_n1', 'vmp_454_n2']
    elif 'Subterrânea' in matriz_alvo:
        cols_vmp = ['vmp_396_consumo_humano', 'vmp_396_dessedentacao_animal', 'vmp_396_irrigacao', 'vmp_396_recreacao']
    else:
        cols_vmp = ['vmp_430_padrao']

    # 3. Organiza Pasta
    nome_sub = matriz_alvo.replace(" ", "_")
    caminho_pasta = os.path.join(caminho_raiz, nome_sub)
    if not os.path.exists(caminho_pasta): os.makedirs(caminho_pasta)

    # 4. Cria arquivo Excel único para a matriz
    path_final = os.path.join(caminho_pasta, f"01_Tabela_Conformidade_{nome_sub}.xlsx")
    
    with pd.ExcelWriter(path_final, engine='xlsxwriter') as writer:
        # Itera por campanha criando uma ABA para cada
        for camp in df_m['nome_campanha'].unique():
            df_c = df_m[df_m['nome_campanha'] == camp]
            
            # Pivotagem: Parâmetro e Unidade no Index, Pontos nas Colunas
            tabela_pivot = df_c.pivot_table(
                index=['nome_parametro', 'unidade_medida'],
                columns='nome_ponto',
                values='valor_medido',
                aggfunc='first'
            ).reset_index()

            # Mapeamento de VMPs (Garante que todos os limites apareçam na linha do parâmetro)
            # Removemos duplicatas para o merge não explodir linhas
            df_vmp_map = df_c[['nome_parametro', 'unidade_medida'] + cols_vmp].drop_duplicates()
            
            # Une os VMPs com os resultados pivotados (Valor Real Medido)
            tabela_final = pd.merge(df_vmp_map, tabela_pivot, on=['nome_parametro', 'unidade_medida'], how='left')

            # Nome da aba (limite Excel 31 chars)
            sheet_name = re.sub(r'[\\/*?:"<>|]', "", str(camp))[:31]
            tabela_final.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # --- FORMATAÇÃO DE CORES ---
            workbook  = writer.book
            worksheet = writer.sheets[sheet_name]
            fmt_vermelho = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})
            fmt_header = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})

            # Ajuste de largura das colunas
            worksheet.set_column(0, 0, 35) # Nome Parâmetro
            worksheet.set_column(1, 1, 15) # Unidade

            num_vmp_cols = len(cols_vmp)
            inicio_dados = 2 + num_vmp_cols # Primeira coluna de Ponto

            # Varredura para pintar violações
            for i in range(len(tabela_final)):
                row_idx = i + 1 
                for col_idx in range(inicio_dados, len(tabela_final.columns)):
                    val = tabela_final.iloc[i, col_idx]
                    if pd.isna(val): continue
                    
                    violou = False
                    # Lógica Técnica de Violação (Opção A)
                    if 'Superficial' in matriz_alvo:
                        v_min = tabela_final.iloc[i, tabela_final.columns.get_loc('vmp_357_cl2_min')]
                        v_max = tabela_final.iloc[i, tabela_final.columns.get_loc('vmp_357_cl2_max')]
                        v_amo = tabela_final.iloc[i, tabela_final.columns.get_loc('vmp_amonia_dinamico')]
                        if (pd.notna(v_max) and val > v_max) or \
                           (pd.notna(v_min) and val < v_min) or \
                           (pd.notna(v_amo) and val > v_amo): violou = True
                    
                    elif 'Sedimento' in matriz_alvo:
                        v_n1 = tabela_final.iloc[i, tabela_final.columns.get_loc('vmp_454_n1')]
                        if pd.notna(v_n1) and val > v_n1: violou = True
                    
                    elif 'Subterrânea' in matriz_alvo:
                        v_ch = tabela_final.iloc[i, tabela_final.columns.get_loc('vmp_396_consumo_humano')]
                        if pd.notna(v_ch) and val > v_ch: violou = True
                    
                    if violou:
                        worksheet.write(row_idx, col_idx, val, fmt_vermelho)

    print(f"✅ Tabela concluída para {matriz_alvo} em: /{nome_sub}")

# --- EXECUÇÃO DO BLOCO ---
matrizes_existentes = df_total['matriz'].unique()
for m in matrizes_existentes:
    gerar_super_tabela_conformidade(df_total, m, CAMINHO_RAIZ)

print("\n✨ TODAS AS TABELAS DE CONFORMIDADE FORAM GERADAS!")


# In[6]:


# CÉLULA ÚNICA: CÁLCULO E VISUALIZAÇÃO DO IQA (IGAM) - COM LEGENDA
import math
import matplotlib.patheffects as path_effects
import matplotlib.patches as mpatches

def processar_iqa_completo(df_total, caminho_raiz):
    print("🚀 Iniciando Ciclo Completo de IQA...")
    
    def calcular_qi_igam(parametro, valor):
        try:
            v = float(valor)
            if v < 0: v = 0
            if 'pH' in parametro:
                if v <= 2: return 2.0
                if v <= 4: return 5.0 + (v-2)*11.0
                if v <= 5: return 27.0 + (v-4)*28.0
                if v <= 6: return 55.0 + (v-5)*25.0
                if v <= 7: return 80.0 + (v-6)*12.0
                if v <= 8: return 92.0
                if v <= 8.5: return 92.0 - (v-8)*4.0
                if v <= 9: return 90.0 - (v-8.5)*10.0
                if v <= 10: return 80.0 - (v-9)*35.0
                return 3.0
            elif 'Oxigênio Dissolvido' in parametro:
                sat = (v / 8.26) * 100 
                if sat <= 20: return 5.0 + sat*0.5
                if sat <= 50: return 15.0 + (sat-20)*0.63
                if sat <= 85: return 34.0 + (sat-50)*0.71
                if sat <= 100: return 59.0 + (sat-85)*2.73
                if sat <= 140: return 100.0 - (sat-100)*0.43
                return 83.0
            elif 'Coliformes' in parametro:
                if v <= 1: return 100.0
                log_v = math.log10(v)
                qi = 98.03 - 36.45*log_v + 3.138*(log_v**2) + 0.067*(log_v**3)
                return max(2.0, min(100.0, qi))
            elif 'Demanda Bioquímica' in parametro:
                if v <= 2: return 100.0 - v*10.0
                if v <= 5: return 80.0 - (v-2)*12.0
                if v <= 10: return 44.0 - (v-5)*6.0
                if v <= 20: return 14.0 - (v-10)*0.5
                return 2.0
            elif 'Nitrogênio Total' in parametro:
                if v <= 1: return 100.0 - v*10.0
                if v <= 5: return 90.0 - (v-1)*15.0
                if v <= 10: return 30.0 - (v-5)*4.0
                if v <= 100: return 10.0 - (v-10)*0.08
                return 1.0
            elif 'Fósforo Total' in parametro:
                if v <= 0.1: return 100.0 - v*400.0
                if v <= 0.5: return 60.0 - (v-0.1)*125.0
                if v <= 1.0: return 10.0 - (v-0.5)*10.0
                return 5.0
            elif 'Turbidez' in parametro:
                if v <= 5: return 100.0 - v*3.0
                if v <= 10: return 85.0 - (v-5)*2.0
                if v <= 40: return 75.0 - (v-10)*1.3
                if v <= 100: return 36.0 - (v-40)*0.48
                return 5.0
            elif 'Sólidos Totais' in parametro:
                if v <= 50: return 100.0 - v*0.4
                if v <= 100: return 80.0 - (v-50)*0.1
                if v <= 500: return 75.0 - (v-100)*0.11
                return 30.0
            return 100.0
        except: return 100.0

    df_agua = df_total[df_total['matriz'] == 'Água Superficial'].copy()
    df_pivot = df_agua.pivot_table(index=['nome_campanha', 'nome_ponto'], columns='nome_parametro', values='valor_medido').reset_index()

    pesos = {
        'Oxigênio Dissolvido In Situ': 0.17, 'pH In Situ': 0.12, 'Demanda Bioquímica de Oxigênio': 0.10,
        'Nitrogênio Total': 0.10, 'Fósforo Total': 0.10, 'Temperatura da Amostra': 0.10,
        'Turbidez': 0.08, 'Sólidos Totais': 0.08, 'Coliformes Termotolerantes por tubos múltiplos - NMP': 0.15
    }

    def calc_iqa(row):
        prod, w_soma = 1.0, 0
        for p, w in pesos.items():
            val = row.get(p)
            if pd.notna(val):
                prod *= (calcular_qi_igam(p, val) ** w)
                w_soma += w
        return round(prod ** (1/w_soma), 2) if w_soma > 0 else None

    df_pivot['IQA_Valor'] = df_pivot.apply(calc_iqa, axis=1)
    df_pivot['IQA_Classe'] = df_pivot['IQA_Valor'].apply(lambda v: 'Excelente' if v>=79 else 'Bom' if v>=51 else 'Médio' if v>=36 else 'Ruim' if v>=19 else 'Muito Ruim')

    faixas = [
        (0, 25, '#FF0000', 'Muito Ruim (<25)'), 
        (25, 50, '#FFC000', 'Ruim (26-50)'), 
        (50, 70, '#FFFF00', 'Médio (51-70)'), 
        (70, 90, '#00B050', 'Bom (71-90)'), 
        (90, 100, '#0070C0', 'Excelente (>91)')
    ]
    
    for camp in df_pivot['nome_campanha'].unique():
        df_c = df_pivot[df_pivot['nome_campanha'] == camp].dropna(subset=['IQA_Valor'])
        if df_c.empty: continue

        fig, ax = plt.subplots(figsize=(16, 9))
        legend_patches = []
        
        for low, high, color, lbl in faixas: 
            ax.axhspan(low, high, color=color, alpha=0.9, zorder=0)
            legend_patches.append(mpatches.Patch(color=color, label=lbl))
        
        x_idx = range(len(df_c))
        ax.scatter(x_idx, df_c['IQA_Valor'], color='black', s=80, zorder=5, label='IQA')
        legend_patches.append(plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='black', markersize=10, label='IQA'))

        for i, row in df_c.reset_index().iterrows():
            txt = ax.annotate(f"{row['IQA_Valor']}", (i, row['IQA_Valor']), xytext=(8, 8), textcoords='offset points', fontsize=11, fontweight='bold', zorder=10)
            txt.set_path_effects([path_effects.withStroke(linewidth=3, foreground='white')])

        ax.set_title(f"Índice de Qualidade de Água (IQA) - {camp}", fontsize=18, fontweight='bold', pad=20)
        ax.set_ylim(0, 105); ax.set_xticks(x_idx); ax.set_xticklabels(df_c['nome_ponto'], rotation=45, ha='right')
        
        # POSICIONA A LEGENDA EMBAIXO (como no seu modelo)
        ax.legend(handles=legend_patches, loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3, fontsize=11)
        
        nome_arq = camp.replace(" ", "_").replace("/", "-")
        plt.savefig(os.path.join(caminho_raiz, "Água_Superficial", f"04_Grafico_IQA_{nome_arq}.png"), dpi=300, bbox_inches='tight')
        plt.close()
        df_c[['nome_campanha', 'nome_ponto', 'IQA_Valor', 'IQA_Classe']].to_excel(os.path.join(caminho_raiz, "Água_Superficial", f"04_Resumo_IQA_{nome_arq}.xlsx"), index=False)

    print(f"✅ Sucesso: Resultados de IQA gerados na pasta /Água_Superficial")

processar_iqa_completo(df_total, CAMINHO_RAIZ)


# In[7]:


# CÉLULA ÚNICA: CÁLCULO E VISUALIZAÇÃO DO IET (LAMPARELLI 2004) - COM LEGENDA
import math
import matplotlib.patheffects as path_effects
import matplotlib.patches as mpatches

def processar_iet_completo(df_total, caminho_raiz):
    print("🚀 Iniciando Ciclo de IET...")
    
    df_agua = df_total[df_total['matriz'] == 'Água Superficial'].copy()
    df_pivot = df_agua.pivot_table(index=['nome_campanha', 'nome_ponto'], columns='nome_parametro', values='valor_medido').reset_index()

    col_pt, col_cl = 'Fósforo Total', 'Clorofila A'
    if col_pt not in df_pivot.columns or col_cl not in df_pivot.columns:
        print(f"❌ Erro: Faltam dados de PT ou Clorofila."); return

    def calc_iet(row):
        try:
            pt_ug, cl_ug = float(row[col_pt]) * 1000, float(row[col_cl]) * 1000
            iet_pt = 10 * (6 - ((1.77 - 0.42 * math.log(pt_ug)) / math.log(2)))
            iet_cl = 10 * (6 - ((-0.7 - 0.6 * math.log(cl_ug)) / math.log(2)))
            return round((iet_pt + iet_cl) / 2, 2)
        except: return None

    df_pivot['IET_Valor'] = df_pivot.apply(calc_iet, axis=1)
    df_pivot['IET_Classe'] = df_pivot['IET_Valor'].apply(lambda v: 'Ultraoligotrófico' if v<=47 else 'Oligotrófico' if v<=52 else 'Mesotrófico' if v<=59 else 'Eutrófico' if v<=63 else 'Supereutrófico' if v<=67 else 'Hipereutrófico')

    faixas = [
        (0, 47, '#0070C0', 'Ultraoligotrófico (<47)'), 
        (47, 52, '#D35400', 'Oligotrófico (47-52)'), 
        (52, 59, '#FFFF00', 'Mesotrófico (52-59)'), 
        (59, 63, '#4B0082', 'Eutrófico (59-63)'), 
        (63, 67, '#FF0000', 'Supereutrófico (63-67)'), 
        (67, 100, '#FFC0CB', 'Hipereutrófico (>67)')
    ]
    
    for camp in df_pivot['nome_campanha'].unique():
        df_c = df_pivot[df_pivot['nome_campanha'] == camp].dropna(subset=['IET_Valor'])
        if df_c.empty: continue

        fig, ax = plt.subplots(figsize=(16, 9))
        legend_patches = []
        for low, high, color, lbl in faixas:
            ax.axhspan(low, high, color=color, alpha=0.9, zorder=0)
            legend_patches.append(mpatches.Patch(color=color, label=lbl))
        
        x_idx = range(len(df_c))
        ax.scatter(x_idx, df_c['IET_Valor'], color='black', s=100, zorder=5)
        legend_patches.append(plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='black', markersize=10, label='IET'))

        for i, row in df_c.reset_index().iterrows():
            txt = ax.annotate(f"{row['IET_Valor']}", (i, row['IET_Valor']), xytext=(10, 10), textcoords='offset points', fontsize=11, fontweight='bold', zorder=10)
            txt.set_path_effects([path_effects.withStroke(linewidth=3, foreground='white')])

        ax.set_title(f"Índice de Estado Trófico (IET) - {camp}", fontsize=18, fontweight='bold', pad=20)
        ax.set_ylim(30, 90); ax.set_xticks(x_idx); ax.set_xticklabels(df_c['nome_ponto'], rotation=45, ha='right')
        
        # LEGENDA EMBAIXO
        ax.legend(handles=legend_patches, loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3, fontsize=11)
        
        nome_arq = camp.replace(" ", "_").replace("/", "-")
        plt.savefig(os.path.join(caminho_raiz, "Água_Superficial", f"05_Grafico_IET_{nome_arq}.png"), dpi=300, bbox_inches='tight')
        plt.close()
        df_c[['nome_campanha', 'nome_ponto', 'IET_Valor', 'IET_Classe']].to_excel(os.path.join(caminho_raiz, "Água_Superficial", f"05_Resumo_IET_{nome_arq}.xlsx"), index=False)

    print(f"✅ Sucesso: Resultados de IET gerados na pasta /Água_Superficial")

processar_iet_completo(df_total, CAMINHO_RAIZ)


# In[10]:


# CÉLULA ÚNICA: IQASB (METODOLOGIA RIGOROSA ABAS) - VERSÃO V2 (CORRIGIDA)
import math
import matplotlib.patheffects as path_effects
import matplotlib.patches as mpatches

def processar_iqasb_estrito(df_total, caminho_raiz):
    print("🔬 Calculando IQASB seguindo a Metodologia ABAS à risca...")
    
    # 1. Filtra dados da matriz subterrânea
    df_sub = df_total[df_total['matriz'] == 'Água Subterrânea'].copy()
    
    # Pivotagem com aggfunc='first' para aceitar textos (<) e números sem dar erro de média
    df_pivot = df_sub.pivot_table(
        index=['nome_campanha', 'nome_ponto'], 
        columns='nome_parametro', 
        values=['valor_medido', 'vmp_396_consumo_humano', 'sinal_limite'],
        aggfunc='first'
    )
    
    # Achata o multi-index das colunas (ex: ('valor_medido', 'Zinco') vira 'valor_medido_Zinco')
    df_pivot.columns = [f"{col[0]}_{col[1]}" for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()

    # 2. TABELA DE PESOS OFICIAIS (Wi) - METODOLOGIA ABAS
    pesos_wi = {
        'Arsênio Total': 5, 'Cádmio Total': 5, 'Chumbo Total': 5, 
        'Cromo Total': 5, 'Mercúrio Total': 5, 'Níquel Total': 5, 'Cianeto Total': 5,
        'Nitrato': 4, 'Bário Total': 4,
        'Ferro Total': 2, 'Manganês Total': 2, 'Alumínio Total': 2, 
        'Cobre Total': 2, 'Zinco Total': 2, 'Cloretos': 2, 'Sólidos Dissolvidos Totais': 2
    }

    def calc_estrito(row):
        soma_qi_wi, soma_wi, count_params = 0, 0, 0
        
        for param, wi in pesos_wi.items():
            val_col = f"valor_medido_{param}"
            vmp_col = f"vmp_396_consumo_humano_{param}"
            sin_col = f"sinal_limite_{param}"
            
            if val_col in row and pd.notna(row[val_col]):
                try:
                    val = float(row[val_col])
                    vmp = float(row[vmp_col]) if pd.notna(row[vmp_col]) else None
                    
                    # TRATAMENTO DE LIMITE DE DETECÇÃO (<)
                    # Se o dado for < 0.01, usamos 0.005 (metade do valor)
                    if sin_col in row and row[sin_col] == '<':
                        val = val / 2.0
                    
                    if vmp and vmp > 0:
                        qi = (val / vmp) * 100
                        soma_qi_wi += (qi * wi)
                        soma_wi += wi
                        count_params += 1
                except: continue
        
        if soma_wi > 0:
            return round(soma_qi_wi / soma_wi, 2), count_params
        return None, 0

    # Aplica o cálculo e extrai os resultados das tuplas
    resul_calc = df_pivot.apply(calc_estrito, axis=1)
    df_pivot['IQASB_Valor'] = [x[0] for x in resul_calc]
    df_pivot['Qtd_Params_Usados'] = [x[1] for x in resul_calc]

    # Classificação Final
    def classificar(v):
        if v is None: return "N/A"
        if v <= 25: return 'Ótima'
        if v <= 50: return 'Boa'
        if v <= 75: return 'Regular'
        if v <= 100: return 'Ruim'
        return 'Imprópria'

    df_pivot['IQASB_Classe'] = df_pivot['IQASB_Valor'].apply(classificar)

    # 3. VISUALIZAÇÃO (Gráfico com Legendas de Faixa)
    faixas = [
        (0, 25, '#00B050', 'Ótima (0-25)'), 
        (25, 50, '#92D050', 'Boa (26-50)'), 
        (50, 75, '#FFFF00', 'Regular (51-75)'), 
        (75, 100, '#FFC000', 'Ruim (76-100)'), 
        (100, 300, '#FF0000', 'Imprópria (>100)')
    ]
    
    for camp in df_pivot['nome_campanha'].unique():
        df_c = df_pivot[df_pivot['nome_campanha'] == camp].dropna(subset=['IQASB_Valor'])
        if df_c.empty: continue

        fig, ax = plt.subplots(figsize=(16, 9))
        legend_patches = []
        for low, high, color, lbl in faixas: 
            ax.axhspan(low, high, color=color, alpha=0.8, zorder=0)
            legend_patches.append(mpatches.Patch(color=color, label=lbl))
        
        x_idx = range(len(df_c))
        ax.scatter(x_idx, df_c['IQASB_Valor'], color='black', s=100, zorder=5, label='IQASB')
        legend_patches.append(plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='black', markersize=10, label='IQASB'))
        
        for i, row in df_c.reset_index().iterrows():
            txt = ax.annotate(f"{row['IQASB_Valor']}", (i, row['IQASB_Valor']), 
                              xytext=(10, 10), textcoords='offset points', 
                              fontsize=11, fontweight='bold')
            txt.set_path_effects([path_effects.withStroke(linewidth=3, foreground='white')])

        ax.set_title(f"IQASB Metodologia Estrita (ABAS) - {camp}", fontsize=18, fontweight='bold', pad=20)
        ax.set_ylabel("Valor do Índice", fontsize=14)
        ax.set_ylim(0, max(110, df_c['IQASB_Valor'].max() * 1.3))
        ax.set_xticks(x_idx)
        ax.set_xticklabels(df_c['nome_ponto'], rotation=45, ha='right')
        
        # Legenda posicionada abaixo do gráfico
        ax.legend(handles=legend_patches, loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3)
        
        nome_arq = camp.replace(" ", "_").replace("/", "-")
        plt.savefig(os.path.join(caminho_raiz, "Água_Subterrânea", f"06_IQASB_ESTRITO_{nome_arq}.png"), dpi=300, bbox_inches='tight')
        plt.close()

        # Excel Resumo com Metadados de Auditoria
        df_c[['nome_campanha', 'nome_ponto', 'IQASB_Valor', 'IQASB_Classe', 'Qtd_Params_Usados']].to_excel(
            os.path.join(caminho_raiz, "Água_Subterrânea", f"06_Resumo_IQASB_ESTRITO_{nome_arq}.xlsx"), index=False
        )

    print("✅ IQASB Estrito finalizado com sucesso nas pastas.")

# EXECUÇÃO
processar_iqasb_estrito(df_total, CAMINHO_RAIZ)


# In[11]:


# CÉLULA ÚNICA: QUOCIENTE DE PEL MÉDIO (m-PEL-q) - VERSÃO FERSAM001
import math
import matplotlib.patheffects as path_effects
import matplotlib.patches as mpatches

def processar_mpelq_completo(df_total, caminho_raiz):
    print("🚀 Iniciando Cálculo de m-PEL-q (Risco de Toxicidade Aditiva em Sedimento)...")
    
    # 1. PROCESSAMENTO DE DADOS
    df_sed = df_total[df_total['matriz'] == 'Sedimento'].copy()
    
    # Pivotagem robusta usando nomes exatos detectados no Supabase
    df_pivot = df_sed.pivot_table(
        index=['nome_campanha', 'nome_ponto'], 
        columns='nome_parametro', 
        values=['valor_medido', 'vmp_454_n2', 'sinal_limite'],
        aggfunc='first'
    )
    df_pivot.columns = [f"{col[0]}_{col[1]}" for col in df_pivot.columns]
    df_pivot = df_pivot.reset_index()

    # LISTA DE METAIS COM NOMES EXATOS DO PROJETO FERSAM001
    metais_alvo = [
        'Arsênio Total', 'Cádmio Total', 'Chumbo Total', 'Cromo Total', 
        'Mercúrio Total', 'Cobre', 'Níquel', 'Zinco'
    ]

    def calc_mpelq(row):
        soma_quocientes = 0
        count_metais = 0
        
        for metal in metais_alvo:
            val_col = f"valor_medido_{metal}"
            n2_col = f"vmp_454_n2_{metal}"
            sin_col = f"sinal_limite_{metal}"
            
            if val_col in row and pd.notna(row[val_col]) and pd.notna(row[n2_col]):
                val = float(row[val_col])
                n2 = float(row[n2_col])
                
                # Tratamento Rigoroso: Metade do Limite de Detecção se <
                if sin_col in row and row[sin_col] == '<':
                    val = val / 2.0
                
                if n2 > 0:
                    soma_quocientes += (val / n2)
                    count_metais += 1
        
        return round(soma_quocientes / count_metais, 4) if count_metais > 0 else None

    df_pivot['m_PEL_q'] = df_pivot.apply(calc_mpelq, axis=1)

    # Classificação CETESB (Probabilidade de Efeitos Adversos à Biota)
    def classificar_sedimento(v):
        if v is None: return "Sem dados"
        if v <= 0.1: return 'Improvável'
        if v <= 1.0: return 'Possível'
        return 'Provável'

    df_pivot['Status_Toxicidade'] = df_pivot['m_PEL_q'].apply(classificar_sedimento)

    # 2. GRÁFICOS E EXCEL
    faixas = [
        (0, 0.1, '#0070C0', 'Improvável (≤0.1)'), 
        (0.1, 1.0, '#FFFF00', 'Possível (0.1-1.0)'), 
        (1.0, 5.0, '#FF0000', 'Provável (>1.0)')
    ]
    
    for camp in df_pivot['nome_campanha'].unique():
        df_c = df_pivot[df_pivot['nome_campanha'] == camp].dropna(subset=['m_PEL_q'])
        if df_c.empty: continue

        fig, ax = plt.subplots(figsize=(16, 9))
        legend_patches = []
        for low, high, color, lbl in faixas:
            ax.axhspan(low, high, color=color, alpha=0.8, zorder=0)
            legend_patches.append(mpatches.Patch(color=color, label=lbl))
        
        x_idx = range(len(df_c))
        ax.scatter(x_idx, df_c['m_PEL_q'], color='black', s=120, zorder=5)

        for i, row in df_c.reset_index().iterrows():
            txt = ax.annotate(f"{row['m_PEL_q']}", (i, row['m_PEL_q']), 
                              xytext=(10, 10), textcoords='offset points',
                              fontsize=11, fontweight='bold', zorder=10)
            txt.set_path_effects([path_effects.withStroke(linewidth=3, foreground='white')])

        ax.set_title(f"Quociente de PEL Médio (m-PEL-q) - {camp}", fontsize=18, fontweight='bold', pad=20)
        ax.set_ylabel("Valor do Índice m-PEL-q", fontsize=14)
        ax.set_ylim(0, max(1.2, df_c['m_PEL_q'].max() * 1.5))
        ax.set_xticks(x_idx)
        ax.set_xticklabels(df_c['nome_ponto'], rotation=45, ha='right')
        
        ax.legend(handles=legend_patches, title="Risco de Efeitos Adversos (CETESB)", 
                  loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3)
        
        nome_arq = camp.replace(" ", "_").replace("/", "-")
        plt.savefig(os.path.join(caminho_raiz, "Sedimento", f"07_Grafico_mPELq_{nome_arq}.png"), dpi=300, bbox_inches='tight')
        plt.close()

        # Excel Resumo
        df_c[['nome_campanha', 'nome_ponto', 'm_PEL_q', 'Status_Toxicidade']].to_excel(
            os.path.join(caminho_raiz, "Sedimento", f"07_Resumo_mPELq_{nome_arq}.xlsx"), index=False
        )

    print(f"✅ Sucesso: Resultados de m-PEL-q salvos na pasta /Sedimento")

# EXECUÇÃO
processar_mpelq_completo(df_total, CAMINHO_RAIZ)


# In[ ]:




