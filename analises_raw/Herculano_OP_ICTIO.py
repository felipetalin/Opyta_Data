#!/usr/bin/env python
# coding: utf-8

# In[1]:


# --- Bloco 1: Instalações e Importações Essenciais ---
print("--- Iniciando Bloco 1: Configuração do Ambiente ---")

# ⚠️ Descomente apenas se for a primeira execução no ambiente
# !pip install pandas sqlalchemy psycopg2-binary python-dotenv scikit-bio plotly matplotlib seaborn scipy numpy

# ===============================
# 📦 Manipulação de Dados
# ===============================
import pandas as pd
import numpy as np

# ===============================
# 🗄️ Conexão com Banco de Dados
# ===============================
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# ===============================
# 📊 Visualização
# ===============================
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

# Manter padrão visual consistente
sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (10, 6)
plt.rcParams["axes.titlesize"] = 14
plt.rcParams["axes.labelsize"] = 12

# ===============================
# 📈 Análises Estatísticas e Ecológicas
# ===============================
from scipy.spatial.distance import pdist, squareform
from scipy.cluster.hierarchy import linkage, dendrogram
from scipy.stats import entropy

# ===============================
# 🔎 Controle de Versão do Ambiente
# ===============================
print("Versão do Pandas:", pd.__version__)
print("Versão do NumPy:", np.__version__)

print("Bibliotecas importadas com sucesso.")
print("--- Bloco 1 concluído ---")


# In[2]:


# --- Bloco 2: Conexão e Carga de Dados do Projeto Alvo ---
print("--- Iniciando Bloco 2: Carregando dados do projeto 'Diagnóstico Ouro Preto' ---")

# --- PARÂMETROS DA ANÁLISE ---
projeto_alvo = 'Diagnóstico Ouro Preto'
grupo_alvo = 'Ictiofauna'
# ---------------------------------

print(f"Projeto Alvo: '{projeto_alvo}'")
print(f"Grupo Biológico Alvo: '{grupo_alvo}'")

# --- CONEXÃO COM BANCO ---
load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_name = os.getenv('DB_NAME')
db_port = os.getenv('DB_PORT', '5432')

# Checagem mínima (evita URL quebrada)
vars_faltando = [k for k, v in {
    "DB_USER": db_user,
    "DB_PASSWORD": db_password,
    "DB_HOST": db_host,
    "DB_NAME": db_name
}.items() if not v]

if vars_faltando:
    raise ValueError(f"[ERRO] Variáveis ausentes no .env: {', '.join(vars_faltando)}")

db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# --- QUERY PARAMETRIZADA (mais segura e robusta) ---
query = """
SELECT *
FROM biota_analise_consolidada
WHERE nome_projeto = %(projeto)s
  AND grupo_biologico = %(grupo)s;
"""

params = {"projeto": projeto_alvo, "grupo": grupo_alvo}

print("\nExecutando query no banco de dados...")
try:
    df_projeto = pd.read_sql_query(query, engine, params=params)

    if df_projeto.empty:
        print("\n[AVISO] Nenhum dado encontrado para Ictiofauna neste projeto.")
    else:
        print(f"\n[SUCESSO] {len(df_projeto)} registros de Ictiofauna carregados com sucesso!")

        # Campanhas
        if 'nome_campanha' in df_projeto.columns:
            campanhas_encontradas = df_projeto['nome_campanha'].dropna().unique()
            print("\nCampanhas disponíveis neste conjunto de dados:")
            for campanha in sorted(campanhas_encontradas):
                print(f"- {campanha}")
        else:
            print("\n[AVISO] Coluna 'nome_campanha' não encontrada no dataset.")

        # --- EXPORT DO DATAFRAME (padrão do projeto) ---
        output_path = f"df_projeto_{grupo_alvo.replace(' ', '_')}_{projeto_alvo.replace(' ', '_')}.xlsx"
        df_projeto.to_excel(output_path, index=False)
        print(f"\n[EXPORT] DataFrame exportado para: {output_path}")

        print("\n--- Amostra dos Dados Carregados (5 primeiras linhas) ---")
        display(df_projeto.head())
        print("---------------------------------------------------------")

except Exception as e:
    print(f"\n[ERRO] Ocorreu um erro ao buscar os dados: {e}")

print("\n--- Bloco 2 concluído ---")


# In[3]:


# --- Bloco 3: Configuração dos Outputs e Diretório de Resultados (Atualizado para Ictiofauna) ---
print("--- Iniciando Bloco 3: Configurando o diretório de resultados para Ictiofauna ---")

# --- PARÂMETRO PRINCIPAL DE SAÍDA ---
# <<<<<<< CAMINHO ATUALIZADO >>>>>>>
pasta_destino = r"G:\Meu Drive\Opyta\Clientes\Clientes\Clientes\Geomil\OP Herculano\Planilhas e resultados\Resultados\4-Ictio\Campanha 1+2"
# ------------------------------------

print(f"Pasta de destino para todos os resultados: {pasta_destino}")

try:
    # Garante que a pasta de destino exista. Se não, ela será criada.
    os.makedirs(pasta_destino, exist_ok=True)
    print("Diretório de destino está pronto.")
        
except Exception as e:
    print(f"[ERRO] Não foi possível criar o diretório de destino. Detalhe do erro: {e}")

print("\n--- Bloco 3 concluído. ---")


# In[4]:


# --- Bloco 4: Análise de Composição Taxonômica ---
print("--- Iniciando Bloco 4: Tabela de Composição Taxonômica ---")

def _modo_ou_primeiro(serie):
    """Retorna o valor mais frequente (modo). Em empate ou vazio, retorna o primeiro não-nulo."""
    s = serie.dropna()
    if s.empty:
        return np.nan
    modos = s.mode()
    return modos.iloc[0] if not modos.empty else s.iloc[0]

# 1. ANÁLISE: Gerar a tabela
if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio. Nenhuma análise pode ser feita.")
else:
    tabela_composicao = (
        df_projeto
        .groupby('nome_cientifico', as_index=False)
        .agg(
            ordem=('ordem', _modo_ou_primeiro),
            familia=('familia', _modo_ou_primeiro),
            nome_popular=('nome_popular', _modo_ou_primeiro),
            origem=('origem', _modo_ou_primeiro),
        )
        .sort_values(['ordem', 'familia', 'nome_cientifico'], na_position='last')
        .reset_index(drop=True)
    )

    # 2. VISUALIZAÇÃO NO NOTEBOOK: Mostrar a tabela
    print(f"\n[SUCESSO] {len(tabela_composicao)} táxons únicos encontrados para o grupo '{grupo_alvo}'.")
    print("--- Pré-visualização da Tabela ---")
    display(tabela_composicao)
    print("-" * 34)

    # 3. OUTPUT (EXPORTAÇÃO): Salvar o resultado
    try:
        nome_arquivo = f"01_tabela_composicao_{grupo_alvo.lower()}.xlsx"
        caminho_completo = os.path.join(pasta_destino, nome_arquivo)

        print(f"\nExportando tabela para: {caminho_completo}")
        tabela_composicao.to_excel(caminho_completo, index=False, engine='openpyxl')
        print("[SUCESSO] Arquivo salvo com sucesso!")

    except Exception as e:
        print(f"\n[ERRO] Falha ao exportar a tabela para Excel. Detalhe: {e}")

print("\n--- Bloco 4 concluído ---")


# In[10]:


# --- Bloco 4.5: Definição da Paleta de Cores Padrão ---
print("--- Iniciando Bloco 4.5: Definindo cores padrão para os gráficos ---")

# ============================================================
# 🎨 PADRÃO VISUAL DO PROJETO ICTIOFAUNA
# ORDEM OFICIAL DAS CAMPANHAS:
#   1º = Seca
#   2º = Chuva
# ============================================================

# --- COR PRINCIPAL (uso geral) ---
COR_PRINCIPAL = '#1f77b4'  # Azul padrão profissional

# --- CAMPANHAS ---
# Ordem fixa e obrigatória
ORDEM_CAMPANHAS = ['Seca', 'Chuva']

# Mapa fixo de cores
CORES_CAMPANHAS = {
    'Seca':  '#73bfe2',  # azul claro
    'Chuva': '#1f77b4',  # azul escuro
}

# Paleta pronta para seaborn/matplotlib respeitando a ordem correta
PALETA_CAMPANHAS = [CORES_CAMPANHAS[c] for c in ORDEM_CAMPANHAS]

# --- ORDENS TAXONÔMICAS (Donut/Rosca) ---
PALETA_ORDEM_DONUT = ['#ff7f0e', '#1f77b4', '#2ca02c', '#9467bd', '#8c564b']

# --- DIVERSIDADE (Shannon & Pielou) ---
COR_DIVERSIDADE_BAR = '#0077b6'
COR_EQUITABILIDADE_DOT = '#00b4d8'

# --- SUFICIÊNCIA AMOSTRAL ---
COR_OBSERVADA_LINE = '#0077b6'
COR_ESTIMADA_LINE = '#00b4d8'
COR_ESTIMADA_FILL = '#00b4d8'

print("Cores padrão definidas com sucesso.")
print("Ordem oficial das campanhas:", ORDEM_CAMPANHAS)
print("Cores atribuídas:", CORES_CAMPANHAS)
print("--- Bloco 4.5 concluído ---")


# In[14]:


# --- Bloco 5: Análise de Riqueza por Ponto Amostral (Fonte menor + Legenda inferior) ---
print("--- Iniciando Bloco 5: Gráfico de Riqueza por Ponto ---")

# Ordem oficial das campanhas (mantendo nomes completos)
ORDEM_CAMPANHAS_COMPLETA = [
    "1º Campanha (Seca)",
    "2º Campanha (Chuva)"
]

if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio. Nenhuma análise pode ser feita.")
else:
    # ------------------------------------------------------------------
    # 1) ANÁLISE: calcular riqueza por campanha e ponto
    # ------------------------------------------------------------------
    df_riqueza = (
        df_projeto
        .groupby(["nome_campanha", "nome_ponto"])["nome_cientifico"]
        .nunique()
        .reset_index()
        .rename(columns={"nome_cientifico": "riqueza"})
    )

    # limpeza de strings
    df_riqueza["nome_campanha"] = df_riqueza["nome_campanha"].astype(str).str.strip()
    df_riqueza["nome_ponto"] = df_riqueza["nome_ponto"].astype(str).str.strip()

    # aplica ordem das campanhas
    df_riqueza["nome_campanha"] = pd.Categorical(
        df_riqueza["nome_campanha"],
        categories=ORDEM_CAMPANHAS_COMPLETA,
        ordered=True
    )

    # ordem dos pontos
    pontos_ordem = sorted(df_riqueza["nome_ponto"].dropna().unique())
    df_riqueza["nome_ponto"] = pd.Categorical(
        df_riqueza["nome_ponto"],
        categories=pontos_ordem,
        ordered=True
    )

    df_riqueza = df_riqueza.sort_values(
        ["nome_ponto", "nome_campanha"]
    ).reset_index(drop=True)

    # ------------------------------------------------------------------
    # 2) EXPORTAÇÃO DO DATAFRAME DO GRÁFICO
    # ------------------------------------------------------------------
    try:
        nome_df = f"02_df_riqueza_por_ponto_{grupo_alvo.lower()}.xlsx"
        caminho_df = os.path.join(pasta_destino, nome_df)
        df_riqueza.to_excel(caminho_df, index=False, engine="openpyxl")
        print(f"[EXPORT] DataFrame exportado para: {caminho_df}")
    except Exception as e:
        print(f"[ERRO] Falha ao exportar DataFrame. Detalhe: {e}")

    # ------------------------------------------------------------------
    # 3) GRÁFICO (PADRÃO RELATÓRIO: fonte menor + legenda inferior)
    # ------------------------------------------------------------------
    print("\n--- Gerando gráfico ---")

    fig_riqueza = px.bar(
        df_riqueza,
        x="nome_ponto",
        y="riqueza",
        color="nome_campanha",
        barmode="group",
        text="riqueza",
        category_orders={
            "nome_campanha": ORDEM_CAMPANHAS_COMPLETA,
            "nome_ponto": pontos_ordem
        },
        labels={
            "nome_ponto": "Ponto Amostral",
            "riqueza": "Riqueza de Espécies",
            "nome_campanha": "Campanha"
        },
        title=f"Riqueza de Táxons por Ponto Amostral - {grupo_alvo}",
        color_discrete_sequence=PALETA_CAMPANHAS
    )

    # Layout padrão (sem fundo; fonte menor; legenda embaixo)
    fig_riqueza.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="black", size=14, family="Arial"),

        title=dict(
            x=0.5,
            xanchor="center",
            font=dict(size=20, color="black")
        ),

        xaxis=dict(
            showgrid=False,
            showline=True,
            linecolor="black",
            ticks="outside",
            tickfont=dict(size=14, color="black"),
            title_font=dict(size=16, color="black")
        ),

        yaxis=dict(
            showgrid=True,
            gridcolor="lightgray",
            gridwidth=1,
            zeroline=False,
            showline=True,
            linecolor="black",
            ticks="outside",
            tickfont=dict(size=14, color="black"),
            title_font=dict(size=16, color="black")
        ),

        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.20,              # legenda abaixo
            xanchor="center",
            x=0.5,
            title_text="",
            font=dict(size=14, color="black")
        ),

        margin=dict(t=80, b=140, l=80, r=40)
    )

    # borda nas barras + texto acima
    fig_riqueza.update_traces(
        textposition="outside",
        cliponaxis=False,
        marker_line_color="black",
        marker_line_width=1
    )

    fig_riqueza.show()

    # ------------------------------------------------------------------
    # 4) EXPORTAÇÃO DO GRÁFICO
    # ------------------------------------------------------------------
    try:
        nome_arquivo = f"02_grafico_riqueza_por_ponto_{grupo_alvo.lower()}.png"
        caminho_fig = os.path.join(pasta_destino, nome_arquivo)

        print(f"\nExportando gráfico para: {caminho_fig}")
        fig_riqueza.write_image(caminho_fig, scale=2)
        print("[SUCESSO] Gráfico salvo com sucesso!")

    except Exception as e:
        print(f"[ERRO] Falha ao exportar gráfico. Verifique se 'kaleido' está instalado. Detalhe: {e}")

print("\n--- Bloco 5 concluído ---")


# In[16]:


# --- Bloco 6: Análise de Abundância por Ponto Amostral (PADRÃO OFICIAL + EXPORT DF) ---
print("--- Iniciando Bloco 6: Gráfico de Abundância por Ponto ---")

ORDEM_CAMPANHAS_COMPLETA = [
    "1º Campanha (Seca)",
    "2º Campanha (Chuva)"
]

if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio.")
else:

    # -------------------------------------------------------------
    # 1) FILTRAR DADOS QUANTITATIVOS
    # -------------------------------------------------------------
    df_quant = df_projeto[
        df_projeto["tipo_amostragem"].astype(str).str.contains("Quantitativa", case=False, na=False)
    ].copy()

    if df_quant.empty:
        print("[AVISO] Nenhum dado quantitativo encontrado.")
    else:

        # -------------------------------------------------------------
        # 2) CALCULAR ABUNDÂNCIA TOTAL (SOMA DA CONTAGEM)
        # -------------------------------------------------------------
        df_abundancia = (
            df_quant
            .groupby(["nome_campanha", "nome_ponto"], dropna=False)["contagem"]
            .sum()
            .reset_index()
            .rename(columns={"contagem": "abundancia_total"})
        )

        # limpeza de strings
        df_abundancia["nome_campanha"] = df_abundancia["nome_campanha"].astype(str).str.strip()
        df_abundancia["nome_ponto"] = df_abundancia["nome_ponto"].astype(str).str.strip()

        # ordem fixa das campanhas
        df_abundancia["nome_campanha"] = pd.Categorical(
            df_abundancia["nome_campanha"],
            categories=ORDEM_CAMPANHAS_COMPLETA,
            ordered=True
        )

        # ordem dos pontos (ordenar alfabeticamente)
        pontos_ordem = sorted(df_abundancia["nome_ponto"].dropna().unique())
        df_abundancia["nome_ponto"] = pd.Categorical(
            df_abundancia["nome_ponto"],
            categories=pontos_ordem,
            ordered=True
        )

        df_abundancia = df_abundancia.sort_values(
            ["nome_ponto", "nome_campanha"]
        ).reset_index(drop=True)

        # -------------------------------------------------------------
        # 3) EXPORTAÇÃO DO DATAFRAME (OBRIGATÓRIA)
        # -------------------------------------------------------------
        try:
            nome_df = f"03_df_abundancia_por_ponto_{grupo_alvo.lower()}.xlsx"
            caminho_df = os.path.join(pasta_destino, nome_df)
            df_abundancia.to_excel(caminho_df, index=False, engine="openpyxl")
            print(f"[EXPORT] DataFrame exportado para: {caminho_df}")
        except Exception as e:
            print(f"[ERRO] Falha ao exportar o DataFrame: {e}")

        # -------------------------------------------------------------
        # 4) GERAR GRÁFICO
        # -------------------------------------------------------------
        print("\n--- Gerando gráfico de abundância ---")

        fig_abundancia = px.bar(
            df_abundancia,
            x="nome_ponto",
            y="abundancia_total",
            color="nome_campanha",
            barmode="group",
            text="abundancia_total",
            category_orders={
                "nome_campanha": ORDEM_CAMPANHAS_COMPLETA,
                "nome_ponto": pontos_ordem
            },
            labels={
                "nome_ponto": "Ponto Amostral",
                "abundancia_total": "Abundância Total (Nº de Indivíduos)",
                "nome_campanha": "Campanha"
            },
            title=f"Abundância Total por Ponto Amostral - {grupo_alvo}",
            color_discrete_sequence=PALETA_CAMPANHAS
        )

        # -------------------------------------------------------------
        # 5) PADRÃO VISUAL (IGUAL AO BLOCO 5)
        # -------------------------------------------------------------
        fig_abundancia.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(color="black", size=14, family="Arial"),

            title=dict(
                x=0.5,
                xanchor="center",
                font=dict(size=20, color="black")
            ),

            xaxis=dict(
                showgrid=False,
                showline=True,
                linecolor="black",
                ticks="outside",
                tickfont=dict(size=14, color="black"),
                title_font=dict(size=16, color="black")
            ),

            yaxis=dict(
                showgrid=True,
                gridcolor="lightgray",
                gridwidth=1,
                zeroline=False,
                showline=True,
                linecolor="black",
                ticks="outside",
                tickfont=dict(size=14, color="black"),
                title_font=dict(size=16, color="black")
            ),

            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.20,
                xanchor="center",
                x=0.5,
                title_text="",
                font=dict(size=14, color="black")
            ),

            margin=dict(t=80, b=140, l=80, r=40)
        )

        fig_abundancia.update_traces(
            textposition="outside",
            cliponaxis=False,
            marker_line_color="black",
            marker_line_width=1
        )

        fig_abundancia.show()

        # -------------------------------------------------------------
        # 6) EXPORTAÇÃO DA FIGURA
        # -------------------------------------------------------------
        try:
            nome_arquivo = f"03_grafico_abundancia_por_ponto_{grupo_alvo.lower()}.png"
            caminho_fig = os.path.join(pasta_destino, nome_arquivo)

            print(f"\nExportando gráfico para: {caminho_fig}")
            fig_abundancia.write_image(caminho_fig, scale=2)
            print("[SUCESSO] Gráfico salvo com sucesso!")

        except Exception as e:
            print(f"[ERRO] Falha ao exportar gráfico: {e}")

print("\n--- Bloco 6 concluído ---")


# In[20]:


# --- Bloco 7 (PADRÃO FINAL): Análise de Riqueza por Ordem ---
print("--- Iniciando Bloco 7: Gráficos de Riqueza por Ordem ---")

if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio.")
else:
    # =========================
    # 1) ANÁLISE: riqueza por ordem
    # =========================
    df_riqueza_ordem = (
        df_projeto
        .groupby('ordem')['nome_cientifico']
        .nunique()
        .reset_index()
        .rename(columns={'nome_cientifico': 'numero_de_especies'})
        .sort_values(by='numero_de_especies', ascending=False)
        .reset_index(drop=True)
    )

    total_spp = int(df_riqueza_ordem['numero_de_especies'].sum())

    # =========================
    # 2) EXPORTAÇÃO DO DATAFRAME (Barras/Donut usam o mesmo df)
    # =========================
    try:
        nome_df = f"04_df_riqueza_por_ordem_{grupo_alvo.lower()}.xlsx"
        caminho_df = os.path.join(pasta_destino, nome_df)
        df_riqueza_ordem.to_excel(caminho_df, index=False, engine='openpyxl')
        print(f"[EXPORT] DataFrame exportado para: {caminho_df}")
    except Exception as e:
        print(f"[ERRO] Falha ao exportar DataFrame. Detalhe: {e}")

    # ============================================================
    # 3) GRÁFICO 1: BARRAS (padrão fundo branco, fonte preta)
    # ============================================================
    print("\n--- Gráfico de Barras: Riqueza por Ordem ---")

    fig_bar = px.bar(
        df_riqueza_ordem,
        x='ordem',
        y='numero_de_especies',
        text='numero_de_especies',
        title=f"Riqueza de {grupo_alvo} por Ordem",
        labels={'ordem': 'Ordem', 'numero_de_especies': 'Número de Espécies'},
        color_discrete_sequence=[COR_PRINCIPAL]
    )

    fig_bar.update_traces(
        textposition='outside',
        cliponaxis=False,
        marker_line_color='black',
        marker_line_width=1
    )

    fig_bar.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black', size=14, family='Arial'),
        title=dict(x=0.5, xanchor='center', font=dict(size=22, color='black')),
        xaxis=dict(
            title_font=dict(size=18, color='black'),
            tickfont=dict(size=14, color='black'),
            showline=True,
            linecolor='black'
        ),
        yaxis=dict(
            title_font=dict(size=18, color='black'),
            tickfont=dict(size=14, color='black'),
            showline=True,
            linecolor='black',
            gridcolor='lightgray'
        ),
        margin=dict(t=90, b=90, l=80, r=40)
    )

    fig_bar.show()

    # EXPORT PNG (barras)
    try:
        nome_arquivo_bar = f"04_grafico_riqueza_ordem_barras_{grupo_alvo.lower()}.png"
        caminho_bar = os.path.join(pasta_destino, nome_arquivo_bar)
        fig_bar.write_image(caminho_bar, scale=2)
        print(f"[SUCESSO] Gráfico de barras salvo em: {caminho_bar}")
    except Exception as e:
        print(f"[ERRO] Falha ao salvar gráfico de barras. Detalhe: {e}")

    # ============================================================
    # 4) GRÁFICO 2: ROSCA/DONUT (padrão fundo branco, fonte preta, legenda embaixo)
    # ============================================================
    print("\n--- Gráfico de Rosca: Composição Percentual por Ordem ---")

    fig_donut = px.pie(
        df_riqueza_ordem,
        names='ordem',
        values='numero_de_especies',
        hole=0.5,
        title=f"Riqueza de {grupo_alvo} por Ordem (%)",
        color_discrete_sequence=PALETA_ORDEM_DONUT
    )

    fig_donut.update_traces(
        textposition='inside',
        textinfo='percent+label',
        marker=dict(line=dict(color='black', width=1))
    )

    fig_donut.add_annotation(
        text=f"Total<br>{total_spp} spp.",
        x=0.5,
        y=0.5,
        font=dict(size=18, color='black', family='Arial'),
        showarrow=False
    )

    fig_donut.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='black', size=14, family='Arial'),
        title=dict(x=0.5, xanchor='center', font=dict(size=22, color='black')),
        legend=dict(
            orientation='h',
            yanchor='top',
            y=-0.15,
            xanchor='center',
            x=0.5,
            title_text='',
            font=dict(size=14, color='black')
        ),
        margin=dict(t=90, b=140, l=40, r=40)
    )

    fig_donut.show()

    # EXPORT PNG (donut)
    try:
        nome_arquivo_donut = f"05_grafico_riqueza_ordem_rosca_{grupo_alvo.lower()}.png"
        caminho_donut = os.path.join(pasta_destino, nome_arquivo_donut)
        fig_donut.write_image(caminho_donut, scale=2)
        print(f"[SUCESSO] Gráfico de rosca salvo em: {caminho_donut}")
    except Exception as e:
        print(f"[ERRO] Falha ao salvar gráfico de rosca. Detalhe: {e}")

print("\n--- Bloco 7 concluído ---")


# In[21]:


# --- Bloco 8 (PADRÃO FINAL): Análise de CPUE por Ponto Amostral ---
print("--- Iniciando Bloco 8: Gráficos de CPUE por Ponto ---")

import re

# =========================
# 0) VALIDADORES / REGRAS
# =========================
if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio. Nenhuma análise pode ser feita.")
else:
    # Precisamos de: tipo_amostragem, esforco, contagem, biomassa, nome_campanha, nome_ponto
    cols_necessarias = {'tipo_amostragem', 'esforco', 'contagem', 'biomassa', 'nome_campanha', 'nome_ponto'}
    faltantes = cols_necessarias - set(df_projeto.columns)

    if faltantes:
        print(f"[AVISO] Colunas faltantes para CPUE: {sorted(list(faltantes))}")
    else:
        # =========================
        # 1) FILTRO QUANTITATIVO + LIMPEZA NUMÉRICA
        # =========================
        df_quant = df_projeto[df_projeto['tipo_amostragem'].str.contains('Quantit', case=False, na=False)].copy()

        if df_quant.empty:
            print("[AVISO] Nenhum dado quantitativo encontrado na seleção para calcular CPUE.")
        else:
            # garante numérico
            df_quant['esforco'] = pd.to_numeric(df_quant['esforco'], errors='coerce')
            df_quant['contagem'] = pd.to_numeric(df_quant['contagem'], errors='coerce')
            df_quant['biomassa'] = pd.to_numeric(df_quant['biomassa'], errors='coerce')

            # remove esforço inválido
            df_quant = df_quant.dropna(subset=['esforco'])
            df_quant = df_quant[df_quant['esforco'] > 0].copy()

            if df_quant.empty:
                print("[AVISO] Todos os registros quantitativos possuem 'esforco' nulo/zero. CPUE não pode ser calculado.")
            else:
                # =========================
                # 2) CÁLCULO CPUE (por registro) e AGREGAR POR CAMPANHA+PONTO
                #    CPUEn = (n / esforço)*100 ; CPUEb = (biomassa / esforço)*100
                # =========================
                df_quant['cpuen'] = (df_quant['contagem'] / df_quant['esforco']) * 100
                df_quant['cpueb'] = (df_quant['biomassa'] / df_quant['esforco']) * 100

                df_cpue_ponto = (
                    df_quant
                    .groupby(['nome_campanha', 'nome_ponto'], as_index=False)[['cpuen', 'cpueb']]
                    .sum()
                )

                # =========================
                # 3) ORDEM FIXA DAS CAMPANHAS (mantém nomes exatos)
                # =========================
                ORDEM_CAMPANHAS_EXATA = ["1º Campanha (Seca)", "2º Campanha (Chuva)"]

                # filtra apenas campanhas que existirem no df (evita categoria "vazia")
                campanhas_existentes = [c for c in ORDEM_CAMPANHAS_EXATA if c in df_cpue_ponto['nome_campanha'].unique().tolist()]
                if not campanhas_existentes:
                    # fallback (se por algum motivo vier diferente)
                    campanhas_existentes = sorted(df_cpue_ponto['nome_campanha'].dropna().unique().tolist())

                df_cpue_ponto['nome_campanha'] = pd.Categorical(
                    df_cpue_ponto['nome_campanha'],
                    categories=campanhas_existentes,
                    ordered=True
                )

                # ordem de pontos (alfanum)
                pontos_ordem = sorted(df_cpue_ponto['nome_ponto'].dropna().unique(), key=lambda x: str(x))
                df_cpue_ponto['nome_ponto'] = pd.Categorical(
                    df_cpue_ponto['nome_ponto'],
                    categories=pontos_ordem,
                    ordered=True
                )

                df_cpue_ponto = df_cpue_ponto.sort_values(['nome_ponto', 'nome_campanha']).reset_index(drop=True)

                # =========================
                # 4) EXPORTAÇÃO DO DATAFRAME (OBRIGATÓRIO)
                # =========================
                try:
                    nome_df = f"06_df_cpue_por_ponto_{grupo_alvo.lower()}.xlsx"
                    caminho_df = os.path.join(pasta_destino, nome_df)
                    df_cpue_ponto.to_excel(caminho_df, index=False, engine='openpyxl')
                    print(f"[EXPORT] DataFrame do CPUE por ponto exportado para: {caminho_df}")
                except Exception as e:
                    print(f"[ERRO] Falha ao exportar o DataFrame do CPUE. Detalhe: {e}")

                # =========================
                # 5) FUNÇÃO: layout padrão (fundo branco, fonte preta, legenda embaixo)
                # =========================
                def aplicar_layout_padrao(fig, y_title, main_title):
                    fig.update_layout(
                        plot_bgcolor='white',
                        paper_bgcolor='white',
                        font=dict(color='black', size=14, family='Arial'),
                        title=dict(x=0.5, xanchor='center', font=dict(size=22, color='black')),
                        xaxis=dict(
                            title="Ponto Amostral",
                            title_font=dict(size=18, color='black'),
                            tickfont=dict(size=14, color='black'),
                            showline=True,
                            linecolor='black'
                        ),
                        yaxis=dict(
                            title=y_title,
                            title_font=dict(size=18, color='black'),
                            tickfont=dict(size=14, color='black'),
                            showline=True,
                            linecolor='black',
                            gridcolor='lightgray'
                        ),
                        legend=dict(
                            orientation='h',
                            yanchor='top',
                            y=-0.20,
                            xanchor='center',
                            x=0.5,
                            title_text='',
                            font=dict(size=14, color='black')
                        ),
                        margin=dict(t=90, b=140, l=90, r=40)
                    )
                    fig.update_traces(
                        marker_line_color='black',
                        marker_line_width=1
                    )
                    fig.update_layout(title_text=main_title)
                    return fig

                # =========================
                # 6) PALETAS (respeita sua regra: Campanha 1 = Seca, Campanha 2 = Chuva)
                #    Aqui usamos PALETA_CAMPANHAS[0] para Seca, PALETA_CAMPANHAS[1] para Chuva
                #    Se você já fixou PALETA_CAMPANHAS nessa ordem, está ok.
                # =========================
                # garante 2 cores se houver 2 campanhas
                if len(campanhas_existentes) == 1:
                    cores_cpue = [PALETA_CAMPANHAS[0]]
                else:
                    cores_cpue = [PALETA_CAMPANHAS[0], PALETA_CAMPANHAS[1]]

                # =========================
                # 7) GRÁFICO CPUEn (um único gráfico com as 2 campanhas em barras agrupadas)
                # =========================
                print("\n--- Gráfico CPUEn por Ponto (Pré-visualização) ---")

                fig_cpuen = px.bar(
                    df_cpue_ponto,
                    x='nome_ponto',
                    y='cpuen',
                    color='nome_campanha',
                    barmode='group',
                    text='cpuen',
                    category_orders={'nome_ponto': pontos_ordem, 'nome_campanha': campanhas_existentes},
                    color_discrete_sequence=cores_cpue,
                    labels={'nome_ponto': 'Ponto Amostral', 'cpuen': 'CPUEn (ind./100m²)', 'nome_campanha': 'Campanha'}
                )

                fig_cpuen.update_traces(texttemplate='%{text:.2f}', textposition='outside', cliponaxis=False)
                fig_cpuen = aplicar_layout_padrao(
                    fig_cpuen,
                    y_title="CPUEn (ind./100m²)",
                    main_title=f"CPUEn (ind./100m²) por Ponto Amostral - {grupo_alvo}"
                )
                fig_cpuen.show()

                try:
                    nome_arq = f"06_grafico_cpuen_por_ponto_{grupo_alvo.lower()}.png"
                    caminho_img = os.path.join(pasta_destino, nome_arq)
                    fig_cpuen.write_image(caminho_img, scale=2)
                    print(f"[SUCESSO] Gráfico CPUEn salvo em: {caminho_img}")
                except Exception as e:
                    print(f"[ERRO] Falha ao salvar gráfico CPUEn. Detalhe: {e}")

                # =========================
                # 8) GRÁFICO CPUEb (um único gráfico com as 2 campanhas)
                # =========================
                print("\n--- Gráfico CPUEb por Ponto (Pré-visualização) ---")

                fig_cpueb = px.bar(
                    df_cpue_ponto,
                    x='nome_ponto',
                    y='cpueb',
                    color='nome_campanha',
                    barmode='group',
                    text='cpueb',
                    category_orders={'nome_ponto': pontos_ordem, 'nome_campanha': campanhas_existentes},
                    color_discrete_sequence=cores_cpue,
                    labels={'nome_ponto': 'Ponto Amostral', 'cpueb': 'CPUEb (g/100m²)', 'nome_campanha': 'Campanha'}
                )

                fig_cpueb.update_traces(texttemplate='%{text:.2f}', textposition='outside', cliponaxis=False)
                fig_cpueb = aplicar_layout_padrao(
                    fig_cpueb,
                    y_title="CPUEb (g/100m²)",
                    main_title=f"CPUEb (g/100m²) por Ponto Amostral - {grupo_alvo}"
                )
                fig_cpueb.show()

                try:
                    nome_arq = f"07_grafico_cpueb_por_ponto_{grupo_alvo.lower()}.png"
                    caminho_img = os.path.join(pasta_destino, nome_arq)
                    fig_cpueb.write_image(caminho_img, scale=2)
                    print(f"[SUCESSO] Gráfico CPUEb salvo em: {caminho_img}")
                except Exception as e:
                    print(f"[ERRO] Falha ao salvar gráfico CPUEb. Detalhe: {e}")

print("\n--- Bloco 8 concluído ---")


# In[28]:


# =========================================================
# BLOCO 9 (COMPLETO ATUALIZADO):
# CPUE por Espécie (CPUEn + CPUEb) - HORIZONTAL
# Espécies no eixo vertical (itálico)
# =========================================================

print("--- Iniciando Bloco 9: CPUE por Espécie (Seca x Chuva - Horizontal) ---")

import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt

CAMPANHA_SECA = "1º Campanha (Seca)"
CAMPANHA_CHUVA = "2º Campanha (Chuva)"
ORDEM_CAMPANHAS = [CAMPANHA_SECA, CAMPANHA_CHUVA]

COR_SECA = "#79c6e8"
COR_CHUVA = "#1f77b4"

def _estilo_padrao_matplotlib(ax):
    ax.set_facecolor("white")
    ax.figure.set_facecolor("white")
    ax.grid(axis="x", linestyle="-", linewidth=0.7, alpha=0.35)
    ax.grid(axis="y", visible=False)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    ax.tick_params(axis="both", colors="black", labelsize=11)

def _adicionar_valores_horiz(ax, bars, fmt="{:.2f}", dx=0.02, fontsize=9):
    xmax = max([b.get_width() for b in bars]) if bars else 0
    offset = xmax * dx if xmax > 0 else 0.1
    for b in bars:
        w = b.get_width()
        ax.text(
            w + offset,
            b.get_y() + b.get_height() / 2,
            fmt.format(w),
            va="center",
            fontsize=fontsize,
            color="black"
        )

if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio.")
else:
    df_quant = df_projeto[
        df_projeto["tipo_amostragem"].astype(str).str.contains("Quantit", case=False, na=False)
    ].copy()

    if df_quant.empty or "esforco" not in df_quant.columns:
        print("[AVISO] Nenhum dado quantitativo ou coluna 'esforco' encontrada.")
    else:
        df_quant["contagem"] = pd.to_numeric(df_quant["contagem"], errors="coerce").fillna(0)
        df_quant["biomassa"] = pd.to_numeric(df_quant["biomassa"], errors="coerce").fillna(0)
        df_quant["esforco"] = pd.to_numeric(df_quant["esforco"], errors="coerce")

        df_quant = df_quant[df_quant["esforco"].notna() & (df_quant["esforco"] > 0)].copy()

        df_quant["cpuen"] = (df_quant["contagem"] / df_quant["esforco"]) * 100
        df_quant["cpueb"] = (df_quant["biomassa"] / df_quant["esforco"]) * 100

        df_cpue_sp = (
            df_quant
            .groupby(["nome_campanha", "nome_cientifico"], as_index=False, observed=False)[["cpuen", "cpueb"]]
            .sum()
        )

        df_cpue_sp["nome_campanha"] = pd.Categorical(
            df_cpue_sp["nome_campanha"],
            categories=ORDEM_CAMPANHAS,
            ordered=True
        )

        # Pivot
        cpuen_sp = (
            df_cpue_sp.pivot_table(
                index="nome_cientifico",
                columns="nome_campanha",
                values="cpuen",
                aggfunc="sum",
                observed=False
            )
            .reindex(columns=ORDEM_CAMPANHAS)
            .fillna(0)
        )

        cpueb_sp = (
            df_cpue_sp.pivot_table(
                index="nome_cientifico",
                columns="nome_campanha",
                values="cpueb",
                aggfunc="sum",
                observed=False
            )
            .reindex(columns=ORDEM_CAMPANHAS)
            .fillna(0)
        )

        # Ordena por total CPUEn
        ordem_especies = cpuen_sp.sum(axis=1).sort_values(ascending=True).index.tolist()
        cpuen_sp = cpuen_sp.loc[ordem_especies].reset_index()
        cpueb_sp = cpueb_sp.loc[ordem_especies].reset_index()

        # Exporta DataFrames
        cpuen_sp.to_excel(
            os.path.join(pasta_destino, f"08_df_cpuen_por_especie_{grupo_alvo.lower()}.xlsx"),
            index=False,
            engine="openpyxl"
        )
        cpueb_sp.to_excel(
            os.path.join(pasta_destino, f"09_df_cpueb_por_especie_{grupo_alvo.lower()}.xlsx"),
            index=False,
            engine="openpyxl"
        )
        print("[EXPORT] DataFrames exportados.")

        # =====================================================
        # FIG 1 - CPUEn (Horizontal)
        # =====================================================
        y = np.arange(len(cpuen_sp["nome_cientifico"]))
        height = 0.38

        fig, ax = plt.subplots(figsize=(12, 8))

        bars_seca = ax.barh(
            y - height/2,
            cpuen_sp[CAMPANHA_SECA],
            height,
            label=CAMPANHA_SECA,
            color=COR_SECA,
            edgecolor="black"
        )
        bars_chuva = ax.barh(
            y + height/2,
            cpuen_sp[CAMPANHA_CHUVA],
            height,
            label=CAMPANHA_CHUVA,
            color=COR_CHUVA,
            edgecolor="black"
        )

        ax.set_title(f"CPUEn (ind./100m²) por Espécie - {grupo_alvo}", fontsize=18, pad=15)
        ax.set_xlabel("CPUEn (ind./100m²)", fontsize=14)
        ax.set_ylabel("Espécie", fontsize=14)

        ax.set_yticks(y)
        ax.set_yticklabels(cpuen_sp["nome_cientifico"], fontstyle="italic")

        _estilo_padrao_matplotlib(ax)
        _adicionar_valores_horiz(ax, bars_seca)
        _adicionar_valores_horiz(ax, bars_chuva)

        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08),
                  ncol=2, frameon=False, fontsize=12)

        plt.tight_layout()
        plt.savefig(os.path.join(pasta_destino, f"08_grafico_cpuen_por_especie_{grupo_alvo.lower()}.png"),
                    dpi=200, bbox_inches="tight")
        plt.show()

        # =====================================================
        # FIG 2 - CPUEb (Horizontal)
        # =====================================================
        fig, ax = plt.subplots(figsize=(12, 8))

        bars_seca = ax.barh(
            y - height/2,
            cpueb_sp[CAMPANHA_SECA],
            height,
            label=CAMPANHA_SECA,
            color=COR_SECA,
            edgecolor="black"
        )
        bars_chuva = ax.barh(
            y + height/2,
            cpueb_sp[CAMPANHA_CHUVA],
            height,
            label=CAMPANHA_CHUVA,
            color=COR_CHUVA,
            edgecolor="black"
        )

        ax.set_title(f"CPUEb (g/100m²) por Espécie - {grupo_alvo}", fontsize=18, pad=15)
        ax.set_xlabel("CPUEb (g/100m²)", fontsize=14)
        ax.set_ylabel("Espécie", fontsize=14)

        ax.set_yticks(y)
        ax.set_yticklabels(cpueb_sp["nome_cientifico"], fontstyle="italic")

        _estilo_padrao_matplotlib(ax)
        _adicionar_valores_horiz(ax, bars_seca)
        _adicionar_valores_horiz(ax, bars_chuva)

        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.08),
                  ncol=2, frameon=False, fontsize=12)

        plt.tight_layout()
        plt.savefig(os.path.join(pasta_destino, f"09_grafico_cpueb_por_especie_{grupo_alvo.lower()}.png"),
                    dpi=200, bbox_inches="tight")
        plt.show()

print("--- Bloco 9 concluído ---")


# In[34]:


# =========================================================
# BLOCO 10 (COMPLETO E INDEPENDENTE)
# Diversidade Alfa (Shannon H' + Pielou J')
# - Seca e Chuva no mesmo gráfico
# - Sem scikit-bio
# - Exporta DataFrame
# =========================================================

print("--- Iniciando Bloco 10: Diversidade Alfa (Seca x Chuva) ---")

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

CAMPANHA_SECA = "1º Campanha (Seca)"
CAMPANHA_CHUVA = "2º Campanha (Chuva)"
ORDEM_CAMPANHAS = [CAMPANHA_SECA, CAMPANHA_CHUVA]

COR_SHANNON = "#0077b6"
COR_PIELOU = "#00b4d8"

# -------------------------------------------------
# FUNÇÕES MATEMÁTICAS (SEM SKBIO)
# -------------------------------------------------

def calcular_shannon(counts):
    counts = np.array(counts, dtype=float)
    counts = counts[counts > 0]

    if len(counts) == 0:
        return 0.0

    proporcoes = counts / counts.sum()
    return -np.sum(proporcoes * np.log(proporcoes))


def calcular_pielou(counts):
    counts = np.array(counts, dtype=float)
    counts = counts[counts > 0]

    if len(counts) <= 1:
        return 0.0

    H = calcular_shannon(counts)
    S = len(counts)

    return H / np.log(S)


def estilo_padrao(ax):
    ax.set_facecolor("white")
    ax.figure.set_facecolor("white")
    ax.grid(axis="y", linestyle="-", linewidth=0.7, alpha=0.35)
    ax.grid(axis="x", visible=False)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    ax.tick_params(axis="both", labelsize=11)


# -------------------------------------------------
# 1) DADOS
# -------------------------------------------------

if df_projeto.empty:
    print("[AVISO] DataFrame vazio.")
else:
    df_div = df_projeto[
        df_projeto["tipo_amostragem"].astype(str).str.contains("Quantit", case=False, na=False)
    ].copy()

    df_div["contagem"] = pd.to_numeric(df_div["contagem"], errors="coerce").fillna(0)

    if df_div.empty or df_div["contagem"].sum() == 0:
        print("[AVISO] Sem dados quantitativos.")
    else:
        df_div = df_div[df_div["nome_campanha"].isin(ORDEM_CAMPANHAS)]

        resultados = []

        for campanha in ORDEM_CAMPANHAS:

            df_c = df_div[df_div["nome_campanha"] == campanha]

            if df_c.empty:
                continue

            matriz = df_c.pivot_table(
                index="nome_ponto",
                columns="nome_cientifico",
                values="contagem",
                aggfunc="sum",
                fill_value=0
            )

            # por ponto
            for ponto in matriz.index:
                linha = matriz.loc[ponto].values

                resultados.append({
                    "nome_campanha": campanha,
                    "nome_ponto": ponto,
                    "Shannon_H": calcular_shannon(linha),
                    "Pielou_J": calcular_pielou(linha)
                })

            # geral campanha
            total_campanha = matriz.sum(axis=0).values

            resultados.append({
                "nome_campanha": campanha,
                "nome_ponto": f"{campanha} (Geral)",
                "Shannon_H": calcular_shannon(total_campanha),
                "Pielou_J": calcular_pielou(total_campanha)
            })

        df_div_final = pd.DataFrame(resultados)

        # -------------------------------------------------
        # EXPORTA DATAFRAME
        # -------------------------------------------------
        try:
            caminho_df = os.path.join(
                pasta_destino,
                f"10_df_diversidade_alfa_{grupo_alvo.lower()}.xlsx"
            )
            df_div_final.to_excel(caminho_df, index=False, engine="openpyxl")
            print(f"[EXPORT] DataFrame salvo em: {caminho_df}")
        except Exception as e:
            print(f"[ERRO] Exportação DataFrame: {e}")

        # -------------------------------------------------
        # 2) GRÁFICO ÚNICO (Seca + Chuva)
        # -------------------------------------------------

        etiquetas = df_div_final["nome_ponto"].tolist()
        shannon_vals = df_div_final["Shannon_H"].tolist()
        pielou_vals = df_div_final["Pielou_J"].tolist()

        x = np.arange(len(etiquetas))

        fig, ax1 = plt.subplots(figsize=(14, 7))

        bars = ax1.bar(
            x,
            shannon_vals,
            color=COR_SHANNON,
            edgecolor="black",
            label="Diversidade (H')"
        )

        ax1.set_ylabel("Shannon (H')", fontsize=14)
        ax1.set_xlabel("Ponto Amostral", fontsize=14)
        ax1.set_xticks(x)
        ax1.set_xticklabels(etiquetas, rotation=45, ha="right")

        estilo_padrao(ax1)

        # eixo secundário
        ax2 = ax1.twinx()
        ax2.plot(
            x,
            pielou_vals,
            marker="o",
            linestyle="None",
            color=COR_PIELOU,
            markersize=7,
            label="Equitabilidade (J')"
        )
        ax2.set_ylabel("Pielou (J')", fontsize=14)
        ax2.set_ylim(0, 1.1)

        # linha separadora seca/chuva
        n_seca = df_div_final[df_div_final["nome_campanha"] == CAMPANHA_SECA].shape[0]
        if n_seca > 0 and n_seca < len(x):
            ax1.axvline(x=n_seca - 0.5, color="grey", linestyle="--")

        ax1.set_title(
            f"Diversidade (Shannon e Pielou) - {grupo_alvo}",
            fontsize=18,
            pad=16
        )

        # legenda inferior
        handles1, labels1 = ax1.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()

        fig.legend(
            handles1 + handles2,
            labels1 + labels2,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.08),
            ncol=2,
            frameon=False
        )

        plt.tight_layout()

        # -------------------------------------------------
        # EXPORTA GRÁFICO
        # -------------------------------------------------
        try:
            caminho_png = os.path.join(
                pasta_destino,
                f"10_grafico_diversidade_alfa_{grupo_alvo.lower()}.png"
            )
            fig.savefig(caminho_png, dpi=300, bbox_inches="tight")
            print(f"[SUCESSO] Gráfico salvo em: {caminho_png}")
        except Exception as e:
            print(f"[ERRO] Exportação gráfico: {e}")

        plt.show()

print("--- Bloco 10 concluído ---")


# In[33]:


# =========================================================
# BLOCO 11 (COMPLETO): Dendrograma ÚNICO (Seca + Chuva SOMADAS)
# - Bray-Curtis (SciPy) | Sem seaborn | Sem scikit-bio
# - Layout no padrão do modelo (eixo topo 0–100% + ticks)
# - Exporta DataFrames (matriz + distâncias) + PNG
# =========================================================

print("--- Iniciando Bloco 11: Dendrograma ÚNICO (Seca+Chuva somadas) ---")

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scipy.spatial.distance import pdist, squareform
from scipy.cluster.hierarchy import linkage, dendrogram

CAMPANHA_SECA = "1º Campanha (Seca)"
CAMPANHA_CHUVA = "2º Campanha (Chuva)"
ORDEM_CAMPANHAS = [CAMPANHA_SECA, CAMPANHA_CHUVA]

def _estilo_padrao_dendrograma(ax):
    ax.set_facecolor("white")
    ax.figure.set_facecolor("white")
    ax.grid(False)

    # remove spines “sobrando”
    for spine in ["left", "bottom"]:
        ax.spines[spine].set_visible(False)

    # mantém topo/direita (padrão do modelo)
    ax.spines["top"].set_visible(True)
    ax.spines["right"].set_visible(True)

    ax.tick_params(axis="both", colors="black", labelsize=11)

    # labels (pontos) em negrito/preto
    for label in ax.get_yticklabels():
        label.set_fontweight("bold")
        label.set_color("black")

def _safe_numeric_df(mat: pd.DataFrame) -> pd.DataFrame:
    out = mat.copy()
    for c in out.columns:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)
    return out

if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio.")
else:
    # 1) filtra quantitativo
    df_sim = df_projeto[
        df_projeto["tipo_amostragem"].astype(str).str.contains("Quantit", case=False, na=False)
    ].copy()

    if df_sim.empty:
        print("[AVISO] Nenhum dado quantitativo encontrado.")
    else:
        # força contagem numérica
        df_sim["contagem"] = pd.to_numeric(df_sim.get("contagem"), errors="coerce").fillna(0)

        # 2) mantém só Seca/Chuva (se existirem; se não, usa tudo)
        campanhas_presentes = df_sim["nome_campanha"].dropna().unique().tolist()
        alvo = [c for c in ORDEM_CAMPANHAS if c in campanhas_presentes]
        if len(alvo) > 0:
            df_sim = df_sim[df_sim["nome_campanha"].isin(alvo)].copy()

        if df_sim["contagem"].sum() == 0:
            print("[AVISO] Soma das contagens é 0; não dá para calcular Bray-Curtis.")
        else:
            # 3) matriz comunidade SOMADA (ponto x espécie), somando campanhas
            matriz = df_sim.pivot_table(
                index="nome_ponto",
                columns="nome_cientifico",
                values="contagem",
                aggfunc="sum",
                fill_value=0
            )

            if matriz.empty:
                print("[AVISO] Matriz de comunidade vazia após pivot.")
            else:
                matriz = _safe_numeric_df(matriz)

                # remove pontos sem indivíduos
                matriz = matriz.loc[matriz.sum(axis=1) > 0]

                if matriz.shape[0] < 2:
                    print("[AVISO] Não há pontos suficientes para dendrograma (mín: 2).")
                else:
                    # 4) Bray-Curtis (distância 0..1)
                    dist_condensed = pdist(matriz.values, metric="braycurtis")
                    dist_square = squareform(dist_condensed)

                    # linkage average
                    Z = linkage(dist_condensed, method="average")

                    # 5) exporta DataFrames
                    try:
                        nome_mat = f"11_df_matriz_comunidade_{grupo_alvo.lower()}_seca_chuva_somadas.xlsx"
                        matriz.to_excel(os.path.join(pasta_destino, nome_mat), engine="openpyxl")

                        df_dist = pd.DataFrame(dist_square, index=matriz.index, columns=matriz.index)
                        nome_dist = f"11_df_distancias_braycurtis_{grupo_alvo.lower()}_seca_chuva_somadas.xlsx"
                        df_dist.to_excel(os.path.join(pasta_destino, nome_dist), engine="openpyxl")

                        print("[EXPORT] Matriz comunidade + distâncias exportadas (Seca+Chuva somadas).")
                    except Exception as e:
                        print(f"[ERRO] Falha ao exportar DataFrames: {e}")

                    # 6) plot no padrão do modelo (eixo topo 0–100%)
                    print("\n--- Dendrograma ÚNICO (Seca+Chuva somadas) ---")

                    fig, ax = plt.subplots(figsize=(12, 8))

                    dendrogram(
                        Z,
                        labels=matriz.index.tolist(),
                        orientation="right",
                        ax=ax,
                        color_threshold=None
                    )

                    # eixo no topo
                    ax.xaxis.tick_top()
                    ax.xaxis.set_label_position("top")

                    # trava distância 0..1 e mostra similaridade 0..100%
                    ax.set_xlim(1.0, 0.0)  # distância invertida (para 0%->100% em cima)
                    ticks_sim = np.arange(0, 101, 10)               # 0..100
                    ticks_dist = 1 - (ticks_sim / 100.0)            # distâncias equivalentes
                    ax.set_xticks(ticks_dist)
                    ax.set_xticklabels([str(t) for t in ticks_sim])

                    ax.set_xlabel("Similaridade de Bray-Curtis (%)", fontsize=14, labelpad=10)

                    titulo = f"Dendrograma de Similaridade (Bray-Curtis) - {grupo_alvo}\nSeca + Chuva"
                    ax.set_title(titulo, fontsize=16, pad=18)

                    _estilo_padrao_dendrograma(ax)

                    plt.tight_layout()

                    # 7) exporta PNG
                    try:
                        nome_png = f"11_dendrograma_similaridade_{grupo_alvo.lower()}_seca_chuva_somadas.png"
                        caminho_png = os.path.join(pasta_destino, nome_png)
                        fig.savefig(caminho_png, dpi=300, bbox_inches="tight")
                        print(f"[SUCESSO] Dendrograma salvo em: {caminho_png}")
                    except Exception as e:
                        print(f"[ERRO] Falha ao salvar PNG: {e}")

                    plt.show()

print("--- Bloco 11 concluído ---")


# In[35]:


# =========================================================
# BLOCO 12 (COMPLETO): Curva de Suficiência Amostral
# - Riqueza Observada (Sobs) + Jackknife 1 (Sest)
# - Aleatorizações com média + desvio
# - Layout padrão (fundo branco, textos pretos, legenda embaixo)
# - Exporta DataFrame da curva + PNG
# =========================================================

print("--- Iniciando Bloco 12: Curva de Suficiência Amostral ---")

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# --------- CONFIG (você pode ajustar) ----------
N_RANDOMIZATIONS = 200   # 100 ok, 200 melhor
SEED = 42                # reprodutível (opcional)
# ----------------------------------------------

rng = np.random.default_rng(SEED)

def jackknife_1_estimator(pres_abs_matrix: np.ndarray) -> float:
    """
    Jackknife 1 para riqueza:
    Sest = Sobs + Q1 * ((k-1)/k)
    onde:
      Sobs = número de espécies observadas
      Q1 = número de espécies que ocorreram em apenas 1 amostra
      k = número de amostras
    pres_abs_matrix: matriz (k x spp) com 0/1
    """
    k = pres_abs_matrix.shape[0]
    if k == 0:
        return 0.0

    # Sobs (espécies com pelo menos 1 ocorrência)
    spp_occ = pres_abs_matrix.sum(axis=0)
    s_obs = int((spp_occ > 0).sum())

    # Q1 (espécies com ocorrência em apenas 1 amostra)
    q1 = int((spp_occ == 1).sum())

    return float(s_obs + q1 * ((k - 1) / k))

def _estilo_padrao(ax):
    ax.set_facecolor("white")
    ax.figure.set_facecolor("white")
    ax.grid(axis="y", linestyle="-", linewidth=0.7, alpha=0.35)
    ax.grid(axis="x", visible=False)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)
    ax.tick_params(axis="both", colors="black", labelsize=11)
    for lab in ax.get_xticklabels():
        lab.set_color("black")
    for lab in ax.get_yticklabels():
        lab.set_color("black")

if df_projeto.empty:
    print("[AVISO] DataFrame 'df_projeto' está vazio.")
else:
    # 1) usa apenas quantitativo (se quiser incluir tudo, remova este filtro)
    df_suf = df_projeto[
        df_projeto["tipo_amostragem"].astype(str).str.contains("Quantit", case=False, na=False)
    ].copy()

    if df_suf.empty:
        print("[AVISO] Nenhum dado quantitativo encontrado para curva.")
    else:
        # contagem numérica
        df_suf["contagem"] = pd.to_numeric(df_suf.get("contagem"), errors="coerce").fillna(0)

        # 2) Matriz presença/ausência por ponto x espécie
        # presença = 1 se soma de contagem > 0
        mat = df_suf.pivot_table(
            index="nome_ponto",
            columns="nome_cientifico",
            values="contagem",
            aggfunc="sum",
            fill_value=0,
            observed=False
        )

        if mat.empty:
            print("[AVISO] Matriz de comunidade vazia.")
        else:
            # converte para presença/ausência
            mat_pa = (mat > 0).astype(int)

            # remove pontos sem nenhuma espécie
            mat_pa = mat_pa.loc[mat_pa.sum(axis=1) > 0]

            n_samples = mat_pa.shape[0]

            if n_samples < 2:
                print("[AVISO] Não há amostras suficientes (mínimo 2) para gerar a curva.")
            else:
                # 3) Aleatorizações
                sobs_curves = np.zeros((N_RANDOMIZATIONS, n_samples), dtype=float)
                sest_curves = np.zeros((N_RANDOMIZATIONS, n_samples), dtype=float)

                mat_values = mat_pa.values

                for r in range(N_RANDOMIZATIONS):
                    idx = rng.permutation(n_samples)
                    shuffled = mat_values[idx, :]

                    # incremental
                    for i in range(1, n_samples + 1):
                        subset = shuffled[:i, :]
                        spp_occ = subset.sum(axis=0)
                        sobs = float((spp_occ > 0).sum())
                        sest = jackknife_1_estimator(subset)

                        sobs_curves[r, i - 1] = sobs
                        sest_curves[r, i - 1] = sest

                mean_sobs = sobs_curves.mean(axis=0)
                mean_sest = sest_curves.mean(axis=0)
                std_sest = sest_curves.std(axis=0)

                x_axis = np.arange(1, n_samples + 1)

                # 4) DataFrame de saída (para histórico / auditoria)
                df_curva = pd.DataFrame({
                    "n_amostras": x_axis,
                    "riqueza_obs_media": mean_sobs,
                    "riqueza_est_jackknife1_media": mean_sest,
                    "jackknife1_desvio_padrao": std_sest,
                    "jackknife1_inf": mean_sest - std_sest,
                    "jackknife1_sup": mean_sest + std_sest,
                })

                # exporta dataframe
                try:
                    nome_df = f"12_df_curva_suficiencia_{grupo_alvo.lower()}.xlsx"
                    caminho_df = os.path.join(pasta_destino, nome_df)
                    df_curva.to_excel(caminho_df, index=False, engine="openpyxl")
                    print(f"[EXPORT] DataFrame da curva exportado em: {caminho_df}")
                except Exception as e:
                    print(f"[ERRO] Falha ao exportar DataFrame da curva: {e}")

                # 5) Plot (padrão)
                print("\n--- Curva de Suficiência Amostral ---")
                fig, ax = plt.subplots(figsize=(12, 7))

                # cores padrão do seu tema
                COR_OBS = "#0077b6"
                COR_EST = "#00b4d8"

                ax.plot(x_axis, mean_sobs, linewidth=2.2, label="Riqueza Observada", color=COR_OBS)
                ax.plot(x_axis, mean_sest, linewidth=2.2, label="Riqueza Estimada (Jackknife 1)", color=COR_EST)
                ax.fill_between(
                    x_axis,
                    mean_sest - std_sest,
                    mean_sest + std_sest,
                    alpha=0.18,
                    color=COR_EST
                )

                ax.set_title(f"Curva de Suficiência Amostral - {grupo_alvo}", fontsize=18, pad=14)
                ax.set_xlabel("Número de unidades amostrais", fontsize=14, labelpad=10)
                ax.set_ylabel("Riqueza", fontsize=14, labelpad=10)

                _estilo_padrao(ax)

                # valores finais (na ponta direita)
                ax.text(x_axis[-1] + 0.15, mean_sobs[-1], f"{mean_sobs[-1]:.0f}",
                        color="black", va="center", fontsize=11)
                ax.text(x_axis[-1] + 0.15, mean_sest[-1], f"{mean_sest[-1]:.1f}",
                        color="black", va="center", fontsize=11)

                # legenda embaixo (padrão que você quer)
                ax.legend(
                    loc="upper center",
                    bbox_to_anchor=(0.5, -0.18),
                    ncol=2,
                    frameon=False,
                    fontsize=12
                )

                plt.tight_layout()

                # 6) Exporta PNG
                try:
                    nome_png = f"12_curva_suficiencia_amostral_{grupo_alvo.lower()}.png"
                    caminho_png = os.path.join(pasta_destino, nome_png)
                    fig.savefig(caminho_png, dpi=300, bbox_inches="tight")
                    print(f"[SUCESSO] Curva salva em: {caminho_png}")
                except Exception as e:
                    print(f"[ERRO] Falha ao salvar curva: {e}")

                plt.show()

print("--- Bloco 12 concluído ---")


# In[3]:


# =========================================================
# BLOCO 13 (COMPLETO): Darwin Core - Modelo IEF (Excel)
# - Lê o ARQUIVO MODELO (no seu Windows)
# - Preenche no padrão Darwin Core usando biota_analise_consolidada
# - Exporta .xlsx na pasta de resultados
# =========================================================

print("\n--- Iniciando Bloco 13: Darwin Core (Modelo IEF Excel) ---")

import os
import re
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------
# 1) CONFIGURAÇÕES
# ---------------------------------------------------------
PROJETO_ALVO = "Diagnóstico Ouro Preto"
GRUPO_ALVO   = "Ictiofauna"

# ✅ IMPORTANTE: coloque aqui o caminho REAL do modelo no seu PC/Drive
# Exemplo (ajuste para onde você salvou o arquivo):
CAMINHO_MODELO = r"G:\Meu Drive\Opyta\Clientes\Clientes\Clientes\Geomil\OP Herculano\Planilhas e resultados\DarwinCore\Darwincore_biota_aquática_IEF_2025.xlsx"

# ✅ Pasta de saída que você definiu
CAMINHO_SAIDA = r"G:\Meu Drive\Opyta\Clientes\Clientes\Clientes\Geomil\OP Herculano\Planilhas e resultados\Resultados\4-Ictio\Campanha 1+2"

# ✅ Nome final "bonito" (não é o nome do modelo)
NOME_ARQUIVO_SAIDA = f"DarwinCore_{GRUPO_ALVO}_{re.sub(r'[^A-Za-z0-9]+','_',PROJETO_ALVO)}.xlsx"

PAIS = "Brazil"
ESTADO = "Minas Gerais"
INSTITUICAO = "Opyta"
BASIS_OF_RECORD = "HumanObservation"

# ---------------------------------------------------------
# 2) CONEXÃO COM BANCO
# ---------------------------------------------------------
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_port = os.getenv("DB_PORT", "5432")

if not all([db_user, db_password, db_host, db_name]):
    raise RuntimeError("❌ Variáveis de ambiente do banco não configuradas (DB_USER, DB_PASSWORD, DB_HOST, DB_NAME).")

print("🔌 Conectando ao banco...")
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# ---------------------------------------------------------
# 3) CARREGAR DADOS DO PROJETO/GRUPO
# ---------------------------------------------------------
print(f"📥 Extraindo dados: Projeto='{PROJETO_ALVO}' | Grupo='{GRUPO_ALVO}' ...")

sql = """
    SELECT *
    FROM public.biota_analise_consolidada
    WHERE nome_projeto = :projeto
      AND grupo_biologico = :grupo
"""
df = pd.read_sql_query(text(sql), engine, params={"projeto": PROJETO_ALVO, "grupo": GRUPO_ALVO})

if df.empty:
    raise RuntimeError("⚠️ Nenhum dado encontrado para o filtro informado.")

print(f"✅ Registros encontrados: {len(df)}")

# ---------------------------------------------------------
# 4) LER MODELO IEF (EXCEL)
# ---------------------------------------------------------
if not os.path.exists(CAMINHO_MODELO):
    raise FileNotFoundError(f"❌ Modelo não encontrado. Ajuste CAMINHO_MODELO:\n{CAMINHO_MODELO}")

print("📄 Carregando modelo oficial IEF...")

# tenta achar a aba principal (se existir mais de uma)
xlsx = pd.ExcelFile(CAMINHO_MODELO)
aba_escolhida = xlsx.sheet_names[0]  # padrão: primeira aba

# se houver uma aba com "darwin" no nome, preferir
for sh in xlsx.sheet_names:
    if "darwin" in sh.lower() or "dwc" in sh.lower():
        aba_escolhida = sh
        break

df_modelo = pd.read_excel(CAMINHO_MODELO, sheet_name=aba_escolhida)
colunas_modelo = df_modelo.columns.tolist()

print(f"✅ Modelo carregado. Aba utilizada: '{aba_escolhida}'")
print(f"✅ Nº colunas no modelo: {len(colunas_modelo)}")

# cria DF de saída vazio com o mesmo cabeçalho
df_saida = pd.DataFrame(columns=colunas_modelo)

# ---------------------------------------------------------
# 5) FUNÇÕES E MAPEAMENTO ROBUSTO
# ---------------------------------------------------------
def _safe_str(x):
    return "" if pd.isna(x) else str(x).strip()

def _iso_date(x):
    if pd.isna(x) or x is None:
        return ""
    t = pd.to_datetime(x, errors="coerce")
    if pd.isna(t):
        return ""
    # mantém data completa
    return t.strftime("%Y-%m-%d")

def _get_col(df_, *cands):
    for c in cands:
        if c in df_.columns:
            return c
    return None

# mapeia colunas existentes no seu banco
c_campanha = _get_col(df, "nome_campanha")
c_ponto    = _get_col(df, "nome_ponto")
c_data     = _get_col(df, "data_hora_coleta")
c_lat      = _get_col(df, "latitude")
c_lon      = _get_col(df, "longitude")
c_mun      = _get_col(df, "municipio")
c_curso    = _get_col(df, "curso_d_agua", "waterBody")
c_metodo   = _get_col(df, "metodo_de_captura")
c_esforco  = _get_col(df, "esforco")
c_un_esf   = _get_col(df, "unidade_esforco")

c_sci      = _get_col(df, "nome_cientifico")
c_reino    = _get_col(df, "reino")
c_filo     = _get_col(df, "filo")
c_classe   = _get_col(df, "classe")
c_ordem    = _get_col(df, "ordem")
c_familia  = _get_col(df, "familia")
c_genero   = _get_col(df, "genero")

c_count    = _get_col(df, "contagem", "numero_de_individuos")
c_biomassa = _get_col(df, "biomassa")

# helper: só seta se a coluna existir no MODELO
def _set_if_exists(dic, key, value):
    if key in colunas_modelo:
        dic[key] = value

# ---------------------------------------------------------
# 6) CONSTRUIR LINHAS (1 linha por registro do df)
# ---------------------------------------------------------
print("🔄 Preenchendo Darwin Core no padrão do modelo IEF...")

for _, r in df.iterrows():
    linha = {c: "" for c in colunas_modelo}

    # --- EVENTO / LOCALIDADE ---
    _set_if_exists(linha, "eventDate", _iso_date(r.get(c_data)) if c_data else "")
    _set_if_exists(linha, "decimalLatitude", r.get(c_lat) if c_lat else "")
    _set_if_exists(linha, "decimalLongitude", r.get(c_lon) if c_lon else "")
    _set_if_exists(linha, "country", PAIS)
    _set_if_exists(linha, "stateProvince", ESTADO)
    _set_if_exists(linha, "municipality", _safe_str(r.get(c_mun)) if c_mun else "")
    _set_if_exists(linha, "waterBody", _safe_str(r.get(c_curso)) if c_curso else "")
    _set_if_exists(linha, "verbatimLocality", _safe_str(r.get(c_ponto)) if c_ponto else "")
    _set_if_exists(linha, "samplingProtocol", _safe_str(r.get(c_metodo)) if c_metodo else "")

    # samplingEffort pode ser "esforco + unidade"
    esforco_txt = ""
    if c_esforco:
        esforco_txt = _safe_str(r.get(c_esforco))
        if c_un_esf:
            un = _safe_str(r.get(c_un_esf))
            if un:
                esforco_txt = f"{esforco_txt} {un}".strip()
    _set_if_exists(linha, "samplingEffort", esforco_txt)

    # --- TAXONOMIA ---
    _set_if_exists(linha, "scientificName", _safe_str(r.get(c_sci)) if c_sci else "")
    _set_if_exists(linha, "kingdom", _safe_str(r.get(c_reino)) if c_reino else "")
    _set_if_exists(linha, "phylum", _safe_str(r.get(c_filo)) if c_filo else "")
    _set_if_exists(linha, "class", _safe_str(r.get(c_classe)) if c_classe else "")
    _set_if_exists(linha, "order", _safe_str(r.get(c_ordem)) if c_ordem else "")
    _set_if_exists(linha, "family", _safe_str(r.get(c_familia)) if c_familia else "")
    _set_if_exists(linha, "genus", _safe_str(r.get(c_genero)) if c_genero else "")

    # --- OCORRÊNCIA ---
    _set_if_exists(linha, "basisOfRecord", BASIS_OF_RECORD)
    if c_count:
        val = pd.to_numeric(r.get(c_count), errors="coerce")
        _set_if_exists(linha, "individualCount", "" if pd.isna(val) else int(val))
    if c_biomassa:
        bio = pd.to_numeric(r.get(c_biomassa), errors="coerce")
        _set_if_exists(linha, "organismQuantity", "" if pd.isna(bio) else float(bio))
        _set_if_exists(linha, "organismQuantityType", "grams")

    _set_if_exists(linha, "institutionCode", INSTITUICAO)

    df_saida.loc[len(df_saida)] = linha

# ---------------------------------------------------------
# 7) EXPORTAR
# ---------------------------------------------------------
os.makedirs(CAMINHO_SAIDA, exist_ok=True)
caminho_final = os.path.join(CAMINHO_SAIDA, NOME_ARQUIVO_SAIDA)

df_saida.to_excel(caminho_final, index=False, engine="openpyxl")

print("-" * 60)
print("✅ GRAN FINALE CONCLUÍDO (Darwin Core IEF em Excel)")
print(f"📂 Arquivo salvo em:\n{caminho_final}")
print(f"📊 Total de linhas exportadas: {len(df_saida)}")
print("-" * 60)
print("--- Bloco 13 concluído ---")


# In[ ]:




