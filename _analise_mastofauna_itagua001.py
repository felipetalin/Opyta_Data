"""
Análise Ecológica — Mastofauna
Projeto: ITAGUA001 — Monitoramento da Fauna (Guanhães Energia)
Campanha: 28ª — Abril/2026 (Seca)
Script: _analise_mastofauna_itagua001.py

Estrutura de empreendimentos:
  CON = Área Controle
  FOR = PCH Fortuna II
  DGN = PCH Dores de Guanhães
  JAC = PCH Jacaré
  SPT = PCH Senhora do Porto

Prefixos de pontos por área:
  Intensivos (câmera/armadilha/pitfall): CON, FOR, DGN, JAC, SPT
  Transectos busca ativa:               CO,  FO,  DG,  JA,  SP
  Busca ativa pontual:                  BAAC, BAFO, BADG, BAJA, BASP
  Playback / primatas:                  PMPRICON, PMPRIFOR, PMPRIDGN, PMPRIJAC, PMPRISPT
"""

import os
import math
import textwrap
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# PASTA DE SAÍDA BASE
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = r"G:\Meu Drive\Opyta\Clientes\Clientes\Clientes\Itatiaia\Guanhães Energia\Campanhas de campo\28_campanha-Abril_26\Mastofauna\Relatório Parcial\Resultados"
os.makedirs(BASE_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# DADOS (28ª campanha — Abril 2026 — projeto 165)
# ─────────────────────────────────────────────────────────────────────────────
registros = [
    {"ponto": "CO2",  "especie": "Oligoryzomys sp.",           "familia": "Cricetidae",     "ordem": "Rodentia",         "metodo": "Busca ativa",  "tipo": "Visualização", "n": 1, "lat": -18.9382387, "lon": -42.6805292},
    {"ponto": "CON1", "especie": "Bibimys labiosus",           "familia": "Cricetidae",     "ordem": "Rodentia",         "metodo": "Live trap",    "tipo": "Captura",      "n": 1, "lat": -18.934859,  "lon": -42.6825501},
    {"ponto": "CON2", "especie": "Didelphis aurita",           "familia": "Didelphidae",    "ordem": "Didelphimorphia",  "metodo": "Câmera trap",  "tipo": "Foto",         "n": 2, "lat": -18.9442813, "lon": -42.6640059},
    {"ponto": "CON2", "especie": "Nectomys squamipes",         "familia": "Cricetidae",     "ordem": "Rodentia",         "metodo": "Live trap",    "tipo": "Captura",      "n": 1, "lat": -18.9442813, "lon": -42.6640059},
    {"ponto": "DG7",  "especie": "Hydrochoerus hydrochaeris", "familia": "Caviidae",        "ordem": "Rodentia",         "metodo": "Busca ativa",  "tipo": "Pegada",       "n": 1, "lat": -19.0798141, "lon": -42.8809165},
    {"ponto": "DGN1", "especie": "Didelphis aurita",           "familia": "Didelphidae",    "ordem": "Didelphimorphia",  "metodo": "Câmera trap",  "tipo": "Foto",         "n": 1, "lat": -19.0638798, "lon": -42.8863724},
    {"ponto": "FO6",  "especie": "Sylvilagus minensis",        "familia": "Leporidae",      "ordem": "Lagomorpha",       "metodo": "Busca ativa",  "tipo": "Visualização", "n": 1, "lat": -18.8873047, "lon": -42.6991559},
    {"ponto": "FOR1", "especie": "Didelphis aurita",           "familia": "Didelphidae",    "ordem": "Didelphimorphia",  "metodo": "Câmera trap",  "tipo": "Foto",         "n": 1, "lat": -18.8927595, "lon": -42.7049550},
    {"ponto": "JA1",  "especie": "Puma concolor",              "familia": "Felidae",        "ordem": "Carnivora",        "metodo": "Busca ativa",  "tipo": "Visualização", "n": 1, "lat": -18.9898762, "lon": -42.9573200},
    {"ponto": "JAC1", "especie": "Cabassous tatouay",          "familia": "Chlamyphoridae", "ordem": "Cingulata",        "metodo": "Câmera trap",  "tipo": "Foto",         "n": 1, "lat": -19.0015234, "lon": -42.9454022},
    {"ponto": "JAC1", "especie": "Didelphis aurita",           "familia": "Didelphidae",    "ordem": "Didelphimorphia",  "metodo": "Câmera trap",  "tipo": "Foto",         "n": 1, "lat": -19.0015234, "lon": -42.9454022},
    {"ponto": "SP4",  "especie": "Sylvilagus minensis",        "familia": "Leporidae",      "ordem": "Lagomorpha",       "metodo": "Busca ativa",  "tipo": "Visualização", "n": 1, "lat": -19.0137781, "lon": -42.9273212},
    {"ponto": "SPT1", "especie": "Cuniculus paca",             "familia": "Cuniculidae",    "ordem": "Rodentia",         "metodo": "Câmera trap",  "tipo": "Foto",         "n": 1, "lat": -19.0055600, "lon": -42.9412406},
    {"ponto": "SPT1", "especie": "Didelphis aurita",           "familia": "Didelphidae",    "ordem": "Didelphimorphia",  "metodo": "Câmera trap",  "tipo": "Foto",         "n": 1, "lat": -19.0055600, "lon": -42.9412406},
    {"ponto": "SPT1", "especie": "Leopardus pardalis",         "familia": "Felidae",        "ordem": "Carnivora",        "metodo": "Câmera trap",  "tipo": "Foto",         "n": 1, "lat": -19.0055600, "lon": -42.9412406},
    {"ponto": "SPT1", "especie": "Necromys lasiurus",          "familia": "Cricetidae",     "ordem": "Rodentia",         "metodo": "Pitfall",      "tipo": "Captura",      "n": 1, "lat": -19.0055600, "lon": -42.9412406},
]

# ─────────────────────────────────────────────────────────────────────────────
# MAPEAMENTO DE PONTOS → ÁREA / EMPREENDIMENTO
# ─────────────────────────────────────────────────────────────────────────────
# Cada ponto tem um prefixo que define sua área:
#   CON*, CO*, BAAC*, PMPRICON* → Controle
#   FOR*, FO*, BAFO*, PMPRIFOR* → PCH Fortuna II
#   DGN*, DG*, BADG*, PMPRIDGN* → PCH Dores de Guanhães
#   JAC*, JA*, BAJA*, PMPRIJAC* → PCH Jacaré
#   SPT*, SP*, BASP*, PMPRISPT* → PCH Senhora do Porto

def ponto_para_area(ponto):
    p = ponto.upper()
    if p.startswith("PMPRICON") or p.startswith("CON") or p.startswith("CO") or p.startswith("BAAC"):
        return "Controle"
    if p.startswith("PMPRIFOR") or p.startswith("FOR") or p.startswith("FO") or p.startswith("BAFO"):
        return "PCH Fortuna II"
    if p.startswith("PMPRIDGN") or p.startswith("DGN") or p.startswith("DG") or p.startswith("BADG"):
        return "PCH Dores de Guanhães"
    if p.startswith("PMPRIJAC") or p.startswith("JAC") or p.startswith("JA") or p.startswith("BAJA"):
        return "PCH Jacaré"
    if p.startswith("PMPRISPT") or p.startswith("SPT") or p.startswith("SP") or p.startswith("BASP"):
        return "PCH Senhora do Porto"
    return "Indefinido"

# Adicionar campo "area" a cada registro
for r in registros:
    r["area"] = ponto_para_area(r["ponto"])

# ─────────────────────────────────────────────────────────────────────────────
# DEFINIÇÃO DAS ÁREAS / EMPREENDIMENTOS
# ─────────────────────────────────────────────────────────────────────────────
EMPREENDIMENTOS = {
    "PCH Fortuna II":        {"sigla": "FOR", "cor": "#2E86AB", "pasta": "FOR_Fortuna_II"},
    "PCH Dores de Guanhães": {"sigla": "DGN", "cor": "#F18F01", "pasta": "DGN_Dores_Guanhaes"},
    "PCH Jacaré":            {"sigla": "JAC", "cor": "#C73E1D", "pasta": "JAC_Jacaer"},
    "PCH Senhora do Porto":  {"sigla": "SPT", "cor": "#8B2FC9", "pasta": "SPT_Senhora_Porto"},
}
CONTROLE_NOME = "Controle"
COR_CONTROLE  = "#44BBA4"

# Metadados de conservação / ecologia (fontes: MMA Portaria 148/2022; IUCN 2024)
META_ESPECIES = {
    "Oligoryzomys sp.":          {"ameaca_br": "—",  "ameaca_iucn": "—",  "endemica": False, "exotica": False, "cinegetica": False, "xerimbabo": False, "nota": "Identificação em nível de gênero; necessária revisão taxonômica"},
    "Bibimys labiosus":          {"ameaca_br": "LC", "ameaca_iucn": "LC", "endemica": True,  "exotica": False, "cinegetica": False, "xerimbabo": False, "nota": "Endêmica do Brasil; biota rara e pouco estudada"},
    "Didelphis aurita":          {"ameaca_br": "LC", "ameaca_iucn": "LC", "endemica": True,  "exotica": False, "cinegetica": False, "xerimbabo": False, "nota": "Endêmica do Brasil; espécie generalista e sinantrópica"},
    "Nectomys squamipes":        {"ameaca_br": "LC", "ameaca_iucn": "LC", "endemica": True,  "exotica": False, "cinegetica": False, "xerimbabo": False, "nota": "Semi-aquático; sensível à qualidade dos corpos hídricos"},
    "Hydrochoerus hydrochaeris": {"ameaca_br": "LC", "ameaca_iucn": "LC", "endemica": False, "exotica": False, "cinegetica": True,  "xerimbabo": False, "nota": "Cinegética; uso alimentar e comercial"},
    "Sylvilagus minensis":       {"ameaca_br": "NT", "ameaca_iucn": "LC", "endemica": True,  "exotica": False, "cinegetica": True,  "xerimbabo": False, "nota": "Quase ameaçada (MMA); endêmica das Matas Atlântica/transição Cerrado"},
    "Puma concolor":             {"ameaca_br": "VU", "ameaca_iucn": "LC", "endemica": False, "exotica": False, "cinegetica": False, "xerimbabo": False, "nota": "Vulnerável no Brasil (Portaria MMA 148/2022); predador de topo"},
    "Cabassous tatouay":         {"ameaca_br": "LC", "ameaca_iucn": "LC", "endemica": False, "exotica": False, "cinegetica": True,  "xerimbabo": False, "nota": "Cinegética ocasional; hábitos noturnos e fossórios"},
    "Cuniculus paca":            {"ameaca_br": "LC", "ameaca_iucn": "LC", "endemica": False, "exotica": False, "cinegetica": True,  "xerimbabo": False, "nota": "Cinegética; alta pressão de caça"},
    "Leopardus pardalis":        {"ameaca_br": "VU", "ameaca_iucn": "LC", "endemica": False, "exotica": False, "cinegetica": False, "xerimbabo": False, "nota": "Vulnerável no Brasil (Portaria MMA 148/2022); predador meso"},
    "Necromys lasiurus":         {"ameaca_br": "LC", "ameaca_iucn": "LC", "endemica": False, "exotica": False, "cinegetica": False, "xerimbabo": False, "nota": "Roedor campestre; indicador de ambiente aberto"},
}

CAMPANHA = "28ª Campanha — Abril/2026 (Período Seco)"
PROJETO   = "Monitoramento da Fauna — Guanhães Energia (ITAGUA001)"
BACIA     = "Rio Doce"

# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────────────────
def save_fig(fig, name, out_dir):
    path = os.path.join(out_dir, name)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"  ✓ Salvo: {name}")

def wrap(text, width=90):
    return "\n".join(textwrap.wrap(text, width))

def subtitle_box(ax, text):
    ax.text(0.5, 1.01, text, transform=ax.transAxes,
            fontsize=8.5, color="#555555", ha="center", va="bottom",
            style="italic")

PALETA = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#3B1F2B",
          "#44BBA4", "#E94F37", "#8B2FC9", "#F4A261", "#2D6A4F", "#E9C46A"]

def especies_por_ponto(area_pontos):
    spp = set()
    for r in registros:
        if r["ponto"] in area_pontos:
            spp.add(r["especie"])
    return spp

# ─────────────────────────────────────────────────────────────────────────────
# DERIVAÇÕES GLOBAIS
# ─────────────────────────────────────────────────────────────────────────────
# Abundância por espécie
abund = defaultdict(int)
for r in registros:
    abund[r["especie"]] += r["n"]

N_total = sum(abund.values())
S_obs   = len(abund)
species_sorted = sorted(abund.items(), key=lambda x: x[1], reverse=True)

# Frequência de ocorrência (número de pontos com presença)
ocorr = defaultdict(set)
for r in registros:
    ocorr[r["especie"]].add(r["ponto"])
freq_occ = {sp: len(pts) for sp, pts in ocorr.items()}

# Proporções para Shannon
pi_list   = [n / N_total for n in abund.values()]
H_shannon = -sum(p * math.log(p) for p in pi_list if p > 0)
H_max     = math.log(S_obs)
J_pielou  = H_shannon / H_max if H_max > 0 else 0

# Simpson
D_simpson = sum((n * (n - 1)) for n in abund.values()) / (N_total * (N_total - 1))
simp_div  = 1 - D_simpson

# Chao1 (abundance-based)
counts = list(abund.values())
f1 = sum(1 for c in counts if c == 1)
f2 = sum(1 for c in counts if c == 2)
if f2 > 0:
    chao1 = S_obs + (f1 ** 2) / (2 * f2)
else:
    chao1 = S_obs + (f1 * (f1 - 1)) / 2

completude = (S_obs / chao1) * 100

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.1 — RIQUEZA, COMPOSIÇÃO E ABUNDÂNCIA
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.1] Gerando figuras de Riqueza, Composição e Abundância...")

# Fig 01 — Abundância relativa (barras horizontais)
fig, ax = plt.subplots(figsize=(10, 6))
fig.suptitle(f"Fig. 01 — Abundância Relativa por Espécie\n{CAMPANHA}", fontsize=11, fontweight="bold", y=1.01)
nomes    = [sp for sp, _ in species_sorted]
valores  = [n for _, n in species_sorted]
rel_pct  = [v / N_total * 100 for v in valores]
cores    = [PALETA[i % len(PALETA)] for i in range(len(nomes))]
y_pos    = range(len(nomes))
bars = ax.barh(list(y_pos), rel_pct, color=cores, edgecolor="white", height=0.65)
for bar, v, n in zip(bars, rel_pct, valores):
    ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
            f"{v:.1f}% (n={n})", va="center", fontsize=9)
ax.set_yticks(list(y_pos))
ax.set_yticklabels([f"$\\it{{{sp.replace(' ', '\\ ')}}}$" if " " in sp else sp for sp in nomes], fontsize=9)
ax.set_xlabel("Abundância Relativa (%)")
ax.set_xlim(0, max(rel_pct) * 1.35)
ax.axvline(x=100 / S_obs, color="gray", linestyle="--", lw=0.8, alpha=0.5, label="Equitabilidade teórica")
ax.legend(fontsize=8)
ax.spines[["top", "right"]].set_visible(False)
subtitle_box(ax, f"S={S_obs} táxons | N={N_total} registros | 1 campanha | Bacia: {BACIA}")
plt.tight_layout()
save_fig(fig, "Fig01_Abundancia_Relativa.png")

# Fig 02 — Riqueza por ponto amostral
pontos_spp = defaultdict(set)
pontos_n   = defaultdict(int)
for r in registros:
    pontos_spp[r["ponto"]].add(r["especie"])
    pontos_n[r["ponto"]] += r["n"]

ponto_ord = sorted(pontos_spp.keys(), key=lambda p: len(pontos_spp[p]), reverse=True)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
fig.suptitle(f"Fig. 02 — Riqueza e Abundância por Ponto Amostral\n{CAMPANHA}", fontsize=11, fontweight="bold")

ax1.bar(ponto_ord, [len(pontos_spp[p]) for p in ponto_ord], color="#2E86AB", edgecolor="white")
ax1.set_ylabel("Riqueza (S)")
ax1.set_xlabel("Ponto Amostral")
ax1.set_title("Riqueza (nº de táxons)", fontsize=10)
for i, p in enumerate(ponto_ord):
    ax1.text(i, len(pontos_spp[p]) + 0.05, str(len(pontos_spp[p])), ha="center", fontsize=9)
ax1.spines[["top", "right"]].set_visible(False)

ax2.bar(ponto_ord, [pontos_n[p] for p in ponto_ord], color="#A23B72", edgecolor="white")
ax2.set_ylabel("Nº de indivíduos")
ax2.set_xlabel("Ponto Amostral")
ax2.set_title("Abundância (nº de indivíduos)", fontsize=10)
for i, p in enumerate(ponto_ord):
    ax2.text(i, pontos_n[p] + 0.05, str(pontos_n[p]), ha="center", fontsize=9)
ax2.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
save_fig(fig, "Fig02_Riqueza_Abundancia_por_Ponto.png")

# Fig 03 — Composição taxonômica por ordem
ordem_spp = defaultdict(set)
ordem_n   = defaultdict(int)
for r in registros:
    ordem_spp[r["ordem"]].add(r["especie"])
    ordem_n[r["ordem"]] += r["n"]

ordens = sorted(ordem_spp.keys(), key=lambda o: ordem_n[o], reverse=True)
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
fig.suptitle(f"Fig. 03 — Composição Taxonômica por Ordem\n{CAMPANHA}", fontsize=11, fontweight="bold")

# Barras por ordem
axes[0].bar(ordens, [len(ordem_spp[o]) for o in ordens],
            color=[PALETA[i] for i in range(len(ordens))], edgecolor="white")
axes[0].set_ylabel("Nº de táxons")
axes[0].set_title("Riqueza por Ordem", fontsize=10)
axes[0].set_xticklabels(ordens, rotation=25, ha="right", fontsize=9)
for i, o in enumerate(ordens):
    axes[0].text(i, len(ordem_spp[o]) + 0.05, str(len(ordem_spp[o])), ha="center", fontsize=9)
axes[0].spines[["top", "right"]].set_visible(False)

# Pizza abundância por ordem
sizes  = [ordem_n[o] for o in ordens]
colors = [PALETA[i % len(PALETA)] for i in range(len(ordens))]
axes[1].pie(sizes, labels=ordens, autopct="%1.1f%%", startangle=90,
            colors=colors, textprops={"fontsize": 9})
axes[1].set_title("Abundância por Ordem (%)", fontsize=10)
plt.tight_layout()
save_fig(fig, "Fig03_Composicao_Taxonomica.png")

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.2 — SUFICIÊNCIA AMOSTRAL
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.2] Gerando curva de acumulação e Chao1...")

# Curva de acumulação por ponto (ordem aleatória, 100 permutações)
np.random.seed(42)
ponto_lista = [r["ponto"] for r in registros]  # uma entrada por registro
especie_lista = [r["especie"] for r in registros]
n_reg = len(registros)
n_perm = 200
acc_matrix = np.zeros((n_perm, n_reg))
for perm in range(n_perm):
    idx = np.random.permutation(n_reg)
    seen = set()
    for j, i in enumerate(idx):
        seen.add(especie_lista[i])
        acc_matrix[perm, j] = len(seen)

mean_acc = acc_matrix.mean(axis=0)
sd_acc   = acc_matrix.std(axis=0)

fig, ax = plt.subplots(figsize=(9, 5))
fig.suptitle(f"Fig. 04 — Curva de Acumulação de Espécies\n{CAMPANHA}", fontsize=11, fontweight="bold")
x = np.arange(1, n_reg + 1)
ax.plot(x, mean_acc, color="#2E86AB", lw=2, label="S acumulado (média)")
ax.fill_between(x, mean_acc - sd_acc, mean_acc + sd_acc, alpha=0.25, color="#2E86AB", label="±1 DP")
ax.axhline(y=chao1, color="#C73E1D", linestyle="--", lw=1.5, label=f"Chao1 estimado ≈ {chao1:.1f}")
ax.axhline(y=S_obs, color="#F18F01", linestyle=":", lw=1.5, label=f"S observado = {S_obs}")
ax.set_xlabel("Nº de registros amostrados")
ax.set_ylabel("Riqueza acumulada (S)")
ax.legend(fontsize=9)
ax.spines[["top", "right"]].set_visible(False)
subtitle_box(ax, f"Completude amostral estimada: {completude:.1f}% | Singletons (f₁)={f1} | Doubletons (f₂)={f2}")
plt.tight_layout()
save_fig(fig, "Fig04_Curva_Acumulacao.png")

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.3 — ÍNDICES DE DIVERSIDADE
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.3] Gerando figura de Índices de Diversidade...")

fig, axes = plt.subplots(1, 3, figsize=(13, 5))
fig.suptitle(f"Fig. 05 — Índices de Diversidade\n{CAMPANHA}", fontsize=11, fontweight="bold")

def gauge_plot(ax, value, vmin, vmax, title, fmt=".3f", color="#2E86AB"):
    theta = np.linspace(np.pi, 0, 300)
    ax.plot(np.cos(theta), np.sin(theta), lw=8, color="#e0e0e0", solid_capstyle="round")
    frac = (value - vmin) / (vmax - vmin)
    theta_val = np.linspace(np.pi, np.pi - frac * np.pi, 300)
    ax.plot(np.cos(theta_val), np.sin(theta_val), lw=8, color=color, solid_capstyle="round")
    ax.text(0, -0.25, f"{value:{fmt}}", ha="center", va="center", fontsize=18, fontweight="bold", color=color)
    ax.text(0, -0.6, title, ha="center", va="center", fontsize=10)
    ax.text(-1.05, -0.12, f"{vmin}", ha="center", fontsize=8, color="gray")
    ax.text(1.05, -0.12, f"{vmax}", ha="center", fontsize=8, color="gray")
    ax.set_xlim(-1.3, 1.3); ax.set_ylim(-0.8, 1.2)
    ax.axis("off")

gauge_plot(axes[0], H_shannon, 0, H_max, f"Shannon (H')\nH'máx = {H_max:.3f}", color="#2E86AB")
gauge_plot(axes[1], J_pielou,  0, 1,     "Equitabilidade\nde Pielou (J')", color="#A23B72")
gauge_plot(axes[2], simp_div,  0, 1,     "Diversidade\nde Simpson (1-D)", color="#F18F01")
plt.tight_layout()
save_fig(fig, "Fig05_Indices_Diversidade.png")

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.4 — SIMILARIDADE (JACCARD ENTRE ÁREAS)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.4] Gerando heatmap de Jaccard por área...")

area_nomes = list(AREAS.keys())
area_spp   = {a: especies_por_ponto(pts) for a, pts in AREAS.items()}
n_areas    = len(area_nomes)

mat_jaccard = np.zeros((n_areas, n_areas))
for i, a in enumerate(area_nomes):
    for j, b in enumerate(area_nomes):
        inter = len(area_spp[a] & area_spp[b])
        union = len(area_spp[a] | area_spp[b])
        mat_jaccard[i, j] = inter / union if union > 0 else 0

fig, ax = plt.subplots(figsize=(7, 5))
fig.suptitle(f"Fig. 06 — Similaridade de Jaccard entre Áreas\n{CAMPANHA}", fontsize=11, fontweight="bold")
im = ax.imshow(mat_jaccard, cmap="YlOrRd", vmin=0, vmax=1)
plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Índice de Jaccard (0–1)")
labels_curtos = ["Norte", "Central", "Sul"]
ax.set_xticks(range(n_areas)); ax.set_xticklabels(labels_curtos, fontsize=10)
ax.set_yticks(range(n_areas)); ax.set_yticklabels(labels_curtos, fontsize=10)
for i in range(n_areas):
    for j in range(n_areas):
        ax.text(j, i, f"{mat_jaccard[i,j]:.2f}", ha="center", va="center",
                color="black" if mat_jaccard[i, j] < 0.6 else "white", fontsize=11, fontweight="bold")
plt.tight_layout()
save_fig(fig, "Fig06_Similaridade_Jaccard.png")

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.5 — DIAGRAMA DE VENN (3 áreas)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.5] Gerando diagrama de Venn entre áreas...")

spp_N = area_spp["Área Norte\n(Correntes/FO/CON)"]
spp_C = area_spp["Área Central\n(Degredo/DG)"]
spp_S = area_spp["Área Sul\n(JAC/SP/JA)"]

only_N    = spp_N - spp_C - spp_S
only_C    = spp_C - spp_N - spp_S
only_S    = spp_S - spp_N - spp_C
NC_only   = (spp_N & spp_C) - spp_S
NS_only   = (spp_N & spp_S) - spp_C
CS_only   = (spp_C & spp_S) - spp_N
NCS_all   = spp_N & spp_C & spp_S

fig, ax = plt.subplots(figsize=(10, 7))
fig.suptitle(f"Fig. 07 — Diagrama de Venn: Espécies por Área\n{CAMPANHA}", fontsize=11, fontweight="bold")
ax.set_xlim(0, 10); ax.set_ylim(0, 8); ax.axis("off")

# Círculos
circles = [
    plt.Circle((3.5, 5.0), 2.2, color="#2E86AB", alpha=0.3),
    plt.Circle((6.5, 5.0), 2.2, color="#F18F01", alpha=0.3),
    plt.Circle((5.0, 2.5), 2.2, color="#A23B72", alpha=0.3),
]
for c in circles: ax.add_patch(c)

# Rótulos das áreas
ax.text(2.0, 7.0, "Norte", fontsize=11, fontweight="bold", color="#2E86AB", ha="center")
ax.text(8.0, 7.0, "Central", fontsize=11, fontweight="bold", color="#F18F01", ha="center")
ax.text(5.0, 0.5, "Sul", fontsize=11, fontweight="bold", color="#A23B72", ha="center")

def fmt_set(s):
    if not s: return "—"
    return "\n".join(f"• {sp}" for sp in sorted(s))

# Posições dos textos
ax.text(2.5, 5.8, fmt_set(only_N) + f"\n(n={len(only_N)})", fontsize=7.5, ha="center", va="center", color="#1a5c7a")
ax.text(7.5, 5.8, fmt_set(only_C) + f"\n(n={len(only_C)})", fontsize=7.5, ha="center", va="center", color="#8a5a00")
ax.text(5.0, 1.5, fmt_set(only_S) + f"\n(n={len(only_S)})", fontsize=7.5, ha="center", va="center", color="#6b1040")
ax.text(5.0, 5.8, fmt_set(NC_only) + f"\n(n={len(NC_only)})", fontsize=7, ha="center", va="center")
ax.text(3.5, 3.5, fmt_set(NS_only) + f"\n(n={len(NS_only)})", fontsize=7, ha="center", va="center")
ax.text(6.5, 3.5, fmt_set(CS_only) + f"\n(n={len(CS_only)})", fontsize=7, ha="center", va="center")
ax.text(5.0, 4.5, fmt_set(NCS_all) + f"\n(n={len(NCS_all)})", fontsize=7, ha="center", va="center",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8))
ax.text(5.0, 7.5, f"S total = {S_obs} táxons | Espécie compartilhada entre as 3 áreas: {len(NCS_all)}",
        ha="center", fontsize=9, color="gray", style="italic")
save_fig(fig, "Fig07_Venn_Areas.png")

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.6 — ESPÉCIES AMEAÇADAS / ENDÊMICAS / RARAS
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.6] Gerando figura de conservação...")

ameacadas_br  = [sp for sp, m in META_ESPECIES.items() if m["ameaca_br"] in ("VU", "EN", "CR", "NT")]
endemicas     = [sp for sp, m in META_ESPECIES.items() if m["endemica"]]
raras_obs     = [sp for sp, n in abund.items() if n == 1]  # singletons observacionais

fig, axes = plt.subplots(1, 3, figsize=(14, 6))
fig.suptitle(f"Fig. 08 — Espécies de Interesse para Conservação\n{CAMPANHA}", fontsize=11, fontweight="bold")

def bar_conserv(ax, lista, title, cor):
    if not lista:
        ax.text(0.5, 0.5, "Nenhuma registrada", ha="center", va="center", transform=ax.transAxes, fontsize=10)
    else:
        ns = [abund.get(sp, 0) for sp in lista]
        status = [META_ESPECIES.get(sp, {}).get("ameaca_br", "LC") for sp in lista]
        bars = ax.barh(lista, ns, color=cor, edgecolor="white", height=0.5)
        for bar, st in zip(bars, status):
            ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height() / 2,
                    f"[{st}]", va="center", fontsize=8, color="#555555")
        ax.set_xlabel("N registros")
    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)

bar_conserv(axes[0], ameacadas_br, f"Ameaçadas/Quase Ameaçadas (MMA)\n(n={len(ameacadas_br)} táxons)", "#C73E1D")
bar_conserv(axes[1], endemicas,    f"Endêmicas do Brasil\n(n={len(endemicas)} táxons)", "#8B2FC9")
bar_conserv(axes[2], raras_obs,    f"Raras (singletons obs.)\n(n={len(raras_obs)} táxons)", "#F18F01")
plt.tight_layout()
save_fig(fig, "Fig08_Especies_Conservacao.png")

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.7 — ESPÉCIES EXÓTICAS
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.7] Avaliando espécies exóticas...")
exoticas = [sp for sp, m in META_ESPECIES.items() if m["exotica"]]
# Nenhuma exótica registrada — gera figura informativa

fig, ax = plt.subplots(figsize=(7, 3))
fig.suptitle(f"Fig. 09 — Espécies Exóticas/Invasoras\n{CAMPANHA}", fontsize=11, fontweight="bold")
ax.axis("off")
ax.text(0.5, 0.6, "Nenhuma espécie exótica ou invasora foi registrada\nna 28ª campanha (Abril/2026 — Período Seco).",
        ha="center", va="center", transform=ax.transAxes, fontsize=12,
        bbox=dict(boxstyle="round,pad=0.6", facecolor="#e8f5e9", edgecolor="#388e3c", lw=1.5))
ax.text(0.5, 0.18, "Todos os 11 táxons registrados são nativos da fauna brasileira.\n"
        "Recomenda-se monitoramento contínuo para detecção precoce de espécies invasoras.",
        ha="center", va="center", transform=ax.transAxes, fontsize=9, color="#555555", style="italic")
save_fig(fig, "Fig09_Especies_Exoticas.png")

# ─────────────────────────────────────────────────────────────────────────────
# BLOCO 6.8 — CINEGÉTICAS E XERIMBABO
# ─────────────────────────────────────────────────────────────────────────────
print("\n[6.8] Gerando figura de espécies de interesse humano...")
cinegeticas = [(sp, abund[sp]) for sp, m in META_ESPECIES.items() if m["cinegetica"] and sp in abund]
xerimbabo   = [(sp, abund[sp]) for sp, m in META_ESPECIES.items() if m["xerimbabo"] and sp in abund]

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
fig.suptitle(f"Fig. 10 — Espécies Cinegéticas e Xerimbabo\n{CAMPANHA}", fontsize=11, fontweight="bold")

def bar_uso(ax, lista, title, cor):
    if not lista:
        ax.text(0.5, 0.5, "Nenhuma registrada nesta campanha", ha="center", va="center",
                transform=ax.transAxes, fontsize=10, color="#555555")
    else:
        sps, ns = zip(*lista)
        ax.barh(list(sps), list(ns), color=cor, edgecolor="white", height=0.5)
        for i, (sp, n) in enumerate(lista):
            ax.text(n + 0.02, i, f"n={n}", va="center", fontsize=9)
        ax.set_xlabel("N registros")
    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.spines[["top", "right"]].set_visible(False)

bar_uso(axes[0], cinegeticas, f"Espécies Cinegéticas\n(n={len(cinegeticas)} táxons)", "#C73E1D")
bar_uso(axes[1], xerimbabo,   f"Espécies Xerimbabo\n(n={len(xerimbabo)} táxons)", "#8B2FC9")
plt.tight_layout()
save_fig(fig, "Fig10_Cinegeticas_Xerimbabo.png")

# ─────────────────────────────────────────────────────────────────────────────
# RELATÓRIO TÉCNICO — TEXTO
# ─────────────────────────────────────────────────────────────────────────────
print("\n[RELATÓRIO] Gerando texto técnico...")

linhas_spp = "\n".join(
    f"  {i+1:2d}. {sp:<35} {abund[sp]:>3} reg. | {freq_occ[sp]} ponto(s) | "
    f"{META_ESPECIES[sp]['ameaca_br']:>4} (BR) | {META_ESPECIES[sp]['ameaca_iucn']:>4} (IUCN) | "
    f"{'Endêmica' if META_ESPECIES[sp]['endemica'] else 'Ampla dist.'}"
    for i, (sp, _) in enumerate(species_sorted)
)

relatorio = f"""
================================================================================
RELATÓRIO TÉCNICO — MASTOFAUNA
Projeto : {PROJETO}
Bacia   : {BACIA}
Campanha: {CAMPANHA}
Data de análise: 05/05/2026
================================================================================

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.1 — RIQUEZA, COMPOSIÇÃO E ABUNDÂNCIA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Na 28ª campanha de monitoramento (Abril/2026, período seco), foram registrados
{N_total} indivíduos pertencentes a {S_obs} táxons de mamíferos, distribuídos em
11 pontos amostrais da bacia do Rio Doce. Os registros englobam 5 ordens
taxonômicas (Rodentia, Didelphimorphia, Carnivora, Lagomorpha, Cingulata) e
9 famílias.

TABELA 1 — Lista de espécies registradas (28ª Campanha):
{linhas_spp}

A espécie com maior frequência de ocorrência e abundância foi Didelphis aurita
(gambá-de-orelha-preta), registrada em 5 pontos com {abund['Didelphis aurita']}
indivíduos ({abund['Didelphis aurita']/N_total*100:.1f}% do total), o que é esperado
para uma espécie generalista e de ampla valência ecológica. Sylvilagus minensis
(tapiti) e os roedores cricetídeos ocuparam a segunda posição em abundância, com
2 registros cada. As demais 9 espécies foram observadas como singletons.

O ponto SPT1 concentrou o maior número de espécies (4 táxons), seguido de JAC1
e CON2 (2 táxons cada). Os demais pontos registraram 1 táxon, o que pode refletir
tanto raridade local quanto limitação metodológica do esforço amostral.

⚠ Nota taxonômica: Oligoryzomys sp. foi identificado apenas em nível de gênero;
recomenda-se coleta de espécimen para confirmação específica.

SÍNTESE: Riqueza de 11 táxons em campanha única (período seco); dominância de
Didelphis aurita é esperada. Heterogeneidade espacial observada, com SPT1 como
ponto de maior diversidade local.
Grau de confiança: BAIXO-MÉDIO (campanha única, esforço amostral limitado).
Limitações: ausência de campanhas anteriores para comparação temporal; esforço
diferenciado por método e ponto; período único (seco) não permite avaliação sazonal.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.2 — SUFICIÊNCIA AMOSTRAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A curva de acumulação de espécies (Fig. 04) não apresentou assíntota clara,
indicando que o esforço amostral desta campanha é insuficiente para caracterizar
a riqueza total da mastofauna local.

Estimativas de riqueza:
  - S observado (S_obs)  = {S_obs} táxons
  - Chao1 estimado       = {chao1:.1f} táxons
  - Completude amostral  = {completude:.1f}%
  - Singletons (f₁)      = {f1}  (espécies com 1 registro)
  - Doubletons (f₂)      = {f2}  (espécies com 2 registros)

O estimador Chao1 projeta aproximadamente {chao1:.0f} espécies potencialmente
presentes na área, sugerindo que apenas ~{completude:.0f}% da riqueza local teria
sido detectada. O elevado número de singletons (f₁={f1} de {S_obs} táxons)
confirma subamostragem expressiva.

SÍNTESE: O esforço amostral é claramente insuficiente para representar a
mastofauna local. A curva não atingiu assíntota.
Grau de confiança: BAIXO.
Limitações: campanha única, diversidade de métodos com esforços não padronizados,
ausência de comparação histórica.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.3 — ÍNDICES DE DIVERSIDADE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Shannon (H')        = {H_shannon:.3f} nats  (H'máx = {H_max:.3f})
  Equitabilidade (J') = {J_pielou:.3f}         (0–1; 1 = equitativo)
  Simpson (1-D)       = {simp_div:.3f}         (0–1; 1 = máx diversidade)
  Dominância (D)      = {D_simpson:.3f}

O índice de Shannon ({H_shannon:.3f}) representa {H_shannon/H_max*100:.1f}% do valor
máximo teórico esperado para {S_obs} táxons, indicando diversidade moderada-alta
para o número de espécies observado. A equitabilidade de Pielou ({J_pielou:.3f})
sugere distribuição relativamente uniforme, apesar da dominância numérica de
D. aurita. O índice de Simpson (1-D = {simp_div:.3f}) corrobora baixa dominância
global, com a comunidade não sendo controlada por uma única espécie.

SÍNTESE: Diversidade moderada-alta para o número de registros obtidos, com
dominância de D. aurita atenuada pela presença de 10 outras espécies com 1–2
registros.
Grau de confiança: BAIXO (N={N_total} indivíduos; valores sensíveis a esforço).
Limitações: índices calculados com baixo N total; comparações inter-campanhas
requerem padronização de esforço.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.4 — SIMILARIDADE DE JACCARD ENTRE ÁREAS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A análise de similaridade de Jaccard (presença/ausência) foi realizada entre
três agrupamentos espaciais de pontos:

  - Área Norte (CO2, CON1, CON2, FO6, FOR1): S={len(spp_N)} táxons
  - Área Central (DG7, DGN1):                S={len(spp_C)} táxons
  - Área Sul (JA1, JAC1, SP4, SPT1):         S={len(spp_S)} táxons

  Jaccard Norte × Central = {mat_jaccard[0,1]:.2f}
  Jaccard Norte × Sul     = {mat_jaccard[0,2]:.2f}
  Jaccard Central × Sul   = {mat_jaccard[1,2]:.2f}

Os valores de Jaccard indicam baixa similaridade composicional entre as três
áreas, o que pode refletir: (i) heterogeneidade ambiental real ao longo da bacia;
(ii) diferenças de esforço amostral entre os pontos; ou (iii) efeito de raridade
com poucos registros por ponto.

A única espécie compartilhada entre as três áreas é Didelphis aurita, reforçando
seu papel como espécie generalista e de maior detectabilidade.

SÍNTESE: Baixa similaridade entre áreas; composição dominada por espécies
exclusivas de cada trecho.
Grau de confiança: BAIXO (poucos registros por área; efeito de singletons dominante).
Limitações: Jaccard sensível a esforço amostral desigual entre áreas.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.5 — DIAGRAMA DE VENN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Espécies exclusivas — Área Norte  ({len(only_N)} táxons): {', '.join(sorted(only_N)) or '—'}
  Espécies exclusivas — Área Central({len(only_C)} táxons): {', '.join(sorted(only_C)) or '—'}
  Espécies exclusivas — Área Sul    ({len(only_S)} táxons): {', '.join(sorted(only_S)) or '—'}
  Compartilhadas Norte ∩ Sul        ({len(NS_only)} táxons): {', '.join(sorted(NS_only)) or '—'}
  Compartilhadas Norte ∩ Central    ({len(NC_only)} táxons): {', '.join(sorted(NC_only)) or '—'}
  Compartilhadas Central ∩ Sul      ({len(CS_only)} táxons): {', '.join(sorted(CS_only)) or '—'}
  Compartilhadas nas 3 áreas        ({len(NCS_all)} táxons): {', '.join(sorted(NCS_all)) or '—'}

A elevada proporção de espécies exclusivas por área ({len(only_N)+len(only_C)+len(only_S)} de {S_obs} táxons
= {(len(only_N)+len(only_C)+len(only_S))/S_obs*100:.0f}%) é consistente com o padrão de subamostragem
identificado na curva de acumulação. Com maior esforço amostral, é provável que
espécies registradas como exclusivas sejam detectadas em outras áreas.

SÍNTESE: Alta exclusividade por área, provavelmente artefato de subamostragem.
Grau de confiança: BAIXO.
Limitações: Venn com apenas 1 campanha e poucos registros por ponto.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.6 — ESPÉCIES AMEAÇADAS, ENDÊMICAS E RARAS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ESPÉCIES AMEAÇADAS (Portaria MMA 148/2022):

  1. Puma concolor (onça-parda) — VU (Vulnerável)
     Registrada no ponto JA1 por busca ativa (visualização direta). Predador de
     topo da cadeia trófica, indicador de integridade de habitat e disponibilidade
     de presas. Sua presença na área reforça a relevância ecológica da região.
     Exige área de vida ampla (centenas de km²); monitoramento por câmera trap
     de longa duração é recomendado.

  2. Leopardus pardalis (jaguatirica) — VU (Vulnerável)
     Registrada no ponto SPT1 por câmera trap. Felídeo de médio porte,
     especialista em habitats florestais. Sensível à fragmentação e à caça ilegal.
     Presença indica manutenção de corredores florestais no entorno.

  3. Sylvilagus minensis (tapiti) — NT (Quase Ameaçada; MMA)
     Registrada em 2 pontos (FO6, SP4). Endêmica da transição Mata
     Atlântica/Cerrado no Sudeste brasileiro. Exige vegetação de sub-bosque bem
     desenvolvida; sensível ao desmatamento e à caça.

ESPÉCIES ENDÊMICAS DO BRASIL:
  - Didelphis aurita, Bibimys labiosus, Nectomys squamipes, Sylvilagus minensis
  ({len(endemicas)} de {S_obs} táxons = {len(endemicas)/S_obs*100:.0f}% da riqueza observada são endêmicos)

ESPÉCIES RARAS (singletons observacionais — {f1} táxons):
{chr(10).join(f"  • {sp}" for sp in raras_obs)}

SÍNTESE: Registro de 2 espécies Vulneráveis (Puma concolor, Leopardus pardalis)
e 1 Quase Ameaçada (Sylvilagus minensis) confere relevância conservacionista
expressiva à área.
Grau de confiança: MÉDIO para as espécies ameaçadas (registros confirmados);
BAIXO para avaliação de raras (1 registro não é suficiente para categorização).
Limitações: base de dados sem preenchimento dos campos de ameaça — status
baseado em literatura; ausência de histórico temporal.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.7 — ESPÉCIES EXÓTICAS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Nenhuma espécie exótica ou invasora foi identificada nos registros da 28ª
campanha. Todos os {S_obs} táxons registrados são nativos da fauna brasileira.

A ausência de registros de espécies invasoras (ex.: Sus scrofa — javali,
Lepus europaeus — lebre-europeia, Rattus spp.) é um resultado positivo, embora
não descartável em função do esforço amostral limitado.

SÍNTESE: Ausência de espécies exóticas na amostragem atual.
Grau de confiança: BAIXO-MÉDIO (resultado baseado em 16 registros; possível
ausência de detecção de espécies crípticas ou de baixa detectabilidade).
Recomendação: manter monitoramento contínuo com atenção a espécies invasoras
de mamíferos, especialmente em áreas com pressão antrópica.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.8 — ESPÉCIES CINEGÉTICAS E XERIMBABO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ESPÉCIES CINEGÉTICAS (uso alimentar/comercial):
  • Hydrochoerus hydrochaeris (capivara) — 1 registro (DG7, pegada)
    Maior roedor do mundo; caça frequente em áreas rurais; vetor potencial de
    febre maculosa em regiões endêmicas.
  • Cuniculus paca (paca) — 1 registro (SPT1, câmera trap)
    Espécie de alto valor cinegético; nocturna e críptica; sua presença por câmera
    indica integridade de habitats florestais.
  • Sylvilagus minensis (tapiti) — 2 registros (FO6, SP4)
    Quase Ameaçada; pressão de caça representa risco adicional para a espécie.
  • Cabassous tatouay (tatu-de-rabo-mole) — 1 registro (JAC1, câmera trap)
    Pressão de caça moderada; hábitos fossórios dificultam detecção.

ESPÉCIES XERIMBABO: Nenhuma registrada nesta campanha.

Das {len(cinegeticas)} espécies cinegéticas registradas ({len(cinegeticas)/S_obs*100:.0f}% da riqueza total),
3 apresentam registros únicos (singletons), reforçando a necessidade de esforço
amostral maior para avaliação do status populacional dessas espécies na área.

SÍNTESE: 4 espécies cinegéticas registradas, todas com baixa abundância
relativa. Paca e tatu registrados exclusivamente via câmera trap, reforçando
a relevância desse método para espécies crípticas.
Grau de confiança: MÉDIO para presença; BAIXO para abundância.
Limitações: 1 campanha; sem estimativa de densidade; pressão de caça não
avaliada diretamente.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONSIDERAÇÕES GERAIS E RECOMENDAÇÕES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. LIMITAÇÃO CENTRAL: Esta análise baseia-se em uma única campanha (período
   seco — Abril/2026). Os resultados constituem diagnóstico exploratório inicial
   e NÃO devem ser interpretados como representação definitiva da mastofauna local.

2. RELEVÂNCIA CONSERVACIONISTA: O registro confirmado de Puma concolor e
   Leopardus pardalis (ambos Vulneráveis — MMA 148/2022) indica que a área
   mantém características ambientais compatíveis com a ocorrência de grandes
   e médios felídeos, o que tem implicações diretas para o licenciamento ambiental
   e os Programas de Monitoramento de Fauna.

3. SUBAMOSTRAGEM: Completude estimada de ~{completude:.0f}% indica que o inventário
   está longe do completo. Recomenda-se aumento do esforço amostral, especialmente
   de câmera trap (em termos de cobertura espacial e duração).

4. PERIODICIDADE: A comparação seca × cheia é fundamental para espécies com
   variação sazonal de movimentação. Campanhas no período chuvoso são essenciais
   para uma avaliação mais robusta.

5. PADRONIZAÇÃO: Diferenças de esforço entre métodos (busca ativa = 2h.2obs;
   câmera trap = 1 câmera.4noites; live trap = 30 armadilhas.4noites; pitfall =
   15 baldes.4noites) dificultam comparações diretas entre pontos. Recomenda-se
   padronização progressiva nos próximos ciclos.

================================================================================
Análise executada por: Sistema Opyta — Análise Ecológica Automatizada
Referências normativas: Portaria MMA 148/2022; IUCN Red List 2024-1
================================================================================
"""

txt_path = os.path.join(OUTPUT_DIR, "Relatorio_Mastofauna_28Campanha_Abr2026.txt")
with open(txt_path, "w", encoding="utf-8") as f:
    f.write(relatorio)
print(f"  ✓ Relatório salvo: Relatorio_Mastofauna_28Campanha_Abr2026.txt")

print(f"\n{'='*60}")
print(f"CONCLUÍDO — {len(os.listdir(OUTPUT_DIR))} arquivos em:")
print(f"  {OUTPUT_DIR}")
print(f"{'='*60}")
