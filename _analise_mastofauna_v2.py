"""
Análise Ecológica — Mastofauna
Projeto: ITAGUA001 — Monitoramento da Fauna (Guanhães Energia)
Campanha: 28ª — Abril/2026 (Período Seco)

Estrutura: 4 relatórios independentes, um por empreendimento (PCH),
cada um comparando os pontos da PCH versus a Área Controle.

Empreendimentos:
  FOR = PCH Fortuna II
  DGN = PCH Dores de Guanhães
  JAC = PCH Jacaré
  SPT = PCH Senhora do Porto
  CON = Área Controle (referência para todos os 4 relatórios)

Prefixos de pontos:
  Câmera/armadilha/pitfall: CON*, FOR*, DGN*, JAC*, SPT*
  Busca ativa transecto:    CO*, FO*, DG*, JA*, SP*
  Busca ativa pontual:      BAAC*, BAFO*, BADG*, BAJA*, BASP*
  Playback primatas:        PMPRICON*, PMPRIFOR*, PMPRIDGN*, PMPRIJAC*, PMPRISPT*
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
# CONFIGURAÇÕES GERAIS
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = (r"G:\Meu Drive\Opyta\Clientes\Clientes\Clientes\Itatiaia"
            r"\Guanhães Energia\Campanhas de campo\28_campanha-Abril_26"
            r"\Mastofauna\Relatório Parcial\Resultados")

CAMPANHA = "28ª Campanha — Abril/2026 (Período Seco)"
PROJETO  = "Monitoramento da Fauna — Guanhães Energia (ITAGUA001)"
BACIA    = "Rio Doce"

PALETA   = ["#2E86AB", "#A23B72", "#F18F01", "#C73E1D", "#3B1F2B",
            "#44BBA4", "#E94F37", "#8B2FC9", "#F4A261", "#2D6A4F", "#E9C46A"]

# ─────────────────────────────────────────────────────────────────────────────
# DADOS — registros da 28ª campanha (projeto 165)
# ─────────────────────────────────────────────────────────────────────────────
REGISTROS_RAW = [
    # ponto, especie, familia, ordem, metodo, tipo, n
    ("CO2",  "Oligoryzomys sp.",           "Cricetidae",     "Rodentia",        "Busca ativa", "Visualização", 1),
    ("CON1", "Bibimys labiosus",           "Cricetidae",     "Rodentia",        "Live trap",   "Captura",      1),
    ("CON2", "Didelphis aurita",           "Didelphidae",    "Didelphimorphia", "Câmera trap", "Foto",         2),
    ("CON2", "Nectomys squamipes",         "Cricetidae",     "Rodentia",        "Live trap",   "Captura",      1),
    ("DG7",  "Hydrochoerus hydrochaeris",  "Caviidae",       "Rodentia",        "Busca ativa", "Pegada",       1),
    ("DGN1", "Didelphis aurita",           "Didelphidae",    "Didelphimorphia", "Câmera trap", "Foto",         1),
    ("FO6",  "Sylvilagus minensis",        "Leporidae",      "Lagomorpha",      "Busca ativa", "Visualização", 1),
    ("FOR1", "Didelphis aurita",           "Didelphidae",    "Didelphimorphia", "Câmera trap", "Foto",         1),
    ("JA1",  "Puma concolor",              "Felidae",        "Carnivora",       "Busca ativa", "Visualização", 1),
    ("JAC1", "Cabassous tatouay",          "Chlamyphoridae", "Cingulata",       "Câmera trap", "Foto",         1),
    ("JAC1", "Didelphis aurita",           "Didelphidae",    "Didelphimorphia", "Câmera trap", "Foto",         1),
    ("SP4",  "Sylvilagus minensis",        "Leporidae",      "Lagomorpha",      "Busca ativa", "Visualização", 1),
    ("SPT1", "Cuniculus paca",             "Cuniculidae",    "Rodentia",        "Câmera trap", "Foto",         1),
    ("SPT1", "Didelphis aurita",           "Didelphidae",    "Didelphimorphia", "Câmera trap", "Foto",         1),
    ("SPT1", "Leopardus pardalis",         "Felidae",        "Carnivora",       "Câmera trap", "Foto",         1),
    ("SPT1", "Necromys lasiurus",          "Cricetidae",     "Rodentia",        "Pitfall",     "Captura",      1),
]

# ─────────────────────────────────────────────────────────────────────────────
# MAPEAMENTO PONTO → ÁREA
# ─────────────────────────────────────────────────────────────────────────────
def ponto_para_area(ponto):
    p = ponto.upper()
    if (p.startswith("PMPRICON") or p.startswith("CON") or
            (p.startswith("CO") and not p.startswith("CON")) or p.startswith("BAAC")):
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

# Enriquecer registros com a área
registros = []
for row in REGISTROS_RAW:
    ponto, especie, familia, ordem, metodo, tipo, n = row
    registros.append({
        "ponto": ponto, "especie": especie, "familia": familia,
        "ordem": ordem, "metodo": metodo, "tipo": tipo, "n": n,
        "area": ponto_para_area(ponto),
    })

# ─────────────────────────────────────────────────────────────────────────────
# METADADOS DE CONSERVAÇÃO / ECOLOGIA
# ─────────────────────────────────────────────────────────────────────────────
META = {
    "Oligoryzomys sp.":          {"br": "—",  "iucn": "—",  "end": False, "ext": False, "cin": False, "xer": False},
    "Bibimys labiosus":          {"br": "LC", "iucn": "LC", "end": True,  "ext": False, "cin": False, "xer": False},
    "Didelphis aurita":          {"br": "LC", "iucn": "LC", "end": True,  "ext": False, "cin": False, "xer": False},
    "Nectomys squamipes":        {"br": "LC", "iucn": "LC", "end": True,  "ext": False, "cin": False, "xer": False},
    "Hydrochoerus hydrochaeris": {"br": "LC", "iucn": "LC", "end": False, "ext": False, "cin": True,  "xer": False},
    "Sylvilagus minensis":       {"br": "NT", "iucn": "LC", "end": True,  "ext": False, "cin": True,  "xer": False},
    "Puma concolor":             {"br": "VU", "iucn": "LC", "end": False, "ext": False, "cin": False, "xer": False},
    "Cabassous tatouay":         {"br": "LC", "iucn": "LC", "end": False, "ext": False, "cin": True,  "xer": False},
    "Cuniculus paca":            {"br": "LC", "iucn": "LC", "end": False, "ext": False, "cin": True,  "xer": False},
    "Leopardus pardalis":        {"br": "VU", "iucn": "LC", "end": False, "ext": False, "cin": False, "xer": False},
    "Necromys lasiurus":         {"br": "LC", "iucn": "LC", "end": False, "ext": False, "cin": False, "xer": False},
}

# ─────────────────────────────────────────────────────────────────────────────
# DEFINIÇÃO DOS EMPREENDIMENTOS
# ─────────────────────────────────────────────────────────────────────────────
EMPREENDIMENTOS = [
    {"nome": "PCH Fortuna II",        "sigla": "FOR", "cor": "#2E86AB", "pasta": "01_FOR_Fortuna_II"},
    {"nome": "PCH Dores de Guanhães", "sigla": "DGN", "cor": "#F18F01", "pasta": "02_DGN_Dores_Guanhaes"},
    {"nome": "PCH Jacaré",            "sigla": "JAC", "cor": "#C73E1D", "pasta": "03_JAC_Jacaer"},
    {"nome": "PCH Senhora do Porto",  "sigla": "SPT", "cor": "#8B2FC9", "pasta": "04_SPT_Senhora_Porto"},
]
COR_CONTROLE = "#44BBA4"

# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ─────────────────────────────────────────────────────────────────────────────
def save_fig(fig, name, out_dir):
    path = os.path.join(out_dir, name)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"    ✓ {name}")


def subtitle_box(ax, text):
    ax.text(0.5, 1.01, text, transform=ax.transAxes,
            fontsize=8.5, color="#555555", ha="center", va="bottom", style="italic")


def gauge_plot(ax, value, vmin, vmax, title, fmt=".3f", color="#2E86AB"):
    theta = np.linspace(np.pi, 0, 300)
    ax.plot(np.cos(theta), np.sin(theta), lw=8, color="#e0e0e0", solid_capstyle="round")
    frac = max(0, min(1, (value - vmin) / (vmax - vmin))) if vmax > vmin else 0
    theta_v = np.linspace(np.pi, np.pi - frac * np.pi, 300)
    ax.plot(np.cos(theta_v), np.sin(theta_v), lw=8, color=color, solid_capstyle="round")
    ax.text(0, -0.25, f"{value:{fmt}}", ha="center", va="center",
            fontsize=18, fontweight="bold", color=color)
    ax.text(0, -0.6, title, ha="center", va="center", fontsize=9)
    ax.text(-1.1, -0.15, f"{vmin}", ha="center", fontsize=7, color="gray")
    ax.text(1.1,  -0.15, f"{vmax}", ha="center", fontsize=7, color="gray")
    ax.set_xlim(-1.35, 1.35); ax.set_ylim(-0.8, 1.2)
    ax.axis("off")


def diversidade(abund_dict):
    """Retorna H', J', 1-D, D, S, N, Chao1, completude, f1, f2."""
    counts = list(abund_dict.values())
    S = len(counts)
    N = sum(counts)
    if N == 0 or S == 0:
        return dict(S=0, N=0, H=0, H_max=0, J=0, D=0, simp=0,
                    chao1=0, completude=0, f1=0, f2=0)
    pi = [c / N for c in counts]
    H   = -sum(p * math.log(p) for p in pi if p > 0)
    Hm  = math.log(S)
    J   = H / Hm if Hm > 0 else 0
    D   = sum(c * (c - 1) for c in counts) / (N * (N - 1)) if N > 1 else 0
    f1  = sum(1 for c in counts if c == 1)
    f2  = sum(1 for c in counts if c == 2)
    chao1 = S + (f1**2 / (2 * f2)) if f2 > 0 else S + f1 * (f1 - 1) / 2
    return dict(S=S, N=N, H=round(H, 4), H_max=round(Hm, 4), J=round(J, 4),
                D=round(D, 4), simp=round(1 - D, 4),
                chao1=round(chao1, 1), completude=round(S / chao1 * 100, 1) if chao1 > 0 else 100,
                f1=f1, f2=f2)


def jaccard(set_a, set_b):
    u = len(set_a | set_b)
    return len(set_a & set_b) / u if u > 0 else 0


def registros_area(recs, area_name):
    return [r for r in recs if r["area"] == area_name]


def abund_dict(recs):
    d = defaultdict(int)
    for r in recs:
        d[r["especie"]] += r["n"]
    return dict(d)


def spp_set(recs):
    return {r["especie"] for r in recs}


def curva_acum(recs, n_perm=300):
    esp_lista = [r["especie"] for r in recs for _ in range(r["n"])]
    n = len(esp_lista)
    if n == 0:
        return np.array([0]), np.array([0])
    mat = np.zeros((n_perm, n))
    for p in range(n_perm):
        idx = np.random.permutation(n)
        seen = set()
        for j, i in enumerate(idx):
            seen.add(esp_lista[i])
            mat[p, j] = len(seen)
    return mat.mean(axis=0), mat.std(axis=0)


# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÃO PRINCIPAL: GERA TODOS OS BLOCOS PARA UMA PCH vs CONTROLE
# ─────────────────────────────────────────────────────────────────────────────
def gerar_relatorio_pch(emp, recs_pch, recs_con, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    nome_pch  = emp["nome"]
    sigla     = emp["sigla"]
    cor_pch   = emp["cor"]

    ab_pch    = abund_dict(recs_pch)
    ab_con    = abund_dict(recs_con)
    spp_pch   = spp_set(recs_pch)
    spp_con   = spp_set(recs_con)
    div_pch   = diversidade(ab_pch)
    div_con   = diversidade(ab_con)

    np.random.seed(42)

    # ── FIG 01: Abundância relativa por espécie (comparativo PCH vs Controle) ──
    todas_spp = sorted(spp_pch | spp_con,
                       key=lambda s: ab_pch.get(s, 0) + ab_con.get(s, 0), reverse=True)
    n_spp = len(todas_spp)
    if n_spp == 0:
        print(f"  [AVISO] {nome_pch}: sem registros. Pulando relatório.")
        return

    fig, ax = plt.subplots(figsize=(11, max(4, n_spp * 0.6 + 1.5)))
    fig.suptitle(f"Fig. 01 — Abundância Relativa por Espécie\n{nome_pch} vs. Área Controle | {CAMPANHA}",
                 fontsize=10, fontweight="bold", y=1.01)

    y_pos = np.arange(n_spp)
    h = 0.35
    N_p = div_pch["N"] or 1
    N_c = div_con["N"] or 1
    pct_pch = [ab_pch.get(s, 0) / N_p * 100 for s in todas_spp]
    pct_con = [ab_con.get(s, 0) / N_c * 100 for s in todas_spp]

    ax.barh(y_pos + h/2, pct_pch, h, color=cor_pch, label=nome_pch, edgecolor="white")
    ax.barh(y_pos - h/2, pct_con, h, color=COR_CONTROLE, label="Controle", edgecolor="white")
    for i, s in enumerate(todas_spp):
        if pct_pch[i] > 0:
            ax.text(pct_pch[i] + 0.3, i + h/2, f"{pct_pch[i]:.1f}% (n={ab_pch.get(s,0)})",
                    va="center", fontsize=8)
        if pct_con[i] > 0:
            ax.text(pct_con[i] + 0.3, i - h/2, f"{pct_con[i]:.1f}% (n={ab_con.get(s,0)})",
                    va="center", fontsize=8)

    ax.set_yticks(y_pos)
    ax.set_yticklabels([f"$\\it{{{s.replace(' ', '\\ ')}}}$" if " " in s else s
                        for s in todas_spp], fontsize=9)
    ax.set_xlabel("Abundância Relativa (%)")
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    subtitle_box(ax, (f"{nome_pch}: S={div_pch['S']} táxons, N={div_pch['N']}  |  "
                      f"Controle: S={div_con['S']} táxons, N={div_con['N']}"))
    plt.tight_layout()
    save_fig(fig, "Fig01_Abundancia_Relativa.png", out_dir)

    # ── FIG 02: Riqueza e Abundância por ponto ──
    pontos_pch = sorted({r["ponto"] for r in recs_pch})
    pontos_con = sorted({r["ponto"] for r in recs_con})
    todos_pontos = pontos_pch + pontos_con
    pt_s = {p: len({r["especie"] for r in registros if r["ponto"] == p}) for p in todos_pontos}
    pt_n = {p: sum(r["n"] for r in registros if r["ponto"] == p) for p in todos_pontos}
    cores_pts = [cor_pch if p in pontos_pch else COR_CONTROLE for p in todos_pontos]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(f"Fig. 02 — Riqueza e Abundância por Ponto Amostral\n{nome_pch} vs. Controle | {CAMPANHA}",
                 fontsize=10, fontweight="bold")

    ax1.bar(todos_pontos, [pt_s[p] for p in todos_pontos], color=cores_pts, edgecolor="white")
    ax1.set_title("Riqueza (S)", fontsize=10); ax1.set_ylabel("Nº de táxons")
    ax1.set_xticklabels(todos_pontos, rotation=35, ha="right", fontsize=8)
    for i, p in enumerate(todos_pontos):
        ax1.text(i, pt_s[p] + 0.05, str(pt_s[p]), ha="center", fontsize=9)
    ax1.spines[["top","right"]].set_visible(False)

    ax2.bar(todos_pontos, [pt_n[p] for p in todos_pontos], color=cores_pts, edgecolor="white")
    ax2.set_title("Abundância (N)", fontsize=10); ax2.set_ylabel("Nº de indivíduos")
    ax2.set_xticklabels(todos_pontos, rotation=35, ha="right", fontsize=8)
    for i, p in enumerate(todos_pontos):
        ax2.text(i, pt_n[p] + 0.03, str(pt_n[p]), ha="center", fontsize=9)
    ax2.spines[["top","right"]].set_visible(False)

    from matplotlib.patches import Patch
    ax1.legend(handles=[Patch(facecolor=cor_pch, label=nome_pch),
                        Patch(facecolor=COR_CONTROLE, label="Controle")], fontsize=8)
    plt.tight_layout()
    save_fig(fig, "Fig02_Riqueza_Abundancia_Pontos.png", out_dir)

    # ── FIG 03: Composição taxonômica (por ordem) ──
    recs_combo = recs_pch + recs_con
    ord_spp = defaultdict(set); ord_n = defaultdict(int)
    for r in recs_combo:
        ord_spp[r["ordem"]].add(r["especie"]); ord_n[r["ordem"]] += r["n"]
    ordens = sorted(ord_spp.keys(), key=lambda o: ord_n[o], reverse=True)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(f"Fig. 03 — Composição Taxonômica por Ordem\n{nome_pch} + Controle | {CAMPANHA}",
                 fontsize=10, fontweight="bold")
    cores_ord = [PALETA[i % len(PALETA)] for i in range(len(ordens))]
    axes[0].bar(ordens, [len(ord_spp[o]) for o in ordens], color=cores_ord, edgecolor="white")
    axes[0].set_ylabel("Nº de táxons"); axes[0].set_title("Riqueza por Ordem", fontsize=10)
    axes[0].set_xticklabels(ordens, rotation=25, ha="right", fontsize=9)
    for i, o in enumerate(ordens):
        axes[0].text(i, len(ord_spp[o]) + 0.05, str(len(ord_spp[o])), ha="center", fontsize=9)
    axes[0].spines[["top","right"]].set_visible(False)
    axes[1].pie([ord_n[o] for o in ordens], labels=ordens, autopct="%1.1f%%",
                startangle=90, colors=cores_ord, textprops={"fontsize": 9})
    axes[1].set_title("Abundância por Ordem (%)", fontsize=10)
    plt.tight_layout()
    save_fig(fig, "Fig03_Composicao_Taxonomica.png", out_dir)

    # ── FIG 04: Curva de Acumulação (PCH vs Controle separados) ──
    mean_p, sd_p = curva_acum(recs_pch)
    mean_c, sd_c = curva_acum(recs_con)

    fig, ax = plt.subplots(figsize=(9, 5))
    fig.suptitle(f"Fig. 04 — Curva de Acumulação de Espécies\n{nome_pch} vs. Controle | {CAMPANHA}",
                 fontsize=10, fontweight="bold")
    if len(mean_p) > 1:
        xp = np.arange(1, len(mean_p) + 1)
        ax.plot(xp, mean_p, color=cor_pch, lw=2, label=f"{nome_pch} (Chao1≈{div_pch['chao1']:.0f})")
        ax.fill_between(xp, mean_p - sd_p, mean_p + sd_p, alpha=0.25, color=cor_pch)
        ax.axhline(y=div_pch["chao1"], color=cor_pch, linestyle="--", lw=1.2, alpha=0.6)
    if len(mean_c) > 1:
        xc = np.arange(1, len(mean_c) + 1)
        ax.plot(xc, mean_c, color=COR_CONTROLE, lw=2, label=f"Controle (Chao1≈{div_con['chao1']:.0f})")
        ax.fill_between(xc, mean_c - sd_c, mean_c + sd_c, alpha=0.25, color=COR_CONTROLE)
        ax.axhline(y=div_con["chao1"], color=COR_CONTROLE, linestyle="--", lw=1.2, alpha=0.6)
    ax.set_xlabel("Nº de registros amostrados"); ax.set_ylabel("S acumulado")
    ax.legend(fontsize=9); ax.spines[["top","right"]].set_visible(False)
    subtitle_box(ax, (f"{nome_pch}: completude≈{div_pch['completude']:.0f}%  |  "
                      f"Controle: completude≈{div_con['completude']:.0f}%"))
    plt.tight_layout()
    save_fig(fig, "Fig04_Curva_Acumulacao.png", out_dir)

    # ── FIG 05: Índices de Diversidade (PCH vs Controle — gauges) ──
    fig, axes = plt.subplots(2, 3, figsize=(13, 9))
    fig.suptitle(f"Fig. 05 — Índices de Diversidade\n{nome_pch} vs. Controle | {CAMPANHA}",
                 fontsize=10, fontweight="bold")

    H_max_global = math.log(max(div_pch["S"], div_con["S"], 2))
    pares = [
        (div_pch["H"],    0, H_max_global,  "Shannon H'\n" + nome_pch,   ".3f", cor_pch),
        (div_pch["J"],    0, 1,             "Pielou J'\n" + nome_pch,    ".3f", cor_pch),
        (div_pch["simp"], 0, 1,             "Simpson 1-D\n" + nome_pch,  ".3f", cor_pch),
        (div_con["H"],    0, H_max_global,  "Shannon H'\nControle",       ".3f", COR_CONTROLE),
        (div_con["J"],    0, 1,             "Pielou J'\nControle",        ".3f", COR_CONTROLE),
        (div_con["simp"], 0, 1,             "Simpson 1-D\nControle",      ".3f", COR_CONTROLE),
    ]
    for ax_, (val, vmin, vmax, tit, fmt, cor) in zip(axes.flat, pares):
        gauge_plot(ax_, val, vmin, vmax, tit, fmt, cor)
    plt.tight_layout()
    save_fig(fig, "Fig05_Indices_Diversidade.png", out_dir)

    # ── FIG 06: Jaccard PCH vs Controle ──
    j_val = jaccard(spp_pch, spp_con)
    fig, ax = plt.subplots(figsize=(6, 4))
    fig.suptitle(f"Fig. 06 — Similaridade de Jaccard\n{nome_pch} vs. Controle | {CAMPANHA}",
                 fontsize=10, fontweight="bold")
    areas2 = [nome_pch, "Controle"]
    mat_j  = np.array([[1.0, j_val], [j_val, 1.0]])
    im = ax.imshow(mat_j, cmap="YlOrRd", vmin=0, vmax=1)
    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Jaccard (0–1)")
    ax.set_xticks([0, 1]); ax.set_xticklabels(areas2, fontsize=9)
    ax.set_yticks([0, 1]); ax.set_yticklabels(areas2, fontsize=9)
    for i in range(2):
        for j in range(2):
            ax.text(j, i, f"{mat_j[i,j]:.2f}", ha="center", va="center",
                    fontsize=14, fontweight="bold",
                    color="white" if mat_j[i,j] > 0.6 else "black")
    subtitle_box(ax, (f"Espécies exclusivas {sigla}: {len(spp_pch - spp_con)}  |  "
                      f"Exclusivas Controle: {len(spp_con - spp_pch)}  |  "
                      f"Compartilhadas: {len(spp_pch & spp_con)}"))
    plt.tight_layout()
    save_fig(fig, "Fig06_Jaccard_PCH_Controle.png", out_dir)

    # ── FIG 07: Venn PCH vs Controle (2 conjuntos) ──
    only_pch = spp_pch - spp_con
    only_con = spp_con - spp_pch
    shared   = spp_pch & spp_con

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.suptitle(f"Fig. 07 — Espécies Exclusivas e Compartilhadas\n{nome_pch} vs. Controle | {CAMPANHA}",
                 fontsize=10, fontweight="bold")
    ax.set_xlim(0, 10); ax.set_ylim(0, 7); ax.axis("off")

    c1 = plt.Circle((3.2, 3.5), 2.5, color=cor_pch, alpha=0.3)
    c2 = plt.Circle((6.8, 3.5), 2.5, color=COR_CONTROLE, alpha=0.3)
    ax.add_patch(c1); ax.add_patch(c2)
    ax.text(1.2, 6.2, nome_pch, fontsize=11, fontweight="bold", color=cor_pch)
    ax.text(6.0, 6.2, "Controle",  fontsize=11, fontweight="bold", color=COR_CONTROLE)

    def fmt_venn(s):
        if not s: return "(nenhuma)"
        return "\n".join(f"• {sp}" for sp in sorted(s))

    ax.text(2.2, 3.5, fmt_venn(only_pch) + f"\n\nn={len(only_pch)}",
            ha="center", va="center", fontsize=8.5, color="#1a4a6a")
    ax.text(7.8, 3.5, fmt_venn(only_con) + f"\n\nn={len(only_con)}",
            ha="center", va="center", fontsize=8.5, color="#1a6a5a")
    ax.text(5.0, 3.5, fmt_venn(shared) + f"\n\nn={len(shared)}",
            ha="center", va="center", fontsize=8.5,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.9))
    ax.text(5.0, 0.5, (f"S total ({nome_pch}+Controle) = {len(spp_pch | spp_con)}  |  "
                       f"Jaccard = {j_val:.2f}"),
            ha="center", fontsize=9, color="#555555", style="italic")
    save_fig(fig, "Fig07_Venn_PCH_Controle.png", out_dir)

    # ── FIG 08: Espécies de Conservação ──
    todas_spp_combo = list(spp_pch | spp_con)
    ab_combo = abund_dict(recs_pch + recs_con)

    ameacadas = [s for s in todas_spp_combo if META.get(s, {}).get("br") in ("VU","EN","CR","NT")]
    endemicas = [s for s in todas_spp_combo if META.get(s, {}).get("end")]
    singletons = [s for s in todas_spp_combo if ab_combo.get(s, 0) == 1]

    fig, axes = plt.subplots(1, 3, figsize=(14, 6))
    fig.suptitle(f"Fig. 08 — Espécies de Interesse para Conservação\n"
                 f"{nome_pch} + Controle | {CAMPANHA}", fontsize=10, fontweight="bold")

    def bar_conserv(ax_, lista, title, cor):
        if not lista:
            ax_.text(0.5, 0.5, "Nenhuma registrada", ha="center", va="center",
                     transform=ax_.transAxes, fontsize=10)
        else:
            ns = [ab_combo.get(s, 0) for s in lista]
            st = [META.get(s, {}).get("br", "—") for s in lista]
            bars_ = ax_.barh(lista, ns, color=cor, edgecolor="white", height=0.5)
            for bar_, s_ in zip(bars_, st):
                ax_.text(bar_.get_width() + 0.02, bar_.get_y() + bar_.get_height() / 2,
                         f"[{s_}]", va="center", fontsize=8, color="#555555")
            ax_.set_xlabel("N registros")
        ax_.set_title(title, fontsize=10, fontweight="bold")
        ax_.spines[["top","right"]].set_visible(False)

    bar_conserv(axes[0], ameacadas, f"Ameaçadas/NT (MMA)\n(n={len(ameacadas)})", "#C73E1D")
    bar_conserv(axes[1], endemicas, f"Endêmicas BR\n(n={len(endemicas)})",        "#8B2FC9")
    bar_conserv(axes[2], singletons, f"Singletons obs.\n(n={len(singletons)})",   "#F18F01")
    plt.tight_layout()
    save_fig(fig, "Fig08_Conservacao.png", out_dir)

    # ── FIG 09: Espécies Exóticas ──
    exoticas = [s for s in todas_spp_combo if META.get(s, {}).get("ext")]
    fig, ax = plt.subplots(figsize=(7, 3))
    fig.suptitle(f"Fig. 09 — Espécies Exóticas/Invasoras\n"
                 f"{nome_pch} + Controle | {CAMPANHA}", fontsize=10, fontweight="bold")
    ax.axis("off")
    if exoticas:
        for i, s in enumerate(exoticas):
            ax.text(0.5, 0.8 - i * 0.15, f"• {s} (n={ab_combo.get(s,0)})",
                    ha="center", va="center", transform=ax.transAxes, fontsize=11)
    else:
        ax.text(0.5, 0.6, "Nenhuma espécie exótica ou invasora registrada.",
                ha="center", va="center", transform=ax.transAxes, fontsize=12,
                bbox=dict(boxstyle="round,pad=0.6", facecolor="#e8f5e9", edgecolor="#388e3c", lw=1.5))
        ax.text(0.5, 0.22, "Todos os táxons registrados são nativos da fauna brasileira.",
                ha="center", va="center", transform=ax.transAxes, fontsize=9,
                color="#555555", style="italic")
    save_fig(fig, "Fig09_Exoticas.png", out_dir)

    # ── FIG 10: Cinegéticas ──
    cinegeticas = [(s, ab_combo[s]) for s in todas_spp_combo
                   if META.get(s, {}).get("cin") and s in ab_combo]
    xerimbabo   = [(s, ab_combo[s]) for s in todas_spp_combo
                   if META.get(s, {}).get("xer") and s in ab_combo]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(f"Fig. 10 — Espécies Cinegéticas e Xerimbabo\n"
                 f"{nome_pch} + Controle | {CAMPANHA}", fontsize=10, fontweight="bold")

    def bar_uso(ax_, lista, title, cor):
        if not lista:
            ax_.text(0.5, 0.5, "Nenhuma registrada", ha="center", va="center",
                     transform=ax_.transAxes, fontsize=10, color="#555555")
        else:
            sps, ns = zip(*lista)
            ax_.barh(list(sps), list(ns), color=cor, edgecolor="white", height=0.5)
            for i_, (_, n_) in enumerate(lista):
                ax_.text(n_ + 0.02, i_, f"n={n_}", va="center", fontsize=9)
            ax_.set_xlabel("N registros")
        ax_.set_title(title, fontsize=10, fontweight="bold")
        ax_.spines[["top","right"]].set_visible(False)

    bar_uso(axes[0], cinegeticas, f"Cinegéticas (n={len(cinegeticas)})", "#C73E1D")
    bar_uso(axes[1], xerimbabo,   f"Xerimbabo (n={len(xerimbabo)})",     "#8B2FC9")
    plt.tight_layout()
    save_fig(fig, "Fig10_Cinegeticas_Xerimbabo.png", out_dir)

    # ── RELATÓRIO TEXTO ──
    ab_pch_sorted = sorted(ab_pch.items(), key=lambda x: x[1], reverse=True)
    ab_con_sorted = sorted(ab_con.items(), key=lambda x: x[1], reverse=True)

    def tabela_spp(ab_sorted, area_label):
        linhas = []
        N_ = sum(v for _, v in ab_sorted) or 1
        for i, (sp, n) in enumerate(ab_sorted):
            m = META.get(sp, {})
            linhas.append(
                f"  {i+1:2d}. {sp:<35} n={n:2d} ({n/N_*100:4.1f}%) | "
                f"BR={m.get('br','—'):>4} | IUCN={m.get('iucn','—'):>4} | "
                f"{'Endêmica' if m.get('end') else 'Amp.dist.'}"
            )
        return "\n".join(linhas)

    relatorio = f"""
================================================================================
RELATÓRIO TÉCNICO — MASTOFAUNA
Empreendimento : {nome_pch}
Projeto        : {PROJETO}
Bacia          : {BACIA}
Campanha       : {CAMPANHA}
Análise        : {nome_pch} comparado com Área Controle
Data análise   : 05/05/2026
================================================================================

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.1 — RIQUEZA, COMPOSIÇÃO E ABUNDÂNCIA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{nome_pch.upper()}:
  Riqueza (S)         = {div_pch['S']} táxons
  Abundância (N)      = {div_pch['N']} indivíduos
  Pontos amostrados   = {len(pontos_pch)} ({', '.join(sorted(pontos_pch))})
{tabela_spp(ab_pch_sorted, nome_pch) if ab_pch_sorted else "  (sem registros)"}

ÁREA CONTROLE:
  Riqueza (S)         = {div_con['S']} táxons
  Abundância (N)      = {div_con['N']} indivíduos
  Pontos amostrados   = {len(pontos_con)} ({', '.join(sorted(pontos_con))})
{tabela_spp(ab_con_sorted, "Controle") if ab_con_sorted else "  (sem registros)"}

Espécies compartilhadas ({len(shared)}): {', '.join(sorted(shared)) or '—'}
Espécies exclusivas {sigla} ({len(only_pch)}): {', '.join(sorted(only_pch)) or '—'}
Espécies exclusivas Controle ({len(only_con)}): {', '.join(sorted(only_con)) or '—'}
Similaridade de Jaccard: {j_val:.2f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.2 — SUFICIÊNCIA AMOSTRAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{nome_pch}:
  Chao1 estimado   = {div_pch['chao1']} táxons
  Completude       = {div_pch['completude']}%
  Singletons f₁    = {div_pch['f1']} | Doubletons f₂ = {div_pch['f2']}

Área Controle:
  Chao1 estimado   = {div_con['chao1']} táxons
  Completude       = {div_con['completude']}%
  Singletons f₁    = {div_con['f1']} | Doubletons f₂ = {div_con['f2']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.3 — ÍNDICES DE DIVERSIDADE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Índice           | {sigla:<20} | Controle
  ─────────────────────────────────────────────
  Shannon (H')     | {div_pch['H']:<20.3f} | {div_con['H']:.3f}
  Equitab. (J')    | {div_pch['J']:<20.3f} | {div_con['J']:.3f}
  Simpson (1-D)    | {div_pch['simp']:<20.3f} | {div_con['simp']:.3f}
  Dominância (D)   | {div_pch['D']:<20.3f} | {div_con['D']:.3f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.4 — SIMILARIDADE (JACCARD)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Jaccard {sigla} × Controle = {j_val:.2f}
  ({len(shared)} espécie(s) compartilhada(s) de {len(spp_pch | spp_con)} total)

  Jaccard = 0.0: comunidades sem espécie em comum
  Jaccard = 1.0: comunidades idênticas em composição

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.5 — DIAGRAMA DE VENN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Exclusivas {sigla}      ({len(only_pch):2d}): {', '.join(sorted(only_pch)) or '—'}
  Compartilhadas       ({len(shared):2d}): {', '.join(sorted(shared)) or '—'}
  Exclusivas Controle  ({len(only_con):2d}): {', '.join(sorted(only_con)) or '—'}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.6 — ESPÉCIES AMEAÇADAS, ENDÊMICAS E RARAS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Ameaçadas/NT (MMA 148/2022): {len(ameacadas)} táxons
{chr(10).join(f"    • {s} [{META.get(s,{}).get('br','—')}]" for s in ameacadas) or '    (nenhuma)'}

  Endêmicas do Brasil: {len(endemicas)} táxons
{chr(10).join(f"    • {s}" for s in endemicas) or '    (nenhuma)'}

  Singletons observacionais: {len(singletons)} táxons
{chr(10).join(f"    • {s}" for s in singletons) or '    (nenhuma)'}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.7 — ESPÉCIES EXÓTICAS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Exóticas registradas: {len(exoticas)} táxons
{chr(10).join(f"    • {s}" for s in exoticas) or '    Nenhuma espécie exótica foi registrada.'}
  {"Todos os táxons registrados são nativos." if not exoticas else ""}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BLOCO 6.8 — ESPÉCIES CINEGÉTICAS E XERIMBABO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Cinegéticas ({len(cinegeticas)} táxons):
{chr(10).join(f"    • {s} (n={n})" for s,n in cinegeticas) or '    (nenhuma)'}

  Xerimbabo ({len(xerimbabo)} táxons):
{chr(10).join(f"    • {s} (n={n})" for s,n in xerimbabo) or '    (nenhuma)'}

================================================================================
Análise executada por: Sistema Opyta — Análise Ecológica Automatizada
Fontes: Portaria MMA 148/2022; IUCN Red List 2024-1
================================================================================
"""

    txt_name = f"Relatorio_Mastofauna_{sigla}_28Campanha_Abr2026.txt"
    with open(os.path.join(out_dir, txt_name), "w", encoding="utf-8") as f:
        f.write(relatorio)
    print(f"    ✓ {txt_name}")


# ─────────────────────────────────────────────────────────────────────────────
# EXECUÇÃO: LOOP SOBRE OS 4 EMPREENDIMENTOS
# ─────────────────────────────────────────────────────────────────────────────
def main():
    np.random.seed(42)
    recs_con = registros_area(registros, "Controle")

    print(f"\n{'='*65}")
    print(f"  ANÁLISE MASTOFAUNA — {CAMPANHA}")
    print(f"  {PROJETO}")
    print(f"{'='*65}\n")
    print(f"  Registros Controle: {len(recs_con)} (pontos: "
          f"{sorted({r['ponto'] for r in recs_con})})\n")

    for emp in EMPREENDIMENTOS:
        nome_pch = emp["nome"]
        pasta    = emp["pasta"]
        out_dir  = os.path.join(BASE_DIR, pasta)
        recs_pch = registros_area(registros, nome_pch)

        print(f"  ── {nome_pch} ──")
        print(f"     Registros PCH: {len(recs_pch)} | "
              f"Pontos: {sorted({r['ponto'] for r in recs_pch})}")
        gerar_relatorio_pch(emp, recs_pch, recs_con, out_dir)
        print()

    print(f"\n{'='*65}")
    print(f"  CONCLUÍDO — 4 relatórios gerados em:")
    print(f"  {BASE_DIR}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
