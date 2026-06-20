"""
validators/common_checks.py
===========================

Checagens reutilizáveis de qualidade de dados, compartilhadas entre os
validadores de cadastro (espécies/parâmetros) e de importação por grupo.

Cada checagem recebe um ``pandas.DataFrame`` e devolve uma lista de
``Finding`` (modelo rico de ``validators/findings.py``). São funções puras: não
acessam banco nem alteram os dados. Portadas/consolidadas a partir das regras de
auditoria do Opyta_Data_Analysis (`src/opyta_analysis/revisao/`).
"""
from __future__ import annotations

import pandas as pd

from .findings import Finding, Severidade, TipoAchado


def check_required_columns(
    df: pd.DataFrame, required: list[str], *, categoria: str = "Estrutura"
) -> list[Finding]:
    """Bloqueia se colunas obrigatórias estiverem ausentes."""
    faltando = [c for c in required if c not in df.columns]
    if not faltando:
        return []
    return [
        Finding(
            categoria=categoria,
            problema=f"Colunas obrigatórias ausentes: {', '.join(faltando)}",
            severidade=Severidade.ALTA,
            tipo_achado=TipoAchado.ERRO_CONFIRMADO,
            sugestao="Use o modelo oficial de planilha e preencha todas as colunas obrigatórias.",
        )
    ]


def check_no_nulls(
    df: pd.DataFrame, columns: list[str], *, categoria: str = "Preenchimento"
) -> list[Finding]:
    """Alerta para nulos em colunas que não deveriam ter valores em branco."""
    findings: list[Finding] = []
    for col in columns:
        if col not in df.columns:
            continue
        mask = df[col].isna()
        if mask.any():
            linhas = [int(i) + 2 for i in df.index[mask].tolist()]  # +2 = header + 1-based
            findings.append(
                Finding(
                    categoria=categoria,
                    problema=f"Coluna '{col}' contém {int(mask.sum())} valor(es) em branco.",
                    severidade=Severidade.MEDIA,
                    tipo_achado=TipoAchado.ALERTA,
                    evidencia=f"coluna={col}",
                    sugestao="Preencher os campos obrigatórios antes da migração.",
                    linhas=linhas[:50],
                )
            )
    return findings


def check_duplicates(
    df: pd.DataFrame, subset: list[str], *, categoria: str = "Duplicidade"
) -> list[Finding]:
    """Alerta para linhas duplicadas no grão informado."""
    cols = [c for c in subset if c in df.columns]
    if not cols:
        return []
    dup_mask = df.duplicated(subset=cols, keep=False)
    if not dup_mask.any():
        return []
    linhas = [int(i) + 2 for i in df.index[dup_mask].tolist()]
    return [
        Finding(
            categoria=categoria,
            problema=f"{int(dup_mask.sum())} linha(s) duplicada(s) no grão ({', '.join(cols)}).",
            severidade=Severidade.MEDIA,
            tipo_achado=TipoAchado.DIVERGENCIA,
            evidencia=f"grao={cols}",
            sugestao="Remover ou consolidar registros duplicados.",
            linhas=linhas[:50],
        )
    ]


def check_numeric_non_negative(
    df: pd.DataFrame, columns: list[str], *, categoria: str = "Valores"
) -> list[Finding]:
    """Alerta para valores numéricos negativos onde não fazem sentido (contagens/abundância)."""
    findings: list[Finding] = []
    for col in columns:
        if col not in df.columns:
            continue
        serie = pd.to_numeric(
            df[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
            errors="coerce",
        )
        mask = serie < 0
        if mask.any():
            linhas = [int(i) + 2 for i in df.index[mask].tolist()]
            findings.append(
                Finding(
                    categoria=categoria,
                    problema=f"Coluna '{col}' contém {int(mask.sum())} valor(es) negativo(s).",
                    severidade=Severidade.ALTA,
                    tipo_achado=TipoAchado.ERRO_CONFIRMADO,
                    evidencia=f"coluna={col}",
                    sugestao="Contagens/abundâncias não podem ser negativas. Revisar a origem.",
                    linhas=linhas[:50],
                )
            )
    return findings
