"""
validators/findings.py
======================

Modelo rico de achados de auditoria, portado da camada de revisão consolidada no
Opyta_Data_Analysis (`src/opyta_analysis/revisao/rules.py`).

O pipeline de importação atual (`validators/importacao/report.py`) usa um modelo
simples de 3 severidades (block | warning | info). Este módulo acrescenta, de
forma **aditiva e opcional**, uma classificação mais rica para auditoria e
revisão por humano:

- ``severidade``  : ALTA | MEDIA | BAIXA
- ``categoria``   : agrupador livre (ex.: "Espécies", "Campanha", "Esforço")
- ``tipo_achado`` : erro_confirmado | alerta_para_conferencia | divergencia_candidata
- ``status``      : pendente | confirmado | falso_positivo | corrigido | nao_aplica

Inclui um gerador de *checklist de revisão* (`prepare_checklist`) que numera os
achados (REV-0001, ...) e adiciona as colunas operacionais para o revisor —
exatamente o insumo de uma futura página "Qualidade & Auditoria de Dados".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Severidade(str, Enum):
    ALTA = "ALTA"
    MEDIA = "MEDIA"
    BAIXA = "BAIXA"


class TipoAchado(str, Enum):
    ERRO_CONFIRMADO = "erro_confirmado"
    ALERTA = "alerta_para_conferencia"
    DIVERGENCIA = "divergencia_candidata"


class StatusRevisao(str, Enum):
    PENDENTE = "pendente"
    CONFIRMADO = "confirmado"
    FALSO_POSITIVO = "falso_positivo"
    CORRIGIDO = "corrigido"
    NAO_APLICA = "nao_aplica"


@dataclass
class Finding:
    """Um achado de auditoria/validação com classificação rica."""

    categoria: str
    problema: str
    severidade: Severidade = Severidade.MEDIA
    tipo_achado: TipoAchado = TipoAchado.ALERTA
    evidencia: str = ""
    sugestao: str = ""
    status: StatusRevisao = StatusRevisao.PENDENTE
    linhas: list[int] = field(default_factory=list)

    def to_row(self) -> dict[str, Any]:
        return {
            "severidade": self.severidade.value,
            "categoria": self.categoria,
            "tipo_achado": self.tipo_achado.value,
            "problema": self.problema,
            "evidencia": self.evidencia,
            "sugestao": self.sugestao,
            "status": self.status.value,
            "linhas": ", ".join(str(n) for n in self.linhas) if self.linhas else "",
        }


def prepare_checklist(findings: list[Finding]) -> list[dict[str, Any]]:
    """
    Converte achados em linhas de checklist de revisão, numeradas (REV-0001, ...)
    e com as colunas operacionais para o revisor preencher.
    """
    rows: list[dict[str, Any]] = []
    for index, finding in enumerate(findings, start=1):
        row = {"id_revisao": f"REV-{index:04d}", **finding.to_row()}
        row.setdefault("procede", "")
        row.setdefault("acao", "")
        row.setdefault("responsavel", "")
        row.setdefault("observacao_revisor", "")
        rows.append(row)
    return rows


def summarize(findings: list[Finding]) -> dict[str, int]:
    """Resumo por severidade e tipo de achado (para cards/KPIs)."""
    resumo = {
        "total": len(findings),
        "ALTA": 0,
        "MEDIA": 0,
        "BAIXA": 0,
        "erro_confirmado": 0,
        "alerta_para_conferencia": 0,
        "divergencia_candidata": 0,
    }
    for f in findings:
        resumo[f.severidade.value] = resumo.get(f.severidade.value, 0) + 1
        resumo[f.tipo_achado.value] = resumo.get(f.tipo_achado.value, 0) + 1
    return resumo


# ---------------------------------------------------------------------------
# Ponte com o modelo simples do pipeline de importação
# ---------------------------------------------------------------------------
_SEVERITY_TO_RICH = {
    "block": (Severidade.ALTA, TipoAchado.ERRO_CONFIRMADO),
    "warning": (Severidade.MEDIA, TipoAchado.ALERTA),
    "info": (Severidade.BAIXA, TipoAchado.ALERTA),
}


def from_validation_issue(issue: Any, categoria: str | None = None) -> Finding:
    """
    Converte um ``ValidationIssue`` (modelo simples block/warning/info do
    pipeline de importação) em um ``Finding`` rico. Não importa o tipo
    diretamente para evitar acoplamento — espera apenas os atributos
    ``code``, ``severity``, ``message`` e (opcional) ``lines``.
    """
    severity = str(getattr(issue, "severity", "info")).lower()
    sev, tipo = _SEVERITY_TO_RICH.get(severity, (Severidade.BAIXA, TipoAchado.ALERTA))
    return Finding(
        categoria=categoria or str(getattr(issue, "code", "geral")),
        problema=str(getattr(issue, "message", "")),
        severidade=sev,
        tipo_achado=tipo,
        evidencia=str(getattr(issue, "code", "")),
        linhas=list(getattr(issue, "lines", []) or []),
    )
