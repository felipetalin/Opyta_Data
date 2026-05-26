"""
validators/importacao/report.py
Estruturas de dados do relatório de validação de importação.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ValidationIssue:
    """Um problema encontrado durante a validação."""
    code: str
    severity: str          # "block" | "warning" | "info"
    message: str
    lines: list[int] = field(default_factory=list)  # linhas afetadas


@dataclass
class ValidationReport:
    """Relatório consolidado de uma execução do pipeline de validação importação."""

    # Contadores gerais
    total_campanhas: int = 0
    total_pontos: int = 0
    total_registros: int = 0
    total_esforco_dias: float = 0.0
    total_cadastro_especies: int = 0
    total_cadastro_especies_novas: int = 0
    total_cadastro_especies_existentes: int = 0

    # Espécies desconhecidas
    especies_desconhecidas: list[str] = field(default_factory=list)
    total_especies_desconhecidas: int = 0

    # Problemas
    issues: list[ValidationIssue] = field(default_factory=list)

    # DataFrames (se sem bloqueios)
    df_capa: object = field(default=None, repr=False)
    df_pontos: object = field(default=None, repr=False)
    df_esforco: object = field(default=None, repr=False)
    df_resultados: object = field(default=None, repr=False)
    df_cadastro_especies: object = field(default=None, repr=False)

    # ---------------------------------------------------------------------------
    # Propriedades derivadas
    # ---------------------------------------------------------------------------

    @property
    def blocks(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "block"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def infos(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "info"]

    @property
    def can_proceed(self) -> bool:
        """True somente se não há nenhum bloqueio."""
        return len(self.blocks) == 0
