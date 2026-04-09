"""
validators/especies/report.py
Estruturas de dados do relatório de validação.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CorrectionDetail:
    """Registra uma correção automática aplicada a uma célula."""
    row: int
    column: str
    original: str
    corrected: str
    reason: str


@dataclass
class ValidationIssue:
    """Um problema encontrado durante a validação."""
    code: str
    severity: str          # "block" | "warning"
    message: str
    row: int | None = None     # None = problema estrutural (sem linha específica)
    column: str | None = None


@dataclass
class ValidationReport:
    """Relatório consolidado de uma execução do pipeline de validação."""

    # Contadores gerais
    total_rows: int = 0
    total_valid: int = 0
    total_new: int = 0
    total_existing: int = 0

    # Listas nominais
    new_species: list[str] = field(default_factory=list)
    existing_species: list[str] = field(default_factory=list)

    # Correções automáticas e problemas
    corrections: list[CorrectionDetail] = field(default_factory=list)
    issues: list[ValidationIssue] = field(default_factory=list)

    # DataFrame limpo (preenchido pelo pipeline após normalização)
    cleaned_df: object = field(default=None, repr=False)

    # ---------------------------------------------------------------------------
    # Propriedades derivadas
    # ---------------------------------------------------------------------------

    @property
    def total_corrections(self) -> int:
        return len(self.corrections)

    @property
    def blocks(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "block"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def can_proceed(self) -> bool:
        """True somente se não há nenhum bloqueio."""
        return len(self.blocks) == 0
