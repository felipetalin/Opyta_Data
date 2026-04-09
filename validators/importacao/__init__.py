"""
validators/importacao/__init__.py
API pública do módulo. Importe daqui para usar no código da aplicação.

Uso mínimo:
    from validators.importacao import validate_importacao_file, render_validation_report
    from core.engine import get_engine

    report = validate_importacao_file(
        uploaded_file,
        group="Zooplâncton",
        engine=get_engine()
    )
    render_validation_report(report)

    if report.can_proceed:
        # Prosseguir com migração
        ...
"""
from .pipeline import validate_importacao_file
from .render import render_validation_report
from .report import ValidationIssue, ValidationReport

__all__ = [
    "validate_importacao_file",
    "render_validation_report",
    "ValidationReport",
    "ValidationIssue",
]
