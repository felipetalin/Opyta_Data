"""
validators/especies/__init__.py
API pública do módulo. Importe daqui para usar no código da aplicação.

Uso mínimo:
    from validators.especies import validate_especies_file, render_validation_report

    report = validate_especies_file(uploaded_file, engine=get_engine())
    render_validation_report(report)

    if report.can_proceed:
        # report.cleaned_df está disponível para cadastramento
        ...
"""
from .pipeline import validate_especies_file
from .render import render_validation_report
from .report import CorrectionDetail, ValidationIssue, ValidationReport

__all__ = [
    "validate_especies_file",
    "render_validation_report",
    "ValidationReport",
    "ValidationIssue",
    "CorrectionDetail",
]
