# --- SCRIPT DE VALIDACAO DE IMPORTACAO (CLI) ---
# Uso: python scripts/validar_importacao.py <arquivo.xlsx> <GrupoBiologico>
# Exemplo:
#   python scripts/validar_importacao.py "projeto_ictio_real.xlsx" Ictiofauna

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.engine import get_engine
from validators.importacao.pipeline import validate_importacao_file

try:
    from core.audit import record_operation
except Exception:  # auditoria é opcional; nunca deve quebrar a validação
    record_operation = None


def _registrar_auditoria(arquivo: str, grupo: str, report) -> None:
    """Grava um registro de auditoria da validação (best-effort)."""
    if record_operation is None:
        return
    try:
        record_operation(
            "validacao",
            status="ok" if report.can_proceed else "bloqueado",
            grupo=grupo,
            source_files=[arquivo],
            metrics={
                "total_campanhas": report.total_campanhas,
                "total_pontos": report.total_pontos,
                "total_registros": report.total_registros,
            },
            issues={
                "n_blocks": len(report.blocks),
                "n_warnings": len(report.warnings),
                "n_especies_desconhecidas": len(report.especies_desconhecidas),
            },
        )
    except Exception:
        # Auditoria nunca pode interromper o fluxo de validação.
        pass


def _sep(char: str = "-", width: int = 65) -> str:
    return char * width


def main() -> None:
    if len(sys.argv) < 3:
        print("Uso: python scripts/validar_importacao.py <arquivo.xlsx> <GrupoBiologico>")
        print("Grupos validos: Ictiofauna, Bentos, Fitoplancton, Zooplancton, Avifauna, Herpetofauna, Mastofauna")
        sys.exit(1)

    arquivo = sys.argv[1]
    grupo = sys.argv[2]

    if not Path(arquivo).exists():
        print(f"Erro: arquivo nao encontrado: {arquivo}")
        sys.exit(1)

    print(_sep("="))
    print("  VALIDACAO DE IMPORTACAO")
    print(f"  Arquivo : {Path(arquivo).name}")
    print(f"  Grupo   : {grupo}")
    print(_sep("="))

    engine = get_engine()
    try:
        report = validate_importacao_file(
            arquivo,
            group=grupo,
            engine=engine,
            strict_unknown_species=True,
        )
    finally:
        engine.dispose()

    print(f"\n  Campanhas  : {report.total_campanhas}")
    print(f"  Pontos     : {report.total_pontos}")
    print(f"  Registros  : {report.total_registros}")
    if report.total_cadastro_especies:
        print(f"  Cadastro   : {report.total_cadastro_especies} especie(s)")
        print(f"    - novas no banco      : {report.total_cadastro_especies_novas}")
        print(f"    - ja existentes       : {report.total_cadastro_especies_existentes}")

    if report.blocks:
        print(f"\n{_sep()}")
        print(f"  BLOQUEIOS ({len(report.blocks)}) - migracao NAO pode prosseguir")
        print(_sep())
        for issue in report.blocks:
            linhas = f" [linhas: {issue.lines}]" if issue.lines else ""
            print(f"  [{issue.code}]{linhas}")
            print(f"    {issue.message}")
    else:
        print("\n  Sem bloqueios")

    if report.warnings:
        print(f"\n{_sep()}")
        print(f"  AVISOS ({len(report.warnings)})")
        print(_sep())
        for issue in report.warnings:
            linhas = f" [linhas: {issue.lines}]" if issue.lines else ""
            print(f"  [{issue.code}]{linhas}")
            print(f"    {issue.message}")

    if report.especies_desconhecidas:
        print(f"\n{_sep()}")
        print(f"  ESPECIES NAO ENCONTRADAS NO BANCO ({len(report.especies_desconhecidas)})")
        print(_sep())
        for esp in report.especies_desconhecidas:
            print(f"    - {esp}")

    if report.infos:
        print(f"\n{_sep()}")
        print(f"  INFORMACOES ({len(report.infos)})")
        print(_sep())
        for issue in report.infos:
            print(f"  [{issue.code}] {issue.message}")

    print(f"\n{_sep('=')}")
    if report.can_proceed:
        print("  VALIDACAO OK - arquivo aprovado para migracao")
        _registrar_auditoria(arquivo, grupo, report)
        sys.exit(0)

    print("  VALIDACAO FALHOU - corrija os bloqueios antes de migrar")
    _registrar_auditoria(arquivo, grupo, report)
    sys.exit(1)


if __name__ == "__main__":
    main()
