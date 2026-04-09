"""
validators/especies/render.py
Renderiza o ValidationReport em componentes Streamlit.
Depende dos estilos `.opyta-card` já injetados por inject_saas_styles().
"""
from __future__ import annotations

import streamlit as st

from .report import ValidationReport


def render_validation_report(report: ValidationReport) -> None:
    """
    Exibe o relatório completo de validação na interface.
    Deve ser chamado após inject_saas_styles().
    """
    _render_summary_cards(report)
    st.markdown("<br>", unsafe_allow_html=True)

    # Bloqueios — parar o processo visualmente
    if report.blocks:
        _render_issues_section(
            issues=report.blocks,
            title="🚫 Bloqueios — o cadastro não pode prosseguir",
            color="#DC2626",
            bg="#FEF2F2",
            border="#FECACA",
        )

    # Warnings
    if report.warnings:
        _render_issues_section(
            issues=report.warnings,
            title="⚠️ Avisos — revisão recomendada",
            color="#92400E",
            bg="#FFFBEB",
            border="#FDE68A",
        )

    # Correções automáticas
    if report.corrections:
        _render_corrections(report)

    # Espécies novas
    if report.new_species:
        _render_new_species(report)

    # Sem nenhum problema
    if report.can_proceed and not report.warnings and not report.corrections:
        st.success("Planilha válida. Nenhuma correção necessária. Pronto para cadastramento.")


# ---------------------------------------------------------------------------
# Cards executivos
# ---------------------------------------------------------------------------

def _render_summary_cards(report: ValidationReport) -> None:
    st.subheader("Resumo da validação")

    # Cartão de status geral
    if not report.blocks:
        overall_status = "ok"
        overall_label = "Status geral"
        overall_value = "✅ Aprovada"
        overall_hint = "Sem bloqueios. Planilha pode ser cadastrada."
    else:
        overall_status = "error"
        overall_label = "Status geral"
        overall_value = "🚫 Bloqueada"
        overall_hint = f"{len(report.blocks)} bloqueio(s) precisam ser corrigidos."

    metrics = [
        {
            "label": overall_label,
            "value": overall_value,
            "hint": overall_hint,
            "status": overall_status,
        },
        {
            "label": "Total de linhas lidas",
            "value": str(report.total_rows),
            "hint": "Linhas na aba Especies (excluindo cabeçalho e linhas em branco)",
            "status": "info",
        },
        {
            "label": "Linhas válidas",
            "value": str(report.total_valid),
            "hint": "Sem erros de campo obrigatório",
            "status": "ok" if report.total_valid == report.total_rows else "warn",
        },
        {
            "label": "Espécies novas",
            "value": str(report.total_new),
            "hint": "Não encontradas no banco atual",
            "status": "info",
        },
        {
            "label": "Já cadastradas",
            "value": str(report.total_existing),
            "hint": "Serão atualizadas (upsert)",
            "status": "ok" if report.total_existing == 0 else "warn",
        },
        {
            "label": "Correções automáticas",
            "value": str(report.total_corrections),
            "hint": "Texto normalizado automaticamente",
            "status": "warn" if report.total_corrections > 0 else "ok",
        },
    ]

    cols = st.columns(len(metrics))
    for col, metric in zip(cols, metrics):
        col.markdown(
            f"""
            <div class="opyta-card opyta-card--{metric['status']}">
                <div class="opyta-card__label">{metric['label']}</div>
                <div class="opyta-card__value">{metric['value']}</div>
                <div class="opyta-card__hint">{metric['hint']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Seção de problemas (bloqueios ou warnings)
# ---------------------------------------------------------------------------

def _render_issues_section(
    issues: list,
    title: str,
    color: str,
    bg: str,
    border: str,
) -> None:
    items_html = "".join(
        f"<li><code>[{i.code}]</code> {i.message}</li>"
        for i in issues
    )
    st.markdown(
        f"""
        <div style="background:{bg}; border:1px solid {border};
                    border-radius:12px; padding:14px 16px; margin-bottom:1rem;">
            <div style="color:{color}; font-weight:700; margin-bottom:0.5rem;">{title}</div>
            <ul style="margin:0; padding-left:1.2rem; color:{color}; font-size:0.9rem;">
                {items_html}
            </ul>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ---------------------------------------------------------------------------
# Tabela de correções automáticas
# ---------------------------------------------------------------------------

def _render_corrections(report: ValidationReport) -> None:
    with st.expander(
        f"✏️ {report.total_corrections} correções automáticas aplicadas — clique para ver",
        expanded=False,
    ):
        import pandas as pd

        rows = [
            {
                "Linha Excel": c.row,
                "Coluna": c.column,
                "Original": c.original,
                "Corrigido": c.corrected,
                "Motivo": c.reason,
            }
            for c in report.corrections
        ]
        df_corr = pd.DataFrame(rows)
        st.dataframe(
            df_corr,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Linha Excel": st.column_config.NumberColumn(width="small"),
                "Coluna": st.column_config.TextColumn(width="medium"),
                "Original": st.column_config.TextColumn(width="large"),
                "Corrigido": st.column_config.TextColumn(width="large"),
                "Motivo": st.column_config.TextColumn(width="large"),
            },
        )


# ---------------------------------------------------------------------------
# Lista de espécies novas
# ---------------------------------------------------------------------------

def _render_new_species(report: ValidationReport) -> None:
    with st.expander(
        f"🆕 {report.total_new} espécie(s) novas — clique para ver a lista",
        expanded=True,
    ):
        for name in sorted(report.new_species):
            st.markdown(f"- *{name}*")
