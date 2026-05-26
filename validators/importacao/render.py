"""
validators/importacao/render.py
Renderiza o ValidationReport em componentes Streamlit.
Depende dos estilos `.opyta-card` já injetados por inject_saas_styles().
"""
from __future__ import annotations

import streamlit as st

from .report import ValidationReport


def render_validation_report(report: ValidationReport) -> None:
    """
    Exibe o relatório completo de validação da importação na interface.
    Deve ser chamado após inject_saas_styles().
    """
    _render_summary_cards(report)
    st.markdown("<br>", unsafe_allow_html=True)

    # Bloqueios — parar o processo visualmente
    if report.blocks:
        _render_issues_section(
            issues=report.blocks,
            title="🚫 Bloqueios — o arquivo não pode ser migrado",
            color="#DC2626",
            bg="#FEF2F2",
            border="#FECACA",
        )

    # Warnings
    if report.warnings:
        _render_issues_section(
            issues=report.warnings,
            title="⚠️ Avisos — dados que precisam atenção",
            color="#92400E",
            bg="#FFFBEB",
            border="#FDE68A",
        )

    # Infos
    if report.infos:
        _render_issues_section(
            issues=report.infos,
            title="ℹ️ Informações — o que será validado",
            color="#0369A1",
            bg="#F0F9FF",
            border="#BAE6FD",
        )

    # Espécies desconhecidas (if any)
    if report.especies_desconhecidas:
        _render_unknown_species(report)

    # Sem nenhum problema
    if report.can_proceed and not report.warnings and not report.infos:
        st.success("Arquivo validado com sucesso. Pronto para migração.")


# ---------------------------------------------------------------------------
# Cards executivos
# ---------------------------------------------------------------------------

def _render_summary_cards(report: ValidationReport) -> None:
    st.subheader("Resumo da validação")

    # Cartão de status geral
    if not report.blocks:
        overall_status = "ok"
        overall_label = "Status geral"
        overall_value = "✅ Aprovado"
        overall_hint = "Sem bloqueios. Arquivo pode ser migrado."
    else:
        overall_status = "error"
        overall_label = "Status geral"
        overall_value = "🚫 Bloqueado"
        overall_hint = f"{len(report.blocks)} bloqueio(s) impedem a migração."

    metrics = [
        {
            "label": overall_label,
            "value": overall_value,
            "hint": overall_hint,
            "status": overall_status,
        },
        {
            "label": "Campanhas",
            "value": str(report.total_campanhas),
            "hint": "Número de campanhas na aba Pontos",
            "status": "info",
        },
        {
            "label": "Pontos de coleta",
            "value": str(report.total_pontos),
            "hint": "Número de pontos na aba Pontos",
            "status": "info",
        },
        {
            "label": "Registros",
            "value": str(report.total_registros),
            "hint": "Número de registros nos resultados",
            "status": "ok" if report.total_registros > 0 else "warn",
        },
        {
            "label": "Cadastro spp.",
            "value": str(report.total_cadastro_especies),
            "hint": "Especies na aba Cadastro_Especies/Especies",
            "status": "ok" if report.total_cadastro_especies > 0 else "info",
        },
        {
            "label": "Esforco total",
            "value": f"{report.total_esforco_dias:.1f}d",
            "hint": "Dias de amostragem",
            "status": "info",
        },
        {
            "label": "Espécies desconhecidas",
            "value": str(report.total_especies_desconhecidas),
            "hint": "Não encontradas no catálogo",
            "status": "warn" if report.total_especies_desconhecidas > 0 else "ok",
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
# Seção de problemas
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
# Lista de espécies desconhecidas
# ---------------------------------------------------------------------------

def _render_unknown_species(report: ValidationReport) -> None:
    with st.expander(
        f"🆕 {report.total_especies_desconhecidas} espécie(s) desconhecida(s) — clique para ver",
        expanded=False,
    ):
        for species in report.especies_desconhecidas:
            st.markdown(f"- *{species}*")
        if len(report.especies_desconhecidas) < report.total_especies_desconhecidas:
            st.caption(
                f"Mostrando 50 de {report.total_especies_desconhecidas} espécies desconhecidas."
            )
