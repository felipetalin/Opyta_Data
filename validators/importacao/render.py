"""
validators/importacao/render.py
Renderiza o ValidationReport em componentes Streamlit.
Depende dos estilos `.opyta-card` já injetados por inject_saas_styles().

Além de listar os achados, esta versão permite que a pessoa corrija as
linhas afetadas diretamente na tela (quando a origem do achado é conhecida)
e exporta um checklist completo dos erros para quem preferir corrigir no
Excel original. A aplicação efetiva das correções (reescrever o Excel de
trabalho e revalidar) é feita pela página que chama este módulo — aqui só
coletamos as edições em `st.session_state` e registramos um manifesto em
`st.session_state[f"{key_prefix}_editor_registry"]`.
"""
from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from .messages import DEFAULT_FIX_GUIDANCE, EDITABLE_SHEET_KEYS, FIX_GUIDANCE, SHEET_LABELS
from .report import ValidationIssue, ValidationReport

_SHEET_TO_DF_ATTR = {
    "capa": "df_capa",
    "pontos": "df_pontos",
    "esforco": "df_esforco",
    "resultados": "df_resultados",
    "cadastro_especies": "df_cadastro_especies",
}

_SEVERITY_META = {
    "block": {
        "title": "Bloqueios — o arquivo não pode ser migrado",
        "color": "#DC2626",
        "bg": "#FEF2F2",
        "border": "#FECACA",
        "icon": "🚫",
        "expanded": True,
    },
    "warning": {
        "title": "Avisos — dados que precisam de atenção",
        "color": "#92400E",
        "bg": "#FFFBEB",
        "border": "#FDE68A",
        "icon": "⚠️",
        "expanded": False,
    },
    "info": {
        "title": "Informações — o que foi validado",
        "color": "#0369A1",
        "bg": "#F0F9FF",
        "border": "#BAE6FD",
        "icon": "ℹ️",
        "expanded": False,
    },
}


def render_validation_report(report: ValidationReport, *, key_prefix: str = "import") -> None:
    """
    Exibe o relatório completo de validação da importação na interface.
    Deve ser chamado após inject_saas_styles(), e de forma incondicional a
    cada rerun (fora de blocos `if botao:`) para que as tabelas editáveis
    não desapareçam quando a pessoa interage com elas.
    """
    _render_summary_cards(report)
    st.markdown("<br>", unsafe_allow_html=True)

    gen = st.session_state.get(f"{key_prefix}_correction_gen", 0)
    registry: list[dict] = []

    any_issue = False
    for severity in ("block", "warning", "info"):
        issues = [i for i in report.issues if i.severity == severity]
        if not issues:
            continue
        any_issue = True
        meta = _SEVERITY_META[severity]
        st.markdown(f"#### {meta['icon']} {meta['title']} ({len(issues)})")
        for i, issue in enumerate(issues):
            _render_issue(report, issue, meta, key_prefix, gen, i, registry)

    st.session_state[f"{key_prefix}_editor_registry"] = registry

    if report.especies_desconhecidas:
        _render_unknown_species_list(report)

    if not any_issue and report.can_proceed:
        st.success("Arquivo validado com sucesso. Pronto para migração.")

    _render_checklist_download(report, key_prefix)


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
# Um achado por vez: mensagem + orientação + (se possível) tabela editável
# ---------------------------------------------------------------------------

def _render_issue(
    report: ValidationReport,
    issue: ValidationIssue,
    meta: dict,
    key_prefix: str,
    gen: int,
    index_in_group: int,
    registry: list[dict],
) -> None:
    n_lines = len(issue.lines)
    sheet_label = SHEET_LABELS.get(issue.sheet, "-")
    header = f"[{issue.code}]"
    if n_lines:
        header += f" — {n_lines} linha(s) afetada(s) em {sheet_label}"
    elif issue.sheet:
        header += f" — {sheet_label}"

    with st.expander(header, expanded=meta["expanded"] and index_in_group == 0):
        st.markdown(
            f"<div style='color:{meta['color']}; font-size:0.92rem;'>{issue.message}</div>",
            unsafe_allow_html=True,
        )
        tip = FIX_GUIDANCE.get(issue.code, DEFAULT_FIX_GUIDANCE)
        st.markdown(f"**Como corrigir:** {tip}")

        df_attr = _SHEET_TO_DF_ATTR.get(issue.sheet or "")
        df_source = getattr(report, df_attr, None) if df_attr else None

        if not issue.lines or df_source is None or getattr(df_source, "empty", True):
            return

        row_indices = sorted({ln - 2 for ln in issue.lines if (ln - 2) in df_source.index})
        if not row_indices:
            return

        subset = df_source.loc[row_indices].copy()
        subset.insert(0, "_linha_excel", [idx + 2 for idx in row_indices])

        if issue.sheet not in EDITABLE_SHEET_KEYS:
            st.caption("Linhas afetadas (correção feita fora desta planilha):")
            st.dataframe(subset, hide_index=True, use_container_width=True)
            return

        st.caption(
            "Edite os valores diretamente abaixo para corrigir sem sair do app "
            "(depois clique em 'Aplicar correções e revalidar', logo abaixo do relatório):"
        )
        editor_key = f"{key_prefix}_edit_{gen}_{issue.sheet}_{issue.code}_{index_in_group}"
        edited = st.data_editor(
            subset,
            key=editor_key,
            disabled=["_linha_excel"],
            num_rows="fixed",
            hide_index=True,
            use_container_width=True,
        )
        st.session_state[f"{editor_key}__value"] = edited
        registry.append(
            {
                "sheet": issue.sheet,
                "value_key": f"{editor_key}__value",
                "row_indices": row_indices,
            }
        )


def _render_unknown_species_list(report: ValidationReport) -> None:
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


# ---------------------------------------------------------------------------
# Checklist exportável (para quem prefere corrigir fora do app)
# ---------------------------------------------------------------------------

def _render_checklist_download(report: ValidationReport, key_prefix: str) -> None:
    if not report.issues:
        return

    rows = []
    for issue in report.issues:
        rows.append(
            {
                "Severidade": issue.severity,
                "Codigo": issue.code,
                "Aba": SHEET_LABELS.get(issue.sheet, "-"),
                "Linhas_Excel": ", ".join(str(line) for line in issue.lines) or "-",
                "Mensagem": issue.message,
                "Como_corrigir": FIX_GUIDANCE.get(issue.code, DEFAULT_FIX_GUIDANCE),
            }
        )

    df_checklist = pd.DataFrame(rows)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_checklist.to_excel(writer, sheet_name="Checklist_Erros", index=False)

    st.download_button(
        label="⬇️ Baixar checklist completo de erros (Excel)",
        data=buffer.getvalue(),
        file_name="checklist_erros_validacao.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"{key_prefix}_download_checklist",
    )
