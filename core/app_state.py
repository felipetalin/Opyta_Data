from __future__ import annotations

import streamlit as st


STATUS_LABELS = {
    "pendente": ("❌", "pendente"),
    "em_andamento": ("⚠️", "proxima etapa recomendada"),
    "concluido": ("✅", "concluido"),
}

FLOW_STAGES = [
    ("base_mestre_status", "Base Mestre"),
    ("importacao_status", "Importacao"),
    ("consolidacao_status", "Consolidacao"),
    ("analises_status", "Analises"),
    ("exportacao_status", "Exportacao"),
]


def initialize_system_status() -> None:
    for key, _ in FLOW_STAGES:
        if key not in st.session_state:
            st.session_state[key] = "pendente"


def mark_stage_completed(stage_key: str) -> None:
    initialize_system_status()

    valid_keys = [key for key, _ in FLOW_STAGES]
    if stage_key not in valid_keys:
        return

    for key in valid_keys:
        if st.session_state.get(key) == "em_andamento":
            st.session_state[key] = "pendente"

    st.session_state[stage_key] = "concluido"

    current_index = valid_keys.index(stage_key)
    for next_key in valid_keys[current_index + 1:]:
        if st.session_state.get(next_key) != "concluido":
            st.session_state[next_key] = "em_andamento"
            break


def render_system_status() -> None:
    initialize_system_status()

    with st.container(border=True):
        st.markdown("### Estado do Sistema")

        for _, (key, label) in enumerate(FLOW_STAGES, start=1):
            icon, text = STATUS_LABELS.get(
                st.session_state.get(key, "pendente"),
                STATUS_LABELS["pendente"],
            )
            st.markdown(f"**{label}**: {icon} {text}")

        st.caption("O sistema indica a proxima etapa recomendada com base no fluxo operacional.")
