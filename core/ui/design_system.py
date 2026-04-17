"""
Design System para Opyta - Componentes reutilizáveis

Objetivo: Padrão único de UX entre todas as páginas.
Filosofia: Mínimo, reutilizável, sem reinventar a roda.
"""

from __future__ import annotations

import streamlit as st


def render_metric_card(
    title: str,
    value: str | float | int,
    delta: str | float | None = None,
    help_text: str | None = None,
    col_width: float = 1,
) -> None:
    """
    Renderiza um card de métrica padronizado.
    
    Args:
        title: Rótulo da métrica
        value: Valor principal (número ou string formatada)
        delta: Mudança relativa (ex: "+5%", "-2 unidades")
        help_text: Tooltip opcional
        col_width: Fração de largura da coluna (1.0 = full width)
    
    Exemplo:
        render_metric_card("Riqueza Média", 45.3, "+2.1", "Espécies por ponto")
    """
    col1, col2, col3 = st.columns([col_width, 1 - col_width - 0.01, 0.01])
    with col1:
        st.metric(label=title, value=value, delta=delta, help=help_text)


def render_status_badge(
    label: str,
    status: str,
    icon: str = "",
) -> None:
    """
    Renderiza um badge de status (✅ Sucesso, ⏳ Processando, ❌ Erro, ⚠️ Aviso).
    
    Args:
        label: Texto descritivo
        status: Um de: "success", "processing", "error", "warning"
        icon: Ícone customizado (opcional, sobrescreve padrão)
    
    Exemplo:
        render_status_badge("Validação", "success")
        render_status_badge("Migração", "processing")
    """
    status_map = {
        "success": ("✅", "#00D084"),
        "processing": ("⏳", "#0066FF"),
        "error": ("❌", "#FF4444"),
        "warning": ("⚠️", "#FFAA00"),
    }
    
    icon_str, color = status_map.get(status.lower(), ("❓", "#666666"))
    if icon:
        icon_str = icon
    
    html = f'<span style="color: {color}; font-weight: bold;">{icon_str} {label}</span>'
    st.markdown(html, unsafe_allow_html=True)


def render_section_header(
    title: str,
    icon: str = "📊",
    subtitle: str | None = None,
) -> None:
    """
    Renderiza cabeçalho de seção padronizado com ícone.
    
    Args:
        title: Título principal
        icon: Ícone emoji (padrão: 📊)
        subtitle: Texto descritivo opcional (menor, cinza)
    
    Exemplo:
        render_section_header("Análise de Biodiversidade", "🦗", "Indicadores ecológicos por ponto")
    """
    st.markdown(f"### {icon} {title}")
    if subtitle:
        st.markdown(f"<small style='color: #666;'>{subtitle}</small>", unsafe_allow_html=True)
    st.divider()


def render_empty_state(
    title: str = "Nenhum dado disponível",
    icon: str = "📭",
    message: str | None = None,
) -> None:
    """
    Renderiza estado vazio padronizado com mensagem.
    
    Args:
        title: Título do estado vazio
        icon: Ícone emoji
        message: Mensagem descritiva (opcional)
    
    Exemplo:
        render_empty_state("Nenhuma campanha selecionada", "🎯", "Selecione um projeto para começar")
    """
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        st.write(f"## {icon}")
        st.write(f"**{title}**")
        if message:
            st.write(f"_{message}_")


def render_filter_pills(
    filters: dict[str, str | list[str]],
    on_clear: callable = None,
) -> None:
    """
    Renderiza pills de filtros ativos (com opção de clear).
    
    Args:
        filters: Dict com nome do filtro → valor(es) selecionado(s)
        on_clear: Callback ao clicar "Clear all"
    
    Exemplo:
        filters = {"Projeto": "Bacanga", "Campanhas": ["2024-01", "2024-02"]}
        render_filter_pills(filters, on_clear=lambda: st.rerun())
    """
    if not filters:
        return
    
    col1, col2 = st.columns([8, 2])
    
    with col1:
        pills = []
        for key, value in filters.items():
            if isinstance(value, list):
                value_str = f"{len(value)} selecionados"
            else:
                value_str = str(value)
            pills.append(f"**{key}**: {value_str}")
        
        st.markdown(" | ".join(pills))
    
    with col2:
        if st.button("🗑️ Limpar", key="clear_filters", use_container_width=True):
            if on_clear:
                on_clear()


def render_info_box(
    message: str,
    box_type: str = "info",
    icon: str | None = None,
) -> None:
    """
    Renderiza caixa de informação/aviso/erro padronizada.
    
    Args:
        message: Conteúdo da mensagem
        box_type: Um de: "info", "warning", "error", "success"
        icon: Ícone customizado (opcional)
    
    Exemplo:
        render_info_box("Validação com sucesso", "success", "✨")
    """
    icon_map = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
        "success": "✅",
    }
    
    icon_str = icon or icon_map.get(box_type.lower(), "📌")
    
    if box_type.lower() == "error":
        st.error(f"{icon_str} {message}")
    elif box_type.lower() == "warning":
        st.warning(f"{icon_str} {message}")
    elif box_type.lower() == "success":
        st.success(f"{icon_str} {message}")
    else:
        st.info(f"{icon_str} {message}")
