from __future__ import annotations

import re
from typing import Iterable

import streamlit as st


def inject_saas_styles() -> None:
	st.markdown(
		"""
		<style>
		.opyta-card {
			border-radius: 14px;
			padding: 14px 16px;
			border: 1px solid rgba(49, 51, 63, 0.12);
			background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(249,250,251,0.96));
			min-height: 108px;
		}
		.opyta-card--ok { border-left: 5px solid #16A34A; }
		.opyta-card--warn { border-left: 5px solid #D97706; }
		.opyta-card--error { border-left: 5px solid #DC2626; }
		.opyta-card--info { border-left: 5px solid #2563EB; }
		.opyta-card__label {
			font-size: 0.84rem;
			color: #4B5563;
			margin-bottom: 0.3rem;
		}
		.opyta-card__value {
			font-size: 1.62rem;
			font-weight: 700;
			color: #111827;
			line-height: 1.2;
		}
		.opyta-card__hint {
			font-size: 0.78rem;
			color: #6B7280;
			margin-top: 0.45rem;
		}
		.opyta-step {
			border: 1px solid rgba(49, 51, 63, 0.2);
			border-radius: 10px;
			padding: 10px 12px;
			text-align: center;
			font-size: 0.85rem;
			min-height: 76px;
		}
		.opyta-step--done { background: #ECFDF3; border-color: #A7F3D0; }
		.opyta-step--active { background: #EFF6FF; border-color: #BFDBFE; }
		.opyta-step--pending { background: #F9FAFB; border-color: #E5E7EB; }
		.opyta-step--error { background: #FEF2F2; border-color: #FECACA; }
		.opyta-alert {
			border-radius: 12px;
			border: 1px solid #FDE68A;
			background: #FFFBEB;
			padding: 12px 14px;
			margin-bottom: 0.75rem;
		}
		.opyta-alert-title {
			color: #92400E;
			font-weight: 700;
			margin-bottom: 0.35rem;
		}
		</style>
		""",
		unsafe_allow_html=True,
	)


def render_stepper(steps: list[str], current_step: int, has_error: bool = False) -> None:
	cols = st.columns(len(steps))
	for idx, (col, step) in enumerate(zip(cols, steps)):
		if has_error and idx == current_step:
			state = "error"
			icon = "X"
			subtitle = "Erro"
		elif idx < current_step:
			state = "done"
			icon = "OK"
			subtitle = "Concluida"
		elif idx == current_step:
			state = "active"
			icon = "..."
			subtitle = "Em andamento"
		else:
			state = "pending"
			icon = "-"
			subtitle = "Pendente"

		col.markdown(
			f"""
			<div class="opyta-step opyta-step--{state}">
				<div><strong>{idx + 1}. {step}</strong></div>
				<div style="margin-top:6px; color:#374151;">{icon} {subtitle}</div>
			</div>
			""",
			unsafe_allow_html=True,
		)


def render_executive_summary(title: str, metrics: list[dict]) -> None:
	st.subheader(title)
	cols = st.columns(len(metrics))
	for col, metric in zip(cols, metrics):
		label = metric.get("label", "-")
		value = metric.get("value", "-")
		hint = metric.get("hint", "")
		status = metric.get("status", "info")
		col.markdown(
			f"""
			<div class="opyta-card opyta-card--{status}">
				<div class="opyta-card__label">{label}</div>
				<div class="opyta-card__value">{value}</div>
				<div class="opyta-card__hint">{hint}</div>
			</div>
			""",
			unsafe_allow_html=True,
		)


def render_alert_block(alerts: Iterable[str], title: str = "Alertas") -> None:
	alert_list = [a for a in alerts if str(a).strip()]
	if not alert_list:
		st.success("Sem alertas criticos detectados nesta execucao.")
		return

	items = "".join([f"<li>{a}</li>" for a in alert_list])
	st.markdown(
		f"""
		<div class="opyta-alert">
			<div class="opyta-alert-title">{title}</div>
			<ul style="margin:0; padding-left:1.2rem; color:#78350F;">
				{items}
			</ul>
		</div>
		""",
		unsafe_allow_html=True,
	)


def render_technical_log(stdout: str, stderr: str = "", title: str = "Ver log tecnico") -> None:
	with st.expander(title, expanded=False):
		if stdout.strip():
			st.markdown("**Saida principal**")
			st.code(stdout, language="text")
		if stderr.strip():
			st.markdown("**Saida de erro**")
			st.code(stderr, language="text")
		if not stdout.strip() and not stderr.strip():
			st.info("Sem mensagens tecnicas para exibir.")


def render_action_buttons(actions: list[dict]) -> str | None:
	if not actions:
		return None

	cols = st.columns(len(actions))
	clicked_key = None

	for col, action in zip(cols, actions):
		label = action.get("label", "Acao")
		key = action.get("key", label)
		primary = bool(action.get("primary", False))
		disabled = bool(action.get("disabled", False))
		if col.button(label, key=key, type="primary" if primary else "secondary", disabled=disabled):
			clicked_key = key

	return clicked_key


def extract_ignored_columns(stdout: str) -> list[str]:
	if not stdout:
		return []

	match = re.search(
		r"colunas\s+nao\s+encontradas.*?:\s*(.+)",
		stdout,
		re.IGNORECASE,
	)
	if not match:
		return []

	raw = match.group(1)
	return [part.strip() for part in raw.split(",") if part.strip()]


def extract_alert_lines(stdout: str, stderr: str = "") -> list[str]:
	text = "\n".join([stdout or "", stderr or ""])
	lines = [line.strip(" -") for line in text.splitlines() if line.strip()]
	alerts: list[str] = []

	for line in lines:
		lowered = line.lower()
		if "aviso" in lowered or "warning" in lowered:
			alerts.append(line)
			continue
		if "inconsist" in lowered:
			alerts.append(line)
			continue
		if "ignorada" in lowered or "ignoradas" in lowered:
			alerts.append(line)

	dedup: list[str] = []
	seen = set()
	for line in alerts:
		if line not in seen:
			dedup.append(line)
			seen.add(line)
	return dedup

