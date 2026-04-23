"""dashboard/chat_renderer.py
─────────────────────────────────────────────────────────────────────────
BNPL Pod — shared chat-bubble renderer (Layer 2 Tab 4 + Layer 1 agent log).

Produces HTML markup that mirrors the DOM shape of the Layer 1 React
agent-log component, so the same visual grammar (role-coloured avatar +
header chips + body) reads identically across the tear-sheet and the
risk terminal.

This module has no Streamlit dependency — callers pass JSONL row dicts
and embed the returned HTML via ``st.markdown(..., unsafe_allow_html=True)``.

Design tokens are imported from :mod:`dashboard.design_tokens`. If that
module is unavailable (defensive), we fall back to the verbatim hex
values so the renderer never crashes.
"""

from __future__ import annotations

import html
from typing import Any, Mapping

try:
    from dashboard.design_tokens import C, AGENT_COLORS
except Exception:  # pragma: no cover — renderer must never crash callers
    C = {  # type: ignore[assignment]
        "card": "#1E293B",
        "border": "#334155",
        "borderMuted": "#1F2937",
        "textPrimary": "#F8FAFC",
        "textSecondary": "#94A3B8",
        "textMuted": "#64748B",
        "accent": "#38BDF8",
        "warn": "#FBBF24",
        "critical": "#EF4444",
        "violet": "#8B5CF6",
    }
    AGENT_COLORS = {  # type: ignore[assignment]
        "MACRO": "#38BDF8",
        "QUANT": "#8B5CF6",
        "RISK": "#FBBF24",
        "ALL": "#94A3B8",
        "UNKNOWN": "#64748B",
    }


# Compact two-letter avatar tags. These match the Layer 1 React spec:
# MACRO → MC · QUANT → QT · RISK → RK.
_AVATAR = {
    "MACRO": "MC",
    "QUANT": "QT",
    "RISK": "RK",
    "ALL": "··",
    "UNKNOWN": "??",
}


def _agent_color(role: str) -> str:
    r = (role or "UNKNOWN").upper()
    return AGENT_COLORS.get(r, AGENT_COLORS.get("UNKNOWN", "#64748B"))


def _avatar(role: str) -> str:
    r = (role or "UNKNOWN").upper()
    return _AVATAR.get(r, "??")


def _short_model(model: str) -> str:
    """Strip vendor/path prefixes, keep up to 28 chars."""
    if not model:
        return "—"
    tail = str(model).split("/")[-1]
    return tail[:28]


def _esc(x: Any) -> str:
    return html.escape(str(x), quote=True)


def render_agent_row(
    agent: str | None,
    model: str | None,
    ts: str | None,
    latency_ms: int | float | None = None,
    tokens: int | float | None = None,
    msg: str | None = None,
    *,
    provider: str | None = None,
    error: str | None = None,
    truncate_chars: int = 400,
) -> str:
    """Return a single HTML block representing one agent-debate row.

    Parameters
    ----------
    agent
        Role tag (``MACRO`` / ``QUANT`` / ``RISK``). Case-insensitive.
    model
        Model identifier (e.g. ``nemotron-4-340b-instruct``). Will be
        truncated and vendor-stripped for display.
    ts
        ISO-8601 timestamp. Rendered verbatim after normalising the
        ``T`` separator to a space and trimming sub-second precision.
    latency_ms, tokens
        Telemetry chips. Either may be ``None``; those chips are then
        suppressed.
    msg
        Message body. Long bodies are truncated at ``truncate_chars``
        with a trailing ellipsis.
    provider
        Optional provider tag rendered beside the model chip.
    error
        If set, rendered in the critical-red colour below the body.
    truncate_chars
        Soft cap on body length (no expander in this renderer — Tab 4
        can wrap the returned HTML in an ``st.expander`` if needed).
    """
    role = (agent or "UNKNOWN").upper()
    avatar_bg = _agent_color(role)
    avatar_txt = _avatar(role)

    ts_clean = (ts or "").replace("T", " ")[:19] if ts else ""

    body = (msg or "").strip()
    if len(body) > truncate_chars:
        body = body[:truncate_chars].rstrip() + "…"

    chips: list[str] = []
    if model:
        chips.append(
            f"<span style='font-family:\"JetBrains Mono\",ui-monospace,monospace;"
            f"font-size:10px;color:{C['textSecondary']};"
            f"background:{C['borderMuted']};"
            f"padding:1px 6px;border-radius:3px;"
            f"border:1px solid {C['border']};'>"
            f"{_esc(_short_model(model))}</span>"
        )
    if provider:
        chips.append(
            f"<span style='font-family:\"JetBrains Mono\",ui-monospace,monospace;"
            f"font-size:10px;color:{C['textMuted']};'>· {_esc(provider)}</span>"
        )
    if ts_clean:
        chips.append(
            f"<span style='font-family:\"JetBrains Mono\",ui-monospace,monospace;"
            f"font-size:10px;color:{C['textMuted']};'>· {_esc(ts_clean)}</span>"
        )
    if latency_ms is not None:
        chips.append(
            f"<span style='font-family:\"JetBrains Mono\",ui-monospace,monospace;"
            f"font-size:10px;color:{C['textMuted']};'>"
            f"· {int(latency_ms)}ms</span>"
        )
    if tokens is not None:
        chips.append(
            f"<span style='font-family:\"JetBrains Mono\",ui-monospace,monospace;"
            f"font-size:10px;color:{C['textMuted']};'>"
            f"· {int(tokens)}tok</span>"
        )

    error_html = ""
    if error:
        err_txt = _esc(str(error)[:240])
        error_html = (
            f"<div style='margin-top:4px;font-size:11px;"
            f"color:{C['critical']};'>⚠ {err_txt}</div>"
        )

    body_html = ""
    if body:
        body_html = (
            f"<div style='margin-top:4px;font-size:12.5px;line-height:1.5;"
            f"color:{C['textPrimary']};white-space:pre-wrap;'>"
            f"{_esc(body)}</div>"
        )

    return (
        f"<div style='display:grid;grid-template-columns:34px 1fr;gap:10px;"
        f"align-items:flex-start;padding:10px 12px;"
        f"border-left:2px solid {avatar_bg};"
        f"border-bottom:1px solid {C['borderMuted']};"
        f"background:{C['card']};border-radius:4px;margin-bottom:6px;'>"
        # avatar
        f"<div style='width:32px;height:32px;border-radius:50%;"
        f"background:{avatar_bg};color:{C['card']};"
        f"display:flex;align-items:center;justify-content:center;"
        f"font-family:\"JetBrains Mono\",ui-monospace,monospace;"
        f"font-size:11px;font-weight:600;letter-spacing:0.05em;'>"
        f"{avatar_txt}</div>"
        # body column
        f"<div style='min-width:0;'>"
        # header row
        f"<div style='display:flex;flex-wrap:wrap;align-items:baseline;gap:6px;'>"
        f"<span style='font-family:Inter,system-ui,sans-serif;"
        f"font-weight:600;font-size:11.5px;color:{avatar_bg};"
        f"letter-spacing:0.05em;'>{_esc(role)}</span>"
        + "".join(chips)
        + "</div>"
        + body_html
        + error_html
        + "</div></div>"
    )


def render_agent_log(
    rows: list[Mapping[str, Any]],
    *,
    max_height_px: int = 520,
    role_filter: set[str] | None = None,
    truncate_chars: int = 400,
) -> str:
    """Render a scrollable chat-bubble list for ``rows``.

    Rows may use either the Layer 2 JSONL shape (``role``, ``provider``,
    ``model``, ``ts``, ``latency_ms``, ``meta.tokens``, ``meta.error``,
    optional ``message`` / ``prompt_hash``) or the Layer 1 snapshot
    shape (``agent``, ``model``, ``ts``, ``latencyMs``, ``tokens``,
    ``msg``). This helper normalises both.
    """
    if not rows:
        return (
            f"<div style='padding:14px;color:{C['textMuted']};"
            f"font-size:11.5px;text-align:center;border:1px dashed {C['border']};"
            f"border-radius:4px;'>no agent rows</div>"
        )

    blocks: list[str] = []
    for r in rows:
        role = str(r.get("role") or r.get("agent") or "unknown").upper()
        if role_filter and role not in role_filter:
            continue
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        tokens = r.get("tokens", None)
        if tokens is None and isinstance(meta, dict):
            tokens = meta.get("tokens")
        error = None
        if isinstance(meta, dict):
            error = meta.get("error")
        msg = r.get("msg") or r.get("message") or r.get("prompt_hash") or ""
        blocks.append(
            render_agent_row(
                agent=role,
                model=r.get("model"),
                ts=r.get("ts"),
                latency_ms=r.get("latency_ms", r.get("latencyMs")),
                tokens=tokens,
                msg=msg,
                provider=r.get("provider"),
                error=error,
                truncate_chars=truncate_chars,
            )
        )

    return (
        f"<div style='max-height:{int(max_height_px)}px;overflow-y:auto;"
        f"padding-right:6px;'>"
        + "".join(blocks)
        + "</div>"
    )


__all__ = ["render_agent_row", "render_agent_log"]
