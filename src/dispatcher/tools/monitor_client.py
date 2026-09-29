"""Terminal-monitor state + rendering (Rich). The CLI glues these
to the SSE stream; kept separate so the fold/render logic tests
without a terminal."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from collections.abc import Iterable, Iterator

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


@dataclass
class MonitorState:
  """Folds SSE frames into cluster + per-attempt dicts. Payload
  shapes are the server's; stored as-is."""

  cluster: dict[str, Any] = field(default_factory=dict)
  attempts: dict[str, dict[str, Any]] = field(default_factory=dict)
  attempt_order: list[str] = field(default_factory=list)
  last_heartbeat_at: str | None = None
  connected: bool = False
  connection_error: str | None = None

  def apply(self, event_type: str, payload: dict[str, Any]) -> None:
    if event_type == "snapshot":
      self.cluster = payload
    elif event_type == "cluster_updated":
      self.cluster.update(payload)
    elif event_type in ("attempt_updated", "attempt_submitted"):
      aid = payload.get("attempt_id")
      if aid is None:
        return
      if aid not in self.attempts:
        self.attempt_order.append(aid)
      self.attempts[aid] = payload
    elif event_type == "attempt_cancelled":
      # No more attempt_updated frames will follow (the server
      # popped it first); badge the last snapshot instead of
      # dropping the row.
      aid = payload.get("attempt_id")
      if aid is None or aid not in self.attempts:
        return
      self.attempts[aid] = {
        **self.attempts[aid],
        "cancelled": True,
      }
    elif event_type == "heartbeat":
      at = payload.get("at")
      if at is not None:
        self.last_heartbeat_at = at


def parse_sse_lines(
  lines: Iterable[str],
) -> Iterator[tuple[str, dict[str, Any]]]:
  """`(event_type, payload)` frames out of SSE lines; unknown
  fields and unparseable data are dropped."""
  current_event: str | None = None
  current_data: str | None = None
  for line in lines:
    if line == "":
      if current_event is not None and current_data is not None:
        try:
          payload = json.loads(current_data)
        except json.JSONDecodeError:
          payload = None
        if payload is not None:
          yield current_event, payload
      current_event = None
      current_data = None
      continue
    if line.startswith("event: "):
      current_event = line[len("event: ") :]
    elif line.startswith("data: "):
      current_data = line[len("data: ") :]


# ── rendering ────────────────────────────────────────────────────

_PROGRESS_WIDTH = 22


def _progress_bar(counts: dict[str, int]) -> Text:
  """green=ok, yellow=err, magenta=ghosted+unknown (no evidence
  yet — NOT folded into err), cyan=running, dim=pending. The %
  counts evidence-based terminals only."""
  total = max(1, counts.get("total", 0))
  ok = counts.get("done_ok", 0)
  err = counts.get("done_err", 0)
  unresolved = counts.get("ghosted", 0) + counts.get("unknown", 0)
  running = counts.get("running", 0)

  ok_w = int(_PROGRESS_WIDTH * ok / total)
  err_w = int(_PROGRESS_WIDTH * err / total)
  unres_w = int(_PROGRESS_WIDTH * unresolved / total)
  run_w = int(_PROGRESS_WIDTH * running / total)
  pnd_w = _PROGRESS_WIDTH - ok_w - err_w - unres_w - run_w
  if pnd_w < 0:
    run_w += pnd_w
    pnd_w = 0
    if run_w < 0:
      unres_w += run_w
      run_w = 0
    if unres_w < 0:
      err_w += unres_w
      unres_w = 0

  bar = Text()
  bar.append("█" * ok_w, style="bold green")
  bar.append("▓" * err_w, style="bold yellow")
  bar.append("▒" * unres_w, style="bold magenta")
  bar.append("▒" * run_w, style="bold cyan")
  bar.append("░" * pnd_w, style="dim")
  pct = int(100 * (ok + err) / total)
  bar.append(f" {pct:>3d}%", style="dim")
  return bar


def primary_metric(
  metrics: dict[str, Any],
) -> tuple[str, float] | None:
  """The value key shown in the compact table: 'reward' if the
  attempt reports it, else the alphabetically-first mean."""
  means = (metrics or {}).get("means") or {}
  if not means:
    return None
  if "reward" in means:
    return "reward", float(means["reward"])
  key = sorted(means)[0]
  return key, float(means[key])


def _metric_cell(metrics: dict[str, Any]) -> str:
  pm = primary_metric(metrics)
  if pm is None:
    return "-"
  return f"{pm[1]:.2f}"


def _attempt_label(attempt: dict[str, Any]) -> Text:
  label = Text(
    attempt.get("alias")
    or attempt.get("label")
    or attempt.get("attempt_id", "?")
  )
  if attempt.get("cancelled"):
    label.append(" [cancelled]", style="red")
  elif attempt.get("paused"):
    label.append(" [paused]", style="yellow")
  return label


def render_compact(state: MonitorState):
  cfg = state.cluster.get("settings", {}) or {}
  self_host = state.cluster.get("self_host", "?")
  cap = cfg.get("max_concurrent", "?")
  running_total = state.cluster.get("running_total", 0)
  per_host = state.cluster.get("running_per_host", {}) or {}
  hosts_cfg = cfg.get("hosts", {}) or {}
  per_host_bits = " ".join(
    f"{h}{'*' if not hs.get('active', True) else ''} "
    f"{per_host.get(h, 0)}/{hs.get('max_concurrent', '?')}"
    for h, hs in hosts_cfg.items()
  )

  header = Text()
  header.append("dispatcher monitor", style="bold")
  header.append(f"  · {self_host}", style="dim")
  header.append(f"  · running {running_total}/{cap}", style="cyan")
  if per_host_bits:
    header.append(f"  · {per_host_bits}", style="dim")
  if not state.connected:
    header.append("  ·  ", style="dim")
    header.append("disconnected", style="bold red")
    if state.connection_error:
      header.append(f" ({state.connection_error})", style="red")

  table = Table(
    expand=False,
    show_lines=False,
    padding=(0, 1),
    header_style="bold",
  )
  table.add_column("attempt", no_wrap=False)
  table.add_column("ok", justify="right")
  table.add_column("err", justify="right")
  table.add_column("run", justify="right")
  table.add_column("pnd", justify="right")
  table.add_column("tot", justify="right")
  table.add_column("metric", justify="right")
  table.add_column("progress")

  if not state.attempts:
    table.add_row("(no attempts)", "-", "-", "-", "-", "-", "-", "")
  else:
    for aid in state.attempt_order:
      attempt = state.attempts.get(aid)
      if attempt is None:
        continue
      counts = attempt.get("counts", {}) or {}
      metrics = attempt.get("metrics", {}) or {}
      table.add_row(
        _attempt_label(attempt),
        str(metrics.get("ok", counts.get("done_ok", 0))),
        str(metrics.get("err", counts.get("done_err", 0))),
        str(counts.get("running", 0)),
        str(counts.get("pending", 0)),
        str(counts.get("total", 0)),
        _metric_cell(metrics),
        _progress_bar(counts),
      )

  footer = Text("q or ctrl+c to quit", style="dim")
  return Group(header, table, footer)


def render_detail(state: MonitorState, attempt_id: str):
  attempt = state.attempts.get(attempt_id)
  if attempt is None:
    return Panel(
      Text(
        f"attempt {attempt_id!r} not in the monitor stream",
        style="red",
      ),
      title="dispatcher monitor",
    )
  counts = attempt.get("counts", {}) or {}
  metrics = attempt.get("metrics", {}) or {}

  summary = Table.grid(padding=(0, 2))
  summary.add_column(justify="right", style="bold")
  summary.add_column()
  summary.add_row("attempt_id", attempt.get("attempt_id", "?"))
  summary.add_row("alias", attempt.get("alias", "?"))
  summary.add_row("label", attempt.get("label", "?"))
  summary.add_row("state", "paused" if attempt.get("paused") else "active")
  summary.add_row(
    "weight / max_concurrent",
    f"{attempt.get('weight')} / {attempt.get('max_concurrent')}",
  )
  summary.add_row(
    "counts",
    f"ok {metrics.get('ok', 0)}  "
    f"err {metrics.get('err', 0)}  "
    f"run {counts.get('running', 0)}  "
    f"pnd {counts.get('pending', 0)}  "
    f"tot {counts.get('total', 0)}",
  )
  means = (metrics or {}).get("means") or {}
  if means:
    summary.add_row(
      "means",
      "  ".join(f"{k} {v:.3f}" for k, v in sorted(means.items())),
    )

  return Panel(
    Group(summary, Text(""), _progress_bar(counts)),
    title=attempt.get("alias") or attempt_id,
    subtitle="q or ctrl+c to quit",
  )
