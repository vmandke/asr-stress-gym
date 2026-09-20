#!/usr/bin/env python3
"""Render M8's four portable SVG charts from benchmark CSVs.

SVG keeps `make bench` dependency-free: the artifacts open in a browser,
Markdown viewer, or dashboard later, without requiring matplotlib, a GUI,
or a platform-specific plotting package.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
from collections import defaultdict
from pathlib import Path


WIDTH, HEIGHT, LEFT, RIGHT, TOP, BOTTOM = 900, 460, 78, 30, 42, 66
COLORS = ["#38bdf8", "#f97316", "#a78bfa", "#22c55e", "#ef4444", "#eab308"]


def rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as fp:
        return list(csv.DictReader(fp))


def num(value: str | None) -> float | None:
    try:
        return float(value or "")
    except ValueError:
        return None


def svg_chart(title: str, x_label: str, y_label: str, series: dict[str, list[tuple[float, float]]], out: Path, marker: float | None = None) -> None:
    plot_w, plot_h = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM
    points = [point for values in series.values() for point in values]
    if not points:
        raise ValueError(f"{title}: no numeric samples")
    xs, ys = [p[0] for p in points], [p[1] for p in points]
    x_min, x_max = min(0.0, min(xs)), max(xs)
    y_min, y_max = 0.0, max(ys)
    if x_max <= x_min:
        x_max = x_min + 1
    if y_max <= y_min:
        y_max = y_min + 1

    def px(x: float) -> float:
        return LEFT + (x - x_min) / (x_max - x_min) * plot_w

    def py(y: float) -> float:
        return TOP + plot_h - (y - y_min) / (y_max - y_min) * plot_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        "<style>text{font-family:system-ui,sans-serif;fill:#dbeafe} .muted{fill:#94a3b8} .grid{stroke:#334155;stroke-width:1} .axis{stroke:#94a3b8;stroke-width:1.2}</style>",
        '<rect width="100%" height="100%" fill="#0f172a" rx="10"/>',
        f'<text x="{LEFT}" y="26" font-size="18" font-weight="700">{html.escape(title)}</text>',
    ]
    for i in range(5):
        y = y_min + (y_max - y_min) * i / 4
        line_y = py(y)
        parts.append(f'<line class="grid" x1="{LEFT}" y1="{line_y:.1f}" x2="{LEFT + plot_w}" y2="{line_y:.1f}"/>')
        parts.append(f'<text class="muted" x="{LEFT - 10}" y="{line_y + 4:.1f}" text-anchor="end" font-size="12">{y:.1f}</text>')
    parts.append(f'<line class="axis" x1="{LEFT}" y1="{TOP}" x2="{LEFT}" y2="{TOP + plot_h}"/>')
    parts.append(f'<line class="axis" x1="{LEFT}" y1="{TOP + plot_h}" x2="{LEFT + plot_w}" y2="{TOP + plot_h}"/>')
    for i in range(5):
        x = x_min + (x_max - x_min) * i / 4
        line_x = px(x)
        parts.append(f'<text class="muted" x="{line_x:.1f}" y="{TOP + plot_h + 20}" text-anchor="middle" font-size="12">{x:.1f}</text>')
    if marker is not None and x_min <= marker <= x_max:
        marker_x = px(marker)
        parts.append(f'<line x1="{marker_x:.1f}" y1="{TOP}" x2="{marker_x:.1f}" y2="{TOP + plot_h}" stroke="#f43f5e" stroke-width="2" stroke-dasharray="6 5"/>')
        parts.append(f'<text x="{marker_x + 5:.1f}" y="{TOP + 16}" fill="#fda4af" font-size="12">worker kill</text>')
    for index, (name, values) in enumerate(series.items()):
        color = COLORS[index % len(COLORS)]
        values = sorted(values)
        path = " ".join(("M" if i == 0 else "L") + f" {px(x):.1f} {py(y):.1f}" for i, (x, y) in enumerate(values))
        parts.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        legend_x = LEFT + index * 155
        parts.append(f'<line x1="{legend_x}" y1="{HEIGHT - 18}" x2="{legend_x + 20}" y2="{HEIGHT - 18}" stroke="{color}" stroke-width="3"/>')
        parts.append(f'<text x="{legend_x + 27}" y="{HEIGHT - 14}" font-size="12">{html.escape(name)}</text>')
    parts.append(f'<text class="muted" x="{LEFT + plot_w / 2:.1f}" y="{HEIGHT - 38}" text-anchor="middle" font-size="13">{html.escape(x_label)}</text>')
    parts.append(f'<text class="muted" transform="translate(18 {TOP + plot_h / 2:.1f}) rotate(-90)" text-anchor="middle" font-size="13">{html.escape(y_label)}</text>')
    parts.append("</svg>")
    out.write_text("\n".join(parts))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, type=Path)
    args = parser.parse_args()
    root = args.dir
    metadata = json.loads((root / "metadata.json").read_text()) if (root / "metadata.json").exists() else {}

    latency: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in rows(root / "latency-kill.csv"):
        x, y = num(row.get("ts_ms")), num(row.get("latency_ms"))
        if x is not None and y is not None and row.get("event") in {"partial", "final", "partial.reset"}:
            latency[row["event"]].append((x, y))
    svg_chart("Realtime latency over time", "milliseconds since load start", "client-observed latency (ms)", latency, root / "latency-over-time.svg", metadata.get("kill_ms"))

    concurrency: dict[str, list[tuple[float, float]]] = {"partial p95": [], "final p95": []}
    for row in rows(root / "capacity.csv"):
        x = num(row.get("streams"))
        if x is None:
            continue
        for name, field in (("partial p95", "partial_p95_ms"), ("final p95", "final_p95_ms")):
            y = num(row.get(field))
            if y is not None:
                concurrency[name].append((x, y))
    svg_chart("Latency versus concurrency", "requested concurrent streams", "p95 latency (ms)", concurrency, root / "latency-vs-concurrency.svg")

    cache: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in rows(root / "cache-vs-no-cache.csv"):
        x, y = num(row.get("elapsed_audio_ms")), num(row.get("inference_ms"))
        if x is not None and y is not None:
            cache[row["mode"]].append((x / 1000, y))
    svg_chart("Inference cost as an utterance grows", "utterance audio (seconds)", "worker inference time (ms)", cache, root / "inference-vs-utterance.svg")

    recovery: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for index, row in enumerate(rows(root / "recovery.csv")):
        y = num(row.get("recovery_ms"))
        if y is not None:
            recovery[row["mode"]].append((float(index + 1), y))
    svg_chart("Recovery time by mode", "benchmark case", "kill to partial.reset (ms)", recovery, root / "recovery-time-by-mode.svg")


if __name__ == "__main__":
    main()
