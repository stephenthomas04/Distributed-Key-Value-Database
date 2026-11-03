"""Render two standalone SVG charts from measured JSON, without dependencies."""
import html
import json
from pathlib import Path
import sys

root = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
rows = [json.loads((root / f"{m}-{c}.json").read_text())
        for m in ("read", "write") for c in (1, 16)]


def chart(filename, title, fields):
    if any(row[field] is None for row in rows for field, _ in fields):
        raise ValueError("cannot chart missing latency measurements")
    maximum = max(row[field] for row in rows for field, _ in fields) or 1
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="900" height="430" viewBox="0 0 900 430">',
             '<rect width="900" height="430" fill="white"/>',
             f'<text x="30" y="30" font-family="sans-serif" font-size="20">{html.escape(title)}</text>']
    colors = ["#2563eb", "#d97706", "#059669"]
    for idx, row in enumerate(rows):
        x = 70 + idx * 210
        parts.append(f'<text x="{x}" y="360" font-family="sans-serif">{row["mode"]} c={row["concurrency"]}</text>')
        for j, (field, label) in enumerate(fields):
            value = row[field]
            height = value / maximum * 230
            bx = x + j * 55
            parts.extend([f'<rect x="{bx}" y="{330-height:.2f}" width="44" height="{height:.2f}" fill="{colors[j]}"/>',
                          f'<text x="{bx}" y="{320-height:.2f}" font-family="sans-serif" font-size="11">{value:.2f}</text>'])
    for j, (_, label) in enumerate(fields):
        parts.append(f'<text x="{70+j*210}" y="400" fill="{colors[j]}" font-family="sans-serif">{html.escape(label)}</text>')
    parts.append('</svg>')
    (root / filename).write_text("\n".join(parts), encoding="utf-8")


chart("throughput.svg", "Successful operations per second", [("success_ops_per_second", "operations / second")])
chart("latency.svg", "Successful-operation latency (milliseconds)", [("p50_ms", "p50"), ("p95_ms", "p95"), ("p99_ms", "p99")])
