from __future__ import annotations
import base64
import csv
import html
import json
import logging
from pathlib import Path
from typing import Hashable

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .collector import VectorStatsCollector


logger = logging.getLogger(__name__)


def render_matrix(
    collector: VectorStatsCollector,
    *,
    features: list[str],
    partitions: bool = False,
    column_width: int = 6,
) -> None:
    status_map = (
        collector.group_partition_status
        if partitions
        else collector.group_feature_status
    )
    if not status_map or not features:
        return

    column_width = max(column_width, min(10, max(len(fid)
                       for fid in features)))

    def status_for(group: Hashable, fid: str) -> str:
        statuses = status_map.get(group, {})
        return statuses.get(fid, "absent")

    sorted_groups = sorted(status_map.keys(), key=collector._group_sort_key)
    focus_groups = [
        g
        for g in sorted_groups
        if any(status_for(g, fid) != "present" for fid in features)
    ]
    if not focus_groups:
        focus_groups = sorted_groups
    if collector.matrix_rows is not None:
        focus_groups = focus_groups[: collector.matrix_rows]

    matrix_label = "Partition" if partitions else "Feature"
    logger.info("\n-> %s availability heatmap:", matrix_label)

    header = " " * 20 + " ".join(
        f"{fid[-column_width:]:>{column_width}}" for fid in features
    )
    logger.info(header)

    for group in focus_groups:
        label = collector._format_group_key(group)
        label = label[:18].ljust(18)
        cells = " ".join(
            f"{collector._symbol_for(status_for(group, fid)):^{column_width}}"
            for fid in features
        )
        logger.info("  %s %s", label, cells)

    logger.info("    Legend: # present | ! null | . missing")


def export_matrix_data(collector: VectorStatsCollector) -> None:
    output = collector.matrix_output
    if not output:
        return

    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if collector.matrix_format == "html":
            _write_matrix_html(collector, path)
        else:
            _write_matrix_csv(collector, path)
        message = f"[write] Saved availability matrix to {path}"
        logger.info("\n%s", message)
    except OSError as exc:
        warning = f"[warn] Failed to write availability matrix to {path}: {exc}"
        logger.warning("\n%s", warning)


def _write_matrix_csv(collector: VectorStatsCollector, path: Path) -> None:
    rows: list[tuple[str, str, str, str]] = []
    for group, statuses in collector.group_feature_status.items():
        group_key = collector._format_group_key(group)
        for fid, status in statuses.items():
            rows.append(("feature", fid, group_key, status))

    for group, statuses in collector.group_partition_status.items():
        group_key = collector._format_group_key(group)
        for pid, status in statuses.items():
            rows.append(("partition", pid, group_key, status))

    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["kind", "identifier", "group_key", "status"])
        writer.writerows(rows)


def _write_matrix_html(collector: VectorStatsCollector, path: Path) -> None:
    feature_ids = collector._collect_feature_ids()
    partition_ids = collector._collect_partition_ids()
    group_keys = collector._collect_group_keys()

    sections: list[str] = []
    scripts: list[str] = []
    legend_entries: dict[str, tuple[str, str]] = {}

    def render_table(
        title: str,
        identifiers: list[str],
        status_map: dict,
        sub_map: dict,
        table_id: str,
    ) -> None:
        if not identifiers:
            sections.append(
                "<section class='matrix-section'>"
                f"<h2>{html.escape(title)}</h2>"
                "<p>No data.</p>"
                "</section>"
            )
            return

        base_class = {
            "present": "status-present",
            "null": "status-null",
            "absent": "status-absent",
        }

        statuses_found: set[str] = {"absent"}
        for statuses in status_map.values():
            statuses_found.update(statuses.values())
        for group_sub in sub_map.values():
            for sub_statuses in group_sub.values():
                statuses_found.update(sub_statuses)

        preferred_order = {"present": 0, "null": 1, "absent": 2}
        ordered_statuses = sorted(
            statuses_found,
            key=lambda s: (preferred_order.get(s, len(preferred_order)), s),
        )
        status_to_index = {
            status: idx for idx, status in enumerate(ordered_statuses)
        }
        default_index = status_to_index["absent"]

        row_labels = [collector._format_group_key(
            group) for group in group_keys]
        row_count = len(row_labels)
        col_count = len(identifiers)
        total_cells = row_count * col_count
        codes = bytearray(total_cells)
        sub_indices: dict[int, list[int]] = {}

        for row_idx, group in enumerate(group_keys):
            statuses = status_map.get(group, {})
            sub_cells = sub_map.get(group, {})
            for col_idx, identifier in enumerate(identifiers):
                cell_idx = row_idx * col_count + col_idx
                status = statuses.get(identifier, "absent")
                codes[cell_idx] = status_to_index.get(status, default_index)
                sub_statuses = sub_cells.get(identifier)
                if sub_statuses:
                    sub_indices[cell_idx] = [
                        status_to_index.get(sub_status, default_index)
                        for sub_status in sub_statuses
                    ]

        status_class_map = {
            status: base_class.get(status, "status-missing")
            for status in ordered_statuses
        }
        status_class_map["__default__"] = "status-missing"

        symbol_map = {
            status: collector._symbol_for(status) for status in ordered_statuses
        }
        symbol_map["__default__"] = "."

        payload = {
            "rows": row_labels,
            "cols": identifiers,
            "statuses": ordered_statuses,
            "encoded": base64.b64encode(bytes(codes)).decode("ascii"),
            "sub": sub_indices,
            "statusClass": status_class_map,
            "symbols": symbol_map,
        }

        header_cells = "".join(
            f"<th scope='col'>{html.escape(identifier)}</th>"
            for identifier in identifiers
        )

        sections.append(
            "<section class='matrix-section'>"
            f"<h2>{html.escape(title)}</h2>"
            "<div class='matrix-info'>Scroll horizontally and vertically to explore.</div>"
            f"<div class='table-container' id='{table_id}-container'>"
            "<table class='heatmap'>"
            "<thead>"
            "<tr>"
            "<th scope='col' class='group-col'>Group</th>"
            f"{header_cells}"
            "</tr>"
            "</thead>"
            f"<tbody id='{table_id}-body' data-colspan='{col_count + 1}'></tbody>"
            "</table>"
            "</div>"
            "</section>"
        )

        scripts.append(f"setupMatrix('{table_id}', {json.dumps(payload)});")

        for status, css_class in status_class_map.items():
            if status.startswith("__"):
                continue
            legend_entries.setdefault(
                status, (css_class, status.replace("_", " ").title())
            )

    render_table(
        "Feature Availability",
        feature_ids,
        collector.group_feature_status,
        collector.group_feature_sub,
        "feature",
    )
    render_table(
        "Partition Availability",
        partition_ids,
        collector.group_partition_status,
        collector.group_partition_sub,
        "partition",
    )

    for base_status, css in (
        ("present", "status-present"),
        ("null", "status-null"),
        ("absent", "status-absent"),
    ):
        legend_entries.setdefault(
            base_status, (css, base_status.replace("_", " ").title())
        )

    ordered_legend = sorted(
        legend_entries.items(),
        key=lambda item: (
            {"present": 0, "null": 1, "absent": 2}.get(item[0], 99),
            item[0],
        ),
    )
    legend_html = "".join(
        f"<span><span class='swatch {css}'></span>{label}</span>"
        for _, (css, label) in ordered_legend
    )

    style = """
        :root { color-scheme: light; }
        * { box-sizing: border-box; }
        body {
            font-family: Arial, sans-serif;
            margin: 24px;
            background: #f9f9fa;
            color: #222;
        }
        h1 { margin: 0; }
        .matrix-wrapper {
            border: 1px solid #d0d0d0;
            border-radius: 8px;
            background: #fff;
            box-shadow: 0 1px 3px rgba(0,0,0,0.08);
            overflow: hidden;
        }
        .matrix-header {
            padding: 16px 20px;
            border-bottom: 1px solid #e2e2e8;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 16px;
        }
        .matrix-header h1 {
            font-size: 22px;
        }
        .legend {
            display: inline-flex;
            flex-wrap: wrap;
            gap: 12px;
            font-size: 13px;
            color: #444;
        }
        .legend span {
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        .legend .swatch {
            width: 14px;
            height: 14px;
            border-radius: 3px;
            border: 1px solid rgba(0,0,0,0.08);
            background: #bdc3c7;
        }
        .matrix-section {
            padding: 18px 20px 24px;
            border-top: 1px solid #f0f0f4;
        }
        .matrix-section:first-of-type {
            border-top: none;
        }
        .matrix-section h2 {
            margin: 0 0 10px;
            font-size: 18px;
        }
        .matrix-info {
            font-size: 12px;
            color: #666;
            margin-bottom: 10px;
        }
        .table-container {
            border: 1px solid #d0d0d0;
            border-radius: 6px;
            overflow: auto;
            max-height: 55vh;
        }
        table.heatmap {
            border-collapse: collapse;
            min-width: 100%;
        }
        .heatmap th,
        .heatmap td {
            border: 1px solid #d0d0d0;
            padding: 0 5px;
            text-align: center;
            font-size: 13px;
            line-height: 1.2;
            vertical-align: middle;
        }
        .heatmap thead th {
            position: sticky;
            top: 0;
            background: #f3f3f6;
            z-index: 2;
        }
        .heatmap thead th.group-col,
        .heatmap tbody th.group-col {
            min-width: 160px;
        }
        .heatmap tbody th {
            position: sticky;
            left: 0;
            background: #fff;
            text-align: left;
            font-weight: normal;
            color: #333;
            z-index: 1;
        }
        .status-present { background: #2ecc71; color: #fff; font-weight: bold; }
        .status-null { background: #f1c40f; color: #000; font-weight: bold; }
        .status-absent { background: #e74c3c; color: #fff; font-weight: bold; }
        .status-missing { background: #bdc3c7; color: #000; font-weight: bold; }
        .sub {
            display: flex;
            gap: 5px;
            height: calc(100% - 2px);
            min-height: 24px;
            padding: 0 2px;
            margin: 1px 0;
            align-items: stretch;
            justify-content: center;
        }
        .sub span {
            flex: 1;
            display: block;
            position: relative;
            border-radius: 4px;
            overflow: hidden;
            border: 1px solid rgba(0,0,0,0.15);
            background: #fff;
            min-width: 12px;
        }
        .sub span::after {
            content: "";
            position: absolute;
            inset: 0;
            display: block;
            border-radius: 4px;
        }
        .sub .status-present::after { background: #2ecc71; }
        .sub .status-null::after { background: #f1c40f; }
        .sub .status-absent::after { background: #e74c3c; }
        .sub .status-missing::after { background: #bdc3c7; }
    """

    script = """
        function setupMatrix(rootId, payload) {
            const container = document.getElementById(rootId + "-container");
            const tbody = document.getElementById(rootId + "-body");
            if (!container || !tbody) {
                return;
            }

            const rows = payload.rows;
            const cols = payload.cols;
            const statuses = payload.statuses;
            const encoded = payload.encoded;
            const sub = payload.sub || {};
            const statusClass = payload.statusClass || {};
            const symbols = payload.symbols || {};
            const colCount = cols.length;
            const totalRows = rows.length;
            const data = decode(encoded);

            const defaultClass = statusClass["__default__"] || "status-missing";
            const defaultSymbol = symbols["__default__"] || ".";

            const rowHeightEstimate = 28;
            let rowHeight = rowHeightEstimate;
            let previousStart = -1;

            renderInitial();

            container.addEventListener("scroll", () => {
                window.requestAnimationFrame(renderVisibleRows);
            });

            function renderInitial() {
                renderVisibleRows();
                const sampleRow = tbody.querySelector("tr.data-row");
                if (sampleRow) {
                    rowHeight = sampleRow.getBoundingClientRect().height || rowHeightEstimate;
                    previousStart = -1;
                    renderVisibleRows();
                }
            }

            function renderVisibleRows() {
                const visibleHeight = container.clientHeight;
                const scrollTop = container.scrollTop;
                const buffer = 20;

                const start = Math.max(0, Math.floor(scrollTop / rowHeight) - buffer);
                const visibleCount = Math.ceil(visibleHeight / rowHeight) + buffer * 2;
                const end = Math.min(totalRows, start + visibleCount);

                if (start === previousStart) {
                    return;
                }
                previousStart = start;

                const topSpacer = start * rowHeight;
                const bottomSpacer = Math.max(0, (totalRows - end) * rowHeight);
                const colspan = Number(tbody.dataset.colspan) || (colCount + 1);

                let html = "";

                if (topSpacer > 0) {
                    html += `<tr class="virtual-spacer"><td colspan="${colspan}" style="height:${topSpacer}px;border:none;padding:0;"></td></tr>`;
                }

                for (let rowIdx = start; rowIdx < end; rowIdx++) {
                    html += buildRow(rowIdx);
                }

                if (bottomSpacer > 0) {
                    html += `<tr class="virtual-spacer"><td colspan="${colspan}" style="height:${bottomSpacer}px;border:none;padding:0;"></td></tr>`;
                }

                tbody.innerHTML = html;
            }

            function buildRow(rowIdx) {
                const group = rows[rowIdx];
                const rowLabel = escapeHtml(group);
                const startOffset = rowIdx * colCount;
                let cells = "";

                for (let colIdx = 0; colIdx < colCount; colIdx++) {
                    const cellIndex = startOffset + colIdx;
                    const code = data[cellIndex];
                    const status = statuses[code] || "absent";
                    const cssClass = statusClass[status] || defaultClass;
                    const title = escapeHtml(status);
                    const symbol = symbols[status] !== undefined ? symbols[status] : defaultSymbol;
                    const subEntry = sub[cellIndex];

                    if (subEntry && subEntry.length) {
                        const spans = subEntry.map((subCode) => {
                            const subStatus = statuses[subCode] || "absent";
                            const subClass = statusClass[subStatus] || defaultClass;
                            return `<span class="${subClass}" title="${escapeHtml(subStatus)}"></span>`;
                        }).join("");
                        cells += `<td title="${title}"><div class="sub">${spans}</div></td>`;
                    } else {
                        cells += `<td class="${cssClass}" title="${title}">${escapeHtml(symbol)}</td>`;
                    }
                }

                return `<tr class="data-row"><th scope="row" class="group-col">${rowLabel}</th>${cells}</tr>`;
            }
        }

        function decode(data) {
            const binary = atob(data);
            const arr = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; i++) {
                arr[i] = binary.charCodeAt(i);
            }
            return arr;
        }

        function escapeHtml(value) {
            return String(value)
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }
    """

    script_calls = "\n".join(scripts)

    html_output = (
        "<html><head><meta charset='utf-8'>"
        f"<style>{style}</style>"
        "<title>Feature Availability</title></head><body>"
        "<div class='matrix-wrapper'>"
        "<div class='matrix-header'>"
        "<h1>Availability Matrix</h1>"
        f"<div class='legend'>{legend_html}</div>"
        "<div style='margin-left:auto;font-size:12px;color:#666;'>Scroll to inspect large matrices.</div>"
        "</div>"
        f"{''.join(sections)}"
        f"<script>{script}{script_calls}</script>"
        "</div>"
        "</body></html>"
    )

    with path.open("w", encoding="utf-8") as fh:
        fh.write(html_output)
