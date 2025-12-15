from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Tuple


GroupKey = Tuple[str, str]


@dataclass
class ReportEnvironment:
    db_get_groups: Callable[[], Dict[GroupKey, List[sqlite3.Row]]]
    color_choices: Dict[str, Dict[str, str]]


def _format_size(num_bytes: int) -> str:
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "Б":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} {units[-1]}"


def _row_has_color(row: sqlite3.Row, emoji: str) -> bool:
    comments = (row["comments"] or "") if row else ""
    return emoji in comments


def build_color_group_report(
    env: ReportEnvironment,
    color_key: str,
    top_n: int = 15,
) -> str:
    choice = env.color_choices.get(color_key)
    if not choice:
        return "Неизвестный тип отчёта."
    emoji = choice["emoji"]
    label = choice["label"]

    groups = env.db_get_groups()
    entries: List[Tuple[GroupKey, int, int]] = []
    total_count = 0
    total_size = 0

    for key, rows in groups.items():
        count = 0
        size = 0
        for row in rows:
            if not _row_has_color(row, emoji):
                continue
            count += 1
            try:
                size += int(row["size_bytes"] or 0)
            except Exception:
                continue
        if count:
            entries.append((key, count, size))
            total_count += count
            total_size += size

    if not entries:
        return f"Нет исходников цвета {emoji} ({label})."

    entries.sort(key=lambda item: (-item[1], -item[2], item[0]))

    lines = [
        f"Отчёт: {emoji} + Группы",
        f"Всего {label} исходников: {total_count} шт · {_format_size(total_size)}",
        "",
        "Топ групп по количеству:",
    ]

    limited = entries[:top_n]
    for idx, (key, count, size) in enumerate(limited, 1):
        codec, resolution = key
        lines.append(f"{idx}. {codec} {resolution} — {count} шт · {_format_size(size)}")

    if len(entries) > len(limited):
        lines.append(f"... и ещё {len(entries) - len(limited)} групп.")

    return "\n".join(lines)

