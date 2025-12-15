#!/usr/bin/env python3
# coding: utf-8

"""
Переносит готовые компиляции из локальной папки output на сетевой диск y:\\output\\<дата>.
Использует помощники из main.py, чтобы сохранить единый формат комментариев и путей.

Запуск:
    python move_output.py --start 2025-11-26 --end 2025-11-28
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Tuple
import sqlite3
import time

from main import (
    get_conn,
    move_output_to_network_storage,
    combine_comments,
)


def parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as err:
        raise argparse.ArgumentTypeError(f"Некорректная дата '{value}': {err}")


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _move_for_date_once(target_date: date) -> Tuple[List[int], List[Tuple[int, str]]]:
    iso_date = target_date.isoformat()
    conn = get_conn()
    conn.execute("PRAGMA busy_timeout = 5000")
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, video_path, comments FROM compilations WHERE pmv_date = ? ORDER BY id",
            (iso_date,),
        )
        rows = cur.fetchall()
        moved_ids: List[int] = []
        skipped: List[Tuple[int, str]] = []

        for row in rows:
            pmv_id = int(row["id"])
            old_path = Path(row["video_path"])
            if not old_path.exists():
                skipped.append((pmv_id, f"файл отсутствует: {old_path}"))
                continue
            try:
                new_path, move_note = move_output_to_network_storage(old_path, date_folder=iso_date)
            except Exception as exc:  # noqa: BLE001
                skipped.append((pmv_id, f"ошибка переноса: {exc}"))
                continue

            updated_comments = combine_comments(row["comments"], move_note)
            cur.execute(
                "UPDATE compilations SET video_path = ?, comments = ? WHERE id = ?",
                (str(new_path.resolve()), updated_comments, pmv_id),
            )
            moved_ids.append(pmv_id)

        conn.commit()
        return moved_ids, skipped
    finally:
        conn.close()


def move_for_date(target_date: date) -> Tuple[List[int], List[Tuple[int, str]]]:
    attempts = 5
    for attempt in range(1, attempts + 1):
        try:
            return _move_for_date_once(target_date)
        except sqlite3.OperationalError as exc:
            if "database is locked" in str(exc).lower() and attempt < attempts:
                wait = attempt * 2
                print(f"[{target_date}] БД занята, повтор через {wait}с...")
                time.sleep(wait)
                continue
            raise


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Переносит готовые PMV в y:\\output\\<дата> и обновляет БД."
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        required=True,
        help="Начальная дата (включительно) в формате YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end",
        type=parse_date,
        required=True,
        help="Конечная дата (включительно) в формате YYYY-MM-DD.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    start_date: date = args.start
    end_date: date = args.end

    if end_date < start_date:
        parser.error("Конечная дата не может быть раньше начальной.")

    total_moved = 0
    total_skipped: List[Tuple[int, str]] = []

    for day in daterange(start_date, end_date):
        moved_ids, skipped = move_for_date(day)
        total_moved += len(moved_ids)
        total_skipped.extend(skipped)

        if moved_ids:
            print(f"[{day.isoformat()}] Перенесены PMV: {', '.join(map(str, moved_ids))}")
        else:
            print(f"[{day.isoformat()}] Нет файлов для переноса.")
        for pmv_id, reason in skipped:
            print(f"[{day.isoformat()}] Пропуск #{pmv_id}: {reason}")

    print("=" * 60)
    print(f"Всего перенесено: {total_moved}")
    if total_skipped:
        print(f"Пропущено: {len(total_skipped)}")
    else:
        print("Пропусков нет.")


if __name__ == "__main__":
    main()
