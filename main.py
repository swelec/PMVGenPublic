#!/usr/bin/env python3
# coding: utf-8

import contextlib
import os
import json
import re
import sqlite3
import subprocess
import tempfile
import time
import shutil
import random
import unicodedata
import shlex
from pathlib import Path
from datetime import datetime, date
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Tuple, Optional, Union, Awaitable, Set
from collections import defaultdict
import math
import wave
from array import array

from shutil import which

import zipfile
import urllib.request
import importlib.util
import asyncio
import sys
import types
from bisect import bisect_left, bisect_right
import heapq

try:
    import private_settings  # type: ignore
except ModuleNotFoundError:
    private_settings = types.SimpleNamespace()

_PRIVATE_SETTING_SENTINEL = object()


def _get_private_setting(name: str, default: Any = None) -> Any:
    value = getattr(private_settings, name, _PRIVATE_SETTING_SENTINEL)
    if value is not _PRIVATE_SETTING_SENTINEL:
        return value
    value = os.environ.get(name, _PRIVATE_SETTING_SENTINEL)
    if value is not _PRIVATE_SETTING_SENTINEL:
        return value
    return default


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _coerce_bool(value: Any, default: bool) -> bool:
    """
    ????????? ???????? ???????? ? bool, ??????????? ?????? ???? true/false, 1/0 ? ?.?.
    """
    if value in (_PRIVATE_SETTING_SENTINEL, None):
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "y"}:
        return True
    if text in {"0", "false", "no", "off", "n"}:
        return False
    return default


from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from scan import ScanEnvironment, run_scan
from reports import ReportEnvironment, build_color_group_report

# =========================
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# =========================

BUILD_NAME = "build3444"

SCRIPT_DIR = Path(__file__).resolve().parent

FFMPEG_ZIP_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"


def _ensure_ffmpeg_binaries() -> None:
    """
    Проверяет, есть ли ffmpeg.exe и ffprobe.exe рядом со скриптом.
    Если нет — скачивает последнюю сборку FFmpeg (release-essentials),
    вытаскивает бинарники и кладёт в SCRIPT_DIR.
    """
    ffmpeg_path = SCRIPT_DIR / "ffmpeg.exe"
    ffprobe_path = SCRIPT_DIR / "ffprobe.exe"

    if ffmpeg_path.exists() and ffprobe_path.exists():
        # Уже есть — ничего не делаем
        return

    print("[FFMPEG] ffmpeg.exe или ffprobe.exe не найдены. Скачиваю FFmpeg...")

    import tempfile
    import shutil

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            zip_path = tmpdir / "ffmpeg.zip"

            # Скачиваем архив
            print(f"[FFMPEG] Скачивание {FFMPEG_ZIP_URL} ...")
            urllib.request.urlretrieve(FFMPEG_ZIP_URL, zip_path)

            # Распаковываем
            print("[FFMPEG] Распаковка архива...")
            with zipfile.ZipFile(zip_path, "r") as zf:
                members = zf.namelist()

                # Ищем ffmpeg.exe и ffprobe.exe внутри архива
                ffmpeg_member = None
                ffprobe_member = None

                for m in members:
                    lower = m.lower()
                    if lower.endswith("bin/ffmpeg.exe"):
                        ffmpeg_member = m
                    elif lower.endswith("bin/ffprobe.exe"):
                        ffprobe_member = m

                if not ffmpeg_member or not ffprobe_member:
                    raise RuntimeError("Не удалось найти ffmpeg.exe или ffprobe.exe в архиве FFmpeg")

                # Извлекаем во временную папку
                zf.extract(ffmpeg_member, tmpdir)
                zf.extract(ffprobe_member, tmpdir)

                # Копируем рядом со скриптом
                extracted_ffmpeg = tmpdir / ffmpeg_member
                extracted_ffprobe = tmpdir / ffprobe_member

                shutil.copy2(extracted_ffmpeg, ffmpeg_path)
                shutil.copy2(extracted_ffprobe, ffprobe_path)

            print("[FFMPEG] ffmpeg.exe и ffprobe.exe скачаны и сохранены рядом со скриптом.")
    except Exception as e:
        raise RuntimeError(
            f"Не удалось автоматически скачать и установить FFmpeg: {e}\n"
            f"Попробуй установить FFmpeg вручную или через winget."
        )



def _locate_bin(name: str) -> str:
    """
    Ищем бинарник в таком порядке:
    1) В папке со скриптом (SCRIPT_DIR/ffmpeg.exe и т.п.)
    2) Для ffmpeg/ffprobe — пытаемся автоматически скачать
    3) В системном PATH (which)
    """
    exe_name = name + ".exe" if os.name == "nt" else name

    # 1. Локально, рядом со скриптом
    local_path = SCRIPT_DIR / exe_name
    if local_path.exists():
        return str(local_path)

    # 2. Для ffmpeg/ffprobe — автодокачка
    if name in ("ffmpeg", "ffprobe"):
        _ensure_ffmpeg_binaries()
        if local_path.exists():
            return str(local_path)

    # 3. В PATH
    found = which(name)
    if found:
        return found

    raise FileNotFoundError(f"Не удалось найти бинарник '{name}'. "
                            f"Попробуй установить его в PATH или положить {exe_name} рядом со скриптом.")

DB_PATH = SCRIPT_DIR / "pmv_bot.db"
OUTPUT_DIR = SCRIPT_DIR / "output"
_network_output_root_value = _get_private_setting("NETWORK_OUTPUT_ROOT")
_enable_network_copy_value = _get_private_setting("ENABLE_NETWORK_COPY")
ENABLE_NETWORK_COPY = _coerce_bool(_enable_network_copy_value, False)
NETWORK_OUTPUT_ROOT = (
    Path(str(_network_output_root_value))
    if _network_output_root_value
    else OUTPUT_DIR
)
MEDIA_PLAYER_EXECUTABLE = Path(r"C:\Program Files (x86)\K-Lite Codec Pack\MPC-HC64\mpc-hc64.exe")
TEMP_DIRS = [
    SCRIPT_DIR / "tmp",
    OUTPUT_DIR,
]
MUSIC_PROJECTS_DIR = SCRIPT_DIR / "music_projects"
MUSIC_INPUT_DIR = SCRIPT_DIR / "Music"

_music_generator_module = None
MUSIC_INPUT_DIR = SCRIPT_DIR / "Music"

DEFAULT_EXTS = {".mp4", ".mov", ".mkv", ".m4v"}

# ГЛОБАЛЬНЫЕ ПУТИ К FFMPEG/FFPROBE
FFMPEG_BIN = _locate_bin("ffmpeg")
FFPROBE_BIN = _locate_bin("ffprobe")

LOGS_DIR = SCRIPT_DIR / "logs"
RANDOMPMV_LOG_PATH = LOGS_DIR / "randompmv_history.jsonl"
CODEX_FEEDBACK_LOG_PATH = LOGS_DIR / "codex_feedback.jsonl"


# Параметры нарезки по умолчанию
DEFAULT_TARGET_MINUTES = 30       # если пользователь введёт чушь — подстрахуемся
PER_FILE_MIN_SECONDS = 300        # 5 минут (минимум, НО теперь не ломает таргет)
PER_FILE_MAX_SECONDS = 600        # 10 минут
RANDOM_SEED = 42
USE_TS_CONCAT = True              # как и раньше
MAX_OUTPUT_BYTES = 100 * 1024**3  # ограничение на размер итогового файла (~100 ГБ)
SNAP_TO_KEYFRAMES = True          # подстраивать старт клипов к предыдущему ключевому кадру
PER_DIR_MAX_FIRST_PASS = 1        # на первом проходе брать не более N исходников из папки
MIN_SMALL_CLIP_SECONDS = 3        # минимальная длина маленького клипа (сек)
ALLOWED_STRATEGIES = ["max_group", "weighted_random", "random"]
CURRENT_STRATEGY = "max_group"
GLITCH_EFFECTS_PER_VIDEO = 0      # сколько глитч-вставок делать на видео
TRANSITION_EFFECTS_PER_VIDEO = 0  # сколько переходов добавлять между клипами
FX_GLITCH_DURATION = 0.25         # длительность глитч-вставки (сек)
FX_TRANSITION_DURATION = 0.35     # длительность перехода (сек)
XFADE_TRANSITIONS = [
    "fade",
    "fadeblack",
    "fadewhite",
    "wipeleft",
    "wiperight",
    "wipeup",
    "wipedown",
    "smoothleft",
    "smoothright",
    "circleopen",
    "circleclose",
]
POI_POINTS_PER_MIN_RANGE = (1, 3)
POI_SPREAD_SECONDS = 2.0
POI_ANALYSIS_RESET = 1.0
POI_MAX_POINTS = 120
RATEGRP_PMV_MAX_QUEUE = 250
CLIP_HEAD_GUARD_SECONDS = 60
CLIP_TAIL_GUARD_SECONDS = 60
NAS_SSH_HOST = _get_private_setting("NAS_SSH_HOST", "")
NAS_SSH_PORT = _coerce_int(_get_private_setting("NAS_SSH_PORT", 22), 22)
NAS_SSH_USER = _get_private_setting("NAS_SSH_USER", "")
NAS_SSH_PASSWORD = _get_private_setting("NAS_SSH_PASSWORD", "")
NAS_SHARE_PREFIX = _get_private_setting("NAS_SHARE_PREFIX", "")
NAS_SHARE_ROOT = _get_private_setting("NAS_SHARE_ROOT", "")
NAS_SIM_REMOTE_ROOT = _get_private_setting("NAS_SIM_REMOTE_ROOT", "")
NAS_SYMLINK_COLOR_FOLDERS = {
    "green": "green",
    "yellow": "yellow",
    "red": "red",
    "pink": "pink",
    "blue": "blue",
    "favorite": "favorite",
    "inspect": "inspect",
    "delete": "delete",
}
NAS_SSH_TIMEOUT = _coerce_int(_get_private_setting("NAS_SSH_TIMEOUT", 30), 30)
NEWCOMPMUSIC_DURATION_BUCKETS = [
    ("short", "1–4 мин", 0, 4 * 60 + 59),
    ("medium", "5–8 мин", 5 * 60, 8 * 60 + 59),
    ("long", "9+ мин", 9 * 60, None),
]
NEWCOMPMUSIC_DURATION_LABELS = {key: label for key, label, _, _ in NEWCOMPMUSIC_DURATION_BUCKETS}

RANDOMPMV_COUNT_OPTIONS = [5, 10, 15, 20, 25, 30]
RANDOMPMV_MIN_BATCH = 1
RANDOMPMV_MAX_BATCH = 30
RANDOMPMV_SOURCES_PER_MINUTE = 5.0  # базовое значение по умолчанию, оставлено для совместимости
RANDOMPMV_MIN_SOURCES_PER_MINUTE = 2.0
RANDOMPMV_MAX_SOURCES_PER_MINUTE = 5.0
RANDOMPMV_FULL_RATIO_MINUTES = 10.0
RANDOMPMV_NEW_SOURCE_CHOICES = [5, 10, 15, 20, 30, 40, 50, 60]
BADCLIP_MAX_MATCHES = 10

# =========================
# Telegram доступ
# =========================
# ВПИШИ СВОИ ЗНАЧЕНИЯ:
TELEGRAM_BOT_TOKEN = _get_private_setting("TELEGRAM_BOT_TOKEN", "")
_allowed_user_id_value = _get_private_setting("ALLOWED_USER_ID")
ALLOWED_USER_ID = _coerce_int(_allowed_user_id_value, 0)  # твой Telegram user id (целое число)

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Не задан TELEGRAM_BOT_TOKEN")
if not ALLOWED_USER_ID:
    raise RuntimeError("Не задан ALLOWED_USER_ID")

# =========================
# БАЗА ДАННЫХ (SQLite)
# =========================

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # Таблица исходников
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_path TEXT NOT NULL UNIQUE,
            video_name TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            codec TEXT,
            resolution TEXT,
            pmv_list TEXT DEFAULT '',
            comments TEXT DEFAULT '',
            date_added TEXT NOT NULL
        )
        """
    )

    # Таблица компиляций
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS compilations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_path TEXT NOT NULL,
            pmv_date TEXT NOT NULL,
            source_ids TEXT NOT NULL,
            comments TEXT DEFAULT ''
        )
        """
    )

    # Таблица кнопок (теги)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS buttons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            short_name TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL
        )
        """
    )

    # Таблица папок загрузки
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS upload_folders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            folder_path TEXT NOT NULL UNIQUE,
            date_added TEXT NOT NULL,
            ignored INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute("PRAGMA table_info(upload_folders)")
    upload_cols = [row[1] for row in cur.fetchall()]
    if "ignored" not in upload_cols:
        cur.execute(
            "ALTER TABLE upload_folders ADD COLUMN ignored INTEGER NOT NULL DEFAULT 0"
        )

    # Таблица с рандомными названиями для PMV
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS random_names (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            adjective TEXT NOT NULL,
            noun TEXT NOT NULL,
            verb TEXT NOT NULL,
            number INTEGER NOT NULL
        )
        """
    )

    # Заполняем random_names, если пусто (10 строк)
    cur.execute("SELECT COUNT(*) AS cnt FROM random_names")
    cnt = cur.fetchone()["cnt"]
    if cnt == 0:
        rows = [
            ("тихий", "океан", "дрейфует", 1),
            ("яркий", "ветер", "поёт", 7),
            ("быстрый", "пульс", "замирает", 3),
            ("ночной", "город", "дышит", 9),
            ("медленный", "огонь", "танцует", 5),
            ("золотой", "закат", "тает", 2),
            ("лёгкий", "дым", "скользит", 8),
            ("глубокий", "ритм", "качает", 4),
            ("сумрачный", "свет", "манит", 6),
            ("нежный", "шторм", "шепчет", 10),
        ]
        cur.executemany(
            "INSERT INTO random_names (adjective, noun, verb, number) VALUES (?, ?, ?, ?)",
            rows,
        )

    conn.commit()
    conn.close()

def db_get_all_compilations() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    # свежие сверху
    cur.execute(
        """
        SELECT id, video_path, pmv_date, source_ids, comments
        FROM compilations
        ORDER BY pmv_date DESC, id DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def db_append_compilation_comment(comp_id: int, new_piece: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT comments FROM compilations WHERE id = ?", (comp_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    current = (row["comments"] or "").strip()
    if not current:
        updated = new_piece
    else:
        updated = current + " | " + new_piece
    cur.execute("UPDATE compilations SET comments = ? WHERE id = ?", (updated, comp_id))
    conn.commit()
    conn.close()


def db_append_source_comment(source_id: int, new_piece: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT comments FROM sources WHERE id = ?", (source_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    current = (row["comments"] or "").strip()
    if not current:
        updated = new_piece
    else:
        updated = current + " | " + new_piece
    cur.execute("UPDATE sources SET comments = ? WHERE id = ?", (updated, source_id))
    conn.commit()
    conn.close()


def combine_comments(*pieces: Optional[str]) -> str:
    parts = [p.strip() for p in pieces if p and p.strip()]
    return " | ".join(parts)


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
        fh.write("\n")


def log_randompmv_event(event: Dict[str, Any]) -> None:
    payload = dict(event)
    payload.setdefault("timestamp", datetime.now().isoformat(timespec="seconds"))
    _append_jsonl(RANDOMPMV_LOG_PATH, payload)


def _randompmv_compute_target_sources(duration_minutes: float) -> Tuple[int, float]:
    minutes = max(1.0, float(duration_minutes))
    if minutes >= RANDOMPMV_FULL_RATIO_MINUTES:
        per_minute = RANDOMPMV_MAX_SOURCES_PER_MINUTE
    else:
        span = max(1.0, RANDOMPMV_FULL_RATIO_MINUTES - 1.0)
        progress = max(0.0, min(1.0, (minutes - 1.0) / span))
        per_minute = RANDOMPMV_MIN_SOURCES_PER_MINUTE + (
            (RANDOMPMV_MAX_SOURCES_PER_MINUTE - RANDOMPMV_MIN_SOURCES_PER_MINUTE) * progress
        )
    per_minute = max(
        RANDOMPMV_MIN_SOURCES_PER_MINUTE,
        min(per_minute, RANDOMPMV_MAX_SOURCES_PER_MINUTE),
    )
    return max(1, math.ceil(minutes * per_minute)), per_minute


def log_codex_feedback(thought: str, assumption: str, actions: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
    entry: Dict[str, Any] = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "thought": thought,
        "assumption": assumption,
    }
    if actions:
        entry["actions"] = actions
    if meta:
        entry["meta"] = meta
    _append_jsonl(CODEX_FEEDBACK_LOG_PATH, entry)


def parse_source_id_list(field: str) -> List[int]:
    result: List[int] = []
    if not field:
        return result
    for token in field.replace(";", ",").split(","):
        token = token.strip()
        if token.isdigit():
            result.append(int(token))
    return result


def merge_pmv_lists(*values: Optional[str]) -> str:
    """
    Объединяет списки PMV-участий без дубликатов, сохраняя порядок появления.
    """
    merged: List[str] = []
    seen: Set[str] = set()
    for value in values:
        if not value:
            continue
        for piece in value.split(","):
            cleaned = piece.strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                merged.append(cleaned)
    return ", ".join(merged)


def db_add_upload_folder(folder_path: str, ignored: bool = False) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO upload_folders (folder_path, date_added, ignored)
        VALUES (?, ?, ?)
        ON CONFLICT(folder_path) DO UPDATE SET ignored = excluded.ignored
        """,
        (folder_path, date.today().isoformat(), int(ignored)),
    )
    conn.commit()
    conn.close()


def db_add_scan_ignore(folder_path: str) -> None:
    db_add_upload_folder(folder_path, ignored=True)


def db_get_upload_folders(include_ignored: bool = False) -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    if include_ignored:
        cur.execute("SELECT * FROM upload_folders ORDER BY id")
    else:
        cur.execute("SELECT * FROM upload_folders WHERE ignored = 0 ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    return rows


def db_get_scan_ignored_folders() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM upload_folders WHERE ignored = 1 ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    return rows


def db_insert_source(
    video_path: Path,
    codec: str,
    resolution: str,
    size_bytes: Optional[int] = None,
    video_name: Optional[str] = None,
) -> Optional[int]:
    p = video_path.resolve()
    size_bytes = size_bytes if size_bytes is not None else p.stat().st_size
    video_name = video_name or p.name
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO sources (video_path, video_name, size_bytes, codec, resolution, date_added)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                str(p),
                video_name,
                size_bytes,
                codec,
                resolution,
                date.today().isoformat(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    except sqlite3.IntegrityError:
        return None
    finally:
        conn.close()


def db_get_unused_sources_grouped() -> Dict[Tuple[str, str], List[sqlite3.Row]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM sources
        WHERE pmv_list IS NULL OR pmv_list = ''
        """
    )
    rows = cur.fetchall()
    conn.close()

    groups: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for r in rows:
        codec = r["codec"] or "?"
        resolution = r["resolution"] or "??x??"
        key = (codec, resolution)
        groups.setdefault(key, []).append(r)
    return groups


def db_search_sources_by_term(term: str, limit: int = 50) -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    pattern = f"%{term.lower()}%"
    cur.execute(
        """
        SELECT *
        FROM sources
        WHERE lower(video_name) LIKE ? OR lower(video_path) LIKE ?
        ORDER BY date_added DESC, id DESC
        LIMIT ?
        """,
        (pattern, pattern, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def db_get_all_sources_grouped() -> Dict[Tuple[str, str], List[sqlite3.Row]]:
    """
    Берёт ВСЕ исходники (и уже участвовавшие, и нет)
    и группирует по (codec, resolution).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM sources")
    rows = cur.fetchall()
    conn.close()

    groups: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for r in rows:
        codec = r["codec"] or "?"
        resolution = r["resolution"] or "??x??"
        key = (codec, resolution)
        groups.setdefault(key, []).append(r)
    return groups


def load_music_projects() -> List[Dict[str, Any]]:
    projects: List[Dict[str, Any]] = []
    if not MUSIC_PROJECTS_DIR.exists():
        return projects

    usage_map = collect_music_project_usage()

    for entry in MUSIC_PROJECTS_DIR.iterdir():
        if not entry.is_dir():
            continue
        manifest_path = entry / "manifest.json"
        audio_path = entry / "audio.mp3"
        usage_info = usage_map.get(entry.name, {"count": 0, "last_date": None})
        try:
            manifest_data: Dict[str, Any] = {}
            segments_count = 0
            total_duration = None
            if manifest_path.exists():
                manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
                segments = (manifest_data.get("analysis") or {}).get("segments") or []
                segments_count = len(segments)
                if segments:
                    total_duration = float(segments[-1].get("end", 0.0) or 0.0)
            projects.append(
                {
                    "slug": entry.name,
                    "name": manifest_data.get("name") or entry.name,
                    "dir": entry,
                    "manifest_path": manifest_path,
                    "audio_path": audio_path if audio_path.exists() else None,
                    "segments_count": segments_count,
                    "duration": total_duration,
                    "manifest_data": manifest_data or None,
                    "usage_count": usage_info.get("count", 0),
                    "last_used": usage_info.get("last_date"),
                }
            )
        except Exception as exc:
            projects.append(
                {
                    "slug": entry.name,
                    "name": f"{entry.name} (ошибка манифеста: {exc})",
                    "dir": entry,
                    "manifest_path": manifest_path,
                    "audio_path": audio_path if audio_path.exists() else None,
                    "segments_count": 0,
                    "duration": None,
                    "manifest_data": None,
                    "usage_count": usage_info.get("count", 0),
                    "last_used": usage_info.get("last_date"),
                }
            )

    projects.sort(key=lambda p: (p.get("usage_count", 0), p["name"].lower()))
    return projects


def collect_music_project_usage() -> Dict[str, Dict[str, Any]]:
    usage: Dict[str, Dict[str, Any]] = {}
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT pmv_date, comments FROM compilations WHERE comments LIKE '%music_project=%'")
    rows = cur.fetchall()
    conn.close()

    for row in rows:
        comments = row["comments"] or ""
        date_str = row["pmv_date"]
        try:
            pmv_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            pmv_date = None
        for match in re.findall(r"music_project=([\w\-]+)", comments):
            info = usage.setdefault(match, {"count": 0, "last_date": None})
            info["count"] += 1
            if pmv_date and (info["last_date"] is None or pmv_date > info["last_date"]):
                info["last_date"] = pmv_date
    return usage


def _normalize_path_str(path_like: Union[str, Path]) -> str:
    if isinstance(path_like, Path):
        raw = path_like
    else:
        raw = Path(str(path_like))
    try:
        return str(raw.resolve(strict=False)).lower()
    except Exception:
        return str(raw).lower()


def _normalize_path_prefix(path_like: Union[str, Path]) -> str:
    normalized = _normalize_path_str(path_like)
    normalized = normalized.replace("\\", "/").rstrip("/")
    return normalized


def _is_path_under_prefixes(
    target_path: Union[str, Path], prefixes: Iterable[str]
) -> bool:
    target = _normalize_path_prefix(target_path)
    for prefix in prefixes:
        if not prefix:
            continue
        check = prefix.rstrip("/")
        if target == check or target.startswith(check + "/"):
            return True
    return False


def collect_music_track_usage() -> Dict[str, int]:
    usage: Dict[str, int] = {}
    if not MUSIC_PROJECTS_DIR.exists():
        return usage

    input_by_name: Dict[str, str] = {
        p.name.lower(): _normalize_path_str(p) for p in list_music_input_files()
    }

    for entry in MUSIC_PROJECTS_DIR.iterdir():
        if not entry.is_dir():
            continue
        manifest_path = entry / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        source_file = data.get("source_file") or data.get("original_audio")
        if not source_file:
            audio_path = data.get("audio_path")
            if audio_path:
                # Backward compatibility: older manifests did not store source_file.
                candidate = input_by_name.get(Path(audio_path).name.lower())
                if candidate:
                    source_file = candidate
        if not source_file:
            continue
        norm = _normalize_path_str(source_file)
        if not norm:
            continue
        usage[norm] = usage.get(norm, 0) + 1
    return usage


TRACK_SPLIT_RE = re.compile(r"\s*[-–—]\s*")
SLUG_TOKEN_RE = re.compile(r"[^a-z0-9]+")


def truncate_button_label(text: str, max_len: int = 30) -> str:
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def truncate_label_keep_suffix(text: str, max_len: int = 30) -> str:
    text = text.strip()
    if len(text) <= max_len:
        return text
    return "…" + text[-(max_len - 1):]


def extract_track_title_components(path: Path) -> Tuple[str, str]:
    stem = path.stem.strip()
    normalized = stem.replace("—", "-").replace("–", "-")
    parts = [p.strip() for p in normalized.split("-", 1)]
    if len(parts) == 2:
        artist, title = parts
    else:
        artist, title = "", stem
    return artist, title or stem


def slugify_token(value: str) -> str:
    if not value:
        return "project"
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = SLUG_TOKEN_RE.sub("_", normalized.lower()).strip("_")
    return slug or "project"


def build_main_reply_keyboard() -> ReplyKeyboardMarkup:
    """
    Постоянное меню снизу с основными командами.
    """
    rows = [
        [KeyboardButton("musicprep"), KeyboardButton("newcompmusic")],
        [KeyboardButton("rategrp"), KeyboardButton("Найти")],
        [KeyboardButton("CreateRandomPMV"), KeyboardButton("Отчёты")],
        [KeyboardButton("scan")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


def build_reports_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🟢 + Группы", callback_data="report_group:green")],
        [InlineKeyboardButton("🟡 + Группы", callback_data="report_group:yellow")],
        [InlineKeyboardButton("🔴 + Группы", callback_data="report_group:red")],
        [InlineKeyboardButton("🩷 + Группы", callback_data="report_group:pink")],
    ]
    return InlineKeyboardMarkup(rows)


def build_newcomp_duration_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"newcomp_bucket:{key}")]
        for key, label, _, _ in NEWCOMPMUSIC_DURATION_BUCKETS
    ]
    rows.append([InlineKeyboardButton("↩️ Новые проекты", callback_data="newcomp_show:unused")])
    return InlineKeyboardMarkup(rows)


def project_duration_seconds(project: Dict[str, Any]) -> Optional[int]:
    duration = project.get("duration")
    if duration is not None:
        try:
            return max(0, int(float(duration)))
        except Exception:
            pass
    manifest = project.get("manifest_data") or {}
    manifest_dur = manifest.get("duration")
    if manifest_dur:
        try:
            return max(0, int(float(manifest_dur)))
        except Exception:
            pass
    segments = project.get("parsed_segments") or []
    if segments:
        try:
            return max(0, int(float(segments[-1].end)))
        except Exception:
            pass
    return None


def project_matches_duration(project: Optional[Dict[str, Any]], bucket_key: Optional[str]) -> bool:
    if not bucket_key:
        return True
    if not project:
        return False
    seconds = project_duration_seconds(project)
    if seconds is None:
        return bucket_key == "long"
    for key, _, min_sec, max_sec in NEWCOMPMUSIC_DURATION_BUCKETS:
        if key != bucket_key:
            continue
        if max_sec is None:
            return seconds >= min_sec
        return min_sec <= seconds <= max_sec
    return True


def filter_project_tokens_by_duration(
    session: Dict[str, Any],
    tokens: List[str],
    bucket_key: Optional[str],
) -> List[str]:
    if not bucket_key:
        return tokens
    project_map: Dict[str, Dict[str, Any]] = session.get("music_projects_map") or {}
    filtered: List[str] = []
    for token in tokens:
        project = project_map.get(token)
        if project_matches_duration(project, bucket_key):
            filtered.append(token)
    return filtered


def build_musicprep_track_keyboard(
    session: Dict[str, Any],
    show_used: bool,
) -> Tuple[str, InlineKeyboardMarkup]:
    track_map: Dict[str, Dict[str, Any]] = session.get("music_tracks") or {}
    tokens = session.get("music_tracks_used" if show_used else "music_tracks_unused") or []
    def sort_key(token: str) -> Tuple[int, str]:
        info = track_map.get(token) or {}
        count = int(info.get("usage") or 0)
        path = Path(info.get("path") or "")
        _, title = extract_track_title_components(path)
        return (count, title.lower())
    tokens = sorted(tokens, key=sort_key)
    rows: List[List[InlineKeyboardButton]] = []
    for token in tokens:
        info = track_map.get(token)
        if not info:
            continue
        path = Path(info["path"])
        _, title = extract_track_title_components(path)
        count = int(info.get("usage") or 0)
        base_label = f"{count} · {title}"
        label = truncate_button_label(base_label)
        rows.append([InlineKeyboardButton(label or "?", callback_data=f"musicprep_track:{token}")])

    toggle_target = "unused" if show_used else "used"
    toggle_label = "Показать новые" if show_used else "Посмотреть использованные"
    rows.append([InlineKeyboardButton(toggle_label, callback_data=f"musicprep_show:{toggle_target}")])

    if not tokens:
        text = (
            "Использованных треков пока нет." if show_used else "Новых треков не найдено."
        )
    else:
        text = "🎵 Использованные треки:" if show_used else "🎵 Доступные треки (новые):"
    return text, InlineKeyboardMarkup(rows)


def build_musicprep_seconds_keyboard() -> InlineKeyboardMarkup:
    zero_row = [InlineKeyboardButton("0", callback_data="musicprep_seconds:0")]
    first_row = [InlineKeyboardButton(str(i), callback_data=f"musicprep_seconds:{i}") for i in range(1, 6)]
    second_row = [InlineKeyboardButton(str(i), callback_data=f"musicprep_seconds:{i}") for i in range(6, 11)]
    third_row = [InlineKeyboardButton(str(i), callback_data=f"musicprep_seconds:{i}") for i in range(11, 15)]
    fourth_row = [InlineKeyboardButton("15", callback_data="musicprep_seconds:15")]
    return InlineKeyboardMarkup([zero_row, first_row, second_row, third_row, fourth_row])


def build_musicprep_mode_keyboard() -> InlineKeyboardMarkup:
    row = [
        InlineKeyboardButton("beat", callback_data="musicprep_mode:beat"),
        InlineKeyboardButton("onset", callback_data="musicprep_mode:onset"),
        InlineKeyboardButton("uniform", callback_data="musicprep_mode:uniform"),
    ]
    return InlineKeyboardMarkup([row])


MUSICPREP_SENSITIVITY_PRESETS: Dict[str, List[Dict[str, Any]]] = {
    "beat": [
        {
            "key": "soft",
            "label": "Чувствительный (ловить хэты)",
            "description": "Добавляет сегменты на тихих долях.",
            "analysis_kwargs": {"beat_tightness": 0.6, "sensitivity_scale": 1.4},
        },
        {
            "key": "default",
            "label": "Стандартный",
            "description": "Сбалансированный режим.",
            "analysis_kwargs": {},
        },
        {
            "key": "tight",
            "label": "Только сильные доли",
            "description": "Пропускает слабые удары и тишину.",
            "analysis_kwargs": {"beat_tightness": 2.0, "sensitivity_scale": 0.85},
        },
    ],
    "onset": [
        {
            "key": "soft",
            "label": "Больше всплесков",
            "description": "Чувствительный к хай-хэтам.",
            "analysis_kwargs": {"onset_delta": 0.02, "sensitivity_scale": 1.5},
        },
        {
            "key": "default",
            "label": "Стандартный",
            "description": "Рекомендованные параметры.",
            "analysis_kwargs": {},
        },
        {
            "key": "tight",
            "label": "Только громкие пики",
            "description": "Фокус на мощных ударах.",
            "analysis_kwargs": {"onset_delta": 0.12, "sensitivity_scale": 0.8},
        },
    ],
}


def get_musicprep_sensitivity_options(mode: str) -> List[Dict[str, Any]]:
    return MUSICPREP_SENSITIVITY_PRESETS.get(mode, [])


def build_musicprep_sensitivity_keyboard(mode: str) -> InlineKeyboardMarkup:
    options = get_musicprep_sensitivity_options(mode)
    rows: List[List[InlineKeyboardButton]] = []
    for opt in options:
        rows.append(
            [
                InlineKeyboardButton(
                    opt["label"], callback_data=f"musicprep_sens:{mode}:{opt['key']}"
                )
            ]
        )
    if not rows:
        rows = [[InlineKeyboardButton("Стандарт", callback_data="musicprep_sens:auto:default")]]
    return InlineKeyboardMarkup(rows)


async def finalize_musicprep_project(
    send_func: Callable[[str], Awaitable[None]],
    sess: Dict[str, Any],
    user_id: int,
    mode: str,
    analysis_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    file_str = sess.get("musicprep_file")
    if not file_str:
        return await send_func("Не удалось определить выбранный трек.")

    file_path = Path(file_str)
    mod = load_music_generator_module()

    segment_len = sess.get("musicprep_segment")
    if segment_len is None:
        segment_len = getattr(mod, "DEFAULT_TARGET_SEGMENT", 1.0)

    project_partial = sess.get("musicprep_project_partial")
    if project_partial:
        project_name = f"{project_partial}_{mode}"
    else:
        project_name = sess.get("musicprep_name")

    try:
        manifest = mod.create_music_project(
            mp3_path=file_path,
            name=project_name,
            target_segment=float(segment_len),
            segment_mode=mode,
            analysis_kwargs=analysis_kwargs or {},
        )
    except Exception as exc:
        user_sessions.pop(user_id, None)
        return await send_func(f"Ошибка при создании музыкального проекта: {exc}")

    user_sessions.pop(user_id, None)
    await send_func(
        "✅ Музыкальный проект создан.\n"
        f"Имя: {manifest.name}\n"
        f"Slug: {manifest.slug}\n"
        f"Файл: {manifest.audio_path}\n"
        f"Сегментов: {len(manifest.analysis.segments)}\n"
        f"Режим: {manifest.analysis.mode}"
    )


def build_musicprepcheck_keyboard(projects: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    sorted_projects = sorted(
        projects,
        key=lambda proj: proj.get("manifest_data", {}).get("created_at")
        or proj.get("created_at")
        or "",
        reverse=True,
    )
    rows: List[List[InlineKeyboardButton]] = []
    for proj in sorted_projects:
        slug = proj.get("slug") or sanitize_filename(proj.get("name") or "project")
        name = proj.get("name") or proj.get("slug")
        segs = proj.get("segments_count") or 0
        label = truncate_label_keep_suffix(f"{name} ({segs} сег)")
        rows.append(
            [InlineKeyboardButton(label or slug or "?", callback_data=f"musicprepcheck_project:{slug}")]
        )
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("Нет проектов", callback_data="noop")]])


def build_newcomp_project_keyboard(session: Dict[str, Any], show_used: bool) -> Tuple[str, InlineKeyboardMarkup]:
    projects_map: Dict[str, Dict[str, Any]] = session.get("music_projects_map") or {}
    tokens = session.get("music_projects_used" if show_used else "music_projects_unused") or []
    duration_filter = session.get("music_projects_duration_filter") if show_used else None
    if show_used:
        tokens = filter_project_tokens_by_duration(session, tokens, duration_filter)
    rows: List[List[InlineKeyboardButton]] = []

    for token in tokens:
        proj = projects_map.get(token)
        if not proj:
            continue
        label = truncate_button_label(proj["name"])
        usage = proj.get("usage_count") or 0
        if show_used and usage:
            label = truncate_button_label(f"{label} ({usage})")
        rows.append([InlineKeyboardButton(label or token, callback_data=f"newcomp_project:{token}")])

    toggle_target = "unused" if show_used else "used"
    toggle_label = "Показать новые" if show_used else "Посмотреть использованные"
    if show_used:
        rows.append([InlineKeyboardButton("Изменить длительность", callback_data="newcomp_bucket_menu")])
    rows.append([InlineKeyboardButton(toggle_label, callback_data=f"newcomp_show:{toggle_target}")])

    if not tokens:
        if show_used:
            label = NEWCOMPMUSIC_DURATION_LABELS.get(duration_filter or "", "любой длительности")
            text = f"Используемых проектов ({label}) пока нет."
        else:
            text = "Новых проектов не найдено."
    else:
        if show_used:
            label = NEWCOMPMUSIC_DURATION_LABELS.get(duration_filter or "", "любой длительности")
            text = f"🎵 Используемые проекты ({label}):"
        else:
            text = "🎵 Доступные новые проекты:"
    return text, InlineKeyboardMarkup(rows)


async def prompt_newcomp_duration(
    session: Dict[str, Any],
    send_fn: Callable[[str, Optional[InlineKeyboardMarkup]], Awaitable[Any]],
) -> None:
    session["state"] = "newcompmusic_choose_duration"
    await send_fn("Выберите длительность используемых проектов:", build_newcomp_duration_keyboard())


def build_numeric_keyboard(prefix: str, total: int, per_row: int = 5) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for i in range(1, total + 1):
        row.append(InlineKeyboardButton(str(i), callback_data=f"{prefix}:{i}"))
        if len(row) >= per_row:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows or [[InlineKeyboardButton("1", callback_data=f"{prefix}:1")]])


NEWCOMPMUSIC_SOURCE_CHOICES = [5, 10, 20, 30, 50, 60, 80, 100]
NEWCOMPMUSIC_ORIENTATION_CHOICES = ("VR", "HOR", "VER")
RATEGRP_COLOR_CHOICES: Dict[str, Dict[str, str]] = {
    "green": {"emoji": "🟢", "label": "зелёная"},
    "yellow": {"emoji": "🟡", "label": "жёлтая"},
    "red": {"emoji": "🔴", "label": "красная"},
    "pink": {"emoji": "🩷", "label": "розовая"},
    "blue": {"emoji": "🔵", "label": "синяя"},
    "favorite": {"emoji": "⭐", "label": "избранное"},
    "inspect": {"emoji": "👁", "label": "присмотреться"},
    "delete": {"emoji": "❌", "label": "удалить"},
}
RATEGRP_COLOR_EMOJIS = tuple(choice["emoji"] for choice in RATEGRP_COLOR_CHOICES.values())
RATEGRP_COLOR_PROMPT = " / ".join(choice["emoji"] for choice in RATEGRP_COLOR_CHOICES.values())

# Добавляем в NAS_SYMLINK_COLOR_FOLDERS алиасы по эмодзи, чтобы не зависеть
# от текстовых ключей цветов.
for _color_key, _color_info in RATEGRP_COLOR_CHOICES.items():
    _emoji = _color_info["emoji"]
    _folder = NAS_SYMLINK_COLOR_FOLDERS.get(_color_key)
    if _folder and _emoji not in NAS_SYMLINK_COLOR_FOLDERS:
        NAS_SYMLINK_COLOR_FOLDERS[_emoji] = _folder


def extract_color_emoji(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    for emoji in RATEGRP_COLOR_EMOJIS:
        if emoji in text:
            return emoji
    return None


def db_set_source_color(source_id: int, emoji: str) -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT comments FROM sources WHERE id = ?", (source_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    current = (row["comments"] or "").strip()
    parts = [part.strip() for part in current.split("|") if part.strip()]
    parts = [
        part
        for part in parts
        if not any(color_emoji in part for color_emoji in RATEGRP_COLOR_EMOJIS)
    ]
    parts.append(f"color={emoji}")
    updated = " | ".join(parts)
    cur.execute("UPDATE sources SET comments = ? WHERE id = ?", (updated, source_id))
    conn.commit()
    conn.close()
    return updated


def build_newcomp_sources_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for count in NEWCOMPMUSIC_SOURCE_CHOICES:
        row.append(InlineKeyboardButton(str(count), callback_data=f"newcomp_sources:{count}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def build_newcomp_orientation_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(label, callback_data=f"newcomp_orient:{label}")
        for label in NEWCOMPMUSIC_ORIENTATION_CHOICES
    ]
    return InlineKeyboardMarkup([buttons])


def build_rategrp_orientation_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(label, callback_data=f"rategrp_orient:{label}")
        for label in NEWCOMPMUSIC_ORIENTATION_CHOICES
    ]
    extra = [InlineKeyboardButton("ИЗ PMV", callback_data="rategrp_from_pmv")]
    return InlineKeyboardMarkup([buttons, extra])


def build_rategrp_color_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for idx, (key, choice) in enumerate(RATEGRP_COLOR_CHOICES.items(), 1):
        row.append(InlineKeyboardButton(choice["emoji"], callback_data=f"rategrp_color:{key}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def build_rategrp_rerate_keyboard(
    available: List[Tuple[str, str, int]]
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for color_key, emoji, count in available:
        label = f"{emoji} ({count})"
        rows.append([InlineKeyboardButton(label, callback_data=f"rategrp_rerate_color:{color_key}")])
    rows.append([InlineKeyboardButton("↩️ Назад", callback_data="rategrp_rerate_back")])
    return InlineKeyboardMarkup(rows)


def build_newcomp_groupmode_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📁 По папкам", callback_data="newcomp_groupmode:folders")],
            [InlineKeyboardButton("🎨 По оценкам", callback_data="newcomp_groupmode:colors")],
        ]
    )


def build_newcomp_color_keyboard(
    color_counts: Dict[str, int],
    unrated_count: int,
    combo_counts: Optional[Dict[str, int]] = None,
) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    single_row: List[InlineKeyboardButton] = []
    for key, info in RATEGRP_COLOR_CHOICES.items():
        emoji = info["emoji"]
        count = color_counts.get(emoji, 0)
        single_row.append(
            InlineKeyboardButton(f"{emoji} ({count})", callback_data=f"newcomp_color:{key}")
        )
    if single_row:
        rows.append(single_row)
    combo_counts = combo_counts or {}
    green = RATEGRP_COLOR_CHOICES["green"]["emoji"]
    yellow = RATEGRP_COLOR_CHOICES["yellow"]["emoji"]
    red = RATEGRP_COLOR_CHOICES["red"]["emoji"]
    green_new_total = combo_counts.get(
        "green_new", color_counts.get(green, 0) + unrated_count
    )
    green_yellow_total = combo_counts.get(
        "green_yellow", color_counts.get(green, 0) + color_counts.get(yellow, 0)
    )
    green_yellow_red_total = combo_counts.get(
        "green_yellow_red",
        color_counts.get(green, 0)
        + color_counts.get(yellow, 0)
        + color_counts.get(red, 0),
    )
    combo_row: List[InlineKeyboardButton] = []
    combo_row.append(
        InlineKeyboardButton(
            f"{green}+🆕 ({green_new_total})",
            callback_data="newcomp_color:green_new",
        )
    )
    combo_row.append(
        InlineKeyboardButton(
            f"{green}+{yellow} ({green_yellow_total})",
            callback_data="newcomp_color:green_yellow",
        )
    )
    combo_row.append(
        InlineKeyboardButton(
            f"{green}+{yellow}+{red} ({green_yellow_red_total})",
            callback_data="newcomp_color:green_yellow_red",
        )
    )
    rows.append(combo_row)
    rows.append([InlineKeyboardButton("⬅ Назад", callback_data="newcomp_color_back")])
    return InlineKeyboardMarkup(rows)


def build_newcomp_algo_keyboard() -> InlineKeyboardMarkup:
    row = [
        InlineKeyboardButton("CAR", callback_data="newcomp_algo:car"),
        InlineKeyboardButton("WAV", callback_data="newcomp_algo:wav"),
        InlineKeyboardButton("BST", callback_data="newcomp_algo:bst"),
        InlineKeyboardButton("POI", callback_data="newcomp_algo:poi"),
        InlineKeyboardButton("LAY", callback_data="newcomp_algo:strata"),
    ]
    return InlineKeyboardMarkup([row])


def build_randompmv_count_keyboard() -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for idx, value in enumerate(RANDOMPMV_COUNT_OPTIONS, 1):
        row.append(InlineKeyboardButton(str(value), callback_data=f"randompmv_count:{value}"))
        if idx % 3 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


def build_randompmv_newcount_keyboard() -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for idx, value in enumerate(RANDOMPMV_NEW_SOURCE_CHOICES, 1):
        row.append(InlineKeyboardButton(str(value), callback_data=f"randompmv_newcount:{value}"))
        if idx % 4 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(buttons)


def build_newcomp_folder_keyboard(
    options: List[Dict[str, Any]],
    unused_only: bool = False,
) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for opt in options:
        label = truncate_button_label(f"{opt['label']} ({opt['count']})", 28)
        row.append(
            InlineKeyboardButton(label or "?", callback_data=f"newcomp_folder:{opt['token']}")
        )
        if len(row) >= 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    controls: List[List[InlineKeyboardButton]] = []
    if not buttons:
        controls.append([InlineKeyboardButton("Все папки", callback_data="newcomp_folder:all")])
    else:
        controls.extend(buttons)
    toggle_label = "Все исходники" if unused_only else "🆕 Только новое"
    toggle_target = "all" if unused_only else "new"
    controls.append([InlineKeyboardButton(toggle_label, callback_data=f"newcomp_folder_mode:{toggle_target}")])
    controls.append([InlineKeyboardButton("Назад к группам", callback_data="newcomp_folder_back")])
    return InlineKeyboardMarkup(controls)


def build_ratepmv_pmv_keyboard(rows: List[sqlite3.Row]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    for idx, row in enumerate(rows, 1):
        path = Path(row["video_path"])
        label = truncate_button_label(f"{idx}. {path.stem}", max_len=35)
        if not label:
            label = f"#{idx}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"ratepmv_select:{idx}")])
    if not buttons:
        return InlineKeyboardMarkup([[InlineKeyboardButton("Нет доступных PMV", callback_data="noop")]])

    bulk_row = [
        InlineKeyboardButton(f"{score} -> всем", callback_data=f"ratepmv_bulk:{score}")
        for score in range(1, 6)
    ]
    buttons.append(bulk_row)
    return InlineKeyboardMarkup(buttons)


def build_ratepmv_score_keyboard() -> InlineKeyboardMarkup:
    row = [
        InlineKeyboardButton(str(score), callback_data=f"ratepmv_rate:{score}")
        for score in range(1, 6)
    ]
    return InlineKeyboardMarkup([row])


async def run_newcompmusic_generation(
    send_fn: Callable[[str], Awaitable[None]],
    sess: Dict[str, Any],
    resolved_key: str,
    user_id: int,
) -> None:
    resolved_key, algo_meta = resolve_clip_algorithm(resolved_key)
    selected = sess.get("music_selected") or {}
    parsed_segments: List[MusicSegment] = selected.get("parsed_segments") or []
    if not parsed_segments:
        return await send_fn("Не удалось найти сегменты в выбранном проекте.")

    audio_path_str = selected.get("audio_path")
    if not audio_path_str:
        return await send_fn("В проекте отсутствует audio.mp3. Проверьте папку music_projects.")

    sources_count = int(sess.get("music_sources") or 0)
    if sources_count <= 0:
        return await send_fn("Не указано количество исходников.")

    await send_fn("Запускаю музыкальную компиляцию. Это может занять несколько минут...")

    move_comment = ""
    try:
        preferred_group = None
        if sess.get("music_group_choice"):
            preferred_group = tuple(sess["music_group_choice"]["key"])
        group_choice = sess.get("music_group_choice") or {}
        preferred_folder = group_choice.get("folder_path")
        orientation_label = (group_choice.get("orientation") or "HOR").upper()
        color_rows = sess.get("music_color_rows")
        group_number = group_choice.get("group_number")
        min_new_required = max(0, int(sess.get("music_min_new_sources") or 0))
        if color_rows:
            source_rows = pick_specific_source_rows(
                list(color_rows),
                sources_count,
                min_new_required=min_new_required,
            )
        else:
            source_rows = choose_random_source_rows(
                sources_count,
                group_strategy=CURRENT_STRATEGY,
                preferred_group=preferred_group,
                preferred_folder=preferred_folder,
            )
        out_path, source_ids, (resolved_key, algo_meta) = make_music_synced_pmv(
            selected.get("name") or selected.get("slug") or "music",
            parsed_segments,
            Path(audio_path_str),
            source_rows,
            resolved_key,
            orientation=orientation_label,
            group_number=group_number,
        )
        out_path, move_comment = move_output_to_network_storage(out_path)
    except Exception as exc:
        user_sessions.pop(user_id, None)
        await send_fn(f"Ошибка при генерации: {exc}")
        if sess.get("_raise_on_newcomp_error"):
            raise
        return

    pmv_tag = Path(out_path).name
    comments = combine_comments(f"music_project={selected.get('slug')}", move_comment)
    db_insert_compilation(
        out_path,
        source_ids,
        comments=comments,
    )
    db_update_sources_pmv_list(source_ids, pmv_tag)
    autotag = sess.get("music_color_autotag")
    if autotag:
        emoji = autotag.get("emoji")
        autotag_ids = set(int(i) for i in autotag.get("ids") or [])
        if emoji and autotag_ids:
            for sid in source_ids:
                if sid in autotag_ids:
                    db_append_source_comment(sid, f"color={emoji}")

    user_sessions.pop(user_id, None)

    duration = selected.get("duration")
    minutes = (duration / 60.0) if duration else None
    msg_lines = [
        "✅ Музыкальная компиляция готова!",
        f"Файл: {out_path}",
        f"Проект: {selected.get('name')} (slug: {selected.get('slug')}).",
        f"Алгоритм клипов: {algo_meta['title']} ({resolved_key}/{algo_meta.get('short')}).",
        f"Использовано исходников: {len(source_ids)}.",
        f"Сегментов по манифесту: {len(parsed_segments)}.",
    ]
    if minutes:
        msg_lines.append(f"Длительность проекта ≈ {minutes:.1f} мин.")
    await send_fn("\n".join(msg_lines))


def _prepare_randompmv_session(
    used_group_keys: Optional[Set[Tuple[str, str]]] = None,
    min_new_sources: int = 0,
) -> Tuple[Dict[str, Any], str, Dict[str, Any]]:
    projects = load_music_projects()
    if not projects:
        raise RuntimeError("Не найдено проектов в music_projects.")

    unused_projects = [p for p in projects if not p.get("usage_count")]
    unused_ids = {id(p) for p in unused_projects}
    other_projects = [p for p in projects if id(p) not in unused_ids]
    project_candidates = unused_projects + other_projects

    groups_raw = get_source_groups_prefer_unused()
    if not groups_raw:
        raise RuntimeError("Нет доступных групп исходников. Выполните /scan.")
    group_entries = [
        SourceGroupEntry(key=key, rows=list(rows), unused_count=unused)
        for key, rows, unused in groups_raw
    ]
    sorted_entries, orientation_map = sort_group_entries_with_orientation(group_entries)
    prepared_groups = [(entry.key, list(entry.rows), entry.unused_count) for entry in sorted_entries]

    fallback_result: Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]] = None
    last_error: Optional[Exception] = None

    for project in project_candidates:
        try:
            strict_result = _prepare_randompmv_from_project(
                project,
                used_group_keys,
                prepared_groups,
                orientation_map,
                require_target=True,
                min_new_sources=min_new_sources,
            )
        except Exception as exc:
            last_error = exc
            continue

        if strict_result:
            return strict_result

        if fallback_result is not None:
            continue

        try:
            fallback_result = _prepare_randompmv_from_project(
                project,
                used_group_keys,
                prepared_groups,
                orientation_map,
                require_target=False,
                min_new_sources=min_new_sources,
            )
        except Exception as exc:
            last_error = exc
            continue

    if fallback_result:
        return fallback_result
    if last_error:
        raise last_error
    raise RuntimeError("Не удалось подобрать группу с достаточным количеством исходников.")


def _prepare_randompmv_from_project(
    project: Dict[str, Any],
    used_group_keys: Optional[Set[Tuple[str, str]]],
    prepared_groups: List[Tuple[Tuple[str, str], List[sqlite3.Row], int]],
    orientation_map: Dict[Tuple[str, str], str],
    require_target: bool,
    min_new_sources: int = 0,
) -> Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]]:
    manifest_data = project.get("manifest_data")
    manifest_path = project.get("manifest_path")
    if not manifest_data and manifest_path and Path(manifest_path).exists():
        manifest_data = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        project["manifest_data"] = manifest_data
    parsed_segments = parse_manifest_segments(manifest_data or {})
    if not parsed_segments:
        raise RuntimeError(f"Для проекта {project.get('name')} не нашлось валидных сегментов.")

    audio_path = project.get("audio_path")
    audio_path_path = Path(audio_path) if audio_path else None
    if not audio_path_path or not audio_path_path.exists():
        raise RuntimeError(f"У проекта {project.get('name')} отсутствует audio.mp3.")

    duration_seconds = float(project.get("duration") or 0.0)
    if duration_seconds <= 0 and parsed_segments:
        duration_seconds = float(parsed_segments[-1].end)
    duration_minutes = max(duration_seconds / 60.0, 1.0)
    target_sources, target_ratio = _randompmv_compute_target_sources(duration_minutes)
    required_total_sources = max(target_sources, max(0, min_new_sources))

    orientation_cycle = list(NEWCOMPMUSIC_ORIENTATION_CHOICES)
    random.shuffle(orientation_cycle)

    def pick_group(
        forbid_used: bool,
        require_target_count: bool,
    ) -> Optional[Tuple[Tuple[str, str], List[sqlite3.Row], int, List[sqlite3.Row], str]]:
        for orient in orientation_cycle:
            filtered = filter_groups_by_orientation(prepared_groups, orientation_map, orient)
            if not filtered:
                continue
            shuffled = filtered[:]
            random.shuffle(shuffled)
            for key, rows, unused in shuffled:
                if forbid_used and used_group_keys and key in used_group_keys:
                    continue
                color_rows = _filter_green_new_rows(rows)
                if not color_rows:
                    continue
                new_available = sum(1 for row in color_rows if _is_unused_source_row(row))
                if min_new_sources > 0 and new_available < min_new_sources:
                    continue
                if require_target_count:
                    if len(color_rows) < required_total_sources:
                        continue
                else:
                    if len(color_rows) < max(min_new_sources, 1):
                        continue
                return (key, list(rows), unused, list(color_rows), orient)
        return None

    search_plan = [(True, True), (False, True)]
    if not require_target:
        search_plan.extend([(True, False), (False, False)])

    chosen: Optional[
        Tuple[Tuple[str, str], List[sqlite3.Row], int, List[sqlite3.Row], str]
    ] = None
    for forbid_used, need_target in search_plan:
        candidate = pick_group(forbid_used, need_target)
        if candidate:
            chosen = candidate
            break

    if not chosen:
        if require_target:
            return None
        raise RuntimeError("Не удалось подобрать группу с зелёными исходниками.")

    key, rows, unused_count, color_rows, chosen_orientation = chosen
    if not color_rows:
        raise RuntimeError("Выбранная группа не содержит исходников с нужными тегами.")
    group_idx = next(
        (idx for idx, (group_key, _, _) in enumerate(prepared_groups, 1) if group_key == key),
        None,
    )
    orientation_label = (orientation_map.get(key) or _resolution_orientation(key[1] or "")[0]).upper()
    green_emoji = RATEGRP_COLOR_CHOICES["green"]["emoji"]
    color_label = f"{green_emoji}+🆕"

    new_sources_available = sum(1 for row in color_rows if _is_unused_source_row(row))
    if min_new_sources > 0 and new_sources_available < min_new_sources:
        raise RuntimeError("Недостаточно новых исходников для требования.")

    total_required = max(target_sources, min_new_sources)
    sources_count = min(len(color_rows), total_required)
    if sources_count <= 0:
        raise RuntimeError("Недостаточно исходников для генерации.")

    autotag_ids: List[int] = []
    for row in color_rows:
        if _rategrp_row_color(row) is None:
            try:
                autotag_ids.append(int(row["id"]))
            except Exception:
                continue
    autotag = {"emoji": green_emoji, "ids": autotag_ids} if autotag_ids else None

    algo_key = random.choice(list(CLIP_SEQUENCE_ALGORITHMS.keys()))

    music_selected = {
        "slug": project.get("slug"),
        "name": project.get("name"),
        "duration": project.get("duration"),
        "segments": len(parsed_segments),
        "manifest": manifest_data,
        "audio_path": str(audio_path_path),
        "parsed_segments": parsed_segments,
    }
    group_choice = {
        "key": key,
        "count": len(color_rows),
        "orientation": orientation_label,
        "total_count": len(rows),
        "unused_count": unused_count,
        "group_number": group_idx,
    }
    session = {
        "state": "newcompmusic_wait_algo",
        "music_selected": music_selected,
        "music_group_choice": group_choice,
        "music_group_rows": list(rows),
        "music_groups_all": prepared_groups,
        "music_group_orientations": orientation_map,
        "music_orientation_preference": chosen_orientation,
        "music_color_rows": list(color_rows),
        "music_color_choice": color_label,
        "music_color_autotag": autotag,
        "music_sources": sources_count,
        "music_folder_only_new": False,
        "_raise_on_newcomp_error": True,
        "music_min_new_sources": max(0, min_new_sources),
    }
    meta = {
        "project": project.get("name") or project.get("slug"),
        "orientation": chosen_orientation or orientation_label,
        "group": f"{key[0]} {key[1]}",
        "group_key": [key[0], key[1]],
        "sources": sources_count,
        "color": color_label,
        "algo_key": algo_key,
        "duration_minutes": duration_minutes,
        "target_sources": target_sources,
        "target_ratio": target_ratio,
        "available_color_sources": len(color_rows),
        "limited_sources": len(color_rows) < target_sources,
        "min_new_sources": max(0, min_new_sources),
        "new_sources_available": new_sources_available,
    }
    return session, algo_key, meta


async def run_randompmv_batch(
    send_fn: Callable[[str], Awaitable[None]],
    user_id: int,
    total_runs: int,
    min_new_sources: int = 0,
) -> None:
    total = max(RANDOMPMV_MIN_BATCH, min(int(total_runs), RANDOMPMV_MAX_BATCH))
    created = 0
    used_groups: Set[Tuple[str, str]] = set()
    for idx in range(1, total + 1):
        try:
            session, algo_key, meta = _prepare_randompmv_session(used_groups, min_new_sources=min_new_sources)
        except Exception as exc:
            log_randompmv_event(
                {
                    "run_index": idx,
                    "total_runs": total,
                    "status": "prepare_error",
                    "error": str(exc),
                }
            )
            await send_fn(f"❌ Не удалось подготовить Random PMV #{idx}: {exc}")
            break

        algo_meta = CLIP_SEQUENCE_ALGORITHMS.get(algo_key, {})
        base_event = {
            "run_index": idx,
            "total_runs": total,
            **meta,
        }
        log_randompmv_event({**base_event, "status": "start"})

        source_line = f"{meta['sources']} исходников."
        if meta.get("target_sources") and meta["sources"] < meta["target_sources"]:
            source_line += f" (нужно ≈ {meta['target_sources']})"
        if min_new_sources > 0:
            source_line += f", новых ≥ {min_new_sources}"
        await send_fn(
            f"▶️ Random PMV #{idx}/{total}: проект {meta['project']}, "
            f"{meta['orientation']} / {meta['group']}, {source_line} ({meta['color']}), "
            f"алгоритм {algo_meta.get('short', algo_key)}."
        )
        try:
            user_sessions[user_id] = session
            await run_newcompmusic_generation(send_fn, session, algo_key, user_id)
        except Exception as exc:
            log_randompmv_event({**base_event, "status": "generation_error", "error": str(exc)})
            break
        else:
            created += 1
            log_randompmv_event({**base_event, "status": "success"})
            group_key = session.get("music_group_choice", {}).get("key")
            if group_key:
                used_groups.add(tuple(group_key))

    user_sessions.pop(user_id, None)
    if created:
        await send_fn(f"✅ Random PMV завершён. Создано {created} из {total}.")
    else:
        await send_fn("⚠️ Random PMV не удалось создать.")


def load_music_generator_module():
    global _music_generator_module
    if _music_generator_module is not None:
        return _music_generator_module
    module_path = SCRIPT_DIR / "music_guided_generator.py"
    if not module_path.exists():
        raise RuntimeError("music_guided_generator.py не найден рядом со скриптом.")
    spec = importlib.util.spec_from_file_location("music_guided_generator", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Не удалось загрузить music_guided_generator.py")
    module = types.ModuleType("music_guided_generator")
    module.__file__ = str(module_path)
    sys.modules["music_guided_generator"] = module
    spec.loader.exec_module(module)
    _music_generator_module = module
    return module


def list_music_input_files() -> List[Path]:
    MUSIC_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    files = []
    for path in sorted(MUSIC_INPUT_DIR.iterdir(), key=lambda p: p.name.lower()):
        if path.is_file() and path.suffix.lower() in {".mp3", ".wav", ".flac", ".m4a"}:
            files.append(path)
    return files


RESOLUTION_RE = re.compile(r"(\d+)\s*[xхXХ]\s*(\d+)")
ORIENTATION_ORDER = {"VR": 0, "HOR": 1, "VER": 2}


@dataclass
class SourceGroupEntry:
    key: Tuple[str, str]
    rows: List[sqlite3.Row]
    unused_count: int = 0


def _resolution_pixels(res: str) -> int:
    if not res:
        return 0
    match = RESOLUTION_RE.search(res)
    if not match:
        return 0
    try:
        width = int(match.group(1))
        height = int(match.group(2))
        return width * height
    except ValueError:
        return 0


def _resolution_orientation(res: str) -> Tuple[str, int]:
    """
    Приблизительно определяем тип контента:
    VR (~2:1), горизонт, вертикаль.
    """
    match = RESOLUTION_RE.search(res or "")
    if not match:
        return "HOR", ORIENTATION_ORDER["HOR"]
    try:
        width = int(match.group(1))
        height = int(match.group(2))
    except ValueError:
        return "HOR", ORIENTATION_ORDER["HOR"]
    if width <= 0 or height <= 0:
        return "HOR", ORIENTATION_ORDER["HOR"]
    if width >= height:
        ratio = width / height if height else float("inf")
        if ratio >= 1.8:
            return "VR", ORIENTATION_ORDER["VR"]
        return "HOR", ORIENTATION_ORDER["HOR"]
    else:
        return "VER", ORIENTATION_ORDER["VER"]


def _friendly_folder_label(folder: Path, roots: Optional[List[Path]] = None) -> str:
    roots = roots or []
    try:
        folder_resolved = folder.resolve(strict=False)
    except Exception:
        folder_resolved = folder
    for root in roots:
        try:
            root_resolved = root.resolve(strict=False)
        except Exception:
            root_resolved = root
        try:
            rel = folder_resolved.relative_to(root_resolved)
            rel_str = str(rel).replace("\\", "/")
            base = root_resolved.name or str(root_resolved)
            if not rel_str:
                return base
            return f"{base}/{rel_str}"
        except ValueError:
            continue
    label = folder_resolved.name or str(folder_resolved)
    return label.replace("\\", "/")


def _is_unused_source_row(row: sqlite3.Row) -> bool:
    try:
        pmv_list = row["pmv_list"]
    except Exception:
        pmv_list = None
    return not (pmv_list or "").strip()


def compute_group_folder_options(
    rows: List[sqlite3.Row],
    unused_only: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    folder_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if unused_only and not _is_unused_source_row(row):
            continue
        try:
            parent = Path(row["video_path"]).resolve(strict=False).parent
        except Exception:
            parent = Path(row["video_path"]).parent
        folder_key = _normalize_path_prefix(parent)
        info = folder_map.setdefault(
            folder_key,
            {"rows": [], "count": 0, "path": parent},
        )
        info["rows"].append(row)
        info["count"] += 1

    sorted_folders = sorted(
        folder_map.items(),
        key=lambda item: (-item[1]["count"], item[0]),
    )
    roots = [Path(r["folder_path"]) for r in db_get_upload_folders(include_ignored=True)]

    options: List[Dict[str, Any]] = []
    token_map: Dict[str, Dict[str, Any]] = {}
    for idx, (folder_key, info) in enumerate(sorted_folders, 1):
        token = f"folder{idx}"
        label = _friendly_folder_label(Path(info["path"]), roots)
        option = {
            "token": token,
            "label": label,
            "count": info["count"],
            "path": str(Path(info["path"])),
        }
        options.append(option)
        token_map[token] = {
            **info,
            "label": label,
            "path": option["path"],
        }

    total_rows = [row for row in rows if (not unused_only or _is_unused_source_row(row))]
    token_map["all"] = {
        "rows": total_rows,
        "count": len(total_rows),
        "path": None,
        "label": "Все папки",
    }
    options.insert(
        0,
        {
            "token": "all",
            "label": "Все папки",
            "count": len(total_rows),
            "path": None,
        },
    )
    return options, token_map


def filter_groups_by_orientation(
    groups: List[Tuple[Tuple[str, str], List[sqlite3.Row], int]],
    orientation_map: Dict[Tuple[str, str], str],
    target: str,
) -> List[Tuple[Tuple[str, str], List[sqlite3.Row], int]]:
    normalized = (target or "").upper()
    if normalized not in NEWCOMPMUSIC_ORIENTATION_CHOICES:
        return []
    filtered: List[Tuple[Tuple[str, str], List[sqlite3.Row], int]] = []
    for key, rows, unused in groups:
        label = (orientation_map.get(key) or "").upper()
        if not label:
            label = _resolution_orientation(key[1] or "")[0]
        if label.upper() == normalized:
            filtered.append((key, rows, unused))
    return filtered


def _build_group_selection_lines(
    sess: Dict[str, Any],
    group_entries: List[SourceGroupEntry],
    orientation: str,
    prompt_kind: str = "text",
) -> List[str]:
    orientation_map = sess.get("music_group_orientations") or {}

    def orientation_prefix(entry: SourceGroupEntry) -> str:
        return orientation_map.get(entry.key, "")

    project_info = sess.get("music_selected") or {}
    lines: List[str] = []
    if project_info.get("name"):
        lines.append(f"Проект: {project_info.get('name')} (slug: {project_info.get('slug')}).")
    segs = project_info.get("segments")
    if segs is not None:
        lines.append(f"Смен клипов: {segs}")
    duration = project_info.get("duration")
    if duration:
        lines.append(f"Продолжительность ≈ {(duration / 60.0):.1f} минут.")
    lines.append(f"Ориентация: {orientation}.")
    lines.append("")
    lines.extend(
        format_source_group_lines(
            group_entries,
            "Выберите группу исходников (codec + разрешение):",
            prefix_func=orientation_prefix,
        )
    )
    lines.append("")
    if prompt_kind == "inline":
        lines.append("Нажмите номер группы на клавиатуре ниже.")
    else:
        lines.append("Пришлите номер группы (например: 3).")
    return lines


def format_folder_selection_message(
    codec: str,
    resolution: str,
    project_info: Dict[str, Any],
    options: List[Dict[str, Any]],
    max_listed: int = 20,
    unused_only: bool = False,
) -> str:
    project_name = project_info.get("name") or project_info.get("slug") or ""
    lines = [
        f"Группа выбрана: {codec} {resolution} (исходников: {options[0]['count']}).",
    ]
    if project_name:
        lines.append(f"Проект: {project_name}.")
    if unused_only:
        lines.append("Фильтр включен: только новые исходники.")
    lines.append("Теперь выберите подпапку (или «Все папки»):")
    listed = 0
    for idx, opt in enumerate(options, 1):
        if listed >= max_listed:
            break
        label = opt["label"]
        lines.append(f"{idx}. {label} ({opt['count']})")
        listed += 1
    remaining = len(options) - listed
    if remaining > 0:
        lines.append(f"... и ещё {remaining} вариантов. Используйте кнопки ниже.")
    return "\n".join(lines)


def compose_newcomp_folder_prompt(
    sess: Dict[str, Any],
) -> Tuple[str, InlineKeyboardMarkup]:
    rows = sess.get("music_group_rows") or []
    options, folder_map = compute_group_folder_options(
        rows,
        unused_only=sess.get("music_folder_only_new", False),
    )
    sess["music_folder_options"] = options
    sess["music_folder_map"] = folder_map
    codec, res = sess.get("music_group_choice", {}).get("key") or ("?", "?")
    project_info = sess.get("music_selected") or {}
    msg_text = format_folder_selection_message(
        codec,
        res,
        project_info,
        options,
        unused_only=sess.get("music_folder_only_new", False),
    )
    msg_text += "\n\nЕсли нужно выбрать другую группу, нажмите кнопку «Назад к группам» ниже."
    keyboard = build_newcomp_folder_keyboard(
        options, unused_only=sess.get("music_folder_only_new", False)
    )
    return msg_text, keyboard


def _rategrp_row_has_color(row: sqlite3.Row) -> bool:
    comments = ""
    try:
        comments = row["comments"] or ""
    except Exception:
        comments = ""
    return any(emoji in comments for emoji in RATEGRP_COLOR_EMOJIS)


def _rategrp_row_color(row: sqlite3.Row) -> Optional[str]:
    comments = ""
    try:
        comments = row["comments"] or ""
    except Exception:
        comments = ""
    for info in RATEGRP_COLOR_CHOICES.values():
        if info["emoji"] in comments:
            return info["emoji"]
    return None


def _rategrp_balanced_shuffle(rows: List[sqlite3.Row]) -> List[sqlite3.Row]:
    pool = list(rows)
    if len(pool) <= 1:
        return pool

    def pick_index(length: int, zone: str) -> int:
        if length <= 1:
            return 0
        third = max(1, length // 3)
        if zone == "front":
            return random.randint(0, max(0, third - 1))
        if zone == "back":
            start = max(0, length - third)
            return random.randint(start, length - 1)
        mid_start = max(0, (length // 2) - (third // 2))
        mid_end = min(length - 1, mid_start + third - 1)
        return random.randint(mid_start, max(mid_start, mid_end))

    pattern = ["front", "back", "middle"]
    idx = 0
    result: List[sqlite3.Row] = []
    while pool:
        zone = pattern[idx % len(pattern)]
        pick = pick_index(len(pool), zone)
        result.append(pool.pop(pick))
        idx += 1
    return result


def _rategrp_rows_to_queue(rows: List[sqlite3.Row], shuffle: bool = True) -> List[Dict[str, Any]]:
    shuffled_rows = _rategrp_balanced_shuffle(rows) if shuffle else rows
    queue: List[Dict[str, Any]] = []
    for row in shuffled_rows:
        try:
            sid = int(row["id"])
            path_str = str(row["video_path"])
        except Exception:
            continue
        queue.append({"id": sid, "path": path_str, "name": Path(path_str).name})
    return queue


def _fetch_unrated_pmv_rows(limit: int = RATEGRP_PMV_MAX_QUEUE) -> List[sqlite3.Row]:
    rows = db_get_used_sources_list()
    if not rows:
        return []
    usage = collect_source_usage_stats()
    scored: List[Tuple[int, float, sqlite3.Row]] = []
    for row in rows:
        if _rategrp_row_has_color(row):
            continue
        try:
            sid = int(row["id"])
        except Exception:
            continue
        info = usage.get(sid) or {}
        last_date = info.get("last_date")
        ordinal = last_date.toordinal() if last_date else 0
        scored.append((ordinal, random.random(), row))
    scored.sort(key=lambda item: (-item[0], item[1]))
    ordered = [item[2] for item in scored]
    if limit > 0:
        ordered = ordered[:limit]
    return ordered


async def _start_rategrp_from_pmv(
    session: Dict[str, Any],
    send_fn: Callable[[str, Optional[InlineKeyboardMarkup]], Awaitable[Any]],
) -> bool:
    rows = _fetch_unrated_pmv_rows()
    if not rows:
        await send_fn("Не нашёл участвовавших в PMV исходников без оценки.", None)
        return False
    queue = _rategrp_rows_to_queue(rows, shuffle=True)
    session["rategrp_queue"] = queue
    session["rategrp_total"] = len(queue)
    session["rategrp_processed"] = 0
    session["rategrp_queue_origin"] = "pmv"
    session["state"] = "rategrp_rate_source"

    await send_fn(
        f"Из последних PMV найдено {len(queue)} неоценённых исходников. Оценим их!",
        None,
    )
    await rategrp_send_next_prompt(session, send_fn)
    return True


def _search_find_matches(term: str, limit: int = BADCLIP_MAX_MATCHES) -> List[Dict[str, Any]]:
    normalized = term.strip().lower()
    if not normalized:
        return []
    rows = db_get_all_compilations()
    today = datetime.now().strftime("%Y-%m-%d")
    pmv_primary: List[Dict[str, Any]] = []
    pmv_secondary: List[Dict[str, Any]] = []
    for row in rows:
        path = Path(row["video_path"])
        haystack = f"{path.name.lower()} {str(path).lower()}"
        if normalized not in haystack:
            continue
        entry = {
            "type": "pmv",
            "id": int(row["id"]),
            "video_path": str(path),
            "pmv_date": row["pmv_date"],
            "source_ids": row["source_ids"],
            "stem": path.stem,
        }
        container = (
            pmv_primary
            if (today in str(path.parent) or (row["pmv_date"] or "").startswith(today))
            else pmv_secondary
        )
        container.append(entry)

    source_rows = db_search_sources_by_term(normalized, limit * 2)
    source_entries: List[Dict[str, Any]] = []
    for row in source_rows:
        try:
            resolved = str(Path(row["video_path"]))
        except Exception:
            resolved = str(row["video_path"])
        source_entries.append(
            {
                "type": "source",
                "id": int(row["id"]),
                "video_path": resolved,
                "video_name": row["video_name"],
                "codec": row["codec"],
                "resolution": row["resolution"],
                "comments": row["comments"],
            }
        )

    combined = pmv_primary + pmv_secondary + source_entries
    results: List[Dict[str, Any]] = []
    seen_keys: Set[Tuple[str, int]] = set()
    for entry in combined:
        key = (entry["type"], entry["id"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        results.append(entry)
        if len(results) >= limit:
            break
    return results


def build_find_keyboard(matches: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for idx, entry in enumerate(matches, 1):
        if entry.get("type") == "pmv":
            label = truncate_label_keep_suffix(f"PMV · {entry.get('stem')}", 48)
        else:
            color = extract_color_emoji(entry.get("comments"))
            prefix = f"{color} " if color else ""
            label = truncate_label_keep_suffix(f"{prefix}{entry.get('video_name')}", 48)
        rows.append([InlineKeyboardButton(label or str(idx), callback_data=f"find_pick:{idx - 1}")])
    rows.append([InlineKeyboardButton("↩️ Новый поиск", callback_data="find_retry")])
    return InlineKeyboardMarkup(rows)


async def _start_find_pmv_queue(
    session: Dict[str, Any],
    match: Dict[str, Any],
    send_fn: Callable[[str, Optional[InlineKeyboardMarkup]], Awaitable[Any]],
) -> bool:
    source_ids = parse_source_id_list(match.get("source_ids") or "")
    if not source_ids:
        await send_fn("У выбранного PMV нет сохранённых исходников.", None)
        return False
    rows = db_get_sources_by_ids(source_ids)
    if not rows:
        await send_fn("Не удалось найти исходники в базе.", None)
        return False
    rows_map = {int(row["id"]): row for row in rows}
    ordered_rows = [rows_map[sid] for sid in source_ids if sid in rows_map]
    queue = _rategrp_rows_to_queue(ordered_rows, shuffle=False)
    if not queue:
        await send_fn("Очередь пустая — возможно файлы удалены.", None)
        return False
    session["rategrp_queue"] = queue
    session["rategrp_total"] = len(queue)
    session["rategrp_processed"] = 0
    session["rategrp_queue_origin"] = "find_pmv"
    session["state"] = "rategrp_rate_source"
    session["find_current"] = match
    session["find_mode"] = True
    session["find_rows"] = ordered_rows
    session["rategrp_rerate_rows"] = ordered_rows
    session["rategrp_group_choice"] = {
        "label": truncate_button_label(match.get("stem") or Path(match.get("video_path", "?")).stem),
        "orientation": match.get("pmv_date") or "PMV",
        "key": ("PMV", match.get("stem") or "?"),
        "count": len(queue),
    }
    await send_fn(
        f"PMV «{match.get('stem')}» выбрана. Исходников: {len(queue)}. Используйте цветные кнопки ниже.",
        None,
    )
    await rategrp_send_next_prompt(session, send_fn)
    return True


async def _start_find_single_source(
    session: Dict[str, Any],
    row: sqlite3.Row,
    send_fn: Callable[[str, Optional[InlineKeyboardMarkup]], Awaitable[Any]],
) -> bool:
    try:
        sid = int(row["id"])
    except Exception:
        await send_fn("Не удалось определить ID исходника.", None)
        return False
    path = str(row["video_path"])
    queue = [{"id": sid, "path": path, "name": Path(path).name}]
    session["rategrp_queue"] = queue
    session["rategrp_total"] = len(queue)
    session["rategrp_processed"] = 0
    session["rategrp_queue_origin"] = "find_single"
    session["state"] = "rategrp_rate_source"
    session["find_mode"] = True
    session["find_single_row"] = row
    session["rategrp_rerate_rows"] = [row]
    session["rategrp_group_choice"] = {
        "label": row["video_name"],
        "orientation": row["codec"] or "SRC",
        "key": ("SRC", row["resolution"] or "?"),
    }
    color = extract_color_emoji(row["comments"])
    prefix = f"{color} " if color else ""
    await send_fn(f"Исходник найден: {prefix}{row['video_name']}.", None)
    await rategrp_send_next_prompt(session, send_fn)
    return True


def _count_rategrp_unrated(rows: List[sqlite3.Row]) -> int:
    return sum(1 for row in rows if not _rategrp_row_has_color(row))


def _count_rows_for_folder_mode(rows: List[sqlite3.Row], unused_only: bool) -> int:
    if not unused_only:
        return len(rows)
    return sum(1 for row in rows if _is_unused_source_row(row))


def format_rategrp_group_prompt(
    session: Dict[str, Any],
    group_entries: List[SourceGroupEntry],
    orientation: str,
    prompt_kind: str = "text",
) -> List[str]:
    orientation_map = session.get("rategrp_group_orientations") or {}

    def prefix_func(entry: SourceGroupEntry) -> str:
        return orientation_map.get(entry.key, "")

    display_entries: List[SourceGroupEntry] = []
    for entry in group_entries:
        rows = list(entry.rows)
        display_entries.append(
            SourceGroupEntry(
                key=entry.key,
                rows=rows,
                unused_count=_count_rategrp_unrated(rows),
            )
        )

    lines = [
        f"Ориентация: {orientation}.",
        "",
    ]
    lines.extend(
        format_source_group_lines(
            display_entries,
            "Выберите группу исходников (🆕 = без оценки):",
            prefix_func=prefix_func,
        )
    )
    lines.append("")
    if prompt_kind == "inline":
        lines.append("Нажмите номер группы на клавиатуре ниже.")
    else:
        lines.append("Пришлите номер группы (например: 3).")
    return lines


def _compute_rategrp_color_counts(rows: List[sqlite3.Row]) -> Tuple[Dict[str, int], int]:
    counts = {info["emoji"]: 0 for info in RATEGRP_COLOR_CHOICES.values()}
    unrated = 0
    for row in rows:
        emoji = _rategrp_row_color(row)
        if emoji and emoji in counts:
            counts[emoji] += 1
        else:
            unrated += 1
    return counts, unrated


def _rategrp_available_colors(rows: List[sqlite3.Row]) -> List[Tuple[str, str, int]]:
    counts, _ = _compute_rategrp_color_counts(rows)
    available: List[Tuple[str, str, int]] = []
    for key, info in RATEGRP_COLOR_CHOICES.items():
        emoji = info["emoji"]
        count = counts.get(emoji, 0)
        if count > 0:
            available.append((key, emoji, count))
    return available


def _filter_rows_by_color(
    rows: List[sqlite3.Row],
    allowed: Set[str],
    include_unrated: bool = False,
) -> List[sqlite3.Row]:
    filtered: List[sqlite3.Row] = []
    for row in rows:
        emoji = _rategrp_row_color(row)
        if emoji:
            if emoji in allowed:
                filtered.append(row)
        else:
            if include_unrated:
                filtered.append(row)
    return filtered


def _filter_green_new_rows(rows: List[sqlite3.Row]) -> List[sqlite3.Row]:
    """Возвращает все зелёные исходники и любые исходники без PMV-истории."""
    filtered: List[sqlite3.Row] = []
    green_emoji = RATEGRP_COLOR_CHOICES["green"]["emoji"]
    for row in rows:
        emoji = _rategrp_row_color(row)
        if emoji == green_emoji or _is_unused_source_row(row):
            filtered.append(row)
    return filtered


def _prepare_rategrp_queue(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    unrated_rows = [row for row in rows if not _rategrp_row_has_color(row)]
    return _rategrp_rows_to_queue(unrated_rows)


def _rategrp_update_cached_row_color(
    session: Dict[str, Any],
    source_id: int,
    updated_comments: Optional[str],
) -> None:
    if not updated_comments:
        return
    rows = session.get("rategrp_rerate_rows") or []
    for row in rows:
        try:
            row_id = int(row.get("id") if isinstance(row, dict) else row["id"])
        except Exception:
            continue
        if row_id == source_id:
            try:
                row["comments"] = updated_comments  # type: ignore[index]
            except Exception:
                pass
            break


async def rategrp_send_next_prompt(
    session: Dict[str, Any],
    send_fn: Callable[[str, Optional[InlineKeyboardMarkup]], Awaitable[Any]],
    prefix: Optional[str] = None,
) -> None:
    queue: List[Dict[str, Any]] = session.get("rategrp_queue") or []
    total = int(session.get("rategrp_total", len(queue) or 0))
    processed = int(session.get("rategrp_processed", 0))
    if not queue:
        origin = session.get("rategrp_queue_origin")
        session["rategrp_queue_origin"] = None
        if origin == "find_pmv":
            session["state"] = "find_wait_term"
            session["find_matches"] = []
            lines = [prefix] if prefix else []
            lines.append("Исходники из выбранного PMV закончились. Пришлите часть имени следующего файла.")
            await send_fn("\n".join(line for line in lines if line), None)
        elif origin == "find_single":
            session["state"] = "find_wait_term"
            session["find_matches"] = []
            lines = [prefix] if prefix else []
            lines.append("Исходник оценён. Можете продолжить поиск или ввести другую часть имени.")
            await send_fn("\n".join(line for line in lines if line), None)
        elif origin == "rerate":
            rows = session.get("rategrp_rerate_rows") or []
            available = _rategrp_available_colors(rows)
            if available:
                session["state"] = "rategrp_choose_rerate_color"
                lines = [prefix] if prefix else []
                lines.append("Выберите цвет, который хотите переоценить ещё раз.")
                await send_fn(
                    "\n".join(line for line in lines if line),
                    build_rategrp_rerate_keyboard(available),
                )
            else:
                session["state"] = "rategrp_choose_group"
                lines = [prefix] if prefix else []
                lines.append("В этой группе больше нет исходников для переоценки. Выберите другую группу.")
                await send_fn("\n".join(line for line in lines if line), None)
        else:
            session["state"] = "rategrp_choose_group"
            lines = [prefix] if prefix else []
            lines.append("В этой группе не осталось неоценённых исходников. Выберите другую группу.")
            await send_fn("\n".join(line for line in lines if line), None)
        return
    current = queue[0]
    idx = processed + 1
    group_choice = session.get("rategrp_group_choice") or {}
    codec, res = group_choice.get("key") or ("?", "?")
    group_label = group_choice.get("label") or f"{codec} {res}"
    orientation = group_choice.get("orientation") or session.get("rategrp_orientation_preference") or "?"
    lines = []
    if prefix:
        lines.append(prefix)
    lines.append(f"Группа: {group_label} ({orientation}).")
    if total:
        lines.append(f"Исходник {idx}/{max(total, idx)}")
    else:
        lines.append(f"Исходник {idx}")
    path = current["path"]
    lines.append("Путь:")
    lines.append(f"```\n{path}\n```")
    lines.append(f"Оцените исходник кнопками ниже: {RATEGRP_COLOR_PROMPT}")
    await send_fn("\n".join(lines), build_rategrp_color_keyboard())
    launch_media_preview(path)


async def rategrp_apply_rating(
    session: Dict[str, Any],
    color_key: str,
    send_fn: Callable[[str, Optional[InlineKeyboardMarkup]], Awaitable[Any]],
) -> None:
    queue: List[Dict[str, Any]] = session.get("rategrp_queue") or []
    if not queue:
        await send_fn("Очередь пустая. Выберите другую группу.", None)
        return
    choice = RATEGRP_COLOR_CHOICES.get(color_key)
    if not choice:
        await send_fn("Неизвестный цвет. Используйте кнопки.", None)
        return
    current = queue.pop(0)
    session["rategrp_queue"] = queue
    session["rategrp_processed"] = int(session.get("rategrp_processed", 0)) + 1
    emoji = choice["emoji"]
    try:
        updated_comments = db_set_source_color(current["id"], emoji)
        _rategrp_update_cached_row_color(session, current["id"], updated_comments)
    except Exception as exc:
        await send_fn(f"Не удалось сохранить оценку: {exc}", None)
        return
    prefix = f"{emoji} Исходник {current['name']} отмечен ({choice['label']})."
    await rategrp_send_next_prompt(session, send_fn, prefix=prefix)


async def _rategrp_start_rerate(
    session: Dict[str, Any],
    color_key: str,
    send_fn: Callable[[str, Optional[InlineKeyboardMarkup]], Awaitable[Any]],
) -> bool:
    info = RATEGRP_COLOR_CHOICES.get(color_key)
    rows = session.get("rategrp_rerate_rows") or []
    if not info:
        await send_fn("Неизвестный цвет.", None)
        return False
    if not rows:
        await send_fn("Нет исходников для переоценки. Выберите другую группу.", None)
        return False
    emoji = info["emoji"]
    filtered = [row for row in rows if _rategrp_row_color(row) == emoji]
    if not filtered:
        available = _rategrp_available_colors(rows)
        msg = "Не нашёл исходников этого цвета. Выберите другой."
        markup = build_rategrp_rerate_keyboard(available) if available else None
        await send_fn(msg, markup)
        return False
    queue = _rategrp_rows_to_queue(filtered)
    session["rategrp_queue"] = queue
    session["rategrp_total"] = len(queue)
    session["rategrp_processed"] = 0
    session["state"] = "rategrp_rate_source"
    session["rategrp_queue_origin"] = "rerate"
    await send_fn(
        f"Переоценка цвета {emoji} ({info['label']}). Исходников: {len(queue)}.", None
    )
    await rategrp_send_next_prompt(session, send_fn)
    return True


def normalize_rategrp_color_input(text: str) -> Optional[str]:
    lowered = text.strip().lower()
    mapping = {
        "green": "green",
        "зел": "green",
        "зелен": "green",
        "зеленая": "green",
        "🟢": "green",
        "yellow": "yellow",
        "жёлт": "yellow",
        "желт": "yellow",
        "желтая": "yellow",
        "🟡": "yellow",
        "red": "red",
        "красн": "red",
        "красная": "red",
        "🔴": "red",
        "pink": "pink",
        "розов": "pink",
        "розовая": "pink",
        "🩷": "pink",
        "blue": "blue",
        "син": "blue",
        "синяя": "blue",
        "🔵": "blue",
        "favorite": "favorite",
        "fav": "favorite",
        "звезд": "favorite",
        "избр": "favorite",
        "⭐": "favorite",
        "inspect": "inspect",
        "глаз": "inspect",
        "интерес": "inspect",
        "👁": "inspect",
        "delete": "delete",
        "удал": "delete",
        "крест": "delete",
        "❌": "delete",
    }
    for key, target in mapping.items():
        if lowered.startswith(key):
            return target
    return None


def _source_limit_message(sess: Dict[str, Any], available: int) -> str:
    if sess.get("music_color_rows"):
        return f"Для выбранного цвета доступно только {available} исходников. Возьмём их все."
    return f"В выбранной подпапке только {available} исходников. Возьмём их все."


def launch_media_preview(file_path: Union[str, Path]) -> None:
    if not MEDIA_PLAYER_EXECUTABLE.exists():
        return
    try:
        target = Path(file_path)
    except Exception:
        return
    if not target.exists():
        return
    try:
        subprocess.Popen([str(MEDIA_PLAYER_EXECUTABLE), str(target)])
    except Exception:
        pass


def apply_newcomp_folder_choice(
    sess: Dict[str, Any],
    token: str,
    next_state: str,
) -> Tuple[int, str]:
    folder_map: Dict[str, Dict[str, Any]] = sess.get("music_folder_map") or {}
    info = folder_map.get(token)
    if not info:
        raise ValueError("Не удалось найти такую подпапку.")
    rows = list(info.get("rows") or [])
    if not rows:
        raise ValueError("В выбранной подпапке нет исходников.")
    folder_label = info.get("label") or "Все папки"
    folder_path = info.get("path")
    choice = sess.setdefault("music_group_choice", {})
    choice["count"] = len(rows)
    choice["folder_path"] = folder_path
    choice["folder_label"] = folder_label
    sess["music_folder_choice"] = {
        "token": token,
        "label": folder_label,
        "path": folder_path,
        "count": len(rows),
    }
    sess["state"] = next_state
    return len(rows), folder_label


def _shorten_codec(codec: str) -> str:
    codec = (codec or "").strip().lower()
    if not codec:
        return "??"
    clean = "".join(ch for ch in codec if ch.isalnum())
    if not clean:
        clean = codec
    return clean[:2].ljust(2, "?")


def _format_compact_date(value: Optional[date]) -> str:
    if not value:
        return "------"
    return value.strftime("%d%m%y")


def collect_source_usage_stats() -> Dict[int, Dict[str, Any]]:
    """
    Строит индекс по source_id -> {count, last_date} из таблицы compilations.
    """
    usage: Dict[int, Dict[str, Any]] = {}
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT pmv_date, source_ids FROM compilations")
    rows = cur.fetchall()
    conn.close()

    for row in rows:
        date_str = row["pmv_date"]
        try:
            last_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            last_date = None
        source_field = row["source_ids"] or ""
        for part in source_field.replace(";", ",").split(","):
            part = part.strip()
            if not part.isdigit():
                continue
            sid = int(part)
            info = usage.setdefault(sid, {"count": 0, "last_date": None})
            info["count"] += 1
            if last_date and (info["last_date"] is None or last_date > info["last_date"]):
                info["last_date"] = last_date
    return usage


def _group_last_used_date(entry: SourceGroupEntry, usage_index: Dict[int, Dict[str, Any]]) -> Optional[date]:
    last: Optional[date] = None
    for row in entry.rows:
        sid = int(row["id"])
        info = usage_index.get(sid)
        if not info:
            continue
        dt = info.get("last_date")
        if dt and (last is None or dt > last):
            last = dt
    return last


def format_source_group_lines(
    entries: List[SourceGroupEntry],
    header: str,
    prefix_func: Optional[Callable[[SourceGroupEntry], str]] = None,
) -> List[str]:
    usage_index = collect_source_usage_stats()
    lines = [header]
    for idx, entry in enumerate(entries, 1):
        codec, res = entry.key
        codec_short = _shorten_codec(codec)
        resolution_label = f"{(res or '??x??')}{codec_short}"
        total = len(entry.rows)
        new_block = f"(🆕{entry.unused_count})" if entry.unused_count else ""
        count_label = f"{total}{new_block}"
        last_date = _format_compact_date(_group_last_used_date(entry, usage_index))
        prefix = ""
        if prefix_func:
            custom = (prefix_func(entry) or "").strip()
            if custom:
                prefix = f"{custom} "
        lines.append(f"{idx}. {prefix}{resolution_label} - {count_label} {last_date}")
    return lines


def sort_group_entries_with_orientation(
    entries: List[SourceGroupEntry],
) -> Tuple[List[SourceGroupEntry], Dict[Tuple[str, str], str]]:
    orientation_map: Dict[Tuple[str, str], str] = {}
    annotated: List[Tuple[SourceGroupEntry, str, int]] = []
    for entry in entries:
        label, order = _resolution_orientation(entry.key[1] or "")
        orientation_map[entry.key] = label
        annotated.append((entry, label, order))
    annotated.sort(
        key=lambda item: (
            item[2],
            -_resolution_pixels(item[0].key[1] or ""),
            -len(item[0].rows),
            f"{(item[0].key[0] or '').lower()}_{(item[0].key[1] or '').lower()}",
        )
    )
    sorted_entries = [item[0] for item in annotated]
    return sorted_entries, orientation_map


def sort_source_group_entries(entries: List[SourceGroupEntry]) -> List[SourceGroupEntry]:
    return sorted(
        entries,
        key=lambda e: (
            -_resolution_pixels(e.key[1] or ""),
            -len(e.rows),
            f"{(e.key[0] or '').lower()}_{(e.key[1] or '').lower()}",
        ),
    )

def db_get_used_sources_grouped() -> Dict[Tuple[str, str], List[sqlite3.Row]]:
    """
    Берёт ТОЛЬКО уже участвовавшие в компиляциях исходники (pmv_list не пустой)
    и группирует по (codec, resolution).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM sources
        WHERE pmv_list IS NOT NULL AND pmv_list != ''
        """
    )
    rows = cur.fetchall()
    conn.close()

    groups: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for r in rows:
        codec = r["codec"] or "?"
        resolution = r["resolution"] or "??x??"
        key = (codec, resolution)
        groups.setdefault(key, []).append(r)
    return groups


def db_get_used_sources_list() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM sources
        WHERE pmv_list IS NOT NULL AND pmv_list != ''
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows



def db_update_sources_pmv_list(source_ids: List[int], pmv_tag: str) -> None:
    if not source_ids:
        return
    conn = get_conn()
    cur = conn.cursor()
    for sid in source_ids:
        cur.execute("SELECT pmv_list FROM sources WHERE id = ?", (sid,))
        row = cur.fetchone()
        if not row:
            continue
        current = (row["pmv_list"] or "").strip()
        if not current:
            new_val = pmv_tag
        else:
            parts = [p.strip() for p in current.split(",") if p.strip()]
            if pmv_tag not in parts:
                parts.append(pmv_tag)
            new_val = ", ".join(parts)
        cur.execute("UPDATE sources SET pmv_list = ? WHERE id = ?", (new_val, sid))
    conn.commit()
    conn.close()


def db_insert_compilation(
    video_path: Path,
    source_ids: List[int],
    comments: str = "",
) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO compilations (video_path, pmv_date, source_ids, comments)
        VALUES (?, ?, ?, ?)
        """,
        (
            str(video_path.resolve()),
            date.today().isoformat(),
            ",".join(str(sid) for sid in source_ids),
            comments,
        ),
    )
    conn.commit()
    conn.close()

def db_get_all_sources() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, video_name, video_path, comments
        FROM sources
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def db_get_sources_full() -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, video_name, video_path, size_bytes, codec, resolution,
               pmv_list, comments, date_added
        FROM sources
        ORDER BY id
        """
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()
    return rows


def db_update_source_fields(source_id: int, **fields: Any) -> None:
    if not fields:
        return
    columns = ", ".join(f"{key} = ?" for key in fields.keys())
    params = list(fields.values())
    params.append(source_id)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE sources SET {columns} WHERE id = ?", params)
    conn.commit()
    conn.close()


def db_delete_sources_by_ids(ids: Iterable[int]) -> int:
    ids_list = [int(i) for i in ids]
    if not ids_list:
        return 0
    placeholders = ", ".join("?" for _ in ids_list)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"DELETE FROM sources WHERE id IN ({placeholders})", ids_list)
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted


def db_get_sources_with_comments() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, video_name, video_path, comments
        FROM sources
        WHERE comments IS NOT NULL AND comments != ''
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def db_get_compilations_with_comments() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, video_path, pmv_date, comments
        FROM compilations
        WHERE comments IS NOT NULL AND comments != ''
        ORDER BY pmv_date DESC, id DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def db_get_compilation_by_video_path(video_path: Path) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    resolved = str(video_path.resolve())
    cur.execute("SELECT * FROM compilations WHERE video_path = ?", (resolved,))
    row = cur.fetchone()
    if not row:
        cur.execute(
            "SELECT * FROM compilations WHERE video_path LIKE ? ORDER BY id DESC LIMIT 1",
            (f"%{video_path.name}",),
        )
        row = cur.fetchone()
    conn.close()
    return row


def _normalize_windows_share_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    return normalized.rstrip("/")


def convert_windows_path_to_nas(path: Optional[str]) -> Optional[str]:
    if not path or not NAS_SHARE_PREFIX or not NAS_SHARE_ROOT:
        return None
    normalized = _normalize_windows_share_path(path)
    prefix = _normalize_windows_share_path(NAS_SHARE_PREFIX)
    if not normalized.lower().startswith(prefix.lower()):
        return None
    suffix = normalized[len(prefix):].lstrip("/")
    if not suffix:
        return None
    root = NAS_SHARE_ROOT.rstrip("/")
    return f"{root}/{suffix}"


def collect_symlink_plan() -> Tuple[Dict[str, List[Tuple[int, str, str]]], int]:
    """
    Возвращает {folder: [(source_id, target_path, safe_name), ...]}, пропуская
    исходники без оценки или без сопоставимого пути.
    """
    plan: Dict[str, List[Tuple[int, str, str]]] = defaultdict(list)
    skipped = 0
    conn = get_conn()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT id, video_path, comments FROM sources")
    for row in cur.fetchall():
        color_key = _rategrp_row_color(row)
        if not color_key:
            continue
        folder = NAS_SYMLINK_COLOR_FOLDERS.get(color_key)
        if not folder:
            continue
        video_path = row["video_path"]
        remote_target = convert_windows_path_to_nas(video_path)
        if not remote_target:
            skipped += 1
            continue
        try:
            source_id = int(row["id"])
        except Exception:
            skipped += 1
            continue
        safe_name = sanitize_filename(Path(video_path).name or f"source_{source_id}")
        plan[folder].append((source_id, remote_target, safe_name))
    conn.close()
    return plan, skipped


def sync_nas_symlinks() -> List[str]:
    if not NAS_SSH_HOST or not NAS_SIM_REMOTE_ROOT:
        return []
    plan, skipped = collect_symlink_plan()
    total_links = sum(len(v) for v in plan.values())
    if total_links == 0:
        if skipped:
            return [f"ℹ️ Симлинки: нет доступных исходников (пропущено {skipped})."]
        return ["ℹ️ Симлинки: нет исходников с оценками — синхронизация пропущена."]
    try:
        import paramiko
    except ImportError:
        return [
            "⚠️ Установите пакет paramiko (`pip install paramiko`), чтобы обновлять симлинки на NAS."
        ]

    root = NAS_SIM_REMOTE_ROOT.rstrip("/")
    script_lines = [
        "set -e",
        f"ROOT={shlex.quote(root)}",
        "mkdir -p \"$ROOT\"",
        "if [ -d \"$ROOT\" ]; then find \"$ROOT\" -mindepth 1 -maxdepth 1 -exec rm -rf {} +; fi",
    ]

    for folder, entries in plan.items():
        folder_path = f"{root}/{folder}"
        script_lines.append(f"mkdir -p {shlex.quote(folder_path)}")
        for source_id, target_path, safe_name in entries:
            link_path = f"{folder_path}/{source_id}_{safe_name}"
            script_lines.append(
                f"ln -sf {shlex.quote(target_path)} {shlex.quote(link_path)}"
            )

    script = "\n".join(script_lines) + "\n"

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    messages: List[str] = []
    try:
        client.connect(
            NAS_SSH_HOST,
            port=NAS_SSH_PORT,
            username=NAS_SSH_USER,
            password=NAS_SSH_PASSWORD or None,
            timeout=NAS_SSH_TIMEOUT,
        )
        stdin, stdout, stderr = client.exec_command("bash -s", timeout=NAS_SSH_TIMEOUT)
        stdin.write(script)
        stdin.channel.shutdown_write()
        out = stdout.read().decode("utf-8", errors="ignore").strip()
        err = stderr.read().decode("utf-8", errors="ignore").strip()
        exit_code = stdout.channel.recv_exit_status()
    finally:
        client.close()

    if exit_code != 0:
        return [
            f"⚠️ Ошибка при обновлении симлинков на NAS (код {exit_code}).",
            err or out or "Без дополнительного вывода.",
        ]

    msg = f"✅ На NAS создано {total_links} симлинков в {len(plan)} папках."
    if skipped:
        msg += f" Пропущено путей: {skipped}."
    messages.append(msg)
    if out:
        messages.append(out)
    if err:
        messages.append(err)
    return messages


def db_get_sources_by_ids(ids: Iterable[int]) -> List[sqlite3.Row]:
    ids_list = [int(i) for i in ids if int(i) > 0]
    if not ids_list:
        return []
    placeholders = ",".join("?" for _ in ids_list)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM sources WHERE id IN ({placeholders})", ids_list)
    rows = cur.fetchall()
    conn.close()
    return rows


def db_get_random_name() -> str:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, adjective, noun, verb, number FROM random_names")
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return "PMV_from_files"
    row = random.choice(rows)
    adj = row["adjective"]
    noun = row["noun"]
    verb = row["verb"]
    num = row["number"]
    # без пробелов, чтобы имя файла было аккуратным
    return f"{adj}_{noun}_{verb}{num}"


# Доп. DB-хелперы для проблемных файлов

def db_get_source_id_by_path(video_path: Path) -> Optional[int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM sources WHERE video_path = ?", (str(video_path.resolve()),))
    row = cur.fetchone()
    conn.close()
    return int(row["id"]) if row else None


def db_mark_source_problem(source_id: int, reason: str) -> None:
    reason = reason.strip()
    if not reason:
        reason = "unknown"
    ts = datetime.now().strftime("%Y-%m-%d")
    db_append_source_comment(source_id, f"problem={reason};date={ts}")


def db_get_problem_sources() -> List[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, video_name, video_path, comments
        FROM sources
        WHERE comments LIKE '%problem=%'
        ORDER BY id
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows

# =========================
# FFPROBE / FFMPEG УТИЛИТЫ
# =========================


def ffprobe_available() -> bool:
    try:
        subprocess.check_output([FFPROBE_BIN, "-version"], stderr=subprocess.STDOUT)
        return True
    except Exception:
        return False



def ffmpeg_probe_duration_seconds(path: Path) -> float:
    try:
        out = subprocess.run(
            [FFMPEG_BIN, "-hide_banner", "-i", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        m = re.search(r"Duration:\s*(\d{2}):(\d{2}):(\d{2}\.\d+)", out.stderr or "")
        if not m:
            return 0.0
        hh, mm, ss = m.groups()
        return int(hh) * 3600 + int(mm) * 60 + float(ss)
    except Exception:
        return 0.0


def ffprobe_duration_seconds(path: Path) -> float:
    if not ffprobe_available():
        return ffmpeg_probe_duration_seconds(path)
    try:
        out = subprocess.check_output(
            [
                FFPROBE_BIN,
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            text=True,
        ).strip()
        return float(out)
    except Exception:
        return ffmpeg_probe_duration_seconds(path)



def normalize_codec(codec: str) -> str:
    codec = (codec or "").lower()
    if codec in ("avc1", "h264"):
        return "h264"
    if codec in ("hvc1", "hev1", "hevc"):
        return "hevc"
    return codec


def video_info_sort(path: Path) -> Tuple[str, str]:
    if ffprobe_available():
        try:
            out = subprocess.check_output(
                [
                    FFPROBE_BIN,
                    "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=codec_name,width,height",
                    "-of", "default=nw=1:nk=1",
                    str(path),
                ],
                text=True,
            ).strip().splitlines()
            codec = normalize_codec(out[0] if len(out) > 0 else "")
            w = out[1] if len(out) > 1 else ""
            h = out[2] if len(out) > 2 else ""
            wh = f"{w}x{h}" if w and h else ""
            return codec, wh
        except Exception:
            pass

    try:
        pr = subprocess.run(
            [FFMPEG_BIN, "-hide_banner", "-i", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stderr = pr.stderr or ""
        m = re.search(r"Video:\s*([a-z0-9_]+)", stderr, re.I)
        codec = normalize_codec(m.group(1)) if m else ""
        m2 = re.search(r"(\d{2,5})x(\d{2,5})", stderr)
        wh = f"{m2.group(1)}x{m2.group(2)}" if m2 else ""
        return codec, wh
    except Exception:
        return "", ""



def detect_video_info(path: Path) -> Dict[str, str]:
    info = {"codec_name": "", "width": "", "height": "", "fps": "", "pix_fmt": ""}
    if ffprobe_available():
        try:
            out = subprocess.check_output(
                [
                    FFPROBE_BIN,
                    "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=codec_name,width,height,avg_frame_rate,pix_fmt",
                    "-of", "default=nw=1:nk=1",
                    str(path),
                ],
                text=True,
            ).strip().splitlines()
            vals = (out + [""] * 5)[:5]
            info["codec_name"], info["width"], info["height"], fps, info["pix_fmt"] = vals
            if "/" in fps and fps != "0/0":
                try:
                    a, b = fps.split("/")
                    fps = str(round(float(a) / float(b), 3))
                except Exception:
                    pass
            info["fps"] = fps
            return info
        except Exception:
            pass

    try:
        pr = subprocess.run(
            [FFMPEG_BIN, "-hide_banner", "-i", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stderr = pr.stderr or ""
        m = re.search(
            r"Video:\s*([a-z0-9_]+)\s*(\([^)]+\))?,\s*([0-9a-z_]+)?\s*,\s*(\d{2,5})x(\d{2,5})",
            stderr,
            re.I,
        )
        if m:
            info["codec_name"] = m.group(1).lower()
            info["pix_fmt"] = (m.group(3) or "").lower()
            info["width"] = m.group(4) or ""
            info["height"] = m.group(5) or ""
        m2 = re.search(r"(\d+(?:\.\d+)?|\d+/\d+)\s*fps", stderr, re.I)
        if m2:
            info["fps"] = m2.group(1)
    except Exception:
        pass
    return info



def pick_temp_dir(candidates: List[Path], min_free_bytes: int = 5 * 1024**3) -> Path:
    for d in candidates:
        try:
            d.mkdir(parents=True, exist_ok=True)
            total, used, free = shutil.disk_usage(str(d))
            if free > min_free_bytes:
                return d
        except Exception:
            continue
    d0 = candidates[0]
    d0.mkdir(parents=True, exist_ok=True)
    return d0


# =========================
# ЛОГИКА ПЛАНИРОВАНИЯ КЛИПОВ
# =========================

def allocate_equalish(
    target_sec: int,
    files: List[Path],
    durations: Dict[Path, int],
    per_min: int = PER_FILE_MIN_SECONDS,
    per_max: int = PER_FILE_MAX_SECONDS,
) -> Dict[Path, int]:
    """
    Равномерное распределение времени между файлами.

    ВАЖНО: теперь НЕ форсим per_min, если target слишком маленький,
    чтобы не получить ситуацию, когда 15 минут → 45 минут.
    """
    n = len(files)
    if n == 0 or target_sec <= 0:
        return {f: 0 for f in files}

    total_dur = sum(durations.get(f, 0) for f in files)
    if total_dur <= 0:
        return {f: 0 for f in files}

    ideal = max(1, target_sec // n)
    # только верхний предел, без нижнего
    ideal_clamped = min(per_max, ideal)

    alloc: Dict[Path, int] = {}
    for f in files:
        dur = durations.get(f, 0)
        alloc[f] = min(ideal_clamped, dur)

    max_total = min(target_sec, total_dur)
    current = sum(alloc.values())
    leftover = max_total - current
    if leftover <= 0:
        return alloc

    order = sorted(files, key=lambda f: durations.get(f, 0), reverse=True)

    while leftover > 0:
        progressed = False
        for f in order:
            dur = durations.get(f, 0)
            # всё ещё учитываем пер-файл максимум
            if alloc[f] >= min(per_max, dur):
                continue
            alloc[f] += 1
            leftover -= 1
            progressed = True
            if leftover <= 0:
                break
        if not progressed:
            break

    return alloc


def split_into_big_parts(file_len: int, parts: int) -> List[Tuple[int, int]]:
    parts = max(1, parts)
    base = file_len // parts
    rem = file_len - base * parts
    res = []
    cur = 0
    for i in range(parts):
        L = base + (1 if i < rem else 0)
        res.append((cur, L))
        cur += L
    return res


def jittered_partition(total_len: int, count: int, min_each: int = 1) -> List[int]:
    """Делит total_len на count частей с шумом, соблюдая min_each.
    Гарантирует корректные границы для randint даже при малых total_len.
    """
    rng = random.Random(RANDOM_SEED)
    total_len = max(0, int(total_len))
    min_each = max(1, int(min_each))

    # сколько частей реально можем сделать, учитывая минимум
    max_count = max(1, total_len // min_each) if total_len > 0 else 1
    count = max(1, min(int(count), max_count))

    base = max(min_each, total_len // count) if count > 0 else total_len
    jitter = max(1, int(base * 0.3))

    lens: List[int] = []
    remain = total_len
    left = count
    for _ in range(count):
        if left == 1:
            x = remain
        else:
            low = max(min_each, base - jitter)
            # верхняя граница с учётом оставшегося минимума на хвост
            upper_cap = remain - (left - 1) * min_each
            high = max(low, min(base + jitter, upper_cap))
            if high < low:
                x = max(min_each, min(upper_cap, low))
            else:
                x = rng.randint(low, high)
        lens.append(x)
        remain -= x
    left -= 1
    return lens


def _clip_guard_limits(duration: int) -> Tuple[bool, int, int]:
    head = max(0, int(CLIP_HEAD_GUARD_SECONDS))
    tail = max(0, int(CLIP_TAIL_GUARD_SECONDS))
    if duration <= head + tail:
        return False, 0, duration
    guard_start = min(head, duration)
    guard_end = max(guard_start + 1, duration - tail)
    guard_end = min(guard_end, duration)
    return True, guard_start, guard_end



def _plan_for_file_default(
    file_path: Path,
    file_alloc: int,
    big_parts: int,
    small_per_big: int,
) -> List[Tuple[int, int]]:
    dur = int(ffprobe_duration_seconds(file_path))
    if dur <= 0 or file_alloc <= 0:
        return []

    guard_active, guard_start, guard_end = _clip_guard_limits(dur)

    windows = split_into_big_parts(dur, big_parts)
    base = file_alloc // len(windows)
    rem = file_alloc - base * len(windows)

    clips: List[Tuple[int, int]] = []

    for idx, (wstart, wlen) in enumerate(windows):
        if guard_active:
            eff_start = max(wstart, guard_start)
            eff_end = min(wstart + wlen, guard_end)
            wstart_eff = eff_start
            wlen_eff = max(0, eff_end - eff_start)
        else:
            wstart_eff = wstart
            wlen_eff = wlen
        alloc = min(wlen_eff, base + (1 if idx < rem else 0))
        if alloc <= 0:
            continue
        
        spb = min(small_per_big, max(1, alloc // MIN_SMALL_CLIP_SECONDS))
        lengths = jittered_partition(alloc, spb, min_each=MIN_SMALL_CLIP_SECONDS)

        total_clips_len = sum(lengths)
        slack = max(0, wlen_eff - total_clips_len)
        gaps = spb + 1
        base_gap = slack // gaps
        extra = slack - base_gap * gaps
        cur = wstart_eff + base_gap + (1 if extra > 0 else 0)
        extra_left = max(0, extra - 1)
        for L in lengths:
            clips.append((cur, L))
            cur = cur + L + base_gap + (1 if extra_left > 0 else 0)
            if extra_left > 0:
                extra_left -= 1

    clips.sort(key=lambda x: x[0])
    total = sum(d for _, d in clips)
    if total > file_alloc and clips:
        overflow = total - file_alloc
        s, d = clips[-1]
        d2 = max(1, d - overflow)
        clips[-1] = (s, d2)
    return clips


def plan_for_file(
    file_path: Path,
    file_alloc: int,
    big_parts: int,
    small_per_big: int,
    algo_key: str = "default",
) -> List[Tuple[int, int]]:
    if algo_key == "poi":
        clips = plan_for_file_poi(file_path, file_alloc)
        if clips:
            return clips
    return _plan_for_file_default(file_path, file_alloc, big_parts, small_per_big)


def _extract_audio_energy_profile(path: Path) -> List[Tuple[float, float]]:
    cmd = [
        FFMPEG_BIN,
        "-hide_banner",
        "-nostats",
        "-loglevel",
        "error",
        "-i",
        str(path),
        "-af",
        f"astats=metadata=1:reset={POI_ANALYSIS_RESET},ametadata=print:file=-",
        "-f",
        "null",
        "-",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return []
    output = (result.stdout or "") + (result.stderr or "")
    samples: List[Tuple[float, float]] = []
    for raw in output.splitlines():
        line = raw.strip()
        if not line or "pts_time" not in line:
            continue
        parts: Dict[str, str] = {}
        for token in line.split("|"):
            if "=" in token:
                key, value = token.split("=", 1)
                parts[key.strip()] = value.strip()
        try:
            timestamp = float(parts.get("pts_time", "0"))
        except ValueError:
            continue
        key_name = parts.get("key") or ""
        if "Peak_level" not in key_name:
            continue
        try:
            db_val = float(parts.get("value", "-120"))
        except ValueError:
            continue
        amplitude = 10 ** (db_val / 20.0)
        if math.isfinite(amplitude):
            samples.append((timestamp, amplitude))
    return samples


def _select_audio_poi_points(
    duration: float,
    samples: List[Tuple[float, float]],
) -> List[Tuple[float, float]]:

    if not samples or duration <= 0:
        return []
    samples.sort(key=lambda x: x[0])
    minutes = max(1, math.ceil(duration / 60.0))
    per_minute: Dict[int, List[Tuple[float, float]]] = {}
    for ts, amp in samples:
        idx = min(minutes - 1, int(ts // 60))
        per_minute.setdefault(idx, []).append((amp, ts))
    points: List[Tuple[float, float]] = []
    for minute in range(minutes):
        candidates = per_minute.get(minute, [])
        if not candidates:
            continue
        low, high = POI_POINTS_PER_MIN_RANGE
        want = random.randint(low, high)
        want = max(1, min(want, len(candidates)))
        top = heapq.nlargest(want, candidates)
        for amp, ts in top:
            points.append((ts, amp))
    if not points:
        top_global = heapq.nlargest(min(len(samples), POI_MAX_POINTS), samples, key=lambda x: x[1])
        points = [(ts, amp) for ts, amp in top_global]
    points.sort(key=lambda x: x[0])
    return points


def plan_for_file_poi(file_path: Path, file_alloc: int) -> List[Tuple[int, int]]:
    duration = float(ffprobe_duration_seconds(file_path))
    if duration <= 0 or file_alloc <= 0:
        return []
    duration_seconds = max(1, int(duration))
    file_alloc = min(int(file_alloc), duration_seconds)
    if file_alloc <= 0:
        return []
    samples = _extract_audio_energy_profile(file_path)
    poi_points = _select_audio_poi_points(duration, samples)
    if not poi_points:
        return []
    max_points = max(1, min(len(poi_points), max(1, file_alloc // MIN_SMALL_CLIP_SECONDS)))
    if len(poi_points) > max_points:
        poi_points = heapq.nlargest(max_points, poi_points, key=lambda x: x[1])
    poi_points.sort(key=lambda x: x[0])
    per_clip_min = min(
        MIN_SMALL_CLIP_SECONDS,
        max(1, file_alloc // len(poi_points)),
    )
    lengths = jittered_partition(file_alloc, len(poi_points), min_each=per_clip_min)
    guard_active, guard_start, guard_end = _clip_guard_limits(duration_seconds)
    clips: List[Tuple[int, int]] = []
    for (ts, _), clip_len in zip(poi_points, lengths):
        jitter = random.uniform(-POI_SPREAD_SECONDS, POI_SPREAD_SECONDS)
        center = ts + jitter
        start = int(max(0, round(center - clip_len / 2)))
        if guard_active:
            safe_window = max(1, guard_end - guard_start)
            clip_len = min(clip_len, safe_window)
            min_start = guard_start
            max_start = max(min_start, guard_end - clip_len)
            start = max(min_start, min(start, max_start))
        else:
            max_start = max(0, duration_seconds - clip_len)
            if start > max_start:
                start = max_start
        clips.append((start, clip_len))
    clips.sort(key=lambda x: x[0])
    return clips


ClipQueue = Dict[Path, List[Tuple[int, int]]]
ClipSequence = List[Tuple[Path, int, int]]


def _clone_clip_queue(per_file: ClipQueue) -> ClipQueue:
    return {path: clips[:] for path, clips in per_file.items() if clips}


def _sequence_carousel(per_file: ClipQueue) -> ClipSequence:
    queues = _clone_clip_queue(per_file)
    out: ClipSequence = []
    while queues:
        for path in list(queues.keys()):
            clips = queues.get(path, [])
            if not clips:
                queues.pop(path, None)
                continue
            start, dur = clips.pop(0)
            out.append((path, start, dur))
            if not clips:
                queues.pop(path, None)
    return out


def _sequence_group_waves(per_file: ClipQueue) -> ClipSequence:
    queues = _clone_clip_queue(per_file)
    files = list(queues.keys())
    random.shuffle(files)
    out: ClipSequence = []
    idx = 0
    total = len(files)
    while idx < total:
        remaining = total - idx
        if remaining == 1:
            group_size = 1
        else:
            group_size = random.randint(2, min(4, remaining))
        group_files = files[idx: idx + group_size]
        idx += group_size
        group_map: ClipQueue = {f: queues.pop(f, []) for f in group_files if queues.get(f)}
        # если файл оказался пустым, пропускаем
        group_files = [f for f in group_files if group_map.get(f)]
        while group_files:
            for f in list(group_files):
                clips = group_map.get(f)
                if not clips:
                    group_files.remove(f)
                    continue
                start, dur = clips.pop(0)
                out.append((f, start, dur))
                if not clips:
                    group_files.remove(f)
    # если что-то осталось (например, файлы без групп) — добиваем каруселью
    if queues:
        out.extend(_sequence_carousel(queues))
    return out


def _sequence_burst_shuffle(per_file: ClipQueue) -> ClipSequence:
    queues = _clone_clip_queue(per_file)
    out: ClipSequence = []
    while True:
        candidates = [(f, len(clips)) for f, clips in queues.items() if clips]
        if not candidates:
            break
        total = sum(cnt for _, cnt in candidates)
        pick = random.randint(1, max(1, total))
        acc = 0
        chosen = candidates[0][0]
        for f, cnt in candidates:
            acc += cnt
            if pick <= acc:
                chosen = f
                break
        burst = random.randint(1, max(1, min(3, len(queues[chosen]))))
        for _ in range(burst):
            if not queues[chosen]:
                break
            start, dur = queues[chosen].pop(0)
            out.append((chosen, start, dur))
        if not queues[chosen]:
            queues.pop(chosen)
    return out


def _sequence_poi(per_file: ClipQueue) -> ClipSequence:
    out: ClipSequence = []
    for path, clips in per_file.items():
        for start, dur in sorted(clips, key=lambda x: x[0]):
            out.append((path, start, dur))
    return out


def _sequence_strata(per_file: ClipQueue) -> ClipSequence:
    """Выдаёт все клипы одного файла подряд, затем переходит к следующему."""
    out: ClipSequence = []
    for path, clips in per_file.items():
        for start, dur in sorted(clips, key=lambda x: x[0]):
            out.append((path, start, dur))
    return out


SequenceBuilder = Callable[[ClipQueue], ClipSequence]


CLIP_SEQUENCE_ALGORITHMS: Dict[str, Dict[str, Any]] = {
    "carousel": {
        "short": "CAR",
        "title": "Карусель",
        "description": "Чередует клипы всех исходников по кругу.",
        "builder": _sequence_carousel,
    },
    "waves": {
        "short": "WAV",
        "title": "Волны",
        "description": "Собирает источники в небольшие группы и перемешивает внутри каждой волны.",
        "builder": _sequence_group_waves,
    },
    "bursts": {
        "short": "BST",
        "title": "Бёрсты",
        "description": "Делает случайные серии клипов из одного источника, затем перескакивает на другой.",
        "builder": _sequence_burst_shuffle,
    },
    "poi": {
        "short": "POI",
        "title": "Points of Interest",
        "description": "Ищет громкие участки аудио и вырезает клипы рядом с ними, сохраняя хронологию.",
        "builder": _sequence_poi,
    },
    "strata": {
        "short": "LAY",
        "title": "Слои ключевых точек",
        "description": "Собирает блоками: выдаёт все клипы первого видео, потом второго и т.д.",
        "builder": _sequence_strata,
    },
}

DEFAULT_CLIP_ALGO = "carousel"

CLIP_ALGO_CHOICE_MAP: Dict[str, str] = {}
for _key, _meta in CLIP_SEQUENCE_ALGORITHMS.items():
    CLIP_ALGO_CHOICE_MAP[_key.lower()] = _key
    short = (_meta.get("short") or "").lower()
    if short:
        CLIP_ALGO_CHOICE_MAP[short] = _key


def normalize_clip_algo_choice(choice: str) -> Optional[str]:
    if not choice:
        return None
    return CLIP_ALGO_CHOICE_MAP.get(choice.strip().lower())


def resolve_clip_algorithm(key: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    if not key or key not in CLIP_SEQUENCE_ALGORITHMS:
        key = DEFAULT_CLIP_ALGO
    return key, CLIP_SEQUENCE_ALGORITHMS[key]


class ClipAlgorithmPicker:
    def __init__(self, total_slots: int):
        self.total_slots = max(1, total_slots)
        self.keys = list(CLIP_SEQUENCE_ALGORITHMS.keys())
        if not self.keys:
            raise RuntimeError("Не заданы алгоритмы нарезки.")
        self.unique_quota = min(self.total_slots, len(self.keys))
        self.unique_deck = random.sample(self.keys, len(self.keys))
        self.unique_index = 0
        self.recycle: List[str] = []
        self._current: Optional[str] = None

    def _next_from_pool(self) -> str:
        if self.unique_index < self.unique_quota:
            key = self.unique_deck[self.unique_index]
            self.unique_index += 1
            return key
        if not self.recycle:
            self.recycle = random.sample(self.keys, len(self.keys))
        return self.recycle.pop()

    def current(self) -> str:
        if self._current is None:
            self._current = self._next_from_pool()
        return self._current

    def commit(self) -> None:
        self._current = None


@dataclass
class MusicSegment:
    index: int
    start: float
    end: float
    duration: float
    intensity: float


CLICK_SAMPLE_RATE = 44100
CLICK_DURATION_SECONDS = 0.05
CLICK_FREQUENCY_HZ = 1200.0
CLICK_AMPLITUDE = 12000


def sanitize_filename(text: str) -> str:
    clean = re.sub(r"[^\w\-. ]+", "_", text.strip())
    return clean or "music_project"


def parse_manifest_segments(manifest: Dict[str, Any]) -> List[MusicSegment]:
    analysis = manifest.get("analysis") or {}
    raw_segments = analysis.get("segments") or []
    segments: List[MusicSegment] = []
    for idx, seg in enumerate(raw_segments):
        try:
            start = float(seg.get("start") or 0.0)
            end = float(seg.get("end") or 0.0)
            duration = float(seg.get("duration") or (end - start))
            intensity = float(seg.get("intensity") or 0.0)
        except Exception:
            continue
        if duration <= 0:
            duration = max(0.5, end - start)
        segments.append(
            MusicSegment(
                index=idx,
                start=max(0.0, start),
                end=max(end, start),
                duration=max(0.1, duration),
                intensity=max(0.0, min(1.0, intensity)),
            )
        )
    return segments


def _build_click_track_samples(starts: List[float], total_duration: float) -> array:
    max_time = max(total_duration, max(starts or [0.0])) + CLICK_DURATION_SECONDS + 0.5
    total_samples = max(1, int(math.ceil(max_time * CLICK_SAMPLE_RATE)))
    buf = array("h", [0]) * total_samples
    click_len = max(1, int(CLICK_DURATION_SECONDS * CLICK_SAMPLE_RATE))
    for start in starts:
        if start < 0:
            continue
        start_sample = int(start * CLICK_SAMPLE_RATE)
        if start_sample >= total_samples:
            continue
        for i in range(click_len):
            idx = start_sample + i
            if idx >= total_samples:
                break
            sample_val = int(
                math.sin(2.0 * math.pi * CLICK_FREQUENCY_HZ * (i / CLICK_SAMPLE_RATE))
                * CLICK_AMPLITUDE
            )
            mixed = buf[idx] + sample_val
            if mixed > 32767:
                mixed = 32767
            elif mixed < -32768:
                mixed = -32768
            buf[idx] = mixed
    return buf


def _write_wave_file(path: Path, samples: array, sample_rate: int = CLICK_SAMPLE_RATE) -> None:
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(samples.tobytes())


def mix_audio_with_click_track(audio_path: Path, click_path: Path, output_path: Path) -> None:
    cmd = [
        FFMPEG_BIN,
        "-y",
        "-i",
        str(audio_path),
        "-i",
        str(click_path),
        "-filter_complex",
        "amix=inputs=2:normalize=0",
        "-c:a",
        "libmp3lame",
        str(output_path),
    ]
    subprocess.check_call(cmd)


def generate_musicprep_click_preview(project: Dict[str, Any]) -> Path:
    manifest_data = project.get("manifest_data")
    manifest_path = Path(project.get("manifest_path") or "")
    if not manifest_data and manifest_path.exists():
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
        project["manifest_data"] = manifest_data
    segments = parse_manifest_segments(manifest_data or {})
    if not segments:
        raise ValueError("В проекте нет сегментов для анализа.")
    audio_path = project.get("audio_path")
    if not audio_path:
        candidate = Path(project.get("dir") or MUSIC_PROJECTS_DIR)
        audio_path = candidate / "audio.mp3"
    audio_path = Path(audio_path)
    if not audio_path.exists():
        raise FileNotFoundError(f"Не найден audio.mp3 в проекте {project.get('slug')}.")

    starts = [seg.start for seg in segments]
    duration = max(ffprobe_duration_seconds(audio_path), segments[-1].end)
    tmp_parent = pick_temp_dir(TEMP_DIRS, min_free_bytes=500 * 1024**2)
    with tempfile.TemporaryDirectory(prefix="click_", dir=str(tmp_parent)) as tmpdir:
        tmp_root = Path(tmpdir)
        click_wav = tmp_root / "click_track.wav"
        samples = _build_click_track_samples(starts, duration)
        _write_wave_file(click_wav, samples)
        slug = sanitize_filename(project.get("slug") or project.get("name") or "project").replace(" ", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = Path(project.get("dir") or (MUSIC_PROJECTS_DIR / slug))
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / f"{slug}_clickcheck_{timestamp}.mp3"
        mix_audio_with_click_track(audio_path, click_wav, output_path)
    return output_path


def build_music_source_sequence(
    source_paths: List[Path],
    algo_key: str,
    total_segments: int,
) -> List[Path]:
    if not source_paths:
        raise ValueError("Нет исходников для музыкальной компиляции.")
    if total_segments <= 0:
        return []
    seq: List[Path] = []
    key = algo_key or DEFAULT_CLIP_ALGO
    files = source_paths[:]

    if key == "waves":
        random.shuffle(files)
        idx = 0
        while len(seq) < total_segments:
            group_size = random.randint(2, min(4, len(files)))
            group = files[idx:idx + group_size]
            if not group:
                random.shuffle(files)
                idx = 0
                continue
            for path in group:
                seq.append(path)
                if len(seq) >= total_segments:
                    break
            idx += group_size
            if idx >= len(files):
                idx = 0
                random.shuffle(files)
    elif key == "bursts":
        while len(seq) < total_segments:
            path = random.choice(source_paths)
            burst = random.randint(2, 4)
            for _ in range(burst):
                seq.append(path)
                if len(seq) >= total_segments:
                    break
    elif key == "strata":
        if not files:
            return []
        base = total_segments // len(files)
        rem = total_segments % len(files)
        for idx, path in enumerate(files):
            share = base + (1 if idx < rem else 0)
            if share <= 0:
                continue
            seq.extend([path] * share)
    else:
        idx = 0
        while len(seq) < total_segments:
            seq.append(files[idx % len(files)])
            idx += 1
    return seq[:total_segments]


def choose_random_source_rows(
    count: int,
    group_strategy: str = "max_group",
    preferred_group: Optional[Tuple[str, str]] = None,
    preferred_folder: Optional[str] = None,
) -> List[sqlite3.Row]:
    if count <= 0:
        raise ValueError("Количество исходников должно быть > 0")

    folder_norm = _normalize_path_prefix(preferred_folder) if preferred_folder else None

    def row_matches_folder(row: sqlite3.Row) -> bool:
        if not folder_norm:
            return True
        try:
            parent = Path(row["video_path"]).resolve(strict=False).parent
        except Exception:
            parent = Path(row["video_path"]).parent
        return _normalize_path_prefix(parent) == folder_norm

    unused = db_get_unused_sources_grouped()
    all_groups = db_get_all_sources_grouped()

    def fetch_group_rows(key: Tuple[str, str]) -> List[sqlite3.Row]:
        rows = all_groups.get(key)
        if not rows:
            rows = unused.get(key, [])
        filtered = [row for row in (rows or []) if row_matches_folder(row)]
        return filtered

    if preferred_group:
        rows = fetch_group_rows(preferred_group)
        if len(rows) < count:
            raise RuntimeError(
                f"В выбранной группе недостаточно исходников (есть {len(rows)}, нужно {count})."
            )
        random.shuffle(rows)
        return rows[:count]

    grouped = unused if unused else all_groups
    groups = list(grouped.items())
    if not groups:
        raise RuntimeError("В базе нет подходящих исходников.")

    if group_strategy == "random":
        random.shuffle(groups)
    else:
        groups.sort(key=lambda kv: -len(kv[1]))

    selected: List[sqlite3.Row] = []
    seen_dirs: Dict[Path, int] = {}

    for _, rows in groups:
        if len(selected) >= count:
            break
        random.shuffle(rows)
        for row in rows:
            if not row_matches_folder(row):
                continue
            try:
                path = Path(row["video_path"]).resolve()
                parent = path.parent
            except Exception:
                continue
            if not path.exists():
                continue
            if seen_dirs.get(parent, 0) >= PER_DIR_MAX_FIRST_PASS:
                continue
            selected.append(row)
            seen_dirs[parent] = seen_dirs.get(parent, 0) + 1
            if len(selected) >= count:
                break

    if len(selected) < count:
        leftovers: List[sqlite3.Row] = []
        for _, rows in groups:
            leftovers.extend(rows)
        random.shuffle(leftovers)
        for row in leftovers:
            if len(selected) >= count:
                break
            try:
                path = Path(row["video_path"])
            except Exception:
                continue
            if not path.exists():
                continue
            selected.append(row)

    if len(selected) < count:
        raise RuntimeError("Недостаточно исходников для музыкальной компиляции.")
    return selected[:count]


def pick_specific_source_rows(
    rows: List[sqlite3.Row],
    count: int,
    min_new_required: int = 0,
) -> List[sqlite3.Row]:
    if count <= 0:
        raise ValueError("Количество источников должно быть > 0")
    existing: List[sqlite3.Row] = []
    for row in rows:
        try:
            path = Path(row["video_path"]).resolve()
        except Exception:
            continue
        if not path.exists():
            continue
        existing.append(row)
    if len(existing) < count:
        raise RuntimeError("Недостаточно доступных исходников.")

    min_new_required = max(0, int(min_new_required))
    new_rows = [row for row in existing if _is_unused_source_row(row)]
    if min_new_required and len(new_rows) < min_new_required:
        raise RuntimeError("Меньше новых исходников, чем запрошено.")

    random.shuffle(new_rows)
    selected: List[sqlite3.Row] = []
    needed_new = min(min_new_required, count)
    if needed_new:
        selected.extend(new_rows[:needed_new])
    remaining = [row for row in existing if row not in selected]
    random.shuffle(remaining)
    while len(selected) < count and remaining:
        selected.append(remaining.pop())

    if len(selected) < count:
        raise RuntimeError("Не удалось добрать нужное число источников.")

    random.shuffle(selected)
    return selected[:count]


def make_music_output_name(
    manifest_name: str,
    segments_count: int,
    algo_tag: str,
    orientation: str = "HOR",
    sources_count: int = 0,
    group_number: Optional[int] = None,
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    safe_name = sanitize_filename(manifest_name or "music")
    safe_name = safe_name.replace(" ", "_")
    orientation_tag = (orientation or "HOR").upper()
    segments_part = f"{segments_count}seg"
    sources_part = f"{max(1, sources_count)}fil"
    group_part = "??grp"
    if group_number is not None:
        try:
            group_idx = int(group_number)
            group_part = f"{group_idx}grp"
        except (TypeError, ValueError):
            pass
    base = (
        f"{timestamp}_{orientation_tag}_{group_part}_"
        f"{safe_name}_{segments_part}_{sources_part}_{algo_tag.upper()}"
    )
    candidate = OUTPUT_DIR / f"{base}.mp4"
    idx = 2
    while candidate.exists():
        candidate = OUTPUT_DIR / f"{base} ({idx}).mp4"
        idx += 1
    return candidate


def move_output_to_network_storage(local_path: Path, date_folder: Optional[str] = None) -> Tuple[Path, str]:
    resolved = local_path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Итоговый файл не найден: {resolved}")

    if not ENABLE_NETWORK_COPY:
        comment = (
            f"network_copy=disabled;original_path={resolved};"
            f"timestamp={datetime.now().isoformat(timespec='seconds')}"
        )
        return resolved, comment

    if not date_folder:
        date_folder = date.today().isoformat()
    target_dir = NETWORK_OUTPUT_ROOT / date_folder
    target_dir.mkdir(parents=True, exist_ok=True)

    destination = target_dir / resolved.name
    suffix = resolved.suffix
    stem = resolved.stem
    idx = 2
    while destination.exists():
        destination = target_dir / f"{stem} ({idx}){suffix}"
        idx += 1

    if resolved == destination:
        print(f"[OUTPUT] Файл уже находится в целевой папке: {destination}")
    else:
        print(f"[OUTPUT] Переношу файл {resolved} -> {destination}")
        shutil.move(str(resolved), str(destination))

    comment = (
        f"network_path={destination};date_folder={date_folder};"
        f"moved_from={resolved};moved_at={datetime.now().isoformat(timespec='seconds')}"
    )
    return destination, comment


def mux_audio_with_video(video_path: Path, audio_path: Path, out_path: Path) -> None:
    cmd = [
        FFMPEG_BIN,
        "-y",
        "-i", str(video_path),
        "-i", str(audio_path),
        "-c:v", "copy",
        "-c:a", "aac",
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-shortest",
        "-movflags", "+faststart",
        str(out_path),
    ]
    subprocess.check_call(cmd)


def make_music_synced_pmv(
    manifest_name: str,
    segments: List[MusicSegment],
    audio_path: Path,
    source_rows: List[sqlite3.Row],
    clip_algo_key: str,
    orientation: str = "HOR",
    group_number: Optional[int] = None,
) -> Tuple[Path, List[int], Tuple[str, Dict[str, Any]]]:
    if not segments:
        raise RuntimeError("В манифесте нет сегментов для нарезки.")
    if not audio_path.exists():
        raise FileNotFoundError(f"MP3 не найден: {audio_path}")

    source_paths: List[Path] = []
    source_ids: List[int] = []
    for row in source_rows:
        try:
            sid = int(row["id"])
            path = Path(row["video_path"])
        except Exception:
            continue
        if not path.exists():
            continue
        source_paths.append(path)
        source_ids.append(sid)
    if not source_paths:
        raise RuntimeError("Нет существующих файлов исходников.")

    resolved_key, algo_meta = resolve_clip_algorithm(clip_algo_key)
    total_segments = len(segments)
    sequence_sources = build_music_source_sequence(source_paths, resolved_key, total_segments)
    durations: Dict[Path, float] = {}
    for path in source_paths:
        durations[path] = float(ffprobe_duration_seconds(path))

    print(
        f"[MUSIC] Start sync render: project='{manifest_name}', segments={total_segments}, "
        f"sources={len(source_paths)}, algo={resolved_key}"
    )

    tmp_parent = pick_temp_dir(TEMP_DIRS, min_free_bytes=5 * 1024**3)
    tmp_root = Path(tempfile.mkdtemp(prefix="music_", dir=str(tmp_parent)))
    clip_meta: List[Dict[str, Any]] = []

    try:
        base_sequence = list(zip(segments, sequence_sources))
        for idx, (segment, primary_src) in enumerate(base_sequence, 1):
            seg_dur = max(0.3, segment.duration)
            candidate_sources = [primary_src] + [p for p in source_paths if p != primary_src]
            clip_created = False
            ext = ".ts" if USE_TS_CONCAT else ".mp4"
            clip_path = tmp_root / f"music_clip_{idx:04d}{ext}"

            for src_path in candidate_sources:
                video_dur = durations.get(src_path, 0.0)
                if video_dur <= 0.5:
                    continue

                max_start = max(0.0, video_dur - seg_dur - 0.5)
                if max_start <= 0:
                    actual_dur = min(seg_dur, max(0.3, video_dur - 0.2))
                    if actual_dur <= 0:
                        continue
                    start_pos = 0.0
                else:
                    actual_dur = seg_dur
                    start_pos = random.uniform(0.0, max_start)

                try:
                    extract_clip(src_path, start_pos, actual_dur, clip_path)
                    print(
                        f"[MUSIC] [{idx}/{total_segments}] {src_path.name} start={start_pos:.2f}s "
                        f"dur={actual_dur:.2f}s target={segment.duration:.2f}s intensity={segment.intensity:.2f}"
                    )
                    clip_meta.append(
                        {
                            "path": clip_path,
                            "duration": float(actual_dur),
                        }
                    )
                    clip_created = True
                    break
                except subprocess.CalledProcessError as err:
                    if clip_path.exists():
                        clip_path.unlink(missing_ok=True)
                    print(f"[MUSIC][WARN] extract failed on {src_path.name}: {err}")
                    try:
                        sid = db_get_source_id_by_path(src_path)
                        if sid is not None:
                            db_mark_source_problem(sid, f"music_extract_error")
                    except Exception:
                        pass
                    continue

            if not clip_created:
                print(f"[MUSIC][WARN] Segment {idx} skipped: не удалось вырезать клип ни из одного источника.")
                continue

        if not clip_meta:
            raise RuntimeError("Не удалось вырезать клипы по музыкальным сегментам.")
        if GLITCH_EFFECTS_PER_VIDEO > 0 or TRANSITION_EFFECTS_PER_VIDEO > 0:
            processed_clips, video_profile = apply_video_fx(clip_meta, tmp_root)
            uniform_clips = transcode_clips_to_profile(
                processed_clips, tmp_root, video_profile
            )
        else:
            uniform_clips = [Path(meta["path"]) for meta in clip_meta]
        raw_video_path = tmp_root / "music_raw.mp4"
        print(f"[MUSIC] Конкатенация {len(uniform_clips)} клипов...")
        concat_via_list(uniform_clips, raw_video_path)
        if raw_video_path.with_suffix(".mp4").exists():
            raw_video_path = raw_video_path.with_suffix(".mp4")

        algo_tag = algo_meta.get("short") or resolved_key
        final_path = make_music_output_name(
            manifest_name,
            len(segments),
            algo_tag,
            orientation=orientation,
            sources_count=len(source_paths),
            group_number=group_number,
        )
        print("[MUSIC] Накладываю аудиодорожку...")
        mux_audio_with_video(raw_video_path, audio_path, final_path)
        print(f"[MUSIC] Готово: {final_path}")
        return final_path, source_ids, (resolved_key, algo_meta)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def extract_clip(src: Path, start: float, dur: int, dst: Path) -> None:
    """Извлечение клипа без перекодирования. Для TS добавляем нужный bitstream-фильтр.
    Start может быть float. Добавлен -avoid_negative_ts make_zero для стабильных таймстемпов."""
    if USE_TS_CONCAT and dst.suffix.lower() != ".ts":
        dst = dst.with_suffix(".ts")

    if USE_TS_CONCAT:
        vinfo = detect_video_info(src)
        vcodec = (vinfo.get("codec_name") or "").lower()
        if vcodec.startswith("h264") or vcodec == "avc1":
            bsf_v = "h264_mp4toannexb"
        elif vcodec in ("hevc", "h265") or vcodec.startswith(("hev1", "hvc1")):
            bsf_v = "hevc_mp4toannexb"
        else:
            # неизвестный кодек — копируем сразу в MP4
            cmd = [
                FFMPEG_BIN,
                "-v", "error",
                "-y",
                "-ss", str(start),
                "-t", str(dur),
                "-i", str(src),
                "-c", "copy",
                "-avoid_negative_ts", "make_zero",
                "-movflags", "+faststart",
                str(dst.with_suffix(".mp4")),
            ]
            subprocess.check_call(cmd)
            return

        cmd = [
            FFMPEG_BIN,
            "-v", "error",
            "-y",
            "-ss", str(start),
            "-t", str(dur),
            "-i", str(src),
            "-c", "copy",
            "-bsf:v", bsf_v,
            "-avoid_negative_ts", "make_zero",
            "-f", "mpegts",
            str(dst),
        ]
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            fallback_cmd = [
                FFMPEG_BIN,
                "-v", "error",
                "-y",
                "-ss", str(start),
                "-t", str(dur),
                "-i", str(src),
                "-c", "copy",
                "-avoid_negative_ts", "make_zero",
                "-f", "mpegts",
                str(dst),
            ]
            try:
                subprocess.check_call(fallback_cmd)
            except subprocess.CalledProcessError:
                raise
    else:
        cmd = [
            FFMPEG_BIN,
            "-v", "error",
            "-y",
            "-ss", str(start),
            "-t", str(dur),
            "-i", str(src),
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            str(dst),
        ]
        subprocess.check_call(cmd)


def concat_via_list(clips_paths: List[Path], out_path: Path) -> None:
    """
    Склеивает клипы в один файл.

    Фишки:
    - Больше НЕ используем длинный аргумент вида concat:...|...|...
      (на Windows он легко ломается).
    - Делаем текстовый список (concat demuxer), пути переводим в вид с '/'.
    - Если ожидаемая .ts не найдена, ищем .mp4 (фолбек из extract_clip).
    """
    # Определяем финальное имя вывода
    if out_path.suffix.lower() != ".mp4":
        out_mp4 = out_path.with_suffix(".mp4")
    else:
        out_mp4 = out_path

    real_paths: List[Path] = []

    for p in clips_paths:
        # базовый кандидат — как нам его передали
        cand = p

        if not cand.exists():
            # если такого нет, пробуем .ts / .mp4 рядом
            alt_ts = p.with_suffix(".ts")
            alt_mp4 = p.with_suffix(".mp4")

            if alt_ts.exists():
                cand = alt_ts
            elif alt_mp4.exists():
                cand = alt_mp4
            else:
                raise FileNotFoundError(
                    f"Не найден клип для конкатенации: {p} "
                    f"(пробовали {alt_ts} и {alt_mp4})"
                )

        real_paths.append(cand)

    if not real_paths:
        raise RuntimeError("Список клипов для конкатенации пуст.")

    # Файл-список для concat demuxer
    list_file = out_mp4.parent / f"{out_mp4.stem}_concat_list.txt"

    with open(list_file, "w", encoding="utf-8") as f:
        for c in real_paths:
            # ffmpeg на Windows нормально понимает прямые слэши
            p_str = str(c.resolve()).replace("\\", "/")
            f.write(f"file '{p_str}'\n")

    # Собираем команду ffmpeg
    cmd = [
        FFMPEG_BIN,
        "-v", "error",
        "-y",
        "-fflags", "+genpts",
        "-max_interleave_delta", "0",
        "-f", "concat",
        "-safe", "0",
        "-i", str(list_file),
        "-c", "copy",
        "-movflags", "+faststart",
    ]

    # Если мы работаем с TS-клипами (USE_TS_CONCAT=True),
    # оставляем битстрим-фильтр для AAC → MP4.
    if USE_TS_CONCAT:
        cmd.extend(["-bsf:a", "aac_adtstoasc"])

    cmd.append(str(out_mp4))

    try:
        subprocess.check_call(cmd)
    finally:
        try:
            list_file.unlink()
        except OSError:
            pass


def pick_evenly_spaced_indices(total: int, count: int) -> List[int]:
    if count <= 0 or total <= 0:
        return []
    result: List[int] = []
    used: set[int] = set()
    step = total / (count + 1)
    for i in range(count):
        idx = int(round(step * (i + 1)))
        idx = max(0, min(total - 1, idx))
        while idx in used and idx < total - 1:
            idx += 1
        while idx in used and idx > 0:
            idx -= 1
        if idx not in used:
            used.add(idx)
            result.append(idx)
    return sorted(result)


def pick_positions_from_pool(pool: List[int], count: int) -> List[int]:
    if count <= 0 or not pool:
        return []
    sorted_pool = sorted(pool)
    positions = pick_evenly_spaced_indices(len(sorted_pool), count)
    return [sorted_pool[i] for i in positions if i < len(sorted_pool)]


def determine_fx_encoding_profile(sample_path: Path) -> Dict[str, str]:
    info = detect_video_info(sample_path)
    codec = (info.get("codec_name") or "").lower()
    pix_fmt = info.get("pix_fmt") or ""
    if not pix_fmt or not re.fullmatch(r"[A-Za-z0-9_]+", pix_fmt):
        pix_fmt = "yuv420p"
    fps_val = info.get("fps")
    try:
        fps = float(fps_val) if fps_val else 30.0
    except (TypeError, ValueError):
        fps = 30.0
    fps = fps if fps > 0 else 30.0
    if codec in {"hevc", "h265", "hvc1", "hev1"}:
        video_encoder = "libx265"
    else:
        video_encoder = "libx264"
    return {"video_encoder": video_encoder, "pix_fmt": pix_fmt, "fps": fps}


def create_glitch_clip(
    clip_meta: Dict[str, Any],
    tmp_root: Path,
    profile: Dict[str, str],
    duration: float = FX_GLITCH_DURATION,
) -> Optional[Path]:
    src_path = Path(clip_meta["path"])
    clip_dur = float(clip_meta.get("duration") or duration)
    duration = float(max(0.15, min(duration, clip_dur)))
    start = max(clip_dur - duration, 0.0)
    filters = [
        "rgbashift=rh=5:rv=-5:gh=-4:gv=4:bh=3:bv=-3,noise=alls=20:allf=t+u,eq=contrast=1.2:saturation=1.4",
        "tblend=all_mode='xor',hue=s=0,format=yuv420p",
        "noise=alls=30:allf=t+u,edgedetect=mode=colormix:high=0.2:low=0.05",
    ]
    vf = random.choice(filters)
    out_path = tmp_root / f"{src_path.stem}_glitch_{random.randrange(1_000_000)}.mp4"
    filter_complex = (
        f"[0:v]trim=start={start:.3f}:end={clip_dur:.3f},setpts=PTS-STARTPTS,"
        f"{vf},fps={profile.get('fps') or 30.0},format={profile.get('pix_fmt') or 'yuv420p'}[vout]"
    )
    cmd = [
        FFMPEG_BIN,
        "-v",
        "error",
        "-y",
        "-i",
        str(src_path),
        "-filter_complex",
        filter_complex,
        "-c:v",
        profile.get("video_encoder") or "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-movflags",
        "+faststart",
        "-map",
        "[vout]",
        "-an",
        str(out_path),
    ]
    try:
        subprocess.check_call(cmd)
        return out_path
    except subprocess.CalledProcessError:
        if out_path.exists():
            out_path.unlink(missing_ok=True)
        return None


def create_transition_clip(
    prev_meta: Dict[str, Any],
    next_meta: Dict[str, Any],
    tmp_root: Path,
    profile: Dict[str, str],
    duration: float = FX_TRANSITION_DURATION,
) -> Optional[Path]:
    prev_path = Path(prev_meta["path"])
    next_path = Path(next_meta["path"])
    prev_dur = float(prev_meta.get("duration") or duration)
    next_dur = float(next_meta.get("duration") or duration)
    duration = float(min(duration, prev_dur, next_dur, 1.0))
    if duration < 0.15:
        return None
    start_prev = max(prev_dur - duration, 0.0)
    transition = random.choice(XFADE_TRANSITIONS) if XFADE_TRANSITIONS else "fade"
    out_path = tmp_root / f"transition_{prev_path.stem}_{next_path.stem}_{random.randrange(1_000_000)}.mp4"
    fps = profile.get("fps") or 30.0
    video_fmt = profile.get("pix_fmt") or "yuv420p"
    filter_complex = (
        f"[0:v]trim=start={start_prev:.3f}:end={prev_dur:.3f},setpts=PTS-STARTPTS,"
        f"fps={fps},format={video_fmt}[v0];"
        f"[1:v]trim=start=0:end={duration:.3f},setpts=PTS-STARTPTS,fps={fps},format={video_fmt}[v1];"
        f"[v0][v1]xfade=transition={transition}:duration={duration:.3f}:offset=0[vout]"
    )
    cmd = [
        FFMPEG_BIN,
        "-v",
        "error",
        "-y",
        "-i",
        str(prev_path),
        "-i",
        str(next_path),
        "-filter_complex",
        filter_complex,
        "-c:v",
        profile.get("video_encoder") or "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-movflags",
        "+faststart",
        "-map",
        "[vout]",
        "-an",
        str(out_path),
    ]
    try:
        subprocess.check_call(cmd)
        return out_path
    except subprocess.CalledProcessError:
        if out_path.exists():
            out_path.unlink(missing_ok=True)
        return None


def apply_video_fx(
    clips_meta: List[Dict[str, Any]], tmp_root: Path
) -> Tuple[List[Path], Dict[str, str]]:
    if not clips_meta:
        return [], {"video_encoder": "libx264", "pix_fmt": "yuv420p", "fps": 30.0}
    total_seams = max(0, len(clips_meta) - 1)
    profile = determine_fx_encoding_profile(Path(clips_meta[0]["path"]))

    transition_positions = pick_evenly_spaced_indices(
        total_seams, min(TRANSITION_EFFECTS_PER_VIDEO, total_seams)
    )
    remaining_seams = [i for i in range(total_seams) if i not in transition_positions]
    glitch_positions = pick_positions_from_pool(
        remaining_seams, min(GLITCH_EFFECTS_PER_VIDEO, len(remaining_seams))
    )

    result_paths: List[Path] = []
    for idx, meta in enumerate(clips_meta):
        result_paths.append(Path(meta["path"]))
        if idx >= total_seams:
            continue
        seam_index = idx
        if seam_index in transition_positions:
            clip = create_transition_clip(
                meta, clips_meta[idx + 1], tmp_root, profile, FX_TRANSITION_DURATION
            )
            if clip:
                result_paths.append(clip)
        elif seam_index in glitch_positions:
            clip = create_glitch_clip(meta, tmp_root, profile, FX_GLITCH_DURATION)
            if clip:
                result_paths.append(clip)
    return result_paths, profile


def transcode_clips_to_profile(
    clips: List[Path],
    tmp_root: Path,
    profile: Dict[str, Any],
) -> List[Path]:
    uniform_paths: List[Path] = []
    fps = float(profile.get("fps") or 30.0)
    pix_fmt = profile.get("pix_fmt") or "yuv420p"
    video_encoder = profile.get("video_encoder") or "libx264"

    for idx, clip in enumerate(clips, 1):
        out_path = tmp_root / f"uniform_{idx:04d}.mp4"
        cmd = [
            FFMPEG_BIN,
            "-v",
            "error",
            "-y",
            "-i",
            str(clip),
            "-vf",
            f"fps={fps},format={pix_fmt}",
            "-c:v",
            video_encoder,
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-an",
            "-movflags",
            "+faststart",
            str(out_path),
        ]
        subprocess.check_call(cmd)
        uniform_paths.append(out_path)
    return uniform_paths


def make_output_name(
    selected_files: List[Path],
    target_seconds: int,
    big_parts: int,
    small_per_big: int,
    run_seed: Optional[int] = None,
    algo_tag: Optional[str] = None,
) -> Path:
    """
    Имя вида:
    YYYYMMDD - random_title - BUILD_NAME - <N>files - <M>min - <big_parts>big - <small_per_big>small - seed<seed>.mp4

    + ГАРАНТИЯ: если файл с таким именем уже есть, добавляем " (2)", " (3)" и т.д.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    random_title = db_get_random_name()

    # количество исходников
    files_count = len(selected_files)

    # Преобразуем целевые секунды в минуты (минимум 1)
    minutes = max(1, int(round(target_seconds / 60.0)))

    algo_part = f" - {algo_tag}" if algo_tag else ""
    seed_part = f" - seed{run_seed}" if run_seed is not None else ""

    base_stem = (
        f"{today} - {random_title} - {BUILD_NAME} - "
        f"{files_count}files - {minutes}min - {big_parts}big - {small_per_big}small{algo_part}{seed_part}"
    )

    candidate = OUTPUT_DIR / f"{base_stem}.mp4"
    if not candidate.exists():
        return candidate

    idx = 2
    while True:
        candidate = OUTPUT_DIR / f"{base_stem} ({idx}).mp4"
        if not candidate.exists():
            return candidate
        idx += 1



def estimate_required_bytes(total_seconds: int, assumed_mbps: int = 15) -> int:
    """Грубая оценка требуемого места (байты) исходя из среднего битрейта (~15 Мбит/с по умолчанию)."""
    return int(total_seconds * (assumed_mbps * 1_000_000 / 8))


def make_pmv_from_files(
    selected_paths: List[Path],
    target_seconds: int,
    big_parts: int,
    small_per_big: int,
    clip_algo_key: Optional[str] = None,
) -> Path:
    if not selected_paths:
        raise ValueError("Нет файлов для PMV")

    # Сид генерации — меняется каждый запуск; добавляем в имя файла
    global RANDOM_SEED
    run_seed = int(time.time())
    random.seed(run_seed)
    RANDOM_SEED = run_seed

    durations: Dict[Path, int] = {}
    valid_paths: List[Path] = []
    for p in selected_paths:
        try:
            if not p.exists():
                # помечаем как проблемный и пропускаем
                try:
                    sid = db_get_source_id_by_path(p)
                    if sid is not None:
                        db_mark_source_problem(sid, "missing_file")
                except Exception:
                    pass
                continue
            d = int(ffprobe_duration_seconds(p))
            if d > 0:
                durations[p] = d
                valid_paths.append(p)
            else:
                try:
                    sid = db_get_source_id_by_path(p)
                    if sid is not None:
                        db_mark_source_problem(sid, "zero_duration")
                except Exception:
                    pass
        except Exception:
            try:
                sid = db_get_source_id_by_path(p)
                if sid is not None:
                    db_mark_source_problem(sid, "probe_error")
            except Exception:
                pass
            continue

    # заменяем список на только валидные пути
    selected_paths = valid_paths

    sum_dur = sum(durations.values())
    effective_target = min(target_seconds, sum_dur)
    if effective_target <= 0:
        raise RuntimeError("Целевая длина нулевая или у файлов нулевая длительность")

    # Проверка места: оценим размер финала и резерв под временные клипы
    estimated_out = estimate_required_bytes(effective_target)
    if estimated_out > MAX_OUTPUT_BYTES:
        raise RuntimeError(
            f"Оценочный размер итогового файла превышает лимит 100 ГБ (≈{estimated_out/1024**3:.1f} ГБ)."
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    free_out = shutil.disk_usage(str(OUTPUT_DIR)).free
    if free_out < min(estimated_out, MAX_OUTPUT_BYTES):
        raise RuntimeError(
            f"Недостаточно места в папке вывода: нужно ≈{estimated_out/1024**3:.1f} ГБ, доступно ≈{free_out/1024**3:.1f} ГБ."
        )

    per_file_alloc = allocate_equalish(effective_target, selected_paths, durations)
    resolved_algo_key, algo_meta = resolve_clip_algorithm(clip_algo_key)

    per_file_clips: Dict[Path, List[Tuple[int, int]]] = {}
    for f in selected_paths:
        alloc = per_file_alloc.get(f, 0)
        if alloc <= 0:
            continue
        clips = plan_for_file(f, alloc, big_parts, small_per_big, algo_key=resolved_algo_key)
        per_file_clips[f] = clips

    sequence = algo_meta["builder"](per_file_clips)
    if not sequence:
        raise RuntimeError("Не удалось спланировать последовательность клипов")
    print(f"[CLIP-SEQ] Алгоритм: {algo_meta['title']} ({resolved_algo_key})")

    tmp_parent = pick_temp_dir(TEMP_DIRS, min_free_bytes=max(5 * 1024**3, estimated_out * 2))
    tmp_root = Path(tempfile.mkdtemp(prefix="pmv_", dir=str(tmp_parent)))
    clips_paths: List[Path] = []

    # Кэш ключевых кадров для привязки стартов клипов
    keyframe_cache: Dict[Path, Dict[str, Any]] = {}
    KEYFRAME_INITIAL_WINDOW = 10.0
    KEYFRAME_MAX_WINDOW = 300.0
    KEYFRAME_LOOKAHEAD = 0.25

    def _cache_entry(src: Path) -> Dict[str, Any]:
        return keyframe_cache.setdefault(src, {"times": [], "full_scan": False})

    def _insert_keyframes(entry: Dict[str, Any], values: List[float]) -> None:
        if not values:
            return
        times: List[float] = entry["times"]
        for val in values:
            rounded = round(val, 6)
            idx = bisect_left(times, rounded)
            if idx >= len(times) or abs(times[idx] - rounded) > 1e-6:
                times.insert(idx, rounded)

    def _find_prev(entry: Dict[str, Any], t: float) -> Optional[float]:
        times: List[float] = entry["times"]
        if not times:
            return None
        idx = bisect_right(times, t)
        if idx:
            return times[idx - 1]
        return times[0]

    def _probe_keyframes(src: Path, start: Optional[float], end: Optional[float]) -> List[float]:
        cmd = [
            FFPROBE_BIN,
            "-v", "error",
            "-select_streams", "v:0",
            "-skip_frame", "nokey",
            "-show_entries", "frame=pkt_pts_time",
            "-of", "default=nokey=1:noprint_wrappers=1",
        ]
        if start is not None or end is not None:
            interval_start = 0.0 if start is None else max(0.0, start)
            interval_end = interval_start + 0.05
            if end is not None:
                interval_end = max(interval_end, end)
            cmd.extend(["-read_intervals", f"{interval_start:.3f}%{interval_end:.3f}"])
        cmd.append(str(src))
        try:
            out = subprocess.check_output(cmd, text=True)
        except Exception:
            return []
        found: List[float] = []
        for line in out.strip().splitlines():
            try:
                found.append(float(line.strip()))
            except Exception:
                continue
        return sorted(found)

    def get_prev_keyframe_time(src: Path, t: float) -> float:
        entry = _cache_entry(src)
        cached = _find_prev(entry, t)
        if cached is not None:
            return cached

        window = KEYFRAME_INITIAL_WINDOW
        while window <= KEYFRAME_MAX_WINDOW:
            window_start = max(0.0, t - window)
            window_end = max(t + KEYFRAME_LOOKAHEAD, window_start + 0.05)
            new_vals = _probe_keyframes(src, window_start, window_end)
            _insert_keyframes(entry, new_vals)
            cached = _find_prev(entry, t)
            if cached is not None or window_start <= 0:
                break
            window *= 2

        if cached is not None:
            return cached

        if not entry.get("full_scan"):
            full_vals = _probe_keyframes(src, None, None)
            _insert_keyframes(entry, full_vals)
            entry["full_scan"] = True
            cached = _find_prev(entry, t)
            if cached is not None:
                return cached

        return t

    try:
        total = len(sequence)
        for idx, (src, start, dur) in enumerate(sequence, 1):
            ext = ".ts" if USE_TS_CONCAT else ".mp4"
            out = tmp_root / f"clip_{idx:03d}{ext}"
            start_f = float(start)
            if SNAP_TO_KEYFRAMES:
                start_f = get_prev_keyframe_time(src, start_f)
            print(f"[{idx}/{total}] {src.name}: start={start_f:.3f}, dur={dur}")
            t0 = time.time()
            try:
                extract_clip(src, start_f, dur, out)
                print(f"  -> clip ok ({time.time() - t0:.1f}s)")
                clips_paths.append(out)
            except Exception as e:
                print(f"[ERROR] Ошибка при извлечении клипа из {src}: {e}")
                try:
                    sid = db_get_source_id_by_path(src)
                    if sid is not None:
                        db_mark_source_problem(sid, f"extract_error={e}")
                except Exception:
                    pass
                continue

        if not clips_paths:
            raise RuntimeError("Не удалось извлечь ни одного клипа — все попытки закончились ошибками.")

        out_path = make_output_name(
            selected_files=selected_paths,
            target_seconds=target_seconds,
            big_parts=big_parts,
            small_per_big=small_per_big,
            run_seed=run_seed,
            algo_tag=algo_meta.get("short"),
        )
        concat_via_list(clips_paths, out_path)
        if out_path.with_suffix(".mp4").exists():
            out_path = out_path.with_suffix(".mp4")
        return out_path
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def autocreate_make_one_pairs(
    target_seconds: int,
    max_sources: int,
    min_sources: int,
    excluded_ids: set[int],
    big_parts: int = 5,
    small_per_big: int = 5,
    strategy: str = "max_group",
    clip_algo_key: Optional[str] = None,
) -> Optional[Tuple[Path, List[int], Tuple[str, str]]]:
    """
    Создаёт одно PMV из ПАР: 1 новый + 1 старый исходник, попеременно.
    Группы по (codec,res). Берём только ключи, где есть и новые, и старые.
    """
    new_groups = db_get_unused_sources_grouped()
    old_groups = db_get_used_sources_grouped()
    if not new_groups or not old_groups:
        return None

    # ключи пересечения
    common_keys = []
    for k in new_groups.keys():
        if k in old_groups:
            # фильтруем excluded
            new_rows = [r for r in new_groups[k] if int(r["id"]) not in excluded_ids]
            old_rows = [r for r in old_groups[k] if int(r["id"]) not in excluded_ids]
            if new_rows and old_rows:
                common_keys.append((k, new_rows, old_rows))
    if not common_keys:
        return None

    # выбрать ключ по стратегии
    if strategy == "weighted_random":
        total = sum(min(len(nr), len(orows)) for _, nr, orows in common_keys)
        rnd = random.randint(1, max(1, total))
        acc = 0
        key, new_rows, old_rows = common_keys[0]
        for k, nr, orows in common_keys:
            acc += min(len(nr), len(orows))
            if rnd <= acc:
                key, new_rows, old_rows = k, nr, orows
                break
    elif strategy == "random":
        key, new_rows, old_rows = random.choice(common_keys)
    else:  # max_group по “минимуму пары”
        key, new_rows, old_rows = max(common_keys, key=lambda t: min(len(t[1]), len(t[2])))

    # сколько пар можем взять
    max_pairs = min(len(new_rows), len(old_rows), max_sources // 2)
    need_pairs = max(1, (min_sources + 1) // 2)
    if max_pairs < need_pairs:
        return None

    # диверсификация по папкам для новых и старых отдельно
    def pick_diverse(rows: List[sqlite3.Row], count: int) -> List[sqlite3.Row]:
        dir_map: Dict[Path, List[sqlite3.Row]] = {}
        for r in rows:
            d = Path(r["video_path"]).resolve().parent
            dir_map.setdefault(d, []).append(r)
        for lst in dir_map.values():
            random.shuffle(lst)
        chosen: List[sqlite3.Row] = []
        taken: Dict[Path, int] = {d: 0 for d in dir_map}
        progressed = True
        while len(chosen) < count and progressed:
            progressed = False
            for d, lst in list(dir_map.items()):
                if taken[d] >= PER_DIR_MAX_FIRST_PASS:
                    continue
                if lst:
                    chosen.append(lst.pop())
                    taken[d] += 1
                    progressed = True
                    if len(chosen) >= count:
                        break
        if len(chosen) < count:
            leftovers: List[sqlite3.Row] = []
            for lst in dir_map.values():
                leftovers.extend(lst)
            random.shuffle(leftovers)
            for r in leftovers:
                chosen.append(r)
                if len(chosen) >= count:
                    break
        return chosen

    pick_n = max_pairs
    new_pick = pick_diverse(new_rows, pick_n)
    old_pick = pick_diverse(old_rows, pick_n)

    # чередуем: новый, старый
    chosen_rows: List[sqlite3.Row] = []
    for nr, orow in zip(new_pick, old_pick):
        chosen_rows.append(nr)
        chosen_rows.append(orow)
    # урезаем до лимита
    chosen_rows = chosen_rows[: max_sources]

    paths = [Path(r["video_path"]) for r in chosen_rows]
    source_ids = [int(r["id"]) for r in chosen_rows]

    out_path = make_pmv_from_files(paths, target_seconds, big_parts, small_per_big, clip_algo_key=clip_algo_key)
    out_path, move_comment = move_output_to_network_storage(out_path)
    pmv_tag = Path(out_path).name

    db_insert_compilation(out_path, source_ids, comments=move_comment)
    db_update_sources_pmv_list(source_ids, pmv_tag)
    excluded_ids.update(source_ids)

    return out_path, source_ids, key


def autocreate_pmv_batch(
    total_videos: int,
    minutes_each: int,
    max_sources: int,
    min_sources: int,
) -> str:
    """
    Делает до total_videos PMV:
    - сначала из НОВЫХ (неучаствовавших), потом из СТАРЫХ,
    - при этом:
        new_count  = ceil(total_videos / 2)
        old_count  = floor(total_videos / 2)
    Возвращает текстовый отчёт.
    """
    target_seconds = max(60, minutes_each * 60)

    # сколько нужно попытаться сделать
    new_target = (total_videos + 1) // 2   # округление вверх
    old_target = total_videos // 2         # округление вниз

    created_new = 0
    created_old = 0

    excluded_new: set[int] = set()
    excluded_old: set[int] = set()

    log_lines: List[str] = []
    algo_picker = ClipAlgorithmPicker(total_videos)

    # -------- Новые (как pmvnew) --------
    for i in range(new_target):
        algo_key = algo_picker.current()
        try:
            res = autocreate_make_one_pairs(
                target_seconds=target_seconds,
                max_sources=max_sources,
                min_sources=min_sources,
                excluded_ids=excluded_new,
                strategy=CURRENT_STRATEGY,
                clip_algo_key=algo_key,
            )
        except Exception as e:
            log_lines.append(f"❌ Ошибка при создании нового PMV #{i+1}: {e}")
            break

        if not res:
            # ФОЛБЕК: пробуем собрать только из новых, если пар не набралось
            try:
                res = fallback_new_only_make_one(
                    target_seconds=target_seconds,
                    max_sources=max_sources,
                    min_sources=min_sources,
                    excluded_ids=excluded_new,
                    strategy=CURRENT_STRATEGY,
                    clip_algo_key=algo_key,
                )
            except Exception as e:
                log_lines.append(f"❌ Ошибка (fallback new-only) при создании нового PMV #{i+1}: {e}")
                break

            if not res:
                if i == 0:
                    log_lines.append("⚠️ Не удалось создать ни одного нового PMV — мало неиспользованных исходников.")
                else:
                    log_lines.append("⚠️ Исчерпан пул неиспользованных исходников для новых PMV.")
                break

        out_path, src_ids, (codec, reso) = res
        algo_picker.commit()
        _, algo_meta = resolve_clip_algorithm(algo_key)
        created_new += 1
        log_lines.append(
            f"✅ Новое PMV #{created_new}: {out_path.name} "
            f"(группа {codec} {reso}, исходников: {len(src_ids)}, алгоритм: {algo_meta['short']})"
        )

    # -------- Старые (уже участвовавшие) --------
    for i in range(old_target):
        algo_key = algo_picker.current()
        try:
            res = autocreate_make_one_pairs(
                target_seconds=target_seconds,
                max_sources=max_sources,
                min_sources=min_sources,
                excluded_ids=excluded_old,
                strategy=CURRENT_STRATEGY,
                clip_algo_key=algo_key,
            )
        except Exception as e:
            log_lines.append(f"❌ Ошибка при создании PMV из старых исходников #{i+1}: {e}")
            break

        if not res:
            if i == 0:
                log_lines.append("⚠️ Не удалось создать ни одного PMV из старых исходников.")
            else:
                log_lines.append("⚠️ Исчерпан пул старых исходников для PMV.")
            break

        out_path, src_ids, (codec, reso) = res
        algo_picker.commit()
        _, algo_meta = resolve_clip_algorithm(algo_key)
        created_old += 1
        log_lines.append(
            f"✅ PMV из старых исходников #{created_old}: {out_path.name} "
            f"(группа {codec} {reso}, исходников: {len(src_ids)}, алгоритм: {algo_meta['short']})"
        )

    created_total = created_new + created_old

    header = (
        f"🏁 Автосоздание завершено.\n"
        f"Запрошено видео: {total_videos}\n"
        f"Фактически создано: {created_total} "
        f"(новых: {created_new}, из старых: {created_old})."
    )

    if not log_lines:
        return header
    return header + "\n\n" + "\n".join(log_lines)


# =========================
# ТЕЛЕГРАМ-БОТ
# =========================

user_sessions: Dict[int, Dict] = {}


def check_access(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else 0
    return uid == ALLOWED_USER_ID


async def unauthorized(update: Update) -> None:
    await update.effective_chat.send_message("⛔ У вас нет доступа к этому боту.")



async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    text = (
        f"👋 Привет! PMV-бот {BUILD_NAME}.\n\n"
        "Доступные команды:\n"
        "/addfolder <путь> – добавить папку загрузки\n"
        "/folders – показать папки загрузки\n"
        "/scan – просканировать папки и обновить список исходников\n"
        "/scanignore – добавить подпапку в список исключений сканирования\n\n"
        "/pmvnew – создать новое PMV из ещё НЕ использованных исходников\n"
        "/pmvold – создать PMV из ЛЮБЫХ исходников (включая уже использованные)\n"
        "/autocreate – автоматически создать несколько PMV по заданным параметрам\n"
        "/newcompmusic – собрать PMV под музыку из music_projects\n"
        "/musicprep – проанализировать трек из папки Music и создать проект\n"
        "/videofx – настроить количество глитчей и переходов между клипами\n"
        "/musicprepcheck – создать MP3 со щелчками для проверки сегментов\n"
        "/ratepmv – оценить готовые PMV и при желании отдельные видео внутри\n"
        "/rategrp – отметить исходники цветами внутри выбранной группы\n"
        "/compmv — комментарий к PMV\n"
        "/comvid — комментарий к исходнику\n"
        "/lookcom — просмотр всех комментариев с названиями.\n"
        "/badfiles — список проблемных исходников\n"
        "/strategy [имя] — посмотреть/установить стратегию выбора групп (max_group, weighted_random, random)\n"
        "/move2oculus — скопировать недостающие файлы из y:\\output на Oculus через ADB\n\n"
        "Если adb не найден, поставь Platform Tools в C:\\platform-tools и выполни в PowerShell:\n"
        "$env:Path += ';C:\\platform-tools'\n"
        "setx Path $env:Path\n"
    )
    await update.message.reply_text(text, reply_markup=build_main_reply_keyboard())

async def cmd_lookcom(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Показать комментарии к компиляциям и исходникам.
    """
    if not check_access(update):
        return await unauthorized(update)

    comp_rows = db_get_compilations_with_comments()
    src_rows = db_get_sources_with_comments()

    if not comp_rows and not src_rows:
        return await update.message.reply_text("Пока нет ни одного комментария.")

    parts: List[str] = []

    if comp_rows:
        parts.append("📀 Комментарии к компиляциям:")
        for r in comp_rows:
            name = Path(r["video_path"]).name
            parts.append(f"- {name} (дата: {r['pmv_date']}): {r['comments']}")

    if src_rows:
        if parts:
            parts.append("")  # пустая строка-разделитель
        parts.append("🎞 Комментарии к исходникам:")
        for r in src_rows:
            parts.append(f"- {r['video_name']} (id={r['id']}): {r['comments']}")

    text = "\n".join(parts)
    if len(text) <= 4000:
        await update.message.reply_text(text)
        return

    chunk_size = 3800
    for i in range(0, len(text), chunk_size):
        await update.message.reply_text(text[i:i + chunk_size])



async def cmd_compmv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Добавить комментарий к компиляции (PMV).
    /compmv -> список PMV -> номер -> текст комментария.
    """
    if not check_access(update):
        return await unauthorized(update)

    rows = db_get_all_compilations()
    if not rows:
        return await update.message.reply_text("Пока нет ни одной компиляции.")

    lines = ["🎬 Компиляции:"]
    for idx, r in enumerate(rows, 1):
        name = Path(r["video_path"]).name
        date_str = r["pmv_date"]
        lines.append(f"{idx}. {name} (дата: {date_str}, id={r['id']})")

    lines.append("")
    lines.append("Пришлите номер PMV, к которому хотите добавить комментарий (например: 2).")

    user_sessions[update.effective_user.id] = {
        "state": "compmv_choose",
        "pmv_rows": rows,
    }

    await update.message.reply_text("\n".join(lines))


async def cmd_addfolder(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    args = context.args
    if not args:
        return await update.message.reply_text("Использование: /addfolder C:\\path\\to\\folder")

    folder = " ".join(args).strip().strip('"')
    p = Path(folder)
    if not p.exists() or not p.is_dir():
        return await update.message.reply_text(f"Папка не найдена: {p}")

    db_add_upload_folder(str(p))
    await update.message.reply_text(f"✅ Папка добавлена: {p}")


async def cmd_folders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    rows = db_get_upload_folders(include_ignored=True)
    if not rows:
        return await update.message.reply_text("Папок загрузки пока нет. Добавьте через /addfolder")

    active = [r for r in rows if not r["ignored"]]
    ignored = [r for r in rows if r["ignored"]]
    lines = ["📂 Папки загрузки:"]
    if not active:
        lines.append("Нет активных папок для сканирования.")
    else:
        for r in active:
            lines.append(f"{r['id']}. {r['folder_path']} (с {r['date_added']})")
    if ignored:
        lines.append("")
        lines.append("🚫 Игнорируемые подпапки (/scanignore):")
        for r in ignored:
            lines.append(f"{r['id']}. {r['folder_path']} (с {r['date_added']})")
    await update.message.reply_text("\n".join(lines))


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    rows = db_get_upload_folders()
    if not rows:
        return await update.message.reply_text("Нет активных папок. Добавьте её через /addfolder")
    ignored_rows = db_get_scan_ignored_folders()

    await update.message.reply_text("Начинаю пересканировать каталоги... это может занять время.")

    env = ScanEnvironment(
        default_exts=DEFAULT_EXTS,
        normalize_path_str=_normalize_path_str,
        normalize_path_prefix=_normalize_path_prefix,
        is_path_under_prefixes=_is_path_under_prefixes,
        combine_comments=combine_comments,
        merge_pmv_lists=merge_pmv_lists,
        video_info_sort=video_info_sort,
        db_get_sources_full=db_get_sources_full,
        db_update_source_fields=db_update_source_fields,
        db_insert_source=db_insert_source,
        db_delete_sources_by_ids=db_delete_sources_by_ids,
        db_path=DB_PATH,
        backup_dir=SCRIPT_DIR / "old",
    )

    lines, _stats = run_scan(rows, ignored_rows, env)
    symlink_notes = sync_nas_symlinks()
    if symlink_notes:
        lines.append("")
        lines.extend(symlink_notes)
    await update.message.reply_text("\n".join(lines))

async def cmd_scanignore(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    user_id = update.effective_user.id
    user_sessions[user_id] = {
        "state": "scanignore_wait_path",
    }
    lines = [
        "Пришлите полный путь папки, которую нужно исключить из /scan.",
        "Можно указывать подпапки уже добавленных каталогов (например X:\\tor\\tmp).",
        "Чтобы отменить, отправьте /start или другую команду.",
    ]
    await update.message.reply_text("\n".join(lines))

async def cmd_comvid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Добавить комментарий к исходнику.
    /comvid -> список исходников -> номер -> текст комментария.
    """
    if not check_access(update):
        return await unauthorized(update)

    rows = db_get_all_sources()
    if not rows:
        return await update.message.reply_text("В базе нет исходников.")

    lines = ["🎥 Исходники:"]
    # чтобы список не был совсем безумным, можно ограничить, но по ТЗ выводим все
    for idx, r in enumerate(rows, 1):
        name = r["video_name"]
        lines.append(f"{idx}. {name} (id={r['id']})")

    lines.append("")
    lines.append("Пришлите номер видео, к которому хотите добавить комментарий (например: 5).")

    user_sessions[update.effective_user.id] = {
        "state": "comvid_choose",
        "src_rows": rows,
    }

    await update.message.reply_text("\n".join(lines))

async def cmd_autocreate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Автоматическое создание нескольких PMV подряд.

    Шаги:
    1) Спросить, сколько видео создать.
    2) Спросить длину каждого (в минутах).
    3) Спросить максимум исходников на одно PMV.
    4) Спросить минимум исходников на одно PMV.
    5) Затем автоматически:
       - сначала делает нужное число PMV из НОВЫХ исходников (как pmvnew),
       - затем из СТАРЫХ (уже участвовавших в компиляциях),
       соблюдая пропорцию: половина из новых, половина из старых
       (при нечётном количестве — +1 к новым).
    """
    if not check_access(update):
        return await unauthorized(update)

    user_id = update.effective_user.id

    user_sessions[user_id] = {
        "state": "autocreate_ask_count",
    }

    await update.message.reply_text(
        "🚀 Автоматическое создание PMV.\n\n"
        "Сколько видео создать? Введите целое число (например: 4)."
    )

def db_get_used_sources_grouped() -> Dict[Tuple[str, str], List[sqlite3.Row]]:
    """
    Берёт ТОЛЬКО уже участвовавшие в компиляциях исходники (pmv_list не пустой)
    и группирует по (codec, resolution).
    Это пул «старых» видео для autocreate.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM sources
        WHERE pmv_list IS NOT NULL AND pmv_list != ''
        """
    )
    rows = cur.fetchall()
    conn.close()

    groups: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for r in rows:
        codec = r["codec"] or "?"
        resolution = r["resolution"] or "??x??"
        key = (codec, resolution)
        groups.setdefault(key, []).append(r)
    return groups


def fallback_new_only_make_one(
    target_seconds: int,
    max_sources: int,
    min_sources: int,
    excluded_ids: set[int],
    big_parts: int = 5,
    small_per_big: int = 5,
    strategy: str = "max_group",
    clip_algo_key: Optional[str] = None,
) -> Optional[Tuple[Path, List[int], Tuple[str, str]]]:
    """Резерв: создать PMV только из НОВЫХ исходников (без пар),
    с диверсификацией по папкам и стратегией выбора группы."""
    groups = db_get_unused_sources_grouped()
    if not groups:
        return None

    # фильтр excluded
    filtered: List[Tuple[Tuple[str, str], List[sqlite3.Row]]] = []
    for key, rows in groups.items():
        rem = [r for r in rows if int(r["id"]) not in excluded_ids]
        if rem:
            filtered.append((key, rem))
    if not filtered:
        return None

    # выбор группы по стратегии
    if strategy == "weighted_random":
        total = sum(len(rows) for _, rows in filtered)
        rnd = random.randint(1, max(1, total))
        acc = 0
        key, rows = filtered[0]
        for k, rws in filtered:
            acc += len(rws)
            if rnd <= acc:
                key, rows = k, rws
                break
    elif strategy == "random":
        key, rows = random.choice(filtered)
    else:
        key, rows = max(filtered, key=lambda kv: len(kv[1]))

    use_count = min(len(rows), max_sources)
    if use_count < 1 or len(rows) < max(1, min_sources):
        # best-effort: если не дотягиваем до минимума — берём сколько есть
        use_count = min(len(rows), max_sources)
        if use_count < 1:
            return None

    # диверсификация по папкам
    dir_map: Dict[Path, List[sqlite3.Row]] = {}
    for r in rows:
        d = Path(r["video_path"]).resolve().parent
        dir_map.setdefault(d, []).append(r)
    for lst in dir_map.values():
        random.shuffle(lst)

    chosen_rows: List[sqlite3.Row] = []
    taken: Dict[Path, int] = {d: 0 for d in dir_map}
    progressed = True
    while len(chosen_rows) < use_count and progressed:
        progressed = False
        for d, lst in list(dir_map.items()):
            if taken[d] >= PER_DIR_MAX_FIRST_PASS:
                continue
            if lst:
                chosen_rows.append(lst.pop())
                taken[d] += 1
                progressed = True
                if len(chosen_rows) >= use_count:
                    break
    if len(chosen_rows) < use_count:
        leftovers: List[sqlite3.Row] = []
        for lst in dir_map.values():
            leftovers.extend(lst)
        random.shuffle(leftovers)
        for r in leftovers:
            chosen_rows.append(r)
            if len(chosen_rows) >= use_count:
                break

    paths = [Path(r["video_path"]) for r in chosen_rows]
    source_ids = [int(r["id"]) for r in chosen_rows]

    out_path = make_pmv_from_files(paths, target_seconds, big_parts, small_per_big, clip_algo_key=clip_algo_key)
    out_path, move_comment = move_output_to_network_storage(out_path)
    pmv_tag = Path(out_path).name
    db_insert_compilation(out_path, source_ids, comments=move_comment)
    db_update_sources_pmv_list(source_ids, pmv_tag)
    excluded_ids.update(source_ids)
    return out_path, source_ids, key



async def cmd_pmvnew(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    groups = db_get_unused_sources_grouped()
    if not groups:
        return await update.message.reply_text("Нет исходников без PMV. Сначала воспользуйтесь /scan.")

    group_entries = [
        SourceGroupEntry(key=key, rows=list(rows), unused_count=len(rows))
        for key, rows in groups.items()
    ]
    group_entries = sort_source_group_entries(group_entries)
    group_list: List[Tuple[Tuple[str, str], List[sqlite3.Row]]] = [
        (entry.key, entry.rows) for entry in group_entries
    ]

    lines = format_source_group_lines(
        group_entries, "Найдены группы исходников (codec, resolution):"
    )
    lines.append("")
    lines.append("Ответьте сообщением с номером группы, которую будем обрабатывать (например: 1).")

    user_sessions[update.effective_user.id] = {
        "state": "choose_group",
        "groups": group_list,
    }

    await update.message.reply_text("\n".join(lines))

async def cmd_pmvold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Как pmvnew, но берёт ВСЕ исходники, а не только те, у которых pmv_list пустой.
    """
    if not check_access(update):
        return await unauthorized(update)

    groups = db_get_all_sources_grouped()
    if not groups:
        return await update.message.reply_text("В базе нет исходников. Сначала воспользуйтесь /scan.")

    unused_groups = db_get_unused_sources_grouped()
    group_entries = [
        SourceGroupEntry(
            key=key,
            rows=list(rows),
            unused_count=len(unused_groups.get(key, [])),
        )
        for key, rows in groups.items()
    ]
    group_entries = sort_source_group_entries(group_entries)
    group_list: List[Tuple[Tuple[str, str], List[sqlite3.Row]]] = [
        (entry.key, entry.rows) for entry in group_entries
    ]

    lines = format_source_group_lines(
        group_entries,
        "Найдены группы исходников (codec, resolution) — ВКЛЮЧАЯ уже использованные:",
    )
    lines.append("")
    lines.append("Ответьте сообщением с номером группы, которую будем обрабатывать (например: 1).")

    user_sessions[update.effective_user.id] = {
        "state": "choose_group",
        "groups": group_list,
    }

    await update.message.reply_text("\n".join(lines))



async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    # Локальный хелпер, режет длинные сообщения на части
    async def reply_long(msg: str, chunk_size: int = 4000):
        if len(msg) <= chunk_size:
            await update.message.reply_text(msg)
            return
        for i in range(0, len(msg), chunk_size):
            await update.message.reply_text(msg[i:i + chunk_size])

    lowered_text = text.lower()
    if lowered_text in {"musicprep", "/musicprep"}:
        return await cmd_musicprep(update, context)
    if lowered_text in {"newcompmusic", "/newcompmusic"}:
        return await cmd_newcompmusic(update, context)
    if lowered_text in {"scan", "/scan"}:
        return await cmd_scan(update, context)
    if lowered_text in {"rategrp", "/rategrp"}:
        return await cmd_rategrp(update, context)
    if lowered_text in {"reports", "/reports", "отчёты", "отчеты"}:
        return await cmd_reports(update, context)
    if lowered_text in {"flagpmv", "/flagpmv", "find", "/find", "найти"}:
        return await cmd_find(update, context)
    if lowered_text in {"createrandompmv", "/createrandompmv"}:
        return await cmd_randompmv(update, context)

    sess = user_sessions.get(user_id)
    if not sess:
        return await reply_long(
            "Я вас не понял. Используйте /pmvnew для создания PMV или /scan для обновления исходников."
        )

    state = sess.get("state")

    if state == "scanignore_wait_path":
        candidate = text.strip().strip('"')
        if not candidate:
            return await reply_long("Пришлите путь к папке, например X:\\\\tor\\\\tmp.")
        try:
            raw_path = Path(candidate)
            if not raw_path.is_absolute():
                raw_path = (SCRIPT_DIR / raw_path).resolve(strict=False)
            else:
                raw_path = raw_path.resolve(strict=False)
        except Exception as exc:
            return await reply_long(f"Не удалось разобрать путь: {exc}")

        db_add_scan_ignore(str(raw_path))
        user_sessions.pop(user_id, None)

        note = ""
        if not raw_path.exists():
            note = "\n⚠️ Папка пока не существует, но будет игнорироваться, когда появится."
        return await reply_long(f"Готово. {raw_path} больше не сканируется.{note}")

    if state in {
        "musicprep_wait_track",
        "musicprep_wait_seconds",
        "musicprep_wait_mode",
        "musicprep_wait_sensitivity",
        "newcompmusic_wait_project",
        "newcompmusic_wait_group",
        "newcompmusic_choose_duration",
        "newcompmusic_choose_groupmode",
        "newcompmusic_choose_color",
        "newcompmusic_wait_sources",
        "newcompmusic_wait_algo",
        "musicprepcheck_wait_project",
        "musicprep_ask_sensitivity",
    }:
        return await reply_long("Используйте кнопки под сообщением или запустите команду заново.")

    if state == "randompmv_wait_count":
        sess = sess or {}
        try:
            total_runs = int(text)
        except ValueError:
            return await reply_long("Нужно указать целое число от 1 до 30.")
        if total_runs < RANDOMPMV_MIN_BATCH:
            return await reply_long("Нужно указать положительное число.")
        total_runs = min(total_runs, RANDOMPMV_MAX_BATCH)
        sess["randompmv_total_runs"] = total_runs
        sess["state"] = "randompmv_wait_newcount"
        user_sessions[user_id] = sess
        await update.message.reply_text(
            "Сколько новых исходников обязательно должно быть в PMV?",
            reply_markup=build_randompmv_newcount_keyboard(),
        )
        return

    if state == "randompmv_wait_newcount":
        sess = sess or {}
        try:
            min_new = int(text)
        except ValueError:
            return await reply_long("Введите целое число новых исходников (0 допускается).")
        if min_new < 0:
            return await reply_long("Число новых исходников не может быть отрицательным.")
        total_runs = int(sess.get("randompmv_total_runs") or 0)
        if total_runs <= 0:
            user_sessions.pop(user_id, None)
            return await reply_long("Сначала выберите количество генераций через CreateRandomPMV.")
        user_sessions.pop(user_id, None)
        await update.message.reply_text(
            f"Запускаю {total_runs} Random PMV (новых ≥ {min_new})..."
        )
        return await run_randompmv_batch(reply_long, user_id, total_runs, min_new)


    if state in {"find_wait_term", "find_wait_choice"}:
        term = text.strip()
        if not term:
            return await reply_long("Пришлите часть названия файла, пример: 20251207 или 0734.")
        matches = _search_find_matches(term)
        sess["find_matches"] = matches
        if not matches:
            sess["state"] = "find_wait_term"
            return await reply_long("Не нашла PMV по этому фрагменту. Попробуйте другую часть имени.")
        sess["state"] = "find_wait_choice"
        pmv_count = sum(1 for m in matches if m.get("type") == "pmv")
        src_count = sum(1 for m in matches if m.get("type") == "source")
        lines = ["Нашлись совпадения. Выберите нужный файл кнопкой ниже."]
        lines.append(f"PMV: {pmv_count} · Исходники: {src_count}")
        for idx, match in enumerate(matches, 1):
            if match.get("type") == "pmv":
                lines.append(f"{idx}. PMV · {match.get('stem')}")
            else:
                color = extract_color_emoji(match.get("comments"))
                prefix = f"{color} " if color else ""
                lines.append(f"{idx}. {prefix}{match.get('video_name')}")
        await update.message.reply_text(
            "\n".join(lines),
            reply_markup=build_find_keyboard(matches),
        )
        return

    # =========================
    # NEWCOMPMUSIC: выбор музыкального проекта
    # =========================
    if state == "newcompmusic_choose_project":
        if not text.isdigit():
            return await reply_long("Нужно прислать номер проекта (целое число).")
        idx = int(text)
        projects: List[Dict[str, Any]] = sess.get("music_projects") or []
        if not (1 <= idx <= len(projects)):
            return await reply_long("Нет проекта с таким номером. Попробуйте снова.")
        chosen = projects[idx - 1]
        manifest_data = chosen.get("manifest_data")
        if not manifest_data and chosen.get("manifest_path") and chosen["manifest_path"].exists():
            try:
                manifest_data = json.loads(chosen["manifest_path"].read_text(encoding="utf-8"))
                chosen["manifest_data"] = manifest_data
            except Exception as exc:
                return await reply_long(f"Не удалось прочитать manifest.json: {exc}")
        parsed_segments = parse_manifest_segments(manifest_data or {})
        total_duration = chosen.get("duration")
        if total_duration is None and parsed_segments:
            total_duration = parsed_segments[-1].end
        seg_count = len(parsed_segments)
        minutes = (total_duration or 0.0) / 60.0 if total_duration else None

        groups = get_source_groups_prefer_unused()
        if not groups:
            return await reply_long("Не удалось найти группы исходников. Сначала просканируйте /scan.")

        sess["state"] = "newcompmusic_choose_orientation"
        sess["music_selected"] = {
            "slug": chosen["slug"],
            "name": chosen["name"],
            "duration": total_duration,
            "segments": seg_count,
            "manifest": manifest_data,
            "audio_path": str(chosen.get("audio_path")) if chosen.get("audio_path") else None,
            "parsed_segments": parsed_segments,
        }
        lines = [
            f"Выбран проект: {chosen['name']} (slug: {chosen['slug']}).",
            f"Смен клипов: {seg_count}",
        ]
        if minutes:
            lines.append(f"Продолжительность ≈ {minutes:.1f} минут.")
        group_entries = [
            SourceGroupEntry(key=key, rows=list(rows), unused_count=unused_count)
            for key, rows, unused_count in groups
        ]
        sorted_entries, orientation_map = sort_group_entries_with_orientation(group_entries)
        sess["music_group_orientations"] = orientation_map
        sess["music_groups_all"] = [
            (entry.key, entry.rows, entry.unused_count) for entry in sorted_entries
        ]
        sess["music_groups"] = []
        sess["music_orientation_preference"] = None
        lines.append("")
        lines.append("Выберите ориентацию исходников: VR, HOR или VER.")
        lines.append("Пришлите одно из этих значений или нажмите кнопку ниже.")
        return await reply_long(
            "\n".join(lines),
            reply_markup=build_newcomp_orientation_keyboard(),
        )

    if state == "newcompmusic_choose_orientation":
        choice = text.strip().upper()
        if choice == "BACK":
            return await reply_long("Выбор сброшен. Укажите ориентацию: VR, HOR или VER.")
        if choice not in NEWCOMPMUSIC_ORIENTATION_CHOICES:
            return await reply_long("Введите VR, HOR или VER.")
        all_groups = sess.get("music_groups_all") or []
        if not all_groups:
            return await reply_long("Не удалось найти группы. Запустите /newcompmusic заново.")
        orientation_map = sess.get("music_group_orientations") or {}
        filtered = filter_groups_by_orientation(all_groups, orientation_map, choice)
        if not filtered:
            return await reply_long("Нет групп с такой ориентацией. Выберите другой режим.")
        sess["music_orientation_preference"] = choice
        sess["music_groups"] = filtered
        sess["state"] = "newcompmusic_choose_group"
        group_entries = [
            SourceGroupEntry(key=key, rows=list(rows), unused_count=unused)
            for key, rows, unused in filtered
        ]
        orientation_map = sess.get("music_group_orientations") or {}

        lines = _build_group_selection_lines(
            sess, group_entries, choice, prompt_kind="text"
        )
        return await reply_long("\n".join(lines))

    if state == "newcompmusic_choose_group":
        if not text.isdigit():
            return await reply_long("Нужно прислать номер группы.")
        idx = int(text)
        groups: List[Tuple[Tuple[str, str], List[sqlite3.Row], int]] = sess.get("music_groups") or []
        if not (1 <= idx <= len(groups)):
            return await reply_long("Нет группы с таким номером.")
        key, rows, unused_count = groups[idx - 1]
        if not rows:
            return await reply_long("В выбранной группе нет исходников. Выберите другую.")

        orientation_label = (sess.get("music_group_orientations") or {}).get(key)
        if not orientation_label:
            orientation_label = _resolution_orientation(key[1] or "")[0]
        sess["music_group_choice"] = {
            "key": key,
            "count": len(rows),
            "orientation": orientation_label,
            "total_count": len(rows),
            "unused_count": unused_count,
            "group_number": idx,
        }
        sess["music_group_rows"] = list(rows)
        sess["music_folder_only_new"] = False
        sess.pop("music_color_rows", None)
        sess["state"] = "newcompmusic_choose_groupmode"
        summary = [
            f"Группа {idx} выбрана: {key[0]} {key[1]} (исходников: {len(rows)}).",
            "Как будем группировать исходники?",
        ]
        await reply_long("\n".join(summary))
        return await update.message.reply_text(
            "Выберите вариант:",
            reply_markup=build_newcomp_groupmode_keyboard(),
        )

    if state == "newcompmusic_wait_folder":
        options: List[Dict[str, Any]] = sess.get("music_folder_options") or []
        if not text.isdigit():
            return await reply_long("Выберите подпапку кнопками под сообщением или пришлите её номер.")
        idx = int(text)
        if not (1 <= idx <= len(options)):
            return await reply_long("Нет папки с таким номером. Попробуйте снова.")
        token = options[idx - 1]["token"]
        try:
            count, label = apply_newcomp_folder_choice(sess, token, next_state="newcompmusic_ask_sources")
        except ValueError as exc:
            return await reply_long(str(exc))
        project_info = sess.get("music_selected") or {}
        codec, res = sess.get("music_group_choice", {}).get("key") or ("?", "?")
        lines = [
            f"Папка выбрана: {label} (исходников: {count}).",
            f"Группа: {codec} {res}.",
            "Сколько исходников задействовать? Пришлите целое число (например: 6).",
        ]
        return await reply_long("\n".join(lines))

    if state == "newcompmusic_ask_sources":
        try:
            sources_count = int(text)
            if sources_count <= 0:
                raise ValueError
        except ValueError:
            return await reply_long("Нужно указать положительное число исходников.")
        available = int((sess.get("music_group_choice") or {}).get("count") or 0)
        info_line = None
        if available and sources_count > available:
            sources_count = available
            info_line = _source_limit_message(sess, available)

        sess["music_sources"] = sources_count
        sess["state"] = "newcompmusic_ask_algo"

        algo_parts = [
            f"{key} ({meta['title']})"
            for key, meta in CLIP_SEQUENCE_ALGORITHMS.items()
        ]
        base_line = f"Ок, возьмём {sources_count} исходников."
        if info_line:
            base_line = f"{info_line}\n{base_line}"
        msg = (
            f"ÐÑÑÐ¿Ð¿Ð° {key[0]} {key[1]} Ð²ÑÐ±ÑÐ°Ð½Ð°. "
            "ÐÐ¾Ð²ÑÑ Ð¸ÑÑÐ¾Ð´Ð½Ð¸ÐºÐ¾Ð² Ð½ÐµÑ, Ð²ÑÐ±ÐµÑÐ¸ÑÐµ ÑÐ²ÐµÑ Ð´Ð»Ñ Ð¿ÐµÑÐµÐ¾ÑÐµÐ½ÐºÐ¸:"
        )
        await update.message.reply_text(
            msg, reply_markup=build_rategrp_rerate_keyboard(available)
        )
        return
        sess["rategrp_queue"] = queue
        sess["rategrp_total"] = len(queue)
        sess["rategrp_processed"] = 0
        sess["rategrp_queue_origin"] = "unrated"
        sess["state"] = "rategrp_rate_source"

        async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await update.message.reply_text(msg, reply_markup=markup)

        await send_rategrp(f"Группа {key[0]} {key[1]} выбрана. Неоценённых исходников: {len(queue)}.")
        return await rategrp_send_next_prompt(sess, send_rategrp)

    if state == "rategrp_choose_rerate_color":
        color_key = normalize_rategrp_color_input(text)

        async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await update.message.reply_text(msg, reply_markup=markup)

        if not color_key:
            rows = sess.get("rategrp_rerate_rows") or []
            available = _rategrp_available_colors(rows)
            if available:
                await send_rategrp(
                    "Выберите цвет с помощью кнопок ниже.",
                    build_rategrp_rerate_keyboard(available),
                )
            else:
                await reply_long("Нет исходников для переоценки. Выберите другую группу.")
            return
        if await _rategrp_start_rerate(sess, color_key, send_rategrp):
            return
        return

    if state == "rategrp_rate_source":
        color_key = normalize_rategrp_color_input(text)
        if not color_key:
            return await reply_long(f"Используйте кнопки {RATEGRP_COLOR_PROMPT} или пришлите название цвета.")

        async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await update.message.reply_text(msg, reply_markup=markup)

        return await rategrp_apply_rating(sess, color_key, send_rategrp)
    # ====== MUSICPREP: выбор трека и параметров ======
    if state == "musicprep_choose_file":
        if not text.isdigit():
            return await reply_long("Нужно прислать номер трека.")
        files = sess.get("music_files") or []
        idx = int(text)
        if not (1 <= idx <= len(files)):
            return await reply_long("Нет трека с таким номером.")
        sess["musicprep_file"] = files[idx - 1]
        sess["state"] = "musicprep_ask_name"
        return await reply_long("Введите имя проекта (или оставьте пустым).")

    if state == "musicprep_ask_name":
        sess["musicprep_name"] = text.strip() or None
        sess["state"] = "musicprep_ask_segment"
        mod = load_music_generator_module()
        default_seg = getattr(mod, "DEFAULT_TARGET_SEGMENT", 1.0)
        return await reply_long(
            f"Укажите минимальную длительность сегмента в секундах "
            f"(по умолчанию {default_seg})."
        )

    if state == "musicprep_ask_segment":
        mod = load_music_generator_module()
        default_seg = getattr(mod, "DEFAULT_TARGET_SEGMENT", 1.0)
        try:
            segment_len = float(text)
            if segment_len < 0:
                raise ValueError
        except ValueError:
            segment_len = default_seg
        sess["musicprep_segment"] = segment_len
        sess["state"] = "musicprep_ask_mode"
        modes = getattr(mod, "SEGMENT_MODES", ("beat",))
        return await reply_long(
            "Выберите алгоритм сегментации по музыке "
            f"(доступны: {', '.join(modes)})."
        )

    if state == "musicprep_ask_mode":
        mod = load_music_generator_module()
        modes = getattr(mod, "SEGMENT_MODES", ("beat",))
        mode = text.strip().lower() or getattr(mod, "DEFAULT_SEGMENT_MODE", modes[0])
        if mode not in modes:
            return await reply_long("Неизвестный режим. Повторите ввод.")
        sess["musicprep_selected_mode"] = mode
        options = get_musicprep_sensitivity_options(mode)
        if options:
            sess["musicprep_sensitivity_options"] = options
            sess["state"] = "musicprep_ask_sensitivity"
            lines = [
                f"Алгоритм {mode} выбран.",
                "Выберите чувствительность анализа:",
            ]
            for idx, opt in enumerate(options, 1):
                lines.append(f"{idx}. {opt['label']} — {opt['description']}")
            lines.append("Пришлите номер или ключевое слово.")
            return await reply_long("\n".join(lines))

        async def send(msg: str) -> None:
            await reply_long(msg)

        return await finalize_musicprep_project(send, sess, user_id, mode)

    if state == "musicprep_ask_sensitivity":
        options: List[Dict[str, Any]] = sess.get("musicprep_sensitivity_options") or []
        mode = sess.get("musicprep_selected_mode") or "beat"
        if not options:
            sess["state"] = "musicprep_ask_mode"
            return await reply_long("Повторите выбор алгоритма сегментации.")

        choice = text.strip().lower()
        selected: Optional[Dict[str, Any]] = None
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                selected = options[idx]
        if not selected:
            for opt in options:
                if choice in {opt["key"].lower(), opt["label"].lower()}:
                    selected = opt
                    break
        if not selected:
            return await reply_long("Не удалось распознать вариант. Пришлите номер из списка.")

        analysis_kwargs = selected.get("analysis_kwargs") or {}

        async def send(msg: str) -> None:
            await reply_long(msg)

        return await finalize_musicprep_project(send, sess, user_id, mode, analysis_kwargs)

    # ====== MUSICPREP: выбор трека и параметров ======
    if state == "musicprep_choose_file":
        if not text.isdigit():
            return await reply_long("Нужно прислать номер трека.")
        files = sess.get("music_files") or []
        idx = int(text)
        if not (1 <= idx <= len(files)):
            return await reply_long("Нет трека с таким номером.")
        sess["musicprep_file"] = files[idx - 1]
        sess["state"] = "musicprep_ask_name"
        return await reply_long("Введите имя проекта (или оставьте пустым).")

    if state == "musicprep_ask_name":
        sess["musicprep_name"] = text.strip() or None
        sess["state"] = "musicprep_ask_segment"
        mod = load_music_generator_module()
        default_seg = getattr(mod, "DEFAULT_TARGET_SEGMENT", 1.0)
        return await reply_long(
            f"Укажите минимальную длительность сегмента в секундах "
            f"(по умолчанию {default_seg})."
        )

    if state == "musicprep_ask_segment":
        mod = load_music_generator_module()
        default_seg = getattr(mod, "DEFAULT_TARGET_SEGMENT", 1.0)
        try:
            segment_len = float(text)
            if segment_len < 0:
                raise ValueError
        except ValueError:
            segment_len = default_seg
        sess["musicprep_segment"] = segment_len
        sess["state"] = "musicprep_ask_mode"
        modes = getattr(mod, "SEGMENT_MODES", ("beat",))
        return await reply_long(
            "Выберите алгоритм сегментации по музыке "
            f"(доступны: {', '.join(modes)})."
        )

    if state == "musicprep_ask_mode":
        mod = load_music_generator_module()
        modes = getattr(mod, "SEGMENT_MODES", ("beat",))
        mode = text.strip().lower() or getattr(mod, "DEFAULT_SEGMENT_MODE", modes[0])
        if mode not in modes:
            return await reply_long("Неизвестный режим. Повторите ввод.")
        sess["musicprep_selected_mode"] = mode
        options = get_musicprep_sensitivity_options(mode)
        if options:
            sess["musicprep_sensitivity_options"] = options
            sess["state"] = "musicprep_ask_sensitivity"
            lines = [
                f"Алгоритм {mode} выбран.",
                "Выберите чувствительность анализа:",
            ]
            for idx, opt in enumerate(options, 1):
                lines.append(f"{idx}. {opt['label']} — {opt['description']}")
            lines.append("Пришлите номер или ключевое слово.")
            return await reply_long("\n".join(lines))

        async def send(msg: str) -> None:
            await reply_long(msg)

        return await finalize_musicprep_project(send, sess, user_id, mode)

    # =========================
    # AUTOCREATE: диалог
    # =========================

    # Шаг 1: сколько видео создать
    if state == "autocreate_ask_count":
        try:
            count = int(text)
            if count <= 0:
                raise ValueError
        except ValueError:
            return await reply_long("Нужно положительное целое число — сколько видео создать (например: 4).")

        sess["autocreate_total_videos"] = count
        sess["state"] = "autocreate_ask_length"

        return await reply_long(
            f"Ок, создаём до {count} видео.\n"
            "Теперь введите длину КАЖДОГО видео в минутах (например: 15)."
        )

    # Шаг 2: длина каждого видео
    if state == "autocreate_ask_length":
        try:
            minutes = int(text)
            if minutes <= 0:
                raise ValueError
        except ValueError:
            minutes = DEFAULT_TARGET_MINUTES

        sess["autocreate_minutes"] = minutes
        sess["state"] = "autocreate_ask_max_sources"

        return await reply_long(
            f"Желаемая длительность PMV около {minutes} минут.\n"
            "Теперь введите МАКСИМАЛЬНОЕ количество исходников на одно видео (например: 10)."
        )

    if state == "autocreate_ask_max_sources":
        try:
            max_sources = int(text)
            if max_sources <= 0:
                raise ValueError
        except ValueError:
            max_sources = 10

        min_sources = max_sources
        total_videos = sess.get("autocreate_total_videos", 1)
        minutes = sess.get("autocreate_minutes", DEFAULT_TARGET_MINUTES)

        user_sessions.pop(user_id, None)

        await reply_long(
            f"Запускаю пакетную генерацию из {total_videos} PMV.\n"
            f"Длительность каждого: ~{minutes} минут.\n"
            f"Исходников на одно видео: минимум {min_sources}, максимум {max_sources}.\n"
            "Попробую собрать максимально разнообразные (как по использованным источникам) ролики, "
            "насколько это получится. Ну что, поехали..."
        )

        try:
            report = autocreate_pmv_batch(
                total_videos=total_videos,
                minutes_each=minutes,
                max_sources=max_sources,
                min_sources=min_sources,
            )
        except Exception as e:
            return await reply_long(f"Ой, произошла ошибка во время пакетной генерации PMV: {e}")

        return await reply_long(report)

    # =========================
    # ДАЛЬШЕ — ВСЯ СТАРАЯ ЛОГИКА
    # =========================

    # ====== Шаг 1: выбор группы ======
    if state == "choose_group":
        if not text.isdigit():
            return await reply_long("Нужно просто число — номер группы.")
        idx = int(text)
        groups: List[Tuple[Tuple[str, str], List[sqlite3.Row]]] = sess["groups"]
        if not (1 <= idx <= len(groups)):
            return await reply_long("Неверный номер группы.")
        key, rows = groups[idx - 1]
        codec, res = key

        sess["state"] = "choose_files"
        sess["current_group"] = key
        sess["current_rows"] = rows

        lines = [f"Выбрана группа: {codec} {res}. Файлы:"]
        for i, r in enumerate(rows, 1):
            lines.append(f"{i}. {Path(r['video_path']).name} (id={r['id']})")
        lines.append("")
        lines.append("Ответьте: 'all' чтобы взять все, либо номера файлов через пробел (например: 1 3 5).")
        return await reply_long("\n".join(lines))

    # ====== Шаг 2: выбор файлов ======
    if state == "choose_files":
        rows: List[sqlite3.Row] = sess["current_rows"]

        if text.lower() in {"all", "все"}:
            selected_rows = rows
        else:
            parts = text.replace(",", " ").split()
            idxs = []
            for p in parts:
                if not p.isdigit():
                    continue
                v = int(p)
                if 1 <= v <= len(rows):
                    idxs.append(v - 1)
            if not idxs:
                return await reply_long(
                    "Не получилось распознать номера. Напишите 'all' или номера файлов через пробел."
                )
            selected_rows = [rows[i] for i in idxs]

        if not selected_rows:
            return await reply_long("Пустой выбор. Попробуйте ещё раз.")

        sess["state"] = "choose_length"
        sess["selected_rows"] = selected_rows

        names = ", ".join(Path(r["video_path"]).name for r in selected_rows)
        msg = (
            f"Выбрано файлов: {len(selected_rows)}.\n"
            f"Имена: {names}\n\n"
            "Теперь введите желаемую длину итогового PMV в МИНУТАХ (например: 15)."
        )
        return await reply_long(msg)

    # ====== Шаг 3: выбор длины ======
    if state == "choose_length":
        try:
            minutes = int(text)
            if minutes <= 0:
                raise ValueError
        except ValueError:
            minutes = DEFAULT_TARGET_MINUTES

        sess["target_minutes"] = minutes
        sess["state"] = "choose_big_parts"

        return await reply_long(
            f"Ок, целевая длина ~{minutes} минут.\n"
            "Сколько БОЛЬШИХ частей на каждый файл (big_parts)? (по умолчанию 5)"
        )

    # ====== Шаг 4: выбор big_parts ======
    if state == "choose_big_parts":
        try:
            big_parts = int(text)
            if big_parts <= 0:
                raise ValueError
        except ValueError:
            big_parts = 5

        sess["big_parts"] = big_parts
        sess["state"] = "choose_small_parts"

        return await reply_long(
            f"big_parts = {big_parts}\n"
            "Сколько МАЛЕНЬКИХ клипов в каждой большой части (small_per_big)? (по умолчанию 5)"
        )

    # ====== Шаг 5: выбор small_per_big и запуск нарезки ======
    if state == "choose_small_parts":
        try:
            small_per_big = int(text)
            if small_per_big <= 0:
                raise ValueError
        except ValueError:
            small_per_big = 5

        minutes = sess["target_minutes"]
        selected_rows: List[sqlite3.Row] = sess["selected_rows"]
        big_parts = sess["big_parts"]

        target_seconds = minutes * 60

        await reply_long(
            f"Ок, делаем PMV ~{minutes} минут.\n"
            f"big_parts = {big_parts}, small_per_big = {small_per_big}, файлов: {len(selected_rows)}.\n"
            "Начинаю нарезку, подождите..."
        )

        paths = [Path(r["video_path"]) for r in selected_rows]
        source_ids = [int(r["id"]) for r in selected_rows]

        user_sessions.pop(user_id, None)

        manual_algo_key = random.choice(list(CLIP_SEQUENCE_ALGORITHMS.keys()))
        manual_algo_key, manual_algo_meta = resolve_clip_algorithm(manual_algo_key)

        move_comment = ""
        try:
            out_path = make_pmv_from_files(
                paths,
                target_seconds,
                big_parts,
                small_per_big,
                clip_algo_key=manual_algo_key,
            )
            out_path, move_comment = move_output_to_network_storage(out_path)
        except Exception as e:
            return await reply_long(f"❌ Ошибка при создании PMV: {e}")

        pmv_tag = Path(out_path).name
        db_insert_compilation(out_path, source_ids, comments=move_comment)
        db_update_sources_pmv_list(source_ids, pmv_tag)

        return await reply_long(
            f"✅ Готово!\nФайл: {out_path}\n"
            f"Алгоритм клипов: {manual_algo_meta['title']} ({manual_algo_meta['short']}).\n"
            f"PMV записан в базу, исходники помечены как использованные."
        )

def apply_pmv_rating(
    row_obj: Union[sqlite3.Row, Dict[str, Any]],
    rating: int,
) -> Tuple[Dict[str, Any], str]:
    """
    Помечает PMV оценкой и при необходимости переносит файл в папку rating_<оценка>.
    Возвращает обновлённую запись и имя файла для логов/ответов.
    """
    row = dict(row_obj)
    pmv_id = int(row["id"])
    old_path = Path(row["video_path"])
    pmv_name = old_path.name

    db_append_compilation_comment(pmv_id, f"pmv_rating={rating}")

    try:
        rating_dir = NETWORK_OUTPUT_ROOT / f"rating_{rating}"
        rating_dir.mkdir(parents=True, exist_ok=True)

        new_path = rating_dir / old_path.name
        if old_path.resolve() != new_path.resolve():
            shutil.move(str(old_path), str(new_path))

            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE compilations SET video_path = ? WHERE id = ?",
                (str(new_path.resolve()), pmv_id),
            )
            conn.commit()
            conn.close()

            row["video_path"] = str(new_path.resolve())
    except Exception as e:
        db_append_compilation_comment(pmv_id, f"move_error={e}")

    return row, pmv_name


def apply_pmv_rating_pairs(
    pmv_rows: List[sqlite3.Row],
    pairs: Iterable[Tuple[int, int]],
) -> Tuple[List[str], List[str]]:
    """
    Применяет несколько пар (номер, оценка) к списку PMV.
    Возвращает два списка строк: успешно обработанные и ошибки.
    """
    success_lines: List[str] = []
    error_lines: List[str] = []
    total = len(pmv_rows)

    for idx_val, rating_val in pairs:
        if rating_val < 1 or rating_val > 5:
            error_lines.append(f"PMV №{idx_val}: оценка должна быть 1-5.")
            continue
        if not (1 <= idx_val <= total):
            error_lines.append(f"PMV №{idx_val}: такого номера нет (всего {total}).")
            continue

        try:
            _, pmv_name = apply_pmv_rating(pmv_rows[idx_val - 1], rating_val)
        except Exception as exc:
            error_lines.append(f"PMV №{idx_val}: ошибка {exc}.")
            continue

        success_lines.append(f"{pmv_name} → {rating_val}/5")

    return success_lines, error_lines


async def process_ratepmv_choice(
    sess: Dict[str, Any],
    idx: int,
    rating: int,
    reply_long: Callable[[str], Awaitable[None]],
) -> Optional[bool]:
    if rating < 1 or rating > 5:
        return await reply_long("Оценка должна быть от 1 до 5.")

    pmv_rows: List[sqlite3.Row] = sess.get("pmv_rows") or []
    if not pmv_rows:
        return await reply_long("Не удалось найти список PMV для оценки.")
    if not (1 <= idx <= len(pmv_rows)):
        return await reply_long("Некорректный номер PMV.")

    row_obj = pmv_rows[idx - 1]
    row, pmv_name = apply_pmv_rating(row_obj, rating)

    sess["state"] = "ratepmv_confirm_sources"
    sess["chosen_pmv"] = row
    sess["pmv_rating"] = rating

    await reply_long(
        f"Вы выбрали PMV №{idx}: {pmv_name}\n"
        f"Оценка зафиксирована: {rating}/5.\n"
        f"Файл перенесён в rating_{rating}.\n\n"
        "Напишите, будете ли оценивать источники (да/нет)."
    )
    return True

# ====== RATEPMV: выбор PMV и общей оценки ======
    if state == "ratepmv_choose_pmv":
        tokens = text.replace(",", " ").split()
        digits = [t for t in tokens if t.isdigit()]
        if len(digits) < 2:
            return await reply_long(
                "Укажи хотя бы одну пару `<номер> <оценка 1-5>` (например: `2 5` или `1 5 2 4`)."
            )

        numbers = [int(t) for t in digits]
        if len(numbers) == 2:
            idx, rating = numbers
            return await process_ratepmv_choice(sess, idx, rating, reply_long)

        if len(numbers) % 2 != 0:
            return await reply_long(
                "В пакетном режиме нужно чётное количество чисел — номер PMV и оценка 1-5."
            )

        pmv_rows: List[sqlite3.Row] = sess.get("pmv_rows") or []
        if not pmv_rows:
            return await reply_long("Не удалось найти список PMV для оценки.")

        await reply_long("Принял пакет, выставляю оценки...")

        rating_pairs = [
            (numbers[i], numbers[i + 1]) for i in range(0, len(numbers), 2)
        ]
        success_lines, error_lines = apply_pmv_rating_pairs(pmv_rows, rating_pairs)

        if not success_lines:
            msg = "Не удалось обработать ни одну пару. Проверь номера и оценки."
            if error_lines:
                msg += "\n" + "\n".join(error_lines)
            return await reply_long(msg)

        user_sessions.pop(user_id, None)

        lines = [
            f"✅ Пакетная оценка завершена, обновлено PMV: {len(success_lines)}.",
            "Успешно отмечены:",
        ]
        lines.extend(f"- {entry}" for entry in success_lines)
        if error_lines:
            lines.append("")
            lines.append("⚠️ Пропущены:")
            lines.extend(f"- {entry}" for entry in error_lines)
        lines.append("")
        lines.append("Оценки источников в пакетном режиме не ставятся — отправь конкретный PMV отдельно, если нужно.")
        return await reply_long("\n".join(lines))

    # ====== RATEPMV: спросить, оценивать ли исходники ======
    if state == "ratepmv_confirm_sources":
        answer = text.lower()
        if answer not in {"да", "д", "yes", "y", "нет", "не", "no", "n"}:
            return await reply_long("Ответьте 'да' или 'нет'.")

        if answer in {"нет", "не", "no", "n"}:
            user_sessions.pop(user_id, None)
            return await reply_long("✅ Оценка PMV сохранена.")

        chosen_pmv: sqlite3.Row | dict = sess["chosen_pmv"]
        pmv_id = int(chosen_pmv["id"])
        source_ids_str = chosen_pmv["source_ids"] or ""
        src_ids = [int(x) for x in source_ids_str.split(",") if x.strip().isdigit()]

        if not src_ids:
            user_sessions.pop(user_id, None)
            return await reply_long(
                "В этой компиляции не нашлось исходников (source_ids пустые)."
            )

        conn = get_conn()
        cur = conn.cursor()
        q_marks = ",".join("?" for _ in src_ids)
        cur.execute(f"SELECT * FROM sources WHERE id IN ({q_marks})", src_ids)
        src_rows = cur.fetchall()
        conn.close()

        if not src_rows:
            user_sessions.pop(user_id, None)
            return await reply_long(
                "Не удалось найти исходники в базе. Оценка PMV уже сохранена."
            )

        src_map = {r["id"]: r for r in src_rows}
        ordered_sources = [src_map[sid] for sid in src_ids if sid in src_map]

        # 🔥 НОВОЕ: выкидываем те исходники, которые уже оценены для ЭТОГО PMV
        unrated_sources = []
        already_rated = 0
        marker = f"pmv#{pmv_id}_rating="
        for r in ordered_sources:
            comments = (r["comments"] or "")
            if marker in comments:
                already_rated += 1
            else:
                unrated_sources.append(r)

        if not unrated_sources:
            user_sessions.pop(user_id, None)
            return await reply_long(
                "Все исходники в этой компиляции уже имеют оценки для этого PMV. ✅"
            )

        sess["state"] = "ratepmv_sources_scores"
        sess["sources_rows"] = unrated_sources

        lines = []
        if already_rated:
            lines.append(f"Часть видео уже оценена для этого PMV: пропущено {already_rated} шт.")
        lines.append("Оценим оставшиеся видео в этой компиляции:")
        for i, r in enumerate(unrated_sources, 1):
            lines.append(f"{i}. {r['video_name']} (id={r['id']})")
        lines.append("")
        lines.append(
            "Пришлите оценки через пробел, например: `5 3 4 1 5`.\n"
            "Количество оценок может быть меньше количества видео — "
            "лишние видео останутся без оценки."
        )
        return await reply_long("\n".join(lines))

    # ====== RATEPMV: приём оценок по каждому исходнику ======
    if state == "ratepmv_sources_scores":
        parts = text.replace(",", " ").split()
        ratings: List[int] = []
        for p in parts:
            if not p.isdigit():
                continue
            v = int(p)
            if 1 <= v <= 5:
                ratings.append(v)

        if not ratings:
            return await reply_long(
                "Не получилось распознать ни одной оценки от 1 до 5. Попробуйте ещё раз."
            )

        sources_rows: List[sqlite3.Row] = sess["sources_rows"]
        chosen_pmv: sqlite3.Row | dict = sess["chosen_pmv"]
        pmv_id = int(chosen_pmv["id"])

        for src_row, rate in zip(sources_rows, ratings):
            sid = int(src_row["id"])
            db_append_source_comment(sid, f"pmv#{pmv_id}_rating={rate}")

        user_sessions.pop(user_id, None)
        return await reply_long(
            f"✅ Оценки сохранены.\n"
            f"Видео оценено: {len(ratings)} из {len(sources_rows)} (в этой сессии)."
        )

    # ====== COMPMV: выбор PMV ======
    if state == "compmv_choose":
        if not text.isdigit():
            return await reply_long("Нужно просто число — номер PMV.")

        idx = int(text)
        pmv_rows: List[sqlite3.Row] = sess["pmv_rows"]
        if not (1 <= idx <= len(pmv_rows)):
            return await reply_long("Неверный номер PMV.")

        row = pmv_rows[idx - 1]
        pmv_id = int(row["id"])
        pmv_name = Path(row["video_path"]).name
        

        sess["state"] = "compmv_enter_comment"
        sess["chosen_pmv_id"] = pmv_id

        return await reply_long(
            f"Вы выбрали PMV: {pmv_name} (id={pmv_id}).\n"
            "Теперь пришлите текст комментария."
        )

    # ====== COMPMV: ввод комментария ======
    if state == "compmv_enter_comment":
        pmv_id = sess.get("chosen_pmv_id")
        comment_text = text.strip()
        if not comment_text:
            return await reply_long("Комментарий пустой. Пришлите непустой текст.")

        db_append_compilation_comment(pmv_id, comment_text)

        user_sessions.pop(user_id, None)
        return await reply_long("✅ Комментарий к компиляции сохранён.")

    # ====== COMVID: выбор исходника ======
    if state == "comvid_choose":
        if not text.isdigit():
            return await reply_long("Нужно просто число — номер видео.")

        idx = int(text)
        src_rows: List[sqlite3.Row] = sess["src_rows"]
        if not (1 <= idx <= len(src_rows)):
            return await reply_long("Неверный номер видео.")

        row = src_rows[idx - 1]
        src_id = int(row["id"])
        src_name = row["video_name"]

        sess["state"] = "comvid_enter_comment"
        sess["chosen_src_id"] = src_id

        return await reply_long(
            f"Вы выбрали видео: {src_name} (id={src_id}).\n"
            "Теперь пришлите текст комментария."
        )

    # ====== COMVID: ввод комментария ======
    if state == "comvid_enter_comment":
        src_id = sess.get("chosen_src_id")
        comment_text = text.strip()
        if not comment_text:
            return await reply_long("Комментарий пустой. Пришлите непустой текст.")

        db_append_source_comment(src_id, comment_text)

        user_sessions.pop(user_id, None)
        return await reply_long("✅ Комментарий к исходнику сохранён.")

    # Если что-то пошло не так
    await reply_long("Что-то пошло не так с состоянием диалога. Запустите /pmvnew или /autocreate заново.")
    user_sessions.pop(user_id, None)


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not check_access(update):
        await query.answer("Нет доступа", show_alert=True)
        return await unauthorized(update)

    user_id = query.from_user.id if query.from_user else 0
    data = (query.data or "").strip()
    sess = user_sessions.get(user_id)

    if not sess:
        await query.answer("Сессия устарела", show_alert=True)
        return

    if data.startswith("report_group:"):
        if sess.get("state") != "reports_wait_choice":
            await query.answer("Сначала откройте меню отчётов.", show_alert=True)
            return
        color_key = data.split(":", 1)[1]
        report_env = ReportEnvironment(
            db_get_groups=db_get_all_sources_grouped,
            color_choices=RATEGRP_COLOR_CHOICES,
        )
        text = build_color_group_report(report_env, color_key)
        await query.answer("Готово")
        await query.message.reply_text(text)
        return

    if data.startswith("randompmv_count:"):
        if not sess or sess.get("state") != "randompmv_wait_count":
            return await query.answer("Нет активной сессии CreateRandomPMV", show_alert=True)
        try:
            total_runs = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Не понял выбранное значение", show_alert=True)
        total_runs = max(RANDOMPMV_MIN_BATCH, min(total_runs, RANDOMPMV_MAX_BATCH))
        sess["randompmv_total_runs"] = total_runs
        sess["state"] = "randompmv_wait_newcount"
        user_sessions[user_id] = sess
        await query.answer("Количество запусков сохранено")
        await query.message.reply_text(
            "Сколько новых исходников обязательно включать в каждый PMV?",
            reply_markup=build_randompmv_newcount_keyboard(),
        )
        return

    if data.startswith("randompmv_newcount:"):
        if not sess or sess.get("state") != "randompmv_wait_newcount":
            return await query.answer("Сначала выберите количество PMV", show_alert=True)
        try:
            min_new = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Не понял выбранное значение", show_alert=True)
        if min_new < 0:
            return await query.answer("Число не может быть отрицательным", show_alert=True)
        total_runs = int(sess.get("randompmv_total_runs") or 0)
        if total_runs <= 0:
            user_sessions.pop(user_id, None)
            return await query.answer("Сессия CreateRandomPMV сброшена", show_alert=True)

        async def send_from_query(message: str) -> None:
            await query.message.reply_text(message)

        user_sessions.pop(user_id, None)
        await query.answer(f"Запускаю {total_runs} Random PMV")
        await query.message.reply_text(
            f"Запускаю {total_runs} Random PMV (новых ≥ {min_new})..."
        )
        return await run_randompmv_batch(send_from_query, user_id, total_runs, min_new)

    if data.startswith("find_pick:"):
        if not sess.get("find_mode"):
            return await query.answer("Сначала запустите команду «Найти».", show_alert=True)
        try:
            idx = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Не понял номер", show_alert=True)
        matches: List[Dict[str, Any]] = sess.get("find_matches") or []
        if not (0 <= idx < len(matches)):
            return await query.answer("Нет такого PMV в списке", show_alert=True)

        async def send_find(message: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await query.message.reply_text(message, reply_markup=markup)

        await query.answer("Открываю PMV")
        await query.message.edit_reply_markup(None)
        entry = matches[idx]
        if entry.get("type") == "pmv":
            return await _start_find_pmv_queue(sess, entry, send_find)
        if entry.get("type") == "source":
            rows = db_get_sources_by_ids([entry["id"]])
            if not rows:
                return await query.message.reply_text("Не удалось найти исходник в базе.")
            return await _start_find_single_source(sess, rows[0], send_find)
        return await query.message.reply_text("Неизвестный тип результата.")

    if data == "find_retry":
        if not sess.get("find_mode"):
            return await query.answer("Сначала запустите «Найти».", show_alert=True)
        sess["state"] = "find_wait_term"
        sess["find_matches"] = []
        await query.answer("Введите другой фрагмент имени")
        return await query.message.reply_text(
            "Пришлите новый фрагмент имени PMV. Например, дату 20251207 или время 0734."
        )

    if data.startswith("ratepmv_select:"):
        if sess.get("state") != "ratepmv_choose_pmv":
            return await query.answer("Сейчас не жду выбор PMV", show_alert=True)
        try:
            idx = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Не понял номер", show_alert=True)
        pmv_rows: List[sqlite3.Row] = sess.get("pmv_rows") or []
        if not (1 <= idx <= len(pmv_rows)):
            return await query.answer("Некорректный номер PMV", show_alert=True)
        row = pmv_rows[idx - 1]
        name = Path(row["video_path"]).name
        sess["state"] = "ratepmv_wait_rating"
        sess["ratepmv_selected_idx"] = idx
        await query.answer("PMV выбрано")
        await query.message.reply_text(
            f"PMV №{idx}: {name}\nВыбери оценку 1-5:",
            reply_markup=build_ratepmv_score_keyboard(),
        )
        return

    if data.startswith("ratepmv_rate:"):
        if sess.get("state") != "ratepmv_wait_rating":
            return await query.answer("Сейчас не жду оценку", show_alert=True)
        try:
            rating = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Не понял оценку", show_alert=True)
        idx = int(sess.get("ratepmv_selected_idx") or 0)
        if idx <= 0:
            return await query.answer("Нет выбранного PMV", show_alert=True)

        async def send_from_query(message: str) -> None:
            await query.message.reply_text(message)

        await query.answer(f"Оценка: {rating}")
        await process_ratepmv_choice(sess, idx, rating, send_from_query)
        sess.pop("ratepmv_selected_idx", None)
        return

    if data.startswith("ratepmv_bulk:"):
        if sess.get("state") not in {"ratepmv_choose_pmv", "ratepmv_wait_rating"}:
            return await query.answer("Пакетная оценка сейчас недоступна", show_alert=True)
        try:
            rating = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Не понял оценку", show_alert=True)
        if rating < 1 or rating > 5:
            return await query.answer("Оценка должна быть 1-5", show_alert=True)

        pmv_rows: List[sqlite3.Row] = sess.get("pmv_rows") or []
        if not pmv_rows:
            return await query.answer("Нет списка PMV", show_alert=True)

        await query.answer("Применяю пакетную оценку…")

        pairs = [(idx + 1, rating) for idx in range(len(pmv_rows))]
        success_lines, error_lines = apply_pmv_rating_pairs(pmv_rows, pairs)

        if not success_lines:
            msg = "Не удалось применить пакетную оценку."
            if error_lines:
                msg += "\n" + "\n".join(error_lines)
            return await query.message.reply_text(msg)

        user_sessions.pop(user_id, None)

        lines = [
            f"✅ Все показанные PMV получили оценку {rating}/5: {len(success_lines)} шт.",
            "Успешно отмечены:",
        ]
        lines.extend(f"- {entry}" for entry in success_lines)
        if error_lines:
            lines.append("")
            lines.append("⚠️ Пропущены:")
            lines.extend(f"- {entry}" for entry in error_lines)
        lines.append("")
        lines.append("Оценки источников не ставились. Если нужно — выбери конкретный PMV отдельно.")
        await query.message.reply_text("\n".join(lines))
        return

    if data == "rategrp_from_pmv":
        if sess.get("state") != "rategrp_choose_orientation":
            return await query.answer("Эта опция доступна только в начале команды rategrp.", show_alert=True)

        async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await query.message.reply_text(msg, reply_markup=markup)

        success = await _start_rategrp_from_pmv(sess, send_rategrp)
        if success:
            await query.answer("Показываю исходники из PMV")
        else:
            await query.answer("Нет подходящих исходников", show_alert=True)
        return

    if data.startswith("musicprep_show:"):
        mode = data.split(":", 1)[1]
        show_used = mode == "used"
        sess["state"] = "musicprep_wait_track"
        text, keyboard = build_musicprep_track_keyboard(sess, show_used=show_used)
        try:
            await query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            await query.message.reply_text(text, reply_markup=keyboard)
        return await query.answer()

    if data.startswith("musicprep_track:"):
        tracks = sess.get("music_tracks") or {}
        token = data.split(":", 1)[1]
        info = tracks.get(token)
        if not info:
            return await query.answer("Трек не найден", show_alert=True)
        path = Path(info["path"])
        _, title = extract_track_title_components(path)
        prefix = slugify_token(title or path.stem)
        sess["musicprep_file"] = str(path)
        sess["musicprep_project_prefix"] = prefix
        sess.pop("musicprep_project_partial", None)
        sess["state"] = "musicprep_wait_seconds"
        await query.answer("Трек выбран")
        await query.message.reply_text(
            f"Трек выбран: {path.name}\n"
            f"Префикс проекта: {prefix}\n"
            "Выберите минимальную длительность сегмента:",
            reply_markup=build_musicprep_seconds_keyboard(),
        )
        return

    if data.startswith("musicprep_seconds:"):
        if sess.get("state") not in {"musicprep_wait_seconds", "musicprep_wait_mode"}:
            return await query.answer("Сначала выберите трек", show_alert=True)
        try:
            seconds = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Неверное значение", show_alert=True)
        prefix = sess.get("musicprep_project_prefix") or "project"
        partial = f"{prefix}_{seconds}"
        sess["musicprep_segment"] = float(seconds)
        sess["musicprep_project_partial"] = partial
        sess["state"] = "musicprep_wait_mode"
        await query.answer(f"{seconds} сек.")
        if seconds == 0:
            length_line = "Длина сегмента: авто (минимум не ограничен)."
        else:
            length_line = f"Длина сегмента: {seconds} сек."
        await query.message.reply_text(
            f"{length_line}\n"
            f"Имя проекта станет: {partial}_<algo>\n"
            "Выберите алгоритм сегментации:",
            reply_markup=build_musicprep_mode_keyboard(),
        )
        return

    if data.startswith("musicprep_mode:"):
        if sess.get("state") != "musicprep_wait_mode":
            return await query.answer("Сначала выберите сегменты", show_alert=True)

        mode = data.split(":", 1)[1]
        if mode not in {"beat", "onset", "uniform"}:
            return await query.answer("Неизвестный режим", show_alert=True)

        sess["musicprep_selected_mode"] = mode
        options = get_musicprep_sensitivity_options(mode)
        if options:
            sess["musicprep_sensitivity_options"] = options
            sess["state"] = "musicprep_wait_sensitivity"
            await query.answer("Алгоритм выбран")
            await query.message.reply_text(
                "Выберите чувствительность анализа:",
                reply_markup=build_musicprep_sensitivity_keyboard(mode),
            )
            return

        await query.answer("Запускаю генерацию…")

        async def send(msg: str) -> None:
            await query.message.reply_text(msg)

        return await finalize_musicprep_project(send, sess, user_id, mode)

    if data.startswith("musicprep_sens:"):
        if sess.get("state") != "musicprep_wait_sensitivity":
            return await query.answer("Сначала выберите алгоритм", show_alert=True)
        parts = data.split(":", 2)
        if len(parts) != 3:
            return await query.answer("Неверный параметр", show_alert=True)
        _, mode, key = parts
        options = get_musicprep_sensitivity_options(mode)
        selected = next((opt for opt in options if opt["key"] == key), None)
        if not selected:
            return await query.answer("Не удалось распознать вариант", show_alert=True)
        sess["state"] = None
        await query.answer("Чувствительность выбрана")

        async def send(msg: str) -> None:
            await query.message.reply_text(msg)

        return await finalize_musicprep_project(
            send, sess, user_id, mode, selected.get("analysis_kwargs") or {}
        )

    if data.startswith("musicprepcheck_project:"):
        if sess.get("state") != "musicprepcheck_wait_project":
            return await query.answer("Сначала запусти /musicprepcheck", show_alert=True)
        slug = data.split(":", 1)[1]
        projects_map: Dict[str, Dict[str, Any]] = sess.get("musicprepcheck_projects") or {}
        project = projects_map.get(slug)
        if not project:
            return await query.answer("Проект не найден", show_alert=True)
        await query.answer("Готовлю щелчки…")
        try:
            output_path = generate_musicprep_click_preview(project)
        except Exception as exc:
            return await query.message.reply_text(f"Не удалось создать MP3 со щелчками: {exc}")

        caption = f"Щелчки для проекта {project.get('name') or slug}"
        try:
            with output_path.open("rb") as fh:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=fh,
                    filename=output_path.name,
                    caption=caption,
                )
        except Exception:
            await query.message.reply_text(f"Файл сохранён: {output_path}")
        else:
            await query.message.reply_text(f"Готово. Файл: {output_path}")
        return

    if data.startswith("newcomp_show:"):
        mode = data.split(":", 1)[1]
        show_used = mode == "used"
        if show_used and not sess.get("music_projects_duration_filter"):
            async def send_duration(text: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
                try:
                    await query.edit_message_text(text, reply_markup=markup)
                except Exception:
                    await query.message.reply_text(text, reply_markup=markup)

            await prompt_newcomp_duration(sess, send_duration)
            await query.answer()
            return
        sess["state"] = "newcompmusic_wait_project"
        text, keyboard = build_newcomp_project_keyboard(sess, show_used=show_used)
        try:
            await query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            await query.message.reply_text(text, reply_markup=keyboard)
        return await query.answer()

    if data == "newcomp_bucket_menu":
        async def send_duration(text: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            try:
                await query.edit_message_text(text, reply_markup=markup)
            except Exception:
                await query.message.reply_text(text, reply_markup=markup)

        await prompt_newcomp_duration(sess, send_duration)
        return await query.answer()

    if data.startswith("newcomp_bucket:"):
        bucket = data.split(":", 1)[1]
        valid_keys = {key for key, _, _, _ in NEWCOMPMUSIC_DURATION_BUCKETS}
        if bucket not in valid_keys:
            return await query.answer("Неизвестная длительность", show_alert=True)
        sess["music_projects_duration_filter"] = bucket
        sess["state"] = "newcompmusic_wait_project"
        text, keyboard = build_newcomp_project_keyboard(sess, show_used=True)
        try:
            await query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            await query.message.reply_text(text, reply_markup=keyboard)
        return await query.answer("Фильтр обновлён")

    if data.startswith("newcomp_project:"):
        projects_map: Dict[str, Dict[str, Any]] = sess.get("music_projects_map") or {}
        token = data.split(":", 1)[1]
        chosen = projects_map.get(token)
        if not chosen:
            return await query.answer("Проект не найден", show_alert=True)

        manifest_data = chosen.get("manifest_data")
        manifest_path = chosen.get("manifest_path")
        if not manifest_data and manifest_path and Path(manifest_path).exists():
            try:
                manifest_data = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
                chosen["manifest_data"] = manifest_data
            except Exception as exc:
                return await query.answer(f"Ошибка manifest.json: {exc}", show_alert=True)

        parsed_segments = parse_manifest_segments(manifest_data or {})
        total_duration = chosen.get("duration")
        if total_duration is None and parsed_segments:
            total_duration = parsed_segments[-1].end
        seg_count = len(parsed_segments)
        minutes = (total_duration / 60.0) if total_duration else None

        groups = get_source_groups_prefer_unused()
        if not groups:
            return await query.answer("Нет групп исходников. Сначала /scan.", show_alert=True)

        sess["state"] = "newcompmusic_choose_orientation"
        sess["music_selected"] = {
            "slug": chosen.get("slug"),
            "name": chosen.get("name"),
            "duration": total_duration,
            "segments": seg_count,
            "manifest": manifest_data,
            "audio_path": str(chosen.get("audio_path")) if chosen.get("audio_path") else None,
            "parsed_segments": parsed_segments,
        }
        group_entries = [
            SourceGroupEntry(key=key, rows=list(rows), unused_count=unused_count)
            for key, rows, unused_count in groups
        ]
        sorted_entries, orientation_map = sort_group_entries_with_orientation(group_entries)
        sess["music_group_orientations"] = orientation_map
        sess["music_groups_all"] = [
            (entry.key, entry.rows, entry.unused_count) for entry in sorted_entries
        ]
        sess["music_groups"] = []
        sess["music_orientation_preference"] = None

        lines = [
            f"Выбран проект: {chosen['name']} (slug: {chosen['slug']}).",
            f"Смен клипов: {seg_count}",
        ]
        if minutes:
            lines.append(f"Продолжительность ≈ {minutes:.1f} минут.")
        lines.append("")
        lines.append("Выберите ориентацию исходников: VR, HOR или VER.")

        await query.answer("Проект выбран")
        await query.message.reply_text(
            "\n".join(lines),
            reply_markup=build_newcomp_orientation_keyboard(),
        )
        return

    if data.startswith("newcomp_orient:"):
        if sess.get("state") != "newcompmusic_choose_orientation":
            return await query.answer("Сначала выберите проект", show_alert=True)
        target = data.split(":", 1)[1].upper()
        if target not in NEWCOMPMUSIC_ORIENTATION_CHOICES:
            return await query.answer("Неизвестный режим", show_alert=True)
        all_groups = sess.get("music_groups_all") or []
        if not all_groups:
            return await query.answer("Список групп пуст. Запустите команду заново.", show_alert=True)
        orientation_map = sess.get("music_group_orientations") or {}
        filtered = filter_groups_by_orientation(all_groups, orientation_map, target)
        if not filtered:
            return await query.answer("Нет групп в этой ориентации.", show_alert=True)
        sess["music_orientation_preference"] = target
        sess["music_groups"] = filtered
        sess["state"] = "newcompmusic_wait_group"
        group_entries = [
            SourceGroupEntry(key=key, rows=list(rows), unused_count=unused)
            for key, rows, unused in filtered
        ]
        msg_lines = _build_group_selection_lines(
            sess, group_entries, target, prompt_kind="inline"
        )
        await query.answer("Ориентация выбрана")
        await query.message.reply_text(
            "\n".join(msg_lines),
            reply_markup=build_numeric_keyboard("newcomp_group", len(filtered)),
        )
        return

    if data.startswith("rategrp_orient:"):
        if sess.get("state") != "rategrp_choose_orientation":
            return await query.answer("Сначала запустите /rategrp", show_alert=True)
        target = data.split(":", 1)[1].upper()
        if target not in NEWCOMPMUSIC_ORIENTATION_CHOICES:
            return await query.answer("Неизвестный режим", show_alert=True)
        all_groups = sess.get("rategrp_groups_all") or []
        if not all_groups:
            return await query.answer("Нет доступных групп. Запустите /rategrp заново.", show_alert=True)
        orientation_map = sess.get("rategrp_group_orientations") or {}
        filtered = filter_groups_by_orientation(all_groups, orientation_map, target)
        if not filtered:
            return await query.answer("Нет групп в этой ориентации.", show_alert=True)
        sess["rategrp_orientation_preference"] = target
        sess["rategrp_groups"] = filtered
        sess["state"] = "rategrp_choose_group"
        group_entries = [
            SourceGroupEntry(key=key, rows=list(rows), unused_count=unused)
            for key, rows, unused in filtered
        ]
        msg_lines = format_rategrp_group_prompt(sess, group_entries, target, prompt_kind="inline")
        await query.answer("Ориентация выбрана")
        await query.message.reply_text(
            "\n".join(msg_lines),
            reply_markup=build_numeric_keyboard("rategrp_group", len(filtered)),
        )
        return

    if data.startswith("rategrp_group:"):
        if sess.get("state") != "rategrp_choose_group":
            return await query.answer("Сначала выберите ориентацию", show_alert=True)
        try:
            idx = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Неверный номер группы", show_alert=True)
        groups = sess.get("rategrp_groups") or []
        if not (1 <= idx <= len(groups)):
            return await query.answer("Нет группы с таким номером", show_alert=True)
        key, rows, _ = groups[idx - 1]
        rows = [dict(row) for row in rows]
        orientation_label = (sess.get("rategrp_group_orientations") or {}).get(key)
        if not orientation_label:
            orientation_label = _resolution_orientation(key[1] or "")[0]
        sess["rategrp_group_choice"] = {
            "key": key,
            "label": f"{key[0]} {key[1]}",
            "orientation": orientation_label,
        }
        sess["rategrp_rerate_rows"] = rows
        queue = _prepare_rategrp_queue(rows)
        if not queue:
            available = _rategrp_available_colors(rows)
            if not available:
                return await query.answer("В этой группе нет исходников.", show_alert=True)
            sess["state"] = "rategrp_choose_rerate_color"
            await query.answer("Выберите цвет для переоценки")

            async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
                await query.message.reply_text(msg, reply_markup=markup)

            await send_rategrp(
                f"Группа {key[0]} {key[1]} выбрана. Новых исходников нет, выберите цвет для переоценки:",
                build_rategrp_rerate_keyboard(available),
            )
            return
        sess["rategrp_queue"] = queue
        sess["rategrp_total"] = len(queue)
        sess["rategrp_processed"] = 0
        sess["rategrp_queue_origin"] = "unrated"
        sess["state"] = "rategrp_rate_source"

        async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await query.message.reply_text(msg, reply_markup=markup)

        await query.answer("Группа выбрана")
        await send_rategrp(f"Группа {key[0]} {key[1]} выбрана. Неоценённых исходников: {len(queue)}.")
        return await rategrp_send_next_prompt(sess, send_rategrp)

    if data.startswith("rategrp_color:"):
        if sess.get("state") != "rategrp_rate_source":
            return await query.answer("Сначала выберите группу", show_alert=True)
        color_key = data.split(":", 1)[1]
        choice = RATEGRP_COLOR_CHOICES.get(color_key)
        if not choice:
            return await query.answer("Неизвестный цвет", show_alert=True)

        async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await query.message.reply_text(msg, reply_markup=markup)

        await query.answer(f"Отмечено {choice['emoji']}")
        return await rategrp_apply_rating(sess, color_key, send_rategrp)

    if data.startswith("rategrp_rerate_color:"):
        if sess.get("state") != "rategrp_choose_rerate_color":
            return await query.answer("Сначала выберите группу", show_alert=True)
        color_key = data.split(":", 1)[1]

        async def send_rategrp(msg: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await query.message.reply_text(msg, reply_markup=markup)

        success = await _rategrp_start_rerate(sess, color_key, send_rategrp)
        if success:
            await query.answer("Запускаю переоценку")
        else:
            await query.answer("Не удалось начать переоценку", show_alert=True)
        return

    if data == "rategrp_rerate_back":
        if sess.get("state") != "rategrp_choose_rerate_color":
            return await query.answer("Сначала выберите группу", show_alert=True)
        sess["state"] = "rategrp_choose_group"
        groups = sess.get("rategrp_groups") or []
        orientation = sess.get("rategrp_orientation_preference") or "?"
        group_entries = [
            SourceGroupEntry(key=key, rows=list(rows), unused_count=unused) for key, rows, unused in groups
        ]
        lines = format_rategrp_group_prompt(sess, group_entries, orientation, prompt_kind="inline")
        keyboard = build_numeric_keyboard("rategrp_group", len(groups)) if groups else None
        await query.message.reply_text("\n".join(lines), reply_markup=keyboard)
        return await query.answer("Выберите другую группу")

    if data.startswith("newcomp_group:"):
        if sess.get("state") != "newcompmusic_wait_group":
            return await query.answer("Сначала выберите проект", show_alert=True)
        try:
            idx = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Неверный номер группы", show_alert=True)
        groups: List[Tuple[Tuple[str, str], List[sqlite3.Row], int]] = sess.get("music_groups") or []
        if not (1 <= idx <= len(groups)):
            return await query.answer("Нет группы с таким номером", show_alert=True)
        key, rows, unused_count = groups[idx - 1]
        if not rows:
            return await query.answer("Группа пустая", show_alert=True)

        orientation_label = (sess.get("music_group_orientations") or {}).get(key)
        if not orientation_label:
            orientation_label = _resolution_orientation(key[1] or "")[0]
        sess["music_group_choice"] = {
            "key": key,
            "count": len(rows),
            "orientation": orientation_label,
            "total_count": len(rows),
            "unused_count": unused_count,
            "group_number": idx,
        }
        sess["music_group_rows"] = list(rows)
        sess["music_folder_only_new"] = False
        sess.pop("music_color_rows", None)
        sess["state"] = "newcompmusic_choose_groupmode"

        await query.answer("Группа выбрана")
        await query.message.reply_text(
            "Группа выбрана. Как будем группировать исходники?",
            reply_markup=build_newcomp_groupmode_keyboard(),
        )
        return

    if data.startswith("newcomp_folder:"):
        if sess.get("state") != "newcompmusic_wait_folder":
            return await query.answer("Сначала выберите группу", show_alert=True)
        token = data.split(":", 1)[1]
        try:
            count, label = apply_newcomp_folder_choice(
                sess, token, next_state="newcompmusic_wait_sources"
            )
        except ValueError as exc:
            return await query.answer(str(exc), show_alert=True)

        choice = sess.get("music_group_choice") or {}
        codec, res = choice.get("key") or ("?", "?")
        project_info = sess.get("music_selected") or {}
        segs = project_info.get("segments")
        duration = project_info.get("duration")
        duration_minutes = (duration / 60.0) if duration else None

        msg_lines = [
            f"Папка выбрана: {label} (исходников: {count}).",
            f"Группа: {codec} {res}.",
        ]
        if duration_minutes:
            msg_lines.append(f"Продолжительность проекта ≈ {duration_minutes:.1f} минут.")
        if segs is not None:
            msg_lines.append(f"Смен клипов: {segs}.")
        msg_lines.append("Выберите, сколько исходников задействовать:")

        await query.answer("Папка выбрана")
        await query.message.reply_text(
            "\n".join(msg_lines),
            reply_markup=build_newcomp_sources_keyboard(),
        )
        return

    if data == "newcomp_folder_back":
        if sess.get("state") != "newcompmusic_wait_folder":
            return await query.answer("Сначала выберите группу", show_alert=True)
        groups = sess.get("music_groups") or []
        if not groups:
            return await query.answer("Нет списка групп. Запустите заново.", show_alert=True)
        sess["state"] = "newcompmusic_wait_group"
        sess.pop("music_group_choice", None)
        sess.pop("music_group_rows", None)
        sess.pop("music_color_rows", None)
        sess["music_folder_only_new"] = False
        orientation_label = sess.get("music_orientation_preference") or "?"
        group_entries = [
            SourceGroupEntry(key=key, rows=list(rows), unused_count=unused)
            for key, rows, unused in groups
        ]
        msg_lines = _build_group_selection_lines(
            sess, group_entries, orientation_label, prompt_kind="inline"
        )
        await query.answer("Выберите другую группу")
        await query.message.reply_text(
            "\n".join(msg_lines),
            reply_markup=build_numeric_keyboard("newcomp_group", len(groups)),
        )
        return

    if data.startswith("newcomp_groupmode:"):
        if sess.get("state") != "newcompmusic_choose_groupmode":
            return await query.answer("Сначала выберите группу", show_alert=True)
        mode = data.split(":", 1)[1]
        rows = sess.get("music_group_rows") or []
        if not rows:
            return await query.answer("Не удалось загрузить группу.", show_alert=True)
        if mode == "folders":
            available = _count_rows_for_folder_mode(rows, False)
            if available == 0:
                return await query.answer("В выбранной группе нет исходников.", show_alert=True)
            sess["music_color_rows"] = None
            sess["music_color_choice"] = None
            sess["music_color_autotag"] = None
            group_choice = sess.get("music_group_choice") or {}
            total = group_choice.get("total_count")
            if total:
                group_choice["count"] = total
            sess["music_group_choice"] = group_choice
            sess["music_folder_only_new"] = False
            sess["state"] = "newcompmusic_wait_folder"
            msg_text, keyboard = compose_newcomp_folder_prompt(sess)
            await query.answer("Показываю папки")
            await query.message.reply_text(msg_text, reply_markup=keyboard)
            return
        if mode == "colors":
            counts, unrated = _compute_rategrp_color_counts(rows)
            total_colored = sum(counts.values())
            if total_colored == 0:
                return await query.answer("В этой группе нет исходников с оценками.", show_alert=True)
            green_emoji = RATEGRP_COLOR_CHOICES["green"]["emoji"]
            yellow_emoji = RATEGRP_COLOR_CHOICES["yellow"]["emoji"]
            red_emoji = RATEGRP_COLOR_CHOICES["red"]["emoji"]
            combo_counts = {
                "green_new": len(_filter_green_new_rows(rows)),
                "green_yellow": len(
                    _filter_rows_by_color(rows, {green_emoji, yellow_emoji}, include_unrated=False)
                ),
                "green_yellow_red": len(
                    _filter_rows_by_color(
                        rows,
                        {green_emoji, yellow_emoji, red_emoji},
                        include_unrated=False,
                    )
                ),
            }
            sess["state"] = "newcompmusic_choose_color"
            await query.answer("Выберите цвет")
            await query.message.reply_text(
                "Выберите цвет оценки:",
                reply_markup=build_newcomp_color_keyboard(counts, unrated, combo_counts),
            )
            return
        return await query.answer("Неизвестный режим", show_alert=True)

    if data == "newcomp_color_back":
        if sess.get("state") != "newcompmusic_choose_color":
            return await query.answer("Сначала выберите режим", show_alert=True)
        sess["state"] = "newcompmusic_choose_groupmode"
        sess.pop("music_color_rows", None)
        sess.pop("music_color_choice", None)
        sess.pop("music_color_autotag", None)
        group_choice = sess.get("music_group_choice") or {}
        total = group_choice.get("total_count")
        if total:
            group_choice["count"] = total
        sess["music_group_choice"] = group_choice
        await query.answer("Возвращаю выбор")
        await query.message.reply_text(
            "Как будем группировать исходники?",
            reply_markup=build_newcomp_groupmode_keyboard(),
        )
        return

    if data.startswith("newcomp_color:"):
        if sess.get("state") != "newcompmusic_choose_color":
            return await query.answer("Сначала выберите режим", show_alert=True)
        color_key = data.split(":", 1)[1]
        rows = sess.get("music_group_rows") or []
        choice = RATEGRP_COLOR_CHOICES.get(color_key)
        include_unrated = False
        allowed: Set[str] = set()
        if choice:
            emoji = choice["emoji"]
            allowed = {emoji}
            filtered = _filter_rows_by_color(rows, allowed, include_unrated=False)
        elif color_key == "green_new":
            allowed = {RATEGRP_COLOR_CHOICES["green"]["emoji"]}
            filtered = _filter_green_new_rows(rows)
            include_unrated = True
        elif color_key == "green_yellow":
            allowed = {
                RATEGRP_COLOR_CHOICES["green"]["emoji"],
                RATEGRP_COLOR_CHOICES["yellow"]["emoji"],
            }
            filtered = _filter_rows_by_color(rows, allowed, include_unrated=False)
        elif color_key == "green_yellow_red":
            allowed = {
                RATEGRP_COLOR_CHOICES["green"]["emoji"],
                RATEGRP_COLOR_CHOICES["yellow"]["emoji"],
                RATEGRP_COLOR_CHOICES["red"]["emoji"],
            }
            filtered = _filter_rows_by_color(rows, allowed, include_unrated=False)
        else:
            return await query.answer("Неизвестный цвет", show_alert=True)
        emoji_label = (
            choice["emoji"]
            if choice
            else {
                "green_new": f"{RATEGRP_COLOR_CHOICES['green']['emoji']}+🆕",
                "green_yellow": f"{RATEGRP_COLOR_CHOICES['green']['emoji']}+{RATEGRP_COLOR_CHOICES['yellow']['emoji']}",
                "green_yellow_red": f"{RATEGRP_COLOR_CHOICES['green']['emoji']}+{RATEGRP_COLOR_CHOICES['yellow']['emoji']}+{RATEGRP_COLOR_CHOICES['red']['emoji']}",
            }.get(color_key, "?")
        )
        if not filtered:
            return await query.answer("Нет исходников с такой оценкой.", show_alert=True)
        sess["music_color_rows"] = filtered
        sess["music_color_choice"] = emoji_label
        autotag = None
        if include_unrated:
            autotag_emoji = RATEGRP_COLOR_CHOICES["green"]["emoji"]
            autotag_ids: List[int] = []
            for row in filtered:
                if _rategrp_row_color(row) is None:
                    try:
                        autotag_ids.append(int(row["id"]))
                    except Exception:
                        continue
            if autotag_ids:
                autotag = {"emoji": autotag_emoji, "ids": autotag_ids}
        else:
            autotag = None
        sess["music_color_autotag"] = autotag
        group_choice = sess.get("music_group_choice") or {}
        group_choice["count"] = len(filtered)
        sess["music_group_choice"] = group_choice
        sess["state"] = "newcompmusic_wait_sources"
        codec, res = group_choice.get("key") or ("?", "?")
        msg_lines = [
            f"Выбран цвет {emoji_label}: {len(filtered)} исходников.",
            f"Группа: {codec} {res}.",
            "Сколько исходников задействовать? Пришлите число или выберите на клавиатуре.",
        ]
        await query.answer("Цвет выбран")
        await query.message.reply_text(
            "\n".join(msg_lines),
            reply_markup=build_newcomp_sources_keyboard(),
        )
        return

    if data.startswith("newcomp_folder_mode:"):
        if sess.get("state") != "newcompmusic_wait_folder":
            return await query.answer("Сначала выберите группу", show_alert=True)
        mode = data.split(":", 1)[1]
        target_unused_only = mode == "new"
        current = bool(sess.get("music_folder_only_new"))
        if target_unused_only == current:
            return await query.answer("Этот режим уже активен.")
        rows = sess.get("music_group_rows") or []
        if not rows:
            return await query.answer("Не удалось загрузить исходники группы.", show_alert=True)
        total_count = _count_rows_for_folder_mode(rows, target_unused_only)
        if target_unused_only and total_count == 0:
            return await query.answer("Новых исходников в этой группе нет.", show_alert=True)
        sess["music_folder_only_new"] = target_unused_only
        msg_text, keyboard = compose_newcomp_folder_prompt(sess)
        notice = "Показываю только новые." if target_unused_only else "Возвращаю все исходники."
        await query.answer(notice)
        await query.message.reply_text(msg_text, reply_markup=keyboard)
        return

    if data.startswith("newcomp_sources:"):
        if sess.get("state") != "newcompmusic_wait_sources":
            return await query.answer("Сначала выберите группу", show_alert=True)
        try:
            count = int(data.split(":", 1)[1])
        except ValueError:
            return await query.answer("Неверное число", show_alert=True)
        available = int((sess.get("music_group_choice") or {}).get("count") or 0)
        info_line = None
        if available and count > available:
            count = available
            info_line = _source_limit_message(sess, available)
        sess["music_sources"] = count
        sess["state"] = "newcompmusic_wait_algo"

        await query.answer("Количество выбрано" if not info_line else "Берём максимум")
        algo_desc = ", ".join(f"{meta['short']} ({meta['title']})" for meta in CLIP_SEQUENCE_ALGORITHMS.values())
        msg_lines = []
        if info_line:
            msg_lines.append(info_line)
        msg_lines.append(f"Ок, возьмём {count} исходников.")
        msg_lines.append(
            f"Выберите метод рандомизации клипов: {algo_desc}",
        )
        await query.message.reply_text(
            "\n".join(msg_lines),
            reply_markup=build_newcomp_algo_keyboard(),
        )
        return

    if data.startswith("newcomp_algo:"):
        if sess.get("state") != "newcompmusic_wait_algo":
            return await query.answer("Сначала выберите количество исходников", show_alert=True)
        short = data.split(":", 1)[1]
        resolved_key = normalize_clip_algo_choice(short)
        if not resolved_key:
            return await query.answer("Неизвестный алгоритм", show_alert=True)

        async def send_from_query(message: str) -> None:
            await query.message.reply_text(message)

        await query.answer("Генерация…")
        return await run_newcompmusic_generation(send_from_query, sess, resolved_key, user_id)

    await query.answer("Неизвестная кнопка", show_alert=True)


async def cmd_badfiles(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    rows = db_get_problem_sources()
    if not rows:
        return await update.message.reply_text("Проблемные файлы не обнаружены.")

    lines = ["⚠️ Проблемные исходники:"]
    for r in rows:
        lines.append(f"- id={r['id']}: {r['video_name']} — {r['video_path']}")
    await update.message.reply_text("\n".join(lines))


async def cmd_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    global CURRENT_STRATEGY

    args = context.args
    if not args:
        return await update.message.reply_text(
            "Текущая стратегия: " + CURRENT_STRATEGY + "\nДоступные: " + ", ".join(ALLOWED_STRATEGIES)
        )

    name = args[0].strip().lower()
    if name not in ALLOWED_STRATEGIES:
        return await update.message.reply_text(
            "Неизвестная стратегия. Доступные: " + ", ".join(ALLOWED_STRATEGIES)
        )

    CURRENT_STRATEGY = name
    return await update.message.reply_text(f"Ок, стратегия установлена: {CURRENT_STRATEGY}")


async def cmd_videofx(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    global GLITCH_EFFECTS_PER_VIDEO, TRANSITION_EFFECTS_PER_VIDEO

    args = context.args
    if not args:
        return await update.message.reply_text(
            "Текущие визуальные эффекты:\n"
            f"- глитч-вставок: {GLITCH_EFFECTS_PER_VIDEO}\n"
            f"- переходов: {TRANSITION_EFFECTS_PER_VIDEO}\n\n"
            "Используйте /videofx <глитчей> <переходов>, например /videofx 6 3."
        )

    try:
        glitches = max(0, int(args[0]))
        transitions = max(
            0, int(args[1]) if len(args) > 1 else TRANSITION_EFFECTS_PER_VIDEO
        )
    except ValueError:
        return await update.message.reply_text(
            "Нужно указать целые числа: /videofx <глитчей> <переходов>"
        )

    GLITCH_EFFECTS_PER_VIDEO = glitches
    TRANSITION_EFFECTS_PER_VIDEO = transitions
    return await update.message.reply_text(
        f"Готово. Глитчей: {GLITCH_EFFECTS_PER_VIDEO}, переходов: {TRANSITION_EFFECTS_PER_VIDEO}."
    )



async def cmd_ratepmv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    rows = db_get_all_compilations()
    if not rows:
        return await update.message.reply_text("Похоже, пока нет готовых PMV и оценивать нечего.")

    unrated = []
    for r in rows:
        comments = (r["comments"] or "").lower()
        if "pmv_rating=" not in comments:
            unrated.append(r)

    if not unrated:
        return await update.message.reply_text("Все PMV уже получили оценки! 🔥")

    unrated_sorted = sorted(
        unrated,
        key=lambda row: ((row["pmv_date"] or ""), int(row["id"] or 0)),
    )
    display_rows = unrated_sorted[:10]
    remaining = max(0, len(unrated_sorted) - len(display_rows))

    lines = [f"Без оценки осталось PMV: {len(unrated_sorted)}."]
    if remaining > 0:
        lines.append(f"Показываю 10 самых старых, ещё ждут своей очереди: {remaining}.")
    lines.append("")
    for idx, r in enumerate(display_rows, 1):
        name = Path(r["video_path"]).name
        date_str = r["pmv_date"]
        lines.append(f"{idx}. {name} (дата: {date_str}, id={r['id']})")

    lines.append("")
    lines.append("Можешь выбрать PMV кнопкой или прислать текстом `<номер> <оценка 1-5>` (пример: `2 5`).")
    lines.append("Для пакетной оценки отправь несколько пар подряд: `<1 5 2 4 3 5>` или нажми кнопку `-> всем` со снизу клавиатуры.")

    user_sessions[update.effective_user.id] = {
        "state": "ratepmv_choose_pmv",
        "pmv_rows": display_rows,
    }

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=build_ratepmv_pmv_keyboard(display_rows),
    )




def get_source_groups_prefer_unused() -> List[Tuple[Tuple[str, str], List[sqlite3.Row], int]]:
    unused = db_get_unused_sources_grouped()
    all_groups = db_get_all_sources_grouped()
    all_keys = set(all_groups.keys()) | set(unused.keys())
    entries: List[SourceGroupEntry] = []

    for key in all_keys:
        rows_all = all_groups.get(key) or []
        if len(rows_all) <= 5:
            continue
        unused_count = len(unused.get(key) or [])
        entries.append(SourceGroupEntry(key=key, rows=list(rows_all), unused_count=unused_count))

    sorted_entries = sort_source_group_entries(entries)
    return [(entry.key, entry.rows, entry.unused_count) for entry in sorted_entries]


async def cmd_newcompmusic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    projects = load_music_projects()
    if not projects:
        return await update.message.reply_text(
            "Папка music_projects пуста. Сначала создайте проекты через music_guided_generator.py."
        )

    project_map: Dict[str, Dict[str, Any]] = {}
    unused_tokens: List[str] = []
    used_tokens: List[str] = []
    for idx, proj in enumerate(projects, 1):
        token = f"mp{idx}"
        project_map[token] = proj
        if proj.get("usage_count"):
            used_tokens.append(token)
        else:
            unused_tokens.append(token)

    session_payload = {
        "state": "newcompmusic_wait_project",
        "music_projects_map": project_map,
        "music_projects_unused": unused_tokens,
        "music_projects_used": used_tokens,
        "music_projects": projects,
        "music_projects_duration_filter": None,
    }
    user_sessions[update.effective_user.id] = session_payload

    show_used = not unused_tokens and bool(used_tokens)
    if show_used and not session_payload.get("music_projects_duration_filter"):
        async def send_duration(text: str, markup: Optional[InlineKeyboardMarkup] = None) -> None:
            await update.message.reply_text(text, reply_markup=markup)

        await prompt_newcomp_duration(session_payload, send_duration)
        return

    text, keyboard = build_newcomp_project_keyboard(session_payload, show_used=show_used)
    await update.message.reply_text(text, reply_markup=keyboard)


async def cmd_randompmv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    user_sessions[update.effective_user.id] = {
        "state": "randompmv_wait_count",
    }
    msg = (
        "CreateRandomPMV: выбери, сколько проектов сгенерировать (5–30), "
        "а затем задай минимум новых источников (0–60) для каждого PMV. "
        "Бот автоматически подберёт группы и алгоритмы под эти требования."
    )
    await update.message.reply_text(msg, reply_markup=build_randompmv_count_keyboard())


async def cmd_rategrp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    groups = get_source_groups_prefer_unused()
    if not groups:
        return await update.message.reply_text("Нет групп исходников. Сначала просканируйте /scan.")

    group_entries = [
        SourceGroupEntry(key=key, rows=list(rows), unused_count=unused_count)
        for key, rows, unused_count in groups
    ]
    sorted_entries, orientation_map = sort_group_entries_with_orientation(group_entries)

    session_payload = {
        "state": "rategrp_choose_orientation",
        "rategrp_group_orientations": orientation_map,
        "rategrp_groups_all": [
            (entry.key, entry.rows, entry.unused_count) for entry in sorted_entries
        ],
        "rategrp_groups": [],
        "rategrp_orientation_preference": None,
    }
    user_sessions[update.effective_user.id] = session_payload

    lines = [
        "Команда rategrp: оценка отдельных исходников цветами.",
        "Сначала выберите ориентацию исходников: VR, HOR или VER,",
        "или нажмите «ИЗ PMV», чтобы получить случайные неоценённые исходники из свежих PMV.",
    ]
    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=build_rategrp_orientation_keyboard(),
    )


async def cmd_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    user_sessions[update.effective_user.id] = {
        "state": "reports_wait_choice",
    }
    await update.message.reply_text(
        "Выберите отчёт по цвету и группам.",
        reply_markup=build_reports_keyboard(),
    )


async def cmd_find(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)
    user_id = update.effective_user.id
    user_sessions[user_id] = {
        "state": "find_wait_term",
        "find_mode": True,
        "find_matches": [],
    }
    await update.message.reply_text(
        "Пришлите часть имени файла, даты или отметки времени. "
        "Я найду совпадения среди готовых PMV и среди исходников.",
    )


async def cmd_musicprep(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    files = list_music_input_files()
    if not files:
        return await update.message.reply_text(
            f"Папка {MUSIC_INPUT_DIR} пуста. Добавьте туда MP3/FLAC/M4A и повторите команду."
        )

    usage_map = collect_music_track_usage()
    track_map: Dict[str, Dict[str, Any]] = {}
    unused_tokens: List[str] = []
    used_tokens: List[str] = []

    for idx, path in enumerate(sorted(files, key=lambda p: p.name.lower()), 1):
        token = f"mt{idx}"
        norm = _normalize_path_str(path)
        count = usage_map.get(norm, 0)
        track_map[token] = {
            "path": str(path),
            "usage": count,
        }
        if count:
            used_tokens.append(token)
        else:
            unused_tokens.append(token)

    session_payload = {
        "state": "musicprep_wait_track",
        "music_tracks": track_map,
        "music_tracks_unused": unused_tokens,
        "music_tracks_used": used_tokens,
    }
    user_sessions[update.effective_user.id] = session_payload

    show_used = not unused_tokens and bool(used_tokens)
    text, keyboard = build_musicprep_track_keyboard(session_payload, show_used=show_used)
    await update.message.reply_text(text, reply_markup=keyboard)


async def cmd_musicprepcheck(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    projects = load_music_projects()
    if not projects:
        return await update.message.reply_text(
            "Папка music_projects пуста. Сначала создайте проекты через music_guided_generator.py."
        )

    project_map: Dict[str, Dict[str, Any]] = {}
    for proj in projects:
        slug = proj.get("slug") or sanitize_filename(proj.get("name") or "project")
        project_map[slug] = proj
    keyboard = build_musicprepcheck_keyboard(projects)
    user_sessions[update.effective_user.id] = {
        "state": "musicprepcheck_wait_project",
        "musicprepcheck_projects": project_map,
    }

    await update.message.reply_text(
        "Выберите проект, и я сгенерирую MP3 со щелчками по сегментам анализа.",
        reply_markup=keyboard,
    )


async def cmd_move2oculus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not check_access(update):
        return await unauthorized(update)

    script_path = SCRIPT_DIR / "move2oculus.py"
    if not script_path.exists():
        return await update.message.reply_text("move2oculus.py не найден рядом со скриптом.")

    await update.message.reply_text("Запускаю синхронизацию с Oculus. Подождите, это может занять несколько минут...")

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout_lines: List[str] = []
    stderr_lines: List[str] = []
    chunk_size = 3500
    output_queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue()

    async def send_chunks(msg: str) -> None:
        if not msg:
            return
        for i in range(0, len(msg), chunk_size):
            await update.message.reply_text(msg[i : i + chunk_size])

    async def read_stream(stream, label: str, collector: List[str]) -> None:
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode(errors="ignore").rstrip()
            if not text:
                continue
            collector.append(text)
            await output_queue.put((label, text))

    async def pump_output() -> None:
        while True:
            label, text = await output_queue.get()
            prefix = "STDOUT" if label == "stdout" else "STDERR"
            await send_chunks(f"{prefix}: {text}")
            output_queue.task_done()

    stdout_task = asyncio.create_task(read_stream(process.stdout, "stdout", stdout_lines))
    stderr_task = asyncio.create_task(read_stream(process.stderr, "stderr", stderr_lines))
    pump_task = asyncio.create_task(pump_output())

    await process.wait()
    await stdout_task
    await stderr_task
    await output_queue.join()
    pump_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await pump_task

    returncode = process.returncode or 0
    if returncode == 0:
        header = "✅ Синхронизация завершена."
    else:
        header = f"❌ Ошибка (код {returncode})."

    tail_stdout = "\n".join(stdout_lines[-20:])
    tail_stderr = "\n".join(stderr_lines[-20:])
    parts = [header]
    if tail_stdout:
        parts.append("Последние сообщения:\n" + tail_stdout)
    if tail_stderr:
        parts.append("STDERR (последние строки):\n" + tail_stderr)
    await send_chunks("\n\n".join(parts))


# =========================
# MAIN
# =========================

def main() -> None:
    print(f"Запуск PMV Telegram Bot {BUILD_NAME}")
    init_db()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("addfolder", cmd_addfolder))
    app.add_handler(CommandHandler("folders", cmd_folders))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CommandHandler("scanignore", cmd_scanignore))
    app.add_handler(CommandHandler("pmvnew", cmd_pmvnew))
    app.add_handler(CommandHandler("ratepmv", cmd_ratepmv))
    app.add_handler(CommandHandler("badfiles", cmd_badfiles))
    app.add_handler(CommandHandler("strategy", cmd_strategy))
    app.add_handler(CommandHandler("videofx", cmd_videofx))
    app.add_handler(CommandHandler("pmvold", cmd_pmvold))
    app.add_handler(CommandHandler("compmv", cmd_compmv))
    app.add_handler(CommandHandler("comvid", cmd_comvid))
    app.add_handler(CommandHandler("lookcom", cmd_lookcom))
    app.add_handler(CommandHandler("autocreate", cmd_autocreate))
    app.add_handler(CommandHandler("newcompmusic", cmd_newcompmusic))
    app.add_handler(CommandHandler("createrandompmv", cmd_randompmv))
    app.add_handler(CommandHandler("rategrp", cmd_rategrp))
    app.add_handler(CommandHandler("reports", cmd_reports))
    app.add_handler(CommandHandler("find", cmd_find))
    app.add_handler(CommandHandler("musicprep", cmd_musicprep))
    app.add_handler(CommandHandler("musicprepcheck", cmd_musicprepcheck))
    app.add_handler(CommandHandler("move2oculus", cmd_move2oculus))
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))


    app.run_polling()


if __name__ == "__main__":
    main()
