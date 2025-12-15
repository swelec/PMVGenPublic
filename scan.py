from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union
import shutil


PathLike = Union[str, Path]
RowLike = Union[Dict[str, Any], Any]


@dataclass
class ScanEnvironment:
    """
    Объект с зависимостями, необходимыми для сканирования исходников.
    Позволяет вынести тяжёлую бизнес-логику из Telegram-команд.
    """

    default_exts: Set[str]
    normalize_path_str: Callable[[PathLike], str]
    normalize_path_prefix: Callable[[PathLike], str]
    is_path_under_prefixes: Callable[[PathLike, Iterable[str]], bool]
    combine_comments: Callable[[Optional[str], Optional[str]], str]
    merge_pmv_lists: Callable[[Optional[str], Optional[str]], str]
    video_info_sort: Callable[[Path], Tuple[str, str]]
    db_get_sources_full: Callable[[], Sequence[RowLike]]
    db_update_source_fields: Callable[..., None]
    db_insert_source: Callable[..., Optional[int]]
    db_delete_sources_by_ids: Callable[[Iterable[int]], int]
    db_path: Path
    backup_dir: Path


def run_scan(
    upload_folders: Sequence[RowLike],
    ignored_rows: Sequence[RowLike],
    env: ScanEnvironment,
) -> Tuple[List[str], Dict[str, int]]:
    """
    Основной обработчик сканирования.

    Возвращает список строк-результатов для пользователя и подробную статистику.
    """

    backup_lines: List[str] = []
    if env.db_path.exists():
        try:
            env.backup_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%y%m%d_%H%M")
            backup_path = env.backup_dir / f"bd_backup_{stamp}.db"
            shutil.copy2(env.db_path, backup_path)
            backup_lines.append(f"💾 Резервная копия БД: {backup_path}")
        except Exception as exc:
            backup_lines.append(f"⚠️ Не удалось создать бэкап БД: {exc}")
    else:
        backup_lines.append("⚠️ Файл базы данных не найден, пропускаю бэкап.")

    ignore_prefixes = [
        env.normalize_path_prefix(row["folder_path"])
        for row in ignored_rows
        if row.get("folder_path")
    ]

    def _safe_exists(path_str: str) -> bool:
        try:
            return Path(path_str).exists()
        except Exception:
            return False

    def _remove_from_bucket(
        entry: Dict[str, Any],
        size_buckets: Dict[int, List[Dict[str, Any]]],
        size_value: Optional[int] = None,
    ) -> None:
        bucket_size = size_value if size_value is not None else entry["size_bytes"]
        bucket = size_buckets.get(bucket_size)
        if not bucket:
            return
        try:
            bucket.remove(entry)
        except ValueError:
            return
        if not bucket:
            size_buckets.pop(bucket_size, None)

    def _add_to_bucket(
        entry: Dict[str, Any],
        size_buckets: Dict[int, List[Dict[str, Any]]],
        size_value: Optional[int] = None,
    ) -> None:
        bucket_size = size_value if size_value is not None else entry["size_bytes"]
        size_buckets.setdefault(bucket_size, []).append(entry)

    def _merge_duplicates(
        entry: Dict[str, Any],
        size_buckets: Dict[int, List[Dict[str, Any]]],
        path_map: Dict[str, Dict[str, Any]],
        obsolete_ids: Set[int],
    ) -> int:
        merged_local = 0
        bucket = list(size_buckets.get(entry["size_bytes"], []))
        for donor in bucket:
            if donor["id"] == entry["id"]:
                continue
            if donor["id"] in obsolete_ids:
                continue
            if donor["file_exists"]:
                continue
            updates: Dict[str, Any] = {}
            merged_comments = env.combine_comments(entry.get("comments"), donor.get("comments"))
            if merged_comments != entry.get("comments"):
                entry["comments"] = merged_comments
                updates["comments"] = merged_comments
            merged_pmv = env.merge_pmv_lists(entry.get("pmv_list"), donor.get("pmv_list"))
            if merged_pmv != entry.get("pmv_list"):
                entry["pmv_list"] = merged_pmv
                updates["pmv_list"] = merged_pmv
            donor_date = donor.get("date_added") or ""
            target_date = entry.get("date_added") or ""
            if donor_date:
                if not target_date or donor_date < target_date:
                    entry["date_added"] = donor_date
                    updates["date_added"] = donor_date
            if updates:
                env.db_update_source_fields(entry["id"], **updates)
            obsolete_ids.add(donor["id"])
            _remove_from_bucket(donor, size_buckets)
            path_map.pop(donor["norm_path"], None)
            merged_local += 1
        return merged_local

    def _pick_candidate(
        size_bucket: List[Dict[str, Any]],
        file_name: str,
        obsolete_ids: Set[int],
        seen_ids: Set[int],
    ) -> Optional[Dict[str, Any]]:
        if not size_bucket:
            return None
        lower_name = file_name.lower()
        pool = [e for e in size_bucket if e["id"] not in obsolete_ids]
        if not pool:
            return None
        priority_sets = [
            [e for e in pool if not e["file_exists"] and e["video_name"].lower() == lower_name],
            [e for e in pool if not e["file_exists"]],
            [e for e in pool if e["video_name"].lower() == lower_name],
            [e for e in pool if e["id"] not in seen_ids],
            pool,
        ]
        for subset in priority_sets:
            if subset:
                return subset[0]
        return None

    sources: List[Dict[str, Any]] = []
    path_map: Dict[str, Dict[str, Any]] = {}
    size_buckets: Dict[int, List[Dict[str, Any]]] = {}
    for raw in env.db_get_sources_full():
        entry: Dict[str, Any] = dict(raw)
        entry["id"] = int(entry.get("id") or 0)
        raw_path = str(entry.get("video_path") or "")
        try:
            resolved_path = str(Path(raw_path).resolve(strict=False))
        except Exception:
            resolved_path = raw_path
        entry["video_path"] = resolved_path
        entry["video_name"] = entry.get("video_name") or Path(resolved_path).name
        entry["size_bytes"] = int(entry.get("size_bytes") or 0)
        entry["codec"] = entry.get("codec") or ""
        entry["resolution"] = entry.get("resolution") or ""
        entry["pmv_list"] = entry.get("pmv_list") or ""
        entry["comments"] = entry.get("comments") or ""
        entry["date_added"] = entry.get("date_added") or date.today().isoformat()
        entry["norm_path"] = env.normalize_path_str(resolved_path)
        entry["file_exists"] = _safe_exists(resolved_path)
        sources.append(entry)
        path_map[entry["norm_path"]] = entry
        _add_to_bucket(entry, size_buckets)

    total_files = 0
    added = 0
    skipped = 0
    relocated = 0
    merged_duplicates = 0
    meta_updates = 0
    ignored_files = 0
    ignored_dirs = 0
    added_paths: List[str] = []
    relocated_paths: List[str] = []
    seen_ids: Set[int] = set()
    obsolete_ids: Set[int] = set()

    for row in upload_folders:
        root = Path(row["folder_path"])
        if not root.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            cur_dir = Path(dirpath)
            if env.is_path_under_prefixes(cur_dir, ignore_prefixes):
                ignored_dirs += 1
                dirnames[:] = []
                continue
            dirnames[:] = [
                name
                for name in dirnames
                if not env.is_path_under_prefixes(Path(dirpath) / name, ignore_prefixes)
            ]
            for name in filenames:
                path = cur_dir / name
                if env.is_path_under_prefixes(path, ignore_prefixes):
                    ignored_files += 1
                    continue
                if path.suffix.lower() not in env.default_exts:
                    continue
                total_files += 1
                codec, res = env.video_info_sort(path)
                size_bytes = path.stat().st_size
                resolved_path = str(path.resolve())
                norm_path = env.normalize_path_str(resolved_path)
                existing = path_map.get(norm_path)
                if existing:
                    seen_ids.add(existing["id"])
                    updates: Dict[str, Any] = {}
                    if existing["video_name"] != path.name:
                        existing["video_name"] = path.name
                        updates["video_name"] = path.name
                    if existing["size_bytes"] != size_bytes:
                        old_size = existing["size_bytes"]
                        existing["size_bytes"] = size_bytes
                        _remove_from_bucket(existing, size_buckets, old_size)
                        _add_to_bucket(existing, size_buckets)
                        updates["size_bytes"] = size_bytes
                    if existing["codec"] != codec:
                        existing["codec"] = codec
                        updates["codec"] = codec
                    if existing["resolution"] != res:
                        existing["resolution"] = res
                        updates["resolution"] = res
                    if updates:
                        env.db_update_source_fields(existing["id"], **updates)
                        meta_updates += 1
                    existing["file_exists"] = True
                    merged_duplicates += _merge_duplicates(
                        existing, size_buckets, path_map, obsolete_ids
                    )
                    continue

                candidates = size_buckets.get(size_bytes, [])
                candidate = _pick_candidate(candidates, path.name, obsolete_ids, seen_ids)
                if candidate:
                    seen_ids.add(candidate["id"])
                    old_norm = candidate["norm_path"]
                    if old_norm in path_map:
                        path_map.pop(old_norm, None)
                    candidate_updates = {
                        "video_path": resolved_path,
                        "video_name": path.name,
                        "size_bytes": size_bytes,
                        "codec": codec,
                        "resolution": res,
                    }
                    candidate["video_path"] = resolved_path
                    candidate["video_name"] = path.name
                    candidate["size_bytes"] = size_bytes
                    candidate["codec"] = codec
                    candidate["resolution"] = res
                    candidate["norm_path"] = norm_path
                    candidate["file_exists"] = True
                    path_map[norm_path] = candidate
                    env.db_update_source_fields(candidate["id"], **candidate_updates)
                    relocated += 1
                    relocated_paths.append(resolved_path)
                    merged_duplicates += _merge_duplicates(
                        candidate, size_buckets, path_map, obsolete_ids
                    )
                    continue

                inserted_id = env.db_insert_source(path, codec, res, size_bytes=size_bytes, video_name=path.name)
                if inserted_id:
                    entry = {
                        "id": inserted_id,
                        "video_path": resolved_path,
                        "video_name": path.name,
                        "size_bytes": size_bytes,
                        "codec": codec,
                        "resolution": res,
                        "pmv_list": "",
                        "comments": "",
                        "date_added": date.today().isoformat(),
                        "norm_path": norm_path,
                        "file_exists": True,
                    }
                    sources.append(entry)
                    path_map[norm_path] = entry
                    _add_to_bucket(entry, size_buckets)
                    seen_ids.add(inserted_id)
                    added += 1
                    added_paths.append(resolved_path)
                else:
                    skipped += 1

    stale_ids: Set[int] = set()
    for entry in sources:
        sid = entry["id"]
        if sid in seen_ids:
            continue
        if sid in obsolete_ids:
            continue
        if entry["file_exists"]:
            continue
        stale_ids.add(sid)

    delete_targets = obsolete_ids.union(stale_ids)
    deleted_rows = env.db_delete_sources_by_ids(delete_targets) if delete_targets else 0

    lines = backup_lines + [
        "✅ Сканирование исходников завершено.",
        f"Найдено подходящих файлов: {total_files}",
        f"Новых записей: {added}",
        f"Пропущено (уже были в базе): {skipped}",
    ]
    if relocated:
        lines.append(f"Обновлено путей (перемещённые файлы): {relocated}")
    if meta_updates:
        lines.append(f"Обновлено характеристик существующих файлов: {meta_updates}")
    if merged_duplicates:
        lines.append(f"Объединено записей/комментариев: {merged_duplicates}")
    if deleted_rows:
        lines.append(f"Удалено несуществующих путей: {deleted_rows}")
    if ignore_prefixes:
        lines.append(
            f"Игнорируемые подпапки: {len(ignore_prefixes)} "
            f"(пропущено папок: {ignored_dirs}, пропущено файлов: {ignored_files})"
        )
    if added_paths:
        lines.append("")
        last_added = added_paths[-10:]
        lines.append(f"Новых записей {len(added_paths)}. Последние {len(last_added)} из них:")
        for path in last_added:
            lines.append(f"- {path}")
    if relocated_paths:
        lines.append("")
        last_relocated = relocated_paths[-10:]
        lines.append(f"Обновлено путей {len(relocated_paths)}. Последние {len(last_relocated)} из них:")
        for path in last_relocated:
            lines.append(f"- {path}")

    stats = {
        "total_files": total_files,
        "added": added,
        "skipped": skipped,
        "relocated": relocated,
        "merged_duplicates": merged_duplicates,
        "meta_updates": meta_updates,
        "ignored_files": ignored_files,
        "ignored_dirs": ignored_dirs,
        "deleted_rows": deleted_rows,
        "ignore_prefixes": len(ignore_prefixes),
    }

    return lines, stats
