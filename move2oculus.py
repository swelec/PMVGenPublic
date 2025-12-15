#!/usr/bin/env python3
# coding: utf-8

"""
Скрипт синхронизации папки y:\\output с Oculus Quest (папка /sdcard/Movies/output).
Определяет недостающие файлы на шлеме и копирует их через ADB.
"""

from __future__ import annotations

import atexit
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path, PurePosixPath
from typing import Dict, Iterable, List, Optional, Set, Tuple

from main import NETWORK_OUTPUT_ROOT

SCRIPT_DIR = Path(__file__).resolve().parent
REMOTE_OUTPUT_ROOT = PurePosixPath("/sdcard/Movies/output")
LOCK_PATH = SCRIPT_DIR / "move2oculus.lock"


class AdbError(RuntimeError):
    pass


def format_bytes(num: int) -> str:
    step = 1024.0
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    val = float(num)
    for unit in units:
        if abs(val) < step:
            return f"{val:.1f} {unit}".replace(".0", "")
        val /= step
    return f"{val * step:.1f} ПБ".replace(".0", "")


def ensure_adb_available() -> str:
    exe_name = "adb.exe" if os.name == "nt" else "adb"
    adb_path = shutil.which("adb")
    if not adb_path:
        candidates = [
            SCRIPT_DIR / "platform-tools" / exe_name,
            Path("C:/platform-tools") / exe_name,
            Path.home() / "AppData/Local/Android/Sdk/platform-tools" / exe_name,
        ]
        for candidate in candidates:
            if candidate.exists():
                os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + str(candidate.parent)
                adb_path = shutil.which("adb")
                if adb_path:
                    break
        if not adb_path:
            raise AdbError(
                "Команда adb не найдена. Установите Platform Tools (например, в C:\\platform-tools) "
                "и выполните в PowerShell:\n"
                "$env:Path += ';C:\\platform-tools'\nsetx Path $env:Path"
            )
    return adb_path


def read_lock_pid() -> Optional[int]:
    if not LOCK_PATH.exists():
        return None
    try:
        return int(LOCK_PATH.read_text().strip())
    except Exception:
        return None


def process_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        proc = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
        )
        needle = f"{pid}"
        return needle in proc.stdout
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def terminate_process(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/F"],
            capture_output=True,
            text=True,
        )
    else:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.kill(pid, sig)
                time.sleep(1)
            except OSError:
                break


def ensure_single_instance() -> None:
    existing = read_lock_pid()
    current_pid = os.getpid()
    if existing and existing != current_pid:
        if process_running(existing):
            print(f"Обнаружен предыдущий процесс move2oculus (PID {existing}). Завершаю...", flush=True)
            terminate_process(existing)
            time.sleep(2)
        LOCK_PATH.unlink(missing_ok=True)
    LOCK_PATH.write_text(str(current_pid))

    def _cleanup() -> None:
        if LOCK_PATH.exists():
            try:
                LOCK_PATH.unlink()
            except Exception:
                pass

    atexit.register(_cleanup)


def run_adb(args: Iterable[str]) -> subprocess.CompletedProcess[str]:
    cmd = ["adb", *args]
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        raise AdbError(
            f"Команда {' '.join(cmd)} завершилась с кодом {proc.returncode}.\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc


def ensure_device_connected() -> List[str]:
    proc = run_adb(["devices"])
    lines = proc.stdout.strip().splitlines()[1:]  # пропустить заголовок
    devices = []
    for line in lines:
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[1] == "device":
            devices.append(parts[0])
    if not devices:
        raise AdbError("ADB устройство не обнаружено. Подключите шлем и разрешите отладку.")
    return devices


def ensure_remote_root() -> None:
    run_adb(["shell", "mkdir", "-p", str(REMOTE_OUTPUT_ROOT)])


def list_remote_files() -> Set[PurePosixPath]:
    ensure_remote_root()
    proc = run_adb(["shell", "find", str(REMOTE_OUTPUT_ROOT), "-type", "f", "-print"])
    files: Set[PurePosixPath] = set()
    prefix = str(REMOTE_OUTPUT_ROOT)
    for line in proc.stdout.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if not line.startswith(prefix):
            continue
        rel = line[len(prefix) :].lstrip("/")
        if rel:
            files.add(PurePosixPath(rel))
    return files


def list_local_files() -> Dict[PurePosixPath, Tuple[Path, int]]:
    root = Path(NETWORK_OUTPUT_ROOT)
    if not root.exists():
        raise RuntimeError(f"Локальная папка не найдена: {root}")
    files: Dict[PurePosixPath, Tuple[Path, int]] = {}
    for path in root.rglob("*"):
        if path.is_file():
            rel = path.relative_to(root)
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            files[PurePosixPath(*rel.parts)] = (path, size)
    return files


def push_file(local_path: Path, rel_posix: PurePosixPath) -> None:
    remote_path = REMOTE_OUTPUT_ROOT / rel_posix
    remote_dir = remote_path.parent
    run_adb(["shell", "mkdir", "-p", str(remote_dir)])
    print(f"[ADB] push {local_path} -> {remote_path}")
    proc = run_adb(["push", str(local_path), str(remote_path)])
    if proc.stdout.strip():
        print(proc.stdout.strip())


def main() -> None:
    ensure_single_instance()
    ensure_adb_available()
    devices = ensure_device_connected()
    print(f"Найдено устройств ADB: {', '.join(devices)}")

    local_files = list_local_files()
    remote_files = list_remote_files()
    print(f"Локальных файлов: {len(local_files)}, на Oculus: {len(remote_files)}")

    missing: List[Tuple[PurePosixPath, Path, int]] = []
    for rel, (local_path, size_bytes) in local_files.items():
        if rel not in remote_files:
            missing.append((rel, local_path, size_bytes))

    if not missing:
        print("Все файлы уже есть на шлеме. Синхронизация не требуется.")
        return

    total_bytes = sum(size for _, _, size in missing)
    print(
        f"Нужно копировать {len(missing)} файлов (~{format_bytes(total_bytes)})."
    )
    errors: List[str] = []
    copied_bytes = 0
    total_files = len(missing)
    for idx, (rel, local_path, size_bytes) in enumerate(sorted(missing), start=1):
        try:
            print(
                f"[{idx}/{total_files}] {rel} ({format_bytes(size_bytes)})",
                flush=True,
            )
            push_file(local_path, rel)
            copied_bytes += size_bytes
            remaining = max(total_bytes - copied_bytes, 0)
            print(
                f"    ✓ Осталось {total_files - idx} файлов / {format_bytes(remaining)}",
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{rel}: {exc}")

    print("=" * 60)
    print(f"Успешно скопировано: {len(missing) - len(errors)}")
    if errors:
        print(f"Ошибки ({len(errors)}):", file=sys.stderr)
        for err in errors:
            print(err, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
