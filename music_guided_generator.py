#!/usr/bin/env python3
# coding: utf-8
"""
Music-guided PMV pre-processor.

Цель скрипта — подготовить музыкальные проекты, с которыми главный
компилятор (main.py) сможет позже синхронизировать
видео-клипы под ритм / динамику выбранного трека.

Функциональность:
1. Анализ MP3 (темп, биты, относительная громкость).
2. Формирование таймкодов (beat-intervals) с оценкой интенсивности.
3. Сохранение трека и манифеста проекта в отдельной папке.
4. CLI-команда `analyze` для создания нового проекта.

Манифесты сохраняются в JSON, чтобы основной скрипт мог выбрать проект,
прочитать таймкоды и нарезать видео под музыку. Внутри JSON указываются:
    - tempo / beat_times / intensity
    - сегменты с start/end/duration/intensity
    - путь к локальной копии MP3
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:
    import numpy as np
    import librosa
except ImportError as exc:  # pragma: no cover - handled at runtime
    raise SystemExit(
        "Не удалось импортировать зависимости. "
        "Установите их командой: pip install librosa numpy"
    ) from exc


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECTS_ROOT = SCRIPT_DIR / "music_projects"
DEFAULT_TARGET_SEGMENT = 1.0  # seconds (минимальная длительность сегмента)
DEFAULT_SEGMENT_MODE = "beat"
SEGMENT_MODES = ("beat", "onset", "uniform")


def slugify(text: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-_"
    slug = []
    for ch in text.lower():
        slug.append(ch if ch in allowed else "-")
    compact = "".join(slug).strip("-")
    return compact or f"project-{int(time.time())}"


@dataclass
class Segment:
    index: int
    start: float
    end: float
    duration: float
    intensity: float


@dataclass
class MusicAnalysis:
    sample_rate: int
    tempo: float
    beat_times: List[float]
    beat_intensity: List[float]
    rms_curve: List[float]
    segments: List[Segment] = field(default_factory=list)
    mode: str = DEFAULT_SEGMENT_MODE

    def to_dict(self) -> Dict:
        payload = asdict(self)
        payload["segments"] = [asdict(seg) for seg in self.segments]
        return payload


@dataclass
class ProjectManifest:
    name: str
    slug: str
    audio_path: str
    created_at: str
    analysis: MusicAnalysis
    source_file: Optional[str] = None  # original MP3 path (before copying into project)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "slug": self.slug,
            "audio_path": self.audio_path,
            "created_at": self.created_at,
            "source_file": self.source_file,
            "analysis": self.analysis.to_dict(),
        }


def analyze_mp3(
    mp3_path: Path,
    hop_length: int = 512,
    target_segment: float = DEFAULT_TARGET_SEGMENT,
    segment_mode: str = DEFAULT_SEGMENT_MODE,
    beat_tightness: Optional[float] = None,
    onset_delta: Optional[float] = None,
    sensitivity_scale: float = 1.0,
) -> MusicAnalysis:
    """Анализирует MP3 для получения темпа и beat-тактов."""
    y, sr = librosa.load(mp3_path, mono=True)
    beat_kwargs = {}
    if beat_tightness is not None:
        beat_kwargs["tightness"] = beat_tightness
    tempo, beat_frames = librosa.beat.beat_track(y=y, sr=sr, hop_length=hop_length, **beat_kwargs)
    beat_times = librosa.frames_to_time(beat_frames, sr=sr, hop_length=hop_length)

    onset_env = librosa.onset.onset_strength(y=y, sr=sr, hop_length=hop_length)
    beat_strengths = onset_env[beat_frames] if len(beat_frames) else np.zeros(0)
    if beat_strengths.size:
        max_strength = beat_strengths.max()
        if max_strength > 0:
            beat_strengths = beat_strengths / max_strength

    rms = librosa.feature.rms(y=y, frame_length=2048, hop_length=hop_length)[0]
    rms = rms / rms.max() if rms.size and rms.max() > 0 else rms

    beat_intensity = beat_strengths.tolist()

    mode = (segment_mode or DEFAULT_SEGMENT_MODE).lower()
    if mode not in SEGMENT_MODES:
        mode = DEFAULT_SEGMENT_MODE

    if sensitivity_scale <= 0:
        sensitivity_scale = 1.0
    adjusted_segment = max(0.2, target_segment / sensitivity_scale)

    total_duration = len(y) / sr
    dynamic_mins = compute_dynamic_min_durations(
        beat_times,
        rms,
        hop_length=hop_length,
        sr=sr,
        base_len=adjusted_segment,
    )

    if mode == "uniform":
        segments = build_uniform_segments(total_duration, adjusted_segment)
    elif mode == "onset":
        segments = build_onset_segments(
            y,
            sr,
            hop_length=hop_length,
            default_len=adjusted_segment,
            rms_curve=rms,
            onset_delta=onset_delta,
        )
    else:
        segments = build_segments(
            beat_times,
            beat_intensity,
            default_len=adjusted_segment,
            min_duration=adjusted_segment,
            dynamic_min_durations=dynamic_mins,
        )

    return MusicAnalysis(
        sample_rate=sr,
        tempo=float(tempo),
        beat_times=beat_times.tolist(),
        beat_intensity=beat_intensity,
        rms_curve=rms.tolist(),
        segments=segments,
        mode=mode,
    )


def build_segments(
    beat_times: Iterable[float],
    beat_strengths: Iterable[float],
    default_len: float,
    min_duration: float,
    dynamic_min_durations: Optional[List[float]] = None,
) -> List[Segment]:
    beats = list(beat_times)
    strengths = list(beat_strengths)
    if len(strengths) < len(beats):
        strengths.extend([0.0] * (len(beats) - len(strengths)))

    segments: List[Segment] = []
    for idx, start in enumerate(beats):
        end = beats[idx + 1] if idx + 1 < len(beats) else start + default_len
        duration = max(0.1, end - start)
        segments.append(
            Segment(
                index=idx,
                start=round(float(start), 3),
                end=round(float(end), 3),
                duration=round(float(duration), 3),
                intensity=round(float(strengths[idx]), 3),
            )
        )
    if min_duration <= 0 and not dynamic_min_durations:
        return segments
    return merge_segments_by_duration(segments, min_duration, dynamic_min_durations)


def merge_segments_by_duration(
    segments: List[Segment],
    min_duration: float,
    dynamic_min_durations: Optional[List[float]] = None,
) -> List[Segment]:
    merged: List[Segment] = []
    acc_start = None
    acc_end = None
    acc_duration = 0.0
    acc_energy = 0.0
    acc_idx = 0
    current_threshold = max(0.1, min_duration)

    def flush(index_hint: int) -> None:
        nonlocal acc_start, acc_end, acc_duration, acc_energy, acc_idx
        if acc_start is None or acc_end is None or acc_duration <= 0:
            acc_start = acc_end = None
            acc_duration = acc_energy = 0.0
            return
        intensity = max(0.0, min(1.0, acc_energy / acc_duration))
        merged.append(
            Segment(
                index=index_hint,
                start=round(acc_start, 3),
                end=round(acc_end, 3),
                duration=round(acc_duration, 3),
                intensity=round(intensity, 3),
            )
        )
        acc_start = acc_end = None
        acc_duration = acc_energy = 0.0

    for idx, seg in enumerate(segments):
        if acc_start is None:
            acc_start = seg.start
            if dynamic_min_durations and idx < len(dynamic_min_durations):
                current_threshold = max(0.1, dynamic_min_durations[idx])
            else:
                current_threshold = max(0.1, min_duration)
        acc_end = seg.end
        acc_duration += seg.duration
        acc_energy += seg.intensity * seg.duration
        if acc_duration >= current_threshold:
            flush(acc_idx)
            acc_idx += 1

    if acc_duration > 0 and acc_start is not None and acc_end is not None:
        flush(acc_idx)

    return merged or segments


def compute_dynamic_min_durations(
    times: Iterable[float],
    rms_curve: np.ndarray,
    hop_length: int,
    sr: int,
    base_len: float,
) -> List[float]:
    times = list(times)
    if not times:
        return []
    if rms_curve is None or rms_curve.size == 0:
        return [base_len] * len(times)

    rms_array = np.asarray(rms_curve, dtype=float)
    max_val = float(rms_array.max())
    if max_val <= 0:
        norm = np.zeros_like(rms_array)
    else:
        norm = rms_array / max_val
    sample_times = np.arange(len(norm)) * hop_length / sr

    fast_len = max(0.35, base_len * 0.5)
    slow_len = max(base_len, base_len * 1.8)

    result: List[float] = []
    for t in times:
        energy = float(np.interp(t, sample_times, norm))
        min_len = float(np.interp(energy, [0.0, 1.0], [slow_len, fast_len]))
        result.append(min_len)
    return result


def build_uniform_segments(total_duration: float, segment_len: float) -> List[Segment]:
    if total_duration <= 0:
        return []
    seg = max(0.5, segment_len)
    segments: List[Segment] = []
    start = 0.0
    idx = 0
    while start < total_duration:
        end = min(total_duration, start + seg)
        duration = max(0.1, end - start)
        segments.append(
            Segment(
                index=idx,
                start=round(start, 3),
                end=round(end, 3),
                duration=round(duration, 3),
                intensity=1.0,
            )
        )
        start = end
        idx += 1
    return segments


def build_onset_segments(
    y: np.ndarray,
    sr: int,
    hop_length: int,
    default_len: float,
    rms_curve: Optional[np.ndarray] = None,
    onset_delta: Optional[float] = None,
) -> List[Segment]:
    onset_kwargs = {}
    if onset_delta is not None:
        onset_kwargs["delta"] = onset_delta
    onset_frames = librosa.onset.onset_detect(
        y=y, sr=sr, hop_length=hop_length, units="frames", **onset_kwargs
    )
    onset_times = librosa.frames_to_time(onset_frames, sr=sr, hop_length=hop_length)
    if onset_times.size == 0:
        total_duration = len(y) / sr
        return build_uniform_segments(total_duration, default_len)
    strengths = np.ones_like(onset_times)
    dynamic_mins = compute_dynamic_min_durations(
        onset_times.tolist(),
        rms_curve=rms_curve,
        hop_length=hop_length,
        sr=sr,
        base_len=default_len,
    )
    segments = build_segments(
        onset_times,
        strengths,
        default_len=default_len,
        min_duration=default_len,
        dynamic_min_durations=dynamic_mins,
    )
    if not segments:
        total_duration = len(y) / sr
        return build_uniform_segments(total_duration, default_len)
    return segments


def ensure_project_paths(slug: str) -> Dict[str, Path]:
    project_dir = PROJECTS_ROOT / slug
    project_dir.mkdir(parents=True, exist_ok=True)
    return {
        "dir": project_dir,
        "audio": project_dir / "audio.mp3",
        "manifest": project_dir / "manifest.json",
        "timecodes": project_dir / "timecodes.txt",
    }


def save_timecodes_txt(segments: List[Segment], dst: Path) -> None:
    lines = ["# start_seconds,end_seconds,intensity"]
    for seg in segments:
        lines.append(f"{seg.start:.3f},{seg.end:.3f},{seg.intensity:.3f}")
    dst.write_text("\n".join(lines), encoding="utf-8")


def create_music_project(
    mp3_path: Path,
    name: Optional[str] = None,
    target_segment: float = DEFAULT_TARGET_SEGMENT,
    segment_mode: str = DEFAULT_SEGMENT_MODE,
    analysis_kwargs: Optional[Dict[str, Any]] = None,
) -> ProjectManifest:
    mp3_path = mp3_path.resolve()
    if not mp3_path.exists():
        raise FileNotFoundError(mp3_path)

    project_name = name or mp3_path.stem
    slug = slugify(project_name)
    paths = ensure_project_paths(slug)

    analysis = analyze_mp3(
        mp3_path,
        target_segment=target_segment,
        segment_mode=segment_mode,
        **(analysis_kwargs or {}),
    )

    shutil.copy2(mp3_path, paths["audio"])
    manifest = ProjectManifest(
        name=project_name,
        slug=slug,
        audio_path=str(paths["audio"]),
        created_at=time.strftime("%Y-%m-%d %H:%M:%S"),
        analysis=analysis,
        source_file=str(mp3_path),
    )

    paths["manifest"].write_text(
        json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    save_timecodes_txt(analysis.segments, paths["timecodes"])

    return manifest


def handle_analyze(args: argparse.Namespace) -> None:
    mp3_path = Path(args.mp3)
    manifest = create_music_project(
        mp3_path=mp3_path,
        name=args.name,
        target_segment=args.segment,
        segment_mode=args.mode,
    )
    print(f"✅ Проект создан: {manifest.slug}")
    print(f"• MP3: {manifest.audio_path}")
    print(f"• Манифест: {PROJECTS_ROOT / manifest.slug / 'manifest.json'}")
    print(f"• Таймкоды: {PROJECTS_ROOT / manifest.slug / 'timecodes.txt'}")
    print(f"• Темп: {manifest.analysis.tempo:.2f} BPM")
    print(f"• Сегментов: {len(manifest.analysis.segments)}")
    print(f"• Режим сегментации: {manifest.analysis.mode}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Music-guided PMV helper."
    )
    sub = parser.add_subparsers(dest="command")

    analyze = sub.add_parser("analyze", help="Создать музыкальный проект из MP3")
    analyze.add_argument("mp3", help="Путь к mp3 файлу")
    analyze.add_argument("--name", help="Имя проекта (по умолчанию = имя файла)")
    analyze.add_argument(
        "--segment",
        type=float,
        default=DEFAULT_TARGET_SEGMENT,
        help="Минимальная длительность сегмента (сек). Большее значение = реже смены.",
    )
    analyze.add_argument(
        "--mode",
        choices=SEGMENT_MODES,
        default=DEFAULT_SEGMENT_MODE,
        help=f"Алгоритм сегментации: {', '.join(SEGMENT_MODES)} (по умолчанию beat).",
    )
    analyze.set_defaults(func=handle_analyze)

    return parser


def list_music_files(folder: Path) -> List[Path]:
    if not folder.exists():
        folder.mkdir(parents=True, exist_ok=True)
    files = []
    for path in sorted(folder.iterdir()):
        if path.is_file() and path.suffix.lower() in {".mp3", ".wav", ".flac", ".m4a"}:
            files.append(path)
    return files


def run_interactive() -> None:
    print("🎵 Music Guided Generator (интерактивный режим)")
    music_folder = SCRIPT_DIR / "Music"
    files = list_music_files(music_folder)
    if not files:
        print(f"Папка {music_folder} пуста. Положите туда MP3 и перезапустите.")
        return

    print("Выберите трек из папки Music:")
    for idx, f in enumerate(files, 1):
        print(f"{idx}. {f.name}")
    print("")

    while True:
        choice = input("Номер трека (или Enter для выхода): ").strip()
        if not choice:
            print("Выход из интерактивного режима.")
            return
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            mp3_path = files[int(choice) - 1]
            break
        print("Неверный номер. Попробуйте снова.")

    project_name = input("Имя проекта (можно оставить пустым): ").strip() or None

    segment_len = DEFAULT_TARGET_SEGMENT
    segment_input = input(
        f"Минимальная длительность сегмента (сек). Чем больше — тем реже смена клипов (по умолчанию {DEFAULT_TARGET_SEGMENT}): "
    ).strip()
    if segment_input:
        try:
            segment_len = max(0.5, float(segment_input))
        except ValueError:
            print("Некорректное число, использую значение по умолчанию.")
            segment_len = DEFAULT_TARGET_SEGMENT

    mode_value = input(
        f"Алгоритм сегментации {SEGMENT_MODES} (по умолчанию {DEFAULT_SEGMENT_MODE}): "
    ).strip().lower() or DEFAULT_SEGMENT_MODE
    if mode_value not in SEGMENT_MODES:
        print("Неизвестный режим. Использую beat.")
        mode_value = DEFAULT_SEGMENT_MODE

    manifest = create_music_project(
        mp3_path=mp3_path,
        name=project_name,
        target_segment=segment_len,
        segment_mode=mode_value,
    )

    print("\n✅ Проект создан!")
    print(f"Слаг: {manifest.slug}")
    print(f"MP3: {manifest.audio_path}")
    project_dir = PROJECTS_ROOT / manifest.slug
    print(f"Манифест: {project_dir / 'manifest.json'}")
    print(f"Таймкоды: {project_dir / 'timecodes.txt'}")
    print(f"Темп: {manifest.analysis.tempo:.2f} BPM")
    print(f"Сегментов: {len(manifest.analysis.segments)}")
    print(f"Режим сегментации: {manifest.analysis.mode}")


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        run_interactive()
    else:
        args.func(args)


if __name__ == "__main__":  # pragma: no cover
    main()
