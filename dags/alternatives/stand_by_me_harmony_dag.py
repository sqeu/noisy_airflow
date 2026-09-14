"""
## Stand By Me (Harmony) DAG

Ben E. King's "Stand By Me" as a three-voice groove — bass line, percussion and
melody — played together as chords. A single task walks the song and, for each
beat, fires the present voices back-to-back so the plugin batches them into one
simultaneous chord. No scheduler sits in the timing path, so the groove stays
tight. For the one-task-per-note variant see `stand_by_me_harmony_slow_dag`.

Changes: A – F#m – D – E (I–vi–IV–V), the classic Stand By Me progression.
"""

import os
import time

import requests
from airflow.sdk import dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

_BEAT = 0.40  # length of a sustained bass / melody note
_PERC = 0.06  # length of a percussion hit
_GAP = 0.06  # silence between beats
_API_BASE_URL = os.environ.get("SOUND_NOTIFIER_API_BASE_URL", "http://api-server:8080")

# One row per beat: (bass_hz, melody_hz | None, percussion "kick"|"tick"|None).
# Bass walks the roots A – F#m – D – E; melody sings on the strong beats and
# rests on the offbeats; percussion alternates kick/tick for a backbeat.
_ARRANGEMENT: list[tuple[float, float | None, str | None]] = [
    (110.00, 554.37, "kick"),   # A  | C#5
    (110.00, None, "tick"),     # A
    (110.00, 554.37, "kick"),   # A  | C#5
    (110.00, None, "tick"),     # A
    (92.50, 493.88, "kick"),    # F# | B4
    (92.50, None, "tick"),      # F#
    (92.50, 440.00, "kick"),    # F# | A4
    (92.50, None, "tick"),      # F#
    (146.83, 440.00, "kick"),   # D  | A4
    (146.83, None, "tick"),     # D
    (146.83, 493.88, "kick"),   # D  | B4
    (146.83, None, "tick"),     # D
    (164.81, 554.37, "kick"),   # E  | C#5
    (164.81, None, "tick"),     # E
    (164.81, 440.00, "kick"),   # E  | A4
    (164.81, None, "tick"),     # E
]

_REPEATS = 2


def _bass(freq: float) -> dict:
    return {"frequency": freq, "duration": _BEAT, "waveform": "sine", "volume": 1.0}


def _melody(freq: float) -> dict:
    return {"frequency": freq, "duration": _BEAT, "waveform": "triangle", "volume": 0.7}


def _percussion(kind: str) -> dict:
    freq = 90.0 if kind == "kick" else 180.0
    return {"frequency": freq, "duration": _PERC, "waveform": "square", "volume": 0.5}


def _beat_voices(
    bass_hz: float, melody_hz: float | None, perc: str | None
) -> list[tuple[str, dict]]:
    voices: list[tuple[str, dict]] = [("bass", _bass(bass_hz))]
    if melody_hz is not None:
        voices.append(("melody", _melody(melody_hz)))
    if perc is not None:
        voices.append(("percussion", _percussion(perc)))
    return voices


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=None,
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 0,
        "on_success_callback": notify_task_success,
        "on_failure_callback": notify_task_failure,
    },
    tags=["example", "sound", "music", "harmony", "chord", "stand-by-me"],
    is_paused_upon_creation=False,
)
def stand_by_me_harmony_dag():
    @task
    def play_song() -> None:
        for i, (bass_hz, melody_hz, perc) in enumerate(_ARRANGEMENT * _REPEATS):
            # One explicit chord per beat: every voice is stated up front so the
            # frontend starts them at the same AudioContext time — no reliance on
            # arrival-timing batching, and no flush latency on <3-voice beats.
            voices = _beat_voices(bass_hz, melody_hz, perc)
            event = {
                "task_id": f"beat_{i:02d}",
                "dag_id": "stand_by_me_harmony_dag",
                "status": "success",
                "sounds": [sound for _, sound in voices],
            }
            try:
                requests.post(
                    f"{_API_BASE_URL}/sound-notifier-api/notify-chord",
                    json=event,
                    timeout=2,
                )
            except Exception:  # noqa: BLE001
                pass
            print(f"♫ beat {i:02d}: bass {bass_hz:.2f} Hz, melody {melody_hz}")
            time.sleep(_BEAT + _GAP)

    play_song()


stand_by_me_harmony_dag()
