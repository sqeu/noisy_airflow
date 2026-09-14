"""
## Seven Nation Army (Harmony) DAG

Chord version of the riff. A single task walks the song and, for each beat,
fires three sound events back-to-back — bass, mid, treble (the same riff stacked
one and two octaves up). They arrive within the plugin's chord window, so the
backend batches them into a single 3-note chord and the browser starts all
three oscillators at the exact same AudioContext time.

Because one task drives the whole song (no scheduler between beats), the chords
land reliably. For the one-task-per-note variant see
`seven_nation_army_harmony_slow_dag`.

Main riff: E G E D C B — each beat voiced in three octaves, repeated 4 times.
"""

import os
import time

import requests
from airflow.sdk import dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

# Base ("bass") voice of the riff. (label, duration_seconds, bass_frequency_hz)
_RIFF: list[tuple[str, float, float]] = [
    ("E", 0.45, 164.81),
    ("E", 0.15, 164.81),
    ("G", 0.15, 196.00),
    ("E", 0.30, 164.81),
    ("D", 0.30, 146.83),
    ("C", 0.30, 130.81),
    ("B", 0.60, 123.47),
]

# Each voice = the bass line multiplied up by whole octaves, with its own timbre
# and volume so three stacked tones stay distinct and don't sum into mush.
_VOICES: dict[str, dict] = {
    "bass": {"mult": 1, "waveform": "sine", "volume": 1.0},
    "mid": {"mult": 2, "waveform": "triangle", "volume": 0.6},
    "treble": {"mult": 4, "waveform": "sine", "volume": 0.4},
}

_REPEATS = 4
_GAP = 0.05  # silence between beats so repeated same-pitch chords stay distinct
_API_BASE_URL = os.environ.get("SOUND_NOTIFIER_API_BASE_URL", "http://api-server:8080")


def _voice_sound(base_freq: float, duration: float, voice: str) -> dict:
    cfg = _VOICES[voice]
    return {
        "frequency": round(base_freq * cfg["mult"], 2),
        "duration": duration,
        "waveform": cfg["waveform"],
        "volume": cfg["volume"],
    }


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
    tags=["example", "sound", "music", "harmony", "chord"],
    is_paused_upon_creation=False,
)
def seven_nation_army_harmony_dag():
    @task
    def play_song() -> None:
        for i, (label, duration, base_freq) in enumerate(_RIFF * _REPEATS):
            # One explicit chord per beat: all three voices are stated up front
            # so the frontend starts them at the same AudioContext time, instead
            # of relying on the backend to batch separately-posted events.
            event = {
                "task_id": f"note_{i}_{label}",
                "dag_id": "seven_nation_army_harmony_dag",
                "status": "success",
                "sounds": [
                    _voice_sound(base_freq, duration, voice) for voice in _VOICES
                ],
            }
            try:
                requests.post(
                    f"{_API_BASE_URL}/sound-notifier-api/notify-chord",
                    json=event,
                    timeout=2,
                )
            except Exception:  # noqa: BLE001
                pass
            print(f"♫ {label} chord (bass/mid/treble, {duration}s)")
            time.sleep(duration + _GAP)

    play_song()


seven_nation_army_harmony_dag()
