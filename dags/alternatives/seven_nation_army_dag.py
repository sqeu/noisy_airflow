"""
## Seven Nation Army DAG

Plays the iconic Seven Nation Army riff (The White Stripes) by posting each
note's sound event directly to the plugin API — no scheduler overhead between
notes.

Main riff: E3 E3 G3 E3 D3 C3 B2  — repeated 4 times (two octaves down for bass).

Frequencies used:
  B2=123.47  C3=130.81  D3=146.83  E3=164.81  G3=196.00
"""

import os
import time

import requests
from airflow.sdk import dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

# (note_label, frequency_hz, duration_seconds) — E3 octave for bass
_RIFF: list[tuple[str, float, float]] = [
    ("E3", 164.81, 0.45),
    ("E3", 164.81, 0.15),
    ("G3", 196.00, 0.15),
    ("E3", 164.81, 0.30),
    ("D3", 146.83, 0.30),
    ("C3", 130.81, 0.30),
    ("B2", 123.47, 0.60),
]

_REPEATS = 4
_GAP = 0.05  # silence between notes so consecutive same-pitch notes are distinct
_API_BASE_URL = os.environ.get("SOUND_NOTIFIER_API_BASE_URL", "http://api-server:8080")


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
    tags=["example", "sound", "music"],
    is_paused_upon_creation=False,
)
def seven_nation_army_dag():
    @task
    def play_song() -> None:
        notes = _RIFF * _REPEATS
        for i, (label, freq, duration) in enumerate(notes):
            event = {
                "task_id": f"note_{i}_{label}",
                "dag_id": "seven_nation_army_dag",
                "status": "success",
                "sound": {"frequency": freq, "duration": duration, "waveform": "sine"},
            }
            try:
                requests.post(
                    f"{_API_BASE_URL}/sound-notifier-api/notify",
                    json=event,
                    timeout=2,
                )
            except Exception:  # noqa: BLE001
                pass
            print(f"♪ {label} ({freq:.2f} Hz, {duration}s)")
            time.sleep(duration + _GAP)

    play_song()


seven_nation_army_dag()
