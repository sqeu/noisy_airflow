"""
## Twinkle Twinkle Little Star DAG

All notes are played inside a **single task** that posts each note's sound
event directly to the plugin API with a sleep between notes.  This avoids the
2–5 s scheduler overhead that would occur if each note were a separate task
chained with >>.

Sound params schema: ``frequency`` (Hz), ``duration`` (seconds), ``waveform``.

Note → frequency mapping (C4 octave):
  C4=261.63  D4=293.66  E4=329.63  F4=349.23  G4=392.00  A4=440.00
"""

import os
import time

import requests
from airflow.sdk import dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

# (note_label, frequency_hz)
_NOTES = [
    # Twinkle twinkle
    ("C", 261.63), ("C", 261.63),
    ("G", 392.00), ("G", 392.00),
    ("A", 440.00), ("A", 440.00),
    ("G", 392.00),
    # little star
    ("F", 349.23), ("F", 349.23),
    ("E", 329.63), ("E", 329.63),
    ("D", 293.66), ("D", 293.66),
    ("C", 261.63),
    # Up above the world so high
    ("G", 392.00), ("G", 392.00),
    ("F", 349.23), ("F", 349.23),
    ("E", 329.63), ("E", 329.63),
    ("D", 293.66),
    # like a diamond in the sky
    ("G", 392.00), ("G", 392.00),
    ("F", 349.23), ("F", 349.23),
    ("E", 329.63), ("E", 329.63),
    ("D", 293.66),
    # Twinkle twinkle
    ("C", 261.63), ("C", 261.63),
    ("G", 392.00), ("G", 392.00),
    ("A", 440.00), ("A", 440.00),
    ("G", 392.00),
    # little star
    ("F", 349.23), ("F", 349.23),
    ("E", 329.63), ("E", 329.63),
    ("D", 293.66), ("D", 293.66),
    ("C", 261.63),
]

_QUARTER = 0.35   # seconds per beat
_GAP = 0.05       # brief silence between notes so consecutive same-pitch notes are distinct
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
def twinkle_dag():
    @task
    def play_song() -> None:
        """Post each note directly to the plugin API, sleeping between notes."""
        for i, (label, freq) in enumerate(_NOTES):
            event = {
                "task_id": f"note_{i}_{label}",
                "dag_id": "twinkle_dag",
                "status": "success",
                "sound": {"frequency": round(freq, 2), "duration": _QUARTER, "waveform": "sine"},
            }
            try:
                requests.post(
                    f"{_API_BASE_URL}/sound-notifier-api/notify",
                    json=event,
                    timeout=2,
                )
            except Exception:  # noqa: BLE001
                pass
            print(f"♪ {label} ({freq:.2f} Hz)")
            time.sleep(_QUARTER + _GAP)

    play_song()


twinkle_dag()
