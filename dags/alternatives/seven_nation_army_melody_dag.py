"""
## Seven Nation Army (Melody) DAG

Accurate-timing version of the riff. A single task posts the *entire* ordered
note sequence to the plugin's `/notify-melody` endpoint in one request. The
browser then schedules every note up front on the Web Audio clock, so the
rhythm is sample-accurate and immune to scheduler / network / SSE jitter.

Contrast with the other variants:
  - `seven_nation_army_slow_dag`: one task per note. Rhythm = scheduler timing
    (2-5 s, jittery) -> gaps and occasional stacked notes. Good for *seeing*
    each task finish, bad for a clean tune.
  - `seven_nation_army_dag`: one task loops and sleeps between per-note POSTs.
    Better, but still one event per note so it inherits per-note delivery jitter.
  - `seven_nation_army_melody_dag` (this one): one event, timing computed in the
    browser. Best-sounding tune.

Main riff: E3 E3 G3 E3 D3 C3 B2  — repeated 4 times.
"""

import os

import requests
from airflow.sdk import dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

# (note_label, frequency_hz, duration_seconds)
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
_GAP = 0.04  # silence after each note so repeated same-pitch notes stay distinct
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
    tags=["example", "sound", "music", "melody"],
    is_paused_upon_creation=False,
)
def seven_nation_army_melody_dag():
    @task
    def play_melody() -> None:
        notes = [
            {"frequency": freq, "duration": duration, "waveform": "sine", "gap": _GAP}
            for _, freq, duration in _RIFF * _REPEATS
        ]
        event = {
            "task_id": "play_melody",
            "dag_id": "seven_nation_army_melody_dag",
            "status": "success",
            "notes": notes,
        }
        try:
            requests.post(
                f"{_API_BASE_URL}/sound-notifier-api/notify-melody",
                json=event,
                timeout=5,
            )
        except Exception:  # noqa: BLE001 - never fail the task on a best-effort beep
            pass
        for label, freq, duration in _RIFF * _REPEATS:
            print(f"♪ {label} ({freq:.2f} Hz, {duration}s)")

    play_melody()


seven_nation_army_melody_dag()
