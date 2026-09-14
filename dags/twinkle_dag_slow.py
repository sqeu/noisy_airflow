"""
## Twinkle Twinkle Little Star DAG

Each task plays one note of "Twinkle Twinkle Little Star" using the
sound_notifier callbacks.  The `sound` params follow the same schema as
sound_dag.py: ``frequency`` (Hz), ``duration`` (seconds), ``waveform``.

Note → frequency mapping (C4 octave):
  C4=261.63  D4=293.66  E4=329.63  F4=349.23  G4=392.00  A4=440.00
"""

from airflow.sdk import dag, task
from pendulum import datetime, duration

from sound_notifier.notifier import notify_task_failure, notify_task_success

# (note_label, frequency_hz)
_NOTES = [
    # Twinkle twinkle
    ("C1", 261.63), ("C2", 261.63),
    ("G1", 392.00), ("G2", 392.00),
    ("A1", 440.00), ("A2", 440.00),
    ("G3", 392.00),
    # little star
    ("F1", 349.23), ("F2", 349.23),
    ("E1", 329.63), ("E2", 329.63),
    ("D1", 293.66), ("D2", 293.66),
    ("C3", 261.63),
    # Up above the world so high
    ("G4", 392.00), ("G5", 392.00),
    ("F3", 349.23), ("F4", 349.23),
    ("E3", 329.63), ("E4", 329.63),
    ("D3", 293.66),
    # like a diamond in the sky
    ("G6", 392.00), ("G7", 392.00),
    ("F5", 349.23), ("F6", 349.23),
    ("E5", 329.63), ("E6", 329.63),
    ("D4_", 293.66),
    # Twinkle twinkle
    ("C4_", 261.63), ("C5", 261.63),
    ("G8", 392.00), ("G9", 392.00),
    ("A3", 440.00), ("A4", 440.00),
    ("G10", 392.00),
    # little star
    ("F7", 349.23), ("F8", 349.23),
    ("E7", 329.63), ("E8", 329.63),
    ("D5", 293.66), ("D6", 293.66),
    ("C6", 261.63),
]

_QUARTER = 0.35  # seconds per beat


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
def twinkle_dag_slow():
    def _make_note_task(label: str, freq: float):
        @task(
            task_id=f"note_{label}",
            params={"sound": {"frequency": round(freq, 2), "duration": _QUARTER, "waveform": "sine"}},
        )
        def play_note():
            print(f"♪ {label} ({freq:.2f} Hz)")

        return play_note()

    # Chain all notes sequentially so they play in order
    notes = [_make_note_task(label, freq) for label, freq in _NOTES]
    for i in range(len(notes) - 1):
        notes[i] >> notes[i + 1]


twinkle_dag_slow()
