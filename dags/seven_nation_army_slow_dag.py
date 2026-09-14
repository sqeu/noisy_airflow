"""
## Seven Nation Army (Slow) DAG

One task per note version of the Seven Nation Army riff.
Each note is a separate Airflow task chained sequentially with >>.
Expect ~2-5 s of scheduler overhead between each note.

Main riff: E3 E3 G3 E3 D3 C3 B2  — repeated 4 times.
"""

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
    tags=["example", "sound", "music", "slow"],
    is_paused_upon_creation=False,
)
def seven_nation_army_slow_dag():
    def _make_note_task(idx: int, label: str, freq: float, duration: float):
        @task(
            task_id=f"note_{idx:02d}_{label}",
            params={
                "sound": {
                    "frequency": freq,
                    "duration": duration,
                    "waveform": "sine",
                    # Never let jitter merge sequential melody notes into a chord.
                    "sequential": True,
                }
            },
        )
        def play_note():
            print(f"♪ {label} ({freq:.2f} Hz, {duration}s)")

        return play_note()

    notes = [
        _make_note_task(i, label, freq, dur)
        for i, (label, freq, dur) in enumerate(_RIFF * _REPEATS)
    ]
    for i in range(len(notes) - 1):
        notes[i] >> notes[i + 1]


seven_nation_army_slow_dag()
