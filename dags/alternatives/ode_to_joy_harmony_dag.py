"""
## Ode to Joy (Harmony) DAG

Beethoven's "Ode to Joy" theme as three overlapping layers, played as one
polyphonic score (posted to `/notify-score`), so the browser schedules every
note at an absolute time on the audio clock and the layers ring together:

  - Melody (Trumpet / lead) — the stepwise tune E E F G G F E D ...
  - Harmony (String ensemble) — long, rich triads held under each measure.
  - Bass (Cello) — a single low note punctuating the first beat of every measure.

A single task builds the whole score and sends it once, so timing is
sample-accurate regardless of scheduler / network jitter. For the
one-task-per-note variant see `ode_to_joy_harmony_slow_dag`.

Key: C major, harmonized I–V (C / G), authentic cadence to close.
"""

import os

import requests
from airflow.sdk import dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

_BEAT = 0.5  # seconds per quarter note (~120 BPM)
_API_BASE_URL = os.environ.get("SOUND_NOTIFIER_API_BASE_URL", "http://api-server:8080")

# Melody pitches (C major, octave 4).
_C4, _D4, _E4, _F4, _G4 = 261.63, 293.66, 329.63, 349.23, 392.00

# Harmony triads (octave 3) and bass roots (octave 2).
_C_MAJ = (130.81, 164.81, 196.00)  # C3 E3 G3
_G_MAJ = (196.00, 246.94, 293.66)  # G3 B3 D4
_C2, _G2 = 65.41, 98.00

# Melody: (beat_index, frequency, duration_in_beats). Moves mostly by step.
_MELODY: list[tuple[int, float, int]] = [
    (0, _E4, 1), (1, _E4, 1), (2, _F4, 1), (3, _G4, 1),
    (4, _G4, 1), (5, _F4, 1), (6, _E4, 1), (7, _D4, 1),
    (8, _C4, 1), (9, _C4, 1), (10, _D4, 1), (11, _E4, 1),
    (12, _E4, 1), (13, _D4, 1), (14, _D4, 2),
    (16, _E4, 1), (17, _E4, 1), (18, _F4, 1), (19, _G4, 1),
    (20, _G4, 1), (21, _F4, 1), (22, _E4, 1), (23, _D4, 1),
    (24, _C4, 1), (25, _C4, 1), (26, _D4, 1), (27, _E4, 1),
    (28, _D4, 1), (29, _C4, 1), (30, _C4, 2),
]

# Harmony: (beat_index, triad, duration_in_beats). One held chord per measure,
# split V–I on the final measure for the cadence.
_HARMONY: list[tuple[int, tuple[float, ...], int]] = [
    (0, _C_MAJ, 4), (4, _G_MAJ, 4), (8, _C_MAJ, 4), (12, _G_MAJ, 4),
    (16, _C_MAJ, 4), (20, _G_MAJ, 4), (24, _C_MAJ, 4),
    (28, _G_MAJ, 2), (30, _C_MAJ, 2),
]

# Bass: (beat_index, frequency) — the downbeat of every measure.
_BASS: list[tuple[int, float]] = [
    (0, _C2), (4, _G2), (8, _C2), (12, _G2),
    (16, _C2), (20, _G2), (24, _C2), (28, _G2),
]


def _build_score() -> list[dict]:
    notes: list[dict] = []
    for beat, freq, dur in _MELODY:
        notes.append({
            "start": round(beat * _BEAT, 3),
            "frequency": freq,
            "duration": round(dur * _BEAT, 3),
            "waveform": "square",
            "volume": 0.55,
        })
    for beat, triad, dur in _HARMONY:
        for freq in triad:
            notes.append({
                "start": round(beat * _BEAT, 3),
                "frequency": freq,
                "duration": round(dur * _BEAT, 3),
                "waveform": "triangle",
                "volume": 0.2,
            })
    for beat, freq in _BASS:
        notes.append({
            "start": round(beat * _BEAT, 3),
            "frequency": freq,
            "duration": round(_BEAT, 3),
            "waveform": "sine",
            "volume": 0.85,
        })
    return notes


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
    tags=["example", "sound", "music", "harmony", "score", "ode-to-joy"],
    is_paused_upon_creation=False,
)
def ode_to_joy_harmony_dag():
    @task
    def play_song() -> None:
        event = {
            "task_id": "play_song",
            "dag_id": "ode_to_joy_harmony_dag",
            "status": "success",
            "notes": _build_score(),
        }
        try:
            requests.post(
                f"{_API_BASE_URL}/sound-notifier-api/notify-score",
                json=event,
                timeout=5,
            )
        except Exception:  # noqa: BLE001
            pass
        print("♫ Ode to Joy — melody + strings + cello sent as one score")

    play_song()


ode_to_joy_harmony_dag()
