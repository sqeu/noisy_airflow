"""
## Ode to Joy (Harmony, Slow) DAG

Beethoven's "Ode to Joy" as three voices — Melody (trumpet/lead), Harmony
(strings) and Bass (cello) — with one Airflow task per note. Voices that start
on the same beat run in parallel (so the plugin batches them into a chord), and
beats are chained via `cross_downstream`, so the next beat waits for the
previous beat's voices to finish.

Because timing comes from the scheduler (seconds per task, jittery), this is a
"watch each task fire a note" demo rather than a tight performance. It also
can't sustain a held harmony under a moving melody — the browser plays notes on
a single reserved timeline — so here the harmony is one tone of normal length
per downbeat and the melody steps beat by beat. For the true polyphonic version
(held string chords under the melody, sample-accurate) use
`ode_to_joy_harmony_dag`.

Key: C major, harmonized I–V (C / G).
"""

from collections import defaultdict

from airflow.sdk import cross_downstream, dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

_BEAT = 0.5  # seconds per quarter note

# Melody pitches (C major, octave 4).
_C4, _D4, _E4, _F4, _G4 = 261.63, 293.66, 329.63, 349.23, 392.00

# Harmony chord roots (octave 3) and bass roots (octave 2).
_C3, _G3 = 130.81, 196.00
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

# Harmony: (beat_index, root) — one chord root on each measure downbeat.
_HARMONY: list[tuple[int, float]] = [
    (0, _C3), (4, _G3), (8, _C3), (12, _G3),
    (16, _C3), (20, _G3), (24, _C3), (28, _G3),
]

# Bass: (beat_index, frequency) — the downbeat of every measure.
_BASS: list[tuple[int, float]] = [
    (0, _C2), (4, _G2), (8, _C2), (12, _G2),
    (16, _C2), (20, _G2), (24, _C2), (28, _G2),
]


def _sound(freq: float, dur_beats: int, waveform: str, volume: float) -> dict:
    return {
        "frequency": freq,
        "duration": round(dur_beats * _BEAT, 3),
        "waveform": waveform,
        "volume": volume,
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
    tags=["example", "sound", "music", "harmony", "chord", "ode-to-joy", "slow"],
    is_paused_upon_creation=False,
)
def ode_to_joy_harmony_slow_dag():
    # Group every voice by the beat it starts on; voices sharing a beat chord.
    by_beat: dict[int, list[tuple[str, dict]]] = defaultdict(list)
    for beat, freq, dur in _MELODY:
        by_beat[beat].append(("melody", _sound(freq, dur, "square", 0.55)))
    for beat, freq in _HARMONY:
        by_beat[beat].append(("harmony", _sound(freq, 1, "triangle", 0.5)))
    for beat, freq in _BASS:
        by_beat[beat].append(("bass", _sound(freq, 1, "sine", 0.85)))

    def _make_task(beat: int, voice: str, sound: dict):
        @task(task_id=f"b{beat:02d}_{voice}", params={"sound": sound})
        def play_voice():
            print(f"♪ beat {beat:02d} {voice} ({sound['frequency']:.2f} Hz)")

        return play_voice()

    beats = [
        [_make_task(beat, voice, sound) for voice, sound in by_beat[beat]]
        for beat in sorted(by_beat)
    ]
    for prev, nxt in zip(beats, beats[1:]):
        cross_downstream(prev, nxt)


ode_to_joy_harmony_slow_dag()
