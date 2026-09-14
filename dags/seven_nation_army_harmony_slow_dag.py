"""
## Seven Nation Army (Harmony, Slow) DAG

One Airflow task per note *per voice*. For every beat of the riff, three tasks
— bass, mid and treble (the same riff stacked one and two octaves up) — run in
parallel, so they finish around the same time and the plugin batches them into
a single 3-note chord. Beats are chained so the song still advances in order.

Because the timing comes from the scheduler, the three voices of a beat only
merge into a chord if they finish within the plugin's chord window
(`SOUND_NOTIFIER_CHORD_WINDOW_SECONDS`, default 0.1s). Under scheduler jitter a
voice can occasionally play slightly detached. For rock-solid chords use
`seven_nation_army_harmony_dag` (single task, no scheduler in the timing path).

Main riff: E G E D C B — each beat voiced in three octaves, repeated 4 times.
"""

from airflow.sdk import cross_downstream, dag, task
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
    tags=["example", "sound", "music", "harmony", "chord", "slow"],
    is_paused_upon_creation=False,
)
def seven_nation_army_harmony_slow_dag():
    def _make_voice_task(
        idx: int, label: str, voice: str, base_freq: float, duration: float
    ):
        @task(
            task_id=f"note_{idx:02d}_{voice}_{label}",
            params={"sound": _voice_sound(base_freq, duration, voice)},
        )
        def play_voice():
            sound = _voice_sound(base_freq, duration, voice)
            print(f"♪ {voice} {label} ({sound['frequency']:.2f} Hz, {duration}s)")

        return play_voice()

    rows = _RIFF * _REPEATS
    # One parallel group of three voices per beat.
    beats = [
        [_make_voice_task(i, label, voice, base_freq, duration) for voice in _VOICES]
        for i, (label, duration, base_freq) in enumerate(rows)
    ]
    # A beat starts only once all three voices of the previous beat have
    # finished (its three voices stay parallel to each other, so they chord).
    for prev, nxt in zip(beats, beats[1:]):
        cross_downstream(prev, nxt)


seven_nation_army_harmony_slow_dag()
