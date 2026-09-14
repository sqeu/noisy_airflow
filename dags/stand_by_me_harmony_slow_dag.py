"""
## Stand By Me (Harmony, Slow) DAG

Ben E. King's "Stand By Me" as a three-voice groove — bass line, percussion and
melody. One Airflow task per voice per beat: the voices of a beat run in
parallel (so they finish together and the plugin batches them into a chord),
and each beat is gated on the previous beat via `cross_downstream`, so the next
beat starts only once all of the previous beat's voices finish.

Because timing comes from the scheduler, a beat's voices only chord if they
finish within the plugin's chord window (`SOUND_NOTIFIER_CHORD_WINDOW_SECONDS`,
default 0.1s). Under scheduler jitter a voice can play slightly detached. For a
tight groove use `stand_by_me_harmony_dag` (single task, no scheduler in the
timing path).

Changes: A – F#m – D – E (I–vi–IV–V), the classic Stand By Me progression.
"""

from airflow.sdk import cross_downstream, dag, task
from pendulum import datetime

from sound_notifier.notifier import notify_task_failure, notify_task_success

_BEAT = 0.40  # length of a sustained bass / melody note
_PERC = 0.06  # length of a percussion hit

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

# One pass keeps the task graph manageable (~40 tasks); raise for a longer play.
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
    tags=["example", "sound", "music", "harmony", "chord", "stand-by-me", "slow"],
    is_paused_upon_creation=False,
)
def stand_by_me_harmony_slow_dag():
    def _make_voice_task(idx: int, voice: str, sound: dict):
        @task(task_id=f"beat_{idx:02d}_{voice}", params={"sound": sound})
        def play_voice():
            print(f"♪ beat {idx:02d} {voice} ({sound['frequency']:.2f} Hz)")

        return play_voice()

    # One parallel group of voices (bass + maybe melody + maybe percussion) per beat.
    beats = [
        [_make_voice_task(i, voice, sound) for voice, sound in _beat_voices(*row)]
        for i, row in enumerate(_ARRANGEMENT * _REPEATS)
    ]
    # A beat starts only once all of the previous beat's voices have finished;
    # the voices within a beat stay parallel to each other, so they chord.
    for prev, nxt in zip(beats, beats[1:]):
        cross_downstream(prev, nxt)


stand_by_me_harmony_slow_dag()
