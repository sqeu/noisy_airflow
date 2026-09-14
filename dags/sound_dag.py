"""
## Sound Notification example DAG

This DAG demonstrates using task-level `on_success_callback` and
`on_failure_callback` hooks (set via `default_args` so they apply to every
task) to play a sound each time a task finishes.

Since Airflow tasks run inside the Astro Docker containers there is no audio
hardware to play through, so the callback (see `plugins/sound_notifier/notifier.py`)
rings the ASCII terminal bell character, which most terminal emulators turn
into an audible beep on the host machine when tailing logs (e.g.
`astro dev logs --scheduler --follow`). It also best-effort tries local CLI
audio players (`afplay`/`paplay`/`aplay`) in case real audio output is ever
available.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow
"""

from airflow.sdk import dag, task
from pendulum import datetime, duration

from sound_notifier.notifier import notify_task_failure, notify_task_success


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    max_consecutive_failed_dag_runs=5,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 2,
        "retry_delay": duration(seconds=5),
        # rings a sound after every task instance succeeds/fails
        "on_success_callback": notify_task_success,
        "on_failure_callback": notify_task_failure,
    },
    tags=["example", "sound"],
    is_paused_upon_creation=False,
)
def sound_dag():
    @task(params={"sound": {"frequency": 440, "duration": 0.2, "waveform": "sine"}})
    def extract() -> list[int]:
        print("Extracting data...")
        return [1, 2, 3]

    @task(params={"sound": {"frequency": 660, "duration": 0.35, "waveform": "triangle"}})
    def transform(data: list[int]) -> int:
        print("Transforming data...")
        return sum(data)

    @task(params={"sound": {"frequency": 880, "duration": 0.5, "waveform": "square"}})
    def load(total: int) -> None:
        print(f"Loading total: {total}")

    load(transform(extract()))


sound_dag()
