from __future__ import annotations

import logging
import os
import requests

log = logging.getLogger(__name__)

# Base URL for the API server, reachable from the scheduler/worker containers on
# the Astro CLI's internal docker network. In Airflow 3 the compose service is
# named "api-server" (it was "webserver" in Airflow 2). Override via env if needed.
_API_BASE_URL = os.environ.get("SOUND_NOTIFIER_API_BASE_URL", "http://api-server:8080")


def _report_task_finished(context: dict, status: str) -> None:
    """Send task status and sound parameters to the plugin API (best-effort)."""
    task_instance = context["task_instance"]
    task_params = context.get("params") or {}
    sound_params = task_params.get("sound", {})
    if not isinstance(sound_params, dict):
        sound_params = {}

    event = {
        "task_id": task_instance.task_id,
        "dag_id": task_instance.dag_id,
        "status": status,
        "sound": sound_params,
    }
    try:
        response = requests.post(
            f"{_API_BASE_URL}/sound-notifier-api/notify",
            json=event,
            timeout=2,
        )
        log.info("status_code: %s, content: %s", response.status_code, response.content)

    except Exception:  # noqa: BLE001 - never fail the task on this
        log.warning("Could not reach sound-notifier API", exc_info=True)


def notify_task_success(context: dict) -> None:
    task_id = context["task_instance"].task_id
    log.info("Task '%s' succeeded", task_id)
    _report_task_finished(context, "success")


def notify_task_failure(context: dict) -> None:
    task_id = context["task_instance"].task_id
    log.info("Task '%s' failed", task_id)
    _report_task_finished(context, "failure")
