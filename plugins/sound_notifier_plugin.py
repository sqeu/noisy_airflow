"""Airflow plugin that exposes sound-notification helpers.

Registers a small React app (see static/beep_app.js) on the Airflow UI
dashboard with a button that plays a beep in the browser, and an SSE stream
that the API endpoint pushes to so the React app can beep whenever a task
finishes. DAGs use `notify_task_success` / `notify_task_failure` from
`sound_notifier.notifier` as `on_success_callback` / `on_failure_callback`;
those callbacks POST to the `/notify` endpoint which relays the event over SSE.

Notifications that arrive close together in time (e.g. parallel tasks in the
same DAG run finishing around the same moment) are batched into a single
"chord" event with up to `_MAX_CHORD_SOUNDS` sounds, so the frontend can start
them all at the exact same AudioContext time instead of one-by-one.
"""

import asyncio
import json
import os
from pathlib import Path

from airflow.plugins_manager import AirflowPlugin
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles


_STATIC_DIR = Path(__file__).parent / "sound_notifier" / "static"

static_app = FastAPI()
static_app.mount("/", StaticFiles(directory=_STATIC_DIR), name="sound-notifier-static")

static_app_with_metadata = {
    "app": static_app,
    "url_prefix": "/sound-notifier-static",
    "name": "Sound Notifier Static Assets",
}

# Subscribers pushed to by notify_task_success (see notifier.py) so the React
# app beeps without polling.
_subscribers: list[asyncio.Queue] = []

# Events that land within this window of each other (e.g. parallel tasks
# finishing together) are combined into one chord instead of separate beeps.
# Kept well below the smallest gap between sequential notes in the melody DAGs
# (~0.2s) so intentionally sequential notes aren't merged into one chord.
_CHORD_WINDOW_SECONDS = float(
    os.environ.get("SOUND_NOTIFIER_CHORD_WINDOW_SECONDS", "0.1")
)
_MAX_CHORD_SOUNDS = int(os.environ.get("SOUND_NOTIFIER_MAX_CHORD_SOUNDS", "3"))

_batch_lock = asyncio.Lock()
_pending_events: list[dict] = []
_flush_task: "asyncio.Task | None" = None

api_app = FastAPI()


async def _flush_pending() -> None:
    """Emit whatever events have accumulated as a single chord SSE message."""
    global _pending_events, _flush_task
    async with _batch_lock:
        events, _pending_events = _pending_events, []
        _flush_task = None
    if not events:
        return
    chord_event = {
        "type": "chord",
        "sounds": [e["sound"] for e in events if isinstance(e.get("sound"), dict)],
        "tasks": [
            {k: e.get(k) for k in ("task_id", "dag_id", "status")} for e in events
        ],
    }
    for q in list(_subscribers):
        await q.put(chord_event)


async def _schedule_flush() -> None:
    try:
        await asyncio.sleep(_CHORD_WINDOW_SECONDS)
    except asyncio.CancelledError:
        return
    await _flush_pending()


@api_app.post("/notify")
async def notify_finished_task(event: dict) -> dict:
    global _flush_task

    # Sequential (melody) notes must never be merged into a simultaneous chord:
    # if two notes of a tune happen to finish within the chord window, stacking
    # them plays them on top of each other instead of in sequence. Such events
    # opt out and are pushed straight through so the frontend playhead spaces
    # them back-to-back in order. Chord-batching stays on for parallel tasks.
    sound = event.get("sound") if isinstance(event.get("sound"), dict) else {}
    if sound.get("sequential"):
        standalone_event = {
            "type": "chord",
            "sounds": [sound],
            "tasks": [
                {k: event.get(k) for k in ("task_id", "dag_id", "status")}
            ],
        }
        for q in list(_subscribers):
            await q.put(standalone_event)
        return event

    async with _batch_lock:
        _pending_events.append(event)
        flush_now = len(_pending_events) >= _MAX_CHORD_SOUNDS
        if flush_now and _flush_task is not None:
            _flush_task.cancel()
            _flush_task = None
        elif not flush_now and _flush_task is None:
            _flush_task = asyncio.create_task(_schedule_flush())

    if flush_now:
        await _flush_pending()

    return event


@api_app.post("/notify-melody")
async def notify_melody(event: dict) -> dict:
    """Push a whole ordered melody as ONE un-batched SSE event.

    The frontend schedules the entire note sequence on the AudioContext clock in
    a single pass, so the rhythm is sample-accurate and immune to network/SSE/
    scheduler jitter (unlike one-note-per-task delivery, where the spacing is
    whatever the scheduler + network happened to be). ``event["notes"]`` is an
    ordered list of sound dicts, each optionally carrying its own ``gap``.
    """
    notes = event.get("notes")
    if not isinstance(notes, list):
        notes = []

    melody_event = {
        "type": "melody",
        "notes": [n for n in notes if isinstance(n, dict)],
        "task_id": event.get("task_id"),
        "dag_id": event.get("dag_id"),
        "status": event.get("status", "success"),
    }
    for q in list(_subscribers):
        await q.put(melody_event)

    return event


@api_app.post("/notify-chord")
async def notify_chord(event: dict) -> dict:
    """Push a set of sounds as ONE simultaneous chord, un-batched.

    Unlike ``/notify`` (which *infers* a chord by batching events that happen to
    arrive close together), this states the chord explicitly: every sound in
    ``event["sounds"]`` is emitted in a single chord event and the frontend
    starts them at the same AudioContext time. No timing/count coincidence
    required, and no flush-timer latency for chords smaller than
    ``_MAX_CHORD_SOUNDS``.
    """
    sounds = event.get("sounds")
    if not isinstance(sounds, list):
        sounds = []
    sounds = [s for s in sounds if isinstance(s, dict)][:_MAX_CHORD_SOUNDS]

    chord_event = {
        "type": "chord",
        "sounds": sounds,
        "tasks": [{k: event.get(k) for k in ("task_id", "dag_id", "status")}],
    }
    for q in list(_subscribers):
        await q.put(chord_event)

    return event


@api_app.post("/notify-score")
async def notify_score(event: dict) -> dict:
    """Push a full polyphonic score as ONE event, scheduled by absolute time.

    Each entry in ``event["notes"]`` carries an explicit ``start`` offset
    (seconds) plus the usual sound fields, and the frontend schedules them all
    against a single origin on the AudioContext clock. Independent layers may
    overlap freely (held harmony under a moving melody, punctuating bass), which
    the chord/melody events cannot express because they reserve the playhead
    through their longest voice.
    """
    notes = event.get("notes")
    if not isinstance(notes, list):
        notes = []

    score_event = {
        "type": "score",
        "notes": [n for n in notes if isinstance(n, dict)],
        "task_id": event.get("task_id"),
        "dag_id": event.get("dag_id"),
        "status": event.get("status", "success"),
    }
    for q in list(_subscribers):
        await q.put(score_event)

    return event


@api_app.get("/events")
async def event_stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    _subscribers.append(queue)

    async def generator():
        try:
            # Send an initial snapshot so the client knows the stream is
            # connected before any real event arrives.
            yield f"data: {json.dumps({'type': 'snapshot'})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    # Keepalive comment to prevent proxies from closing idle connections.
                    yield ": keepalive\n\n"
        finally:
            _subscribers.remove(queue)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


api_app_with_metadata = {
    "app": api_app,
    "url_prefix": "/sound-notifier-api",
    "name": "Sound Notifier API",
}

beep_react_app = {
    "name": "Sound Beep",
    "bundle_url": "/sound-notifier-static/beep_app.js",
    "destination": "base",
    "url_route": "sound-beep-app",
}


class SoundNotifierPlugin(AirflowPlugin):
    name = "sound_notifier"
    fastapi_apps = [static_app_with_metadata, api_app_with_metadata]
    react_apps = [beep_react_app]
