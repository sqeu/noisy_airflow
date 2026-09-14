// Airflow's plugin loader dynamic-imports this file, then expects it to have
// set `globalThis.AirflowPlugin` to a React function component (see
// airflow-core/src/airflow/ui/src/pages/ReactPlugin.tsx). React itself is
// exposed as `globalThis.React` by the host app, so no bundler/JSX is needed.
globalThis.AirflowPlugin = function SoundBeepApp() {
  const React = globalThis.React;
  const { useEffect, useRef, useState } = React;

  // One AudioContext reused for the component's lifetime, plus the audio-clock
  // time the next note is allowed to start at. Scheduling off this running
  // "playhead" (instead of each event's arrival time) buffers out network/SSE
  // jitter so consecutive notes stay gapless instead of lagging one another.
  const audioCtxRef = useRef(null);
  // Shared limiter all voices route through so 3 simultaneous chord tones
  // summed together can't clip/distort the output.
  const masterNodeRef = useRef(null);
  const nextStartTimeRef = useRef(0);
  // Headroom before the very first (or resumed) note, absorbing scheduling jitter.
  const SCHEDULE_LOOKAHEAD = 0.08;
  // Fixed silence inserted between consecutive notes so back-to-back notes
  // don't butt into each other's release tail and sound like one blurred note.
  const NOTE_GAP = 0.04;
  // Fade-in length so gain doesn't jump straight to full volume (which clicks/pops).
  const ATTACK = 0.008;

  function getAudioContext() {
    if (!audioCtxRef.current) {
      const ctx = new (window.AudioContext || window.webkitAudioContext)();
      const limiter = ctx.createDynamicsCompressor();
      limiter.threshold.value = -12;
      limiter.knee.value = 12;
      limiter.ratio.value = 12;
      limiter.attack.value = 0.003;
      limiter.release.value = 0.15;
      limiter.connect(ctx.destination);
      audioCtxRef.current = ctx;
      masterNodeRef.current = limiter;
    }
    // Autoplay policies can leave the context suspended; resume it so notes
    // don't silently drop instead of playing.
    if (audioCtxRef.current.state === "suspended") {
      audioCtxRef.current.resume().catch(() => {});
    }
    return audioCtxRef.current;
  }

  // Schedules a single tone to start at an absolute AudioContext time and
  // returns its clamped duration. Shared by playChord (parallel voices) and
  // playMelody (sequential notes) so both use the exact same synthesis path.
  function scheduleVoice(audioCtx, sound = {}, startTime) {
    // Create an oscillator (the sound generator) and a gain node (volume control).
    const oscillator = audioCtx.createOscillator();
    const gainNode = audioCtx.createGain();

    // Configure the beep, clamping to safe ranges.
    const frequency = Number.isFinite(Number(sound.frequency))
      ? Math.min(Math.max(Number(sound.frequency), 40), 2000)
      : 440;
    const duration = Number.isFinite(Number(sound.duration))
      ? Math.min(Math.max(Number(sound.duration), 0.05), 2)
      : 0.3;
    const volume = Number.isFinite(Number(sound.volume))
      ? Math.min(Math.max(Number(sound.volume), 0.01), 1)
      : 1;
    oscillator.type = ["sine", "square", "triangle", "sawtooth"].includes(sound.waveform)
      ? sound.waveform
      : "sine";
    oscillator.frequency.value = frequency;

    // Volume envelope: quick fade in (avoids a click), hold, then fade out at the end.
    const attack = Math.min(ATTACK, duration / 4);
    gainNode.gain.setValueAtTime(0.0001, startTime);
    gainNode.gain.exponentialRampToValueAtTime(volume, startTime + attack);
    gainNode.gain.exponentialRampToValueAtTime(0.001, startTime + duration);

    // Connect the nodes together: Generator -> Volume -> Limiter -> Speakers
    oscillator.connect(gainNode);
    gainNode.connect(masterNodeRef.current);

    oscillator.start(startTime);
    oscillator.stop(startTime + duration);
    // Release references promptly instead of waiting on garbage collection.
    oscillator.onended = () => {
      oscillator.disconnect();
      gainNode.disconnect();
    };

    return duration;
  }

  // Plays up to 3 tones together (e.g. base/mid/high from parallel tasks) by
  // scheduling every oscillator to start at the same shared AudioContext time.
  function playChord(sounds = [{}]) {
    const voices = (Array.isArray(sounds) ? sounds : [sounds]).slice(0, 3);
    if (voices.length === 0) return;

    const audioCtx = getAudioContext();
    // Never earlier than the lookahead from now, nor before the previous note ends.
    const startTime = Math.max(audioCtx.currentTime + SCHEDULE_LOOKAHEAD, nextStartTimeRef.current);
    let maxDuration = 0;

    voices.forEach((sound = {}) => {
      maxDuration = Math.max(maxDuration, scheduleVoice(audioCtx, sound, startTime));
    });

    // Reserve the audio clock through this chord (plus a fixed gap) so the
    // next note starts cleanly after this one instead of overlapping it.
    nextStartTimeRef.current = startTime + maxDuration + NOTE_GAP;
    // Wall-clock seconds from now until playback finishes, for UI to sync to.
    return nextStartTimeRef.current - audioCtx.currentTime;
  }

  // Plays an ordered list of notes as a single, gapless, correctly-timed
  // sequence. The entire rhythm is computed up front on the AudioContext clock,
  // so it is immune to network/SSE/scheduler jitter — unlike one-note-per-event
  // playback where the spacing is whatever the delivery timing happened to be.
  // Each note may carry its own `gap` (silence after it); otherwise NOTE_GAP.
  function playMelody(notes = []) {
    const sequence = Array.isArray(notes) ? notes : [notes];
    if (sequence.length === 0) return;

    const audioCtx = getAudioContext();
    // Start after the lookahead, and never before anything already queued ends.
    let cursor = Math.max(audioCtx.currentTime + SCHEDULE_LOOKAHEAD, nextStartTimeRef.current);

    sequence.forEach((note = {}) => {
      const duration = scheduleVoice(audioCtx, note, cursor);
      const gap = Number.isFinite(Number(note.gap)) ? Math.max(Number(note.gap), 0) : NOTE_GAP;
      cursor += duration + gap;
    });

    nextStartTimeRef.current = cursor;
    return nextStartTimeRef.current - audioCtx.currentTime;
  }

  // Plays a full polyphonic score: an unordered list of notes, each with an
  // explicit `start` offset (seconds) from a shared origin. Because every note
  // is scheduled at an absolute audio-clock time, independent layers can overlap
  // freely — a held harmony chord keeps ringing while the melody moves above it
  // and the bass punctuates underneath. This is the only path that reserves the
  // playhead through the whole piece rather than note-by-note.
  function playScore(notes = []) {
    const score = Array.isArray(notes) ? notes : [notes];
    if (score.length === 0) return;

    const audioCtx = getAudioContext();
    const origin = Math.max(audioCtx.currentTime + SCHEDULE_LOOKAHEAD, nextStartTimeRef.current);
    let end = origin;

    score.forEach((note = {}) => {
      const offset = Number.isFinite(Number(note.start)) ? Math.max(Number(note.start), 0) : 0;
      const startTime = origin + offset;
      const duration = scheduleVoice(audioCtx, note, startTime);
      end = Math.max(end, startTime + duration);
    });

    // Reserve the clock through the end of the whole score so a following event
    // doesn't collide with notes still ringing out.
    nextStartTimeRef.current = end + NOTE_GAP;
    return nextStartTimeRef.current - audioCtx.currentTime;
  }

  const [isBouncing, setIsBouncing] = useState(false);
  const bounceTimeoutRef = useRef(null);

  // Keeps the emoji bouncing for exactly as long as the just-scheduled sound
  // takes to finish, so it doesn't idle-bounce in silence or cut off early.
  function bounceFor(durationSeconds) {
    clearTimeout(bounceTimeoutRef.current);
    if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
      setIsBouncing(false);
      return;
    }
    setIsBouncing(true);
    bounceTimeoutRef.current = setTimeout(() => setIsBouncing(false), durationSeconds * 1000);
  }

  useEffect(() => {
    const es = new EventSource("/sound-notifier-api/events");

    es.onmessage = (e) => {
      const data = JSON.parse(e.data);
      if (data.type === "score") {
        bounceFor(playScore(data.notes));
      } else if (data.type === "melody") {
        bounceFor(playMelody(data.notes));
      } else if (data.type === "chord") {
        bounceFor(playChord(data.sounds));
      } else if (data.type != "snapshot") {
        bounceFor(playChord([data.sound]));
      }
    };

    return () => {
      es.close();
      audioCtxRef.current?.close();
      clearTimeout(bounceTimeoutRef.current);
    };
  }, []);

  // Injected once so the emoji's bounce keyframes exist without a separate
  // CSS file (this plugin is loaded as a single dynamic-imported script).
  useEffect(() => {
    const styleId = "sound-notifier-emoji-style";
    if (document.getElementById(styleId)) return;
    const style = document.createElement("style");
    style.id = styleId;
    style.textContent = `
      @keyframes sound-notifier-bounce {
        0%, 100% { transform: translateY(0) scale(1); }
        50% { transform: translateY(-6px) scale(1.3); }
      }
      .sound-notifier-emoji {
        display: inline-block;
        margin-right: 6px;
      }
      .sound-notifier-emoji--bouncing {
        animation: sound-notifier-bounce 0.4s ease infinite;
      }
    `;
    document.head.appendChild(style);
  }, []);

  return React.createElement(
    "button",
    { onClick: () => bounceFor(playChord([{}])), type: "button" },
    `Airflow dance!`,
    React.createElement(
      "span",
      { className: `sound-notifier-emoji${isBouncing ? " sound-notifier-emoji--bouncing" : ""}` },
      " 🪩 💃",
    ),
  );
};
