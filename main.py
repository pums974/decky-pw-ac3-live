from __future__ import annotations

import asyncio
import contextlib
import ctypes
import datetime as dt
import glob
import json
import os
import pwd
import re
import shlex
import signal
from collections import deque
from typing import Any, Literal

# The decky plugin module is located at decky-loader/plugin
# For easy intellisense checkout the decky-loader code repo
# and add the `decky-loader/plugin/imports` path to `python.analysis.extraPaths` in `.vscode/settings.json`
import decky

LifecycleState = Literal["stopped", "starting", "running", "stopping", "error"]

ALLOWED_TRANSITIONS: dict[LifecycleState, set[LifecycleState]] = {
    "stopped": {"starting", "error"},
    "starting": {"running", "stopping", "error"},
    "running": {"stopping", "error"},
    "stopping": {"stopped", "error"},
    "error": {"starting", "stopping", "stopped", "error"},
}

LOG_BUFFER_MAX_LINES = 2000

DECK_HDMI_CARD = "alsa_card.pci-0000_04_00.1"
DECK_INTERNAL_SPEAKER_CARD = "alsa_card.pci-0000_04_00.5-platform-nau8821-max"
# Card name varies across Decks/SteamOS versions (...nau8821-max vs ...acp5x_mach.0); match by PCI address.
DECK_INTERNAL_SPEAKER_CARD_PREFIX = "alsa_card.pci-0000_04_00.5-"
DEFAULT_ALSA_DEVICE = "hw:0,8"
# Normal PipeWire profile for the dock HDMI output; restored when the pre-start profile was unusable ("off").
DECK_HDMI_DEFAULT_PROFILE = "output:hdmi-stereo-extra2"
DEFAULT_LATENCY = "1536/48000"
DEFAULT_FFMPEG_THREAD_QUEUE_SIZE = 4
DEFAULT_FFMPEG_CHUNK_FRAMES = 1536
DEFAULT_ALSA_LATENCY_US = 120000
DEFAULT_ALSA_IEC_CARD = 0
DEFAULT_ALSA_IEC_INDEX = 2
PIPELINE_INPUT_SINK = "pw-ac3-live-input"
# User-visible labels for the encoder's sink (Steam/game audio device lists). Applied at launch via
# PIPEWIRE_PROPS, which pw_stream_new merges over the encoder's built-in properties; node.name is kept
# because routing uses PIPELINE_INPUT_SINK.
PIPELINE_INPUT_LABEL = "Surround Sound Fix (5.1)"
PIPELINE_INPUT_PROPS = (
    "{ "
    f'node.description = "{PIPELINE_INPUT_LABEL}" '
    'node.nick = "Surround Sound Fix" '
    'application.name = "Surround Sound Fix" '
    "}"
)

# Latency presets for the ALSA output buffer (more headroom = fewer stutters, more delay).
LATENCY_PRESETS_US: dict[str, int] = {"low": 60000, "normal": 120000, "safe": 200000}
DEFAULT_LATENCY_PRESET = "normal"
SETTINGS_FILENAME = "settings.json"
ADVANCED_SETTING_KEYS = (
    "allow_external_binary_override",
    "binary_path",
    "alsa_device",
    "latency",
    "buffer_size",
    "output_buffer_size",
    "ffmpeg_thread_queue_size",
    "ffmpeg_chunk_frames",
    "alsa_iec_card",
    "alsa_iec_index",
)
DEFAULT_SETTINGS: dict[str, Any] = {
    "version": 1,
    "auto_start": True,
    "latency_preset": DEFAULT_LATENCY_PRESET,
    "advanced": {},
}

# Automation (display hotplug, sleep/resume, crash recovery).
DISPLAY_POLL_INTERVAL_S = 2.0
RESUME_SETTLE_S = 3.0
AUTO_RETRY_BACKOFF_S = (5.0, 15.0, 30.0)

# Helper processes (sleep inhibitor, logind monitor). plugin_loader.service uses KillMode=process, so when
# Decky restarts, children of the backend are NOT killed; they must die with us on their own.
SLEEP_INHIBIT_WHY = "Stop the AC-3 encoder cleanly before sleep"
SLEEP_INHIBIT_ARGS = (
    "systemd-inhibit",
    "--what=sleep",
    "--mode=delay",
    "--who=pw-ac3-live",
    f"--why={SLEEP_INHIBIT_WHY}",
)
# Held command: exits by itself when the backend dies (same trick SteamOS uses for SSH sessions),
# which makes systemd-inhibit exit and release the lock.
def _inhibit_hold_command() -> tuple[str, ...]:
    return ("tail", f"--pid={os.getpid()}", "-f", "/dev/null")

SLEEP_MONITOR_ARGS = (
    "gdbus",
    "monitor",
    "--system",
    "--dest",
    "org.freedesktop.login1",
    "--object-path",
    "/org/freedesktop/login1",
)
_PR_SET_PDEATHSIG = 1


def _setsid_and_die_with_parent() -> None:
    os.setsid()
    _die_with_parent()


def _die_with_parent() -> None:
    """preexec_fn: have the kernel SIGTERM this child when the backend dies (even via SIGKILL)."""
    with contextlib.suppress(Exception):
        ctypes.CDLL(None, use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0)

BUNDLED_BINARY_RELATIVE_PATH = os.path.join("bin", "pw-ac3-live")


class StartFailure(Exception):
    def __init__(self, error: dict[str, Any]) -> None:
        super().__init__(error["message"])
        self.error = error


class Plugin:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self._lifecycle_lock = asyncio.Lock()

        self._state: LifecycleState = "stopped"
        self._last_error: dict[str, Any] | None = None
        self._started_at: str | None = None
        self._last_transition_at = self._utc_now()

        self._encoder_process: asyncio.subprocess.Process | None = None
        self._encoder_pgid: int | None = None

        self._encoder_monitor_task: asyncio.Task[None] | None = None
        self._encoder_stderr_task: asyncio.Task[None] | None = None

        self._audio_snapshot: dict[str, Any] | None = None
        self._audio_state_modified = False
        self._start_config: dict[str, Any] | None = None

        self._logs: deque[dict[str, Any]] = deque(maxlen=LOG_BUFFER_MAX_LINES)
        self._next_log_seq = 1
        self._runtime_env_warning_logged = False

        self._settings: dict[str, Any] = json.loads(json.dumps(DEFAULT_SETTINGS))
        self._display_info: dict[str, Any] = self._empty_display_info()
        self._display_initialized = False
        self._want_running = False
        self._suspended = False
        self._resume_settle_until = 0.0
        self._auto_retry_count = 0
        self._next_auto_retry_at = 0.0
        self._display_task: asyncio.Task[None] | None = None
        self._sleep_task: asyncio.Task[None] | None = None
        self._sleep_monitor_proc: asyncio.subprocess.Process | None = None
        self._inhibitor_proc: asyncio.subprocess.Process | None = None

    # Diagnostic method for frontend/backend connectivity checks.
    async def add(self, left: int, right: int) -> int:
        return left + right

    async def long_running(self) -> None:
        await asyncio.sleep(15)
        await decky.emit("timer_event", "Hello from pw-ac3-live backend", True, 2)

    async def start_timer(self) -> None:
        if self.loop is None:
            self.loop = asyncio.get_running_loop()
        self.loop.create_task(self.long_running())

    async def start_encoder(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        # Manual start: remember that the user wants the encoder on (restored after sleep/replug).
        self._want_running = True
        self._reset_auto_retry()
        if config is None:
            config = self._config_from_settings()
        return await self._start_impl(config, source="manual")

    async def stop_encoder(
        self,
        force: bool = False,
        timeout_ms: int = 5000,
    ) -> dict[str, Any]:
        # Manual stop: stay off until the user starts again or a compatible TV is (re)connected.
        self._want_running = False
        return await self._stop_impl(force=force, timeout_ms=timeout_ms, source="stop")

    async def _start_impl(
        self,
        config: dict[str, Any] | None,
        source: str = "manual",
    ) -> dict[str, Any]:
        async with self._lifecycle_lock:
            if source != "manual" and self._suspended:
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": None,
                }

            if self._state not in {"stopped", "error"} or self._pipeline_is_alive_locked():
                error = self._make_error(
                    code="START_REJECTED_STATE",
                    message="Cannot start encoder from current state.",
                    detail=f"state={self._state}",
                    hint="Stop the current session before starting again.",
                )
                self._append_log("WARN", error["detail"])
                await self._emit_error(error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": error,
                }

            if self._audio_snapshot is not None or self._audio_state_modified:
                # A previous session died without restoring audio (e.g. encoder crash). Restore first,
                # otherwise the new snapshot would capture the half-modified state (HDMI profile "off").
                self._append_log("INFO", "Restoring leftover audio state from previous session before start.")
                await self._execute_cleanup_locked(force_stop=True, timeout_ms=5000, source="pre_start")

            normalized_config, config_error = self._normalize_start_config(config)
            if config_error is not None:
                self._last_error = config_error
                self._append_log("ERROR", f"{config_error['code']}: {config_error['detail']}")
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(config_error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": config_error,
                }

            self._transition_locked("starting")
            self._last_error = None
            self._start_config = normalized_config
            await self._emit_state_changed_locked()

            start_error: dict[str, Any] | None = None
            try:
                binary_path = self._resolve_binary_path(normalized_config)
                self._append_log("INFO", f"Resolved encoder binary: {binary_path}")

                snapshot, snapshot_error = await self._capture_audio_snapshot_locked()
                if snapshot_error is not None:
                    raise StartFailure(snapshot_error)
                self._audio_snapshot = snapshot

                pre_start_error = await self._apply_pre_start_audio_actions_locked()
                if pre_start_error is not None:
                    raise StartFailure(pre_start_error)

                encoder_command = self._build_encoder_command(binary_path, normalized_config)

                spawn_error = await self._spawn_pipeline_locked(encoder_command)
                if spawn_error is not None:
                    raise StartFailure(spawn_error)

                route_error = await self._route_audio_into_encoder_locked()
                if route_error is not None:
                    raise StartFailure(route_error)

            except StartFailure as failure:
                start_error = failure.error
            except Exception as exc:
                decky.logger.exception("Unhandled exception during start_encoder")
                self._append_log(
                    "ERROR",
                    f"Unhandled startup exception: {type(exc).__name__}: {exc}",
                )
                start_error = self._make_error(
                    code="START_PROCESS_FAILED",
                    message="Unexpected startup failure.",
                    detail=f"{type(exc).__name__}: {exc}",
                    hint="Inspect backend logs for stack trace and retry start.",
                )

            if start_error is not None:
                cleanup_error = await self._execute_cleanup_locked(
                    force_stop=True,
                    timeout_ms=5000,
                    source="start_failure",
                )
                if cleanup_error is not None:
                    self._append_log(
                        "ERROR",
                        (
                            "Cleanup after failed start had additional issues: "
                            f"{cleanup_error['code']} {cleanup_error['detail']}"
                        ),
                    )

                self._last_error = start_error
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(start_error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": start_error,
                }

            self._transition_locked("running")
            self._started_at = self._utc_now()
            await self._emit_state_changed_locked()
            self._append_log(
                "INFO",
                (
                    "Encoder running "
                    f"encoder_pid={self._active_encoder_pid_locked()} source={source} "
                    f"alsa_latency_us={normalized_config['alsa_latency_us']}"
                ),
            )
            return {
                "ok": True,
                "status": self._build_status_payload_locked(),
                "error": None,
            }

    async def _stop_impl(
        self,
        force: bool = False,
        timeout_ms: int = 5000,
        source: str = "stop",
        tolerate_restore_partial: bool = False,
    ) -> dict[str, Any]:
        start = asyncio.get_running_loop().time()
        normalized_timeout_ms = self._normalize_timeout_ms(timeout_ms)

        async with self._lifecycle_lock:
            has_runtime_handles = (
                self._pipeline_is_alive_locked()
                or self._audio_snapshot is not None
                or self._audio_state_modified
            )
            if self._state == "stopped" and not has_runtime_handles:
                error = self._make_error(
                    code="STOP_REJECTED_STATE",
                    message="Encoder is already stopped.",
                    detail=f"state={self._state}",
                    hint="Start the encoder before issuing stop.",
                )
                self._append_log("WARN", error["detail"])
                await self._emit_error(error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": error,
                    "stopped_pid": None,
                    "duration_ms": self._duration_ms(start),
                }

            if self._state == "stopping":
                error = self._make_error(
                    code="STOP_REJECTED_STATE",
                    message="Encoder stop is already in progress.",
                    detail=f"state={self._state}",
                    hint="Wait for the current stop transition to complete.",
                )
                self._append_log("WARN", error["detail"])
                await self._emit_error(error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": error,
                    "stopped_pid": None,
                    "duration_ms": self._duration_ms(start),
                }

            stopped_pid = self._active_encoder_pid_locked()
            if self._state != "stopped":
                self._transition_locked("stopping")
                await self._emit_state_changed_locked()

            cleanup_error = await self._execute_cleanup_locked(
                force_stop=force,
                timeout_ms=normalized_timeout_ms,
                source=source,
            )

            if (
                cleanup_error is not None
                and tolerate_restore_partial
                and cleanup_error.get("code") == "AUDIO_RESTORE_PARTIAL"
            ):
                # Expected when the HDMI sink vanished (unplug/sleep): nothing left to restore it to.
                self._append_log("WARN", f"Stop ({source}): {cleanup_error['detail']}")
                cleanup_error = None

            if cleanup_error is not None:
                self._last_error = cleanup_error
                self._append_log("ERROR", f"{cleanup_error['code']}: {cleanup_error['detail']}")
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(cleanup_error)
                return {
                    "ok": False,
                    "status": self._build_status_payload_locked(),
                    "error": cleanup_error,
                    "stopped_pid": stopped_pid,
                    "duration_ms": self._duration_ms(start),
                }

            self._last_error = None
            self._transition_locked("stopped")
            await self._emit_state_changed_locked()
            self._append_log("INFO", f"Encoder stopped and audio cleanup completed (source={source}).")
            return {
                "ok": True,
                "status": self._build_status_payload_locked(),
                "error": None,
                "stopped_pid": stopped_pid,
                "duration_ms": self._duration_ms(start),
            }

    async def get_status(self) -> dict[str, Any]:
        async with self._lifecycle_lock:
            return {
                "status": self._build_status_payload_locked(),
                "pipeline": {
                    "encoder_pid": self._active_encoder_pid_locked(),
                },
                "last_transition_at": self._last_transition_at,
                "display": self._display_info,
                "automation": self._automation_payload(),
            }

    async def get_recent_logs(
        self,
        limit: int = 200,
        since_seq: int | None = None,
    ) -> dict[str, Any]:
        async with self._lifecycle_lock:
            normalized_limit = self._normalize_log_limit(limit)
            normalized_since = self._normalize_since_seq(since_seq)

            entries = list(self._logs)
            if normalized_since is not None:
                entries = [entry for entry in entries if entry["seq"] > normalized_since]

            truncated = len(entries) > normalized_limit
            if truncated:
                entries = entries[-normalized_limit:]

            return {
                "entries": entries,
                "next_seq": self._next_log_seq,
                "truncated": truncated,
            }

    # ------------------------------------------------------------------ settings API

    async def get_settings(self) -> dict[str, Any]:
        return {
            "settings": self._settings,
            "latency_presets_ms": {k: v // 1000 for k, v in LATENCY_PRESETS_US.items()},
            "display": self._display_info,
            "automation": self._automation_payload(),
        }

    async def set_settings(self, patch: dict[str, Any] | None = None) -> dict[str, Any]:
        if not isinstance(patch, dict):
            patch = {}
        candidate = json.loads(json.dumps(self._settings))

        if "auto_start" in patch:
            if not isinstance(patch["auto_start"], bool):
                return self._settings_error("auto_start must be a boolean.", patch["auto_start"])
            candidate["auto_start"] = patch["auto_start"]

        if "latency_preset" in patch:
            if patch["latency_preset"] not in LATENCY_PRESETS_US:
                return self._settings_error("Unknown latency preset.", patch["latency_preset"])
            candidate["latency_preset"] = patch["latency_preset"]

        if "advanced" in patch:
            advanced = patch["advanced"]
            if not isinstance(advanced, dict):
                return self._settings_error("advanced must be an object.", advanced)
            merged = dict(candidate.get("advanced") or {})
            for key, value in advanced.items():
                if key not in ADVANCED_SETTING_KEYS:
                    continue
                if value is None or value == "":
                    merged.pop(key, None)  # back to built-in default
                else:
                    merged[key] = value
            candidate["advanced"] = merged

        _, config_error = self._normalize_start_config(self._config_from_settings(candidate))
        if config_error is not None:
            return {"ok": False, "settings": self._settings, "error": config_error}

        encoder_affecting = (
            candidate["latency_preset"] != self._settings["latency_preset"]
            or candidate["advanced"] != self._settings["advanced"]
        )
        auto_enabled_now = candidate["auto_start"] and not self._settings["auto_start"]
        self._settings = candidate
        self._save_settings()
        self._append_log("INFO", f"Settings updated: {json.dumps(patch, sort_keys=True)}")
        await decky.emit("settings_changed", self._settings)

        if auto_enabled_now and self._display_info.get("needs_encoder"):
            self._want_running = True
            self._reset_auto_retry()

        if encoder_affecting:
            async with self._lifecycle_lock:
                running = self._state == "running"
            if running and self.loop is not None:
                self.loop.create_task(self._restart_for_settings())

        return {"ok": True, "settings": self._settings, "error": None}

    def _settings_error(self, message: str, value: Any) -> dict[str, Any]:
        return {
            "ok": False,
            "settings": self._settings,
            "error": self._make_error(
                code="SETTINGS_INVALID",
                message=message,
                detail=f"value={value!r}",
                hint="Check the setting value.",
            ),
        }

    async def _restart_for_settings(self) -> None:
        self._append_log("INFO", "Restarting encoder to apply new settings.")
        await self._stop_impl(source="settings_restart", tolerate_restore_partial=True)
        result = await self._start_impl(self._config_from_settings(), source="settings_restart")
        if result.get("ok"):
            await self._notify("Encoder restarted with new settings.")

    def _settings_path(self) -> str:
        return os.path.join(decky.DECKY_PLUGIN_SETTINGS_DIR, SETTINGS_FILENAME)

    def _load_settings(self) -> None:
        settings = json.loads(json.dumps(DEFAULT_SETTINGS))
        try:
            with open(self._settings_path(), encoding="utf-8") as handle:
                stored = json.load(handle)
            if isinstance(stored, dict):
                if isinstance(stored.get("auto_start"), bool):
                    settings["auto_start"] = stored["auto_start"]
                if stored.get("latency_preset") in LATENCY_PRESETS_US:
                    settings["latency_preset"] = stored["latency_preset"]
                if isinstance(stored.get("advanced"), dict):
                    settings["advanced"] = {
                        k: v for k, v in stored["advanced"].items() if k in ADVANCED_SETTING_KEYS
                    }
        except FileNotFoundError:
            pass
        except Exception as exc:
            self._append_log("WARN", f"Could not read settings, using defaults: {exc}")

        _, config_error = self._normalize_start_config(self._config_from_settings(settings))
        if config_error is not None:
            self._append_log("WARN", f"Stored advanced settings invalid, ignoring them: {config_error['detail']}")
            settings["advanced"] = {}
        self._settings = settings
        self._append_log("INFO", f"Settings loaded: {json.dumps(settings, sort_keys=True)}")

    def _save_settings(self) -> None:
        path = self._settings_path()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp_path = f"{path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(self._settings, handle, indent=2, sort_keys=True)
            os.replace(tmp_path, path)
        except Exception as exc:
            self._append_log("ERROR", f"Could not save settings to {path}: {exc}")

    def _config_from_settings(self, settings: dict[str, Any] | None = None) -> dict[str, Any]:
        settings = settings or self._settings
        config = dict(settings.get("advanced") or {})
        config["alsa_latency_us"] = LATENCY_PRESETS_US.get(
            settings.get("latency_preset", DEFAULT_LATENCY_PRESET),
            LATENCY_PRESETS_US[DEFAULT_LATENCY_PRESET],
        )
        return config

    def _automation_payload(self) -> dict[str, Any]:
        return {
            "want_running": self._want_running,
            "suspended": self._suspended,
            "auto_retry_count": self._auto_retry_count,
        }

    async def _notify(self, message: str) -> None:
        self._append_log("INFO", f"Notice: {message}")
        with contextlib.suppress(Exception):
            await decky.emit("automation_notice", message)

    def _reset_auto_retry(self) -> None:
        self._auto_retry_count = 0
        self._next_auto_retry_at = 0.0

    # ------------------------------------------------------------------ display detection

    @staticmethod
    def _empty_display_info() -> dict[str, Any]:
        return {
            "connected": False,
            "name": None,
            "manufacture_id": None,
            "product_id": None,
            "lpcm_channels": 0,
            "ac3_channels": 0,
            "needs_encoder": False,
            "eld": None,
        }

    def _read_display_info(self) -> dict[str, Any]:
        """Read the HDMI sink's ELD (what the TV says it can play).

        needs_encoder: the TV accepts AC-3 5.1 but only stereo LPCM, i.e. multichannel PCM would be
        downmixed/cut to stereo while Dolby Digital gets through.
        """
        card = (self._settings.get("advanced") or {}).get("alsa_iec_card", DEFAULT_ALSA_IEC_CARD)
        info = self._empty_display_info()
        for path in sorted(glob.glob(f"/proc/asound/card{card}/eld#*")):
            try:
                with open(path, encoding="utf-8", errors="replace") as handle:
                    text = handle.read()
            except OSError:
                continue
            fields: dict[str, str] = {}
            for line in text.splitlines():
                parts = line.split(None, 1)
                if len(parts) == 2:
                    fields[parts[0]] = parts[1].strip()
            if fields.get("monitor_present") != "1" or fields.get("eld_valid") != "1":
                continue

            lpcm_channels = 0
            ac3_channels = 0
            for key, value in fields.items():
                match = re.fullmatch(r"sad(\d+)_coding_type", key)
                if match is None:
                    continue
                try:
                    channels = int(fields.get(f"sad{match.group(1)}_channels", "0"))
                except ValueError:
                    channels = 0
                if re.search(r"\]\s*LPCM\b", value):
                    lpcm_channels = max(lpcm_channels, channels)
                elif re.search(r"\]\s*AC-3\b", value):
                    ac3_channels = max(ac3_channels, channels)

            info.update(
                connected=True,
                name=fields.get("monitor_name") or None,
                manufacture_id=fields.get("manufacture_id"),
                product_id=fields.get("product_id"),
                lpcm_channels=lpcm_channels,
                ac3_channels=ac3_channels,
                needs_encoder=ac3_channels >= 6 and lpcm_channels < 6,
                eld=os.path.basename(path),
            )
            break
        return info

    async def _display_monitor_loop(self) -> None:
        while True:
            try:
                await self._on_display_info(self._read_display_info())
                await self._reconcile()
            except asyncio.CancelledError:
                raise
            except Exception:
                decky.logger.exception("display monitor iteration failed")
            await asyncio.sleep(DISPLAY_POLL_INTERVAL_S)

    async def _on_display_info(self, info: dict[str, Any]) -> None:
        previous = self._display_info
        first = not self._display_initialized
        self._display_initialized = True
        if not first and info == previous:
            return
        self._display_info = info

        if info["connected"]:
            self._append_log(
                "INFO",
                (
                    f"Display: {info['name']!r} ({info['manufacture_id']}/{info['product_id']}) "
                    f"LPCM {info['lpcm_channels']}ch, AC-3 {info['ac3_channels']}ch, "
                    f"needs_encoder={info['needs_encoder']}"
                ),
            )
        else:
            self._append_log("INFO", "Display: no HDMI audio sink connected")
        with contextlib.suppress(Exception):
            await decky.emit("display_changed", info)

        if info["connected"] and not first and not previous.get("connected"):
            # HDMI just (re)appeared (plug-in, wake, Game Mode switch): let PipeWire rebuild the card first.
            self._resume_settle_until = asyncio.get_running_loop().time() + RESUME_SETTLE_S

        became_compatible = info["needs_encoder"] and (first or not previous.get("needs_encoder"))
        if became_compatible and self._settings.get("auto_start"):
            self._append_log("INFO", "Auto-start: TV needs AC-3 for 5.1, requesting encoder start")
            self._want_running = True
            self._reset_auto_retry()

    async def _reconcile(self) -> None:
        """Drive the encoder toward the desired state (called every display poll)."""
        if self._suspended:
            return
        loop = asyncio.get_running_loop()
        now = loop.time()

        async with self._lifecycle_lock:
            state = self._state
            alive = self._pipeline_is_alive_locked()

        if state == "running" and not self._display_info["connected"]:
            self._append_log("INFO", "HDMI display disconnected; stopping encoder (will restart on reconnect).")
            await self._stop_impl(source="display_lost", tolerate_restore_partial=True)
            return

        if not (self._want_running and self._display_info["connected"]):
            return
        if state not in {"stopped", "error"} or alive:
            return
        if now < self._resume_settle_until or now < self._next_auto_retry_at:
            return
        if self._auto_retry_count > len(AUTO_RETRY_BACKOFF_S):
            return

        result = await self._start_impl(self._config_from_settings(), source="auto")
        if result.get("ok"):
            self._reset_auto_retry()
            name = self._display_info.get("name") or "TV"
            await self._notify(f"Dolby Digital 5.1 encoder started for {name}.")
            return
        if self._suspended or result.get("error") is None:
            return

        if self._auto_retry_count < len(AUTO_RETRY_BACKOFF_S):
            delay = AUTO_RETRY_BACKOFF_S[self._auto_retry_count]
            self._auto_retry_count += 1
            self._next_auto_retry_at = now + delay
            self._append_log("WARN", f"Auto-start failed; retry {self._auto_retry_count} in {delay:.0f}s")
        else:
            self._auto_retry_count += 1
            self._want_running = False
            self._append_log("ERROR", "Auto-start failed repeatedly; giving up until next reconnect or manual start.")
            await self._notify("Encoder failed to start. Open Surround Sound Fix for details.")

    # ------------------------------------------------------------------ sleep / resume

    async def _acquire_sleep_inhibitor(self) -> None:
        proc = self._inhibitor_proc
        if proc is not None and proc.returncode is None:
            return
        try:
            self._inhibitor_proc = await asyncio.create_subprocess_exec(
                *SLEEP_INHIBIT_ARGS,
                *_inhibit_hold_command(),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
                preexec_fn=_die_with_parent,
                env=self._build_subprocess_env(),
            )
        except Exception as exc:
            self._inhibitor_proc = None
            self._append_log("WARN", f"Could not take sleep delay lock: {exc}")

    async def _release_sleep_inhibitor(self) -> None:
        proc = self._inhibitor_proc
        self._inhibitor_proc = None
        if proc is None or proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, signal.SIGKILL)

    async def _sleep_monitor_loop(self) -> None:
        while True:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *SLEEP_MONITOR_ARGS,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                    preexec_fn=_die_with_parent,
                )
                self._sleep_monitor_proc = proc
                assert proc.stdout is not None
                async for raw_line in proc.stdout:
                    match = re.search(r"PrepareForSleep \((true|false)", raw_line.decode(errors="replace"))
                    if match is not None:
                        await self._on_prepare_for_sleep(match.group(1) == "true")
                await proc.wait()
                self._append_log("WARN", f"Sleep monitor exited rc={proc.returncode}; restarting")
            except asyncio.CancelledError:
                raise
            except Exception:
                decky.logger.exception("sleep monitor failed")
            await asyncio.sleep(5.0)

    async def _on_prepare_for_sleep(self, going_to_sleep: bool) -> None:
        loop = asyncio.get_running_loop()
        if going_to_sleep:
            self._suspended = True
            self._append_log("INFO", "System is going to sleep")
            async with self._lifecycle_lock:
                active = (
                    self._state in {"running", "starting", "error"}
                    or self._pipeline_is_alive_locked()
                    or self._audio_snapshot is not None
                )
            if active:
                await self._stop_impl(source="suspend", timeout_ms=3000, tolerate_restore_partial=True)
            await self._release_sleep_inhibitor()
        else:
            self._append_log("INFO", "System resumed from sleep")
            self._suspended = False
            self._resume_settle_until = loop.time() + RESUME_SETTLE_S
            self._reset_auto_retry()
            await self._acquire_sleep_inhibitor()

    def _kill_orphaned_helpers(self) -> list[int]:
        """Kill helper processes leaked by a previous backend instance (re-parented to PID 1).

        Returns the PIDs of orphaned encoders that were signalled (their audio changes need restoring).
        """
        killed_encoders: list[int] = []
        inhibit_prefix = "\0".join(SLEEP_INHIBIT_ARGS) + "\0"
        monitor_cmdline = "\0".join(SLEEP_MONITOR_ARGS)
        uid = os.getuid()
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/cmdline", "rb") as handle:
                    cmdline = handle.read().rstrip(b"\0").decode(errors="replace")
                args = cmdline.split("\0")
                is_encoder = args[0].endswith("/pw-ac3-live") and "--alsa-direct" in args
                if not (cmdline.startswith(inhibit_prefix) or cmdline == monitor_cmdline or is_encoder):
                    continue
                st = os.stat(f"/proc/{entry}")
                with open(f"/proc/{entry}/stat", encoding="utf-8") as handle:
                    ppid = int(handle.read().rsplit(")", 1)[1].split()[1])
            except (OSError, ValueError, IndexError):
                continue
            if st.st_uid != uid or ppid != 1:
                continue
            with contextlib.suppress(ProcessLookupError, PermissionError):
                # systemd-inhibit is a session leader (start_new_session): take its held child with it.
                if (cmdline.startswith(inhibit_prefix) or is_encoder) and os.getpgid(int(entry)) == int(entry):
                    os.killpg(int(entry), signal.SIGTERM)
                else:
                    os.kill(int(entry), signal.SIGTERM)
                if is_encoder:
                    killed_encoders.append(int(entry))
                self._append_log("INFO", f"Killed orphaned helper from a previous instance: pid={entry} {args[0]}")
        return killed_encoders

    async def _restore_after_orphaned_encoder(self, pids: list[int]) -> None:
        """An encoder from a previous instance was running: wait for it, then undo its audio changes."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 5.0
        while loop.time() < deadline and any(os.path.exists(f"/proc/{pid}") for pid in pids):
            await asyncio.sleep(0.1)
        for pid in pids:
            if os.path.exists(f"/proc/{pid}"):
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(pid, signal.SIGKILL)
        hdmi_sink = DECK_HDMI_CARD.replace("alsa_card.", "alsa_output.", 1) + "." + DECK_HDMI_DEFAULT_PROFILE.split(":", 1)[1]
        for command in (
            ["iecset", "-c", "0", "-n", "2", "audio", "on"],
            ["pactl", "set-card-profile", DECK_HDMI_CARD, DECK_HDMI_DEFAULT_PROFILE],
        ):
            await self._run_command(command, timeout_s=5.0, check=False)
        await asyncio.sleep(0.5)
        await self._run_command(["pactl", "set-default-sink", hdmi_sink], timeout_s=5.0, check=False)
        self._append_log("INFO", f"Restored HDMI audio after orphaned encoder (default sink {hdmi_sink}).")

    async def _stop_automation(self) -> None:
        for task in (self._display_task, self._sleep_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._display_task = None
        self._sleep_task = None
        proc = self._sleep_monitor_proc
        self._sleep_monitor_proc = None
        if proc is not None and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
        await self._release_sleep_inhibitor()

    # Asyncio-compatible long-running code, executed in a task when the plugin is loaded
    async def _main(self) -> None:
        self.loop = asyncio.get_event_loop()
        self._append_log("INFO", "pw-ac3-live backend loaded")
        self._load_settings()
        orphaned_encoders = self._kill_orphaned_helpers()
        if orphaned_encoders:
            await self._restore_after_orphaned_encoder(orphaned_encoders)
        async with self._lifecycle_lock:
            await self._emit_state_changed_locked()
        await self._acquire_sleep_inhibitor()
        self._sleep_task = self.loop.create_task(self._sleep_monitor_loop())
        self._display_task = self.loop.create_task(self._display_monitor_loop())

    # Function called first during the unload process, utilize this to handle your plugin being stopped, but not
    # completely removed
    async def _unload(self) -> None:
        self._append_log("INFO", "pw-ac3-live backend unloading")
        await self._stop_automation()
        await self._cleanup_for_lifecycle("unload")

    # Function called after `_unload` during uninstall, utilize this to clean up processes and other remnants of your
    # plugin that may remain on the system
    async def _uninstall(self) -> None:
        self._append_log("INFO", "pw-ac3-live backend uninstall")
        await self._stop_automation()
        await self._cleanup_for_lifecycle("uninstall")

    # Migrations that should be performed before entering `_main()`.
    async def _migration(self) -> None:
        self._append_log("INFO", "Running migrations for decky-pw-ac3-live")

        decky.migrate_logs(
            os.path.join(
                decky.DECKY_USER_HOME,
                ".config",
                "decky-pw-ac3-live",
                "plugin.log",
            )
        )
        decky.migrate_settings(
            os.path.join(decky.DECKY_HOME, "settings", "decky-pw-ac3-live.json"),
            os.path.join(decky.DECKY_USER_HOME, ".config", "decky-pw-ac3-live"),
        )
        decky.migrate_runtime(
            os.path.join(decky.DECKY_HOME, "decky-pw-ac3-live"),
            os.path.join(decky.DECKY_USER_HOME, ".local", "share", "decky-pw-ac3-live"),
        )

    def _normalize_start_config(
        self,
        config: dict[str, Any] | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        if config is None:
            config = {}
        if not isinstance(config, dict):
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="start_encoder config must be an object.",
                detail=f"received_type={type(config).__name__}",
                hint="Pass a JSON object to start_encoder.",
            )

        allow_external = config.get("allow_external_binary_override", False)
        if not isinstance(allow_external, bool):
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="allow_external_binary_override must be a boolean.",
                detail=f"allow_external_binary_override={allow_external!r}",
                hint="Use true or false.",
            )

        binary_path = config.get("binary_path")
        if binary_path is not None and (not isinstance(binary_path, str) or not binary_path.strip()):
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="binary_path must be a non-empty string when provided.",
                detail=f"binary_path={binary_path!r}",
                hint="Provide an absolute path to an executable for dev override.",
            )

        latency = config.get("latency", DEFAULT_LATENCY)
        if not isinstance(latency, str) or re.fullmatch(r"\d+/48000", latency) is None:
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="latency must match N/48000 format.",
                detail=f"latency={latency!r}",
                hint="Use values like 1536/48000.",
            )

        def parse_optional_positive_int(name: str) -> tuple[int | None, dict[str, Any] | None]:
            raw = config.get(name)
            if raw is None:
                return None, None
            if not isinstance(raw, int) or raw <= 0:
                return None, self._make_error(
                    code="CONFIG_INVALID",
                    message=f"{name} must be a positive integer when provided.",
                    detail=f"{name}={raw!r}",
                    hint=f"Set {name} to a value greater than zero or omit it.",
                )
            return raw, None

        ffmpeg_thread_queue_size = config.get(
            "ffmpeg_thread_queue_size",
            DEFAULT_FFMPEG_THREAD_QUEUE_SIZE,
        )
        if not isinstance(ffmpeg_thread_queue_size, int) or ffmpeg_thread_queue_size < 1:
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="ffmpeg_thread_queue_size must be >= 1.",
                detail=f"ffmpeg_thread_queue_size={ffmpeg_thread_queue_size!r}",
                hint="Use a positive integer.",
            )

        ffmpeg_chunk_frames = config.get("ffmpeg_chunk_frames", DEFAULT_FFMPEG_CHUNK_FRAMES)
        if not isinstance(ffmpeg_chunk_frames, int) or ffmpeg_chunk_frames < 1:
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="ffmpeg_chunk_frames must be >= 1.",
                detail=f"ffmpeg_chunk_frames={ffmpeg_chunk_frames!r}",
                hint="Use a positive integer.",
            )

        alsa_latency_us = config.get("alsa_latency_us", DEFAULT_ALSA_LATENCY_US)
        if not isinstance(alsa_latency_us, int) or alsa_latency_us <= 0:
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="alsa_latency_us must be > 0.",
                detail=f"alsa_latency_us={alsa_latency_us!r}",
                hint="Use a positive integer.",
            )

        alsa_iec_card = config.get("alsa_iec_card", DEFAULT_ALSA_IEC_CARD)
        if not isinstance(alsa_iec_card, int) or alsa_iec_card < 0:
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="alsa_iec_card must be >= 0.",
                detail=f"alsa_iec_card={alsa_iec_card!r}",
                hint="Use a non-negative integer card index.",
            )

        alsa_iec_index = config.get("alsa_iec_index", DEFAULT_ALSA_IEC_INDEX)
        if not isinstance(alsa_iec_index, int) or alsa_iec_index < 0:
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="alsa_iec_index must be >= 0.",
                detail=f"alsa_iec_index={alsa_iec_index!r}",
                hint="Use a non-negative integer IEC958 index.",
            )

        alsa_device = config.get("alsa_device", DEFAULT_ALSA_DEVICE)
        if not isinstance(alsa_device, str) or not alsa_device.strip():
            return None, self._make_error(
                code="CONFIG_INVALID",
                message="alsa_device must be a non-empty string.",
                detail=f"alsa_device={alsa_device!r}",
                hint="Use a valid ALSA device identifier such as hw:0,8.",
            )

        buffer_size, buffer_size_error = parse_optional_positive_int("buffer_size")
        if buffer_size_error is not None:
            return None, buffer_size_error

        output_buffer_size, output_buffer_size_error = parse_optional_positive_int(
            "output_buffer_size"
        )
        if output_buffer_size_error is not None:
            return None, output_buffer_size_error

        normalized = {
            "allow_external_binary_override": allow_external,
            "binary_path": binary_path.strip() if isinstance(binary_path, str) else None,
            "alsa_device": alsa_device.strip(),
            "latency": latency,
            "buffer_size": buffer_size,
            "output_buffer_size": output_buffer_size,
            "ffmpeg_thread_queue_size": ffmpeg_thread_queue_size,
            "ffmpeg_chunk_frames": ffmpeg_chunk_frames,
            "alsa_latency_us": alsa_latency_us,
            "alsa_iec_card": alsa_iec_card,
            "alsa_iec_index": alsa_iec_index,
        }
        return normalized, None

    def _resolve_binary_path(self, config: dict[str, Any]) -> str:
        runtime_candidate = os.path.join(decky.DECKY_PLUGIN_RUNTIME_DIR, BUNDLED_BINARY_RELATIVE_PATH)
        plugin_candidate = os.path.join(decky.DECKY_PLUGIN_DIR, BUNDLED_BINARY_RELATIVE_PATH)

        for candidate in (runtime_candidate, plugin_candidate):
            if os.path.exists(candidate):
                if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                    return candidate
                raise StartFailure(
                    self._make_error(
                        code="BUNDLED_BINARY_NOT_EXECUTABLE",
                        message="Bundled pw-ac3-live binary is not executable.",
                        detail=f"path={candidate}",
                        hint="Ensure bin/pw-ac3-live has executable permissions in the plugin package.",
                    )
                )

        allow_external = config["allow_external_binary_override"]
        binary_override = config["binary_path"]
        if allow_external and binary_override is not None:
            if os.path.isfile(binary_override) and os.access(binary_override, os.X_OK):
                self._append_log("WARN", f"Using external binary override path={binary_override}")
                return binary_override
            raise StartFailure(
                self._make_error(
                    code="BINARY_NOT_FOUND",
                    message="External binary override path is invalid or not executable.",
                    detail=f"path={binary_override}",
                    hint="Use an executable file path or disable external override.",
                )
            )

        raise StartFailure(
            self._make_error(
                code="BUNDLED_BINARY_MISSING",
                message="Bundled pw-ac3-live binary was not found in plugin paths.",
                detail=(
                    f"checked={runtime_candidate}, {plugin_candidate}; "
                    f"allow_external_binary_override={allow_external}"
                ),
                hint="Install a package that includes bin/pw-ac3-live.",
            )
        )

    def _build_encoder_command(self, binary_path: str, config: dict[str, Any]) -> list[str]:
        command = [
            binary_path,
            "--alsa-direct",
            "--target",
            config["alsa_device"],
            "--alsa-latency-us",
            str(config["alsa_latency_us"]),
            "--alsa-iec-card",
            str(config["alsa_iec_card"]),
            "--alsa-iec-index",
            str(config["alsa_iec_index"]),
            "--latency",
            config["latency"],
            "--ffmpeg-thread-queue-size",
            str(config["ffmpeg_thread_queue_size"]),
            "--ffmpeg-chunk-frames",
            str(config["ffmpeg_chunk_frames"]),
        ]
        if config["buffer_size"] is not None:
            command.extend(["--buffer-size", str(config["buffer_size"])])
        if config["output_buffer_size"] is not None:
            command.extend(["--output-buffer-size", str(config["output_buffer_size"])])
        return command

    async def _spawn_pipeline_locked(
        self,
        encoder_command: list[str],
    ) -> dict[str, Any] | None:
        encoder_cmd_str = self._command_display(encoder_command)
        self._append_log("INFO", f"Starting encoder: {encoder_cmd_str}")

        encoder: asyncio.subprocess.Process | None = None
        encoder_pgid: int | None = None
        subprocess_env = self._build_subprocess_env()
        subprocess_env["PIPEWIRE_PROPS"] = PIPELINE_INPUT_PROPS

        try:
            encoder = await asyncio.create_subprocess_exec(
                *encoder_command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                env=subprocess_env,
                # Own process group (for clean group termination) + SIGTERM if the backend dies, so a
                # Decky restart (KillMode=process) can't leave an unmanaged encoder holding hw:0,8.
                preexec_fn=_setsid_and_die_with_parent,
            )
        except FileNotFoundError as exc:
            return self._make_error(
                code="DEPENDENCY_MISSING",
                message="Encoder binary is missing.",
                detail=f"command={encoder_cmd_str} error={exc}",
                hint="Verify bundled pw-ac3-live binary exists and is executable.",
            )
        except PermissionError as exc:
            return self._make_error(
                code="PERMISSION_DENIED",
                message="Encoder process could not be spawned due to permissions.",
                detail=f"command={encoder_cmd_str} error={exc}",
                hint="Check Decky plugin execution permissions.",
            )
        except Exception as exc:
            return self._make_error(
                code="START_PROCESS_FAILED",
                message="Failed to spawn encoder process.",
                detail=f"command={encoder_cmd_str} error={exc}",
                hint="Check logs and command arguments.",
            )

        encoder_pgid = os.getpgid(encoder.pid)
        self._encoder_process = encoder
        self._encoder_pgid = encoder_pgid

        if encoder.stderr is not None:
            self._encoder_stderr_task = asyncio.create_task(
                self._consume_stream(encoder.stderr, "encoder-stderr", encoder.pid),
                name=f"pw-ac3-live-encoder-stderr-{encoder.pid}",
            )

        self._encoder_monitor_task = asyncio.create_task(
            self._watch_pipeline_exit(encoder.pid),
            name=f"pw-ac3-live-monitor-{encoder.pid}",
        )

        self._append_log(
            "INFO",
            (
                "Encoder process spawned "
                f"encoder_pid={encoder.pid} encoder_pgid={encoder_pgid}"
            ),
        )
        return None

    async def _watch_pipeline_exit(self, encoder_pid: int) -> None:
        encoder = self._encoder_process
        if encoder is None:
            return
        if encoder.pid != encoder_pid:
            return

        encoder_rc = await encoder.wait()

        async with self._lifecycle_lock:
            if not self._pipeline_matches_locked(encoder_pid):
                return

            self._append_log(
                "ERROR",
                (
                    "Encoder process exited unexpectedly "
                    f"encoder_pid={encoder_pid} encoder_rc={encoder_rc}"
                ),
            )
            await self._clear_pipeline_handles_locked()

            if self._state in {"running", "starting"}:
                error = self._make_error(
                    code="PIPELINE_BROKEN",
                    message="Encoder process exited unexpectedly.",
                    detail=f"encoder_pid={encoder_pid} encoder_rc={encoder_rc}",
                    hint="Inspect recent logs and restart encoder.",
                )
                self._last_error = error
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(error)
                return

            if self._state == "stopping":
                self._transition_locked("stopped")
                await self._emit_state_changed_locked()

    async def _capture_audio_snapshot_locked(
        self,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        snapshot: dict[str, Any] = {
            "captured_at": self._utc_now(),
            "default_sink": None,
            "hdmi_profile": None,
            "internal_speaker_profile": None,
            "iecset": None,
            "amixer": {
                "Master": None,
                "PCM": None,
                "IEC958,2": None,
            },
        }

        default_sink_res = await self._run_command(["pactl", "get-default-sink"], timeout_s=5.0)
        if not default_sink_res["ok"]:
            return None, self._command_failure_to_error(
                default_sink_res,
                code="AUDIO_SNAPSHOT_FAILED",
                message="Failed to snapshot default sink before startup.",
                hint="Verify pactl availability and PipeWire runtime state.",
            )

        cards_res = await self._run_command(["pactl", "list", "cards"], timeout_s=8.0)
        if not cards_res["ok"]:
            return None, self._command_failure_to_error(
                cards_res,
                code="AUDIO_SNAPSHOT_FAILED",
                message="Failed to snapshot card profiles before startup.",
                hint="Verify pactl availability and card access.",
            )

        snapshot["default_sink"] = default_sink_res["stdout"].strip() or None
        cards_dump = cards_res["stdout"]
        hdmi_profile = self._extract_card_active_profile(cards_dump, DECK_HDMI_CARD)
        if hdmi_profile in (None, "off"):
            # Happens right after HDMI hotplug / Game Mode switch while PipeWire rebuilds the card.
            # Never restore to "off" (that would leave the user without HDMI audio).
            self._append_log(
                "INFO",
                f"HDMI profile was {hdmi_profile!r} at start; will restore {DECK_HDMI_DEFAULT_PROFILE} on stop",
            )
            hdmi_profile = DECK_HDMI_DEFAULT_PROFILE
        snapshot["hdmi_profile"] = hdmi_profile
        internal_card = self._find_card_name(cards_dump, DECK_INTERNAL_SPEAKER_CARD_PREFIX)
        snapshot["internal_speaker_card"] = internal_card
        snapshot["internal_speaker_profile"] = (
            self._extract_card_active_profile(cards_dump, internal_card) if internal_card else None
        )

        iecset_res = await self._run_command(
            ["iecset", "-c", "0", "-n", "2"],
            timeout_s=5.0,
            check=False,
        )
        if iecset_res["ok"]:
            snapshot["iecset"] = self._parse_iecset_state(iecset_res["stdout"] + "\n" + iecset_res["stderr"])

        for control in ("Master", "PCM", "IEC958,2"):
            amixer_res = await self._run_command(
                ["amixer", "-c", "0", "get", control],
                timeout_s=5.0,
                check=False,
            )
            if amixer_res["ok"]:
                snapshot["amixer"][control] = self._parse_amixer_state(amixer_res["stdout"])

        self._append_log(
            "INFO",
            (
                "Audio snapshot captured "
                f"default_sink={snapshot['default_sink']} "
                f"hdmi_profile={snapshot['hdmi_profile']} "
                f"internal_speaker_profile={snapshot['internal_speaker_profile']}"
            ),
        )
        return snapshot, None

    async def _apply_pre_start_audio_actions_locked(self) -> dict[str, Any] | None:
        self._audio_state_modified = True
        release_error = await self._release_hdmi_device_locked()
        if release_error is not None:
            return release_error

        mandatory_steps = [
            (
                ["iecset", "-c", "0", "-n", "2", "audio", "off", "rate", "48000"],
                "disable IEC958 audio before pipeline start",
            ),
        ]

        for command, description in mandatory_steps:
            result = await self._run_command(command, timeout_s=8.0)
            if result["ok"]:
                continue
            return self._command_failure_to_error(
                result,
                code="START_PROCESS_FAILED",
                message=f"Failed to {description}.",
                hint="Inspect command detail in logs and verify Steam Deck audio state.",
            )

        master_error = await self._apply_amixer_control_with_fallback_locked(
            controls=["Master", "Speaker", "Headphone"],
            description="set Master to 100% unmuted",
            include_volume=True,
        )
        if master_error is not None:
            return master_error

        pcm_error = await self._apply_amixer_control_with_fallback_locked(
            controls=["PCM"],
            description="set PCM to 100% unmuted",
            include_volume=True,
        )
        if pcm_error is not None:
            return pcm_error

        iec958_error = await self._apply_amixer_control_with_fallback_locked(
            controls=["IEC958,2", "IEC958"],
            description="set IEC958,2 unmuted",
            include_volume=False,
        )
        if iec958_error is not None:
            return iec958_error

        return None

    def _hdmi_pcm_status_path(self) -> str | None:
        device = (self._start_config or {}).get("alsa_device", DEFAULT_ALSA_DEVICE)
        match = re.fullmatch(r"(?:plug)?hw:(\d+),(\d+)", str(device).strip())
        if match is None:
            return None
        return f"/proc/asound/card{match.group(1)}/pcm{match.group(2)}p/sub0/status"

    def _hdmi_pcm_is_closed(self) -> bool:
        path = self._hdmi_pcm_status_path()
        if path is None:
            return True
        try:
            with open(path, encoding="utf-8") as handle:
                return handle.read().strip().startswith("closed")
        except OSError:
            return True

    async def _release_hdmi_device_locked(self) -> dict[str, Any] | None:
        """Make PipeWire let go of the HDMI PCM so the encoder can own it.

        After an HDMI hotplug (or the Game Mode switch, which looks like one) PipeWire can report the
        card profile as "off" while a stale sink node still holds the device open. ALSA's HDMI config
        locks the IEC958 control while the device is open, so iecset then fails with EPERM.
        Cycling the profile on -> off tears that stale node down.
        """
        await self._run_command(
            ["pactl", "set-card-profile", DECK_HDMI_CARD, DECK_HDMI_DEFAULT_PROFILE],
            timeout_s=8.0,
            check=False,
        )
        await asyncio.sleep(0.3)
        off_res = await self._run_command(
            ["pactl", "set-card-profile", DECK_HDMI_CARD, "off"],
            timeout_s=8.0,
        )
        if not off_res["ok"]:
            return self._command_failure_to_error(
                off_res,
                code="START_PROCESS_FAILED",
                message="Failed to set HDMI card profile off.",
                hint="Inspect command detail in logs and verify Steam Deck audio state.",
            )

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 3.0
        while not self._hdmi_pcm_is_closed():
            if loop.time() >= deadline:
                return self._make_error(
                    code="HDMI_DEVICE_BUSY",
                    message="The HDMI audio device is still in use by another program.",
                    detail=f"{self._hdmi_pcm_status_path()} is not closed after setting the HDMI profile off",
                    hint="Turn the encoder off and on again. If it persists, reboot.",
                )
            await asyncio.sleep(0.1)
        return None

    async def _apply_amixer_control_with_fallback_locked(
        self,
        controls: list[str],
        description: str,
        include_volume: bool,
    ) -> dict[str, Any] | None:
        missing_controls: list[str] = []

        for control in controls:
            command = ["amixer", "-c", "0", "set", control]
            if include_volume:
                command.append("100%")
            command.append("unmute")

            result = await self._run_command(command, timeout_s=8.0, check=False)
            if result["ok"]:
                if control != controls[0]:
                    self._append_log(
                        "WARN",
                        f"{description}: fallback control '{control}' used.",
                    )
                return None

            if self._is_missing_amixer_control(result):
                missing_controls.append(control)
                continue

            return self._command_failure_to_error(
                result,
                code="START_PROCESS_FAILED",
                message=f"Failed to {description}.",
                hint="Inspect command detail in logs and verify Steam Deck audio mixer controls.",
            )

        self._append_log(
            "WARN",
            (
                f"{description}: no matching mixer control found, continuing. "
                f"checked={missing_controls}"
            ),
        )
        return None

    async def _route_audio_into_encoder_locked(self) -> dict[str, Any] | None:
        sink_error = await self._wait_for_sink_locked(PIPELINE_INPUT_SINK, timeout_s=6.0)
        if sink_error is not None:
            return sink_error

        set_default_res = await self._run_command(
            ["pactl", "set-default-sink", PIPELINE_INPUT_SINK],
            timeout_s=5.0,
        )
        if not set_default_res["ok"]:
            return self._command_failure_to_error(
                set_default_res,
                code="START_PROCESS_FAILED",
                message="Failed to set pw-ac3-live-input as default sink.",
                hint="Inspect pactl output and ensure pw-ac3-live-input exists.",
            )

        move_res = await self._run_command(
            ["pactl", "list", "short", "sink-inputs"],
            timeout_s=5.0,
            check=False,
        )
        if move_res["error"] is not None:
            return self._command_failure_to_error(
                move_res,
                code="START_PROCESS_FAILED",
                message="Failed to query sink inputs for move operation.",
                hint="Verify pactl availability.",
            )
        if move_res["returncode"] != 0:
            return self._make_error(
                code="START_PROCESS_FAILED",
                message="Failed to list sink inputs before move.",
                detail=self._command_result_detail(move_res),
                hint="Inspect pactl output and retry.",
            )

        for line in move_res["stdout"].splitlines():
            parts = line.split()
            if not parts:
                continue
            sink_input_id = parts[0]
            if not sink_input_id.isdigit():
                continue

            move_input_res = await self._run_command(
                ["pactl", "move-sink-input", sink_input_id, PIPELINE_INPUT_SINK],
                timeout_s=5.0,
                check=False,
            )
            if move_input_res["ok"]:
                continue

            if self._is_no_such_entity_error(move_input_res):
                self._append_log(
                    "WARN",
                    (
                        f"Sink input {sink_input_id} disappeared before move; "
                        "continuing routing."
                    ),
                )
                continue

            if move_input_res["returncode"] != 0:
                return self._command_failure_to_error(
                    move_input_res,
                    code="START_PROCESS_FAILED",
                    message=f"Failed to move sink input {sink_input_id}.",
                    hint="Retry start and inspect pactl errors.",
                )

        set_volume_res = await self._run_command(
            ["pactl", "set-sink-volume", PIPELINE_INPUT_SINK, "100%"],
            timeout_s=5.0,
        )
        if not set_volume_res["ok"]:
            return self._command_failure_to_error(
                set_volume_res,
                code="START_PROCESS_FAILED",
                message="Failed to set pw-ac3-live-input volume to 100%.",
                hint="Ensure sink exists and pactl can modify it.",
            )

        set_mute_res = await self._run_command(
            ["pactl", "set-sink-mute", PIPELINE_INPUT_SINK, "0"],
            timeout_s=5.0,
        )
        if not set_mute_res["ok"]:
            return self._command_failure_to_error(
                set_mute_res,
                code="START_PROCESS_FAILED",
                message="Failed to unmute pw-ac3-live-input.",
                hint="Ensure sink exists and pactl can modify it.",
            )

        self._append_log("INFO", "Routed playback into pw-ac3-live-input")
        return None

    async def _wait_for_sink_locked(
        self,
        sink_name: str,
        timeout_s: float,
    ) -> dict[str, Any] | None:
        deadline = asyncio.get_running_loop().time() + timeout_s
        while asyncio.get_running_loop().time() < deadline:
            sinks_res = await self._run_command(
                ["pactl", "list", "short", "sinks"],
                timeout_s=3.0,
                check=False,
            )
            if sinks_res["error"] is not None:
                return self._command_failure_to_error(
                    sinks_res,
                    code="START_PROCESS_FAILED",
                    message="Failed while waiting for pw-ac3-live-input sink.",
                    hint="Inspect pactl availability and PipeWire state.",
                )
            if sinks_res["returncode"] == 0 and self._sink_exists_in_short_list(
                sinks_res["stdout"], sink_name
            ):
                return None
            await asyncio.sleep(0.2)

        return self._make_error(
            code="START_PROCESS_FAILED",
            message="pw-ac3-live-input sink did not appear in time.",
            detail=f"sink={sink_name} timeout_s={timeout_s}",
            hint="Check pw-ac3-live encoder logs and PipeWire state.",
        )

    async def _execute_cleanup_locked(
        self,
        force_stop: bool,
        timeout_ms: int,
        source: str,
    ) -> dict[str, Any] | None:
        stop_error = await self._terminate_pipeline_group_locked(force=force_stop, timeout_ms=timeout_ms)

        restore_failures: list[str] = []

        should_restore_audio = self._audio_state_modified or self._audio_snapshot is not None
        if should_restore_audio:
            iecset_on_res = await self._run_command(
                ["iecset", "-c", "0", "-n", "2", "audio", "on"],
                timeout_s=5.0,
                check=False,
            )
            if not iecset_on_res["ok"]:
                restore_failures.append(
                    f"force_iecset_audio_on: {self._command_result_detail(iecset_on_res)}"
                )

            restore_failures.extend(await self._restore_audio_state_locked())

        self._audio_snapshot = None
        self._audio_state_modified = False
        self._start_config = None
        self._started_at = None

        if stop_error is not None:
            if restore_failures:
                self._append_log(
                    "ERROR",
                    "Restore failures after stop error: " + "; ".join(restore_failures),
                )
            return stop_error

        if restore_failures:
            return self._make_error(
                code="AUDIO_RESTORE_PARTIAL",
                message="Cleanup completed but some audio restore steps failed.",
                detail="; ".join(restore_failures),
                hint="Use recovery actions or manually verify sink/profile state.",
            )

        self._append_log("INFO", f"Cleanup sequence completed for source={source}")
        return None

    async def _restore_audio_state_locked(self) -> list[str]:
        snapshot = self._audio_snapshot
        if snapshot is None:
            return []

        failures: list[str] = []

        hdmi_profile = snapshot.get("hdmi_profile")
        if hdmi_profile:
            step_failure = await self._run_restore_step(
                ["pactl", "set-card-profile", DECK_HDMI_CARD, hdmi_profile],
                "restore_hdmi_profile",
            )
            if step_failure is not None:
                failures.append(step_failure)

        default_sink = snapshot.get("default_sink")
        if default_sink:
            step_failure = await self._run_restore_step(
                ["pactl", "set-default-sink", default_sink],
                "restore_default_sink",
            )
            if step_failure is not None:
                failures.append(step_failure)

        internal_profile = snapshot.get("internal_speaker_profile")
        if internal_profile:
            step_failure = await self._run_restore_step(
                [
                    "pactl",
                    "set-card-profile",
                    snapshot.get("internal_speaker_card") or DECK_INTERNAL_SPEAKER_CARD,
                    internal_profile,
                ],
                "restore_internal_speaker_profile",
            )
            if step_failure is not None:
                failures.append(step_failure)

        iecset_state = snapshot.get("iecset") or {}
        iecset_audio = iecset_state.get("audio")
        if iecset_audio in {"on", "off"}:
            iecset_command = ["iecset", "-c", "0", "-n", "2", "audio", iecset_audio]
            iecset_rate = iecset_state.get("rate")
            if isinstance(iecset_rate, int) and iecset_rate > 0:
                iecset_command.extend(["rate", str(iecset_rate)])
            step_failure = await self._run_restore_step(iecset_command, "restore_iecset_audio")
            if step_failure is not None:
                failures.append(step_failure)

        amixer_state = snapshot.get("amixer") or {}
        for control in ("Master", "PCM"):
            control_state = amixer_state.get(control)
            command = self._build_restore_amixer_command(control, control_state)
            if command is None:
                continue
            step_failure = await self._run_restore_step(command, f"restore_{control.lower()}")
            if step_failure is not None:
                failures.append(step_failure)

        iec958_state = amixer_state.get("IEC958,2")
        iec958_command = self._build_restore_amixer_command("IEC958,2", iec958_state)
        if iec958_command is not None:
            step_failure = await self._run_restore_step(iec958_command, "restore_iec958_2")
            if step_failure is not None:
                failures.append(step_failure)

        return failures

    def _build_restore_amixer_command(
        self,
        control: str,
        control_state: dict[str, Any] | None,
    ) -> list[str] | None:
        if not isinstance(control_state, dict):
            return None

        unmuted = control_state.get("unmuted")
        volume = control_state.get("volume")
        if unmuted is None and volume is None:
            return None

        command = ["amixer", "-c", "0", "set", control]
        if isinstance(volume, int) and volume >= 0:
            command.append(f"{volume}%")
        if isinstance(unmuted, bool):
            command.append("unmute" if unmuted else "mute")
        return command

    async def _run_restore_step(self, command: list[str], step_name: str) -> str | None:
        result = await self._run_command(command, timeout_s=6.0, check=False)
        if result["ok"]:
            return None
        return f"{step_name}: {self._command_result_detail(result)}"

    async def _terminate_pipeline_group_locked(
        self,
        force: bool,
        timeout_ms: int,
    ) -> dict[str, Any] | None:
        if not self._pipeline_is_alive_locked():
            await self._clear_pipeline_handles_locked()
            return None

        encoder_pid = self._active_encoder_pid_locked()
        if encoder_pid is None:
            await self._clear_pipeline_handles_locked()
            return None

        pgid, ownership_error = self._resolve_owned_pgid_locked(encoder_pid)
        if ownership_error is not None:
            return ownership_error
        if pgid is None:
            await self._clear_pipeline_handles_locked()
            return None
        pgids, collect_error = self._collect_encoder_pgids_locked(encoder_pgid=pgid)
        if collect_error is not None:
            return collect_error

        timeout_s = timeout_ms / 1000.0
        if force:
            self._append_log("INFO", f"Force-stopping process groups pgids={pgids} with SIGKILL")
            signal_error = self._send_signal_to_groups(pgids, signal.SIGKILL)
            if signal_error is not None:
                return signal_error

            exited = await self._wait_for_pipeline_exit_locked(timeout_s if timeout_s > 0 else 2.0)
            if not exited:
                return self._make_error(
                    code="STOP_TIMEOUT",
                    message="Encoder did not terminate after SIGKILL.",
                    detail=f"pgids={pgids} timeout_ms={timeout_ms}",
                    hint="Retry stop or restart Decky.",
                )

            await self._clear_pipeline_handles_locked()
            return None

        grace_s = min(timeout_s, 3.0) if timeout_s > 0 else 3.0
        hard_s = max(timeout_s - grace_s, 0.0)
        if hard_s <= 0:
            hard_s = 2.0

        self._append_log("INFO", f"Stopping process groups pgids={pgids} with SIGTERM")
        signal_error = self._send_signal_to_groups(pgids, signal.SIGTERM)
        if signal_error is not None:
            return signal_error

        exited_gracefully = await self._wait_for_pipeline_exit_locked(grace_s)
        if exited_gracefully:
            await self._clear_pipeline_handles_locked()
            return None

        self._append_log("WARN", f"Process groups pgids={pgids} still alive, escalating to SIGKILL")
        signal_error = self._send_signal_to_groups(pgids, signal.SIGKILL)
        if signal_error is not None:
            return signal_error

        exited_after_kill = await self._wait_for_pipeline_exit_locked(hard_s)
        if not exited_after_kill:
            return self._make_error(
                code="STOP_TIMEOUT",
                message="Encoder process group did not terminate in time.",
                detail=f"pgids={pgids} timeout_ms={timeout_ms}",
                hint="Retry stop or restart Decky.",
            )

        await self._clear_pipeline_handles_locked()
        return None

    async def _wait_for_pipeline_exit_locked(self, timeout_s: float) -> bool:
        waits: list[asyncio.Task[int]] = []
        if self._encoder_process is not None and self._encoder_process.returncode is None:
            waits.append(asyncio.create_task(self._encoder_process.wait()))

        if not waits:
            return True

        try:
            await asyncio.wait_for(asyncio.gather(*waits), timeout=timeout_s)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            for task in waits:
                if task.done():
                    continue
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _clear_pipeline_handles_locked(self) -> None:
        await self._cancel_task_locked(self._encoder_stderr_task)
        await self._cancel_task_locked(self._encoder_monitor_task)

        self._encoder_stderr_task = None
        self._encoder_monitor_task = None

        self._encoder_process = None
        self._encoder_pgid = None

    async def _cancel_task_locked(self, task: asyncio.Task[None] | None) -> None:
        if task is None or task.done():
            return
        if task is asyncio.current_task():
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _consume_stream(
        self,
        stream: asyncio.StreamReader,
        stream_name: str,
        pid: int,
    ) -> None:
        while True:
            try:
                raw = await stream.readline()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._append_log(
                    "ERROR",
                    f"{stream_name} reader failed for pid={pid}: {exc}",
                )
                return

            if not raw:
                return

            message = raw.decode(errors="replace").rstrip()
            if message:
                self._append_log("INFO", f"{stream_name}[{pid}]: {message}")

    async def _run_command(
        self,
        command: list[str],
        timeout_s: float,
        check: bool = True,
    ) -> dict[str, Any]:
        command_display = self._command_display(command)

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._build_subprocess_env(),
            )
        except FileNotFoundError as exc:
            error = self._make_error(
                code="DEPENDENCY_MISSING",
                message="Required command is not available.",
                detail=f"command={command_display} error={exc}",
                hint="Install the missing dependency and retry.",
            )
            return {
                "ok": False,
                "returncode": None,
                "stdout": "",
                "stderr": "",
                "error": error,
                "command": command_display,
            }
        except PermissionError as exc:
            error = self._make_error(
                code="PERMISSION_DENIED",
                message="Command execution failed due to permissions.",
                detail=f"command={command_display} error={exc}",
                hint="Check plugin permissions and retry.",
            )
            return {
                "ok": False,
                "returncode": None,
                "stdout": "",
                "stderr": "",
                "error": error,
                "command": command_display,
            }
        except Exception as exc:
            error = self._make_error(
                code="START_PROCESS_FAILED",
                message="Failed to spawn required command.",
                detail=f"command={command_display} error={exc}",
                hint="Inspect runtime logs for details.",
            )
            return {
                "ok": False,
                "returncode": None,
                "stdout": "",
                "stderr": "",
                "error": error,
                "command": command_display,
            }

        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(process.communicate(), timeout=timeout_s)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=1.0)
            error = self._make_error(
                code="START_PROCESS_FAILED",
                message="Command timed out.",
                detail=f"command={command_display} timeout_s={timeout_s}",
                hint="Inspect runtime state and retry.",
            )
            return {
                "ok": False,
                "returncode": None,
                "stdout": "",
                "stderr": "",
                "error": error,
                "command": command_display,
            }

        stdout = stdout_bytes.decode(errors="replace")
        stderr = stderr_bytes.decode(errors="replace")
        returncode = process.returncode

        ok = returncode == 0
        error: dict[str, Any] | None = None
        if not ok and check:
            error = self._make_error(
                code="START_PROCESS_FAILED",
                message="Command returned a non-zero exit code.",
                detail=(
                    f"command={command_display} returncode={returncode} "
                    f"stdout={stdout.strip()!r} stderr={stderr.strip()!r}"
                ),
                hint="Inspect command stderr and verify audio/runtime state.",
            )

        return {
            "ok": ok,
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
            "error": error,
            "command": command_display,
        }

    async def _cleanup_for_lifecycle(self, source: str) -> None:
        async with self._lifecycle_lock:
            if self._state in {"running", "starting", "error"}:
                self._transition_locked("stopping")
                await self._emit_state_changed_locked()

            cleanup_error = await self._execute_cleanup_locked(
                force_stop=True,
                timeout_ms=5000,
                source=source,
            )
            if cleanup_error is not None:
                self._last_error = cleanup_error
                self._transition_locked("error")
                await self._emit_state_changed_locked()
                await self._emit_error(cleanup_error)
                self._append_log("ERROR", f"Lifecycle cleanup failed for {source}: {cleanup_error}")
                return

            self._last_error = None
            if self._state != "stopped":
                self._transition_locked("stopped")
                await self._emit_state_changed_locked()
            self._append_log("INFO", f"Lifecycle cleanup completed for {source}.")

    def _build_subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()

        runtime_dir = self._resolve_audio_runtime_dir()
        if runtime_dir is not None:
            env["XDG_RUNTIME_DIR"] = runtime_dir

            pulse_socket = os.path.join(runtime_dir, "pulse", "native")
            if os.path.exists(pulse_socket):
                env["PULSE_SERVER"] = f"unix:{pulse_socket}"

            bus_socket = os.path.join(runtime_dir, "bus")
            if os.path.exists(bus_socket):
                env["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path={bus_socket}"

            env.setdefault("PIPEWIRE_RUNTIME_DIR", runtime_dir)

            if not self._runtime_env_warning_logged and not os.path.exists(pulse_socket):
                self._runtime_env_warning_logged = True
                self._append_log(
                    "WARN",
                    (
                        "Pulse socket not found while preparing subprocess env: "
                        f"{pulse_socket}"
                    ),
                )

        return env

    def _resolve_audio_runtime_dir(self) -> str | None:
        candidates: list[str] = []

        env_runtime = os.environ.get("XDG_RUNTIME_DIR")
        if isinstance(env_runtime, str) and env_runtime.strip():
            candidates.append(env_runtime.strip())

        current_uid_runtime = f"/run/user/{os.getuid()}"
        if current_uid_runtime not in candidates:
            candidates.append(current_uid_runtime)

        decky_user_uid = self._resolve_decky_user_uid()
        if decky_user_uid is not None:
            decky_runtime = f"/run/user/{decky_user_uid}"
            if decky_runtime not in candidates:
                candidates.append(decky_runtime)

        for candidate in candidates:
            if os.path.isdir(candidate):
                return candidate

        return candidates[0] if candidates else None

    @staticmethod
    def _resolve_decky_user_uid() -> int | None:
        decky_user = getattr(decky, "DECKY_USER", None)
        if not isinstance(decky_user, str) or not decky_user:
            return None

        try:
            return pwd.getpwnam(decky_user).pw_uid
        except KeyError:
            return None

    async def _emit_state_changed_locked(self) -> None:
        await self._emit_safe("encoder_state_changed", self._build_status_payload_locked())

    async def _emit_error(self, error: dict[str, Any]) -> None:
        await self._emit_safe("encoder_error", error)

    async def _emit_safe(self, event: str, *args: Any) -> None:
        try:
            await decky.emit(event, *args)
        except Exception as exc:
            self._append_log("WARN", f"Failed to emit event {event}: {exc}")

    def _transition_locked(self, next_state: LifecycleState) -> None:
        if next_state == self._state:
            return
        if next_state not in ALLOWED_TRANSITIONS[self._state]:
            raise RuntimeError(f"illegal transition {self._state} -> {next_state}")
        self._state = next_state
        self._last_transition_at = self._utc_now()

    def _build_status_payload_locked(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "pid": self._active_encoder_pid_locked(),
            "started_at": self._started_at,
            "last_error": self._last_error,
        }

    def _pipeline_is_alive_locked(self) -> bool:
        return self._active_encoder_pid_locked() is not None

    def _active_encoder_pid_locked(self) -> int | None:
        if self._encoder_process is None:
            return None
        if self._encoder_process.returncode is not None:
            return None
        return self._encoder_process.pid

    def _pipeline_matches_locked(self, encoder_pid: int) -> bool:
        if self._encoder_process is None:
            return False
        return self._encoder_process.pid == encoder_pid

    def _resolve_owned_pgid_locked(self, encoder_pid: int) -> tuple[int | None, dict[str, Any] | None]:
        observed_pgid: int | None = None
        with contextlib.suppress(ProcessLookupError):
            observed_pgid = os.getpgid(encoder_pid)

        if observed_pgid is None:
            return self._encoder_pgid, None

        if self._encoder_pgid is None:
            self._encoder_pgid = observed_pgid
            return observed_pgid, None

        if observed_pgid != self._encoder_pgid:
            return None, self._make_error(
                code="PIPELINE_BROKEN",
                message="Managed process-group ownership changed unexpectedly.",
                detail=(
                    f"encoder_pid={encoder_pid} "
                    f"expected_pgid={self._encoder_pgid} actual_pgid={observed_pgid}"
                ),
                hint="Do not reuse external processes; restart encoder.",
            )

        return observed_pgid, None

    def _collect_encoder_pgids_locked(
        self,
        encoder_pgid: int | None,
    ) -> tuple[list[int], dict[str, Any] | None]:
        if encoder_pgid is None:
            return [], None
        return [encoder_pgid], None

    def _send_signal_to_groups(
        self,
        pgids: list[int],
        signum: signal.Signals,
    ) -> dict[str, Any] | None:
        for pgid in pgids:
            signal_error = self._send_group_signal(pgid, signum)
            if signal_error is not None:
                return signal_error
        return None

    def _send_group_signal(self, pgid: int, signum: signal.Signals) -> dict[str, Any] | None:
        try:
            os.killpg(pgid, signum)
            return None
        except ProcessLookupError:
            return None
        except PermissionError as exc:
            return self._make_error(
                code="PERMISSION_DENIED",
                message="Insufficient permission to signal encoder process group.",
                detail=f"pgid={pgid} signal={signum.name} error={exc}",
                hint="Verify plugin runtime permissions.",
            )
        except Exception as exc:
            return self._make_error(
                code="PIPELINE_BROKEN",
                message="Failed to signal encoder process group.",
                detail=f"pgid={pgid} signal={signum.name} error={exc}",
                hint="Inspect logs and retry stop.",
            )

    def _append_log(self, level: str, message: str) -> None:
        normalized_level = level.upper()
        entry = {
            "seq": self._next_log_seq,
            "ts": self._utc_now(),
            "level": normalized_level,
            "message": message,
        }
        self._next_log_seq += 1
        self._logs.append(entry)

        logger_method_name = normalized_level.lower()
        if logger_method_name == "warn":
            logger_method_name = "warning"
        logger_method = getattr(decky.logger, logger_method_name, decky.logger.info)
        logger_method(message)

    def _make_error(self, code: str, message: str, detail: str, hint: str) -> dict[str, Any]:
        return {
            "code": code,
            "message": message,
            "detail": detail,
            "hint": hint,
            "timestamp": self._utc_now(),
        }

    def _command_failure_to_error(
        self,
        command_result: dict[str, Any],
        code: str,
        message: str,
        hint: str,
    ) -> dict[str, Any]:
        embedded_error = command_result.get("error")
        if embedded_error is not None:
            return {
                "code": embedded_error["code"] if embedded_error.get("code") else code,
                "message": message,
                "detail": self._command_result_detail(command_result),
                "hint": hint,
                "timestamp": self._utc_now(),
            }

        return self._make_error(
            code=code,
            message=message,
            detail=self._command_result_detail(command_result),
            hint=hint,
        )

    def _command_result_detail(self, command_result: dict[str, Any]) -> str:
        command = command_result.get("command")
        returncode = command_result.get("returncode")
        stdout = (command_result.get("stdout") or "").strip()
        stderr = (command_result.get("stderr") or "").strip()
        embedded_error = command_result.get("error")
        embedded_detail = ""
        if embedded_error is not None:
            embedded_detail = embedded_error.get("detail", "")

        return (
            f"command={command} returncode={returncode} stdout={stdout!r} "
            f"stderr={stderr!r} error_detail={embedded_detail!r}"
        )

    @staticmethod
    def _command_display(command: list[str]) -> str:
        return " ".join(shlex.quote(part) for part in command)

    @staticmethod
    def _find_card_name(cards_dump: str, prefix: str) -> str | None:
        for raw_line in cards_dump.splitlines():
            line = raw_line.strip()
            if line.startswith("Name:"):
                name = line.split("Name:", 1)[1].strip()
                if name.startswith(prefix):
                    return name
        return None

    @staticmethod
    def _extract_card_active_profile(cards_dump: str, card_name: str) -> str | None:
        current_name: str | None = None
        current_profile: str | None = None

        for raw_line in cards_dump.splitlines():
            line = raw_line.strip()
            if line.startswith("Name:"):
                if current_name == card_name:
                    return current_profile
                current_name = line.split("Name:", 1)[1].strip()
                current_profile = None
            elif line.startswith("Active Profile:"):
                current_profile = line.split("Active Profile:", 1)[1].strip()

        if current_name == card_name:
            return current_profile
        return None

    @staticmethod
    def _parse_iecset_state(raw_output: str) -> dict[str, Any]:
        parsed: dict[str, Any] = {"audio": None, "rate": None}

        audio_match = re.search(r"\baudio\b[^a-zA-Z]*(on|off)", raw_output, flags=re.IGNORECASE)
        if audio_match is not None:
            parsed["audio"] = audio_match.group(1).lower()

        rate_match = re.search(r"\brate\b[^0-9]*(\d+)", raw_output, flags=re.IGNORECASE)
        if rate_match is not None:
            try:
                parsed["rate"] = int(rate_match.group(1))
            except ValueError:
                parsed["rate"] = None

        return parsed

    @staticmethod
    def _parse_amixer_state(raw_output: str) -> dict[str, Any]:
        state: dict[str, Any] = {"unmuted": None, "volume": None}

        switch_match = re.search(r"\[(on|off)\]", raw_output, flags=re.IGNORECASE)
        if switch_match is not None:
            state["unmuted"] = switch_match.group(1).lower() == "on"

        volume_match = re.search(r"\[(\d{1,3})%\]", raw_output)
        if volume_match is not None:
            try:
                state["volume"] = int(volume_match.group(1))
            except ValueError:
                state["volume"] = None

        return state

    @staticmethod
    def _sink_exists_in_short_list(sinks_output: str, sink_name: str) -> bool:
        for line in sinks_output.splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            if parts[1] == sink_name:
                return True
        return False

    @staticmethod
    def _is_missing_amixer_control(command_result: dict[str, Any]) -> bool:
        haystack = " ".join(
            [
                str(command_result.get("stdout", "")),
                str(command_result.get("stderr", "")),
                command_result.get("error", {}).get("detail", "")
                if isinstance(command_result.get("error"), dict)
                else "",
            ]
        ).lower()

        return any(
            needle in haystack
            for needle in (
                "unable to find simple control",
                "cannot find the given element from control",
                "amixer: unable to find simple control",
            )
        )

    @staticmethod
    def _is_no_such_entity_error(command_result: dict[str, Any]) -> bool:
        haystack = " ".join(
            [
                str(command_result.get("stdout", "")),
                str(command_result.get("stderr", "")),
                command_result.get("error", {}).get("detail", "")
                if isinstance(command_result.get("error"), dict)
                else "",
            ]
        ).lower()

        return any(
            needle in haystack
            for needle in (
                "no such entity",
                "not found",
                "does not exist",
            )
        )

    def _normalize_timeout_ms(self, timeout_ms: int) -> int:
        if not isinstance(timeout_ms, int):
            return 5000
        return max(timeout_ms, 0)

    def _normalize_log_limit(self, limit: int) -> int:
        if not isinstance(limit, int):
            return 200
        return max(1, min(limit, LOG_BUFFER_MAX_LINES))

    def _normalize_since_seq(self, since_seq: int | None) -> int | None:
        if since_seq is None:
            return None
        if not isinstance(since_seq, int):
            return None
        return max(since_seq, 0)

    def _duration_ms(self, start: float) -> int:
        now = asyncio.get_running_loop().time()
        return max(0, int((now - start) * 1000))

    @staticmethod
    def _utc_now() -> str:
        return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
