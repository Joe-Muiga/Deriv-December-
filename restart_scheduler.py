"""
restart_scheduler.py – rolling redeploy scheduler.

Spec point 11 (Aug 2026): the bot force-redeploys itself every
config.REDEPLOY_INTERVAL_HOURS hours, measured from the last actual
redeploy/resumption — NOT anchored to a fixed clock time. Replaces the
previous fixed-Africa/Nairobi-midnight schedule (_next_nairobi_midnight()),
which ignored config.REDEPLOY_INTERVAL_HOURS entirely and always fired once
a day regardless of what that config value was set to.

Public interface (unchanged contract with bot_engine.py):
  - is_redeploy_pending() -> bool
        True once the rolling timer has fired and a redeploy is due.
        bot_engine.py's _main_loop() checks this to pause taking on new
        trades, and _settle_loop() checks it to know when to start
        draining open contracts before actually redeploying.
  - trigger_redeploy() -> None
        Called by bot_engine.py's _settle_loop() ONLY once every open
        contract has been actively, confirmably closed (or the drain
        window has been exceeded and the redeploy is being delayed —
        in which case this is NOT called, see bot_engine.py Fix G).
        Starts the restart + clear-cache + deploy sequence (Render API)
        and clears the pending flag. See the sequence block below.
  - run_scheduler() -> coroutine
        The rolling timer loop itself. Started as a background task by
        bot_engine.py's run() alongside its other loops.

Draining coordination lives entirely in bot_engine.py (it owns the open
contracts and the reconciliation / Multiplier max-hold machinery) — this
module only owns the schedule and the actual deploy-hook trigger.
"""

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests

import config

logger = logging.getLogger(__name__)

# ── Module-level scheduler state ────────────────────────────────────────────
_pending: bool = False
_last_triggered_at: float = 0.0          # last time trigger_redeploy() ran at all
_last_confirmed_redeploy_at: float = 0.0  # last time it actually succeeded (Task 7)
_last_scheduled_at: float = 0.0
_process_start_time: float = time.time()
_last_watchdog_warning_at: float = 0.0
_hook_missing_warned_at: float = 0.0

# Task 7 fix — soft/optional dashboard visibility. restart_scheduler.py
# doesn't otherwise depend on keep_alive.py; imported lazily and guarded
# so a missing/broken keep_alive never breaks the scheduler itself.
def _push_dashboard_flag(**kwargs) -> None:
    try:
        import keep_alive
        keep_alive.update_status(**kwargs)
    except Exception:
        pass


_handoff_until: float = 0.0


def interval_secs() -> float:
    """Oct 2026: strict leg length. REDEPLOY_INTERVAL_SECS wins over the old hours setting."""
    s = getattr(config, "REDEPLOY_INTERVAL_SECS", 0)
    return float(s) if s else float(getattr(config, "REDEPLOY_INTERVAL_HOURS", 3)) * 3600


def in_handoff() -> bool:
    """True from the moment the deploy hook fires until the new instance replaces
    this one (or REDEPLOY_HANDOFF_MAX_SECS passes). bot_engine takes no new
    entries meanwhile so two instances never trade the same token at once."""
    return time.time() < _handoff_until


def is_redeploy_pending() -> bool:
    """True once the rolling REDEPLOY_INTERVAL_HOURS timer has fired and a
    redeploy is due but hasn't been confirmed-triggered yet."""
    return _pending


async def run_scheduler():
    """
    Sleeps for config.REDEPLOY_INTERVAL_HOURS, measured from the moment
    this loop iteration starts (i.e. from the last actual redeploy or from
    process start on the very first iteration) — not anchored to any fixed
    wall-clock time. Sets the pending flag, then repeats.
    bot_engine.py's _settle_loop() is responsible for noticing
    is_redeploy_pending() == True, draining open contracts for real, and
    calling trigger_redeploy() once that's done (which starts the next
    interval).
    """
    global _pending, _last_scheduled_at

    while True:
        try:
            if _sequence_state != "idle":      # restart/deploy sequence owns the process now
                await asyncio.sleep(10)
                continue
            interval_secs_ = interval_secs()
            interval_hours = interval_secs_ / 3600
            interval_secs_v = interval_secs_
            _last_scheduled_at = time.time()

            next_fire = datetime.fromtimestamp(
                _last_scheduled_at + interval_secs_v, tz=timezone.utc
            )
            logger.info(
                f"REDEPLOY SCHEDULER: next redeploy at {next_fire.isoformat()} "
                f"({interval_secs_v:.0f}s from now, rolling) — sleeping {interval_secs_v:.0f}s"
            )
            await asyncio.sleep(interval_secs_v)

            if _sequence_state != "idle":
                continue
            _pending = True
            logger.warning(
                f"REDEPLOY DUE: {interval_hours}h rolling interval elapsed — "
                f"waiting for bot_engine to drain open contracts before "
                f"actually redeploying"
            )

            # Wait here until bot_engine.py's _settle_loop() confirms the
            # drain completed and calls trigger_redeploy() (which clears
            # _pending). Poll rather than block indefinitely so a stuck
            # drain doesn't wedge this loop forever — just keep logging.
            waited = 0
            while _pending:
                await asyncio.sleep(30)
                waited += 30
                if waited % 600 == 0:
                    logger.warning(
                        f"REDEPLOY STILL PENDING: {waited}s since due — "
                        f"bot_engine is still draining open contracts "
                        f"rather than wiping their bookkeeping"
                    )

            # Loop repeats: the next interval is measured from HERE (i.e.
            # from the actual redeploy/resumption), not from the previous
            # fire time — this is what makes it a rolling interval instead
            # of a fixed-clock schedule that could drift or double-fire if
            # a drain ran long.

        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error(f"restart_scheduler.run_scheduler: {exc}")
            await asyncio.sleep(60)


# ── Nov 2026: redeploy = disconnect → restart → clear-cache deploy ──────────
#
# Replaces the old "POST the deploy hook" trigger. Render's deploy hook cannot
# clear the build cache (it only accepts a commit ref), so the cache-clearing
# deploy has to go through the REST API (POST /v1/services/{id}/deploys with
# {"clearCache": "clear"}). Full sequence, in this order:
#
#   1. DISCONNECT from Deriv — done by bot_engine BEFORE it calls
#      trigger_redeploy() (drain -> persist state -> close both websockets).
#   2. HOLD flag  — persists FIXED_PHASE=cooldown for REDEPLOY_HOLD_SECS so the
#      instance that Render's restart brings back up (if it reads the env) sits
#      disconnected instead of trading while the clean build is running.
#   3. RESTART    — POST /v1/services/{id}/restart
#   4. RELEASE    — persists FIXED_PHASE=trading so the NEW build boots trading.
#   5. DEPLOY     — POST /v1/services/{id}/deploys {"clearCache": "clear"}
#
# Step 3 kills THIS process, so main.py's SIGTERM handler defers shutdown until
# steps 4-5 have finished (bounded by REDEPLOY_SIGTERM_GRACE_SECS).
# Needs RENDER_API_KEY + RENDER_SERVICE_ID; without them it falls back to the
# plain deploy hook (no restart, no cache clear) and says so loudly.

_RENDER_API = "https://api.render.com/v1"

_sequence_state: str = "idle"          # "idle" | "running" | "done"
_sigterm_received: bool = False
_sequence_lock = threading.Lock()


def sequence_in_progress() -> bool:
    """True while steps 2-5 are still running (main.py's SIGTERM handler
    defers shutdown while this is True)."""
    return _sequence_state == "running"


def note_sigterm() -> None:
    """Called by main.py's SIGTERM handler when it deferred shutdown."""
    global _sigterm_received
    _sigterm_received = True


def _api_headers() -> dict:
    return {
        "Authorization": f"Bearer {getattr(config, 'RENDER_API_KEY', '')}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _render_call(label: str, path: str, body, ok_codes: tuple) -> bool:
    """POST to Render's API with retries. 400/401/403/404 are not retried."""
    retries = max(1, int(getattr(config, "REDEPLOY_API_RETRIES", 4)))
    service_id = getattr(config, "RENDER_SERVICE_ID", "")
    url = f"{_RENDER_API}/services/{service_id}{path}"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, json=body, headers=_api_headers(), timeout=20)
            if resp.status_code in ok_codes:
                logger.info(f"REDEPLOY SEQUENCE: {label} ✓ HTTP {resp.status_code}")
                return True
            logger.error(
                f"REDEPLOY SEQUENCE: {label} FAILED (attempt {attempt}/{retries}) — "
                f"HTTP {resp.status_code}: {resp.text[:300]}")
            if resp.status_code in (400, 401, 403, 404):
                return False
        except Exception as exc:
            logger.error(
                f"REDEPLOY SEQUENCE: {label} ERROR (attempt {attempt}/{retries}): {exc}")
        if attempt < retries:
            time.sleep(min(5 * attempt, 15))
    return False


def _mark_confirmed() -> None:
    global _last_confirmed_redeploy_at, _handoff_until
    _last_confirmed_redeploy_at = time.time()
    if getattr(config, "REDEPLOY_HANDOFF_PAUSE", True):
        _handoff_until = time.time() + float(getattr(config, "REDEPLOY_HANDOFF_MAX_SECS", 240))
    _push_dashboard_flag(redeploy_hook_missing=False, redeploy_watchdog_overdue=False)


def _fire_hook_sync() -> bool:
    """Legacy plain deploy hook (no restart, no cache clear). Fallback only."""
    hook_url = os.environ.get("RENDER_DEPLOY_HOOK_URL", "") or \
        getattr(config, "RENDER_DEPLOY_HOOK_URL", "")
    if not hook_url:
        logger.critical(
            "\n" + "=" * 78 + "\n"
            "REDEPLOY IMPOSSIBLE: neither RENDER_API_KEY+RENDER_SERVICE_ID nor "
            "RENDER_DEPLOY_HOOK_URL is set.\n" + "=" * 78)
        _push_dashboard_flag(redeploy_hook_missing=True)
        return False
    try:
        resp = requests.post(hook_url, timeout=20)
        if resp.status_code in (200, 201, 202):
            logger.info(f"REDEPLOY HOOK FIRED (plain, no cache clear): HTTP {resp.status_code}")
            _mark_confirmed()
            return True
        logger.error(f"REDEPLOY HOOK FAILED: HTTP {resp.status_code} — {resp.text[:300]}")
    except Exception as exc:
        logger.error(f"REDEPLOY HOOK ERROR: {exc}")
    return False


def _do_sequence(reason: str) -> bool:
    import fixed_cycle   # lazy: reuses its env-var persistence helper

    api_key    = getattr(config, "RENDER_API_KEY", "")
    service_id = getattr(config, "RENDER_SERVICE_ID", "")

    if not getattr(config, "REDEPLOY_USE_API_SEQUENCE", True):
        logger.warning("REDEPLOY SEQUENCE: disabled in config — plain deploy hook only")
        return _fire_hook_sync()

    if not api_key or not service_id:
        logger.critical(
            "\n" + "=" * 78 + "\n"
            "REDEPLOY SEQUENCE: RENDER_API_KEY / RENDER_SERVICE_ID NOT SET\n"
            "Cannot restart the service or clear the build cache. Falling back "
            "to the plain deploy hook (no restart, no cache clear).\n" + "=" * 78)
        return _fire_hook_sync()

    logger.warning(f"REDEPLOY SEQUENCE START ({reason}): Deriv disconnected → "
                   f"restart → clear build cache → redeploy")

    hold = float(getattr(config, "REDEPLOY_HOLD_SECS", 900))
    if hold > 0:
        fixed_cycle._push_sync({
            "FIXED_PHASE":          "cooldown",
            "FIXED_COOLDOWN_UNTIL": f"{time.time() + hold:.0f}",
        })

    if getattr(config, "REDEPLOY_DO_RESTART", True):
        if not _render_call("restart service", "/restart", None, (200, 201, 202, 204)):
            logger.error("REDEPLOY SEQUENCE: restart failed — continuing to the deploy anyway")

    if hold > 0:   # release the hold so the NEW build boots straight into trading
        fixed_cycle._push_sync({"FIXED_PHASE": "trading", "FIXED_COOLDOWN_UNTIL": "0"})

    clear = bool(getattr(config, "REDEPLOY_CLEAR_CACHE", True))
    body  = {"clearCache": "clear" if clear else "do_not_clear"}
    if _render_call(f"deploy (clearCache={body['clearCache']})", "/deploys", body, (200, 201, 202)):
        _mark_confirmed()
        return True

    logger.critical("REDEPLOY SEQUENCE: API deploy FAILED — trying the plain deploy hook")
    return _fire_hook_sync()


def _replacement_overdue() -> None:
    logger.critical(
        "REDEPLOY SEQUENCE: no replacement instance arrived in time — exiting so "
        "Render restarts this service and the bot reconnects")
    os._exit(1)


def run_redeploy_sequence(reason: str = "scheduled", exit_on_failure: bool = None) -> bool:
    """Blocking. Safe to call from any thread. Returns True once the deploy has
    been accepted by Render. Deriv must already be disconnected by the caller."""
    global _sequence_state
    import target_guard
    if target_guard.is_reached():      # balance target hit: the service is being suspended
        logger.warning("REDEPLOY SEQUENCE: skipped - balance target reached, service is suspending")
        return True
    with _sequence_lock:
        if _sequence_state == "running":
            logger.warning("REDEPLOY SEQUENCE: already running — ignoring duplicate trigger")
            return True
        _sequence_state = "running"

    ok = False
    try:
        ok = _do_sequence(reason)
    except Exception as exc:
        logger.critical(f"REDEPLOY SEQUENCE crashed: {exc}", exc_info=True)

    if exit_on_failure is None:
        exit_on_failure = bool(getattr(config, "REDEPLOY_EXIT_ON_FAILURE", True))

    if ok:
        _sequence_state = "done"
        if _sigterm_received:          # restart already asked us to stop — finish now
            logger.info("REDEPLOY SEQUENCE complete after SIGTERM — exiting")
            os._exit(0)
        timeout = float(getattr(config, "REDEPLOY_REPLACE_TIMEOUT_SECS", 1200))
        if timeout > 0:                # safety net: never sit disconnected forever
            t = threading.Timer(timeout, _replacement_overdue)
            t.daemon = True
            t.start()
        return True

    _sequence_state = "idle"
    if exit_on_failure:
        logger.critical("REDEPLOY SEQUENCE FAILED — exiting so Render restarts the service")
        os._exit(1)
    return False


def trigger_redeploy() -> None:
    """
    Called by bot_engine._settle_loop() ONLY once every open contract has been
    confirmably closed AND the bot has disconnected from Deriv. Starts the
    restart + clear-cache + deploy sequence on its own thread (the bot's event
    loop is about to be torn down) and clears the pending flag.
    """
    global _pending, _last_triggered_at
    _last_triggered_at = time.time()
    _pending = False
    t = threading.Thread(target=run_redeploy_sequence, args=("rolling interval",),
                         name="redeploy-sequence", daemon=True)
    t.start()


async def _watchdog_loop():
    """
    Task 7 self-check requirement: an independent watchdog that fires a
    loud, high-visibility warning if wall-clock time since the last
    CONFIRMED redeploy (or process start, before the first one) exceeds
    REDEPLOY_INTERVAL_HOURS by more than a grace margin — regardless of
    what run_scheduler()'s own `_pending` state currently says, and
    regardless of the specific cause (missing hook URL, a stuck drain in
    bot_engine._settle_loop(), a wedged run_scheduler() task, etc). This
    is what makes "the rolling redeploy mechanism has stopped actually
    redeploying" visible in logs/dashboard going forward, rather than
    only discovered after the fact by noticing the process has been up
    far longer than intended.
    """
    global _last_watchdog_warning_at

    check_interval_secs = 60
    grace_secs = getattr(config, "REDEPLOY_WATCHDOG_GRACE_MINS", 20) * 60
    warn_repeat_secs = getattr(config, "REDEPLOY_WATCHDOG_REPEAT_MINS", 15) * 60

    while True:
        try:
            await asyncio.sleep(check_interval_secs)
            if _sequence_state != "idle":
                continue
            interval_hours = interval_secs() / 3600
            interval_secs_w = interval_secs()
            baseline = _last_confirmed_redeploy_at or _process_start_time
            uptime_secs = time.time() - baseline
            overdue_by = uptime_secs - interval_secs_w

            if overdue_by > grace_secs:
                now = time.time()
                if now - _last_watchdog_warning_at >= warn_repeat_secs:
                    _last_watchdog_warning_at = now
                    logger.critical(
                        "\n" + "=" * 78 + "\n"
                        f"REDEPLOY WATCHDOG: no CONFIRMED redeploy in "
                        f"{uptime_secs / 3600:.2f}h — {overdue_by / 60:.0f}min "
                        f"past the {interval_hours}h interval + grace margin. "
                        f"The rolling-redeploy mechanism may be stalled — check "
                        f"RENDER_DEPLOY_HOOK_URL is set and reachable, and that "
                        f"bot_engine's _settle_loop()/drain logic isn't stuck.\n"
                        + "=" * 78
                    )
                    _push_dashboard_flag(
                        redeploy_watchdog_overdue=True,
                        redeploy_watchdog_overdue_mins=round(overdue_by / 60, 1),
                    )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error(f"restart_scheduler._watchdog_loop: {exc}")


def start_restart_scheduler():
    """
    Entry point called by main.py at startup (unchanged name/contract from
    before this fix — only the scheduling logic inside run_scheduler()
    changed, per Implementation Brief v2, Fix G).

    Starts run_scheduler() as a background asyncio task if a loop is
    already running (the normal case — main.py calls this from inside its
    async startup), or spins up a dedicated background thread with its
    own event loop if called from synchronous code before any loop
    exists. Safe to call exactly once at process startup.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(run_scheduler())
        loop.create_task(_watchdog_loop())
        logger.info("restart_scheduler: started on the running event loop (scheduler + watchdog)")
        return
    except RuntimeError:
        pass

    import threading

    def _thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(asyncio.gather(run_scheduler(), _watchdog_loop()))
        except Exception as exc:
            logger.error(f"restart_scheduler thread crashed: {exc}")

    t = threading.Thread(target=_thread_target, name="restart-scheduler", daemon=True)
    t.start()
    logger.info("restart_scheduler: started on a dedicated background thread (scheduler + watchdog)")
