"""
target_guard.py - stop trading and SUSPEND the Render service once the live
Deriv account balance reaches a target you set by hand.

Set on Render (Environment tab):

    TARGET_BALANCE = 5000     # in account currency; empty / 0 / not set -> feature OFF

The moment the live balance is >= TARGET_BALANCE (same or above):

  1. a process-wide "target reached" flag is set, synchronously, inside the
     balance callback. From that instant no proposal or buy is sent: every
     buy path in deriv_client.py refuses, and bot_engine.py opens no entries;
  2. open contracts are NOT waited for - they stay on Deriv's side and settle there;
  3. the scheduled redeploy sequence is blocked, so it cannot restart the
     service behind the suspend request;
  4. this Render service is SUSPENDED through the Render API
     (POST /v1/services/{id}/suspend). The request is retried every few
     seconds until Render accepts it. Needs RENDER_API_KEY; RENDER_SERVICE_ID
     is injected by Render itself.

The balance is also checked once at boot (every redeploy), so a service that is
resumed with the target still at/below the balance stops again within seconds.

To trade again: raise TARGET_BALANCE above the current balance on Render, then
Resume the service.

The balance watched is the one Deriv reports for the account the API token's
session is on (demo or real): the live balance stream, plus a direct poll
every TARGET_CHECK_SECONDS (default 3) as a safety net.
"""

import logging
import os
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_RENDER_API = "https://api.render.com/v1"
_RETRY_SECONDS = 10.0

_lock = threading.Lock()
_reached: bool = False
_reached_at: float = 0.0
_reached_balance: float = 0.0
_reached_target: float = 0.0
_suspend_result: str = "not attempted"
_suspend_thread: Optional[threading.Thread] = None


# ── settings (read live, never stale) ────────────────────────────────────────

def target() -> Optional[float]:
    """Current TARGET_BALANCE, or None when unset / 0 / invalid (feature off)."""
    raw = (os.environ.get("TARGET_BALANCE") or "").strip().replace(",", "").lstrip("$")
    if not raw:
        return None
    try:
        val = float(raw)
    except ValueError:
        logger.error(f"TARGET_BALANCE={raw!r} is not a number - target feature OFF")
        return None
    return val if val > 0 else None


def check_seconds() -> float:
    try:
        return max(1.0, float(os.environ.get("TARGET_CHECK_SECONDS", "3")))
    except ValueError:
        return 3.0


def _api_key() -> str:
    key = os.environ.get("RENDER_API_KEY")
    if not key:
        try:
            import config
            key = getattr(config, "RENDER_API_KEY", "")
        except Exception:
            key = ""
    return (key or "").strip()


def _service_id() -> str:
    sid = os.environ.get("RENDER_SERVICE_ID")
    if not sid:
        try:
            import config
            sid = getattr(config, "RENDER_SERVICE_ID", "")
        except Exception:
            sid = ""
    return (sid or "").strip()


# ── state ────────────────────────────────────────────────────────────────────

def is_reached() -> bool:
    return _reached


def should_stop(balance) -> bool:
    """True if the balance is at or above the target (and the feature is on)."""
    t = target()
    try:
        return t is not None and balance is not None and float(balance) >= t
    except (TypeError, ValueError):
        return False


def check_balance(balance) -> bool:
    """Call with every live balance reading. If it is at/above the target, set
    the stop flag and start the suspend. Returns True if the target is reached
    (now or earlier)."""
    if _reached:
        return True
    if should_stop(balance):
        mark_reached(balance)
        return True
    return False


def mark_reached(balance: float) -> bool:
    """Set the stop flag and start the suspend thread. Returns True only for the
    FIRST caller, so the shutdown sequence runs exactly once."""
    global _reached, _reached_at, _reached_balance, _reached_target, _suspend_thread
    with _lock:
        if _reached:
            return False
        _reached = True
        _reached_at = time.time()
        _reached_balance = float(balance)
        _reached_target = float(target() or 0.0)
    logger.warning(
        f"TARGET REACHED: live balance {float(balance):.2f} >= target {_reached_target:.2f} "
        f"- no more trades, suspending the service")
    try:
        import keep_alive
        keep_alive.update_status(target_reached=True, target_balance=_reached_target,
                                 target_reached_balance=float(balance))
    except Exception:
        pass
    _suspend_thread = threading.Thread(target=_suspend_forever, name="target-suspend", daemon=True)
    _suspend_thread.start()
    return True


def snapshot() -> dict:
    return {
        "target":          target(),
        "reached":         _reached,
        "reached_at":      _reached_at,
        "reached_balance": _reached_balance,
        "suspend_result":  _suspend_result,
    }


# ── Render suspend ───────────────────────────────────────────────────────────

def suspend_service() -> bool:
    """One attempt to suspend THIS Render service. Blocking. True if Render accepted."""
    global _suspend_result
    key, sid = _api_key(), _service_id()
    if not key or not sid:
        _suspend_result = "RENDER_API_KEY / RENDER_SERVICE_ID missing - cannot suspend"
        logger.error(f"TARGET: {_suspend_result}. Trading is stopped, but the service stays "
                     f"running idle until you suspend it by hand.")
        return False
    url = f"{_RENDER_API}/services/{sid}/suspend"
    headers = {"Authorization": f"Bearer {key}", "Accept": "application/json"}
    try:
        r = requests.post(url, headers=headers, timeout=20)
        if r.status_code in (200, 201, 202, 204):
            _suspend_result = "suspended"
            logger.warning("TARGET: Render service SUSPEND accepted - Render will stop this process")
            return True
        _suspend_result = f"Render refused: HTTP {r.status_code} {r.text[:150]}"
        logger.error(f"TARGET: {_suspend_result}")
    except Exception as exc:
        _suspend_result = f"suspend request failed: {exc}"
        logger.error(f"TARGET: {_suspend_result}")
    return False


def _suspend_forever() -> None:
    """Keep asking Render to suspend until it accepts. After an accepted request
    Render stops the process, so a second attempt normally never runs."""
    time.sleep(2.0)   # let the last log lines and any in-flight request settle
    attempt = 0
    while True:
        attempt += 1
        if suspend_service():
            return
        if not _api_key() or not _service_id():
            time.sleep(60.0)        # nothing to retry until the env vars are fixed
        else:
            time.sleep(_RETRY_SECONDS)
        if attempt % 6 == 0:
            logger.error(f"TARGET: still not suspended after {attempt} attempts - "
                         f"last result: {_suspend_result}")
