#!/usr/bin/env python3
"""
Monk Bot - BTC/ETH Divergence Alert Bot

Monitors BTC/ETH price divergence and sends Telegram alerts
for ENTRY, EXIT, and INVALIDATION signals.

Features peak detection: waits for gap to peak and start reversing
before sending entry signal for optimal entry timing.
"""
import time
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional, Tuple, List, NamedTuple

import requests

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    API_BASE_URL,
    API_ENDPOINT,
    SCAN_INTERVAL_SECONDS,
    TRACK_INTERVAL_SECONDS,
    FRESHNESS_THRESHOLD_MINUTES,
    ENTRY_THRESHOLD,
    EXIT_THRESHOLD,
    INVALIDATION_THRESHOLD,
    logger,
)


# =============================================================================
# Constants
# =============================================================================
DEFAULT_LOOKBACK_HOURS = 24
HISTORY_BUFFER_MINUTES = 30


# =============================================================================
# Data Structures
# =============================================================================
class Mode(Enum):
    SCAN = "SCAN"
    PEAK_WATCH = "PEAK_WATCH"   # Gap crossed threshold, waiting for peak reversal
    TRACK = "TRACK"


class Strategy(Enum):
    S1 = "S1"  # Long BTC / Short ETH (when ETH pumps more)
    S2 = "S2"  # Long ETH / Short BTC (when ETH dumps more)


class PricePoint(NamedTuple):
    timestamp: datetime
    btc: Decimal
    eth: Decimal


class PriceData(NamedTuple):
    btc_price: Decimal
    eth_price: Decimal
    btc_updated_at: datetime
    eth_updated_at: datetime


# =============================================================================
# Global State
# =============================================================================
price_history: List[PricePoint] = []
current_mode: Mode = Mode.SCAN
active_strategy: Optional[Strategy] = None

# Peak detection state
peak_gap: Optional[float] = None
peak_strategy: Optional[Strategy] = None

# Runtime settings
settings = {
    "scan_interval": SCAN_INTERVAL_SECONDS,
    "entry_threshold": ENTRY_THRESHOLD,
    "exit_threshold": EXIT_THRESHOLD,
    "invalidation_threshold": INVALIDATION_THRESHOLD,
    "peak_reversal": 0.3,
    "lookback_hours": DEFAULT_LOOKBACK_HOURS,
    "heartbeat_minutes": 30,
}

last_update_id: int = 0
last_heartbeat_time: Optional[datetime] = None
scan_stats = {
    "count": 0,
    "last_btc_price": None,
    "last_eth_price": None,
    "last_btc_ret": None,
    "last_eth_ret": None,
    "last_gap": None,
    "signals_sent": 0,
}


# =============================================================================
# Telegram Bot
# =============================================================================
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def send_alert(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured, skipping alert")
        return False
    try:
        response = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        logger.info("Alert sent successfully")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram alert: {e}")
        return False


# =============================================================================
# Telegram Command Handling
# =============================================================================
LONG_POLL_TIMEOUT = 30


def get_telegram_updates() -> list:
    global last_update_id
    if not TELEGRAM_BOT_TOKEN:
        return []
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {"offset": last_update_id + 1, "timeout": LONG_POLL_TIMEOUT}
        response = requests.get(url, params=params, timeout=LONG_POLL_TIMEOUT + 5)
        response.raise_for_status()
        data = response.json()
        if data.get("ok") and data.get("result"):
            updates = data["result"]
            if updates:
                last_update_id = updates[-1]["update_id"]
            return updates
    except requests.RequestException as e:
        logger.debug(f"Failed to get updates: {e}")
    return []


def process_commands() -> None:
    updates = get_telegram_updates()
    for update in updates:
        message = update.get("message", {})
        text = message.get("text", "")
        chat_id = str(message.get("chat", {}).get("id", ""))
        user_id = str(message.get("from", {}).get("id", ""))
        is_authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not is_authorized:
            continue
        if not text.startswith("/"):
            continue
        reply_chat = chat_id
        parts = text.split()
        command = parts[0].lower().split("@")[0]
        args = parts[1:] if len(parts) > 1 else []
        logger.info(f"Processing command: {command} from chat {chat_id}")
        if command == "/settings":
            handle_settings_command(reply_chat)
        elif command == "/interval":
            handle_interval_command(args, reply_chat)
        elif command == "/threshold":
            handle_threshold_command(args, reply_chat)
        elif command == "/help":
            handle_help_command(reply_chat)
        elif command == "/status":
            handle_status_command(reply_chat)
        elif command == "/lookback":
            handle_lookback_command(args, reply_chat)
        elif command == "/heartbeat":
            handle_heartbeat_command(args, reply_chat)
        elif command == "/peak":
            handle_peak_command(args, reply_chat)
        elif command == "/start":
            handle_help_command(reply_chat)


def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        response = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send reply: {e}")
        return False


def handle_settings_command(reply_chat: str) -> None:
    hb = settings['heartbeat_minutes']
    hb_str = f"{hb} min" if hb > 0 else "Off"
    message = (
        "⚙️ *Current Settings*\n"
        "\n"
        f"📊 Scan Interval: {settings['scan_interval']}s ({settings['scan_interval'] // 60} min)\n"
        f"🕐 Lookback Period: {settings['lookback_hours']}h\n"
        f"💓 Heartbeat: {hb_str}\n"
        f"📈 Entry Threshold: ±{settings['entry_threshold']}%\n"
        f"📉 Exit Threshold: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak Reversal: {settings['peak_reversal']}%\n"
        "\n"
        "*Commands:*\n"
        "`/interval`, `/lookback`, `/heartbeat`, `/threshold`, `/peak`\n"
        "`/help` - Show all commands"
    )
    send_reply(message, reply_chat)


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply("❌ Usage: `/interval <seconds>`\nExample: `/interval 180`", reply_chat)
        return
    try:
        new_interval = int(args[0])
        if new_interval < 60:
            send_reply("❌ Minimum interval is 60 seconds", reply_chat)
            return
        if new_interval > 3600:
            send_reply("❌ Maximum interval is 3600 seconds", reply_chat)
            return
        settings["scan_interval"] = new_interval
        send_reply(f"✅ Scan interval set to *{new_interval} seconds* ({new_interval // 60} min)", reply_chat)
        logger.info(f"Scan interval changed to {new_interval}s via command")
    except ValueError:
        send_reply("❌ Invalid number.", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "❌ Usage:\n"
            "`/threshold entry <value>`\n"
            "`/threshold exit <value>`\n"
            "`/threshold invalid <value>`",
            reply_chat
        )
        return
    try:
        threshold_type = args[0].lower()
        value = float(args[1])
        if value <= 0 or value > 20:
            send_reply("❌ Value must be between 0 and 20", reply_chat)
            return
        if threshold_type == "entry":
            settings["entry_threshold"] = value
            send_reply(f"✅ Entry threshold set to *±{value}%*", reply_chat)
        elif threshold_type == "exit":
            settings["exit_threshold"] = value
            send_reply(f"✅ Exit threshold set to *±{value}%*", reply_chat)
        elif threshold_type in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = value
            send_reply(f"✅ Invalidation threshold set to *±{value}%*", reply_chat)
        else:
            send_reply("❌ Unknown type. Use: `entry`, `exit`, or `invalid`", reply_chat)
        logger.info(f"Threshold {threshold_type} changed to {value}")
    except ValueError:
        send_reply("❌ Invalid number", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"🎯 Current peak reversal: *{settings['peak_reversal']}%*\n\n"
            "Usage: `/peak <value>`\n"
            "Example: `/peak 0.3`\n\n"
            "_Gap must drop this % from peak before entry triggers_",
            reply_chat
        )
        return
    try:
        value = float(args[0])
        if value <= 0 or value > 2.0:
            send_reply("❌ Value must be between 0 and 2.0", reply_chat)
            return
        settings["peak_reversal"] = value
        send_reply(
            f"✅ Peak reversal set to *{value}%*\n\n"
            f"_Entry triggers when gap drops {value}% from peak_",
            reply_chat
        )
        logger.info(f"Peak reversal changed to {value}")
    except ValueError:
        send_reply("❌ Invalid number. Usage: `/peak <value>`", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    global price_history
    if not args:
        send_reply(
            f"📊 Current lookback: *{settings['lookback_hours']}h*\n\n"
            "Usage: `/lookback <hours>`",
            reply_chat
        )
        return
    try:
        new_lookback = int(args[0])
        if new_lookback < 1 or new_lookback > 24:
            send_reply("❌ Lookback must be between 1 and 24 hours", reply_chat)
            return
        old_lookback = settings["lookback_hours"]
        settings["lookback_hours"] = new_lookback
        price_history = []
        send_reply(
            f"✅ Lookback changed from *{old_lookback}h* to *{new_lookback}h*\n\n"
            f"⚠️ History cleared - collecting new {new_lookback}h data...",
            reply_chat
        )
    except ValueError:
        send_reply("❌ Invalid number.", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"💓 Current heartbeat: *{settings['heartbeat_minutes']} minutes*\n\n"
            "Usage: `/heartbeat <minutes>` or `/heartbeat 0` to disable",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 0 or new_interval > 120:
            send_reply("❌ Value must be between 0 and 120", reply_chat)
            return
        settings["heartbeat_minutes"] = new_interval
        if new_interval == 0:
            send_reply("✅ Heartbeat *disabled*", reply_chat)
        else:
            send_reply(f"✅ Heartbeat set to *{new_interval} minutes*", reply_chat)
    except ValueError:
        send_reply("❌ Invalid number.", reply_chat)


def handle_help_command(reply_chat: str) -> None:
    message = (
        "🤖 *Monk Bot Commands*\n"
        "\n"
        "*Settings:*\n"
        "`/settings` - Show current settings\n"
        "`/interval <sec>` - Set scan interval (60-3600)\n"
        "`/lookback <hours>` - Set lookback period (1-24)\n"
        "`/heartbeat <min>` - Set heartbeat interval (0=off)\n"
        "`/threshold entry <val>` - Entry threshold %\n"
        "`/threshold exit <val>` - Exit threshold %\n"
        "`/threshold invalid <val>` - Invalidation %\n"
        "`/peak <val>` - Peak reversal % to confirm entry\n"
        "\n"
        "*Info:*\n"
        "`/status` - Show bot status\n"
        "`/help` - This message"
    )
    send_reply(message, reply_chat)


def handle_status_command(reply_chat: str) -> None:
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback = settings["lookback_hours"]
    ready = "✅ Ready" if hours_of_data >= lookback else f"⏳ {hours_of_data:.1f}h / {lookback}h"
    peak_line = f"Peak Gap: {peak_gap:+.2f}%\n" if (current_mode == Mode.PEAK_WATCH and peak_gap is not None) else ""
    message = (
        "📊 *Bot Status*\n"
        "\n"
        f"Mode: {current_mode.value}\n"
        f"Active Strategy: {active_strategy.value if active_strategy else 'None'}\n"
        f"{peak_line}"
        f"Lookback: {lookback}h\n"
        f"History: {ready}\n"
        f"Data Points: {len(price_history)}\n"
    )
    send_reply(message, reply_chat)


# =============================================================================
# Value Formatting
# =============================================================================
def format_value(value: Decimal) -> str:
    float_val = float(value)
    if abs(float_val) < 0.05:
        return "+0.0"
    return f"+{float_val:.1f}" if float_val >= 0 else f"{float_val:.1f}"


# =============================================================================
# Message Building
# =============================================================================
def get_lookback_label() -> str:
    hours = settings["lookback_hours"]
    return f"{hours}h"


def build_peak_watch_message(strategy: Strategy, gap: Decimal) -> str:
    lb = get_lookback_label()
    if strategy == Strategy.S1:
        direction = "Long BTC / Short ETH"
        reason = f"ETH pumped more than BTC ({lb})"
    else:
        direction = "Long ETH / Short BTC"
        reason = f"ETH dumped more than BTC ({lb})"
    return (
        f"👀 *PEAK WATCH: {strategy.value}*\n"
        f"\n"
        f"_{direction}_\n"
        f"_{reason}_\n"
        f"\n"
        f"Gap: *{format_value(gap)}%*\n"
        f"Waiting for *{settings['peak_reversal']}%* reversal from peak...\n"
        f"\n"
        f"_Entry triggers when gap starts reversing_"
    )


def build_entry_message(strategy: Strategy, btc_ret: Decimal, eth_ret: Decimal, gap: Decimal, peak: float) -> str:
    lb = get_lookback_label()
    if strategy == Strategy.S1:
        direction = "📈 Long BTC / Short ETH"
        reason = f"ETH pumped more than BTC ({lb})"
    else:
        direction = "📈 Long ETH / Short BTC"
        reason = f"ETH dumped more than BTC ({lb})"
    return (
        f"🚨 *ENTRY SIGNAL: {strategy.value}*\n"
        f"\n"
        f"{direction}\n"
        f"_{reason}_\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"│ Peak: {peak:+.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"✅ Gap reversed {settings['peak_reversal']}% from peak\n"
        f"⏰ Tracking mode activated"
    )


def build_exit_message(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal) -> str:
    lb = get_lookback_label()
    return (
        f"✅ *EXIT SIGNAL*\n"
        f"\n"
        f"Gap converged - close position.\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"🔍 Returning to scan mode"
    )


def build_invalidation_message(strategy: Strategy, btc_ret: Decimal, eth_ret: Decimal, gap: Decimal) -> str:
    lb = get_lookback_label()
    return (
        f"⚠️ *INVALIDATION: {strategy.value}*\n"
        f"\n"
        f"Gap widened - consider closing.\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"🔍 Returning to scan mode"
    )


def build_peak_cancelled_message(strategy: Strategy, gap: Decimal) -> str:
    return (
        f"❌ *PEAK WATCH CANCELLED: {strategy.value}*\n"
        f"\n"
        f"Gap retreated below entry threshold.\n"
        f"Current Gap: *{format_value(gap)}%*\n"
        f"\n"
        f"🔍 Returning to scan mode"
    )


def build_heartbeat_message() -> str:
    lb = get_lookback_label()
    btc_ret_str = f" ({format_value(scan_stats['last_btc_ret'])}%)" if scan_stats['last_btc_ret'] is not None else ""
    eth_ret_str = f" ({format_value(scan_stats['last_eth_ret'])}%)" if scan_stats['last_eth_ret'] is not None else ""
    btc_str = f"${float(scan_stats['last_btc_price']):,.2f}{btc_ret_str}" if scan_stats['last_btc_price'] else "N/A"
    eth_str = f"${float(scan_stats['last_eth_price']):,.2f}{eth_ret_str}" if scan_stats['last_eth_price'] else "N/A"
    gap_str = f"{format_value(scan_stats['last_gap'])}%" if scan_stats['last_gap'] is not None else "N/A"
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback = settings["lookback_hours"]
    data_status = f"✅ Ready ({hours_of_data:.1f}h)" if hours_of_data >= lookback else f"⏳ {hours_of_data:.1f}h / {lookback}h"
    peak_line = f"│ Peak: {peak_gap:+.2f}%\n" if (current_mode == Mode.PEAK_WATCH and peak_gap is not None) else ""
    return (
        f"💓 *Heartbeat*\n"
        f"\n"
        f"*Status:* Bot running normally\n"
        f"*Mode:* {current_mode.value}\n"
        f"*Strategy:* {active_strategy.value if active_strategy else 'None'}\n"
        f"\n"
        f"*Last {settings['heartbeat_minutes']} min:*\n"
        f"┌─────────────────────\n"
        f"│ Scans: {scan_stats['count']}\n"
        f"│ Signals: {scan_stats['signals_sent']}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Current Prices:*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"│ Gap ({lb}): {gap_str}\n"
        f"{peak_line}"
        f"└─────────────────────\n"
        f"\n"
        f"*Data:* {data_status}\n"
        f"\n"
        f"_Next update in {settings['heartbeat_minutes']} min_"
    )


def send_heartbeat() -> bool:
    global scan_stats
    success = send_alert(build_heartbeat_message())
    scan_stats["count"] = 0
    scan_stats["signals_sent"] = 0
    return success


def should_send_heartbeat(now: datetime) -> bool:
    if settings["heartbeat_minutes"] == 0 or last_heartbeat_time is None:
        return False
    return (now - last_heartbeat_time).total_seconds() / 60 >= settings["heartbeat_minutes"]


# =============================================================================
# API Fetching
# =============================================================================
def parse_iso_timestamp(ts_str: str) -> Optional[datetime]:
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        if "." in ts_str:
            base, frac_and_tz = ts_str.split(".", 1)
            tz_start = next((i for i, c in enumerate(frac_and_tz) if c in ("+", "-")), -1)
            if tz_start > 6:
                frac_and_tz = frac_and_tz[:6] + frac_and_tz[tz_start:]
            ts_str = base + "." + frac_and_tz
        dt = datetime.fromisoformat(ts_str)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError) as e:
        logger.error(f"Failed to parse timestamp '{ts_str}': {e}")
        return None


def fetch_prices() -> Optional[PriceData]:
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f"API request failed: {e}")
        return None

    listings = data.get("listings", [])
    if not listings:
        return None

    btc_data = next((l for l in listings if l.get("ticker", "").upper() == "BTC"), None)
    eth_data = next((l for l in listings if l.get("ticker", "").upper() == "ETH"), None)

    if not btc_data or not eth_data:
        logger.warning("Missing BTC or ETH data")
        return None

    try:
        btc_price = Decimal(btc_data["mark_price"])
        eth_price = Decimal(eth_data["mark_price"])
    except (KeyError, InvalidOperation) as e:
        logger.error(f"Invalid price: {e}")
        return None

    btc_updated_at = parse_iso_timestamp(btc_data.get("quotes", {}).get("updated_at", ""))
    eth_updated_at = parse_iso_timestamp(eth_data.get("quotes", {}).get("updated_at", ""))

    if not btc_updated_at or not eth_updated_at:
        return None

    return PriceData(btc_price, eth_price, btc_updated_at, eth_updated_at)


# =============================================================================
# Price History Management
# =============================================================================
def append_price(timestamp: datetime, btc: Decimal, eth: Decimal) -> None:
    price_history.append(PricePoint(timestamp, btc, eth))


def prune_history(now: datetime) -> None:
    global price_history
    cutoff = now - timedelta(hours=settings["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES)
    price_history = [p for p in price_history if p.timestamp >= cutoff]


def get_lookback_price(now: datetime) -> Optional[PricePoint]:
    target_time = now - timedelta(hours=settings["lookback_hours"])
    best_point, best_diff = None, timedelta(minutes=30)
    for point in price_history:
        diff = abs(point.timestamp - target_time)
        if diff < best_diff:
            best_diff, best_point = diff, point
    return best_point


# =============================================================================
# Return Calculation
# =============================================================================
def compute_returns(btc_now, eth_now, btc_prev, eth_prev) -> Tuple[Decimal, Decimal, Decimal]:
    btc_change = (btc_now - btc_prev) / btc_prev * Decimal("100")
    eth_change = (eth_now - eth_prev) / eth_prev * Decimal("100")
    return btc_change, eth_change, eth_change - btc_change


# =============================================================================
# Freshness Check
# =============================================================================
def is_data_fresh(now, btc_updated, eth_updated) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return (now - btc_updated) <= threshold and (now - eth_updated) <= threshold


# =============================================================================
# State Machine with Peak Detection
# =============================================================================
def evaluate_and_transition(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal) -> None:
    global current_mode, active_strategy, peak_gap, peak_strategy

    gap_float = float(gap)
    entry_thresh = settings["entry_threshold"]
    exit_thresh = settings["exit_threshold"]
    invalid_thresh = settings["invalidation_threshold"]
    peak_reversal = settings["peak_reversal"]

    # ── SCAN ──────────────────────────────────────────────────────────────
    if current_mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            current_mode = Mode.PEAK_WATCH
            peak_strategy = Strategy.S1
            peak_gap = gap_float
            send_alert(build_peak_watch_message(Strategy.S1, gap))
            logger.info(f"PEAK WATCH S1 started. Gap: {gap_float:.2f}%")

        elif gap_float <= -entry_thresh:
            current_mode = Mode.PEAK_WATCH
            peak_strategy = Strategy.S2
            peak_gap = gap_float
            send_alert(build_peak_watch_message(Strategy.S2, gap))
            logger.info(f"PEAK WATCH S2 started. Gap: {gap_float:.2f}%")

        else:
            logger.debug(f"SCAN: No signal. Gap: {gap_float:.2f}%")

    # ── PEAK WATCH ────────────────────────────────────────────────────────
    elif current_mode == Mode.PEAK_WATCH:
        if peak_strategy == Strategy.S1:
            if gap_float > peak_gap:
                peak_gap = gap_float
                logger.info(f"PEAK WATCH S1: New peak {peak_gap:.2f}%")

            elif gap_float < entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S1, gap))
                logger.info(f"PEAK WATCH S1 cancelled. Gap: {gap_float:.2f}%")
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None

            elif peak_gap - gap_float >= peak_reversal:
                active_strategy = Strategy.S1
                current_mode = Mode.TRACK
                send_alert(build_entry_message(Strategy.S1, btc_ret, eth_ret, gap, peak_gap))
                logger.info(f"ENTRY S1 at peak reversal. Peak: {peak_gap:.2f}%, Now: {gap_float:.2f}%")
                peak_gap, peak_strategy = None, None

            else:
                logger.info(f"PEAK WATCH S1: Gap {gap_float:.2f}% | Peak {peak_gap:.2f}% | Need {peak_reversal}% drop")

        elif peak_strategy == Strategy.S2:
            if gap_float < peak_gap:
                peak_gap = gap_float
                logger.info(f"PEAK WATCH S2: New peak {peak_gap:.2f}%")

            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S2, gap))
                logger.info(f"PEAK WATCH S2 cancelled. Gap: {gap_float:.2f}%")
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None

            elif gap_float - peak_gap >= peak_reversal:
                active_strategy = Strategy.S2
                current_mode = Mode.TRACK
                send_alert(build_entry_message(Strategy.S2, btc_ret, eth_ret, gap, peak_gap))
                logger.info(f"ENTRY S2 at peak reversal. Peak: {peak_gap:.2f}%, Now: {gap_float:.2f}%")
                peak_gap, peak_strategy = None, None

            else:
                logger.info(f"PEAK WATCH S2: Gap {gap_float:.2f}% | Peak {peak_gap:.2f}% | Need {peak_reversal}% rise")

    # ── TRACK ─────────────────────────────────────────────────────────────
    elif current_mode == Mode.TRACK:
        if abs(gap_float) <= exit_thresh:
            send_alert(build_exit_message(btc_ret, eth_ret, gap))
            logger.info(f"EXIT triggered. Gap: {gap_float:.2f}%")
            current_mode, active_strategy = Mode.SCAN, None
            return

        if active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S1, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S1. Gap: {gap_float:.2f}%")
            current_mode, active_strategy = Mode.SCAN, None
            return

        if active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S2, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S2. Gap: {gap_float:.2f}%")
            current_mode, active_strategy = Mode.SCAN, None
            return

        logger.debug(f"TRACK {active_strategy.value if active_strategy else 'None'}: Gap {gap_float:.2f}%")


# =============================================================================
# Startup Message
# =============================================================================
def send_startup_message() -> bool:
    price_data = fetch_prices()
    if price_data:
        price_info = (
            f"\n💰 *Current Prices:*\n"
            f"┌─────────────────────\n"
            f"│ BTC: ${float(price_data.btc_price):,.2f}\n"
            f"│ ETH: ${float(price_data.eth_price):,.2f}\n"
            f"└─────────────────────\n"
        )
    else:
        price_info = "\n⚠️ Unable to fetch current prices\n"

    lb = get_lookback_label()
    return send_alert(
        f"🤖 *Monk Bot Started*\n"
        f"{price_info}\n"
        f"📊 Rolling {lb} % change\n"
        f"📈 Entry: ±{settings['entry_threshold']}%\n"
        f"📉 Exit: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak reversal: {settings['peak_reversal']}%\n"
        f"⏱️ Scan: {settings['scan_interval']}s\n\n"
        f"⏳ Building {lb} history...\n"
        f"💡 Type `/help` for commands"
    )


# =============================================================================
# Command Polling Thread
# =============================================================================
def command_polling_thread() -> None:
    while True:
        try:
            process_commands()
        except Exception as e:
            logger.debug(f"Command polling error: {e}")
            time.sleep(5)


# =============================================================================
# Main Loop
# =============================================================================
def main_loop() -> None:
    global last_heartbeat_time

    logger.info("=" * 60)
    logger.info("Monk Bot starting with Peak Detection")
    logger.info(f"Entry: {settings['entry_threshold']}% | Exit: {settings['exit_threshold']}% | Invalid: {settings['invalidation_threshold']}% | Peak: {settings['peak_reversal']}%")
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message()

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            if should_send_heartbeat(now):
                if send_heartbeat():
                    last_heartbeat_time = now

            price_data = fetch_prices()
            if price_data is None:
                logger.warning("Failed to fetch prices")
            else:
                scan_stats["count"] += 1
                scan_stats["last_btc_price"] = price_data.btc_price
                scan_stats["last_eth_price"] = price_data.eth_price

                if not is_data_fresh(now, price_data.btc_updated_at, price_data.eth_updated_at):
                    logger.warning("Data not fresh, skipping")
                else:
                    append_price(now, price_data.btc_price, price_data.eth_price)
                    prune_history(now)
                    price_then = get_lookback_price(now)

                    if price_then is None:
                        hours = len(price_history) * settings["scan_interval"] / 3600
                        logger.info(f"Building history... ({hours:.1f}h / {settings['lookback_hours']}h)")
                    else:
                        btc_ret, eth_ret, gap = compute_returns(
                            price_data.btc_price, price_data.eth_price,
                            price_then.btc, price_then.eth,
                        )
                        scan_stats["last_gap"] = gap
                        scan_stats["last_btc_ret"] = btc_ret
                        scan_stats["last_eth_ret"] = eth_ret

                        logger.info(
                            f"Mode: {current_mode.value} | "
                            f"BTC {settings['lookback_hours']}h: {format_value(btc_ret)}% | "
                            f"ETH {settings['lookback_hours']}h: {format_value(eth_ret)}% | "
                            f"Gap: {format_value(gap)}%"
                        )

                        prev_mode = current_mode
                        evaluate_and_transition(btc_ret, eth_ret, gap)
                        if current_mode != prev_mode:
                            scan_stats["signals_sent"] += 1

            time.sleep(settings["scan_interval"])

        except KeyboardInterrupt:
            logger.info("Shutting down")
            break
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            time.sleep(60)


# =============================================================================
# Entry Point
# =============================================================================
if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set")
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID not set")
    main_loop()
