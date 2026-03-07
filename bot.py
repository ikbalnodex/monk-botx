#!/usr/bin/env python3
"""
Monk Bot B - Multi-Pair Divergence Alert Bot (Read-Only Redis Consumer)

Supported pairs:
  BTC/ETH  BTC/BNB  ETH/BNB  BTC/SOL  ETH/SOL

Setiap pair punya state machine sendiri (SCAN → PEAK_WATCH → TRACK).
Redis read-only: Bot A yang write, Bot B hanya baca.
"""
import json
import os
import time
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional, Tuple, List, Dict, NamedTuple

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
    UPSTASH_REDIS_URL,
    UPSTASH_REDIS_TOKEN,
    logger,
)

# =============================================================================
# Constants
# =============================================================================
DEFAULT_LOOKBACK_HOURS   = 24
HISTORY_BUFFER_MINUTES   = 30
REDIS_REFRESH_MINUTES    = 1
TP_COIN_A_MOVE_RANGE_PCT = 2.0

ALL_TICKERS = ["BTC", "ETH", "SOL", "BNB"]

PAIRS: List[Tuple[str, str]] = [
    ("BTC", "ETH"),
    ("BTC", "BNB"),
    ("ETH", "BNB"),
    ("BTC", "SOL"),
    ("ETH", "SOL"),
]


def pair_key(a: str, b: str) -> str:
    return f"{a}/{b}"


# =============================================================================
# Enums
# =============================================================================
class Mode(Enum):
    SCAN       = "SCAN"
    PEAK_WATCH = "PEAK_WATCH"
    TRACK      = "TRACK"


class Strategy(Enum):
    S1 = "S1"
    S2 = "S2"


# =============================================================================
# Data Structures
# =============================================================================
class PricePoint(NamedTuple):
    timestamp: datetime
    prices:    dict


class PriceData(NamedTuple):
    prices:        dict
    updated_times: dict


# =============================================================================
# Per-Pair State
# =============================================================================
class PairState:
    def __init__(self, coin_a: str, coin_b: str):
        self.coin_a = coin_a
        self.coin_b = coin_b
        self.key    = pair_key(coin_a, coin_b)
        self._reset()

    def _reset(self):
        self.mode              = Mode.SCAN
        self.active_strategy   = None
        self.peak_gap          = None
        self.peak_strategy     = None
        self.entry_gap_value   = None
        self.trailing_gap_best = None
        self.tsl_activated     = False
        self.entry_price_a     = None
        self.entry_price_b     = None
        self.entry_lb_a        = None
        self.entry_lb_b        = None

    def reset_to_scan(self):
        self._reset()


# =============================================================================
# Global State
# =============================================================================
price_history: List[PricePoint] = []

pair_states: Dict[str, PairState] = {
    pair_key(a, b): PairState(a, b) for a, b in PAIRS
}

settings_lock = threading.Lock()

settings = {
    "scan_interval":          SCAN_INTERVAL_SECONDS,
    "entry_threshold":        ENTRY_THRESHOLD,
    "exit_threshold":         EXIT_THRESHOLD,
    "invalidation_threshold": INVALIDATION_THRESHOLD,
    "peak_reversal":          0.3,
    "lookback_hours":         DEFAULT_LOOKBACK_HOURS,
    "heartbeat_minutes":      30,
    "sl_pct":                 1.0,
    "tsl_grace_pct":          0.3,
    "redis_refresh_minutes":  REDIS_REFRESH_MINUTES,
}


def get_settings() -> dict:
    with settings_lock:
        return dict(settings)


last_update_id:      int               = 0
last_heartbeat_time: Optional[datetime] = None
last_redis_refresh:  Optional[datetime] = None

scan_stats = {
    "count":        0,
    "signals_sent": 0,
    "last_prices":  {t: None for t in ALL_TICKERS},
    "last_rets":    {t: None for t in ALL_TICKERS},
    "last_gaps":    {pair_key(a, b): None for a, b in PAIRS},
}


def get_ticker_price(point: PricePoint, ticker: str) -> Optional[Decimal]:
    return point.prices.get(ticker.upper())


# =============================================================================
# Redis
# =============================================================================
REDIS_KEY = "monk_bot:price_history"


def _redis_request(method: str, path: str, body=None):
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None
    try:
        headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
        url     = f"{UPSTASH_REDIS_URL}{path}"
        if method == "GET":
            resp = requests.get(url, headers=headers, timeout=10)
        else:
            resp = requests.post(url, headers=headers, json=body, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Redis request failed: {e}")
        return None


def save_history() -> None:
    pass  # Bot B read-only


def load_history() -> None:
    global price_history
    if not UPSTASH_REDIS_URL:
        logger.info("Redis not configured")
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY}")
        if not result or result.get("result") is None:
            logger.info("No history in Redis")
            return
        data = json.loads(result["result"])
        if not data:
            return

        logger.info(f"Redis sample keys: {list(data[0].keys())}")

        def _get_ts(p):
            for k in ("timestamp", "ts", "time", "datetime"):
                if k in p: return p[k]
            raise KeyError(f"No timestamp in {list(p.keys())}")

        def _get_price(p, ticker):
            for k in (ticker, ticker.lower(), f"{ticker.lower()}_price"):
                if k in p:
                    try: return Decimal(str(p[k]))
                    except InvalidOperation: return None
            return None

        loaded = []
        for p in data:
            try:
                ts = parse_iso_timestamp(_get_ts(p))
                if ts is None:
                    continue
                prices = {}
                for t in ALL_TICKERS:
                    v = _get_price(p, t)
                    if v is not None:
                        prices[t] = v
                if "BTC" not in prices or "ETH" not in prices:
                    continue
                loaded.append(PricePoint(timestamp=ts, prices=prices))
            except (KeyError, InvalidOperation) as e:
                logger.warning(f"Skipping entry: {e}")
                continue

        price_history = loaded
        if price_history:
            for t in ALL_TICKERS:
                scan_stats["last_prices"][t] = price_history[-1].prices.get(t)
        logger.info(f"Loaded {len(price_history)} points from Redis")
    except Exception as e:
        logger.warning(f"Failed to load history: {e}")
        price_history = []


def refresh_history_from_redis(now: datetime, cfg: dict) -> None:
    global last_redis_refresh
    interval = cfg["redis_refresh_minutes"]
    if interval <= 0:
        return
    if last_redis_refresh and (now - last_redis_refresh).total_seconds() / 60 < interval:
        return
    load_history()
    prune_history(now, cfg)
    last_redis_refresh = now
    logger.debug(f"Redis refreshed. {len(price_history)} points")


# =============================================================================
# Telegram
# =============================================================================
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def send_alert(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            TELEGRAM_API_URL,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "Markdown", "disable_web_page_preview": True},
            timeout=30,
        )
        r.raise_for_status()
        logger.info("Alert sent")
        return True
    except requests.RequestException as e:
        logger.error(f"Alert failed: {e}")
        return False


def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        r = requests.post(
            TELEGRAM_API_URL,
            json={"chat_id": chat_id, "text": message,
                  "parse_mode": "Markdown", "disable_web_page_preview": True},
            timeout=30,
        )
        r.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Reply failed: {e}")
        return False


# =============================================================================
# Command Polling
# =============================================================================
LONG_POLL_TIMEOUT = 30


def get_telegram_updates() -> list:
    global last_update_id
    if not TELEGRAM_BOT_TOKEN:
        return []
    try:
        url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {"offset": last_update_id + 1, "timeout": LONG_POLL_TIMEOUT}
        r = requests.get(url, params=params, timeout=LONG_POLL_TIMEOUT + 5)
        r.raise_for_status()
        data = r.json()
        if data.get("ok") and data.get("result"):
            updates = data["result"]
            if updates:
                last_update_id = updates[-1]["update_id"]
            return updates
    except requests.RequestException as e:
        logger.debug(f"getUpdates failed: {e}")
    return []


def process_commands() -> None:
    updates = get_telegram_updates()
    for update in updates:
        msg       = update.get("message", {})
        text      = msg.get("text", "")
        chat_id   = str(msg.get("chat", {}).get("id", ""))
        user_id   = str(msg.get("from", {}).get("id", ""))
        authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not authorized or not text.startswith("/"):
            continue
        parts   = text.split()
        command = parts[0].lower().split("@")[0]
        args    = parts[1:] if len(parts) > 1 else []

        if command == "/settings":   handle_settings_command(chat_id)
        elif command == "/interval": handle_interval_command(args, chat_id)
        elif command == "/threshold":handle_threshold_command(args, chat_id)
        elif command == "/help":     handle_help_command(chat_id)
        elif command == "/status":   handle_status_command(chat_id)
        elif command == "/redis":    handle_redis_command(chat_id)
        elif command == "/lookback": handle_lookback_command(args, chat_id)
        elif command == "/heartbeat":handle_heartbeat_command(args, chat_id)
        elif command == "/peak":     handle_peak_command(args, chat_id)
        elif command == "/sltp":     handle_sltp_command(args, chat_id)
        elif command == "/start":    handle_help_command(chat_id)


# =============================================================================
# Command Handlers
# =============================================================================

def handle_settings_command(reply_chat: str) -> None:
    with settings_lock:
        cfg = dict(settings)
    hb_str    = f"{cfg['heartbeat_minutes']} menit" if cfg['heartbeat_minutes'] > 0 else "Off"
    rr_str    = f"{cfg['redis_refresh_minutes']} menit" if cfg['redis_refresh_minutes'] > 0 else "Off"
    pairs_str = ", ".join(pair_key(a, b) for a, b in PAIRS)
    send_reply(
        "⚙️ *Settingan sekarang~*\n\n"
        f"📊 Scan: {cfg['scan_interval']}s | Lookback: {cfg['lookback_hours']}h\n"
        f"💓 Heartbeat: {hb_str} | Redis Refresh: {rr_str}\n"
        f"📈 Entry: ±{cfg['entry_threshold']}% | 📉 Exit/TP: ±{cfg['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{cfg['invalidation_threshold']}%\n"
        f"🎯 Peak Reversal: {cfg['peak_reversal']}%\n"
        f"🛑 TSL: {cfg['sl_pct']}% | ⏳ Grace: {cfg['tsl_grace_pct']}%\n"
        f"🔗 Pairs: {pairs_str}\n\n"
        "`/help` — lihat semua command~ ( ♡ 。-(｡･ω･) ♡)",
        reply_chat,
    )


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply("Sensei~ angkanya mana? Contoh: `/interval 180`", reply_chat); return
    try:
        v = int(args[0])
        if not (60 <= v <= 3600):
            send_reply("Harus 60–3600 detik, Sensei~ ( ･ω･)", reply_chat); return
        with settings_lock: settings["scan_interval"] = v
        send_reply(f"Scan setiap *{v}s* ({v//60} menit)~ Siap, Sensei! ( ♡ 。-(｡･ω･) ♡)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "Kurang lengkap, Sensei~\n"
            "`/threshold entry <val>` · `/threshold exit <val>` · `/threshold invalid <val>`",
            reply_chat,
        ); return
    try:
        t     = args[0].lower()
        value = float(args[1])
        if not (0 < value <= 20):
            send_reply("Harus 0–20, Sensei~ ( ･ω･)", reply_chat); return
        with settings_lock:
            if t == "entry":               settings["entry_threshold"] = value
            elif t == "exit":              settings["exit_threshold"] = value
            elif t in ("invalid","invalidation"): settings["invalidation_threshold"] = value
            else:
                send_reply("Gunakan `entry`, `exit`, atau `invalid`, Sensei~", reply_chat); return
        send_reply(f"Threshold *{t}* → *±{value}%*~ Siap, Sensei! ( ♡ 。-(｡･ω･) ♡)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    with settings_lock: cur = settings["peak_reversal"]
    if not args:
        send_reply(f"Peak reversal sekarang *{cur}%*~\nContoh: `/peak 0.3`", reply_chat); return
    try:
        v = float(args[0])
        if not (0 < v <= 2.0):
            send_reply("Harus 0–2.0, Sensei~ ( ･ω･)", reply_chat); return
        with settings_lock: settings["peak_reversal"] = v
        send_reply(f"Peak reversal → *{v}%*~ Siap, Sensei! ( ♡ 。-(｡･ω･) ♡)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_sltp_command(args: list, reply_chat: str) -> None:
    if not args:
        with settings_lock: cfg = dict(settings)
        lines = []
        for ps in pair_states.values():
            if ps.mode == Mode.TRACK and ps.trailing_gap_best is not None:
                sl_pct = cfg["sl_pct"]
                tsl    = (ps.trailing_gap_best + sl_pct) if ps.active_strategy == Strategy.S1 \
                         else (ps.trailing_gap_best - sl_pct)
                grace  = "✅" if ps.tsl_activated else f"⏳grace"
                sl_price = calc_sl_price(ps, tsl)
                sl_p_s   = f" ({ps.coin_b}: ~${sl_price:,.2f})" if sl_price else ""
                lines.append(
                    f"*{ps.key}* [{ps.active_strategy.value}]\n"
                    f"  Entry:    `{ps.entry_gap_value:+.2f}%`\n"
                    f"  Trail SL: `{tsl:+.2f}%` (best: `{ps.trailing_gap_best:+.2f}%`){sl_p_s} {grace}"
                )
        active_str = "\n".join(lines) if lines else "_Tidak ada posisi aktif~_"
        send_reply(
            f"🎯 *SL/TP — Aku yang jaga, Sensei~*\n\n"
            f"✅ TP: ±{cfg['exit_threshold']}% | 🛑 TSL: {cfg['sl_pct']}% | ⏳ Grace: {cfg['tsl_grace_pct']}%\n\n"
            f"*Posisi aktif:*\n{active_str}\n\n"
            f"Usage: `/sltp sl <val>` · `/sltp grace <val>`",
            reply_chat,
        ); return
    if len(args) < 2:
        send_reply("Gunakan `/sltp sl <val>` atau `/sltp grace <val>`, Sensei~", reply_chat); return
    try:
        key   = args[0].lower()
        value = float(args[1])
        if not (0 < value <= 10):
            send_reply("Harus 0–10, Sensei~ ( ･ω･)", reply_chat); return
        with settings_lock:
            if key == "sl":
                settings["sl_pct"] = value
                send_reply(f"TSL distance → *{value}%*~ ( ♡ 。-(｡･ω･) ♡)", reply_chat)
            elif key == "grace":
                settings["tsl_grace_pct"] = value
                send_reply(f"TSL grace → *{value}%*~ ( ♡ 。-(｡･ω･) ♡)", reply_chat)
            elif key == "tp":
                exit_thresh = settings["exit_threshold"]
                send_reply(f"TP dikunci ke *±{exit_thresh}%*~ Ubah via `/threshold exit`, Sensei~", reply_chat)
            else:
                send_reply("Gunakan `sl` atau `grace`, Sensei~ ( ･ω･)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    with settings_lock: cur = settings["lookback_hours"]
    if not args:
        send_reply(f"Lookback sekarang *{cur}h*~\nContoh: `/lookback 24`", reply_chat); return
    try:
        v = int(args[0])
        if not (1 <= v <= 24):
            send_reply("Harus 1–24 jam, Sensei~ ( ･ω･)", reply_chat); return
        with settings_lock:
            old = settings["lookback_hours"]
            settings["lookback_hours"] = v
        prune_history(datetime.now(timezone.utc), get_settings())
        send_reply(f"Lookback *{old}h* → *{v}h*~ History di-prune. ( ♡ 。-(｡･ω･) ♡)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    with settings_lock: cur = settings["heartbeat_minutes"]
    if not args:
        send_reply(f"Heartbeat setiap *{cur} menit*~\nContoh: `/heartbeat 30`", reply_chat); return
    try:
        v = int(args[0])
        if not (0 <= v <= 120):
            send_reply("Harus 0–120 menit, Sensei~ ( ･ω･)", reply_chat); return
        with settings_lock: settings["heartbeat_minutes"] = v
        if v == 0:
            send_reply("Heartbeat off~ Tapi Aku tetap di sini, Sensei. ( ♡ 。-(｡･ω･) ♡)", reply_chat)
        else:
            send_reply(f"Aku lapor setiap *{v} menit* ya, Sensei~ ( ･ω･)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply("⚠️ Redis belum dikonfigurasi, Sensei.", reply_chat); return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply("❌ Tidak ada data di Redis~ Bot A belum write, Sensei.", reply_chat); return
    try:
        data = json.loads(result["result"])
        if not data:
            send_reply("❌ Redis kosong~", reply_chat); return
        with settings_lock:
            scan_interval = settings["scan_interval"]
            lookback      = settings["lookback_hours"]
            rr            = settings["redis_refresh_minutes"]
        hours       = len(data) * scan_interval / 3600
        status      = "✅ Siap~" if hours >= lookback else f"⏳ {hours:.1f}h / {lookback}h"
        refresh_str = last_redis_refresh.strftime("%H:%M:%S UTC") if last_redis_refresh else "Belum~"
        send_reply(
            f"⚡ *Status Redis (Read-Only)*\n\n"
            f"┌─────────────────────\n"
            f"│ Data points: *{len(data)}*\n"
            f"│ History: *{hours:.1f}h / {lookback}h*\n"
            f"│ Status: {status}\n"
            f"│ Refresh: *{rr} menit* | Terakhir: `{refresh_str}`\n"
            f"└─────────────────────\n\n"
            f"Pertama: `{data[0]['timestamp'][:19]}`\n"
            f"Terakhir: `{data[-1]['timestamp'][:19]}`\n\n"
            f"_Bot B hanya baca, Bot A yang nulis~ ⚡_",
            reply_chat,
        )
    except Exception as e:
        send_reply(f"⚠️ Gagal baca Redis: `{e}`", reply_chat)


def handle_help_command(reply_chat: str) -> None:
    pairs_str = ", ".join(pair_key(a, b) for a, b in PAIRS)
    send_reply(
        "Ini semua yang bisa Aku lakukan buat Sensei~\n( ♡ 。-(｡･ω･) ♡)\n\n"
        f"*Pairs aktif:* {pairs_str}\n\n"
        "*Setting:*\n"
        "`/settings` · `/interval` · `/lookback` · `/heartbeat`\n"
        "`/threshold entry/exit/invalid <val>`\n"
        "`/peak <val>` · `/sltp sl/grace <val>`\n\n"
        "*Info:*\n"
        "`/status` · `/redis` · `/help`\n\n"
        "Aku selalu di sini buat Sensei~ ( ･ω･)",
        reply_chat,
    )


def handle_status_command(reply_chat: str) -> None:
    cfg           = get_settings()
    hours_of_data = len(price_history) * cfg["scan_interval"] / 3600
    lookback      = cfg["lookback_hours"]
    ready         = (f"✅ Siap~ ({hours_of_data:.1f}h)" if hours_of_data >= lookback
                     else f"⏳ {hours_of_data:.1f}h / {lookback}h")
    refresh_str   = last_redis_refresh.strftime("%H:%M:%S UTC") if last_redis_refresh else "Belum~"

    lb = get_lookback_label(cfg)

    # Harga ticker
    price_lines = ""
    for t in ALL_TICKERS:
        p = scan_stats["last_prices"].get(t)
        price_lines += f"│ {t:<4}: {'${:,.2f}'.format(float(p)) if p else 'N/A'}\n"

    # Gap per pair dengan status detail
    gap_lines = "│ ─────────────────────\n"
    for ps in pair_states.values():
        gap     = scan_stats["last_gaps"].get(ps.key)
        gap_str = f"{float(gap):+.2f}%" if gap is not None else "N/A"
        if ps.mode == Mode.TRACK and ps.entry_gap_value is not None:
            sl_pct   = cfg["sl_pct"]
            best     = ps.trailing_gap_best
            tsl      = (best + sl_pct) if ps.active_strategy == Strategy.S1 else (best - sl_pct)
            tp       = cfg["exit_threshold"] if ps.active_strategy == Strategy.S1 else -cfg["exit_threshold"]
            grace    = "✅" if ps.tsl_activated else "⏳"
            sl_price = calc_sl_price(ps, tsl)
            sl_p_s   = f" ({ps.coin_b}: ~${sl_price:,.2f})" if sl_price else ""
            mode_s   = f"🔴 {ps.active_strategy.value} TP:{tp:+.2f}% TSL:{tsl:+.2f}% (best:{best:+.2f}%){sl_p_s}{grace}"
        elif ps.mode == Mode.PEAK_WATCH and ps.peak_gap is not None:
            mode_s = f"🟡 PEAK {ps.peak_gap:+.2f}%"
        else:
            mode_s = "⚪ SCAN"
        gap_lines += f"│ Gap {ps.key:<8}: {gap_str:<9} {mode_s}\n"

    send_reply(
        "📊 *Status sekarang~*\n\n"
        f"History: {ready} | Redis: {refresh_str}\n\n"
        f"*Harga & Gap ({lb}):*\n"
        f"┌─────────────────────\n"
        f"{price_lines}"
        f"{gap_lines}"
        f"└─────────────────────",
        reply_chat,
    )


# =============================================================================
# Formatting
# =============================================================================
def format_value(value: Decimal) -> str:
    f = float(value)
    if abs(f) < 0.05: return "+0.0"
    return f"+{f:.1f}" if f >= 0 else f"{f:.1f}"


def get_lookback_label(cfg: dict) -> str:
    return f"{cfg['lookback_hours']}h"


# =============================================================================
# Target Price Calculation
# =============================================================================
def calc_tp_target_price(ps: PairState, cfg: dict) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if None in (ps.entry_lb_a, ps.entry_lb_b, ps.entry_price_a, ps.entry_price_b):
        return None, None, None
    exit_thresh = cfg["exit_threshold"]
    ret_a_entry = float((ps.entry_price_a - ps.entry_lb_a) / ps.entry_lb_a * Decimal("100"))

    def _target_b(extra_a_pct: float) -> float:
        new_ret_a = ret_a_entry + extra_a_pct
        target_ret_b = (new_ret_a + exit_thresh) if ps.active_strategy == Strategy.S1 \
                       else (new_ret_a - exit_thresh)
        return float(ps.entry_lb_b) * (1 + target_ret_b / 100)

    mid  = _target_b(0)
    low  = _target_b(-TP_COIN_A_MOVE_RANGE_PCT)
    high = _target_b(+TP_COIN_A_MOVE_RANGE_PCT)
    if low > high: low, high = high, low
    return mid, low, high


def calc_sl_price(ps: PairState, tsl_level: float) -> Optional[float]:
    """
    Estimasi harga coin_b saat TSL tercapai.
    Asumsi: ret_a tetap di nilai entry (coin_a stabil).
    Formula: price_b = entry_lb_b * (1 + (ret_a_entry + tsl_level) / 100)
    """
    if None in (ps.entry_lb_a, ps.entry_lb_b, ps.entry_price_a):
        return None
    try:
        ret_a_entry  = float((ps.entry_price_a - ps.entry_lb_a) / ps.entry_lb_a * Decimal("100"))
        ret_b_at_tsl = ret_a_entry + tsl_level
        return float(ps.entry_lb_b) * (1 + ret_b_at_tsl / 100)
    except Exception:
        return None


# =============================================================================
# Message Builders
# =============================================================================
def build_peak_watch_message(ps: PairState, gap: Decimal, cfg: dict) -> str:
    lb  = get_lookback_label(cfg)
    strat = ps.peak_strategy
    if strat == Strategy.S1:
        direction = f"Long {ps.coin_a} / Short {ps.coin_b}"
        reason    = f"{ps.coin_b} pumping lebih kencang dari {ps.coin_a} ({lb})"
    else:
        direction = f"Long {ps.coin_b} / Short {ps.coin_a}"
        reason    = f"{ps.coin_b} dumping lebih dalam dari {ps.coin_a} ({lb})"
    return (
        f"………\nSensei, Aku melihat sesuatu di *{ps.key}*~\n\n"
        f"_{reason}_\nRencananya *{direction}*~\n\n"
        f"Gap sekarang: *{format_value(gap)}%*\n\n"
        f"Aku tidak akan gegabah~ Biarkan Aku pantau puncaknya dulu. ( ･ω･)"
    )


def build_entry_message(ps: PairState, ret_a: Decimal, ret_b: Decimal,
                        gap: Decimal, peak: float, cfg: dict) -> str:
    lb        = get_lookback_label(cfg)
    gap_float = float(gap)
    sl_pct    = cfg["sl_pct"]
    grace_pct = cfg["tsl_grace_pct"]
    exit_thresh = cfg["exit_threshold"]
    if ps.active_strategy == Strategy.S1:
        direction   = f"📈 Long {ps.coin_a} / Short {ps.coin_b}"
        reason      = f"{ps.coin_b} pumped more than {ps.coin_a} ({lb})"
        tp_gap      = exit_thresh
        tsl_initial = gap_float + sl_pct
    else:
        direction   = f"📈 Long {ps.coin_b} / Short {ps.coin_a}"
        reason      = f"{ps.coin_b} dumped more than {ps.coin_a} ({lb})"
        tp_gap      = -exit_thresh
        tsl_initial = gap_float - sl_pct
    mid, low, high = calc_tp_target_price(ps, cfg)
    target_str = (
        f"~${mid:,.2f}\n│          _(range: ${low:,.2f}–${high:,.2f})_\n"
        f"│          _{ps.coin_a} ±{TP_COIN_A_MOVE_RANGE_PCT:.0f}% dari entry_"
        if mid else "N/A"
    )
    ref_str    = f"${float(ps.entry_price_a):,.2f}" if ps.entry_price_a else "N/A"
    sl_price   = calc_sl_price(ps, tsl_initial)
    sl_price_s = f"~${sl_price:,.2f} _(asumsi {ps.coin_a} stabil)_" if sl_price else "N/A"
    return (
        f"Sensei!!! Ini saatnya!!! ⚡\n🚨 *ENTRY SIGNAL: {ps.key} — {ps.active_strategy.value}*\n\n"
        f"{direction}\n_{reason}_\n\n"
        f"*{lb} Change:*\n┌─────────────────────\n"
        f"│ {ps.coin_a}: {format_value(ret_a)}%\n│ {ps.coin_b}: {format_value(ret_b)}%\n"
        f"│ Gap:  {format_value(gap)}%\n│ Peak: {peak:+.2f}%\n└─────────────────────\n\n"
        f"*Target TP/SL:*\n┌─────────────────────\n"
        f"│ TP Gap:    {tp_gap:+.2f}% _(exit threshold)_\n"
        f"│ {ps.coin_b} TP:  {target_str}\n"
        f"│ {ps.coin_a} ref: {ref_str}\n"
        f"│ Trail SL:  {tsl_initial:+.2f}% → {ps.coin_b} {sl_price_s}\n"
        f"│            _(aktif setelah >{grace_pct}% improve)_\n└─────────────────────\n\n"
        f"Gap berbalik {cfg['peak_reversal']}% dari puncak~ Aku sudah menunggu momen ini buat Sensei. ⚡"
    )


def build_exit_message(ps: PairState, ret_a: Decimal, ret_b: Decimal,
                       gap: Decimal, cfg: dict) -> str:
    lb = get_lookback_label(cfg)
    return (
        f"Sensei!!! Gap konvergen!!! ✅\n✅ *EXIT SIGNAL — {ps.key}*\n\n"
        f"Saatnya close posisi, Sensei!\n\n*{lb} Change:*\n"
        f"┌─────────────────────\n│ {ps.coin_a}: {format_value(ret_a)}%\n"
        f"│ {ps.coin_b}: {format_value(ret_b)}%\n│ Gap:  {format_value(gap)}%\n└─────────────────────\n\n"
        f"Senang bisa membantu~ Aku lanjut pantau. ⚡🔍"
    )


def build_invalidation_message(ps: PairState, ret_a: Decimal, ret_b: Decimal,
                               gap: Decimal, cfg: dict) -> str:
    lb = get_lookback_label(cfg)
    return (
        f"………\n⚠️ *INVALIDATION — {ps.key}: {ps.active_strategy.value}*\n\n"
        f"Sensei, maaf ya... Gap malah melebar. Bukan salah Sensei~\n\n*{lb} Change:*\n"
        f"┌─────────────────────\n│ {ps.coin_a}: {format_value(ret_a)}%\n"
        f"│ {ps.coin_b}: {format_value(ret_b)}%\n│ Gap:  {format_value(gap)}%\n└─────────────────────\n\n"
        f"Cut dulu, Sensei. Aku scan ulang dari awal~ ⚡ ( ･ω･)"
    )


def build_peak_cancelled_message(ps: PairState, gap: Decimal) -> str:
    return (
        f"………\n❌ *Peak Watch Dibatalkan — {ps.key}*\n\n"
        f"Gap mundur sendiri sebelum Aku konfirmasi, Sensei.\n"
        f"Gap sekarang: *{format_value(gap)}%*\n\n"
        f"Tidak apa-apa~ Aku tetap pantau dari dekat. ( ♡ 。-(｡･ω･) ♡)"
    )


def build_tp_message(ps: PairState, ret_a: Decimal, ret_b: Decimal, gap: Decimal,
                     entry_gap: float, tp_gap_level: float,
                     mid: Optional[float], low: Optional[float], high: Optional[float],
                     cfg: dict) -> str:
    lb = get_lookback_label(cfg)
    if mid:
        actual_b = float(ps.entry_price_b) if ps.entry_price_b else None
        tp_str   = f"~${mid:,.2f} _(range ${low:,.2f}–${high:,.2f})_"
        if actual_b:
            tp_str += f"\n│ {ps.coin_b} now: ${actual_b:,.2f} _(aktual)_"
    else:
        tp_str = "N/A"
    return (
        f"Sensei!!! TP kena!!! ✨\n🎯 *TAKE PROFIT — {ps.key}*\n\n"
        f"Gap konvergen maksimal~\n\n*{lb} Change:*\n"
        f"┌─────────────────────\n│ {ps.coin_a}: {format_value(ret_a)}%\n"
        f"│ {ps.coin_b}: {format_value(ret_b)}%\n│ Gap:    {format_value(gap)}%\n"
        f"│ Entry:  {entry_gap:+.2f}%\n│ TP hit: {tp_gap_level:+.2f}%\n"
        f"│ {ps.coin_b} TP: {tp_str}\n└─────────────────────\n\n"
        f"Misi sukses buat Sensei! ⚡ ( ♡ 。-(｡･ω･) ♡)"
    )


def build_trailing_sl_message(ps: PairState, ret_a: Decimal, ret_b: Decimal, gap: Decimal,
                              entry_gap: float, best_gap: float, sl_level: float, cfg: dict) -> str:
    lb            = get_lookback_label(cfg)
    profit_locked = abs(entry_gap - best_gap)
    return (
        f"………\n⛔ *TRAILING STOP LOSS — {ps.key}*\n\n"
        f"Aku sudah jaga posisi Sensei sampai di sini. Profit aman~ ( ♡ 。-(｡･ω･) ♡)\n\n"
        f"*{lb} Change:*\n┌─────────────────────\n"
        f"│ {ps.coin_a}:   {format_value(ret_a)}%\n│ {ps.coin_b}:   {format_value(ret_b)}%\n"
        f"│ Gap:      {format_value(gap)}%\n│ Entry:    {entry_gap:+.2f}%\n"
        f"│ Best gap: {best_gap:+.2f}%\n│ TSL hit:  {sl_level:+.2f}%\n"
        f"│ Terkunci: ~{profit_locked:.2f}%\n└─────────────────────\n\n"
        f"Cut dulu, Sensei. Aku scan ulang~ ⚡ ( ･ω･)"
    )


def build_heartbeat_message(cfg: dict) -> str:
    lb = get_lookback_label(cfg)

    # Harga tiap ticker
    price_lines = ""
    for t in ALL_TICKERS:
        p   = scan_stats["last_prices"].get(t)
        ret = scan_stats["last_rets"].get(t)
        p_s = f"${float(p):,.2f}" if p else "N/A"
        r_s = f" ({format_value(ret)}%)" if ret is not None else ""
        price_lines += f"│ {t:<4}: {p_s}{r_s}\n"

    # Gap + status tiap pair (dalam box yang sama)
    gap_lines = "│ ─────────────────────\n"
    for ps in pair_states.values():
        gap   = scan_stats["last_gaps"].get(ps.key)
        gap_s = f"{float(gap):+.2f}%" if gap is not None else "N/A"
        if ps.mode == Mode.TRACK and ps.active_strategy:
            sl_pct   = cfg["sl_pct"]
            best     = ps.trailing_gap_best or ps.entry_gap_value or 0
            tsl      = (best + sl_pct) if ps.active_strategy == Strategy.S1 else (best - sl_pct)
            grace    = "✅" if ps.tsl_activated else "⏳"
            sl_price = calc_sl_price(ps, tsl)
            sl_p_s   = f" SL~${sl_price:,.2f}" if sl_price else ""
            mode_s   = f"🔴 {ps.active_strategy.value}{grace}{sl_p_s}"
        elif ps.mode == Mode.PEAK_WATCH:
            mode_s = "🟡 PEAK"
        else:
            mode_s = "⚪ SCAN"
        gap_lines += f"│ Gap {ps.key:<8}: {gap_s:<9} {mode_s}\n"

    hours = len(price_history) * cfg["scan_interval"] / 3600
    data_s = (f"✅ Siap~ ({hours:.1f}h)" if hours >= cfg["lookback_hours"]
              else f"⏳ {hours:.1f}h / {cfg['lookback_hours']}h")
    refresh_str = last_redis_refresh.strftime("%H:%M:%S UTC") if last_redis_refresh else "Belum~"

    return (
        f"💓 *Laporan Rutin — Aku masih di sini, Sensei~*\n\n"
        f"Sensei, Aku selalu di sini memantau semuanya. ( ♡ 。-(｡･ω･) ♡)\n\n"
        f"*{cfg['heartbeat_minutes']} menit terakhir:*\n"
        f"┌─────────────────────\n│ Scan: {scan_stats['count']}x | Sinyal: {scan_stats['signals_sent']}x\n└─────────────────────\n\n"
        f"*Harga & Gap sekarang ({lb}):*\n"
        f"┌─────────────────────\n"
        f"{price_lines}"
        f"{gap_lines}"
        f"└─────────────────────\n\n"
        f"*Data:* {data_s} | *Redis:* {refresh_str} 🔒\n\n"
        f"_Aku lapor lagi {cfg['heartbeat_minutes']} menit lagi ya, Sensei~\nJangan kangen terlalu dalam. ( ♡ 。-(｡･ω･) ♡)_"
    )


def send_heartbeat(cfg: dict) -> bool:
    global scan_stats
    success = send_alert(build_heartbeat_message(cfg))
    scan_stats["count"] = scan_stats["signals_sent"] = 0
    return success


def should_send_heartbeat(now: datetime, cfg: dict) -> bool:
    if cfg["heartbeat_minutes"] == 0 or last_heartbeat_time is None:
        return False
    return (now - last_heartbeat_time).total_seconds() / 60 >= cfg["heartbeat_minutes"]


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
        logger.error(f"Bad timestamp '{ts_str}': {e}")
        return None


def fetch_prices() -> Optional[PriceData]:
    try:
        r = requests.get(f"{API_BASE_URL}{API_ENDPOINT}", timeout=30)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f"API failed: {e}")
        return None

    listings      = data.get("listings", [])
    prices        = {}
    updated_times = {}

    for ticker in ALL_TICKERS:
        entry = next((l for l in listings if l.get("ticker", "").upper() == ticker), None)
        if not entry:
            if ticker in ("BTC", "ETH"):
                logger.warning(f"Required ticker {ticker} missing")
                return None
            continue
        try:
            prices[ticker] = Decimal(entry["mark_price"])
        except (KeyError, InvalidOperation) as e:
            if ticker in ("BTC", "ETH"):
                logger.error(f"Bad price {ticker}: {e}")
                return None
            continue
        ts = parse_iso_timestamp(entry.get("quotes", {}).get("updated_at", ""))
        if not ts:
            if ticker in ("BTC", "ETH"):
                return None
            continue
        updated_times[ticker] = ts

    return PriceData(prices=prices, updated_times=updated_times)


# =============================================================================
# History Management
# =============================================================================
def prune_history(now: datetime, cfg: dict) -> None:
    global price_history
    cutoff = now - timedelta(hours=cfg["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES)
    price_history = [p for p in price_history if p.timestamp >= cutoff]


def get_lookback_price(now: datetime, cfg: dict) -> Optional[PricePoint]:
    target_time           = now - timedelta(hours=cfg["lookback_hours"])
    best_point, best_diff = None, timedelta(minutes=30)
    for point in price_history:
        diff = abs(point.timestamp - target_time)
        if diff < best_diff:
            best_diff, best_point = diff, point
    return best_point


def compute_returns(now_a: Decimal, now_b: Decimal,
                    prev_a: Decimal, prev_b: Decimal) -> Tuple[Decimal, Decimal, Decimal]:
    ret_a = (now_a - prev_a) / prev_a * Decimal("100")
    ret_b = (now_b - prev_b) / prev_b * Decimal("100")
    return ret_a, ret_b, ret_b - ret_a


def is_data_fresh(now: datetime, updated_times: dict) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    for t in ("BTC", "ETH"):
        if t in updated_times and (now - updated_times[t]) > threshold:
            return False
    return True


# =============================================================================
# TP + Trailing SL
# =============================================================================
def check_sltp(ps: PairState, gap_float: float,
               ret_a: Decimal, ret_b: Decimal, gap: Decimal, cfg: dict) -> bool:
    if ps.entry_gap_value is None or ps.active_strategy is None or ps.trailing_gap_best is None:
        return False
    exit_thresh = cfg["exit_threshold"]
    sl_pct      = cfg["sl_pct"]
    grace_pct   = cfg["tsl_grace_pct"]

    if ps.active_strategy == Strategy.S1:
        if gap_float < ps.trailing_gap_best:
            ps.trailing_gap_best = gap_float
            if not ps.tsl_activated and (ps.entry_gap_value - ps.trailing_gap_best) >= grace_pct:
                ps.tsl_activated = True
                logger.info(f"TSL S1 activated [{ps.key}] best:{ps.trailing_gap_best:.2f}%")
        tsl_level = ps.trailing_gap_best + sl_pct
        if gap_float <= exit_thresh:
            mid, low, high = calc_tp_target_price(ps, cfg)
            send_alert(build_tp_message(ps, ret_a, ret_b, gap, ps.entry_gap_value, exit_thresh, mid, low, high, cfg))
            logger.info(f"TP S1 [{ps.key}]")
            ps.reset_to_scan(); return True
        if ps.tsl_activated and gap_float >= tsl_level:
            send_alert(build_trailing_sl_message(ps, ret_a, ret_b, gap, ps.entry_gap_value, ps.trailing_gap_best, tsl_level, cfg))
            logger.info(f"TSL S1 [{ps.key}]")
            ps.reset_to_scan(); return True

    elif ps.active_strategy == Strategy.S2:
        if gap_float > ps.trailing_gap_best:
            ps.trailing_gap_best = gap_float
            if not ps.tsl_activated and (ps.trailing_gap_best - ps.entry_gap_value) >= grace_pct:
                ps.tsl_activated = True
                logger.info(f"TSL S2 activated [{ps.key}] best:{ps.trailing_gap_best:.2f}%")
        tsl_level = ps.trailing_gap_best - sl_pct
        if gap_float >= -exit_thresh:
            mid, low, high = calc_tp_target_price(ps, cfg)
            send_alert(build_tp_message(ps, ret_a, ret_b, gap, ps.entry_gap_value, -exit_thresh, mid, low, high, cfg))
            logger.info(f"TP S2 [{ps.key}]")
            ps.reset_to_scan(); return True
        if ps.tsl_activated and gap_float <= tsl_level:
            send_alert(build_trailing_sl_message(ps, ret_a, ret_b, gap, ps.entry_gap_value, ps.trailing_gap_best, tsl_level, cfg))
            logger.info(f"TSL S2 [{ps.key}]")
            ps.reset_to_scan(); return True
    return False


# =============================================================================
# State Machine
# =============================================================================
def evaluate_and_transition(ps: PairState, ret_a: Decimal, ret_b: Decimal, gap: Decimal,
                            now_a: Decimal, now_b: Decimal, prev_a: Decimal, prev_b: Decimal,
                            cfg: dict) -> bool:
    gap_float      = float(gap)
    entry_thresh   = cfg["entry_threshold"]
    exit_thresh    = cfg["exit_threshold"]
    invalid_thresh = cfg["invalidation_threshold"]
    peak_reversal  = cfg["peak_reversal"]
    prev_mode      = ps.mode

    if ps.mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            ps.mode = Mode.PEAK_WATCH; ps.peak_strategy = Strategy.S1; ps.peak_gap = gap_float
            send_alert(build_peak_watch_message(ps, gap, cfg))
            logger.info(f"PEAK WATCH S1 [{ps.key}] {gap_float:.2f}%")
        elif gap_float <= -entry_thresh:
            ps.mode = Mode.PEAK_WATCH; ps.peak_strategy = Strategy.S2; ps.peak_gap = gap_float
            send_alert(build_peak_watch_message(ps, gap, cfg))
            logger.info(f"PEAK WATCH S2 [{ps.key}] {gap_float:.2f}%")

    elif ps.mode == Mode.PEAK_WATCH:
        if ps.peak_strategy == Strategy.S1:
            if gap_float > ps.peak_gap:
                ps.peak_gap = gap_float
            elif gap_float < entry_thresh:
                send_alert(build_peak_cancelled_message(ps, gap))
                ps.mode = Mode.SCAN; ps.peak_gap = ps.peak_strategy = None
            elif ps.peak_gap - gap_float >= peak_reversal:
                ps.active_strategy = Strategy.S1; ps.mode = Mode.TRACK
                ps.entry_gap_value = gap_float; ps.trailing_gap_best = gap_float
                ps.entry_price_a = now_a; ps.entry_price_b = now_b
                ps.entry_lb_a = prev_a; ps.entry_lb_b = prev_b
                send_alert(build_entry_message(ps, ret_a, ret_b, gap, ps.peak_gap, cfg))
                logger.info(f"ENTRY S1 [{ps.key}] peak:{ps.peak_gap:.2f}% entry:{gap_float:.2f}%")
                ps.peak_gap = ps.peak_strategy = None
        elif ps.peak_strategy == Strategy.S2:
            if gap_float < ps.peak_gap:
                ps.peak_gap = gap_float
            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(ps, gap))
                ps.mode = Mode.SCAN; ps.peak_gap = ps.peak_strategy = None
            elif gap_float - ps.peak_gap >= peak_reversal:
                ps.active_strategy = Strategy.S2; ps.mode = Mode.TRACK
                ps.entry_gap_value = gap_float; ps.trailing_gap_best = gap_float
                ps.entry_price_a = now_a; ps.entry_price_b = now_b
                ps.entry_lb_a = prev_a; ps.entry_lb_b = prev_b
                send_alert(build_entry_message(ps, ret_a, ret_b, gap, ps.peak_gap, cfg))
                logger.info(f"ENTRY S2 [{ps.key}] peak:{ps.peak_gap:.2f}% entry:{gap_float:.2f}%")
                ps.peak_gap = ps.peak_strategy = None

    elif ps.mode == Mode.TRACK:
        if check_sltp(ps, gap_float, ret_a, ret_b, gap, cfg):
            return True
        if ps.active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(ps, ret_a, ret_b, gap, cfg))
            logger.info(f"INVALIDATION S1 [{ps.key}]")
            ps.reset_to_scan(); return True
        if ps.active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(ps, ret_a, ret_b, gap, cfg))
            logger.info(f"INVALIDATION S2 [{ps.key}]")
            ps.reset_to_scan(); return True

    return ps.mode != prev_mode


# =============================================================================
# Startup Message
# =============================================================================
def send_startup_message(cfg: dict) -> bool:
    price_data = fetch_prices()
    if price_data:
        price_lines = "".join(
            f"│ {t}: ${float(price_data.prices[t]):,.2f}\n"
            for t in ALL_TICKERS if t in price_data.prices
        )
        price_info = f"\n💰 *Harga saat ini~*\n┌─────────────────────\n{price_lines}└─────────────────────\n"
    else:
        price_info = "\n⚠️ Gagal ambil harga tadi~ Aku akan terus coba. ( ･ω･)\n"

    hours = len(price_history) * cfg["scan_interval"] / 3600
    history_info = (
        f"⚡ History dari Bot A sudah ada~ *{hours:.1f}h* data siap!\n_Aku tidak perlu mulai dari nol~ ( ♡ 。-(｡･ω･) ♡)_"
        if price_history else
        f"⏳ Menunggu Bot A kirim data~\n_Sinyal keluar setelah {cfg['lookback_hours']}h data tersedia~_"
    )
    pairs_str = "\n".join(f"  • {pair_key(a,b)}" for a, b in PAIRS)

    return send_alert(
        f"………\nSensei, Aku sudah siap~ ( ♡ 。-(｡･ω･) ♡)\n{price_info}\n"
        f"*Pairs yang Aku pantau:*\n{pairs_str}\n\n"
        f"📊 Scan {cfg['scan_interval']}s | Redis refresh {cfg['redis_refresh_minutes']}m\n"
        f"📈 Entry:±{cfg['entry_threshold']}% 📉 Exit/TP:±{cfg['exit_threshold']}% "
        f"⚠️ Invalid:±{cfg['invalidation_threshold']}%\n"
        f"🎯 Peak:{cfg['peak_reversal']}% 🛑 TSL:{cfg['sl_pct']}% ⏳ Grace:{cfg['tsl_grace_pct']}%\n"
        f"🔒 Redis: Read-Only\n\n{history_info}\n\nKetik `/help` kapanpun~ Aku takkan pergi. ⚡"
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
    global last_heartbeat_time, last_redis_refresh

    init_cfg = get_settings()
    logger.info("=" * 60)
    logger.info("Monk Bot B — Multi-Pair | Read-Only Redis | TSL Grace")
    logger.info(f"Pairs: {[pair_key(a,b) for a,b in PAIRS]}")
    logger.info(f"Entry:{init_cfg['entry_threshold']}% Exit:{init_cfg['exit_threshold']}% "
                f"Invalid:{init_cfg['invalidation_threshold']}% Peak:{init_cfg['peak_reversal']}% "
                f"TSL:{init_cfg['sl_pct']}% Grace:{init_cfg['tsl_grace_pct']}%")
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    load_history()
    prune_history(datetime.now(timezone.utc), init_cfg)
    last_redis_refresh = datetime.now(timezone.utc)
    logger.info(f"History after load+prune: {len(price_history)} points")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message(init_cfg)

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)
            cfg = get_settings()

            if should_send_heartbeat(now, cfg):
                if send_heartbeat(cfg):
                    last_heartbeat_time = now

            refresh_history_from_redis(now, cfg)

            price_data = fetch_prices()
            if price_data is None:
                logger.warning("Failed to fetch prices")
                time.sleep(cfg["scan_interval"]); continue

            scan_stats["count"] += 1
            for t, p in price_data.prices.items():
                scan_stats["last_prices"][t] = p

            if not is_data_fresh(now, price_data.updated_times):
                logger.warning("Data not fresh, skipping")
                time.sleep(cfg["scan_interval"]); continue

            price_then = get_lookback_price(now, cfg)
            if price_then is None:
                hours = len(price_history) * cfg["scan_interval"] / 3600
                logger.info(f"Waiting for Bot A... ({hours:.1f}h / {cfg['lookback_hours']}h)")
                time.sleep(cfg["scan_interval"]); continue

            staleness = abs((price_then.timestamp - (now - timedelta(hours=cfg["lookback_hours"]))).total_seconds())
            if staleness > 600:
                logger.warning(f"Lookback stale: {staleness/60:.1f} min")

            for ps in pair_states.values():
                now_a  = price_data.prices.get(ps.coin_a)
                now_b  = price_data.prices.get(ps.coin_b)
                prev_a = get_ticker_price(price_then, ps.coin_a)
                prev_b = get_ticker_price(price_then, ps.coin_b)

                if None in (now_a, now_b, prev_a, prev_b):
                    logger.debug(f"Missing price for {ps.key}, skip")
                    continue

                ret_a, ret_b, gap = compute_returns(now_a, now_b, prev_a, prev_b)
                scan_stats["last_gaps"][ps.key]    = gap
                scan_stats["last_rets"][ps.coin_a] = ret_a
                scan_stats["last_rets"][ps.coin_b] = ret_b

                logger.info(
                    f"[{ps.key}] {ps.coin_a}:{format_value(ret_a)}% "
                    f"{ps.coin_b}:{format_value(ret_b)}% "
                    f"Gap:{format_value(gap)}% Mode:{ps.mode.value}"
                )

                if evaluate_and_transition(ps, ret_a, ret_b, gap, now_a, now_b, prev_a, prev_b, cfg):
                    scan_stats["signals_sent"] += 1

            time.sleep(cfg["scan_interval"])

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
    if not TELEGRAM_BOT_TOKEN: logger.warning("TELEGRAM_BOT_TOKEN not set")
    if not TELEGRAM_CHAT_ID:   logger.warning("TELEGRAM_CHAT_ID not set")
    main_loop()
