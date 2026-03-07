#!/usr/bin/env python3
"""
Monk Bot B - BTC/ETH Divergence Alert Bot (Read-Only Redis Consumer)

Monitors BTC/ETH price divergence dan kirim Telegram alerts
untuk ENTRY, EXIT, INVALIDATION, TP, dan Trailing SL.

Perubahan dari versi sebelumnya:
- Redis READ-ONLY: history dikonsumsi dari Bot A, Bot B tidak pernah write
- TP maksimal = exit threshold (konvergen penuh), bukan tp_pct lagi
- Target harga ETH/BTC ditampilkan saat entry signal (dengan range estimasi)
- Refresh history dari Redis setiap 1 menit
- Thread-safe settings via RWLock pattern
- TSL grace period: TSL baru aktif setelah gap improve >= tsl_grace_pct dari entry
- Safety net exit dihapus untuk menghilangkan double-alert TP+EXIT
"""
import json
import os
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
    UPSTASH_REDIS_URL,
    UPSTASH_REDIS_TOKEN,
    logger,
)


# =============================================================================
# Constants
# =============================================================================
DEFAULT_LOOKBACK_HOURS = 24
HISTORY_BUFFER_MINUTES = 30
REDIS_REFRESH_MINUTES  = 1     # Re-read Redis dari Bot A setiap 1 menit

# Asumsi maksimal pergerakan BTC untuk estimasi range TP harga ETH
TP_BTC_MOVE_RANGE_PCT  = 2.0   # ±2% dari harga BTC entry untuk batas estimasi


# =============================================================================
# Data Structures
# =============================================================================
class Mode(Enum):
    SCAN       = "SCAN"
    PEAK_WATCH = "PEAK_WATCH"
    TRACK      = "TRACK"


class Strategy(Enum):
    S1 = "S1"  # Long BTC / Short ETH
    S2 = "S2"  # Long ETH / Short BTC


class PricePoint(NamedTuple):
    timestamp: datetime
    btc:       Decimal
    eth:       Decimal


class PriceData(NamedTuple):
    btc_price:      Decimal
    eth_price:      Decimal
    btc_updated_at: datetime
    eth_updated_at: datetime


# =============================================================================
# Global State
# =============================================================================
price_history:   List[PricePoint]    = []
current_mode:    Mode                = Mode.SCAN
active_strategy: Optional[Strategy] = None

peak_gap:      Optional[float]    = None
peak_strategy: Optional[Strategy] = None

# State TP/TSL
entry_gap_value:   Optional[float] = None  # gap float saat ENTRY
trailing_gap_best: Optional[float] = None  # gap terbaik sejak entry
tsl_activated:     bool            = False  # True setelah gap improve >= tsl_grace_pct

# State harga saat entry — untuk kalkulasi target harga TP
entry_btc_price: Optional[Decimal] = None  # harga BTC saat entry
entry_eth_price: Optional[Decimal] = None  # harga ETH saat entry
entry_btc_lb:    Optional[Decimal] = None  # harga BTC lookback saat entry
entry_eth_lb:    Optional[Decimal] = None  # harga ETH lookback saat entry

# --- FIX: Thread-safe settings via lock ---
# Semua akses settings harus pakai settings_lock.
# Gunakan get_settings() untuk snapshot atomic di main loop,
# dan settings_lock saat command handler memodifikasi settings.
settings_lock = threading.Lock()

settings = {
    "scan_interval":          SCAN_INTERVAL_SECONDS,
    "entry_threshold":        ENTRY_THRESHOLD,
    "exit_threshold":         EXIT_THRESHOLD,
    "invalidation_threshold": INVALIDATION_THRESHOLD,
    "peak_reversal":          0.3,
    "lookback_hours":         DEFAULT_LOOKBACK_HOURS,
    "heartbeat_minutes":      30,
    # TP = exit_threshold (max konvergen), tidak pakai tp_pct lagi
    "sl_pct":                 1.0,  # Trailing SL distance dari gap terbaik
    # FIX: Grace period — TSL baru aktif setelah gap improve >= nilai ini dari entry
    # Mencegah TSL langsung kena saat gap hanya wiggle kecil setelah entry
    "tsl_grace_pct":          0.3,
    "redis_refresh_minutes":  REDIS_REFRESH_MINUTES,
}


def get_settings() -> dict:
    """Snapshot atomic seluruh settings dict untuk dipakai di satu iterasi loop.

    Ini mencegah race condition dimana command handler mengubah settings
    di tengah-tengah evaluasi sinyal (misalnya entry_threshold berubah
    antara cek S1 dan cek S2).
    """
    with settings_lock:
        return dict(settings)


last_update_id:      int               = 0
last_heartbeat_time: Optional[datetime] = None
last_redis_refresh:  Optional[datetime] = None  # kapan terakhir refresh Redis

scan_stats = {
    "count":          0,
    "last_btc_price": None,
    "last_eth_price": None,
    "last_btc_ret":   None,
    "last_eth_ret":   None,
    "last_gap":       None,
    "signals_sent":   0,
}


# =============================================================================
# History Persistence — READ-ONLY dari Redis (Bot A yang write)
# =============================================================================
REDIS_KEY = "monk_bot:price_history"


def _redis_request(method: str, path: str, body=None):
    """Helper HTTP request ke Upstash REST API."""
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
    """Bot B adalah read-only consumer — write dihandle Bot A."""
    pass  # No-op intentional


def load_history() -> None:
    """Load price_history dari Redis. Dipanggil saat startup dan refresh berkala."""
    global price_history
    if not UPSTASH_REDIS_URL:
        logger.info("Redis not configured, price history akan kosong")
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY}")
        if not result or result.get("result") is None:
            logger.info("No history in Redis yet (Bot A belum write?)")
            return
        data = json.loads(result["result"])
        loaded = []
        for p in data:
            ts = parse_iso_timestamp(p["timestamp"])
            if ts is None:
                continue  # skip malformed timestamp, jangan crash seluruh load
            loaded.append(PricePoint(
                timestamp=ts,
                btc=Decimal(p["btc"]),
                eth=Decimal(p["eth"]),
            ))
        price_history = loaded
        logger.info(f"Loaded {len(price_history)} points from Redis (read-only)")
    except Exception as e:
        logger.warning(f"Failed to load history from Redis: {e}")
        price_history = []


def refresh_history_from_redis(now: datetime, cfg: dict) -> None:
    """Re-read history dari Redis supaya tetap sync dengan Bot A.

    Menerima cfg (snapshot settings) agar tidak ada race condition
    saat membaca redis_refresh_minutes.
    """
    global last_redis_refresh
    interval = cfg["redis_refresh_minutes"]
    if interval <= 0:
        return
    if last_redis_refresh is not None:
        elapsed = (now - last_redis_refresh).total_seconds() / 60
        if elapsed < interval:
            return
    load_history()
    prune_history(now, cfg)
    last_redis_refresh = now
    logger.debug(f"Redis refreshed. {len(price_history)} points after prune")


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
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     message,
                "parse_mode":               "Markdown",
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
        url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
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
        message       = update.get("message", {})
        text          = message.get("text", "")
        chat_id       = str(message.get("chat", {}).get("id", ""))
        user_id       = str(message.get("from", {}).get("id", ""))
        is_authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not is_authorized:
            continue
        if not text.startswith("/"):
            continue
        reply_chat = chat_id
        parts      = text.split()
        command    = parts[0].lower().split("@")[0]
        args       = parts[1:] if len(parts) > 1 else []
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
        elif command == "/redis":
            handle_redis_command(reply_chat)
        elif command == "/lookback":
            handle_lookback_command(args, reply_chat)
        elif command == "/heartbeat":
            handle_heartbeat_command(args, reply_chat)
        elif command == "/peak":
            handle_peak_command(args, reply_chat)
        elif command == "/sltp":
            handle_sltp_command(args, reply_chat)
        elif command == "/start":
            handle_help_command(reply_chat)


def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        response = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id":                  chat_id,
                "text":                     message,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send reply: {e}")
        return False


# =============================================================================
# Command Handlers
# =============================================================================

def handle_settings_command(reply_chat: str) -> None:
    # FIX: snapshot settings atomic sebelum format string
    with settings_lock:
        cfg = dict(settings)

    hb     = cfg["heartbeat_minutes"]
    hb_str = f"{hb} menit" if hb > 0 else "Off"
    rr     = cfg["redis_refresh_minutes"]
    rr_str = f"{rr} menit" if rr > 0 else "Off"
    message = (
        "⚙️ *Ini semua settingan yang Aku jaga buat Sensei~*\n"
        "\n"
        f"📊 Scan Interval: {cfg['scan_interval']}s ({cfg['scan_interval'] // 60} menit)\n"
        f"🕐 Lookback: {cfg['lookback_hours']}h\n"
        f"💓 Heartbeat: {hb_str}\n"
        f"🔄 Redis Refresh: {rr_str}\n"
        f"📈 Entry Threshold: ±{cfg['entry_threshold']}%\n"
        f"📉 Exit Threshold: ±{cfg['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{cfg['invalidation_threshold']}%\n"
        f"🎯 Peak Reversal: {cfg['peak_reversal']}%\n"
        f"✅ TP: saat gap mencapai ±{cfg['exit_threshold']}% _(max konvergen)_\n"
        f"🛑 Trailing SL: {cfg['sl_pct']}% dari gap terbaik\n"
        f"⏳ TSL Grace: {cfg['tsl_grace_pct']}% _(TSL aktif setelah gap improve segini)_\n"
        "\n"
        "*Command yang tersedia:*\n"
        "`/interval`, `/lookback`, `/heartbeat`, `/threshold`, `/peak`, `/sltp`\n"
        "`/help` — Aku jelaskan semuanya buat Sensei~ ( ♡ 。-(｡･ω･) ♡)"
    )
    send_reply(message, reply_chat)


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            "Sensei~ angkanya mana? ( ･ω･)\n"
            "Contoh: `/interval 60`",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 60:
            send_reply(
                "Itu terlalu cepat, Sensei~ Minimal 60 detik ya. (｡•́︿•̀｡)",
                reply_chat
            )
            return
        if new_interval > 3600:
            send_reply(
                "Terlalu lama, Sensei~ Maksimal 3600 detik saja ya. ( ･ω･)",
                reply_chat
            )
            return
        # FIX: wrap write dalam lock
        with settings_lock:
            settings["scan_interval"] = new_interval
        send_reply(
            f"Siap, Sensei~ Aku akan scan setiap *{new_interval} detik* "
            f"({new_interval // 60} menit). ( ♡ 。-(｡･ω･) ♡)",
            reply_chat
        )
        logger.info(f"Scan interval changed to {new_interval}s")
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "Perintahnya kurang lengkap, Sensei~ ( ･ω･)\n"
            "`/threshold entry <nilai>`\n"
            "`/threshold exit <nilai>`\n"
            "`/threshold invalid <nilai>`",
            reply_chat
        )
        return
    try:
        threshold_type = args[0].lower()
        value          = float(args[1])
        if value <= 0 or value > 20:
            send_reply("Harus antara 0 sampai 20, Sensei~ ( ･ω･)", reply_chat)
            return
        # FIX: wrap write dalam lock
        with settings_lock:
            if threshold_type == "entry":
                settings["entry_threshold"] = value
            elif threshold_type == "exit":
                settings["exit_threshold"] = value
            elif threshold_type in ("invalid", "invalidation"):
                settings["invalidation_threshold"] = value
            else:
                send_reply(
                    "Gunakan `entry`, `exit`, atau `invalid` ya, Sensei~ ( ･ω･)",
                    reply_chat
                )
                return

        if threshold_type == "entry":
            send_reply(
                f"Entry threshold sekarang *±{value}%*~ Siap, Sensei! ( ♡ 。-(｡･ω･) ♡)",
                reply_chat
            )
        elif threshold_type == "exit":
            send_reply(
                f"Exit threshold sekarang *±{value}%*.\n"
                f"_TP otomatis ikut berubah ke level ini ya, Sensei~ ✨_",
                reply_chat
            )
        elif threshold_type in ("invalid", "invalidation"):
            send_reply(
                f"Invalidation sekarang *±{value}%*. Siap, Sensei~ ( ･ω･)",
                reply_chat
            )
        logger.info(f"Threshold {threshold_type} changed to {value}")
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    if not args:
        with settings_lock:
            current_val = settings["peak_reversal"]
        send_reply(
            f"🎯 Peak reversal sekarang *{current_val}%*~\n\n"
            "Usage: `/peak <nilai>`\n"
            "Contoh: `/peak 0.3`",
            reply_chat
        )
        return
    try:
        value = float(args[0])
        if value <= 0 or value > 2.0:
            send_reply("Harus antara 0 sampai 2.0, Sensei~ ( ･ω･)", reply_chat)
            return
        # FIX: wrap write dalam lock
        with settings_lock:
            settings["peak_reversal"] = value
        send_reply(
            f"Konfirmasi entry ketika gap berbalik *{value}%* dari puncaknya~\n"
            f"Siap, Sensei! ( ♡ 。-(｡･ω･) ♡)",
            reply_chat
        )
        logger.info(f"Peak reversal changed to {value}")
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_sltp_command(args: list, reply_chat: str) -> None:
    """
    /sltp              → tampilkan info TSL aktif
    /sltp sl <val>     → ubah trailing SL distance
    /sltp grace <val>  → ubah TSL grace period
    /sltp tp <val>     → redirect ke /threshold exit
    """
    if not args:
        with settings_lock:
            cfg = dict(settings)

        trailing_sl_now = ""
        if current_mode == Mode.TRACK and trailing_gap_best is not None and active_strategy is not None:
            if active_strategy == Strategy.S1:
                tsl = trailing_gap_best + cfg["sl_pct"]
            else:
                tsl = trailing_gap_best - cfg["sl_pct"]
            grace_status = "✅ Aktif" if tsl_activated else f"⏳ Menunggu {cfg['tsl_grace_pct']}% improvement"
            trailing_sl_now = (
                f"\n*Trailing SL sekarang:* `{tsl:+.2f}%` "
                f"(best gap: `{trailing_gap_best:+.2f}%`)"
                f"\n*TSL Status:* {grace_status}"
            )
        entry_info = (
            f"\n*Entry gap:* `{entry_gap_value:+.2f}%`"
            if entry_gap_value is not None else ""
        )
        send_reply(
            f"🎯 *SL/TP — Aku yang jaga buat Sensei~* ( ♡ 。-(｡･ω･) ♡)\n"
            f"\n"
            f"✅ TP: saat gap mencapai *±{cfg['exit_threshold']}%* "
            f"_(max konvergen = exit threshold)_\n"
            f"🛑 Trailing SL distance: *{cfg['sl_pct']}%* dari gap terbaik\n"
            f"⏳ TSL Grace: *{cfg['tsl_grace_pct']}%* "
            f"_(TSL aktif setelah gap improve segini dari entry)_\n"
            f"{entry_info}"
            f"{trailing_sl_now}\n"
            f"\n"
            f"*Cara kerja Trailing SL:*\n"
            f"S1 → TSL ikut turun kalau gap konvergen, jarak tetap {cfg['sl_pct']}%\n"
            f"S2 → TSL ikut naik kalau gap konvergen, jarak tetap {cfg['sl_pct']}%\n"
            f"\n"
            f"_TP dikunci ke exit threshold — tidak bisa diubah manual._\n"
            f"Usage: `/sltp sl <nilai>` — ubah trailing SL distance %\n"
            f"Usage: `/sltp grace <nilai>` — ubah TSL grace period %",
            reply_chat,
        )
        return

    if len(args) < 2:
        send_reply(
            "Gunakan `/sltp sl <nilai>` atau `/sltp grace <nilai>` ya, Sensei~\n"
            "TP sudah otomatis di exit threshold~ ( ･ω･)",
            reply_chat,
        )
        return

    try:
        key   = args[0].lower()
        value = float(args[1])
        if value <= 0 or value > 10:
            send_reply("Nilainya harus antara 0 sampai 10, Sensei~ ( ･ω･)", reply_chat)
            return
        if key == "sl":
            with settings_lock:
                settings["sl_pct"] = value
            send_reply(
                f"Trailing SL distance sekarang *{value}%*~\n"
                f"Aku ikutin terus gap terbaiknya ya, Sensei~ ( ♡ 。-(｡･ω･) ♡)",
                reply_chat,
            )
        elif key == "grace":
            # FIX: grace period — mencegah TSL kena saat entry wiggle biasa
            with settings_lock:
                settings["tsl_grace_pct"] = value
            send_reply(
                f"TSL Grace sekarang *{value}%*~\n"
                f"TSL akan aktif setelah gap improve *{value}%* dari entry~\n"
                f"_Makin besar nilainya, makin aman dari whipsaw saat entry~_",
                reply_chat,
            )
        elif key == "tp":
            with settings_lock:
                exit_thresh = settings["exit_threshold"]
            send_reply(
                f"TP sudah dikunci ke exit threshold *±{exit_thresh}%*~\n"
                f"Kalau mau ubah TP, gunakan "
                f"`/threshold exit <nilai>` ya, Sensei~ ( ･ω･)",
                reply_chat,
            )
        else:
            send_reply("Gunakan `sl` atau `grace` ya, Sensei~ ( ･ω･)", reply_chat)
        logger.info(f"SLTP {key} changed to {value}")
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    if not args:
        with settings_lock:
            current_val = settings["lookback_hours"]
        send_reply(
            f"📊 Lookback sekarang *{current_val}h*~\n\n"
            "Usage: `/lookback <jam>`",
            reply_chat
        )
        return
    try:
        new_lookback = int(args[0])
        if new_lookback < 1 or new_lookback > 24:
            send_reply("Harus antara 1 sampai 24 jam, Sensei~ ( ･ω･)", reply_chat)
            return
        with settings_lock:
            old_lookback                = settings["lookback_hours"]
            settings["lookback_hours"]  = new_lookback
        prune_history(datetime.now(timezone.utc), get_settings())
        send_reply(
            f"Lookback sudah diubah dari *{old_lookback}h* jadi *{new_lookback}h*~\n\n"
            f"⚠️ History di-prune sesuai lookback baru.\n"
            f"Data dari Bot A akan Aku ambil saat refresh berikutnya, Sensei~ ( ♡ 。-(｡･ω･) ♡)",
            reply_chat
        )
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        with settings_lock:
            current_val = settings["heartbeat_minutes"]
        send_reply(
            f"💓 Aku lapor setiap *{current_val} menit*~\n\n"
            "Usage: `/heartbeat <menit>` atau `/heartbeat 0` untuk matikan",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 0 or new_interval > 120:
            send_reply("Harus antara 0 sampai 120 menit, Sensei~ ( ･ω･)", reply_chat)
            return
        with settings_lock:
            settings["heartbeat_minutes"] = new_interval
        if new_interval == 0:
            send_reply(
                "Baik, Aku tidak akan kirim laporan rutin lagi~\n"
                "Tapi Aku tetap di sini memantau semuanya buat Sensei. ( ♡ 。-(｡･ω･) ♡)",
                reply_chat
            )
        else:
            send_reply(
                f"Aku akan lapor setiap *{new_interval} menit* ya, Sensei~ ( ･ω･)",
                reply_chat
            )
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. ( ･ω･)", reply_chat)


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply(
            "⚠️ Redis belum dikonfigurasi, Sensei.\n"
            "Pastikan `UPSTASH_REDIS_REST_URL` dan `UPSTASH_REDIS_REST_TOKEN` "
            "sudah diisi~ ( ･ω･)",
            reply_chat
        )
        return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply(
            "❌ *Tidak ada data di Redis~*\n\n"
            "Bot A belum simpan apa-apa. "
            "Tunggu Bot A kirim data dulu ya, Sensei~ ( ･ω･)",
            reply_chat
        )
        return
    try:
        data = json.loads(result["result"])
        if not data:
            send_reply("❌ Redis ada tapi isinya kosong~ ( ･ω･)", reply_chat)
            return
        first_ts     = data[0]["timestamp"]
        last_ts      = data[-1]["timestamp"]
        with settings_lock:
            scan_interval = settings["scan_interval"]
            lookback      = settings["lookback_hours"]
            rr            = settings["redis_refresh_minutes"]
        hours_stored = len(data) * scan_interval / 3600
        status       = (
            "✅ Siap kirim sinyal~"
            if hours_stored >= lookback
            else f"⏳ {hours_stored:.1f}h / {lookback}h"
        )
        last_refresh_str = (
            last_redis_refresh.strftime("%H:%M:%S UTC")
            if last_redis_refresh else "Belum pernah~"
        )
        send_reply(
            f"⚡ *Status Redis (Read-Only)*\n"
            f"\n"
            f"┌─────────────────────\n"
            f"│ Total data points: *{len(data)}*\n"
            f"│ History tersimpan: *{hours_stored:.1f}h*\n"
            f"│ Lookback target: *{lookback}h*\n"
            f"│ Status: {status}\n"
            f"│ Refresh setiap: *{rr} menit*\n"
            f"│ Refresh terakhir: `{last_refresh_str}`\n"
            f"└─────────────────────\n"
            f"\n"
            f"Data pertama: `{first_ts}`\n"
            f"Data terakhir: `{last_ts}`\n"
            f"\n"
            f"_Bot B hanya baca, Bot A yang nulis~ ⚡_",
            reply_chat
        )
    except Exception as e:
        send_reply(
            f"⚠️ Data ada tapi Aku gagal baca: `{e}` ( ･ω･)",
            reply_chat
        )


def handle_help_command(reply_chat: str) -> None:
    message = (
        "Ini semua yang bisa Aku lakukan buat Sensei~\n"
        "( ♡ 。-(｡･ω･) ♡)\n"
        "\n"
        "*Setting:*\n"
        "`/settings` - lihat semua settingan\n"
        "`/interval <detik>` - atur seberapa sering Aku scan (60-3600)\n"
        "`/lookback <jam>` - atur periode lookback (1-24)\n"
        "`/heartbeat <menit>` - atur laporan rutin (0=off)\n"
        "`/threshold entry <val>` - threshold entry %\n"
        "`/threshold exit <val>` - threshold exit % _(sekaligus jadi TP target)_\n"
        "`/threshold invalid <val>` - threshold invalidation %\n"
        "`/peak <val>` - % reversal dari puncak untuk konfirmasi entry\n"
        "`/sltp` - lihat info TP & Trailing SL aktif\n"
        "`/sltp sl <val>` - ubah trailing SL distance %\n"
        "`/sltp grace <val>` - ubah TSL grace period % _(min improvement sebelum TSL aktif)_\n"
        "\n"
        "*Info:*\n"
        "`/status` - lihat kondisi sekarang\n"
        "`/redis` - cek data history dari Bot A di Redis\n"
        "`/help` - tampilkan pesan ini lagi\n"
        "\n"
        "💡 _TP dikunci ke exit threshold — ubah via `/threshold exit`_\n"
        "💡 _TSL grace mencegah stop out dari wiggle kecil setelah entry_\n"
        "\n"
        "Aku selalu di sini buat Sensei~ Jangan ragu minta bantuan ya. ( ･ω･)"
    )
    send_reply(message, reply_chat)


def handle_status_command(reply_chat: str) -> None:
    cfg           = get_settings()
    hours_of_data = len(price_history) * cfg["scan_interval"] / 3600
    lookback      = cfg["lookback_hours"]
    ready         = (
        f"✅ Siap~ ({hours_of_data:.1f}h)"
        if hours_of_data >= lookback
        else f"⏳ {hours_of_data:.1f}h / {lookback}h"
    )
    peak_line   = (
        f"Peak Gap: {peak_gap:+.2f}%\n"
        if (current_mode == Mode.PEAK_WATCH and peak_gap is not None)
        else ""
    )
    track_lines = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        exit_thresh = cfg["exit_threshold"]
        sl_pct      = cfg["sl_pct"]
        if active_strategy == Strategy.S1:
            tp_level  = exit_thresh
            tsl_level = trailing_gap_best + sl_pct
        else:
            tp_level  = -exit_thresh
            tsl_level = trailing_gap_best - sl_pct
        eth_mid, eth_low, eth_high = calc_tp_target_price(active_strategy, cfg)
        eth_str = (
            f"${eth_mid:,.2f} (${eth_low:,.2f}–${eth_high:,.2f})"
            if eth_mid else "N/A"
        )
        tsl_status = "✅ Aktif" if tsl_activated else f"⏳ Grace {cfg['tsl_grace_pct']}%"
        track_lines   = (
            f"Entry Gap:    {entry_gap_value:+.2f}%\n"
            f"Best Gap:     {trailing_gap_best:+.2f}%\n"
            f"TP Gap:       {tp_level:+.2f}%\n"
            f"ETH TP price: {eth_str}\n"
            f"Trail SL:     {tsl_level:+.2f}% [{tsl_status}]\n"
        )
    last_refresh_str = (
        last_redis_refresh.strftime("%H:%M:%S UTC")
        if last_redis_refresh else "Belum~"
    )
    message = (
        "📊 *Status sekarang~*\n"
        "\n"
        f"Mode: {current_mode.value}\n"
        f"Strategi: {active_strategy.value if active_strategy else 'Belum ada~'}\n"
        f"{peak_line}"
        f"{track_lines}"
        f"Lookback: {lookback}h\n"
        f"History: {ready}\n"
        f"Data Points: {len(price_history)}\n"
        f"Redis refresh: {last_refresh_str} 🔒\n"
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
# Lookback Label
# =============================================================================
def get_lookback_label(cfg: dict) -> str:
    return f"{cfg['lookback_hours']}h"


# =============================================================================
# Target Price Calculation
# =============================================================================
def calc_tp_target_price(
    strategy: Strategy,
    cfg: dict,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Estimasi target harga ETH saat TP tercapai, dengan range berdasarkan
    kemungkinan pergerakan BTC.

    Asumsi base: BTC statis dari titik entry.
    Range: BTC bergerak ±TP_BTC_MOVE_RANGE_PCT% dari harga entry.

    S1 (gap konvergen ke +exit_thresh dari atas):
        Kita butuh eth_ret mendekati btc_ret (dari atas).
        target_eth_ret_base = btc_ret_entry + exit_thresh
        Jika BTC naik X%, ETH harus naik lebih sedikit (atau target ETH lebih tinggi).
        Jika BTC turun X%, target ETH lebih rendah.

    S2 (gap konvergen ke -exit_thresh dari bawah):
        Kita butuh eth_ret mendekati btc_ret (dari bawah).
        target_eth_ret_base = btc_ret_entry - exit_thresh
        Jika BTC naik X%, target ETH lebih tinggi.
        Jika BTC turun X%, target ETH lebih rendah.

    Returns: (eth_mid, eth_low, eth_high) — semua dalam USD, atau (None, None, None)
    """
    if None in (entry_btc_lb, entry_eth_lb, entry_btc_price, entry_eth_price):
        return None, None, None

    exit_thresh   = cfg["exit_threshold"]
    btc_ret_entry = float(
        (entry_btc_price - entry_btc_lb) / entry_btc_lb * Decimal("100")
    )

    def _eth_target_for_btc_move(btc_extra_pct: float) -> float:
        """Hitung target ETH jika BTC bergerak btc_extra_pct% dari entry."""
        # BTC return baru dari lookback jika BTC bergerak btc_extra_pct
        new_btc_ret = btc_ret_entry + btc_extra_pct
        if strategy == Strategy.S1:
            # Gap = eth_ret - btc_ret = exit_thresh → eth_ret = btc_ret + exit_thresh
            target_eth_ret = new_btc_ret + exit_thresh
        else:
            # Gap = eth_ret - btc_ret = -exit_thresh → eth_ret = btc_ret - exit_thresh
            target_eth_ret = new_btc_ret - exit_thresh
        return float(entry_eth_lb) * (1 + target_eth_ret / 100)

    eth_mid  = _eth_target_for_btc_move(0)
    eth_low  = _eth_target_for_btc_move(-TP_BTC_MOVE_RANGE_PCT)
    eth_high = _eth_target_for_btc_move(+TP_BTC_MOVE_RANGE_PCT)

    # Normalkan urutan low/high agar konsisten
    if eth_low > eth_high:
        eth_low, eth_high = eth_high, eth_low

    return eth_mid, eth_low, eth_high


# =============================================================================
# Message Building
# =============================================================================

def build_peak_watch_message(strategy: Strategy, gap: Decimal, cfg: dict) -> str:
    lb = get_lookback_label(cfg)
    if strategy == Strategy.S1:
        direction = "Long BTC / Short ETH"
        reason    = f"ETH pumping lebih kencang dari BTC ({lb})"
    else:
        direction = "Long ETH / Short BTC"
        reason    = f"ETH dumping lebih dalam dari BTC ({lb})"
    return (
        f"………\n"
        f"Sensei, Aku melihat sesuatu yang menarik~\n"
        f"\n"
        f"_{reason}_\n"
        f"Rencananya *{direction}*~\n"
        f"\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Tapi Aku tidak akan gegabah, Sensei.\n"
        f"Biarkan Aku memantau puncaknya dulu~ Sinyal sudah siap.\n"
        f"Aku akan kabari saat waktunya tepat. ( ･ω･)"
    )


def build_entry_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
    peak:     float,
    cfg:      dict,
) -> str:
    lb          = get_lookback_label(cfg)
    gap_float   = float(gap)
    sl_pct      = cfg["sl_pct"]
    exit_thresh = cfg["exit_threshold"]
    grace_pct   = cfg["tsl_grace_pct"]

    if strategy == Strategy.S1:
        direction   = "📈 Long BTC / Short ETH"
        reason      = f"ETH pumped more than BTC ({lb})"
        tp_gap      = exit_thresh
        tsl_initial = gap_float + sl_pct
    else:
        direction   = "📈 Long ETH / Short BTC"
        reason      = f"ETH dumped more than BTC ({lb})"
        tp_gap      = -exit_thresh
        tsl_initial = gap_float - sl_pct

    eth_mid, eth_low, eth_high = calc_tp_target_price(strategy, cfg)
    if eth_mid:
        eth_target_str = (
            f"~${eth_mid:,.2f}\n"
            f"│          _(range: ${eth_low:,.2f}–${eth_high:,.2f})_\n"
            f"│          _*BTC ±{TP_BTC_MOVE_RANGE_PCT:.0f}% dari entry_"
        )
    else:
        eth_target_str = "N/A"
    btc_ref_str = f"${float(entry_btc_price):,.2f}" if entry_btc_price else "N/A"

    return (
        f"Sensei!!! Ini saatnya!!! ⚡\n"
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
        f"*Target TP (max konvergen):*\n"
        f"┌─────────────────────\n"
        f"│ TP Gap:   {tp_gap:+.2f}% _(exit threshold)_\n"
        f"│ ETH TP:   {eth_target_str}\n"
        f"│ BTC ref:  {btc_ref_str}\n"
        f"│ Trail SL: {tsl_initial:+.2f}% _(aktif setelah >{grace_pct}% improve)_\n"
        f"└─────────────────────\n"
        f"\n"
        f"Gap sudah berbalik {cfg['peak_reversal']}% dari puncaknya~\n"
        f"Aku sudah menunggu momen ini buat Sensei. ⚡"
    )


def build_exit_message(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal, cfg: dict) -> str:
    lb = get_lookback_label(cfg)
    return (
        f"Sensei!!! Gap sudah konvergen!!! ✅\n"
        f"✅ *EXIT SIGNAL*\n"
        f"\n"
        f"Saatnya close posisi, Sensei!\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Senang bisa membantu, Sensei~\n"
        f"Aku lanjut pantau lagi dari dekat ya~ ⚡🔍"
    )


def build_invalidation_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
    cfg:      dict,
) -> str:
    lb = get_lookback_label(cfg)
    return (
        f"………\n"
        f"⚠️ *INVALIDATION: {strategy.value}*\n"
        f"\n"
        f"Sensei, maaf ya... Aku sudah berusaha, "
        f"tapi gapnya malah melebar. Bukan salah Sensei~ ( ♡ 。-(｡･ω･) ♡)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu ya, Sensei. Aku scan ulang dari awal~ ⚡\n"
        f"Lain kali Aku pasti lebih tepat. ( ･ω･)"
    )


def build_peak_cancelled_message(strategy: Strategy, gap: Decimal) -> str:
    return (
        f"………\n"
        f"❌ *Peak Watch Dibatalkan: {strategy.value}*\n"
        f"\n"
        f"Gapnya mundur sendiri sebelum Aku sempat konfirmasi, Sensei.\n"
        f"Pasar nakal sekali ya~ ( ･ω･)\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Tidak apa-apa, Sensei. Aku tetap di sini pantau dari dekat~ ( ♡ 。-(｡･ω･) ♡)"
    )


def build_tp_message(
    btc_ret:      Decimal,
    eth_ret:      Decimal,
    gap:          Decimal,
    entry_gap:    float,
    tp_gap_level: float,
    eth_mid:      Optional[float],
    eth_low:      Optional[float],
    eth_high:     Optional[float],
    cfg:          dict,
) -> str:
    lb = get_lookback_label(cfg)
    if eth_mid:
        actual_eth = float(entry_eth_price) if entry_eth_price else None
        eth_tp_str = f"~${eth_mid:,.2f} _(est. entry, range ${eth_low:,.2f}–${eth_high:,.2f})_"
        if actual_eth:
            eth_tp_str += f"\n│ ETH now:  ${actual_eth:,.2f} _(harga aktual)_"
    else:
        eth_tp_str = "N/A"
    return (
        f"Sensei!!! TP kena!!! ✨\n"
        f"🎯 *TAKE PROFIT*\n"
        f"\n"
        f"Gap sudah konvergen maksimal sesuai target~\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:     {format_value(btc_ret)}%\n"
        f"│ ETH:     {format_value(eth_ret)}%\n"
        f"│ Gap:     {format_value(gap)}%\n"
        f"│ Entry:   {entry_gap:+.2f}%\n"
        f"│ TP hit:  {tp_gap_level:+.2f}% _(exit threshold)_\n"
        f"│ ETH TP:  {eth_tp_str}\n"
        f"└─────────────────────\n"
        f"\n"
        f"Misi sukses buat Sensei! ⚡ ( ♡ 。-(｡･ω･) ♡)"
    )


def build_trailing_sl_message(
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    entry_gap: float,
    best_gap:  float,
    sl_level:  float,
    cfg:       dict,
) -> str:
    lb            = get_lookback_label(cfg)
    profit_locked = abs(entry_gap - best_gap)
    return (
        f"………\n"
        f"⛔ *TRAILING STOP LOSS*\n"
        f"\n"
        f"Aku sudah jaga posisi Sensei sampai di sini.\n"
        f"Trailing SL kena, profit sudah aman~ ( ♡ 。-(｡･ω･) ♡)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:      {format_value(btc_ret)}%\n"
        f"│ ETH:      {format_value(eth_ret)}%\n"
        f"│ Gap:      {format_value(gap)}%\n"
        f"│ Entry:    {entry_gap:+.2f}%\n"
        f"│ Best gap: {best_gap:+.2f}%\n"
        f"│ TSL hit:  {sl_level:+.2f}%\n"
        f"│ Terkunci: ~{profit_locked:.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu ya, Sensei. Aku scan ulang~ ⚡\n"
        f"Lain kali Aku pasti lebih tepat. ( ･ω･)"
    )


def build_heartbeat_message(cfg: dict) -> str:
    lb          = get_lookback_label(cfg)
    btc_ret_str = (
        f" ({format_value(scan_stats['last_btc_ret'])}%)"
        if scan_stats["last_btc_ret"] is not None else ""
    )
    eth_ret_str = (
        f" ({format_value(scan_stats['last_eth_ret'])}%)"
        if scan_stats["last_eth_ret"] is not None else ""
    )
    btc_str = (
        f"${float(scan_stats['last_btc_price']):,.2f}{btc_ret_str}"
        if scan_stats["last_btc_price"] else "N/A"
    )
    eth_str = (
        f"${float(scan_stats['last_eth_price']):,.2f}{eth_ret_str}"
        if scan_stats["last_eth_price"] else "N/A"
    )
    gap_str       = (
        f"{format_value(scan_stats['last_gap'])}%"
        if scan_stats["last_gap"] is not None else "N/A"
    )
    hours_of_data = len(price_history) * cfg["scan_interval"] / 3600
    lookback      = cfg["lookback_hours"]
    data_status   = (
        f"✅ Siap~ ({hours_of_data:.1f}h)"
        if hours_of_data >= lookback
        else f"⏳ {hours_of_data:.1f}h / {lookback}h"
    )
    peak_line   = (
        f"│ Peak: {peak_gap:+.2f}%\n"
        if (current_mode == Mode.PEAK_WATCH and peak_gap is not None)
        else ""
    )
    track_lines = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        exit_thresh = cfg["exit_threshold"]
        sl_pct      = cfg["sl_pct"]
        if active_strategy == Strategy.S1:
            tp_level  = exit_thresh
            tsl_level = trailing_gap_best + sl_pct
        else:
            tp_level  = -exit_thresh
            tsl_level = trailing_gap_best - sl_pct
        eth_mid, eth_low, eth_high = calc_tp_target_price(active_strategy, cfg)
        eth_str_tp = (
            f"~${eth_mid:,.2f} (${eth_low:,.2f}–${eth_high:,.2f})"
            if eth_mid else "N/A"
        )
        tsl_status = "✅" if tsl_activated else f"⏳grace"
        track_lines   = (
            f"│ Entry:    {entry_gap_value:+.2f}%\n"
            f"│ TP gap:   {tp_level:+.2f}% (ETH: {eth_str_tp})\n"
            f"│ Trail SL: {tsl_level:+.2f}% [{tsl_status}] (best: {trailing_gap_best:+.2f}%)\n"
        )
    last_refresh_str = (
        last_redis_refresh.strftime("%H:%M:%S UTC")
        if last_redis_refresh else "Belum~"
    )
    return (
        f"💓 *Laporan Rutin — Aku masih di sini, Sensei~*\n"
        f"\n"
        f"Sensei, Aku selalu di sini memantau semuanya. ( ♡ 。-(｡･ω･) ♡)\n"
        f"\n"
        f"*Mode:* {current_mode.value}\n"
        f"*Strategi:* {active_strategy.value if active_strategy else 'Belum ada~'}\n"
        f"\n"
        f"*{cfg['heartbeat_minutes']} menit terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ Scan: {scan_stats['count']}x | Sinyal: {scan_stats['signals_sent']}x\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Harga sekarang ({lb}):*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"│ Gap: {gap_str}\n"
        f"{peak_line}"
        f"{track_lines}"
        f"└─────────────────────\n"
        f"\n"
        f"*Data:* {data_status}\n"
        f"*Redis refresh:* {last_refresh_str} 🔒\n"
        f"\n"
        f"_Aku lapor lagi {cfg['heartbeat_minutes']} menit lagi ya, Sensei~\n"
        f"Jangan kangen terlalu dalam. ( ♡ 。-(｡･ω･) ♡)_"
    )


def send_heartbeat(cfg: dict) -> bool:
    global scan_stats
    success = send_alert(build_heartbeat_message(cfg))
    scan_stats["count"]        = 0
    scan_stats["signals_sent"] = 0
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
            tz_start = next(
                (i for i, c in enumerate(frac_and_tz) if c in ("+", "-")), -1
            )
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

    btc_data = next(
        (l for l in listings if l.get("ticker", "").upper() == "BTC"), None
    )
    eth_data = next(
        (l for l in listings if l.get("ticker", "").upper() == "ETH"), None
    )

    if not btc_data or not eth_data:
        logger.warning("Missing BTC or ETH data")
        return None

    try:
        btc_price = Decimal(btc_data["mark_price"])
        eth_price = Decimal(eth_data["mark_price"])
    except (KeyError, InvalidOperation) as e:
        logger.error(f"Invalid price: {e}")
        return None

    btc_updated_at = parse_iso_timestamp(
        btc_data.get("quotes", {}).get("updated_at", "")
    )
    eth_updated_at = parse_iso_timestamp(
        eth_data.get("quotes", {}).get("updated_at", "")
    )

    if not btc_updated_at or not eth_updated_at:
        return None

    return PriceData(btc_price, eth_price, btc_updated_at, eth_updated_at)


# =============================================================================
# Price History Management
# =============================================================================
def prune_history(now: datetime, cfg: dict) -> None:
    global price_history
    cutoff = now - timedelta(
        hours=cfg["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES
    )
    price_history = [p for p in price_history if p.timestamp >= cutoff]


def get_lookback_price(now: datetime, cfg: dict) -> Optional[PricePoint]:
    target_time           = now - timedelta(hours=cfg["lookback_hours"])
    best_point, best_diff = None, timedelta(minutes=30)
    for point in price_history:
        diff = abs(point.timestamp - target_time)
        if diff < best_diff:
            best_diff, best_point = diff, point
    return best_point


# =============================================================================
# Return Calculation
# =============================================================================
def compute_returns(
    btc_now, eth_now, btc_prev, eth_prev
) -> Tuple[Decimal, Decimal, Decimal]:
    btc_change = (btc_now - btc_prev) / btc_prev * Decimal("100")
    eth_change = (eth_now - eth_prev) / eth_prev * Decimal("100")
    return btc_change, eth_change, eth_change - btc_change


# =============================================================================
# Freshness Check
# =============================================================================
def is_data_fresh(now, btc_updated, eth_updated) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return (
        (now - btc_updated) <= threshold
        and (now - eth_updated) <= threshold
    )


# =============================================================================
# State Reset Helper
# =============================================================================
def reset_to_scan() -> None:
    """Reset semua global state ke kondisi SCAN."""
    global current_mode, active_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
    global tsl_activated  # FIX: reset grace flag juga
    current_mode      = Mode.SCAN
    active_strategy   = None
    entry_gap_value   = None
    trailing_gap_best = None
    entry_btc_price   = None
    entry_eth_price   = None
    entry_btc_lb      = None
    entry_eth_lb      = None
    tsl_activated     = False


# =============================================================================
# TP + Trailing SL Checker
# =============================================================================
def check_sltp(
    gap_float: float,
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    cfg:       dict,
) -> bool:
    """
    Cek TP dan Trailing SL saat Mode TRACK.

    TP = exit_threshold (gap konvergen maksimal):
        S1: gap <= +exit_threshold
        S2: gap >= -exit_threshold
    TP selalu aktif sejak entry.

    Trailing SL (FIX — grace period):
        TSL baru aktif setelah gap improve >= tsl_grace_pct dari entry.
        Tujuan: mencegah stop out dari wiggle kecil sesaat setelah entry.

        S1: trailing_gap_best = min gap sejak entry
            TSL aktif jika (entry_gap - trailing_gap_best) >= tsl_grace_pct
            TSL level = trailing_gap_best + sl_pct
            Trigger jika gap >= TSL level

        S2: trailing_gap_best = max gap (makin negatif) sejak entry
            TSL aktif jika (trailing_gap_best - entry_gap) >= tsl_grace_pct
            TSL level = trailing_gap_best - sl_pct
            Trigger jika gap <= TSL level

    Return True jika TP atau TSL terpicu (posisi ditutup).
    """
    global trailing_gap_best, tsl_activated

    if entry_gap_value is None or active_strategy is None or trailing_gap_best is None:
        return False

    exit_thresh = cfg["exit_threshold"]
    sl_pct      = cfg["sl_pct"]
    grace_pct   = cfg["tsl_grace_pct"]

    if active_strategy == Strategy.S1:
        # Update trailing best (min gap — makin kecil = makin konvergen untuk S1)
        if gap_float < trailing_gap_best:
            trailing_gap_best = gap_float
            # FIX: aktifkan TSL hanya setelah gap improve cukup dari entry
            if not tsl_activated and (entry_gap_value - trailing_gap_best) >= grace_pct:
                tsl_activated = True
                logger.info(
                    f"TSL S1 activated. Entry: {entry_gap_value:.2f}%, "
                    f"Best: {trailing_gap_best:.2f}%, "
                    f"Grace: {grace_pct:.2f}%"
                )
            elif tsl_activated:
                logger.info(
                    f"TSL S1 updated. Best: {trailing_gap_best:.2f}%, "
                    f"TSL: {trailing_gap_best + sl_pct:.2f}%"
                )

        tsl_level = trailing_gap_best + sl_pct

        # Cek TP dulu (selalu aktif, tidak perlu grace)
        if gap_float <= exit_thresh:
            eth_mid, eth_low, eth_high = calc_tp_target_price(Strategy.S1, cfg)
            send_alert(build_tp_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, exit_thresh,
                eth_mid, eth_low, eth_high, cfg,
            ))
            logger.info(
                f"TP S1. Entry: {entry_gap_value:.2f}%, "
                f"Now: {gap_float:.2f}%, TP: {exit_thresh:.2f}%"
            )
            reset_to_scan()
            return True

        # Cek TSL — hanya jika sudah aktif (grace terpenuhi)
        if tsl_activated and gap_float >= tsl_level:
            send_alert(build_trailing_sl_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, trailing_gap_best, tsl_level, cfg,
            ))
            logger.info(
                f"TSL S1. Entry: {entry_gap_value:.2f}%, "
                f"Best: {trailing_gap_best:.2f}%, "
                f"TSL: {tsl_level:.2f}%, Now: {gap_float:.2f}%"
            )
            reset_to_scan()
            return True

    elif active_strategy == Strategy.S2:
        # Update trailing best (max gap — makin positif = makin konvergen untuk S2)
        if gap_float > trailing_gap_best:
            trailing_gap_best = gap_float
            # FIX: aktifkan TSL hanya setelah gap improve cukup dari entry
            if not tsl_activated and (trailing_gap_best - entry_gap_value) >= grace_pct:
                tsl_activated = True
                logger.info(
                    f"TSL S2 activated. Entry: {entry_gap_value:.2f}%, "
                    f"Best: {trailing_gap_best:.2f}%, "
                    f"Grace: {grace_pct:.2f}%"
                )
            elif tsl_activated:
                logger.info(
                    f"TSL S2 updated. Best: {trailing_gap_best:.2f}%, "
                    f"TSL: {trailing_gap_best - sl_pct:.2f}%"
                )

        tsl_level = trailing_gap_best - sl_pct

        # Cek TP dulu (selalu aktif)
        if gap_float >= -exit_thresh:
            eth_mid, eth_low, eth_high = calc_tp_target_price(Strategy.S2, cfg)
            send_alert(build_tp_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, -exit_thresh,
                eth_mid, eth_low, eth_high, cfg,
            ))
            logger.info(
                f"TP S2. Entry: {entry_gap_value:.2f}%, "
                f"Now: {gap_float:.2f}%, TP: {-exit_thresh:.2f}%"
            )
            reset_to_scan()
            return True

        # Cek TSL — hanya jika sudah aktif
        if tsl_activated and gap_float <= tsl_level:
            send_alert(build_trailing_sl_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, trailing_gap_best, tsl_level, cfg,
            ))
            logger.info(
                f"TSL S2. Entry: {entry_gap_value:.2f}%, "
                f"Best: {trailing_gap_best:.2f}%, "
                f"TSL: {tsl_level:.2f}%, Now: {gap_float:.2f}%"
            )
            reset_to_scan()
            return True

    return False


# =============================================================================
# State Machine with Peak Detection
# =============================================================================
def evaluate_and_transition(
    btc_ret: Decimal,
    eth_ret: Decimal,
    gap:     Decimal,
    btc_now: Decimal,
    eth_now: Decimal,
    btc_lb:  Decimal,
    eth_lb:  Decimal,
    cfg:     dict,
) -> None:
    global current_mode, active_strategy, peak_gap, peak_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb

    gap_float      = float(gap)
    entry_thresh   = cfg["entry_threshold"]
    exit_thresh    = cfg["exit_threshold"]
    invalid_thresh = cfg["invalidation_threshold"]
    peak_reversal  = cfg["peak_reversal"]

    # -------------------------------------------------------------------------
    if current_mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            current_mode  = Mode.PEAK_WATCH
            peak_strategy = Strategy.S1
            peak_gap      = gap_float
            send_alert(build_peak_watch_message(Strategy.S1, gap, cfg))
            logger.info(f"PEAK WATCH S1 started. Gap: {gap_float:.2f}%")

        elif gap_float <= -entry_thresh:
            current_mode  = Mode.PEAK_WATCH
            peak_strategy = Strategy.S2
            peak_gap      = gap_float
            send_alert(build_peak_watch_message(Strategy.S2, gap, cfg))
            logger.info(f"PEAK WATCH S2 started. Gap: {gap_float:.2f}%")

        else:
            logger.debug(f"SCAN: No signal. Gap: {gap_float:.2f}%")

    # -------------------------------------------------------------------------
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
                active_strategy   = Strategy.S1
                current_mode      = Mode.TRACK
                entry_gap_value   = gap_float
                trailing_gap_best = gap_float
                entry_btc_price   = btc_now
                entry_eth_price   = eth_now
                entry_btc_lb      = btc_lb
                entry_eth_lb      = eth_lb
                send_alert(
                    build_entry_message(Strategy.S1, btc_ret, eth_ret, gap, peak_gap, cfg)
                )
                logger.info(
                    f"ENTRY S1. Peak: {peak_gap:.2f}%, Entry: {gap_float:.2f}%"
                )
                peak_gap, peak_strategy = None, None

            else:
                logger.info(
                    f"PEAK WATCH S1: Gap {gap_float:.2f}% | "
                    f"Peak {peak_gap:.2f}% | Need {peak_reversal}% drop"
                )

        elif peak_strategy == Strategy.S2:
            if gap_float < peak_gap:
                peak_gap = gap_float
                logger.info(f"PEAK WATCH S2: New peak {peak_gap:.2f}%")

            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S2, gap))
                logger.info(f"PEAK WATCH S2 cancelled. Gap: {gap_float:.2f}%")
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None

            elif gap_float - peak_gap >= peak_reversal:
                active_strategy   = Strategy.S2
                current_mode      = Mode.TRACK
                entry_gap_value   = gap_float
                trailing_gap_best = gap_float
                entry_btc_price   = btc_now
                entry_eth_price   = eth_now
                entry_btc_lb      = btc_lb
                entry_eth_lb      = eth_lb
                send_alert(
                    build_entry_message(Strategy.S2, btc_ret, eth_ret, gap, peak_gap, cfg)
                )
                logger.info(
                    f"ENTRY S2. Peak: {peak_gap:.2f}%, Entry: {gap_float:.2f}%"
                )
                peak_gap, peak_strategy = None, None

            else:
                logger.info(
                    f"PEAK WATCH S2: Gap {gap_float:.2f}% | "
                    f"Peak {peak_gap:.2f}% | Need {peak_reversal}% rise"
                )

    # -------------------------------------------------------------------------
    elif current_mode == Mode.TRACK:
        # FIX: check_sltp sudah handle TP (kondisi sama dengan safety net lama).
        # Safety net exit dihapus — mencegah double-alert TP + EXIT.
        # Jika check_sltp return True, posisi sudah ditutup, langsung return.
        if check_sltp(gap_float, btc_ret, eth_ret, gap, cfg):
            return

        # Invalidation — gap melebar ke arah berlawanan
        if active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S1, btc_ret, eth_ret, gap, cfg))
            logger.info(f"INVALIDATION S1. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        if active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S2, btc_ret, eth_ret, gap, cfg))
            logger.info(f"INVALIDATION S2. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        logger.debug(
            f"TRACK {active_strategy.value if active_strategy else 'None'}: "
            f"Gap {gap_float:.2f}%"
            + (f" | TSL grace: {tsl_activated}" if not tsl_activated else "")
        )


# =============================================================================
# Startup Message
# =============================================================================
def send_startup_message(cfg: dict) -> bool:
    price_data = fetch_prices()
    if price_data:
        price_info = (
            f"\n💰 *Harga saat ini~*\n"
            f"┌─────────────────────\n"
            f"│ BTC: ${float(price_data.btc_price):,.2f}\n"
            f"│ ETH: ${float(price_data.eth_price):,.2f}\n"
            f"└─────────────────────\n"
        )
    else:
        price_info = (
            "\n⚠️ Gagal ambil harga tadi, Sensei...\n"
            "Tapi Aku akan terus coba~ ( ･ω･)\n"
        )

    lb           = get_lookback_label(cfg)
    hours_loaded = len(price_history) * cfg["scan_interval"] / 3600

    if len(price_history) > 0:
        history_info = (
            f"⚡ History dari Bot A sudah ada~ *{hours_loaded:.1f}h* data siap!\n"
            f"_Aku tidak perlu mulai dari nol lagi~ ( ♡ 。-(｡･ω･) ♡)_\n"
        )
    else:
        history_info = (
            f"⏳ Menunggu Bot A kirim data ke Redis~\n"
            f"_Sinyal akan keluar setelah {lb} data tersedia~_\n"
        )

    return send_alert(
        f"………\n"
        f"Sensei, Aku sudah siap~ ( ♡ 。-(｡･ω･) ♡)\n"
        f"{price_info}\n"
        f"📊 Scan setiap {cfg['scan_interval']}s | "
        f"Redis refresh setiap {cfg['redis_refresh_minutes']} menit\n"
        f"📈 Entry: ±{cfg['entry_threshold']}%\n"
        f"📉 Exit: ±{cfg['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{cfg['invalidation_threshold']}%\n"
        f"🎯 Peak reversal: {cfg['peak_reversal']}%\n"
        f"✅ TP: saat gap ±{cfg['exit_threshold']}% _(max konvergen)_\n"
        f"🛑 Trailing SL: {cfg['sl_pct']}% distance "
        f"_(aktif setelah >{cfg['tsl_grace_pct']}% improve)_\n"
        f"🔒 Redis: Read-Only (Bot A yang write)\n"
        f"\n"
        f"{history_info}\n"
        f"Ketik `/help` kalau butuh sesuatu.\n"
        f"Aku takkan pergi, Sensei. Selalu di sini. ⚡"
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

    # FIX: gunakan cfg snapshot di awal startup (sebelum thread command mulai)
    init_cfg = get_settings()

    logger.info("=" * 60)
    logger.info("Monk Bot B — Read-Only Redis | Max TP | Trailing SL | Grace TSL")
    logger.info(
        f"Entry: {init_cfg['entry_threshold']}% | "
        f"Exit/TP: {init_cfg['exit_threshold']}% | "
        f"Invalid: {init_cfg['invalidation_threshold']}% | "
        f"Peak: {init_cfg['peak_reversal']}% | "
        f"TSL: {init_cfg['sl_pct']}% | "
        f"Grace: {init_cfg['tsl_grace_pct']}% | "
        f"Redis refresh: {init_cfg['redis_refresh_minutes']}m"
    )
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    # Load history dari Redis (read-only), prune expired
    load_history()
    prune_history(datetime.now(timezone.utc), init_cfg)
    last_redis_refresh = datetime.now(timezone.utc)
    logger.info(
        f"History after initial load & prune: {len(price_history)} points"
    )

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message(init_cfg)

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            # FIX: snapshot settings sekali per iterasi — konsisten di seluruh evaluasi
            cfg = get_settings()

            # Heartbeat
            if should_send_heartbeat(now, cfg):
                if send_heartbeat(cfg):
                    last_heartbeat_time = now

            # Refresh history dari Redis setiap N menit
            refresh_history_from_redis(now, cfg)

            # Fetch harga terkini dari API
            price_data = fetch_prices()
            if price_data is None:
                logger.warning("Failed to fetch prices")
            else:
                scan_stats["count"]         += 1
                scan_stats["last_btc_price"] = price_data.btc_price
                scan_stats["last_eth_price"] = price_data.eth_price

                if not is_data_fresh(
                    now,
                    price_data.btc_updated_at,
                    price_data.eth_updated_at,
                ):
                    logger.warning("Data not fresh, skipping")
                else:
                    # Bot B tidak append ke price_history sendiri
                    # — semua data lookback dari Redis (Bot A)
                    price_then = get_lookback_price(now, cfg)

                    if price_then is None:
                        hours = len(price_history) * cfg["scan_interval"] / 3600
                        logger.info(
                            f"Waiting for Bot A data... "
                            f"({hours:.1f}h / {cfg['lookback_hours']}h)"
                        )
                    else:
                        # Warn jika lookback price terlalu stale
                        target_time = now - timedelta(hours=cfg["lookback_hours"])
                        staleness   = abs((price_then.timestamp - target_time).total_seconds())
                        if staleness > 600:  # > 10 menit dari target
                            logger.warning(
                                f"Lookback price stale: "
                                f"{staleness/60:.1f} min dari target lookback"
                            )

                        btc_ret, eth_ret, gap = compute_returns(
                            price_data.btc_price,
                            price_data.eth_price,
                            price_then.btc,
                            price_then.eth,
                        )
                        scan_stats["last_gap"]     = gap
                        scan_stats["last_btc_ret"] = btc_ret
                        scan_stats["last_eth_ret"] = eth_ret

                        logger.info(
                            f"Mode: {current_mode.value} | "
                            f"BTC {cfg['lookback_hours']}h: "
                            f"{format_value(btc_ret)}% | "
                            f"ETH {cfg['lookback_hours']}h: "
                            f"{format_value(eth_ret)}% | "
                            f"Gap: {format_value(gap)}%"
                        )

                        prev_mode = current_mode
                        evaluate_and_transition(
                            btc_ret, eth_ret, gap,
                            price_data.btc_price,
                            price_data.eth_price,
                            price_then.btc,
                            price_then.eth,
                            cfg,
                        )
                        if current_mode != prev_mode:
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
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN not set")
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID not set")
    main_loop()
