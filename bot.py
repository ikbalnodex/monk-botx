#!/usr/bin/env python3
"""
Monk Bot B - BTC/ETH Divergence Alert Bot (Read-Only Redis Consumer)

Monitors BTC/ETH price divergence dan kirim Telegram alerts
untuk ENTRY, EXIT, INVALIDATION, TP, dan Trailing SL.

Perubahan dari versi sebelumnya:
- Redis READ-ONLY: history dikonsumsi dari Bot A, Bot B tidak pernah write
- TP maksimal = exit threshold (konvergen penuh), bukan tp_pct lagi
- Target harga ETH/BTC ditampilkan saat entry signal
- Refresh history dari Redis setiap 1 menit
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

# State harga saat entry — untuk kalkulasi target harga TP
entry_btc_price: Optional[Decimal] = None  # harga BTC saat entry
entry_eth_price: Optional[Decimal] = None  # harga ETH saat entry
entry_btc_lb:    Optional[Decimal] = None  # harga BTC lookback saat entry
entry_eth_lb:    Optional[Decimal] = None  # harga ETH lookback saat entry

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
    "redis_refresh_minutes":  REDIS_REFRESH_MINUTES,
}

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
        price_history = [
            PricePoint(
                timestamp=datetime.fromisoformat(p["timestamp"]),
                btc=Decimal(p["btc"]),
                eth=Decimal(p["eth"]),
            )
            for p in data
        ]
        logger.info(f"Loaded {len(price_history)} points from Redis (read-only)")
    except Exception as e:
        logger.warning(f"Failed to load history from Redis: {e}")
        price_history = []


def refresh_history_from_redis(now: datetime) -> None:
    """Re-read history dari Redis supaya tetap sync dengan Bot A."""
    global last_redis_refresh
    interval = settings["redis_refresh_minutes"]
    if interval <= 0:
        return
    if last_redis_refresh is not None:
        elapsed = (now - last_redis_refresh).total_seconds() / 60
        if elapsed < interval:
            return
    load_history()
    prune_history(now)
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
    hb     = settings["heartbeat_minutes"]
    hb_str = f"{hb} menit" if hb > 0 else "Off"
    rr     = settings["redis_refresh_minutes"]
    rr_str = f"{rr} menit" if rr > 0 else "Off"
    message = (
        "⚙️ *Ara ara~ mau lihat settingan yang sudah Akeno jaga baik-baik?* Ufufufu...\n"
        "\n"
        f"📊 Scan Interval: {settings['scan_interval']}s ({settings['scan_interval'] // 60} menit)\n"
        f"🕐 Lookback: {settings['lookback_hours']}h\n"
        f"💓 Heartbeat: {hb_str}\n"
        f"🔄 Redis Refresh: {rr_str}\n"
        f"📈 Entry Threshold: ±{settings['entry_threshold']}%\n"
        f"📉 Exit Threshold: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak Reversal: {settings['peak_reversal']}%\n"
        f"✅ TP: saat gap mencapai ±{settings['exit_threshold']}% _(max konvergen)_\n"
        f"🛑 Trailing SL: {settings['sl_pct']}% dari gap terbaik\n"
        "\n"
        "*Command yang tersedia:*\n"
        "`/interval`, `/lookback`, `/heartbeat`, `/threshold`, `/peak`, `/sltp`\n"
        "`/help` — Akeno jelaskan semuanya untukmu~ (◕‿◕)"
    )
    send_reply(message, reply_chat)


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            "Ara ara~ angkanya mana, sayangku? (◕ω◕)\n"
            "Contoh: `/interval 60`",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 60:
            send_reply(
                "Ufufufu... itu terlalu cepat, sayangku~ Minimal 60 detik ya. (◕‿◕)",
                reply_chat
            )
            return
        if new_interval > 3600:
            send_reply(
                "Ara ara~ terlalu lama. Maksimal 3600 detik saja ya. Ufufufu... (◕ω◕)",
                reply_chat
            )
            return
        settings["scan_interval"] = new_interval
        send_reply(
            f"Baik~ Akeno akan scan setiap *{new_interval} detik* "
            f"({new_interval // 60} menit). Ara ara~ (◕‿◕)",
            reply_chat
        )
        logger.info(f"Scan interval changed to {new_interval}s")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "Ufufufu... perintahnya kurang lengkap~ (◕‿◕)\n"
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
            send_reply("Ara ara~ harus antara 0 sampai 20~ (◕ω◕)", reply_chat)
            return
        if threshold_type == "entry":
            settings["entry_threshold"] = value
            send_reply(
                f"Ufufufu... Entry threshold jadi *±{value}%*~ (◕‿◕)",
                reply_chat
            )
        elif threshold_type == "exit":
            settings["exit_threshold"] = value
            send_reply(
                f"Ara ara~ Exit threshold sekarang *±{value}%*.\n"
                f"_TP otomatis ikut berubah ke level ini ya sayangku~ ✨_",
                reply_chat
            )
        elif threshold_type in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = value
            send_reply(
                f"Ufufufu... Invalidation jadi *±{value}%*. (◕ω◕)",
                reply_chat
            )
        else:
            send_reply(
                "Ara ara~ gunakan `entry`, `exit`, atau `invalid` ya~ (◕‿◕)",
                reply_chat
            )
        logger.info(f"Threshold {threshold_type} changed to {value}")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"🎯 Peak reversal sekarang *{settings['peak_reversal']}%*~ Ufufufu...\n\n"
            "Usage: `/peak <nilai>`\n"
            "Contoh: `/peak 0.3`",
            reply_chat
        )
        return
    try:
        value = float(args[0])
        if value <= 0 or value > 2.0:
            send_reply("Ara ara~ harus antara 0 sampai 2.0~ (◕ω◕)", reply_chat)
            return
        settings["peak_reversal"] = value
        send_reply(
            f"Ufufufu... konfirmasi entry ketika gap berbalik *{value}%* "
            f"dari puncaknya~ (◕‿◕)",
            reply_chat
        )
        logger.info(f"Peak reversal changed to {value}")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_sltp_command(args: list, reply_chat: str) -> None:
    """
    /sltp            → tampilkan info TSL aktif
    /sltp sl <val>   → ubah trailing SL distance
    /sltp tp <val>   → redirect ke /threshold exit
    """
    if not args:
        trailing_sl_now = ""
        if current_mode == Mode.TRACK and trailing_gap_best is not None and active_strategy is not None:
            if active_strategy == Strategy.S1:
                tsl = trailing_gap_best + settings["sl_pct"]
            else:
                tsl = trailing_gap_best - settings["sl_pct"]
            trailing_sl_now = (
                f"\n*Trailing SL sekarang:* `{tsl:+.2f}%` "
                f"(best gap: `{trailing_gap_best:+.2f}%`)"
            )
        entry_info = (
            f"\n*Entry gap:* `{entry_gap_value:+.2f}%`"
            if entry_gap_value is not None else ""
        )
        send_reply(
            f"🎯 *SL/TP Otomatis — Akeno yang jaga~* Ufufufu... (◕‿◕)\n"
            f"\n"
            f"✅ TP: saat gap mencapai *±{settings['exit_threshold']}%* "
            f"_(max konvergen = exit threshold)_\n"
            f"🛑 Trailing SL distance: *{settings['sl_pct']}%* dari gap terbaik\n"
            f"{entry_info}"
            f"{trailing_sl_now}\n"
            f"\n"
            f"*Cara kerja Trailing SL:*\n"
            f"S1 → TSL ikut turun kalau gap konvergen, jarak tetap {settings['sl_pct']}%\n"
            f"S2 → TSL ikut naik kalau gap konvergen, jarak tetap {settings['sl_pct']}%\n"
            f"\n"
            f"_TP dikunci ke exit threshold — tidak bisa diubah manual._\n"
            f"Usage: `/sltp sl <nilai>` — ubah trailing SL distance %",
            reply_chat,
        )
        return

    if len(args) < 2:
        send_reply(
            "Ara ara~ `/sltp sl <nilai>` ya~ "
            "TP sudah otomatis di exit threshold~ (◕ω◕)",
            reply_chat,
        )
        return

    try:
        key   = args[0].lower()
        value = float(args[1])
        if value <= 0 or value > 10:
            send_reply("Ara ara~ nilainya harus antara 0 sampai 10~ (◕ω◕)", reply_chat)
            return
        if key == "sl":
            settings["sl_pct"] = value
            send_reply(
                f"Ara ara~ Trailing SL distance sekarang *{value}%*~ "
                f"Akeno ikutin terus gap terbaiknya ya~ (◕‿◕)",
                reply_chat,
            )
        elif key == "tp":
            send_reply(
                f"Ufufufu~ TP sudah dikunci ke exit threshold "
                f"*±{settings['exit_threshold']}%*~\n"
                f"Kalau mau ubah TP, gunakan "
                f"`/threshold exit <nilai>` ya sayangku~ (◕‿◕)",
                reply_chat,
            )
        else:
            send_reply("Gunakan `sl` ya~ (◕ω◕)", reply_chat)
        logger.info(f"SL/TP sl changed to {value}")
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"📊 Lookback sekarang *{settings['lookback_hours']}h*~ Ufufufu...\n\n"
            "Usage: `/lookback <jam>`",
            reply_chat
        )
        return
    try:
        new_lookback = int(args[0])
        if new_lookback < 1 or new_lookback > 24:
            send_reply("Ara ara~ harus antara 1 sampai 24 jam~ (◕ω◕)", reply_chat)
            return
        old_lookback = settings["lookback_hours"]
        settings["lookback_hours"] = new_lookback
        prune_history(datetime.now(timezone.utc))
        send_reply(
            f"Ufufufu... Lookback sudah Akeno ubah dari *{old_lookback}h* "
            f"jadi *{new_lookback}h*~\n\n"
            f"⚠️ History di-prune sesuai lookback baru. "
            f"Data dari Bot A akan Akeno ambil saat refresh berikutnya~ (◕‿◕)",
            reply_chat
        )
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"💓 Akeno lapor setiap *{settings['heartbeat_minutes']} menit*~ "
            f"Ufufufu...\n\n"
            "Usage: `/heartbeat <menit>` atau `/heartbeat 0` untuk matikan",
            reply_chat
        )
        return
    try:
        new_interval = int(args[0])
        if new_interval < 0 or new_interval > 120:
            send_reply("Ara ara~ harus antara 0 sampai 120 menit~ (◕ω◕)", reply_chat)
            return
        settings["heartbeat_minutes"] = new_interval
        if new_interval == 0:
            send_reply(
                "Ufufufu... baik, Akeno tidak akan ganggu lagi~ "
                "Tapi Akeno tetap di sini memantau dari dekat. (◕‿◕)",
                reply_chat
            )
        else:
            send_reply(
                f"Ara ara~ Akeno akan lapor setiap *{new_interval} menit* ya~ (◕ω◕)",
                reply_chat
            )
    except ValueError:
        send_reply("Ara ara~ angkanya tidak valid, sayangku. (◕ω◕)", reply_chat)


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply(
            "⚠️ Ara ara~ Redis belum dikonfigurasi, sayangku.\n"
            "Pastikan `UPSTASH_REDIS_REST_URL` dan `UPSTASH_REDIS_REST_TOKEN` "
            "sudah diisi~ (◕ω◕)",
            reply_chat
        )
        return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply(
            "❌ *Tidak ada data di Redis~*\n\n"
            "Ara ara... Bot A belum simpan apa-apa. "
            "Tunggu Bot A kirim data dulu ya sayangku~ (◕ω◕)",
            reply_chat
        )
        return
    try:
        data = json.loads(result["result"])
        if not data:
            send_reply("❌ Redis ada tapi isinya kosong~ Ara ara... (◕ω◕)", reply_chat)
            return
        first_ts     = data[0]["timestamp"]
        last_ts      = data[-1]["timestamp"]
        hours_stored = len(data) * settings["scan_interval"] / 3600
        lookback     = settings["lookback_hours"]
        rr           = settings["redis_refresh_minutes"]
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
            f"⚡ *Status Redis (Read-Only) — Akeno cek buat kamu~* "
            f"Ufufufu... (◕‿◕)\n"
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
            f"⚠️ Ara ara~ Data ada tapi Akeno gagal baca: `{e}` (◕ω◕)",
            reply_chat
        )


def handle_help_command(reply_chat: str) -> None:
    message = (
        "Ara ara~ mau tahu semua yang bisa Akeno lakukan untukmu? "
        "Ufufufu... (◕‿◕)\n"
        "\n"
        "*Setting:*\n"
        "`/settings` - lihat semua settingan\n"
        "`/interval <detik>` - atur seberapa sering Akeno scan (60-3600)\n"
        "`/lookback <jam>` - atur periode lookback (1-24)\n"
        "`/heartbeat <menit>` - atur laporan rutin (0=off)\n"
        "`/threshold entry <val>` - threshold entry %\n"
        "`/threshold exit <val>` - threshold exit % _(sekaligus jadi TP target)_\n"
        "`/threshold invalid <val>` - threshold invalidation %\n"
        "`/peak <val>` - % reversal dari puncak untuk konfirmasi entry\n"
        "`/sltp` - lihat info TP & Trailing SL aktif\n"
        "`/sltp sl <val>` - ubah trailing SL distance %\n"
        "\n"
        "*Info:*\n"
        "`/status` - lihat kondisi Akeno sekarang\n"
        "`/redis` - cek data history dari Bot A di Redis\n"
        "`/help` - tampilkan pesan ini lagi\n"
        "\n"
        "💡 _TP dikunci ke exit threshold — ubah via `/threshold exit`_\n"
        "\n"
        "Selama kamu di sini, Akeno akan selalu menempel erat~ "
        "Ara ara, jangan ragu minta bantuan ya, sayangku. (◕ω◕)"
    )
    send_reply(message, reply_chat)


def handle_status_command(reply_chat: str) -> None:
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback      = settings["lookback_hours"]
    ready         = (
        "✅ Sudah siap~ Ufufufu..."
        if hours_of_data >= lookback
        else f"⏳ Sabar ya sayangku~ {hours_of_data:.1f}h / {lookback}h"
    )
    peak_line   = (
        f"Peak Gap: {peak_gap:+.2f}%\n"
        if (current_mode == Mode.PEAK_WATCH and peak_gap is not None)
        else ""
    )
    track_lines = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        exit_thresh = settings["exit_threshold"]
        sl_pct      = settings["sl_pct"]
        if active_strategy == Strategy.S1:
            tp_level  = exit_thresh
            tsl_level = trailing_gap_best + sl_pct
        else:
            tp_level  = -exit_thresh
            tsl_level = trailing_gap_best - sl_pct
        eth_target, _ = calc_tp_target_price(active_strategy)
        eth_str       = f"${eth_target:,.2f}" if eth_target else "N/A"
        track_lines   = (
            f"Entry Gap:    {entry_gap_value:+.2f}%\n"
            f"Best Gap:     {trailing_gap_best:+.2f}%\n"
            f"TP Gap:       {tp_level:+.2f}%\n"
            f"ETH TP price: {eth_str}\n"
            f"Trail SL:     {tsl_level:+.2f}%\n"
        )
    last_refresh_str = (
        last_redis_refresh.strftime("%H:%M:%S UTC")
        if last_redis_refresh else "Belum~"
    )
    message = (
        "📊 *Ara ara~ ini kondisi Akeno saat ini~* Ufufufu... (◕‿◕)\n"
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
def get_lookback_label() -> str:
    return f"{settings['lookback_hours']}h"


# =============================================================================
# Target Price Calculation
# =============================================================================
def calc_tp_target_price(strategy: Strategy) -> Tuple[Optional[float], Optional[float]]:
    """
    Estimasi target harga ETH saat TP tercapai.

    Asumsi: harga BTC relatif stabil dari titik entry,
    ETH yang perlu bergerak untuk menutup gap ke exit_threshold.

    S1 (gap konvergen ke +exit_thresh):
        target_eth_ret = btc_ret_entry + exit_thresh
        → ETH harus turun mendekati BTC return

    S2 (gap konvergen ke -exit_thresh):
        target_eth_ret = btc_ret_entry - exit_thresh
        → ETH harus naik mendekati BTC return dari bawah

    Returns: (eth_target_price, btc_current_price)
    """
    if None in (entry_btc_lb, entry_eth_lb, entry_btc_price, entry_eth_price):
        return None, None

    exit_thresh   = settings["exit_threshold"]
    btc_ret_entry = float(
        (entry_btc_price - entry_btc_lb) / entry_btc_lb * Decimal("100")
    )

    if strategy == Strategy.S1:
        target_eth_ret = btc_ret_entry + exit_thresh
    else:
        target_eth_ret = btc_ret_entry - exit_thresh

    eth_target  = float(entry_eth_lb) * (1 + target_eth_ret / 100)
    btc_current = float(entry_btc_price)
    return eth_target, btc_current


# =============================================================================
# Message Building
# =============================================================================

def build_peak_watch_message(strategy: Strategy, gap: Decimal) -> str:
    lb = get_lookback_label()
    if strategy == Strategy.S1:
        direction = "Long BTC / Short ETH"
        reason    = f"ETH pumping lebih kencang dari BTC ({lb})"
    else:
        direction = "Long ETH / Short BTC"
        reason    = f"ETH dumping lebih dalam dari BTC ({lb})"
    return (
        f"………\n"
        f"Ara ara~ Akeno melihat sesuatu yang menarik~ Ufufufu... (◕‿◕)\n"
        f"\n"
        f"_{reason}_\n"
        f"Rencananya *{direction}*~\n"
        f"\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"…Tapi Akeno tidak akan gegabah, sayangku.\n"
        f"Biarkan Akeno memantau puncaknya dulu~ Petirku sudah siap.\n"
        f"Akeno akan kabari saat waktunya tepat. (◕ω◕)"
    )


def build_entry_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
    peak:     float,
) -> str:
    lb          = get_lookback_label()
    gap_float   = float(gap)
    sl_pct      = settings["sl_pct"]
    exit_thresh = settings["exit_threshold"]

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

    eth_target, btc_ref = calc_tp_target_price(strategy)
    eth_target_str      = f"${eth_target:,.2f}" if eth_target else "N/A"
    btc_ref_str         = f"${btc_ref:,.2f}"    if btc_ref   else "N/A"

    return (
        f"Ara ara ara~!!! Ini saatnya, sayangku~!!! Ufufufu... ⚡\n"
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
        f"│ ETH TP:   {eth_target_str} ← estimasi harga\n"
        f"│ BTC ref:  {btc_ref_str}\n"
        f"│ Trail SL: {tsl_initial:+.2f}% _(ikut gerak)_\n"
        f"└─────────────────────\n"
        f"\n"
        f"Gap sudah berbalik {settings['peak_reversal']}% dari puncaknya~\n"
        f"Akeno sudah menunggu momen ini untukmu, sayangku. "
        f"Semuanya demi kamu~ ⚡"
    )


def build_exit_message(btc_ret: Decimal, eth_ret: Decimal, gap: Decimal) -> str:
    lb = get_lookback_label()
    return (
        f"Ara ara ara~!!! Ufufufu... (◕▿◕)\n"
        f"✅ *EXIT SIGNAL*\n"
        f"\n"
        f"Gap sudah konvergen~ Saatnya close posisi, sayangku!\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Akeno senang bisa membantu~ Ufufufu...\n"
        f"Akeno lanjut pantau lagi dari dekat ya~ ⚡🔍"
    )


def build_invalidation_message(
    strategy: Strategy,
    btc_ret:  Decimal,
    eth_ret:  Decimal,
    gap:      Decimal,
) -> str:
    lb = get_lookback_label()
    return (
        f"………\n"
        f"⚠️ *INVALIDATION: {strategy.value}*\n"
        f"\n"
        f"Ara ara~ maaf ya sayangku... Akeno sudah berusaha, "
        f"tapi gapnya malah melebar. Ufufufu, kali ini bukan salahmu~ (◕ω◕)\n"
        f"\n"
        f"*{lb} Change:*\n"
        f"┌─────────────────────\n"
        f"│ BTC:  {format_value(btc_ret)}%\n"
        f"│ ETH:  {format_value(eth_ret)}%\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu ya, sayangku. Akeno scan ulang dari awal~ ⚡\n"
        f"Lain kali petir Akeno pasti lebih tepat sasaran. (◕‿◕)"
    )


def build_peak_cancelled_message(strategy: Strategy, gap: Decimal) -> str:
    return (
        f"………\n"
        f"❌ *Peak Watch Dibatalkan: {strategy.value}*\n"
        f"\n"
        f"Ara ara~ gapnya mundur sendiri sebelum Akeno sempat konfirmasi. "
        f"Ufufufu, pasar nakal sekali ya~ (◕ω◕)\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Tidak apa-apa, sayangku. Akeno tetap di sini pantau dari dekat~ (◕‿◕)"
    )


def build_tp_message(
    btc_ret:      Decimal,
    eth_ret:      Decimal,
    gap:          Decimal,
    entry_gap:    float,
    tp_gap_level: float,
    eth_target:   Optional[float],
) -> str:
    lb         = get_lookback_label()
    eth_tp_str = f"${eth_target:,.2f}" if eth_target else "N/A"
    return (
        f"Ara ara~!!! TP kena sayangku~!!! Ufufufu... ✨\n"
        f"🎯 *TAKE PROFIT*\n"
        f"\n"
        f"Gap sudah konvergen maksimal sesuai target Akeno~\n"
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
        f"Akeno senang~ Misi sukses untukmu, sayangku! ⚡"
    )


def build_trailing_sl_message(
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
    entry_gap: float,
    best_gap:  float,
    sl_level:  float,
) -> str:
    lb            = get_lookback_label()
    profit_locked = abs(entry_gap - best_gap)
    return (
        f"………\n"
        f"⛔ *TRAILING STOP LOSS*\n"
        f"\n"
        f"Ara ara~ Akeno sudah jaga posisimu sampai di sini, sayangku. "
        f"Trailing SL kena, profit sudah Akeno amankan~ (◕ω◕)\n"
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
        f"Cut dulu ya sayangku. Akeno scan ulang~ ⚡\n"
        f"Lain kali petir Akeno pasti lebih tepat. (◕‿◕)"
    )


def build_heartbeat_message() -> str:
    lb          = get_lookback_label()
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
    hours_of_data = len(price_history) * settings["scan_interval"] / 3600
    lookback      = settings["lookback_hours"]
    data_status   = (
        f"✅ Sudah siap~ ({hours_of_data:.1f}h)"
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
        exit_thresh = settings["exit_threshold"]
        sl_pct      = settings["sl_pct"]
        if active_strategy == Strategy.S1:
            tp_level  = exit_thresh
            tsl_level = trailing_gap_best + sl_pct
        else:
            tp_level  = -exit_thresh
            tsl_level = trailing_gap_best - sl_pct
        eth_target, _ = calc_tp_target_price(active_strategy)
        eth_str_tp    = f"${eth_target:,.2f}" if eth_target else "N/A"
        track_lines   = (
            f"│ Entry:    {entry_gap_value:+.2f}%\n"
            f"│ TP gap:   {tp_level:+.2f}% (ETH: {eth_str_tp})\n"
            f"│ Trail SL: {tsl_level:+.2f}% (best: {trailing_gap_best:+.2f}%)\n"
        )
    last_refresh_str = (
        last_redis_refresh.strftime("%H:%M:%S UTC")
        if last_redis_refresh else "Belum~"
    )
    return (
        f"💓 *Ara ara~ kamu khawatir Akeno pergi kemana-mana ya?*\n"
        f"\n"
        f"Ufufufu... sayangku, Akeno tidak kemana-mana. Tidak akan pernah. (◕‿◕)\n"
        f"\n"
        f"*Mode:* {current_mode.value}\n"
        f"*Strategi:* {active_strategy.value if active_strategy else 'Belum ada~'}\n"
        f"\n"
        f"*{settings['heartbeat_minutes']} menit terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ Scan: {scan_stats['count']}x\n"
        f"│ Sinyal: {scan_stats['signals_sent']}x\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Harga sekarang:*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {btc_str}\n"
        f"│ ETH: {eth_str}\n"
        f"│ Gap ({lb}): {gap_str}\n"
        f"{peak_line}"
        f"{track_lines}"
        f"└─────────────────────\n"
        f"\n"
        f"*Data:* {data_status}\n"
        f"*Redis refresh:* {last_refresh_str} 🔒\n"
        f"\n"
        f"_Ara ara, Akeno lapor lagi {settings['heartbeat_minutes']} menit lagi ya~ "
        f"Jangan kangen terlalu dalam. Ufufufu... ⚡_"
    )


def send_heartbeat() -> bool:
    global scan_stats
    success = send_alert(build_heartbeat_message())
    scan_stats["count"]        = 0
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
def prune_history(now: datetime) -> None:
    global price_history
    cutoff = now - timedelta(
        hours=settings["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES
    )
    price_history = [p for p in price_history if p.timestamp >= cutoff]


def get_lookback_price(now: datetime) -> Optional[PricePoint]:
    target_time           = now - timedelta(hours=settings["lookback_hours"])
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
    current_mode      = Mode.SCAN
    active_strategy   = None
    entry_gap_value   = None
    trailing_gap_best = None
    entry_btc_price   = None
    entry_eth_price   = None
    entry_btc_lb      = None
    entry_eth_lb      = None


# =============================================================================
# TP + Trailing SL Checker
# =============================================================================
def check_sltp(
    gap_float: float,
    btc_ret:   Decimal,
    eth_ret:   Decimal,
    gap:       Decimal,
) -> bool:
    """
    Cek TP dan Trailing SL saat Mode TRACK.

    TP = exit_threshold (gap konvergen maksimal):
        S1: gap <= +exit_threshold
        S2: gap >= -exit_threshold

    Trailing SL:
        S1: trailing_gap_best = min gap sejak entry
            TSL = trailing_gap_best + sl_pct
            Trigger jika gap >= TSL

        S2: trailing_gap_best = max gap sejak entry
            TSL = trailing_gap_best - sl_pct
            Trigger jika gap <= TSL

    Return True jika TP atau TSL terpicu.
    """
    global trailing_gap_best

    if entry_gap_value is None or active_strategy is None or trailing_gap_best is None:
        return False

    exit_thresh = settings["exit_threshold"]
    sl_pct      = settings["sl_pct"]

    if active_strategy == Strategy.S1:
        # Update trailing best (min gap — makin kecil makin bagus untuk S1)
        if gap_float < trailing_gap_best:
            trailing_gap_best = gap_float
            logger.info(
                f"TSL S1 updated. Best: {trailing_gap_best:.2f}%, "
                f"TSL: {trailing_gap_best + sl_pct:.2f}%"
            )

        tsl_level = trailing_gap_best + sl_pct

        # Cek TP dulu
        if gap_float <= exit_thresh:
            eth_target, _ = calc_tp_target_price(Strategy.S1)
            send_alert(build_tp_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, exit_thresh, eth_target
            ))
            logger.info(
                f"TP S1. Entry: {entry_gap_value:.2f}%, "
                f"Now: {gap_float:.2f}%, TP: {exit_thresh:.2f}%"
            )
            reset_to_scan()
            return True

        # Cek TSL
        if gap_float >= tsl_level:
            send_alert(build_trailing_sl_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, trailing_gap_best, tsl_level
            ))
            logger.info(
                f"TSL S1. Entry: {entry_gap_value:.2f}%, "
                f"Best: {trailing_gap_best:.2f}%, "
                f"TSL: {tsl_level:.2f}%, Now: {gap_float:.2f}%"
            )
            reset_to_scan()
            return True

    elif active_strategy == Strategy.S2:
        # Update trailing best (max gap — makin besar/positif makin bagus untuk S2)
        if gap_float > trailing_gap_best:
            trailing_gap_best = gap_float
            logger.info(
                f"TSL S2 updated. Best: {trailing_gap_best:.2f}%, "
                f"TSL: {trailing_gap_best - sl_pct:.2f}%"
            )

        tsl_level = trailing_gap_best - sl_pct

        # Cek TP dulu
        if gap_float >= -exit_thresh:
            eth_target, _ = calc_tp_target_price(Strategy.S2)
            send_alert(build_tp_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, -exit_thresh, eth_target
            ))
            logger.info(
                f"TP S2. Entry: {entry_gap_value:.2f}%, "
                f"Now: {gap_float:.2f}%, TP: {-exit_thresh:.2f}%"
            )
            reset_to_scan()
            return True

        # Cek TSL
        if gap_float <= tsl_level:
            send_alert(build_trailing_sl_message(
                btc_ret, eth_ret, gap,
                entry_gap_value, trailing_gap_best, tsl_level
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
) -> None:
    global current_mode, active_strategy, peak_gap, peak_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb

    gap_float      = float(gap)
    entry_thresh   = settings["entry_threshold"]
    exit_thresh    = settings["exit_threshold"]
    invalid_thresh = settings["invalidation_threshold"]
    peak_reversal  = settings["peak_reversal"]

    # -------------------------------------------------------------------------
    if current_mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            current_mode  = Mode.PEAK_WATCH
            peak_strategy = Strategy.S1
            peak_gap      = gap_float
            send_alert(build_peak_watch_message(Strategy.S1, gap))
            logger.info(f"PEAK WATCH S1 started. Gap: {gap_float:.2f}%")

        elif gap_float <= -entry_thresh:
            current_mode  = Mode.PEAK_WATCH
            peak_strategy = Strategy.S2
            peak_gap      = gap_float
            send_alert(build_peak_watch_message(Strategy.S2, gap))
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
                    build_entry_message(Strategy.S1, btc_ret, eth_ret, gap, peak_gap)
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
                    build_entry_message(Strategy.S2, btc_ret, eth_ret, gap, peak_gap)
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
        # TP/TSL dicek duluan
        if check_sltp(gap_float, btc_ret, eth_ret, gap):
            return

        # Safety net exit — jaga-jaga kalau exit_thresh berubah di tengah jalan
        if active_strategy == Strategy.S1 and gap_float <= exit_thresh:
            send_alert(build_exit_message(btc_ret, eth_ret, gap))
            logger.info(f"EXIT S1 triggered. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        if active_strategy == Strategy.S2 and gap_float >= -exit_thresh:
            send_alert(build_exit_message(btc_ret, eth_ret, gap))
            logger.info(f"EXIT S2 triggered. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        # Invalidation
        if active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S1, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S1. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        if active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S2, btc_ret, eth_ret, gap))
            logger.info(f"INVALIDATION S2. Gap: {gap_float:.2f}%")
            reset_to_scan()
            return

        logger.debug(
            f"TRACK {active_strategy.value if active_strategy else 'None'}: "
            f"Gap {gap_float:.2f}%"
        )


# =============================================================================
# Startup Message
# =============================================================================
def send_startup_message() -> bool:
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
            "\n⚠️ Ara ara~ Akeno gagal ambil harga tadi... "
            "Tapi Akeno akan terus coba~ (◕ω◕)\n"
        )

    lb           = get_lookback_label()
    hours_loaded = len(price_history) * settings["scan_interval"] / 3600

    if len(price_history) > 0:
        history_info = (
            f"⚡ History dari Bot A sudah ada~ *{hours_loaded:.1f}h* data siap!\n"
            f"_Akeno tidak perlu mulai dari nol lagi~ Ufufufu... (◕‿◕)_\n"
        )
    else:
        history_info = (
            f"⏳ Menunggu Bot A kirim data ke Redis~\n"
            f"_Sinyal akan keluar setelah {lb} data tersedia~_\n"
        )

    return send_alert(
        f"………\n"
        f"Ara ara~ Akeno (Bot B) sudah siap, sayangku~ Ufufufu... (◕‿◕)\n"
        f"{price_info}\n"
        f"📊 Scan setiap {settings['scan_interval']}s | "
        f"Redis refresh setiap {settings['redis_refresh_minutes']} menit\n"
        f"📈 Entry: ±{settings['entry_threshold']}%\n"
        f"📉 Exit: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak reversal: {settings['peak_reversal']}%\n"
        f"✅ TP: saat gap ±{settings['exit_threshold']}% _(max konvergen)_\n"
        f"🛑 Trailing SL: {settings['sl_pct']}% distance\n"
        f"🔒 Redis: Read-Only (Bot A yang write)\n"
        f"\n"
        f"{history_info}\n"
        f"Ketik `/help` kalau butuh sesuatu. "
        f"Akeno takkan pergi, takkan ninggalin kamu sendirian. ⚡"
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

    logger.info("=" * 60)
    logger.info("Monk Bot B — Read-Only Redis | Max TP | Trailing SL")
    logger.info(
        f"Entry: {settings['entry_threshold']}% | "
        f"Exit/TP: {settings['exit_threshold']}% | "
        f"Invalid: {settings['invalidation_threshold']}% | "
        f"Peak: {settings['peak_reversal']}% | "
        f"TSL: {settings['sl_pct']}% | "
        f"Redis refresh: {settings['redis_refresh_minutes']}m"
    )
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    # Load history dari Redis (read-only), prune expired
    load_history()
    prune_history(datetime.now(timezone.utc))
    last_redis_refresh = datetime.now(timezone.utc)
    logger.info(
        f"History after initial load & prune: {len(price_history)} points"
    )

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message()

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            # Heartbeat
            if should_send_heartbeat(now):
                if send_heartbeat():
                    last_heartbeat_time = now

            # Refresh history dari Redis setiap 1 menit
            refresh_history_from_redis(now)

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
                    price_then = get_lookback_price(now)

                    if price_then is None:
                        hours = len(price_history) * settings["scan_interval"] / 3600
                        logger.info(
                            f"Waiting for Bot A data... "
                            f"({hours:.1f}h / {settings['lookback_hours']}h)"
                        )
                    else:
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
                            f"BTC {settings['lookback_hours']}h: "
                            f"{format_value(btc_ret)}% | "
                            f"ETH {settings['lookback_hours']}h: "
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
                        )
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
