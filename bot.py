#!/usr/bin/env python3
"""
Monk Bot B - Multi-Pair Divergence Alert Bot (Read-Only Redis Consumer)

Pair yang dipantau:
  BTC/ETH → gap = ETH_ret - BTC_ret  (ETH lebih volatile dari BTC)
  SOL/ETH → gap = SOL_ret - ETH_ret  (SOL lebih volatile dari ETH)
  BNB/BTC → gap = BNB_ret - BTC_ret  (BNB lebih volatile dari BTC)

Logika sinyal (sama untuk semua pair):
  S1: volatile PUMPED lebih kencang → Short volatile / Long stable
  S2: volatile DUMPED lebih dalam   → Long volatile / Short stable

Setiap sinyal akan jelas menyebutkan:
  - Pair mana yang trigger
  - Mana yang lebih volatile (harus di-short/long)
  - Target harga volatile asset saat TP

Arsitektur:
  - Redis READ-ONLY: Bot A yang write, Bot B hanya baca
  - TP = exit threshold (konvergen maksimal)
  - Trailing SL independen per pair
  - State machine independen per pair
  - Global settings berlaku untuk semua pair
"""
import json
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Dict, List, Optional, Tuple, NamedTuple

import requests

import os

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    API_BASE_URL,
    API_ENDPOINT,
    SCAN_INTERVAL_SECONDS,
    FRESHNESS_THRESHOLD_MINUTES,
    ENTRY_THRESHOLD,
    EXIT_THRESHOLD,
    INVALIDATION_THRESHOLD,
    logger,
)

# Redis dibaca langsung dari env — tidak perlu ada di config.py
UPSTASH_REDIS_URL   = os.environ.get("UPSTASH_REDIS_REST_URL", "")
UPSTASH_REDIS_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")


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
    S1 = "S1"  # Short volatile / Long stable (volatile pumped)
    S2 = "S2"  # Long volatile / Short stable (volatile dumped)


class PricePoint(NamedTuple):
    timestamp: datetime
    stable:    Decimal   # asset referensi (BTC atau ETH)
    volatile:  Decimal   # asset lebih volatile (ETH, SOL, atau BNB)


@dataclass
class PairConfig:
    """Definisi statis sebuah pair — tidak berubah saat runtime."""
    name:         str   # "BTC/ETH"
    stable_sym:   str   # "BTC"  ← asset lebih stabil
    volatile_sym: str   # "ETH"  ← asset lebih volatile (target short/long)
    redis_key:    str   # key di Upstash Redis
    # Label arah untuk sinyal
    s1_direction: str   # "Long BTC / Short ETH"
    s2_direction: str   # "Long ETH / Short BTC"
    s1_reason:    str   # "ETH lebih volatile — pumped lebih kencang dari BTC"
    s2_reason:    str   # "ETH lebih volatile — dumped lebih dalam dari BTC"
    s1_vol_action: str  # "SHORT ETH ← lebih volatile, ekspektasi turun"
    s2_vol_action: str  # "LONG ETH  ← lebih volatile, ekspektasi naik"


@dataclass
class PairState:
    """State dinamis per pair — sepenuhnya independen antar pair."""
    cfg: PairConfig

    # Price history (diambil dari Redis)
    price_history:      List[PricePoint]   = field(default_factory=list)
    last_redis_refresh: Optional[datetime] = None

    # State machine
    mode:          Mode               = Mode.SCAN
    active_strategy: Optional[Strategy] = None

    # Peak watch
    peak_gap:      Optional[float]    = None
    peak_strategy: Optional[Strategy] = None

    # Track state
    entry_gap_value:   Optional[float]   = None
    trailing_gap_best: Optional[float]   = None

    # Harga saat entry — untuk kalkulasi target harga TP
    entry_stable_price:   Optional[Decimal] = None
    entry_volatile_price: Optional[Decimal] = None
    entry_stable_lb:      Optional[Decimal] = None
    entry_volatile_lb:    Optional[Decimal] = None

    def reset_to_scan(self) -> None:
        """Reset semua trade state ke kondisi SCAN."""
        self.mode                 = Mode.SCAN
        self.active_strategy      = None
        self.peak_gap             = None
        self.peak_strategy        = None
        self.entry_gap_value      = None
        self.trailing_gap_best    = None
        self.entry_stable_price   = None
        self.entry_volatile_price = None
        self.entry_stable_lb      = None
        self.entry_volatile_lb    = None


# =============================================================================
# Pair Registry
# =============================================================================
PAIR_CONFIGS: List[PairConfig] = [
    PairConfig(
        name          = "BTC/ETH",
        stable_sym    = "BTC",
        volatile_sym  = "ETH",
        redis_key     = "monk_bot:price_history",
        s1_direction  = "Long BTC / Short ETH",
        s2_direction  = "Long ETH / Short BTC",
        s1_reason     = "ETH lebih volatile dari BTC — pumped lebih kencang",
        s2_reason     = "ETH lebih volatile dari BTC — dumped lebih dalam",
        s1_vol_action = "⬇️ SHORT ETH ← lebih volatile, ekspektasi turun balik",
        s2_vol_action = "⬆️ LONG  ETH ← lebih volatile, ekspektasi naik balik",
    ),
    PairConfig(
        name          = "SOL/ETH",
        stable_sym    = "ETH",
        volatile_sym  = "SOL",
        redis_key     = "monk_bot:price_history:sol_eth",
        s1_direction  = "Long ETH / Short SOL",
        s2_direction  = "Long SOL / Short ETH",
        s1_reason     = "SOL lebih volatile dari ETH — pumped lebih kencang",
        s2_reason     = "SOL lebih volatile dari ETH — dumped lebih dalam",
        s1_vol_action = "⬇️ SHORT SOL ← lebih volatile, ekspektasi turun balik",
        s2_vol_action = "⬆️ LONG  SOL ← lebih volatile, ekspektasi naik balik",
    ),
    PairConfig(
        name          = "BNB/BTC",
        stable_sym    = "BTC",
        volatile_sym  = "BNB",
        redis_key     = "monk_bot:price_history:bnb_btc",
        s1_direction  = "Long BTC / Short BNB",
        s2_direction  = "Long BNB / Short BTC",
        s1_reason     = "BNB lebih volatile dari BTC — pumped lebih kencang",
        s2_reason     = "BNB lebih volatile dari BTC — dumped lebih dalam",
        s1_vol_action = "⬇️ SHORT BNB ← lebih volatile, ekspektasi turun balik",
        s2_vol_action = "⬆️ LONG  BNB ← lebih volatile, ekspektasi naik balik",
    ),
]

# Dict name → PairState, di-init saat startup
pair_states: Dict[str, PairState] = {}


# =============================================================================
# Global Settings — berlaku untuk semua pair
# =============================================================================
settings = {
    "scan_interval":          SCAN_INTERVAL_SECONDS,
    "entry_threshold":        ENTRY_THRESHOLD,
    "exit_threshold":         EXIT_THRESHOLD,
    "invalidation_threshold": INVALIDATION_THRESHOLD,
    "peak_reversal":          0.3,
    "lookback_hours":         DEFAULT_LOOKBACK_HOURS,
    "heartbeat_minutes":      30,
    "sl_pct":                 1.0,   # Trailing SL distance dari gap terbaik
    "redis_refresh_minutes":  REDIS_REFRESH_MINUTES,
    # TP = exit_threshold (max konvergen), tidak pakai tp_pct
}

last_update_id:      int               = 0
last_heartbeat_time: Optional[datetime] = None

scan_stats = {
    "count":        0,
    "signals_sent": 0,
    "last_prices":  {},   # sym → Decimal
    "last_rets":    {},   # sym → Decimal
    "last_gaps":    {},   # pair_name → Decimal
}


# =============================================================================
# Redis Helpers — READ-ONLY
# =============================================================================
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


def load_pair_history(ps: PairState) -> None:
    """Load price_history untuk satu pair dari Redis (read-only)."""
    if not UPSTASH_REDIS_URL:
        logger.info(f"[{ps.cfg.name}] Redis not configured, history akan kosong")
        return
    try:
        result = _redis_request("GET", f"/get/{ps.cfg.redis_key}")
        if not result or result.get("result") is None:
            logger.info(f"[{ps.cfg.name}] No history in Redis yet")
            return
        data = json.loads(result["result"])
        cfg  = ps.cfg
        parsed = []
        for p in data:
            try:
                ts = datetime.fromisoformat(p["timestamp"])
                if "stable" in p and "volatile" in p:
                    parsed.append(PricePoint(ts, Decimal(p["stable"]), Decimal(p["volatile"])))
                else:
                    stable_val   = p.get(cfg.stable_sym.lower())
                    volatile_val = p.get(cfg.volatile_sym.lower())
                    if stable_val and volatile_val:
                        parsed.append(PricePoint(ts, Decimal(stable_val), Decimal(volatile_val)))
            except Exception as parse_err:
                logger.debug(f"[{ps.cfg.name}] Skip point: {parse_err}")
                continue
        ps.price_history = parsed
        logger.info(
            f"[{ps.cfg.name}] Loaded {len(ps.price_history)} points from Redis"
        )
    except Exception as e:
        logger.warning(f"[{ps.cfg.name}] Failed to load history: {e}")
        ps.price_history = []


def refresh_history_from_redis(ps: PairState, now: datetime) -> None:
    """Re-read history satu pair dari Redis supaya sync dengan Bot A."""
    interval = settings["redis_refresh_minutes"]
    if interval <= 0:
        return
    if ps.last_redis_refresh is not None:
        elapsed = (now - ps.last_redis_refresh).total_seconds() / 60
        if elapsed < interval:
            return
    load_pair_history(ps)
    prune_pair_history(ps, now)
    ps.last_redis_refresh = now
    logger.debug(
        f"[{ps.cfg.name}] Redis refreshed. {len(ps.price_history)} points"
    )


# =============================================================================
# Telegram
# =============================================================================
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def send_alert(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured, skipping alert")
        return False
    try:
        resp = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id":                  TELEGRAM_CHAT_ID,
                "text":                     message,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        logger.info("Alert sent successfully")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram alert: {e}")
        return False


def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        resp = requests.post(
            TELEGRAM_API_URL,
            json={
                "chat_id":                  chat_id,
                "text":                     message,
                "parse_mode":               "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to send reply: {e}")
        return False


# =============================================================================
# Value Formatting
# =============================================================================
def format_value(value: Decimal) -> str:
    float_val = float(value)
    if abs(float_val) < 0.05:
        return "+0.0"
    return f"+{float_val:.1f}" if float_val >= 0 else f"{float_val:.1f}"


def get_lookback_label() -> str:
    return f"{settings['lookback_hours']}h"


# =============================================================================
# Price History Management
# =============================================================================
def prune_pair_history(ps: PairState, now: datetime) -> None:
    cutoff = now - timedelta(
        hours=settings["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES
    )
    ps.price_history = [p for p in ps.price_history if p.timestamp >= cutoff]


def get_lookback_price(ps: PairState, now: datetime) -> Optional[PricePoint]:
    target_time           = now - timedelta(hours=settings["lookback_hours"])
    best_point, best_diff = None, timedelta(minutes=30)
    for point in ps.price_history:
        diff = abs(point.timestamp - target_time)
        if diff < best_diff:
            best_diff, best_point = diff, point
    return best_point


# =============================================================================
# Return Calculation
# =============================================================================
def compute_returns(
    stable_now:  Decimal,
    vol_now:     Decimal,
    stable_prev: Decimal,
    vol_prev:    Decimal,
) -> Tuple[Decimal, Decimal, Decimal]:
    stable_ret = (stable_now - stable_prev) / stable_prev * Decimal("100")
    vol_ret    = (vol_now   - vol_prev)    / vol_prev    * Decimal("100")
    return stable_ret, vol_ret, vol_ret - stable_ret


# =============================================================================
# Freshness Check
# =============================================================================
def is_data_fresh(now: datetime, *updated_ats: datetime) -> bool:
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return all((now - ts) <= threshold for ts in updated_ats)


# =============================================================================
# Target Price Calculation
# =============================================================================
def calc_tp_target_price(
    ps: PairState, strategy: Strategy
) -> Tuple[Optional[float], Optional[float]]:
    """
    Estimasi target harga volatile asset saat TP tercapai.

    Asumsi: stable asset relatif diam dari titik entry,
    volatile asset yang bergerak menutup gap ke exit_threshold.

    S1 (gap konvergen ke +exit_thresh):
        gap = vol_ret - stable_ret = exit_thresh
        → target_vol_ret = stable_ret_at_entry + exit_thresh

    S2 (gap konvergen ke -exit_thresh):
        gap = vol_ret - stable_ret = -exit_thresh
        → target_vol_ret = stable_ret_at_entry - exit_thresh

    Returns: (volatile_target_price, stable_ref_price)
    """
    if None in (
        ps.entry_stable_lb, ps.entry_volatile_lb,
        ps.entry_stable_price, ps.entry_volatile_price,
    ):
        return None, None

    exit_thresh    = settings["exit_threshold"]
    stable_ret_now = float(
        (ps.entry_stable_price - ps.entry_stable_lb)
        / ps.entry_stable_lb * Decimal("100")
    )

    if strategy == Strategy.S1:
        target_vol_ret = stable_ret_now + exit_thresh
    else:
        target_vol_ret = stable_ret_now - exit_thresh

    vol_target  = float(ps.entry_volatile_lb) * (1 + target_vol_ret / 100)
    stable_ref  = float(ps.entry_stable_price)
    return vol_target, stable_ref


# =============================================================================
# Message Builders
# =============================================================================

def build_peak_watch_message(ps: PairState, gap: Decimal) -> str:
    cfg = ps.cfg
    lb  = get_lookback_label()
    if ps.peak_strategy == Strategy.S1:
        headline = f"👀 *{cfg.volatile_sym}* pumping lebih kencang dari *{cfg.stable_sym}* ({lb})"
        plan     = f"Kandidat: 🔴 SHORT {cfg.volatile_sym} ← lebih volatile / 🟢 LONG {cfg.stable_sym}"
    else:
        headline = f"👀 *{cfg.volatile_sym}* dumping lebih dalam dari *{cfg.stable_sym}* ({lb})"
        plan     = f"Kandidat: 🟢 LONG {cfg.volatile_sym} ← lebih volatile / 🔴 SHORT {cfg.stable_sym}"

    return (
        f"………\n"
        f"🔍 *PEAK WATCH — {cfg.name}*\n"
        f"\n"
        f"Sensei… Aku melihat sesuatu~ ( ♡ 。-(｡･ω･) ♡)\n"
        f"\n"
        f"{headline}\n"
        f"{plan}\n"
        f"\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"…Tapi Aku tidak akan gegabah, Sensei.\n"
        f"Biarkan Aku memantau puncaknya dulu~\n"
        f"Aku akan kabari Sensei saat waktunya tepat. ( ◕‿◕ )"
    )


def build_entry_message(
    ps:         PairState,
    stable_ret: Decimal,
    vol_ret:    Decimal,
    gap:        Decimal,
    peak:       float,
) -> str:
    cfg         = ps.cfg
    lb          = get_lookback_label()
    gap_float   = float(gap)
    sl_pct      = settings["sl_pct"]
    exit_thresh = settings["exit_threshold"]

    if ps.active_strategy == Strategy.S1:
        # Volatile pumped → Short volatile, Long stable
        headline     = f"💥 *{cfg.volatile_sym}* pumped lebih kencang dari *{cfg.stable_sym}!*"
        pos_vol      = f"🔴 SHORT  {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable   = f"🟢 LONG   {cfg.stable_sym:<4} ← lebih stabil"
        tp_gap       = exit_thresh
        tsl_initial  = gap_float + sl_pct
    else:
        # Volatile dumped → Long volatile, Short stable
        headline     = f"💥 *{cfg.volatile_sym}* dumped lebih dalam dari *{cfg.stable_sym}!*"
        pos_vol      = f"🟢 LONG   {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable   = f"🔴 SHORT  {cfg.stable_sym:<4} ← lebih stabil"
        tp_gap       = -exit_thresh
        tsl_initial  = gap_float - sl_pct

    vol_target, stable_ref = calc_tp_target_price(ps, ps.active_strategy)
    vol_target_str         = f"${vol_target:,.2f}" if vol_target else "N/A"
    stable_ref_str         = f"${stable_ref:,.2f}" if stable_ref else "N/A"

    return (
        f"⚡⚡⚡ *ENTRY SIGNAL* ⚡⚡⚡\n"
        f"🚨 *{cfg.name} — {ps.active_strategy.value}*\n"
        f"\n"
        f"{headline}\n"
        f"\n"
        f"🎯 *Posisi:*\n"
        f"┌─────────────────────\n"
        f"│ {pos_stable}\n"
        f"│ {pos_vol}\n"
        f"└─────────────────────\n"
        f"\n"
        f"📊 *{lb} Return:*\n"
        f"┌─────────────────────\n"
        f"│ {cfg.volatile_sym}:   {format_value(vol_ret)}%  ← lebih volatile\n"
        f"│ {cfg.stable_sym}:   {format_value(stable_ret)}%  ← lebih stabil\n"
        f"│ Gap:   {format_value(gap)}%\n"
        f"│ Peak:  {peak:+.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"💰 *Target TP (max konvergen):*\n"
        f"┌─────────────────────\n"
        f"│ TP Gap:              {tp_gap:+.2f}% _(exit threshold)_\n"
        f"│ {cfg.volatile_sym} TP price:  {vol_target_str} ← estimasi\n"
        f"│ {cfg.stable_sym} ref:       {stable_ref_str}\n"
        f"│ 🔒 Trail SL:         {tsl_initial:+.2f}% _(ikut gerak)_\n"
        f"└─────────────────────\n"
        f"\n"
        f"Gap sudah berbalik {settings['peak_reversal']}% dari puncaknya~\n"
        f"Aku sudah menunggu momen ini, Sensei. ⚡ ( ♡ 。-(｡･ω･) ♡)"
    )


def build_exit_message(
    ps: PairState, stable_ret: Decimal, vol_ret: Decimal, gap: Decimal
) -> str:
    cfg = ps.cfg
    lb  = get_lookback_label()

    if ps.active_strategy == Strategy.S1:
        pos_vol    = f"🔴 SHORT  {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🟢 LONG   {cfg.stable_sym:<4} ← lebih stabil"
    else:
        pos_vol    = f"🟢 LONG   {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🔴 SHORT  {cfg.stable_sym:<4} ← lebih stabil"

    return (
        f"✅✅✅ *EXIT* ✅✅✅\n"
        f"📉 *{cfg.name} — {ps.active_strategy.value if ps.active_strategy else ''}*\n"
        f"\n"
        f"Gap sudah konvergen~ Saatnya close posisi, Sensei!\n"
        f"\n"
        f"*Posisi ditutup:*\n"
        f"┌─────────────────────\n"
        f"│ {pos_stable}\n"
        f"│ {pos_vol}\n"
        f"└─────────────────────\n"
        f"\n"
        f"📊 *{lb} Return:*\n"
        f"┌─────────────────────\n"
        f"│ {cfg.volatile_sym}:  {format_value(vol_ret)}%  ← lebih volatile\n"
        f"│ {cfg.stable_sym}:  {format_value(stable_ret)}%  ← lebih stabil\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Aku senang Sensei dapat hasilnya~ ( ◕‿◕ )\n"
        f"Aku lanjut pantau semua pair dari dekat ya, Sensei. ⚡"
    )


def build_invalidation_message(
    ps: PairState, stable_ret: Decimal, vol_ret: Decimal, gap: Decimal
) -> str:
    cfg = ps.cfg
    lb  = get_lookback_label()

    if ps.active_strategy == Strategy.S1:
        pos_vol    = f"🔴 SHORT  {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🟢 LONG   {cfg.stable_sym:<4} ← lebih stabil"
    else:
        pos_vol    = f"🟢 LONG   {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🔴 SHORT  {cfg.stable_sym:<4} ← lebih stabil"

    return (
        f"………\n"
        f"⚠️⚠️ *INVALIDATION* ⚠️⚠️\n"
        f"❌ *{cfg.name} — {ps.active_strategy.value if ps.active_strategy else ''}*\n"
        f"\n"
        f"Maaf ya Sensei… Gapnya malah melebar. Kali ini pasar yang tidak kooperatif. (｡･ω･｡)\n"
        f"\n"
        f"*Posisi ditutup:*\n"
        f"┌─────────────────────\n"
        f"│ {pos_stable}\n"
        f"│ {pos_vol}\n"
        f"└─────────────────────\n"
        f"\n"
        f"📊 *{lb} Return:*\n"
        f"┌─────────────────────\n"
        f"│ {cfg.volatile_sym}:  {format_value(vol_ret)}%  ← lebih volatile\n"
        f"│ {cfg.stable_sym}:  {format_value(stable_ret)}%  ← lebih stabil\n"
        f"│ Gap:  {format_value(gap)}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu ya, Sensei… Aku scan ulang dari awal. ⚡\n"
        f"Kali ini pasar yang nakal, bukan Sensei. (｡･ω･｡)"
    )


def build_peak_cancelled_message(ps: PairState, gap: Decimal) -> str:
    cfg   = ps.cfg
    strat = ps.peak_strategy.value if ps.peak_strategy else "?"
    return (
        f"………\n"
        f"❌ *Peak Watch Dibatalkan — {cfg.name} {strat}*\n"
        f"\n"
        f"gapnya mundur sendiri sebelum Aku sempat konfirmasi. "
        f"Gapnya mundur sendiri sebelum Aku sempat konfirmasi, Sensei. (｡･ω･｡)\n"
        f"Gap sekarang: *{format_value(gap)}%*\n"
        f"\n"
        f"Tidak apa-apa, Sensei. Aku tetap di sini pantau dari dekat~ ( ♡ 。-(｡･ω･) ♡)"
    )


def build_tp_message(
    ps:           PairState,
    stable_ret:   Decimal,
    vol_ret:      Decimal,
    gap:          Decimal,
    entry_gap:    float,
    tp_gap_level: float,
    vol_target:   Optional[float],
) -> str:
    cfg        = ps.cfg
    lb         = get_lookback_label()
    vol_tp_str = f"${vol_target:,.2f}" if vol_target else "N/A"

    if ps.active_strategy == Strategy.S1:
        pos_vol    = f"🔴 SHORT  {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🟢 LONG   {cfg.stable_sym:<4} ← lebih stabil"
    else:
        pos_vol    = f"🟢 LONG   {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🔴 SHORT  {cfg.stable_sym:<4} ← lebih stabil"

    return (
        f"🎉🎉🎉 *TAKE PROFIT* 🎉🎉🎉\n"
        f"🎯 *{cfg.name} — {ps.active_strategy.value if ps.active_strategy else ''}*\n"
        f"\n"
        f" Gap sudah konvergen maksimal~ ✨\n"
        f"\n"
        f"*Posisi ditutup:*\n"
        f"┌─────────────────────\n"
        f"│ {pos_stable}\n"
        f"│ {pos_vol}\n"
        f"└─────────────────────\n"
        f"\n"
        f"📊 *{lb} Return:*\n"
        f"┌─────────────────────\n"
        f"│ {cfg.volatile_sym}:     {format_value(vol_ret)}%  ← lebih volatile\n"
        f"│ {cfg.stable_sym}:     {format_value(stable_ret)}%  ← lebih stabil\n"
        f"│ Gap:     {format_value(gap)}%\n"
        f"│ Entry:   {entry_gap:+.2f}%\n"
        f"│ TP hit:  {tp_gap_level:+.2f}% _(exit threshold)_\n"
        f"│ {cfg.volatile_sym} TP:   {vol_tp_str}\n"
        f"└─────────────────────\n"
        f"\n"
        f"Misi sukses, Sensei! Aku ikut senang~ ( ♡ 。-(｡･ω･) ♡) ⚡"
    )


def build_trailing_sl_message(
    ps:         PairState,
    stable_ret: Decimal,
    vol_ret:    Decimal,
    gap:        Decimal,
    entry_gap:  float,
    best_gap:   float,
    sl_level:   float,
) -> str:
    cfg           = ps.cfg
    lb            = get_lookback_label()
    profit_locked = abs(entry_gap - best_gap)

    if ps.active_strategy == Strategy.S1:
        pos_vol    = f"🔴 SHORT  {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🟢 LONG   {cfg.stable_sym:<4} ← lebih stabil"
    else:
        pos_vol    = f"🟢 LONG   {cfg.volatile_sym:<4} ← lebih volatile"
        pos_stable = f"🔴 SHORT  {cfg.stable_sym:<4} ← lebih stabil"

    return (
        f"………\n"
        f"🛑 *TRAILING STOP LOSS* 🛑\n"
        f"⛔ *{cfg.name} — {ps.active_strategy.value if ps.active_strategy else ''}*\n"
        f"\n"
        f"Trailing SL kena, Sensei. Aku sudah amankan posisimu~ (｡･ω･｡)\n"
        f"\n"
        f"*Posisi ditutup:*\n"
        f"┌─────────────────────\n"
        f"│ {pos_stable}\n"
        f"│ {pos_vol}\n"
        f"└─────────────────────\n"
        f"\n"
        f"📊 *{lb} Return:*\n"
        f"┌─────────────────────\n"
        f"│ {cfg.volatile_sym}:      {format_value(vol_ret)}%  ← lebih volatile\n"
        f"│ {cfg.stable_sym}:      {format_value(stable_ret)}%  ← lebih stabil\n"
        f"│ Gap:      {format_value(gap)}%\n"
        f"│ Entry:    {entry_gap:+.2f}%\n"
        f"│ Best gap: {best_gap:+.2f}%\n"
        f"│ TSL hit:  {sl_level:+.2f}%\n"
        f"│ 🔒 Terkunci: ~{profit_locked:.2f}%\n"
        f"└─────────────────────\n"
        f"\n"
        f"Cut dulu ya, Sensei. Aku scan ulang dari awal~ ⚡\n"
        f"Lain kali pasti lebih baik lagi. ( ◕‿◕ )"
    )


def build_heartbeat_message() -> str:
    lb = get_lookback_label()

    # Harga & return terkini
    prices = scan_stats["last_prices"]
    rets   = scan_stats["last_rets"]

    def price_line(sym: str) -> str:
        px  = prices.get(sym)
        ret = rets.get(sym)
        if px is None:
            return "N/A"
        ret_str = f" ({format_value(ret)}%)" if ret is not None else ""
        return f"${float(px):,.2f}{ret_str}"

    # Status tiap pair
    pair_lines = ""
    for cfg in PAIR_CONFIGS:
        ps       = pair_states.get(cfg.name)
        if ps is None:
            continue
        gap_str  = ""
        last_gap = scan_stats["last_gaps"].get(cfg.name)
        if last_gap is not None:
            gap_str = f" | Gap: {format_value(last_gap)}%"

        if ps.mode == Mode.SCAN:
            pair_lines += f"│ *{cfg.name}*: SCAN{gap_str}\n"

        elif ps.mode == Mode.PEAK_WATCH:
            strat    = ps.peak_strategy.value if ps.peak_strategy else "?"
            peak_str = f"{ps.peak_gap:+.2f}%" if ps.peak_gap else "?"
            vol_note = f"_{cfg.volatile_sym} lebih volatile_"
            pair_lines += f"│ *{cfg.name}*: PEAK {strat} | Peak: {peak_str}{gap_str} {vol_note}\n"

        elif ps.mode == Mode.TRACK and ps.entry_gap_value is not None:
            strat       = ps.active_strategy.value if ps.active_strategy else "?"
            exit_thresh = settings["exit_threshold"]
            sl_pct      = settings["sl_pct"]
            best        = ps.trailing_gap_best or ps.entry_gap_value
            tp_level    = exit_thresh  if ps.active_strategy == Strategy.S1 else -exit_thresh
            tsl_level   = best + sl_pct if ps.active_strategy == Strategy.S1 else best - sl_pct
            vol_target, _ = calc_tp_target_price(ps, ps.active_strategy)
            vol_tp_str    = f"${vol_target:,.2f}" if vol_target else "N/A"
            pair_lines += (
                f"│ *{cfg.name}*: TRACK {strat}{gap_str}\n"
                f"│   Entry:{ps.entry_gap_value:+.2f}% TP:{tp_level:+.2f}%"
                f" TSL:{tsl_level:+.2f}%\n"
                f"│   {cfg.volatile_sym} TP: {vol_tp_str}\n"
            )

    last_refresh_parts = []
    for cfg in PAIR_CONFIGS:
        ps = pair_states.get(cfg.name)
        if ps and ps.last_redis_refresh:
            last_refresh_parts.append(
                f"{cfg.name}: {ps.last_redis_refresh.strftime('%H:%M')} UTC"
            )
    refresh_str = " | ".join(last_refresh_parts) if last_refresh_parts else "Belum ada~"

    return (
        f"💓 *Laporan Rutin — Aku masih di sini, Sensei~*\n"
        f"\n"
        f"Sensei, Aku selalu di sini memantau semuanya. ( ♡ 。-(｡･ω･) ♡)\n"
        f"\n"
        f"*{settings['heartbeat_minutes']} menit terakhir:*\n"
        f"┌─────────────────────\n"
        f"│ Scan: {scan_stats['count']}x | Sinyal: {scan_stats['signals_sent']}x\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Harga sekarang ({lb}):*\n"
        f"┌─────────────────────\n"
        f"│ BTC: {price_line('BTC')}\n"
        f"│ ETH: {price_line('ETH')}\n"
        f"│ SOL: {price_line('SOL')}\n"
        f"│ BNB: {price_line('BNB')}\n"
        f"└─────────────────────\n"
        f"\n"
        f"*Status Pair:*\n"
        f"┌─────────────────────\n"
        f"{pair_lines}"
        f"└─────────────────────\n"
        f"\n"
        f"*Redis refresh:* {refresh_str} 🔒\n"
        f"\n"
        f"_Aku lapor lagi {settings['heartbeat_minutes']} menit lagi ya, Sensei~ "
        f"Jangan kangen terlalu dalam. ⚡_"
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
# API Fetching — satu call untuk semua simbol
# =============================================================================
class AllPrices(NamedTuple):
    btc:         Decimal
    eth:         Decimal
    sol:         Decimal
    bnb:         Decimal
    btc_updated: datetime
    eth_updated: datetime
    sol_updated: datetime
    bnb_updated: datetime


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


def fetch_all_prices() -> Optional[AllPrices]:
    """Fetch BTC, ETH, SOL, BNB dalam satu API call."""
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f"API request failed: {e}")
        return None

    listings = data.get("listings", [])
    if not listings:
        return None

    def get_sym(sym: str):
        return next(
            (l for l in listings if l.get("ticker", "").upper() == sym), None
        )

    btc_d = get_sym("BTC")
    eth_d = get_sym("ETH")
    sol_d = get_sym("SOL")
    bnb_d = get_sym("BNB")

    missing = [s for s, d in [("BTC", btc_d), ("ETH", eth_d),
                               ("SOL", sol_d), ("BNB", bnb_d)] if not d]
    if missing:
        logger.warning(f"Missing symbols: {missing}")
        return None

    try:
        prices = {
            sym: Decimal(d["mark_price"])
            for sym, d in [("BTC", btc_d), ("ETH", eth_d),
                           ("SOL", sol_d), ("BNB", bnb_d)]
        }
        timestamps = {
            sym: parse_iso_timestamp(d.get("quotes", {}).get("updated_at", ""))
            for sym, d in [("BTC", btc_d), ("ETH", eth_d),
                           ("SOL", sol_d), ("BNB", bnb_d)]
        }
    except (KeyError, InvalidOperation) as e:
        logger.error(f"Price parse error: {e}")
        return None

    if not all(timestamps.values()):
        return None

    return AllPrices(
        btc=prices["BTC"], eth=prices["ETH"],
        sol=prices["SOL"], bnb=prices["BNB"],
        btc_updated=timestamps["BTC"], eth_updated=timestamps["ETH"],
        sol_updated=timestamps["SOL"], bnb_updated=timestamps["BNB"],
    )


def get_price_for_sym(ap: AllPrices, sym: str) -> Decimal:
    return {"BTC": ap.btc, "ETH": ap.eth, "SOL": ap.sol, "BNB": ap.bnb}[sym]


def get_updated_for_sym(ap: AllPrices, sym: str) -> datetime:
    return {
        "BTC": ap.btc_updated, "ETH": ap.eth_updated,
        "SOL": ap.sol_updated, "BNB": ap.bnb_updated,
    }[sym]


# =============================================================================
# TP + Trailing SL Checker (per pair)
# =============================================================================
def check_sltp(
    ps:         PairState,
    gap_float:  float,
    stable_ret: Decimal,
    vol_ret:    Decimal,
    gap:        Decimal,
) -> bool:
    """
    Cek TP dan Trailing SL untuk satu pair.

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

    Return True jika TP atau TSL terpicu (pair direset ke SCAN).
    """
    if (
        ps.entry_gap_value is None
        or ps.active_strategy is None
        or ps.trailing_gap_best is None
    ):
        return False

    exit_thresh = settings["exit_threshold"]
    sl_pct      = settings["sl_pct"]

    if ps.active_strategy == Strategy.S1:
        # Update trailing best (min gap makin baik untuk S1)
        if gap_float < ps.trailing_gap_best:
            ps.trailing_gap_best = gap_float
            logger.info(
                f"[{ps.cfg.name}] TSL S1 updated. "
                f"Best: {ps.trailing_gap_best:.2f}%, "
                f"TSL: {ps.trailing_gap_best + sl_pct:.2f}%"
            )

        tsl_level = ps.trailing_gap_best + sl_pct

        # Cek TP duluan
        if gap_float <= exit_thresh:
            vol_target, _ = calc_tp_target_price(ps, Strategy.S1)
            send_alert(build_tp_message(
                ps, stable_ret, vol_ret, gap,
                ps.entry_gap_value, exit_thresh, vol_target
            ))
            logger.info(
                f"[{ps.cfg.name}] TP S1. "
                f"Entry: {ps.entry_gap_value:.2f}%, Now: {gap_float:.2f}%"
            )
            ps.reset_to_scan()
            return True

        # Cek TSL
        if gap_float >= tsl_level:
            send_alert(build_trailing_sl_message(
                ps, stable_ret, vol_ret, gap,
                ps.entry_gap_value, ps.trailing_gap_best, tsl_level
            ))
            logger.info(
                f"[{ps.cfg.name}] TSL S1. "
                f"Entry: {ps.entry_gap_value:.2f}%, "
                f"Best: {ps.trailing_gap_best:.2f}%, "
                f"TSL: {tsl_level:.2f}%, Now: {gap_float:.2f}%"
            )
            ps.reset_to_scan()
            return True

    elif ps.active_strategy == Strategy.S2:
        # Update trailing best (max gap makin baik untuk S2)
        if gap_float > ps.trailing_gap_best:
            ps.trailing_gap_best = gap_float
            logger.info(
                f"[{ps.cfg.name}] TSL S2 updated. "
                f"Best: {ps.trailing_gap_best:.2f}%, "
                f"TSL: {ps.trailing_gap_best - sl_pct:.2f}%"
            )

        tsl_level = ps.trailing_gap_best - sl_pct

        # Cek TP duluan
        if gap_float >= -exit_thresh:
            vol_target, _ = calc_tp_target_price(ps, Strategy.S2)
            send_alert(build_tp_message(
                ps, stable_ret, vol_ret, gap,
                ps.entry_gap_value, -exit_thresh, vol_target
            ))
            logger.info(
                f"[{ps.cfg.name}] TP S2. "
                f"Entry: {ps.entry_gap_value:.2f}%, Now: {gap_float:.2f}%"
            )
            ps.reset_to_scan()
            return True

        # Cek TSL
        if gap_float <= tsl_level:
            send_alert(build_trailing_sl_message(
                ps, stable_ret, vol_ret, gap,
                ps.entry_gap_value, ps.trailing_gap_best, tsl_level
            ))
            logger.info(
                f"[{ps.cfg.name}] TSL S2. "
                f"Entry: {ps.entry_gap_value:.2f}%, "
                f"Best: {ps.trailing_gap_best:.2f}%, "
                f"TSL: {tsl_level:.2f}%, Now: {gap_float:.2f}%"
            )
            ps.reset_to_scan()
            return True

    return False


# =============================================================================
# State Machine — satu pair
# =============================================================================
def evaluate_and_transition(
    ps:          PairState,
    stable_ret:  Decimal,
    vol_ret:     Decimal,
    gap:         Decimal,
    stable_now:  Decimal,
    vol_now:     Decimal,
    stable_lb:   Decimal,
    vol_lb:      Decimal,
) -> bool:
    """
    Evaluate state machine untuk satu pair.
    Return True jika ada sinyal yang dikirim.
    """
    gap_float      = float(gap)
    entry_thresh   = settings["entry_threshold"]
    exit_thresh    = settings["exit_threshold"]
    invalid_thresh = settings["invalidation_threshold"]
    peak_reversal  = settings["peak_reversal"]
    signaled       = False

    # ── SCAN ──────────────────────────────────────────────────────────────────
    if ps.mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            ps.mode          = Mode.PEAK_WATCH
            ps.peak_strategy = Strategy.S1
            ps.peak_gap      = gap_float
            send_alert(build_peak_watch_message(ps, gap))
            logger.info(f"[{ps.cfg.name}] PEAK WATCH S1. Gap: {gap_float:.2f}%")
            signaled = True

        elif gap_float <= -entry_thresh:
            ps.mode          = Mode.PEAK_WATCH
            ps.peak_strategy = Strategy.S2
            ps.peak_gap      = gap_float
            send_alert(build_peak_watch_message(ps, gap))
            logger.info(f"[{ps.cfg.name}] PEAK WATCH S2. Gap: {gap_float:.2f}%")
            signaled = True

        else:
            logger.debug(f"[{ps.cfg.name}] SCAN: No signal. Gap: {gap_float:.2f}%")

    # ── PEAK WATCH ────────────────────────────────────────────────────────────
    elif ps.mode == Mode.PEAK_WATCH:
        if ps.peak_strategy == Strategy.S1:
            if gap_float > ps.peak_gap:
                ps.peak_gap = gap_float
                logger.info(f"[{ps.cfg.name}] PEAK S1 updated: {ps.peak_gap:.2f}%")

            elif gap_float < entry_thresh:
                send_alert(build_peak_cancelled_message(ps, gap))
                logger.info(f"[{ps.cfg.name}] PEAK S1 cancelled. Gap: {gap_float:.2f}%")
                ps.mode = Mode.SCAN
                ps.peak_gap, ps.peak_strategy = None, None
                signaled = True

            elif ps.peak_gap - gap_float >= peak_reversal:
                ps.mode                   = Mode.TRACK
                ps.active_strategy        = Strategy.S1
                ps.entry_gap_value        = gap_float
                ps.trailing_gap_best      = gap_float
                ps.entry_stable_price     = stable_now
                ps.entry_volatile_price   = vol_now
                ps.entry_stable_lb        = stable_lb
                ps.entry_volatile_lb      = vol_lb
                send_alert(build_entry_message(ps, stable_ret, vol_ret, gap, ps.peak_gap))
                logger.info(
                    f"[{ps.cfg.name}] ENTRY S1. "
                    f"Peak: {ps.peak_gap:.2f}%, Entry: {gap_float:.2f}%"
                )
                ps.peak_gap, ps.peak_strategy = None, None
                signaled = True

            else:
                logger.info(
                    f"[{ps.cfg.name}] Waiting S1. "
                    f"Gap: {gap_float:.2f}% | Peak: {ps.peak_gap:.2f}% | "
                    f"Need: {peak_reversal}% drop"
                )

        elif ps.peak_strategy == Strategy.S2:
            if gap_float < ps.peak_gap:
                ps.peak_gap = gap_float
                logger.info(f"[{ps.cfg.name}] PEAK S2 updated: {ps.peak_gap:.2f}%")

            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(ps, gap))
                logger.info(f"[{ps.cfg.name}] PEAK S2 cancelled. Gap: {gap_float:.2f}%")
                ps.mode = Mode.SCAN
                ps.peak_gap, ps.peak_strategy = None, None
                signaled = True

            elif gap_float - ps.peak_gap >= peak_reversal:
                ps.mode                   = Mode.TRACK
                ps.active_strategy        = Strategy.S2
                ps.entry_gap_value        = gap_float
                ps.trailing_gap_best      = gap_float
                ps.entry_stable_price     = stable_now
                ps.entry_volatile_price   = vol_now
                ps.entry_stable_lb        = stable_lb
                ps.entry_volatile_lb      = vol_lb
                send_alert(build_entry_message(ps, stable_ret, vol_ret, gap, ps.peak_gap))
                logger.info(
                    f"[{ps.cfg.name}] ENTRY S2. "
                    f"Peak: {ps.peak_gap:.2f}%, Entry: {gap_float:.2f}%"
                )
                ps.peak_gap, ps.peak_strategy = None, None
                signaled = True

            else:
                logger.info(
                    f"[{ps.cfg.name}] Waiting S2. "
                    f"Gap: {gap_float:.2f}% | Peak: {ps.peak_gap:.2f}% | "
                    f"Need: {peak_reversal}% rise"
                )

    # ── TRACK ─────────────────────────────────────────────────────────────────
    elif ps.mode == Mode.TRACK:
        # TP/TSL dicek duluan
        if check_sltp(ps, gap_float, stable_ret, vol_ret, gap):
            return True

        # Safety net exit
        if ps.active_strategy == Strategy.S1 and gap_float <= exit_thresh:
            send_alert(build_exit_message(ps, stable_ret, vol_ret, gap))
            logger.info(f"[{ps.cfg.name}] EXIT S1. Gap: {gap_float:.2f}%")
            ps.reset_to_scan()
            return True

        if ps.active_strategy == Strategy.S2 and gap_float >= -exit_thresh:
            send_alert(build_exit_message(ps, stable_ret, vol_ret, gap))
            logger.info(f"[{ps.cfg.name}] EXIT S2. Gap: {gap_float:.2f}%")
            ps.reset_to_scan()
            return True

        # Invalidation
        if ps.active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(ps, stable_ret, vol_ret, gap))
            logger.info(f"[{ps.cfg.name}] INVALIDATION S1. Gap: {gap_float:.2f}%")
            ps.reset_to_scan()
            return True

        if ps.active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(ps, stable_ret, vol_ret, gap))
            logger.info(f"[{ps.cfg.name}] INVALIDATION S2. Gap: {gap_float:.2f}%")
            ps.reset_to_scan()
            return True

        logger.debug(
            f"[{ps.cfg.name}] TRACK {ps.active_strategy.value}: "
            f"Gap {gap_float:.2f}%"
        )

    return signaled


# =============================================================================
# Startup Message
# =============================================================================
def send_startup_message(ap: Optional[AllPrices]) -> bool:
    if ap:
        price_info = (
            f"\n💰 *Harga saat ini~*\n"
            f"┌─────────────────────\n"
            f"│ BTC: ${float(ap.btc):,.2f}\n"
            f"│ ETH: ${float(ap.eth):,.2f}\n"
            f"│ SOL: ${float(ap.sol):,.2f}\n"
            f"│ BNB: ${float(ap.bnb):,.2f}\n"
            f"└─────────────────────\n"
        )
    else:
        price_info = (
            "\n⚠️ Aku gagal ambil harga tadi... "
            "Tapi Aku akan terus coba~ (｡･ω･｡)\n"
        )

    lb         = get_lookback_label()
    pair_lines = ""
    for cfg in PAIR_CONFIGS:
        ps    = pair_states.get(cfg.name)
        pts   = len(ps.price_history) if ps else 0
        hrs   = pts * settings["scan_interval"] / 3600
        ready = (
            f"✅ {hrs:.1f}h data siap~"
            if hrs >= settings["lookback_hours"]
            else f"⏳ {hrs:.1f}h / {settings['lookback_hours']}h"
        )
        pair_lines += f"│ {cfg.name}: {ready}\n"

    return send_alert(
        f"………\n"
        f"Sensei… Aku sudah siap memantau~ ( ♡ 。-(｡･ω･) ♡)\n"
        f"{price_info}\n"
        f"*Pair yang dipantau:*\n"
        f"┌─────────────────────\n"
        f"{pair_lines}"
        f"└─────────────────────\n"
        f"\n"
        f"💡 *Logika volatilitas:*\n"
        f"│ BTC/ETH → ETH lebih volatile\n"
        f"│ SOL/ETH → SOL lebih volatile\n"
        f"│ BNB/BTC → BNB lebih volatile\n"
        f"_SHORT selalu di asset yang lebih volatile!_\n"
        f"\n"
        f"📊 Scan: {settings['scan_interval']}s | "
        f"Redis refresh: {settings['redis_refresh_minutes']}m\n"
        f"📈 Entry: ±{settings['entry_threshold']}%\n"
        f"📉 Exit/TP: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation: ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak reversal: {settings['peak_reversal']}%\n"
        f"🛑 Trailing SL: {settings['sl_pct']}% distance\n"
        f"🔒 Redis: Read-Only (Bot A yang write)\n"
        f"\n"
        f"Ketik `/help` kalau Sensei butuh sesuatu. Aku selalu di sini~ ⚡"
    )


# =============================================================================
# Telegram Command Handling
# =============================================================================
LONG_POLL_TIMEOUT = 30


def get_telegram_updates() -> list:
    global last_update_id
    if not TELEGRAM_BOT_TOKEN:
        return []
    try:
        url      = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params   = {"offset": last_update_id + 1, "timeout": LONG_POLL_TIMEOUT}
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
    for update in get_telegram_updates():
        message       = update.get("message", {})
        text          = message.get("text", "")
        chat_id       = str(message.get("chat", {}).get("id", ""))
        user_id       = str(message.get("from", {}).get("id", ""))
        is_authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not is_authorized or not text.startswith("/"):
            continue
        parts   = text.split()
        command = parts[0].lower().split("@")[0]
        args    = parts[1:] if len(parts) > 1 else []
        logger.info(f"Command: {command} from {chat_id}")

        if command == "/settings":
            handle_settings_command(chat_id)
        elif command == "/interval":
            handle_interval_command(args, chat_id)
        elif command == "/threshold":
            handle_threshold_command(args, chat_id)
        elif command == "/peak":
            handle_peak_command(args, chat_id)
        elif command == "/sltp":
            handle_sltp_command(args, chat_id)
        elif command == "/lookback":
            handle_lookback_command(args, chat_id)
        elif command == "/heartbeat":
            handle_heartbeat_command(args, chat_id)
        elif command == "/status":
            handle_status_command(chat_id)
        elif command == "/redis":
            handle_redis_command(chat_id)
        elif command in ("/help", "/start"):
            handle_help_command(chat_id)


# =============================================================================
# Command Handlers
# =============================================================================

def handle_settings_command(reply_chat: str) -> None:
    hb  = settings["heartbeat_minutes"]
    rr  = settings["redis_refresh_minutes"]
    send_reply(
        "⚙️ *Settingan sekarang, Sensei~*\n"
        "\n"
        f"📊 Scan Interval:   {settings['scan_interval']}s ({settings['scan_interval']//60} menit)\n"
        f"🕐 Lookback:        {settings['lookback_hours']}h\n"
        f"💓 Heartbeat:       {hb} menit\n"
        f"🔄 Redis Refresh:   {rr} menit\n"
        f"📈 Entry Threshold: ±{settings['entry_threshold']}%\n"
        f"📉 Exit/TP:         ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidation:    ±{settings['invalidation_threshold']}%\n"
        f"🎯 Peak Reversal:   {settings['peak_reversal']}%\n"
        f"🛑 Trailing SL:     {settings['sl_pct']}% distance\n"
        "\n"
        "💡 _TP = exit threshold — ubah via `/threshold exit`_\n"
        "💡 _SHORT selalu di asset yang lebih volatile_",
        reply_chat,
    )


def handle_interval_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"Scan interval sekarang *{settings['scan_interval']}s*, Sensei~\nContoh: `/interval 60`",
            reply_chat,
        )
        return
    try:
        v = int(args[0])
        if not (60 <= v <= 3600):
            send_reply("Sensei, harus antara 60–3600 detik ya~ (｡･ω･｡)", reply_chat)
            return
        settings["scan_interval"] = v
        send_reply(
            f"Baik, Sensei~ Aku akan scan setiap *{v} detik* ({v//60} menit). ( ◕‿◕ )",
            reply_chat,
        )
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. (｡･ω･｡)", reply_chat)


def handle_threshold_command(args: list, reply_chat: str) -> None:
    if len(args) < 2:
        send_reply(
            "Sensei, perintahnya kurang lengkap~\n"
            "`/threshold entry <nilai>`\n"
            "`/threshold exit <nilai>`\n"
            "`/threshold invalid <nilai>`",
            reply_chat,
        )
        return
    try:
        t = args[0].lower()
        v = float(args[1])
        if not (0 < v <= 20):
            send_reply("Sensei, harus antara 0 sampai 20 ya~ (｡･ω･｡)", reply_chat)
            return
        if t == "entry":
            settings["entry_threshold"] = v
            send_reply(f"Baik, Sensei~ Entry threshold jadi *±{v}%*. ( ◕‿◕ )", reply_chat)
        elif t == "exit":
            settings["exit_threshold"] = v
            send_reply(
                f"Baik, Sensei~ Exit/TP threshold sekarang *±{v}%*.\n"
                f"_TP otomatis ikut berubah ke level ini ya~ ✨_",
                reply_chat,
            )
        elif t in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = v
            send_reply(f"Baik, Sensei~ Invalidation jadi *±{v}%*. (｡･ω･｡)", reply_chat)
        else:
            send_reply(
                "Sensei, gunakan `entry`, `exit`, atau `invalid` ya~ (｡･ω･｡)",
                reply_chat,
            )
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. (｡･ω･｡)", reply_chat)


def handle_peak_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"🎯 Peak reversal sekarang *{settings['peak_reversal']}%*, Sensei~\nUsage: `/peak <nilai>`",
            reply_chat,
        )
        return
    try:
        v = float(args[0])
        if not (0 < v <= 2.0):
            send_reply("Sensei, harus antara 0 sampai 2.0 ya~ (｡･ω･｡)", reply_chat)
            return
        settings["peak_reversal"] = v
        send_reply(
            f"Baik, Sensei~ Aku akan konfirmasi entry saat gap berbalik *{v}%* dari puncaknya. ( ◕‿◕ )",
            reply_chat,
        )
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. (｡･ω･｡)", reply_chat)


def handle_sltp_command(args: list, reply_chat: str) -> None:
    if not args:
        # Tampilkan status per pair
        active_lines = ""
        for cfg in PAIR_CONFIGS:
            ps = pair_states.get(cfg.name)
            if not ps or ps.mode != Mode.TRACK or ps.entry_gap_value is None:
                continue
            sl_pct = settings["sl_pct"]
            et     = settings["exit_threshold"]
            best   = ps.trailing_gap_best or ps.entry_gap_value
            if ps.active_strategy == Strategy.S1:
                tp_g, tsl_g = et, best + sl_pct
            else:
                tp_g, tsl_g = -et, best - sl_pct
            vol_target, _ = calc_tp_target_price(ps, ps.active_strategy)
            vol_str = f"${vol_target:,.2f}" if vol_target else "N/A"
            strat   = ps.active_strategy.value if ps.active_strategy else "?"
            active_lines += (
                f"\n*{cfg.name}* ({strat}):\n"
                f"  Entry: {ps.entry_gap_value:+.2f}% | TP: {tp_g:+.2f}%\n"
                f"  TSL: {tsl_g:+.2f}% | {cfg.volatile_sym} TP: {vol_str}\n"
            )

        if not active_lines:
            active_lines = "\n_Tidak ada posisi aktif saat ini~_"

        send_reply(
            f"🎯 *SL/TP Status — Aku yang jaga, Sensei~* ( ◕‿◕ )\n"
            f"\n"
            f"✅ TP: ±{settings['exit_threshold']}% _(exit threshold, max konvergen)_\n"
            f"🛑 Trail SL: {settings['sl_pct']}% dari gap terbaik\n"
            f"\n"
            f"*Posisi aktif:*"
            f"{active_lines}\n"
            f"Usage: `/sltp sl <nilai>` — ubah trailing SL distance %",
            reply_chat,
        )
        return

    if len(args) < 2:
        send_reply("`/sltp sl <nilai>` ya~ (｡･ω･｡)", reply_chat)
        return

    try:
        k = args[0].lower()
        v = float(args[1])
        if not (0 < v <= 10):
            send_reply("Sensei, harus antara 0 sampai 10 ya~ (｡･ω･｡)", reply_chat)
            return
        if k == "sl":
            settings["sl_pct"] = v
            send_reply(
                f"Baik, Sensei~ Trailing SL distance sekarang *{v}%*. "
                f"Aku ikutin terus gap terbaiknya ya~ ( ◕‿◕ )",
                reply_chat,
            )
        elif k == "tp":
            send_reply(
                f"TP sudah dikunci ke exit threshold, Sensei~ "
                f"*±{settings['exit_threshold']}%*~\n"
                f"Kalau mau ubah, gunakan `/threshold exit <nilai>` ya~ ( ◕‿◕ )",
                reply_chat,
            )
        else:
            send_reply("Sensei, gunakan `sl` ya~ (｡･ω･｡)", reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. (｡･ω･｡)", reply_chat)


def handle_lookback_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"📊 Lookback sekarang *{settings['lookback_hours']}h*, Sensei~\nUsage: `/lookback <jam>`",
            reply_chat,
        )
        return
    try:
        v   = int(args[0])
        if not (1 <= v <= 24):
            send_reply("Sensei, harus antara 1 sampai 24 jam ya~ (｡･ω･｡)", reply_chat)
            return
        old = settings["lookback_hours"]
        settings["lookback_hours"] = v
        now = datetime.now(timezone.utc)
        for ps in pair_states.values():
            prune_pair_history(ps, now)
        send_reply(
            f"Baik, Sensei~ Lookback sudah Aku ubah dari *{old}h* jadi *{v}h*~\n\n"
            f"⚠️ History semua pair di-prune. "
            f"Data dari Bot A akan masuk saat refresh berikutnya~ ( ◕‿◕ )",
            reply_chat,
        )
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. (｡･ω･｡)", reply_chat)


def handle_heartbeat_command(args: list, reply_chat: str) -> None:
    if not args:
        send_reply(
            f"💓 Aku lapor setiap *{settings['heartbeat_minutes']} menit*, Sensei~\nUsage: `/heartbeat <menit>` atau `0` untuk matikan",
            reply_chat,
        )
        return
    try:
        v = int(args[0])
        if not (0 <= v <= 120):
            send_reply("Sensei, harus antara 0 sampai 120 menit ya~ (｡･ω･｡)", reply_chat)
            return
        settings["heartbeat_minutes"] = v
        msg = (
            "Baik, Sensei. Aku tidak akan ganggu lagi~ Tapi Aku tetap di sini memantau. ( ◕‿◕ )"
            if v == 0
            else f"Baik, Sensei~ Aku akan lapor setiap *{v} menit* ya. (｡･ω･｡)"
        )
        send_reply(msg, reply_chat)
    except ValueError:
        send_reply("Angkanya tidak valid, Sensei. (｡･ω･｡)", reply_chat)


def handle_status_command(reply_chat: str) -> None:
    lb         = get_lookback_label()
    pair_lines = ""
    for cfg in PAIR_CONFIGS:
        ps = pair_states.get(cfg.name)
        if ps is None:
            continue
        pts  = len(ps.price_history)
        hrs  = pts * settings["scan_interval"] / 3600
        ready = (
            f"✅ {hrs:.1f}h"
            if hrs >= settings["lookback_hours"]
            else f"⏳ {hrs:.1f}h/{settings['lookback_hours']}h"
        )
        last_gap = scan_stats["last_gaps"].get(cfg.name)
        gap_str  = f" | Gap: {format_value(last_gap)}%" if last_gap is not None else ""

        if ps.mode == Mode.SCAN:
            pair_lines += f"│ *{cfg.name}*: SCAN {ready}{gap_str}\n"

        elif ps.mode == Mode.PEAK_WATCH:
            strat    = ps.peak_strategy.value if ps.peak_strategy else "?"
            peak_str = f"{ps.peak_gap:+.2f}%" if ps.peak_gap else "?"
            pair_lines += (
                f"│ *{cfg.name}*: PEAK {strat} | Peak: {peak_str}{gap_str}\n"
                f"│   _{cfg.volatile_sym} lebih volatile_\n"
            )

        elif ps.mode == Mode.TRACK and ps.entry_gap_value is not None:
            strat = ps.active_strategy.value if ps.active_strategy else "?"
            et    = settings["exit_threshold"]
            sl    = settings["sl_pct"]
            best  = ps.trailing_gap_best or ps.entry_gap_value
            tp_g  = et  if ps.active_strategy == Strategy.S1 else -et
            tsl_g = best + sl if ps.active_strategy == Strategy.S1 else best - sl
            vol_target, _ = calc_tp_target_price(ps, ps.active_strategy)
            vol_str = f"${vol_target:,.2f}" if vol_target else "N/A"
            pair_lines += (
                f"│ *{cfg.name}*: TRACK {strat}{gap_str}\n"
                f"│   Entry:{ps.entry_gap_value:+.2f}% | TP:{tp_g:+.2f}%"
                f" | TSL:{tsl_g:+.2f}%\n"
                f"│   {cfg.volatile_sym} TP: {vol_str}\n"
            )

    send_reply(
        f"📊 *Status semua pair, Sensei~* ( ◕‿◕ )\n"
        f"\n"
        f"*Status Pair ({lb}):*\n"
        f"┌─────────────────────\n"
        f"{pair_lines}"
        f"└─────────────────────\n"
        f"\n"
        f"Entry: ±{settings['entry_threshold']}% | "
        f"Exit/TP: ±{settings['exit_threshold']}% | "
        f"TSL: {settings['sl_pct']}%\n"
        f"🔒 Redis: Read-Only",
        reply_chat,
    )


def handle_redis_command(reply_chat: str) -> None:
    if not UPSTASH_REDIS_URL:
        send_reply(
            "⚠️ Sensei, Redis belum dikonfigurasi.\n"
            "Pastikan `UPSTASH_REDIS_REST_URL` dan `UPSTASH_REDIS_REST_TOKEN` sudah diisi~ (｡･ω･｡)",
            reply_chat,
        )
        return

    lines = []
    for cfg in PAIR_CONFIGS:
        result = _redis_request("GET", f"/get/{cfg.redis_key}")
        if not result or result.get("result") is None:
            lines.append(f"│ *{cfg.name}*: ❌ Belum ada data")
            continue
        try:
            data         = json.loads(result["result"])
            pts          = len(data)
            hrs          = pts * settings["scan_interval"] / 3600
            lb           = settings["lookback_hours"]
            status       = "✅" if hrs >= lb else f"⏳ {hrs:.1f}h/{lb}h"
            last_ts      = data[-1]["timestamp"][-8:-1] if data else "-"
            lines.append(f"│ *{cfg.name}*: {status} ({pts} pts) | Last: `{last_ts}`")
        except Exception as e:
            lines.append(f"│ *{cfg.name}*: ⚠️ Error: {e}")

    content = "\n".join(lines)
    send_reply(
        f"⚡ *Status Redis (Read-Only) — Aku cek buat Sensei~* ( ◕‿◕ )\n"
        f"\n"
        f"┌─────────────────────\n"
        f"{content}\n"
        f"└─────────────────────\n"
        f"\n"
        f"_Bot B hanya baca, Bot A yang nulis~ ⚡_",
        reply_chat,
    )


def handle_help_command(reply_chat: str) -> None:
    send_reply(
        "Sensei mau tahu semua yang bisa Aku lakukan? ( ♡ 。-(｡･ω･) ♡)\n"
        "( ◕‿◕ )\n"
        "\n"
        "*Setting:*\n"
        "`/settings` — lihat semua settingan\n"
        "`/interval <detik>` — scan interval (60–3600)\n"
        "`/lookback <jam>` — periode lookback (1–24)\n"
        "`/heartbeat <menit>` — laporan rutin (0=off)\n"
        "`/threshold entry <val>` — entry threshold %\n"
        "`/threshold exit <val>` — exit % _(sekaligus jadi TP target)_\n"
        "`/threshold invalid <val>` — invalidation threshold %\n"
        "`/peak <val>` — reversal dari puncak untuk konfirmasi entry\n"
        "`/sltp` — lihat TP & TSL semua pair aktif\n"
        "`/sltp sl <val>` — ubah trailing SL distance %\n"
        "\n"
        "*Info:*\n"
        "`/status` — status semua pair sekaligus\n"
        "`/redis` — cek data history per pair dari Bot A\n"
        "`/help` — tampilkan pesan ini lagi\n"
        "\n"
        "💡 *Logika volatilitas:*\n"
        "│ BTC/ETH → ETH lebih volatile\n"
        "│ SOL/ETH → SOL lebih volatile\n"
        "│ BNB/BTC → BNB lebih volatile\n"
        "_SHORT selalu di asset yang lebih volatile!_\n"
        "\n"
        "💡 _TP dikunci ke exit threshold — ubah via `/threshold exit`_\n"
        "\n"
        "Sensei tidak perlu ragu minta bantuan. "
        "Aku selalu di sini~ ( ♡ 。-(｡･ω･) ♡)",
        reply_chat,
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
    logger.info("Monk Bot B Multi-Pair — BTC/ETH | SOL/ETH | BNB/BTC")
    logger.info(
        f"Entry: {settings['entry_threshold']}% | "
        f"Exit/TP: {settings['exit_threshold']}% | "
        f"Invalid: {settings['invalidation_threshold']}% | "
        f"Peak: {settings['peak_reversal']}% | "
        f"TSL: {settings['sl_pct']}% | "
        f"Redis refresh: {settings['redis_refresh_minutes']}m"
    )
    logger.info("=" * 60)

    # Init state per pair
    for cfg in PAIR_CONFIGS:
        pair_states[cfg.name] = PairState(cfg=cfg)

    # Load history semua pair dari Redis
    now = datetime.now(timezone.utc)
    for ps in pair_states.values():
        load_pair_history(ps)
        prune_pair_history(ps, now)
        ps.last_redis_refresh = now
        logger.info(
            f"[{ps.cfg.name}] Init: {len(ps.price_history)} points loaded"
        )

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener started")

    # Startup message
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        ap = fetch_all_prices()
        send_startup_message(ap)

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)

            # Heartbeat
            if should_send_heartbeat(now):
                if send_heartbeat():
                    last_heartbeat_time = now

            # Refresh Redis semua pair jika sudah waktunya
            for ps in pair_states.values():
                refresh_history_from_redis(ps, now)

            # Fetch semua harga dalam satu call
            ap = fetch_all_prices()
            if ap is None:
                logger.warning("Failed to fetch prices, skipping scan")
                time.sleep(settings["scan_interval"])
                continue

            scan_stats["count"] += 1
            scan_stats["last_prices"] = {
                "BTC": ap.btc, "ETH": ap.eth,
                "SOL": ap.sol, "BNB": ap.bnb,
            }

            # Evaluasi tiap pair secara independen
            for cfg in PAIR_CONFIGS:
                ps         = pair_states[cfg.name]
                stable_now = get_price_for_sym(ap, cfg.stable_sym)
                vol_now    = get_price_for_sym(ap, cfg.volatile_sym)
                s_upd      = get_updated_for_sym(ap, cfg.stable_sym)
                v_upd      = get_updated_for_sym(ap, cfg.volatile_sym)

                if not is_data_fresh(now, s_upd, v_upd):
                    logger.warning(f"[{cfg.name}] Data not fresh, skipping")
                    continue

                price_then = get_lookback_price(ps, now)
                if price_then is None:
                    hrs = len(ps.price_history) * settings["scan_interval"] / 3600
                    logger.info(
                        f"[{cfg.name}] Waiting for Bot A data... "
                        f"({hrs:.1f}h / {settings['lookback_hours']}h)"
                    )
                    continue

                stable_ret, vol_ret, gap = compute_returns(
                    stable_now, vol_now, price_then.stable, price_then.volatile
                )

                scan_stats["last_rets"][cfg.stable_sym]   = stable_ret
                scan_stats["last_rets"][cfg.volatile_sym] = vol_ret
                scan_stats["last_gaps"][cfg.name]         = gap

                logger.info(
                    f"[{cfg.name}] {cfg.stable_sym}: {format_value(stable_ret)}% | "
                    f"{cfg.volatile_sym}: {format_value(vol_ret)}% | "
                    f"Gap: {format_value(gap)}%"
                )

                prev_mode = ps.mode
                signaled  = evaluate_and_transition(
                    ps, stable_ret, vol_ret, gap,
                    stable_now, vol_now,
                    price_then.stable, price_then.volatile,
                )
                if signaled or ps.mode != prev_mode:
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
