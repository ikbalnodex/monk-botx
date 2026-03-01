# Monk Bot - BTC/ETH Divergence Alert Bot

A lightweight Python bot that monitors BTC/ETH price divergence and sends Telegram alerts for trading signals. Features **peak detection** for optimal entry timing.

## Strategy

The bot implements a pairs trading strategy based on BTC/ETH relative performance over a configurable lookback period (default: 24 hours).

**Strategy 1 (S1): Long BTC / Short ETH**
- Triggers when ETH pumps more than BTC (gap >= +2%)
- ETH outperforming = expect reversion

**Strategy 2 (S2): Long ETH / Short BTC**
- Triggers when ETH dumps more than BTC (gap <= -2%)
- ETH underperforming = expect reversion

### How It Works

1. Bot scans BTC and ETH mark prices every 3 minutes
2. Calculates rolling % change over the lookback period (1h-24h)
3. Gap = ETH % change - BTC % change
4. If gap exceeds threshold, bot enters **Peak Watch** mode
5. Bot waits for gap to peak and reverse before sending entry signal

### Peak Detection

Instead of entering immediately when the gap crosses the threshold, the bot waits for the gap to peak and start reversing — this gives a more optimal entry point.

```
Gap
 |
3.5% -------- PEAK (bot tracks this)
3.2%              \
3.1%               ← ENTRY triggered here (dropped 0.4% from peak)
2.0% --- entry threshold
 |
 └─────────────── time →
```

**Flow:**
1. Gap crosses entry threshold → `PEAK WATCH` (bot monitors, no entry yet)
2. Gap keeps rising → bot updates peak internally
3. Gap drops `peak_reversal`% from peak → `ENTRY SIGNAL` sent
4. Gap retreats below threshold before confirming → `PEAK CANCELLED`

### Signal Types

| Signal | Condition | Action |
|--------|-----------|--------|
| **PEAK WATCH** | Gap crosses ±2.0% | Monitoring for peak reversal |
| **S1 ENTRY** | Gap reverses from peak (S1) | Long BTC / Short ETH |
| **S2 ENTRY** | Gap reverses from peak (S2) | Long ETH / Short BTC |
| **EXIT** | Gap returns to ±0.5% | Close positions |
| **INVALIDATION** | Gap exceeds ±5.0% | Stop loss |
| **PEAK CANCELLED** | Gap retreats before confirming | Back to scan mode |

## Telegram Commands

Control the bot directly from Telegram by messaging it:

| Command | Description |
|---------|-------------|
| `/settings` | View current settings |
| `/lookback <hours>` | Set lookback period (1-24h) |
| `/interval <seconds>` | Set scan interval (60-3600s) |
| `/heartbeat <minutes>` | Set status update interval (0 to disable) |
| `/threshold entry <val>` | Set entry threshold % |
| `/threshold exit <val>` | Set exit threshold % |
| `/threshold invalid <val>` | Set invalidation threshold % |
| `/peak <val>` | Set peak reversal % to confirm entry |
| `/status` | View bot status and data collection progress |
| `/help` | Show all commands |

Commands respond instantly using Telegram long polling.

The bot sends a heartbeat message every 30 minutes (configurable) with a summary of scans performed, current prices, and % change.

## Requirements

- Python 3.10+
- Linux server (Ubuntu 22.04/24.04 recommended) or cloud platform (Railway, Fly.io, Oracle Cloud)
- Telegram bot token

## Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/WooBackBaby/monk-bot.git
cd monk-bot
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment

```bash
cp env.template .env
nano .env
```

Add your Telegram credentials:
```
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
LOG_LEVEL=INFO
```

**Getting Telegram credentials:**
1. Create a bot via [@BotFather](https://t.me/botfather) → get token
2. Message [@userinfobot](https://t.me/userinfobot) → get chat ID
3. Start your bot in Telegram before running

### 4. Test Run

```bash
source venv/bin/activate
export $(cat .env | xargs)
python bot.py
```

## Deploy to Railway (Recommended)

Railway is the easiest way to run this bot 24/7 for free.

1. Fork this repo to your GitHub account
2. Go to [railway.app](https://railway.app) → Login with GitHub
3. New Project → Deploy from GitHub repo → select `monk-bot`
4. Go to service → **Variables** tab → add:
   - `TELEGRAM_BOT_TOKEN` = your token
   - `TELEGRAM_CHAT_ID` = your chat ID
   - `LOG_LEVEL` = `INFO`
5. Go to **Settings** → Start Command → set `python bot.py`
6. Redeploy and check **Logs** tab

## Production Deployment (Ubuntu)

### Automated Install

```bash
sudo ./deploy/install.sh
sudo nano /opt/monk_bot/.env  # Add credentials
sudo systemctl start omni_pairs_bot
sudo systemctl enable omni_pairs_bot
```

### Manual Install

See [detailed deployment guide](deploy/README.md) for step-by-step instructions with security hardening.

### Check Status

```bash
sudo systemctl status omni_pairs_bot
sudo journalctl -u omni_pairs_bot -f
```

## Configuration

Default settings (all configurable via Telegram commands):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lookback_hours` | 24 | Lookback period for % change (1-24h) |
| `scan_interval` | 180 | Time between scans in seconds (3 min) |
| `heartbeat_minutes` | 30 | Status update interval (0 to disable) |
| `entry_threshold` | 2.0 | Gap % to start peak watch |
| `exit_threshold` | 0.5 | Gap % to trigger exit |
| `invalidation_threshold` | 5.0 | Gap % for stop loss |
| `peak_reversal` | 0.3 | Gap must drop this % from peak to confirm entry |

**Quick Testing Setup** (faster signals for testing):
```
/lookback 1
/threshold entry 0.5
/peak 0.2
```

**Optimal Trading Setup:**
```
/lookback 24
/threshold entry 2.0
/threshold exit 0.5
/threshold invalid 5.0
/peak 0.3
```

**Note:** With 24h lookback (default), the bot needs ~24 hours of data collection before sending signals. Use `/lookback 1` for faster startup with 1-hour timeframe.

## API

Uses [Variational Omni API](https://docs.variational.io/technical-documentation/api) for price data:
- Endpoint: `GET /metadata/stats`
- Rate limit: 10 req/10s per IP
- No API key required

## Contributing

Pull requests welcome! Please open an issue first to discuss changes.

## License

MIT License - see [LICENSE](LICENSE) file.

## Disclaimer

This bot is for informational purposes only. Not financial advice. Trade at your own risk. :}
