# London Nervous System

**An autonomous AI that watches London through 100+ real-time data sources — detecting anomalies, finding hidden connections, and building an evolving understanding of the city.**

## What It Does

The London Nervous System is an autonomous AI system that monitors London as a living city in real time. It continuously ingests data from 100+ sources - TfL cameras, air quality sensors, energy grid status, satellite imagery, financial markets, news feeds, police data, and more - and uses a pipeline of specialized AI agents to detect anomalies, find cross-source patterns, and synthesise grounded insights about what's actually happening across the city. What makes it unique is its epistemic integrity model: every insight carries a confidence score that decays through each layer of inference, requiring multi-source corroboration before anything is surfaced as a genuine discovery.

To handle the sheer volume of city-wide data, it uses a neuroscience-inspired "Retina" system - a foveal attention model that divides London's spatial grid into attention zones (fovea, perifovea, periphery), allocating full processing resolution only to areas showing anomalous activity while compressing quiet zones. Anomalies in the periphery trigger "saccades" that instantly promote those cells to full attention, mirroring how the human eye rapidly shifts focus. This lets the system monitor all of London while concentrating compute where it matters most.

It also self-heals - a daemon watches for errors and autonomously spawns Claude Code instances to fix bugs and hot-reload without downtime. The Claude agents can also build new data ingestors allowing for constant growth.

The project started with 5 but now has 100+ data sources. The whole system runs as ~50 concurrent async tasks backed by SQLite, NetworkX spatial graphs, and Gemini for reasoning, producing a live dashboard of London's pulse.

Click the image below for a live demo ↓↓

[![Watch the demo](https://img.youtube.com/vi/FUwQOYofyfc/maxresdefault.jpg)](https://youtu.be/FUwQOYofyfc)

<img width="1789" height="938" alt="image" src="https://github.com/user-attachments/assets/c87be0ff-64b9-4dd7-951d-b7c055b45386" />
<img width="1790" height="689" alt="image" src="https://github.com/user-attachments/assets/5834837c-3618-47d5-901d-c51ca4fafee4" />


## Architecture

```
                         ┌─────────────────┐
                         │    64+ APIs &    │
                         │   Live Feeds     │
                         └────────┬────────┘
                                  │
                                  ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                        INGEST LAYER                           │
  │  TfL · LAQN · Sentinel · GDELT · yfinance · Met Office · ... │
  └───────────────────────────┬───────────────────────────────────┘
                              │ raw observations
                              ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                     RETINA (Attention)                        │
  │                                                               │
  │  ◉ Fovea ── full resolution, every observation analyzed       │
  │  ◎ Perifovea ── medium threshold                              │
  │  · Periphery ── compressed, but anomalies trigger saccades    │
  │                 that snap foveal attention to the area         │
  └───────────────────────────┬───────────────────────────────────┘
                              │ filtered + prioritized
                              ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                    INTELLIGENCE LAYER                         │
  │                                                               │
  │  Interpreters ──▶ Connectors ──▶ Brain (Gemini Pro)           │
  │  vision, numeric,   spatial,       synthesizes signals,       │
  │  text, financial    narrative,     epistemic grounding,       │
  │                     statistical,   generates discoveries      │
  │                     causal                                    │
  └──────────┬──────────────────────────────────┬─────────────────┘
             │                                  │
             ▼                                  ▼
  ┌─────────────────────┐           ┌─────────────────────┐
  │  Validator           │           │  Curiosity &         │
  │  backtests           │           │  Discovery           │
  │  predictions,        │           │  self-directed       │
  │  tracks reliability  │           │  investigation       │
  └─────────────────────┘           └─────────────────────┘
             │                                  │
             └──────────────┬───────────────────┘
                            ▼
  ┌───────────────────────────────────────────────────────────────┐
  │                      PERSISTENCE                              │
  │  SQLite + NetworkX graph (500m grid) + EMA baselines          │
  └───────────────────────────────────────────────────────────────┘

  ┌─────────────────────┐           ┌─────────────────────┐
  │  Chronicler          │           │  Daemon              │
  │  weaves multi-day    │           │  self-healing,       │
  │  narratives from     │           │  spawns Claude Code  │
  │  accumulated insight │           │  to fix failures     │
  └─────────────────────┘           └─────────────────────┘

  ┌───────────────────────────────────────────────────────────────┐
  │                       DASHBOARD                               │
  │  live log stream · interactive map · source health · images   │
  │                  http://localhost:8080                         │
  └───────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Ingestors** poll APIs and feeds → raw `Observation` objects stored in SQLite
2. **Retina** applies foveal attention — high-activity areas get full resolution processing, quiet areas are compressed. Anomalies in peripheral cells trigger "saccades" that snap attention to the area, just like how your eye works
3. **Interpreters** detect anomalies (z-score, vision analysis, text classification)
4. **Connectors** find spatial co-locations, narrative links, and statistical correlations
5. **Brain** synthesizes all signals through epistemic grounding → discoveries
6. **Validator** backtests predictions and tracks source reliability
7. **Curiosity** and **Discovery** agents self-direct investigations
8. **Chronicler** weaves multi-day narratives from accumulated insights

Communication flows through named channels: `#raw`, `#anomalies`, `#hypotheses`, `#discoveries`, `#meta`.

## Data Sources (64+ Categories)

| Category | Sources |
|----------|---------|
| **Traffic & Mobility** | TfL JamCam images, traffic speeds, station crowding, bus AVL, cycle hire, road disruptions, National Rail, Waze, TomTom, HERE |
| **Air Quality** | LAQN, PurpleAir, Breathe London, OpenAQ, Google AQ, Sensor.Community, openSenseMap |
| **Energy & Grid** | Carbon Intensity, grid generation/demand/frequency, UKPN substations, electricity prices, BMRS balancing |
| **Weather** | Open-Meteo forecasts, Met Office observations & warnings |
| **Financial** | yfinance (London-weighted stocks), CoinGecko crypto, Polymarket prediction markets |
| **News & Social** | GDELT, Guardian, BBC London, Bluesky, Mastodon, social sentiment analysis |
| **Satellite** | Sentinel-2 land use change, Sentinel-5P atmospheric composition |
| **Public Services** | Police crime data, NHS syndromic surveillance, Land Registry, Parliament |
| **Events & Culture** | Eventbrite, V&A Museum, Skiddle, Ticketmaster, GLA events |
| **Nature** | iNaturalist wildlife sightings, OpenSky flight tracker |
| **Infrastructure** | Thames Water overflows, Environment Agency floods, RIPE Atlas, NCSC cyber |

## Dashboard

The web dashboard (`static/dashboard.html`) provides:

- **Live log stream** — WebSocket-powered real-time feed of all agent activity
- **Interactive map** — Leaflet.js with anomaly markers on London's 500m grid
- **Source status cards** — health and freshness of each data source
- **Image viewer** — JamCam snapshots with lightbox and AI annotations
- **Stats bar** — message count, active anomalies, agent status

## Getting Started

```bash
# Clone the repo (must be named "london" for the Python package imports)
git clone https://github.com/dhruvsyam123/london-nervous-system.git london

# Create virtual environment
python -m venv london/venv
source london/venv/bin/activate

# Install dependencies
pip install -r london/requirements.txt

# Create .env with your API keys (only GEMINI_API_KEY is required)
echo "GEMINI_API_KEY=your_key_here" > london/.env

# Run from the parent directory (required for Python package resolution)
python -m london
```

Or use the auto-restarting wrapper (recommended for long-running operation):

```bash
cd london/
bash run_loop.sh
```

The SQLite database (`data/graph.db`) is created automatically on first run. The dashboard is served at `http://localhost:8080`.

### API Keys

Only `GEMINI_API_KEY` is required. All other keys are optional — ingestors with missing keys are silently skipped. See `core/config.py` for the full list of supported environment variables.

## Important Notes

**Stopping the system:**
- `Ctrl+C` once → graceful shutdown (waits for in-flight tasks to finish)
- `Ctrl+C` twice → force exit immediately
- If running via `run_loop.sh`, the shell loop may restart the process — close the terminal window to fully stop

**The system is autonomous:**
- The daemon (`core/daemon.py`) monitors system health and can spawn Claude Code CLI instances to automatically diagnose and fix failing components
- When Claude fixes code, the daemon auto-commits the changes and hot-reloads or restarts the system
- Restarts are rate-limited to once per 3 hours; rapid crash loops (10+) trigger a full stop
- To disable the daemon's auto-fix behavior, you would need to modify `core/daemon.py`

**API usage & costs:**
- 60+ ingestors poll APIs at intervals from 2 minutes to daily — this generates significant API traffic
- Most free-tier keys are fine, but keys with paid tiers (TomTom, Google AQ, HERE) may incur costs at scale
- Ingestors with missing keys are silently skipped — check the dashboard or logs to see which sources are active

**Data growth:**
- The SQLite database and image cache grow continuously as data is ingested
- Expect several GB after extended operation
- Images are auto-cleaned (thumbnails after 48h, full images after 7d)

## Project Structure

```
london/
├── run.py                 # Entry point (python -m london)
├── run_loop.sh            # Auto-restarting wrapper script
├── requirements.txt
├── ARCHITECTURE.md        # Detailed architecture docs
│
├── core/                  # Infrastructure
│   ├── board.py           # SQLite message board (7 tables)
│   ├── config.py          # Constants, intervals, rate limits
│   ├── coordinator.py     # Adaptive scheduler with event triggers
│   ├── daemon.py          # Self-healing watcher
│   ├── dashboard.py       # aiohttp WebSocket server
│   ├── epistemics.py      # Confidence decay & grounding
│   ├── graph.py           # NetworkX spatial graph (500m grid)
│   ├── image_store.py     # Two-tier image storage
│   ├── memory.py          # EMA baselines + JSON persistence
│   ├── models.py          # Observation, Anomaly, Connection
│   ├── retina.py          # Foveal attention zones (saccades)
│   └── scheduler.py       # Rate limiter
│
├── agents/                # Intelligence layer
│   ├── brain.py           # Gemini Pro synthesis (~5 min cycle)
│   ├── interpreters.py    # Vision, Numeric, Text, Financial
│   ├── connectors.py      # Spatial, Narrative, Statistical, Causal
│   ├── validator.py       # Prediction backtesting
│   ├── curiosity.py       # Self-directed investigation
│   ├── chronicler.py      # Multi-day narrative weaving
│   ├── discovery.py       # Cross-domain exploration
│   └── explorers.py       # On-demand hypothesis testing
│
├── ingestors/             # 64+ data source adapters
│   ├── base.py            # HTTP client, circuit breaker, retry
│   ├── tfl.py             # TfL JamCam images
│   ├── air_quality.py     # LAQN sensors
│   ├── satellite.py       # Sentinel-2 imagery
│   ├── financial.py       # Stocks, crypto, prediction markets
│   └── ...                # Many more
│
├── data/                  # Runtime data (gitignored)
│   ├── graph.db           # SQLite database
│   ├── images/            # JamCam snapshots
│   ├── memory/            # JSON persistence files
│   └── logs/              # Application logs
│
└── static/                # Web dashboard
    ├── dashboard.html     # Main UI
    └── ask.html           # Query interface
```

## Key Design Decisions

- **Foveal attention (Retina)** — Inspired by the human visual system. The city's ~4,000 grid cells are divided into three zones: **fovea** (full resolution, every observation stored and analyzed), **perifovea** (medium threshold), and **periphery** (compressed — raw observations dropped but z-scores still computed). When an anomaly fires in a peripheral cell, it triggers a "saccade" that promotes that cell and its neighbors to fovea, snapping the system's full attention to the area. Cells decay back to periphery after 30–60 minutes of quiet. This lets the system scale to 64+ sources without drowning in data.
- **Epistemic integrity** — Confidence decays at each processing stage (raw → interpreted → connected → narrated). Requires multi-source corroboration. Prevents the AI from hallucinating patterns.
- **Self-healing daemon** — Watches for failing components and spawns Claude Code instances to diagnose and fix issues autonomously, with hot-reload for most changes.
- **Adaptive scheduling** — Ingestors run on flexible intervals that speed up when anomalies are detected and slow down during quiet periods. Circuit breakers prevent cascade failures.
- **Spatial grounding** — All observations are mapped to a 500m grid covering Greater London (~4,000 cells), enabling spatial co-location detection across unrelated data sources.

## Built With

Python 3.11+ | asyncio | SQLite (aiosqlite) | NetworkX | Google Gemini (Flash + Pro) | Leaflet.js | aiohttp
