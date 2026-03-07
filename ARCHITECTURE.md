# London Nervous System — Architecture Guide

This document is for Claude Code instances spawned by the daemon.
Read this first before modifying any code.

## System Overview

An autonomous AI-driven city monitoring system that continuously ingests data
from 60+ diverse sources, detects anomalies, synthesizes multi-source insights,
and self-improves via LLM-powered agents. ~90 Python files, ~9,000 lines of code,
~50 concurrent async tasks.

```
  APIs (TfL, LAQN, GDELT, Open-Meteo, yfinance, Sentinel-2, ...)
       │
  ┌────▼────┐
  │INGESTORS│  64 async loops, each polls an API
  └────┬────┘  Posts Observations to DB + AgentMessages to #raw
       │
  ┌────▼────────┐
  │INTERPRETERS │  4 agents: Vision, Numeric, Text, Financial
  └────┬────────┘  Read #raw → detect anomalies → post to #anomalies
       │
  ┌────▼──────────┐
  │  CONNECTORS   │  4 agents: Spatial, Narrative, Statistical, CausalChain
  └────┬──────────┘  Read #anomalies → find cross-source patterns → #hypotheses
       │
  ┌────▼──────┐
  │ EXPLORERS │  Spawned on-demand to investigate medium-confidence hypotheses
  └────┬──────┘  Fetch fresh data from APIs → update hypothesis confidence
       │
  ┌────▼─────┐
  │  BRAIN   │  Gemini Pro synthesis every 5 min → #discoveries
  └────┬─────┘  Grounded by epistemic engine (confidence decay, severity caps)
       │
  ┌────▼──────────┐
  │  VALIDATOR    │  Checks predictions past deadline → updates source reliability
  └───────────────┘

  ┌─────────────────┐
  │  CURIOSITY      │  Self-directed learning: samples random data, finds patterns
  └─────────────────┘

  ┌─────────────────┐
  │  DISCOVERY      │  Cross-domain scan, chain extension, implication mining
  └─────────────────┘

  ┌─────────────────┐
  │  WEB SEARCHER   │  DuckDuckGo search for hypothesis verification
  └─────────────────┘

  ┌─────────────────┐
  │  CHRONICLER     │  Long-term narrative tracking across days/weeks
  └─────────────────┘

  ┌─────────────────┐
  │  COORDINATOR    │  Adaptive scheduling: interval ranges, anomaly speedup, backoff
  └─────────────────┘

  ┌─────────────────┐
  │    DAEMON       │  Persistent watcher: error detection → spawns Claude Code
  └─────────────────┘

  ┌─────────────────┐
  │  DASHBOARD      │  aiohttp WebSocket server, live log streaming, image viewer
  └─────────────────┘

  ┌─────────────────┐
  │ EPISTEMIC ENGINE│  Confidence decay, source tracking, grounding checks
  └─────────────────┘
```

## Directory Structure

```
london/
├── run.py                  # Entry point: python -m london.run
├── run_loop.sh             # Outer restart loop for autonomous operation
├── ARCHITECTURE.md         # This file
├── requirements.txt        # Python dependencies
├── __init__.py
├── __main__.py
├── core/
│   ├── board.py            # MessageBoard: SQLite hub (messages, observations, anomalies, connections, baselines, conversations, investigations)
│   ├── config.py           # All config constants, API endpoints, intervals (~130 interval configs)
│   ├── coordinator.py      # Adaptive scheduler with event-driven triggers
│   ├── daemon.py           # Persistent watcher, spawns Claude Code instances
│   ├── claude_runner.py    # Manages claude CLI subprocess spawning
│   ├── dashboard.py        # aiohttp web server + WebSocket log streaming
│   ├── epistemics.py       # Confidence decay, grounding checks, source tracking
│   ├── graph.py            # NetworkX graph: 500m grid (~4,000 cells), spatial index
│   ├── image_store.py      # Two-tier image storage: thumbs (160x120) + full (640x480)
│   ├── memory.py           # EMA baselines (working memory) + JSON files (long-term)
│   ├── models.py           # All dataclasses: Observation, Anomaly, Connection, etc.
│   ├── scheduler.py        # RateLimiter + AsyncScheduler (legacy, kept for compat)
│   └── utils.py            # Shared helpers (parse_json_response)
├── agents/
│   ├── base.py             # BaseAgent: Gemini calls, board helpers, rate tracking
│   ├── brain.py            # BrainAgent: Gemini Pro synthesis with epistemic grounding
│   ├── chronicler.py       # ChroniclerAgent: multi-day narrative tracking (hourly)
│   ├── connectors.py       # Spatial, Narrative, Statistical, CausalChain connectors
│   ├── curiosity.py        # CuriosityAgent: self-directed investigation (~10 min)
│   ├── discovery.py        # DiscoveryEngine: cross-domain scan, chain extension, implication mining
│   ├── explorers.py        # ExplorerSpawner + Explorer: hypothesis investigation
│   ├── interpreters.py     # Vision, Numeric, Text, Financial interpreters
│   ├── validator.py        # ValidatorAgent: prediction checking, source reliability updates
│   └── web_searcher.py     # WebSearchAgent: DuckDuckGo search for hypothesis verification
├── ingestors/
│   ├── base.py             # BaseIngestor: HTTP helpers, circuit breaker, retry logic
│   │
│   │  # ── Traffic & Mobility (15) ──
│   ├── tfl.py              # TfL JamCam images
│   ├── tfl_traffic.py      # TfL road traffic speeds
│   ├── tfl_crowding.py     # TfL station crowding
│   ├── tfl_bus_congestion.py  # TfL bus route congestion
│   ├── tfl_disruption_incidents.py  # TfL disruption incidents
│   ├── tfl_passenger_counts.py  # TfL passenger count data
│   ├── road_disruptions.py # TfL road disruptions
│   ├── planned_works.py    # TfL planned works
│   ├── bus_avl.py          # Bus real-time locations
│   ├── bus_crowding.py     # Bus crowding levels
│   ├── tube_arrivals.py    # Tube arrival predictions
│   ├── cycle_hire.py       # Santander cycle hire
│   ├── micromobility.py    # E-scooter/bike sharing
│   ├── national_rail.py    # National Rail data
│   ├── commuter_displacement.py  # Commuter pattern shifts
│   │
│   │  # ── Traffic Intelligence (3) ──
│   ├── rideshare_demand.py # Ride-sharing demand estimation
│   ├── waze_traffic.py     # Waze community traffic alerts
│   ├── tomtom_traffic.py   # TomTom traffic flow data
│   │
│   │  # ── Air Quality & Environment (9) ──
│   ├── air_quality.py      # LAQN air quality sensors
│   ├── laqn_transport.py   # LAQN transport-corridor readings
│   ├── purpleair.py        # PurpleAir community sensors
│   ├── google_air_quality.py  # Google Air Quality API
│   ├── opensensemap_mobile.py # openSenseMap mobile sensors
│   ├── openaq.py           # OpenAQ global air quality
│   ├── sensor_community.py # Sensor.Community air sensors
│   ├── environment.py      # Environment Agency flood monitoring
│   ├── river_flow.py       # River flow gauges
│   │
│   │  # ── Weather (1) ──
│   ├── weather.py          # Open-Meteo weather forecasts
│   │
│   │  # ── Energy & Grid (6) ──
│   ├── energy.py           # Carbon Intensity API
│   ├── grid_generation.py  # Grid generation mix
│   ├── grid_demand.py      # Grid demand levels
│   ├── grid_status.py      # Grid operational status
│   ├── grid_forecast.py    # Grid demand forecast
│   ├── embedded_generation.py  # Embedded solar/wind generation
│   ├── ukpn_substation.py  # UKPN substation data
│   │
│   │  # ── Financial (1) ──
│   ├── financial.py        # yfinance stocks + CoinGecko crypto + Polymarket
│   │
│   │  # ── News & Social (4) ──
│   ├── news.py             # GDELT news events
│   ├── social_sentiment.py # Social media sentiment
│   ├── bluesky_transport.py # Bluesky transport posts
│   ├── wikidata.py         # Wikidata London entities
│   │
│   │  # ── Satellite & Imagery (3) ──
│   ├── satellite.py        # Planetary Computer Sentinel-2 (land use)
│   ├── sentinel5p.py       # Sentinel-5P atmospheric (NO2, SO2)
│   ├── tfl_digital_health.py  # TfL digital service health
│   │
│   │  # ── Events & Culture (7) ──
│   ├── events.py           # Eventbrite London events
│   ├── vam_museum.py       # V&A Museum collection
│   ├── uk_parliament.py    # UK Parliament data
│   ├── companies_house.py  # Companies House filings
│   ├── historic_england.py # Historic England heritage
│   ├── open_plaques.py     # Open Plaques blue plaques
│   ├── tv_schedule.py      # TV schedule data
│   │
│   │  # ── Public Services (3) ──
│   ├── police_crimes.py    # Police crime data
│   ├── nhs_syndromic.py    # NHS syndromic surveillance
│   ├── land_registry.py    # Land Registry price data
│   │
│   │  # ── Nature & Wildlife (2) ──
│   ├── nature.py           # iNaturalist wildlife
│   ├── misc.py             # Wikipedia pageviews + OSM
│   │
│   │  # ── System Health (6) ──
│   ├── system_health.py    # Internal system metrics
│   ├── sensor_health.py    # Sensor uptime tracking
│   ├── provider_status.py  # API provider status pages
│   ├── data_lineage.py     # Data provenance tracking
│   ├── data_quality.py     # Data quality scoring
│   ├── apm.py              # Application performance monitoring
│   └── retail_spending.py  # Retail spending indicators
│
├── data/
│   ├── graph.db            # SQLite database (WAL mode)
│   ├── images/
│   │   ├── thumbs/         # 160x120 JPEG thumbnails (cleaned >48h)
│   │   └── full/           # 640x480 JPEG full-size (cleaned >7d)
│   ├── memory/             # JSON memory files
│   │   ├── system_meta.json          # System start times
│   │   ├── source_reliability.json   # Per-source EMA reliability scores
│   │   ├── daemon_actions.json       # Daemon action history
│   │   ├── brain_narrative_memory.json
│   │   ├── connection_registry.json
│   │   ├── learned_correlations.json
│   │   ├── evolving_narratives.json
│   │   ├── backtesting_results.json
│   │   └── camera_scores.json
│   └── logs/
│       ├── london.log              # Main log (JSON lines)
│       └── claude_runs/            # Per-instance Claude Code logs
└── static/
    └── london_grid.json    # Cached 500m grid cell definitions
```

## Data Flow: Observation Lifecycle

1. **Ingestor** polls API → creates `Observation` → stores in DB → posts `AgentMessage` to `#raw`
2. **Interpreter** reads `#raw` → runs analysis (LLM or stats) → if anomalous, creates `Anomaly` → stores in DB → posts to `#anomalies`
3. **SpatialConnector** reads `#anomalies` → groups by location + neighbors → if different sources co-located, posts hypothesis to `#hypotheses`
4. **NarrativeConnector** reads `#hypotheses` (spatial) → Gemini Pro narrative → posts enriched hypothesis to `#hypotheses` with prediction
5. **ExplorerSpawner** reads `#hypotheses` (medium confidence 0.3-0.8) → spawns Explorer tasks → posts findings back to `#hypotheses`
6. **Brain** reads `#anomalies`, `#hypotheses`, `#discoveries` → runs epistemic grounding → clusters anomalies → filters suppressed → Gemini Pro synthesis → posts to `#discoveries`
7. **Validator** checks past-deadline predictions → gathers evidence → Gemini Flash evaluation → updates source reliability
8. **Curiosity** samples random data sources/grid cells → Gemini Flash analysis → spawns investigation threads
9. **Discovery** runs cross-domain scan / chain extension / implication mining → posts connections
10. **WebSearcher** picks up investigation threads → DuckDuckGo search → posts findings
11. **Chronicler** reads discoveries + investigations → maintains evolving multi-day narratives → weekly summaries

### Channels

| Channel | Purpose |
|---------|---------|
| `#raw` | Raw sensor data from ingestors |
| `#anomalies` | Detected anomalies from interpreters |
| `#hypotheses` | Cross-source patterns, narratives, explorer findings |
| `#requests` | Questions and investigation requests |
| `#discoveries` | Brain synthesis output |
| `#meta` | System health, accuracy metrics, daemon actions |
| `#conversations` | LLM call logs |

## Key Classes

### MessageBoard (`core/board.py`)
SQLite-backed hub. WAL mode, 10s busy timeout, auto-commit every 2s.
Tables: messages, observations, anomalies, connections, baselines, conversations, investigations.
Key methods: `post()`, `read_channel()`, `store_observation()`, `store_anomaly()`, `store_connection()`, `get_pending_predictions()`.
Auto-reconnect on failure via `_ensure_connection()`.

### MemoryManager (`core/memory.py`)
Working memory: EMA baselines keyed by `(source, location_id, metric)`. α=0.05. Warmup phase (first 5 samples) uses running average. Returns z-scores.
Long-term: JSON files in `data/memory/` (source_reliability, learned_correlations, connection_registry, evolving_narratives).
**Baseline persistence**: `persist_baselines(db)` saves to SQLite every 5 min, `restore_baselines(db)` loads on startup. Baselines survive restarts.

### LondonGraph (`core/graph.py`)
NetworkX DiGraph with 500m grid cells (~4,000 cells covering 51.28-51.70 lat, -0.51 to 0.33 lon).
`latlon_to_cell()` for O(1) spatial lookup. Spatial anomaly index (dict-based) for co-location detection.
`prune_expired_anomalies()` for TTL cleanup.

### Coordinator (`core/coordinator.py`)
Adaptive scheduler. Tasks have `min_interval` to `max_interval` ranges.
- `request_data(source)` — force immediate data fetch (sets interval to min)
- `notify_anomaly(source, severity)` — speed up related ingestors
- Error backoff: exponential backoff up to 3x max_interval on consecutive errors
- Quiet periods: intervals drift back toward max_interval
- Startup probes: tests all ingestors before main loop; blacklists failures for 30 min
- Event queue: max 500 events (FIFO)

### Daemon (`core/daemon.py`)
Persistent async coroutine (NOT periodic). Watches:
- Board channels for self-improvement suggestions, accuracy drops
- Coordinator health for consecutive errors, task staleness, empty data
- Log file for error patterns (KeyError, AttributeError, etc.)
Spawns Claude Code instances (`core/claude_runner.py`) to auto-fix issues. Max 3 concurrent.
On success: detects git changes → auto-commits → hot-reloads or full restarts.

### Dashboard (`core/dashboard.py`)
aiohttp web server with WebSocket log streaming. Serves image thumbnails and full-size images.
Live view of system activity.

### ImageStore (`core/image_store.py`)
Two-tier storage: thumbnails (160x120 JPEG q=30, cleaned >48h) and full-size (640x480 JPEG q=60, cleaned >7d).

### BaseIngestor (`ingestors/base.py`)
HTTP helpers with retry logic. Per-source circuit breaker:
- 3 consecutive failures → circuit opens for 5 min
- Half-open state retries once
- Cooldown increases exponentially (max 1 hour)
- Circuit breaker state stored in `sys.modules` (survives hot-reload)

### BaseAgent (`agents/base.py`)
Gemini LLM integration via `call_gemini()`. Board/channel helpers.
Rate tracking for LLM calls. Agents are stateless — new instance created per `run()` call.

## Epistemic Engine (`core/epistemics.py`)

Prevents the system from hallucinating connections. All code logic, no LLM.

### Confidence Flow
```
raw (1.0) → interpreted (0.85) → connected (0.70) → narrated (0.55) → synthesized (0.45)
```

### Source Multiplier
- 1 source: 0.3x (single-source anomalies get heavy discount)
- 2 sources: 0.65x
- 3+ sources: 1.0x (no discount)

### System Maturity
Ramps 0→1 over first 24h. Young system caps confidence at 0.3.

### GroundingCheck
Produces a `GroundingReport` with hard facts:
- `should_suppress`: True if single-source + mundane explanation, or base_rate > 30%
- `max_expressible_severity`: 1 source → cap at 2, 2 → cap at 3, 3+ → up to 5
- `as_context_for_llm()`: injects facts into prompt so LLM can't dramatize
- Detects mundane explanations (camera offline, scheduled maintenance) via `_MUNDANE_RULES`

### SourceTracker
EMA reliability per source, updated by validator outcomes. Persisted to `source_reliability.json`.

## Agent Reference

| Agent | Module | Cycle | LLM | Purpose |
|-------|--------|-------|-----|---------|
| VisionInterpreter | interpreters.py | ~2 min | Gemini Flash | JamCam image analysis |
| NumericInterpreter | interpreters.py | ~2 min | — | Z-score anomaly detection |
| TextInterpreter | interpreters.py | ~5 min | Gemini Flash | News classification |
| FinancialInterpreter | interpreters.py | ~5 min | Gemini Flash | Market outlier detection |
| SpatialConnector | connectors.py | ~3 min | — | Co-location grouping |
| NarrativeConnector | connectors.py | ~5 min | Gemini Pro | Narrative synthesis |
| StatisticalConnector | connectors.py | ~60 min | — | Cross-correlation analysis |
| CausalChainConnector | connectors.py | ~10 min | — | Template-based causal reasoning |
| ExplorerSpawner | explorers.py | ~5 min | Gemini Flash | Spawns investigation tasks |
| Brain | brain.py | ~5 min | Gemini Pro | Main synthesis engine |
| Validator | validator.py | ~30 min | Gemini Flash | Prediction checking |
| Curiosity | curiosity.py | ~10 min | Gemini Flash | Self-directed learning |
| Discovery | discovery.py | ~15 min | Gemini Flash | Cross-domain exploration |
| WebSearcher | web_searcher.py | on-demand | Gemini Flash | DuckDuckGo verification |
| Chronicler | chronicler.py | ~60 min | Gemini Pro | Multi-day narratives |

## Ingestor Reference (64 sources)

| Category | Count | Sources |
|----------|-------|---------|
| Traffic & Mobility | 18 | TfL JamCam, TfL traffic speeds, station crowding, bus AVL, bus crowding, bus congestion, tube arrivals, cycle hire, micromobility, road disruptions, planned works, disruption incidents, passenger counts, national rail, commuter displacement, rideshare demand, Waze, TomTom |
| Air Quality & Environment | 9 | LAQN, LAQN transport, PurpleAir, Google AQ, openSenseMap, OpenAQ, Sensor.Community, Environment Agency floods, river flow |
| Energy & Grid | 7 | Carbon intensity, grid generation, grid demand, grid status, grid forecast, embedded generation, UKPN substations |
| Weather | 1 | Open-Meteo |
| Financial | 1 | yfinance + CoinGecko + Polymarket |
| News & Social | 4 | GDELT, social sentiment, Bluesky transport, Wikidata |
| Satellite & Imagery | 3 | Sentinel-2 (land use), Sentinel-5P (atmospheric), TfL digital health |
| Events & Culture | 7 | Eventbrite, V&A Museum, UK Parliament, Companies House, Historic England, Open Plaques, TV schedule |
| Public Services | 3 | Police crimes, NHS syndromic, Land Registry |
| Nature & Wildlife | 2 | iNaturalist, Wikipedia/OSM |
| System Health | 6 | System health, sensor health, provider status, data lineage, data quality, APM |
| Other | 1 | Retail spending |

## How to Add an Ingestor

1. Create `london/ingestors/my_source.py`
2. Define async function `async def ingest_my_source(board, graph, scheduler):`
3. Inside: fetch API data, create `Observation` objects, call `board.store_observation()`, post to `#raw`
4. Register in `london/run.py`: `coordinator.register("ingest_my_source", ingest_my_source, ...)`
5. Add API endpoint to `core/config.py` ENDPOINTS dict
6. Add any API keys to `.env.example`

Pattern (from `air_quality.py`):
```python
async def ingest_my_source(board, graph, scheduler):
    async with aiohttp.ClientSession() as session:
        async with session.get(URL) as resp:
            data = await resp.json()
    for item in data:
        obs = Observation(source="my_source", obs_type=ObservationType.NUMERIC, value=item["value"], ...)
        await board.store_observation(obs)
        await board.post(AgentMessage(from_agent="ingestor:my_source", channel="#raw", content=..., data=...))
```

## How to Add an Agent

1. Create class inheriting from `BaseAgent` in `london/agents/`
2. Set `name = "my_agent"`
3. Implement `async def run(self):`
4. Use `self.call_gemini()` for LLM, `self.read_channel()` and `self.post()` for board
5. Create entry point: `async def run_my_agent(board, graph, memory): ...`
6. Register in `run.py`

## Debugging Guide

### Where are the logs?
- `london/data/logs/london.log` — JSON-line log, all levels
- `london/data/logs/claude_runs/` — per-instance Claude Code logs

### How to check baselines
```python
# In Python:
from london.core.memory import MemoryManager
m = MemoryManager()
print(m._baselines)  # in-memory baselines

# In SQLite:
sqlite3 london/data/graph.db "SELECT * FROM baselines LIMIT 10;"
```

### How to read the DB
```bash
sqlite3 london/data/graph.db
.tables                          # list tables
SELECT COUNT(*) FROM anomalies;  # anomaly count
SELECT * FROM connections WHERE prediction_outcome IS NOT NULL LIMIT 5;  # checked predictions
SELECT * FROM baselines LIMIT 10;  # persisted baselines
```

### How to check source reliability
```bash
cat london/data/memory/source_reliability.json
```

### How to view the dashboard
The dashboard runs on the port configured in `core/config.py`. Open in browser for live WebSocket log streaming and image viewing.

## Code Change Propagation — Full Flow

The system can update its own code (via the daemon spawning Claude instances) and
propagate those changes into the running process without losing state. This section
documents exactly how that works, edge cases, and failure modes.

### The Players

```
┌─────────────┐    spawns     ┌─────────────┐    modifies     ┌───────────┐
│   DAEMON    │──────────────▶│ CLAUDE CLI  │───────────────▶│ .py files │
│ (async task)│               │ (subprocess)│                └─────┬─────┘
└──────┬──────┘               └──────┬──────┘                      │
       │ polls every 10s              │ returncode=0                │
       │                              │                            │
       ▼                              ▼                            ▼
  _check_completed_instances()    instance.completed_at set    git diff HEAD
       │                                                           │
       ▼                                                           ▼
  _get_changed_py_files() ◀─── git diff --name-only HEAD ─── changed .py list
       │
       ▼
  _git_auto_commit()  ─── git add + git commit
       │
       ▼
  _hot_reload(changed_files)  ─── importlib.reload + coordinator.restart_task
       │
       ▼
  OR _request_restart()  ─── SIGINT + sentinel file ─── run_loop.sh restarts
```

### Lifecycle of a Code Change

#### 1. Trigger: Claude Instance Completes

The daemon polls `_check_completed_instances()` every 10 seconds. It checks if any
Claude CLI subprocess has finished (`instance.completed_at is not None`) with
`return_code == 0` (success).

#### 2. Detect: What Files Changed?

`_get_changed_py_files()` runs two git commands:
- `git diff --name-only HEAD` → tracked files modified since last commit
- `git ls-files --others --exclude-standard` → new untracked files

Only `.py` files under `london/` are considered. This is the source of truth —
it doesn't rely on Claude's output or guessing.

#### 3. Commit: Auto-Commit Before Reload

`_git_auto_commit()` stages and commits the changed files. This is important because:
- It creates a clean HEAD for the next change detection cycle
- It preserves the change history
- If the change breaks things, it can be reverted

#### 4. Decision: Hot-Reload vs Full Restart

**Full restart** (via `_request_restart()`) is triggered for core infrastructure:
- `core/board.py` — SQLite schema/methods, deeply wired
- `core/coordinator.py` — task scheduling, can't reload while running
- `core/scheduler.py` — rate limiter
- `core/models.py` — dataclass definitions used everywhere
- `run.py` — entry point, import wiring

Full restart: writes `.restart` sentinel file → sends `SIGINT` → `run_loop.sh`
sees the sentinel → restarts `python3 -m london.run` after 3s.

**Hot-reload** (via `_hot_reload()`) handles everything else:
- Agent modules (`agents/*.py`)
- Ingestor modules (`ingestors/*.py`)
- Core utilities (`core/config.py`, `core/memory.py`, `core/utils.py`, etc.)

### Hot-Reload: Detailed Mechanics

#### Reload Order Matters

Python's `importlib.reload()` re-executes a module's source code, creating new
class/function objects. But if module B imports from module A, reloading B will
pick up whatever version of A is currently in `sys.modules`. So:

```
1. Core utilities (config, memory, utils, image_store, epistemics)  ← reload first
2. agents/base.py                                                    ← if changed
3. All agent modules (they inherit from BaseAgent)                   ← reload after base
4. Ingestor modules                                                  ← independent
```

If `base.py` changes, ALL agent modules must be reloaded because they inherit
from `BaseAgent`. Reloading only `brain.py` after `base.py` changed would create
a `BrainAgent` class that inherits from the NEW `BaseAgent` class, but other agents
would still inherit from the OLD one — an inconsistency.

#### Updating Function References (The Critical Step)

This is the subtlety that makes hot-reload actually work. The coordinator holds
`TaskEntry` objects, each with a `func` attribute pointing to a function like
`run_brain`. When we `importlib.reload(london.agents.brain)`, a NEW `run_brain`
function is created in the module — but `TaskEntry.func` still points to the OLD one.

So after reload, we:
1. Look up the reloaded module in `sys.modules`
2. Grab the new function via `getattr(module, "run_brain")`
3. Pass it to `coordinator.restart_task("brain", new_func=run_brain_NEW)`
4. The coordinator updates `entry.func = new_func` before creating the new asyncio.Task

Without this step, restarting a task would run the old code.

#### What Gets Preserved Across Hot-Reload

**Preserved** (shared objects passed as args):
- `MessageBoard` instance (SQLite connection, all data)
- `LondonGraph` instance (grid cells, spatial index)
- `MemoryManager` instance (EMA baselines, JSON memory)
- Coordinator state (all other tasks keep running, event queue intact)
- Daemon state (action history, known anomaly types)
- Investigation threads (in SQLite)

**Reset** (per-task):
- `consecutive_errors` → 0
- `current_interval` → `max_interval` (starts slow)
- Any in-memory state inside the agent class (new instance created each `run()`)

**NOT preserved** (if the class stored state between runs):
- Agent instances are ephemeral — `run_brain()` creates a new `BrainAgent()` each call
- This is intentional: stateless agents are reload-safe

#### What Can Go Wrong

| Failure | Impact | Recovery |
|---------|--------|----------|
| `importlib.reload()` raises (syntax error, import error) | Module NOT updated. Task continues with old code. | Fix the bug, next Claude instance will try again. |
| `base.py` reload fails | All agent reloads skipped. Falls back to full restart. | `_request_restart()` called automatically. |
| New code crashes on first `run()` | Task enters error backoff (exponential). Daemon detects consecutive errors → spawns Claude to fix. | Self-healing: daemon spawns Claude to investigate. |
| Core file changed (board.py, etc.) | Hot-reload skipped entirely. Full restart triggered. | `run_loop.sh` restarts the process. |
| `run_loop.sh` not running (manual `python -m london.run`) | `_request_restart()` sends SIGINT, process exits, nobody restarts it. | Always use `run_loop.sh` for autonomous operation. |
| 3 consecutive fast crashes (<10s uptime) | `run_loop.sh` aborts the loop. | Manual intervention required. Fix the broken code. |

### run_loop.sh: The Outer Loop

```bash
london/run_loop.sh
```

This is the outermost layer. It runs `python3 -m london.run` in a loop:
- If process exits AND `.restart` sentinel exists → restart after 3s
- If process crashes in <10s → increment crash counter. 3 in a row → abort
- If process exits normally (no sentinel) → stop the loop
- `Ctrl+C` once → stops Python, `Ctrl+C` twice → stops the loop

### State Preservation Through Restarts

Even during a **full restart**, most state survives:

| State | Storage | Survives restart? |
|-------|---------|-------------------|
| Observations, messages, anomalies | SQLite `graph.db` | Yes |
| EMA baselines | SQLite `baselines` table | Yes (restored on startup) |
| Investigation threads | SQLite `investigations` table | Yes |
| Evolving narratives | `data/memory/evolving_narratives.json` | Yes |
| Source reliability | `data/memory/source_reliability.json` | Yes |
| Daemon action history | `data/memory/daemon_actions.json` | Yes |
| In-memory working state | Python objects | No (rebuilt from DB) |
| Coordinator event queue | In-memory deque | No (events lost) |
| Active asyncio tasks | In-memory | No (restarted from scratch) |

### Typical Scenarios

**Scenario 1: Claude fixes a failing ingestor**
1. `ingest_tfl` fails 3 times → daemon spawns Claude
2. Claude edits `london/ingestors/tfl.py`, exits with rc=0
3. Daemon detects change, auto-commits, hot-reloads `london.ingestors.tfl`
4. Coordinator restarts `ingest_tfl` with new `ingest_tfl` function
5. All other 50+ tasks keep running uninterrupted

**Scenario 2: Claude modifies BaseAgent**
1. Brain suggests improvement → daemon spawns Claude
2. Claude edits `london/agents/base.py` (adds new helper method)
3. Daemon reloads `base.py` first, then ALL 10 agent modules
4. All agent tasks restarted with new function references
5. Ingestors unaffected (they don't use BaseAgent)

**Scenario 3: Claude changes board.py (core infrastructure)**
1. Claude edits `london/core/board.py` (adds new table)
2. Daemon detects core file → triggers full restart via `_request_restart()`
3. SIGINT → graceful shutdown (baselines persisted)
4. `run_loop.sh` sees sentinel file → restarts after 3s
5. New process initializes fresh, restores all state from DB

**Scenario 4: Claude introduces a syntax error**
1. Claude writes bad code to `london/agents/brain.py`
2. `importlib.reload()` raises `SyntaxError`
3. Daemon logs warning, brain task continues with OLD code
4. Nothing crashes — old code is still valid
5. Next daemon cycle may spawn Claude again to fix the fix


## Common Issues

### Camera offline patterns
TfL cameras go offline frequently for maintenance. The epistemic engine's `_MUNDANE_RULES` has patterns for "offline", "unavailable", etc. These are suppressed by `GroundingCheck.should_suppress`. If too many are getting through, add more patterns to `_MUNDANE_RULES` in `epistemics.py`.

### API rate limits
Rate limits configured in `core/config.py` `RateLimitConfig`. The `RateLimiter` in `scheduler.py` enforces token-bucket limits. If hitting rate limits, increase intervals or reduce concurrent tasks.

### Baselines reset
Baselines are now persisted to SQLite every 5 minutes and restored on startup. If z-scores seem wrong after restart, check `SELECT COUNT(*) FROM baselines;` — should be > 0.

### Sensationalized discoveries
The epistemic engine should prevent this. Check:
1. Is `GroundingCheck` running? Look for "[Brain] Suppressed cluster" in logs
2. Are severity caps working? Single-source anomalies should be max severity 2
3. Is confidence decaying? Check discovery `confidence` values — should be < 0.5 for single-source

### SQLite contention
With 50+ concurrent writers, SQLite WAL mode helps but may still see `database is locked` under heavy load. The board has 10s busy timeout and auto-reconnect. If persistent, check `_ensure_connection()` logs.
