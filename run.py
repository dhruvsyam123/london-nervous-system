"""Main entry point: python -m london.run — launches all agents concurrently."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the london/ directory
load_dotenv(Path(__file__).resolve().parent / ".env")

from .core.board import MessageBoard
from .core.config import INTERVALS, RATE_LIMITS, LOG_DIR, MEMORY_DIR
from .core.coordinator import Coordinator
from .core.dashboard import DashboardServer
from .core.graph import LondonGraph
from .core.memory import MemoryManager
from .core.scheduler import AsyncScheduler

# ── Logging Setup ──────────────────────────────────────────────────────────────

_dashboard = DashboardServer()


def setup_logging() -> None:
    root = logging.getLogger("london")
    # Avoid adding duplicate handlers on auto-restart
    if root.handlers:
        return
    fmt = logging.Formatter(
        '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    console.setLevel(logging.INFO)
    file_h = logging.FileHandler(LOG_DIR / "london.log")
    file_h.setFormatter(fmt)
    file_h.setLevel(logging.DEBUG)

    # Dashboard WebSocket handler
    _dashboard.log_handler.setLevel(logging.INFO)

    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_h)
    root.addHandler(_dashboard.log_handler)


log = logging.getLogger("london.run")


# ── Main ───────────────────────────────────────────────────────────────────────

async def main() -> None:
    setup_logging()
    log.info("=== London Nervous System starting ===")

    # Initialize core systems
    board = MessageBoard()
    await board.init()
    graph = LondonGraph()
    memory = MemoryManager()

    # Initialize the City's Retina (perceptual compression)
    from .core.retina import AttentionManager
    retina = AttentionManager(graph)
    graph.retina = retina  # agents access via self.graph.retina
    board.set_retina(retina)
    scheduler = AsyncScheduler()  # kept for ingestor compatibility
    coordinator = Coordinator()

    # Configure rate limits on both scheduler and coordinator
    for rl in (scheduler, coordinator):
        rl.configure_rate_limit("gemini_flash", RATE_LIMITS.gemini_flash_rpm)
        rl.configure_rate_limit("gemini_pro", RATE_LIMITS.gemini_pro_rpm)
        rl.configure_rate_limit("tfl", RATE_LIMITS.tfl_rpm)
        rl.configure_rate_limit("default", RATE_LIMITS.default_rpm)

    # Restore baselines from previous run
    restored = await memory.restore_baselines(board.get_db())
    log.info("Restored %d baselines from previous run.", restored)

    # Seed retina foci at known London hotspots
    retina.seed_initial_foci([
        (graph.latlon_to_cell(51.505, -0.09), "city_center"),
        (graph.latlon_to_cell(51.515, -0.14), "west_end"),
        (graph.latlon_to_cell(51.503, -0.02), "canary_wharf"),
    ])

    # Persist system start time for maturity tracking
    meta_path = Path(MEMORY_DIR) / "system_meta.json"
    system_meta = {}
    if meta_path.exists():
        try:
            system_meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    system_meta["last_start_time"] = datetime.now(timezone.utc).isoformat()
    if "first_start_time" not in system_meta:
        system_meta["first_start_time"] = system_meta["last_start_time"]
    meta_path.write_text(json.dumps(system_meta, indent=2))

    log.info("Core systems initialized. Graph: %s", graph.stats())

    # Import ingestors (actual exported names)
    from .ingestors.tfl import ingest_tfl
    from .ingestors.air_quality import ingest_air_quality
    from .ingestors.weather import ingest_weather
    from .ingestors.news import ingest_news
    from .ingestors.financial import ingest_financial
    from .ingestors.energy import ingest_energy
    from .ingestors.environment import ingest_environment
    from .ingestors.nature import ingest_nature
    from .ingestors.misc import ingest_misc
    from .ingestors.satellite import ingest_satellite
    from .ingestors.sentinel5p import ingest_sentinel5p
    from .ingestors.tfl_digital_health import ingest_tfl_digital_health
    from .ingestors.events import ingest_events
    from .ingestors.road_disruptions import ingest_road_disruptions
    from .ingestors.tfl_traffic import ingest_tfl_traffic
    from .ingestors.sensor_health import ingest_sensor_health
    from .ingestors.system_health import ingest_system_health
    from .ingestors.waze_traffic import ingest_waze_traffic
    from .ingestors.data_lineage import ingest_data_lineage
    from .ingestors.apm import ingest_apm

    from .ingestors.data_quality import ingest_data_quality
    from .ingestors.tomtom_traffic import ingest_tomtom_traffic
    from .ingestors.tfl_bus_congestion import ingest_tfl_bus_congestion
    from .ingestors.provider_status import ingest_provider_status
    from .ingestors.police_crimes import ingest_police_crimes
    from .ingestors.planned_works import ingest_planned_works
    from .ingestors.bus_avl import ingest_bus_avl
    from .ingestors.grid_generation import ingest_grid_generation
    from .ingestors.social_sentiment import ingest_social_sentiment
    from .ingestors.cycle_hire import ingest_cycle_hire
    from .ingestors.embedded_generation import ingest_embedded_generation
    from .ingestors.bus_crowding import ingest_bus_crowding
    from .ingestors.retail_spending import ingest_retail_spending
    from .ingestors.grid_demand import ingest_grid_demand
    from .ingestors.ukpn_substation import ingest_ukpn_substation
    from .ingestors.tfl_crowding import ingest_tfl_crowding
    from .ingestors.rideshare_demand import ingest_rideshare_demand
    from .ingestors.sensor_community import ingest_sensor_community
    from .ingestors.tfl_passenger_counts import ingest_tfl_passenger_counts
    from .ingestors.commuter_displacement import ingest_commuter_displacement
    from .ingestors.tube_arrivals import ingest_tube_arrivals
    from .ingestors.micromobility import ingest_micromobility
    from .ingestors.tfl_disruption_incidents import ingest_tfl_disruption_incidents
    from .ingestors.laqn_transport import ingest_laqn_transport
    from .ingestors.river_flow import ingest_river_flow
    from .ingestors.purpleair import ingest_purpleair
    from .ingestors.vam_museum import ingest_vam_museum
    from .ingestors.land_registry import ingest_land_registry
    from .ingestors.uk_parliament import ingest_uk_parliament
    from .ingestors.companies_house import ingest_companies_house
    from .ingestors.historic_england import ingest_historic_england
    from .ingestors.open_plaques import ingest_open_plaques
    from .ingestors.grid_forecast import ingest_grid_forecast
    from .ingestors.grid_status import ingest_grid_status
    from .ingestors.google_air_quality import ingest_google_air_quality
    from .ingestors.opensensemap_mobile import ingest_opensensemap_mobile
    from .ingestors.openaq import ingest_openaq
    from .ingestors.national_rail import ingest_national_rail
    from .ingestors.tv_schedule import ingest_tv_schedule
    from .ingestors.nhs_syndromic import ingest_nhs_syndromic
    from .ingestors.windy_webcams import ingest_windy_webcams
    from .ingestors.met_office import ingest_met_office
    from .ingestors.thames_water import ingest_thames_water

    # Import agents (actual exported names)
    from .agents.interpreters import (
        run_vision_interpreter,
        run_numeric_interpreter,
        run_text_interpreter,
        run_financial_interpreter,
    )
    from .agents.connectors import (
        run_spatial_connector,
        run_narrative_connector,
        run_statistical_connector,
        run_causal_chain_connector,
    )
    from .agents.brain import run_brain
    from .agents.validator import run_validator
    from .agents.explorers import run_explorer_spawner
    from .agents.curiosity import run_curiosity_engine
    from .agents.web_searcher import run_web_searcher
    from .agents.chronicler import run_chronicler
    from .agents.discovery import run_discovery_engine

    # Ingestors take (board, graph, scheduler)
    # Agents take (board, graph, memory)
    ingestor_args = (board, graph, scheduler)
    agent_args = (board, graph, memory)
    coordinator._ingestor_args = ingestor_args

    # ── Key-gated ingestor registration ──────────────────────────────────────
    # Ingestors listed here require specific env vars to function.
    # If the vars are missing, the ingestor is skipped entirely at startup
    # (no wasted cycles, no circuit-breaker noise).
    INGESTOR_KEY_REQUIREMENTS: dict[str, list[str]] = {
        "ingest_tomtom_traffic": ["TOMTOM_API_KEY"],
        "ingest_waze_traffic": ["WAZE_FEED_URL"],
        "ingest_bus_avl": ["BODS_API_KEY"],
        "ingest_rideshare_demand": ["UBER_SERVER_TOKEN"],
        "ingest_google_air_quality": ["GOOGLE_AQ_API_KEY"],
        "ingest_openaq": ["OPENAQ_API_KEY"],
        "ingest_purpleair": ["PURPLEAIR_API_KEY"],
        "ingest_events": ["PREDICTHQ_API_KEY"],
    }

    active_ingestors: list[str] = []
    skipped_ingestors: list[tuple[str, list[str]]] = []

    def _register_gated(name, func, *, min_interval, max_interval,
                        priority, args, source_names, kwargs=None):
        """Register an ingestor, skipping it if required env vars are missing."""
        import os
        required = INGESTOR_KEY_REQUIREMENTS.get(name)
        if required:
            missing = [v for v in required if not os.environ.get(v, "").strip()]
            if missing:
                skipped_ingestors.append((name, missing))
                return
        coordinator.register(name, func,
            min_interval=min_interval, max_interval=max_interval,
            priority=priority, args=args,
            kwargs=kwargs or {}, source_names=source_names)
        active_ingestors.append(name)

    # Register all tasks with the coordinator (adaptive intervals)
    # Ingestors: min=half of default, max=double of default
    _register_gated("ingest_tfl", ingest_tfl,
        min_interval=INTERVALS.tfl_jamcams * 0.5, max_interval=INTERVALS.tfl_jamcams * 2,
        priority=2, args=ingestor_args, kwargs={"memory": memory},
        source_names=["tfl_jamcam", "tfl"])
    _register_gated("ingest_air_quality", ingest_air_quality,
        min_interval=INTERVALS.air_quality * 0.5, max_interval=INTERVALS.air_quality * 2,
        priority=3, args=ingestor_args, source_names=["laqn"])
    _register_gated("ingest_weather", ingest_weather,
        min_interval=INTERVALS.weather * 0.5, max_interval=INTERVALS.weather * 2,
        priority=4, args=ingestor_args, source_names=["open_meteo"])
    _register_gated("ingest_met_office", ingest_met_office,
        min_interval=INTERVALS.met_office * 0.5, max_interval=INTERVALS.met_office * 2,
        priority=4, args=ingestor_args, source_names=["met_office"])
    _register_gated("ingest_news", ingest_news,
        min_interval=INTERVALS.news_gdelt * 0.5, max_interval=INTERVALS.news_gdelt * 2,
        priority=3, args=ingestor_args, source_names=["gdelt", "news"])
    _register_gated("ingest_financial", ingest_financial,
        min_interval=INTERVALS.financial_stocks * 0.5, max_interval=INTERVALS.financial_stocks * 2,
        priority=5, args=ingestor_args, source_names=["financial_stocks", "financial_crypto", "polymarket"])
    _register_gated("ingest_energy", ingest_energy,
        min_interval=INTERVALS.carbon_intensity * 0.5, max_interval=INTERVALS.carbon_intensity * 2,
        priority=6, args=ingestor_args, source_names=["carbon_intensity"])
    _register_gated("ingest_environment", ingest_environment,
        min_interval=INTERVALS.environment * 0.5, max_interval=INTERVALS.environment * 2,
        priority=5, args=ingestor_args, source_names=["environment_agency"])
    _register_gated("ingest_thames_water", ingest_thames_water,
        min_interval=INTERVALS.thames_water * 0.5, max_interval=INTERVALS.thames_water * 2,
        priority=5, args=ingestor_args, source_names=["thames_water_edm"])
    _register_gated("ingest_nature", ingest_nature,
        min_interval=INTERVALS.nature * 0.5, max_interval=INTERVALS.nature * 2,
        priority=7, args=ingestor_args, source_names=["nature_inaturalist"])
    _register_gated("ingest_misc", ingest_misc,
        min_interval=INTERVALS.food_hygiene * 0.5, max_interval=INTERVALS.food_hygiene * 2,
        priority=8, args=ingestor_args, source_names=["food_hygiene"])
    _register_gated("ingest_tfl_digital_health", ingest_tfl_digital_health,
        min_interval=INTERVALS.tfl_digital_health * 0.5, max_interval=INTERVALS.tfl_digital_health * 2,
        priority=3, args=ingestor_args, source_names=["tfl_digital_health"])
    _register_gated("ingest_satellite", ingest_satellite,
        min_interval=INTERVALS.sentinel2 * 0.5, max_interval=INTERVALS.sentinel2 * 2,
        priority=9, args=ingestor_args, source_names=["sentinel2"])
    _register_gated("ingest_sentinel5p", ingest_sentinel5p,
        min_interval=INTERVALS.sentinel5p * 0.5, max_interval=INTERVALS.sentinel5p * 2,
        priority=8, args=ingestor_args, source_names=["sentinel5p"])
    _register_gated("ingest_events", ingest_events,
        min_interval=INTERVALS.public_events * 0.5, max_interval=INTERVALS.public_events * 2,
        priority=5, args=ingestor_args, source_names=["public_events"])
    _register_gated("ingest_road_disruptions", ingest_road_disruptions,
        min_interval=INTERVALS.road_disruptions * 0.5, max_interval=INTERVALS.road_disruptions * 2,
        priority=2, args=ingestor_args, source_names=["tfl_road_disruptions"])
    _register_gated("ingest_tfl_traffic", ingest_tfl_traffic,
        min_interval=INTERVALS.tfl_traffic * 0.5, max_interval=INTERVALS.tfl_traffic * 2,
        priority=2, args=ingestor_args, source_names=["tfl_traffic"])
    _register_gated("ingest_sensor_health", ingest_sensor_health,
        min_interval=INTERVALS.sensor_health * 0.5, max_interval=INTERVALS.sensor_health * 2,
        priority=4, args=ingestor_args, source_names=["sensor_health"])
    _register_gated("ingest_system_health", ingest_system_health,
        min_interval=INTERVALS.system_health * 0.5, max_interval=INTERVALS.system_health * 2,
        priority=4, args=ingestor_args, source_names=["system_health"])
    _register_gated("ingest_waze_traffic", ingest_waze_traffic,
        min_interval=INTERVALS.waze_traffic * 0.5, max_interval=INTERVALS.waze_traffic * 2,
        priority=2, args=ingestor_args, source_names=["waze_traffic"])
    _register_gated("ingest_data_lineage", ingest_data_lineage,
        min_interval=INTERVALS.data_lineage * 0.5, max_interval=INTERVALS.data_lineage * 2,
        priority=4, args=ingestor_args, source_names=["data_lineage"])
    _register_gated("ingest_apm", ingest_apm,
        min_interval=INTERVALS.apm * 0.5, max_interval=INTERVALS.apm * 2,
        priority=3, args=ingestor_args, source_names=["apm"])

    _register_gated("ingest_data_quality", ingest_data_quality,
        min_interval=INTERVALS.data_quality * 0.5, max_interval=INTERVALS.data_quality * 2,
        priority=4, args=ingestor_args, source_names=["data_quality"])
    _register_gated("ingest_tomtom_traffic", ingest_tomtom_traffic,
        min_interval=INTERVALS.tomtom_traffic * 0.5, max_interval=INTERVALS.tomtom_traffic * 2,
        priority=2, args=ingestor_args, source_names=["tomtom_traffic"])
    _register_gated("ingest_tfl_bus_congestion", ingest_tfl_bus_congestion,
        min_interval=INTERVALS.tfl_bus_congestion * 0.5, max_interval=INTERVALS.tfl_bus_congestion * 2,
        priority=3, args=ingestor_args, source_names=["tfl_bus_congestion"])
    _register_gated("ingest_provider_status", ingest_provider_status,
        min_interval=INTERVALS.provider_status * 0.5, max_interval=INTERVALS.provider_status * 2,
        priority=3, args=ingestor_args, source_names=["provider_status"])
    _register_gated("ingest_police_crimes", ingest_police_crimes,
        min_interval=INTERVALS.police_crimes * 0.5, max_interval=INTERVALS.police_crimes * 2,
        priority=6, args=ingestor_args, source_names=["police_crimes"])
    _register_gated("ingest_planned_works", ingest_planned_works,
        min_interval=INTERVALS.planned_works * 0.5, max_interval=INTERVALS.planned_works * 2,
        priority=3, args=ingestor_args, source_names=["tfl_planned_works"])
    _register_gated("ingest_bus_avl", ingest_bus_avl,
        min_interval=INTERVALS.bus_avl * 0.5, max_interval=INTERVALS.bus_avl * 2,
        priority=2, args=ingestor_args, source_names=["bods_bus_avl"])
    _register_gated("ingest_grid_generation", ingest_grid_generation,
        min_interval=INTERVALS.grid_generation * 0.5, max_interval=INTERVALS.grid_generation * 2,
        priority=4, args=ingestor_args, source_names=["grid_generation"])
    _register_gated("ingest_social_sentiment", ingest_social_sentiment,
        min_interval=INTERVALS.social_sentiment * 0.5, max_interval=INTERVALS.social_sentiment * 2,
        priority=5, args=ingestor_args, source_names=["social_sentiment"])
    _register_gated("ingest_cycle_hire", ingest_cycle_hire,
        min_interval=INTERVALS.cycle_hire * 0.5, max_interval=INTERVALS.cycle_hire * 2,
        priority=3, args=ingestor_args, source_names=["tfl_cycle_hire"])
    _register_gated("ingest_embedded_generation", ingest_embedded_generation,
        min_interval=INTERVALS.embedded_generation * 0.5, max_interval=INTERVALS.embedded_generation * 2,
        priority=5, args=ingestor_args, source_names=["neso_embedded_gen"])
    _register_gated("ingest_bus_crowding", ingest_bus_crowding,
        min_interval=INTERVALS.bus_crowding * 0.5, max_interval=INTERVALS.bus_crowding * 2,
        priority=2, args=ingestor_args, source_names=["tfl_bus_crowding"])
    _register_gated("ingest_retail_spending", ingest_retail_spending,
        min_interval=INTERVALS.retail_spending * 0.5, max_interval=INTERVALS.retail_spending * 2,
        priority=6, args=ingestor_args, source_names=["retail_spending"])
    _register_gated("ingest_grid_demand", ingest_grid_demand,
        min_interval=INTERVALS.grid_demand * 0.5, max_interval=INTERVALS.grid_demand * 2,
        priority=5, args=ingestor_args, source_names=["grid_demand"])
    _register_gated("ingest_ukpn_substation", ingest_ukpn_substation,
        min_interval=INTERVALS.ukpn_substation * 0.5, max_interval=INTERVALS.ukpn_substation * 2,
        priority=4, args=ingestor_args, source_names=["ukpn_substation"])
    _register_gated("ingest_tfl_crowding", ingest_tfl_crowding,
        min_interval=INTERVALS.tfl_crowding * 0.5, max_interval=INTERVALS.tfl_crowding * 2,
        priority=2, args=ingestor_args, source_names=["tfl_crowding"])
    _register_gated("ingest_rideshare_demand", ingest_rideshare_demand,
        min_interval=INTERVALS.rideshare_demand * 0.5, max_interval=INTERVALS.rideshare_demand * 2,
        priority=4, args=ingestor_args, source_names=["rideshare_demand"])
    _register_gated("ingest_sensor_community", ingest_sensor_community,
        min_interval=INTERVALS.sensor_community * 0.5, max_interval=INTERVALS.sensor_community * 2,
        priority=5, args=ingestor_args, source_names=["sensor_community"])
    _register_gated("ingest_tfl_passenger_counts", ingest_tfl_passenger_counts,
        min_interval=INTERVALS.tfl_passenger_counts * 0.5, max_interval=INTERVALS.tfl_passenger_counts * 2,
        priority=3, args=ingestor_args, source_names=["tfl_passenger_counts"])
    _register_gated("ingest_commuter_displacement", ingest_commuter_displacement,
        min_interval=INTERVALS.commuter_displacement * 0.5, max_interval=INTERVALS.commuter_displacement * 2,
        priority=3, args=ingestor_args, source_names=["commuter_displacement"])
    _register_gated("ingest_tube_arrivals", ingest_tube_arrivals,
        min_interval=INTERVALS.tube_arrivals * 0.5, max_interval=INTERVALS.tube_arrivals * 2,
        priority=2, args=ingestor_args, source_names=["tube_arrivals"])
    _register_gated("ingest_micromobility", ingest_micromobility,
        min_interval=INTERVALS.micromobility * 0.5, max_interval=INTERVALS.micromobility * 2,
        priority=4, args=ingestor_args, source_names=["micromobility"])
    _register_gated("ingest_tfl_disruption_incidents", ingest_tfl_disruption_incidents,
        min_interval=INTERVALS.tfl_disruption_incidents * 0.5, max_interval=INTERVALS.tfl_disruption_incidents * 2,
        priority=2, args=ingestor_args, source_names=["tfl_disruption_incidents"])
    _register_gated("ingest_laqn_transport", ingest_laqn_transport,
        min_interval=INTERVALS.laqn_transport * 0.5, max_interval=INTERVALS.laqn_transport * 2,
        priority=3, args=ingestor_args, source_names=["laqn_transport"])
    _register_gated("ingest_river_flow", ingest_river_flow,
        min_interval=INTERVALS.river_flow * 0.5, max_interval=INTERVALS.river_flow * 2,
        priority=5, args=ingestor_args, source_names=["ea_river_flow"])
    _register_gated("ingest_purpleair", ingest_purpleair,
        min_interval=INTERVALS.purpleair * 0.5, max_interval=INTERVALS.purpleair * 2,
        priority=5, args=ingestor_args, source_names=["purpleair"])
    _register_gated("ingest_vam_museum", ingest_vam_museum,
        min_interval=INTERVALS.vam_museum * 0.5, max_interval=INTERVALS.vam_museum * 2,
        priority=8, args=ingestor_args, source_names=["vam_museum"])
    _register_gated("ingest_land_registry", ingest_land_registry,
        min_interval=INTERVALS.land_registry * 0.5, max_interval=INTERVALS.land_registry * 2,
        priority=7, args=ingestor_args, source_names=["land_registry"])
    _register_gated("ingest_uk_parliament", ingest_uk_parliament,
        min_interval=INTERVALS.uk_parliament * 0.5, max_interval=INTERVALS.uk_parliament * 2,
        priority=7, args=ingestor_args, source_names=["uk_parliament"])
    _register_gated("ingest_companies_house", ingest_companies_house,
        min_interval=INTERVALS.companies_house * 0.5, max_interval=INTERVALS.companies_house * 2,
        priority=8, args=ingestor_args, source_names=["uk_petitions"])
    _register_gated("ingest_historic_england", ingest_historic_england,
        min_interval=INTERVALS.historic_england * 0.5, max_interval=INTERVALS.historic_england * 2,
        priority=9, args=ingestor_args, source_names=["ons_demographics"])
    _register_gated("ingest_open_plaques", ingest_open_plaques,
        min_interval=INTERVALS.open_plaques * 0.5, max_interval=INTERVALS.open_plaques * 2,
        priority=9, args=ingestor_args, source_names=["open_plaques"])
    _register_gated("ingest_grid_forecast", ingest_grid_forecast,
        min_interval=INTERVALS.grid_forecast * 0.5, max_interval=INTERVALS.grid_forecast * 2,
        priority=4, args=ingestor_args, source_names=["neso_grid_forecast"])
    _register_gated("ingest_grid_status", ingest_grid_status,
        min_interval=INTERVALS.grid_status * 0.5, max_interval=INTERVALS.grid_status * 2,
        priority=5, args=ingestor_args, source_names=["neso_grid_status"])
    _register_gated("ingest_google_air_quality", ingest_google_air_quality,
        min_interval=INTERVALS.google_aq * 0.5, max_interval=INTERVALS.google_aq * 2,
        priority=5, args=ingestor_args, source_names=["google_aq"])
    _register_gated("ingest_openaq", ingest_openaq,
        min_interval=INTERVALS.openaq * 0.5, max_interval=INTERVALS.openaq * 2,
        priority=5, args=ingestor_args, source_names=["openaq"])
    _register_gated("ingest_national_rail", ingest_national_rail,
        min_interval=INTERVALS.national_rail * 0.5, max_interval=INTERVALS.national_rail * 2,
        priority=2, args=ingestor_args, source_names=["national_rail"])
    _register_gated("ingest_tv_schedule", ingest_tv_schedule,
        min_interval=INTERVALS.tv_schedule * 0.5, max_interval=INTERVALS.tv_schedule * 2,
        priority=7, args=ingestor_args, source_names=["tv_schedule"])
    _register_gated("ingest_nhs_syndromic", ingest_nhs_syndromic,
        min_interval=INTERVALS.nhs_syndromic * 0.5, max_interval=INTERVALS.nhs_syndromic * 2,
        priority=5, args=ingestor_args, source_names=["nhs_syndromic"])
    _register_gated("ingest_windy_webcams", ingest_windy_webcams,
        min_interval=INTERVALS.windy_webcams * 0.5, max_interval=INTERVALS.windy_webcams * 2,
        priority=3, args=ingestor_args, source_names=["windy_webcam"])

    # ── Startup Report ────────────────────────────────────────────────────────
    log.info("=== Ingestor Startup Report ===")
    log.info("Active: %d ingestors registered", len(active_ingestors))
    if skipped_ingestors:
        for name, missing in sorted(skipped_ingestors):
            log.warning("SKIPPED %s — missing: %s", name, ", ".join(missing))
        log.warning(
            "%d ingestor(s) skipped due to missing API keys",
            len(skipped_ingestors),
        )
    else:
        log.info("All ingestors have required API keys.")
    log.info("=== End Startup Report ===")

    # Post source availability to board so agents know what data is live
    from .core.models import AgentMessage
    await board.post(AgentMessage(
        from_agent="startup",
        channel="#system",
        content=(
            (
                f"System started with {len(active_ingestors)} active data sources. "
                f"Skipped {len(skipped_ingestors)} ingestor(s) due to missing API keys: "
                + ", ".join(n for n, _ in skipped_ingestors)
            ) if skipped_ingestors
            else f"System started with {len(active_ingestors)} active data sources. All keys present."
        ),
        data={
            "active_ingestors": sorted(active_ingestors),
            "skipped_ingestors": [
                {"name": n, "missing_keys": k} for n, k in skipped_ingestors
            ],
            "event": "startup_source_report",
        },
    ))

    # Interpreters
    coordinator.register("vision_interpreter", run_vision_interpreter,
        min_interval=INTERVALS.vision_interpreter * 0.5, max_interval=INTERVALS.vision_interpreter * 2,
        priority=2, args=agent_args)
    coordinator.register("numeric_interpreter", run_numeric_interpreter,
        min_interval=INTERVALS.numeric_interpreter * 0.5, max_interval=INTERVALS.numeric_interpreter * 2,
        priority=2, args=agent_args)
    coordinator.register("text_interpreter", run_text_interpreter,
        min_interval=INTERVALS.text_interpreter * 0.5, max_interval=INTERVALS.text_interpreter * 2,
        priority=3, args=agent_args)
    coordinator.register("financial_interpreter", run_financial_interpreter,
        min_interval=INTERVALS.financial_interpreter * 0.5, max_interval=INTERVALS.financial_interpreter * 2,
        priority=4, args=agent_args)

    # Connectors
    coordinator.register("spatial_connector", run_spatial_connector,
        min_interval=INTERVALS.spatial_connector * 0.5, max_interval=INTERVALS.spatial_connector * 2,
        priority=3, args=agent_args)
    coordinator.register("narrative_connector", run_narrative_connector,
        min_interval=INTERVALS.narrative_connector * 0.5, max_interval=INTERVALS.narrative_connector * 2,
        priority=4, args=agent_args)
    coordinator.register("statistical_connector", run_statistical_connector,
        min_interval=INTERVALS.statistical_connector * 0.5, max_interval=INTERVALS.statistical_connector * 2,
        priority=6, args=agent_args)
    coordinator.register("causal_chain_connector", run_causal_chain_connector,
        min_interval=INTERVALS.spatial_connector * 0.5, max_interval=INTERVALS.spatial_connector * 2,
        priority=5, args=agent_args)

    # Brain + Validator + Explorers
    coordinator.register("brain", run_brain,
        min_interval=INTERVALS.brain * 0.5, max_interval=INTERVALS.brain * 2,
        priority=1, args=agent_args)
    coordinator.register("validator", run_validator,
        min_interval=INTERVALS.validator * 0.5, max_interval=INTERVALS.validator * 2,
        priority=5, args=agent_args)
    coordinator.register("explorer_spawner", run_explorer_spawner,
        min_interval=INTERVALS.explorer_spawner * 0.5, max_interval=INTERVALS.explorer_spawner * 2,
        priority=4, args=agent_args)

    # New agents: Curiosity, Web Searcher, Chronicler
    coordinator.register("curiosity_engine", run_curiosity_engine,
        min_interval=INTERVALS.curiosity_engine * 0.5, max_interval=INTERVALS.curiosity_engine * 2,
        priority=3, args=agent_args)
    coordinator.register("web_searcher", run_web_searcher,
        min_interval=INTERVALS.web_searcher * 0.5, max_interval=INTERVALS.web_searcher * 2,
        priority=3, args=agent_args)
    coordinator.register("chronicler", run_chronicler,
        min_interval=INTERVALS.chronicler * 0.5, max_interval=INTERVALS.chronicler * 2,
        priority=5, args=agent_args)
    coordinator.register("discovery_engine", run_discovery_engine,
        min_interval=INTERVALS.discovery_engine * 0.5, max_interval=INTERVALS.discovery_engine * 2,
        priority=2, args=agent_args)

    # Graceful shutdown handler — second Ctrl+C forces immediate exit
    stop_event = asyncio.Event()

    def handle_signal():
        if stop_event.is_set():
            # Second signal — force exit immediately
            log.warning("Second shutdown signal — forcing exit")
            import os
            os._exit(1)
        log.info("Shutdown signal received (press Ctrl+C again to force)")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Start dashboard
    await _dashboard.start(board, coordinator, memory=memory, graph=graph, retina=retina)

    # Run startup health probes (blacklist broken ingestors early)
    await coordinator.run_startup_probes()

    # Start all tasks via coordinator
    await coordinator.start()
    log.info("=== All %d coordinator tasks started ===", len(coordinator._tasks))

    # Start daemon as persistent async task
    from .core.daemon import Daemon
    daemon = Daemon(board, coordinator)
    _dashboard._daemon = daemon  # expose for /api/claude_instances
    if _dashboard._investigator:
        _dashboard._investigator._daemon = daemon
    daemon_task = asyncio.create_task(daemon.run(), name="daemon")

    # Import image store for cleanup
    from .core.image_store import ImageStore
    image_store = ImageStore()
    last_daily_snapshot = datetime.now(timezone.utc)

    # Periodic maintenance
    async def maintenance():
        nonlocal last_daily_snapshot
        while not stop_event.is_set():
            await asyncio.sleep(300)
            pruned = graph.prune_expired_anomalies()
            retina.tick_decay()
            expired = await board.cleanup_expired()
            # Persist baselines every 5 minutes
            saved = await memory.persist_baselines(board.get_db())
            # Image cleanup
            image_store.cleanup()
            if pruned or expired or saved:
                log.info(
                    "Maintenance: pruned %d anomalies, %d expired messages, %d baselines saved",
                    pruned, expired, saved,
                )
            # Daily baseline snapshot
            now = datetime.now(timezone.utc)
            if (now - last_daily_snapshot).total_seconds() > 86400:
                await memory.snapshot_daily_baselines(board.get_db())
                last_daily_snapshot = now

    maint_task = asyncio.create_task(maintenance())

    # Wait for shutdown
    await stop_event.wait()
    log.info("Shutting down...")
    maint_task.cancel()
    daemon_task.cancel()
    # Persist baselines one final time before shutdown
    try:
        await asyncio.wait_for(memory.persist_baselines(board.get_db()), timeout=3.0)
    except Exception:
        pass
    # Stop coordinator with a hard timeout so Ctrl+C doesn't hang
    try:
        await asyncio.wait_for(coordinator.stop(), timeout=5.0)
    except asyncio.TimeoutError:
        log.warning("Coordinator stop timed out after 5s, forcing exit")
    await _dashboard.stop()
    await board.close()
    log.info("=== London Nervous System stopped ===")

    # Check if daemon requested a restart (sentinel written by daemon.py)
    sentinel = Path(__file__).resolve().parent / "data" / ".restart"
    if sentinel.exists():
        sentinel.unlink(missing_ok=True)
        return "restart"
    return "stop"


def run_with_auto_restart() -> None:
    """Run main() in a loop, auto-restarting when the daemon requests it."""
    import time as _time

    restart_count = 0
    max_rapid_restarts = 10  # safety valve
    last_start = 0.0

    while True:
        now = _time.monotonic()
        # Reset counter if last start was >60s ago (not a rapid restart loop)
        if now - last_start > 60:
            restart_count = 0
        last_start = now

        result = asyncio.run(main())

        if result == "restart":
            restart_count += 1
            if restart_count >= max_rapid_restarts:
                log.info("Too many rapid restarts (%d), stopping.", restart_count)
                break
            # Brief pause to let file system settle
            _time.sleep(2)
            log.info("=== Auto-restarting (restart #%d) ===", restart_count)
            continue
        else:
            # Clean shutdown (user Ctrl+C without restart sentinel)
            break


if __name__ == "__main__":
    run_with_auto_restart()
