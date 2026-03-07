"""Configuration: endpoints, thresholds, model choices, grid parameters."""

from dataclasses import dataclass, field
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "graph.db"
MEMORY_DIR = DATA_DIR / "memory"
CACHE_DIR = DATA_DIR / "cache"
LOG_DIR = DATA_DIR / "logs"
STATIC_DIR = PROJECT_ROOT / "static"

# Ensure dirs exist
for d in (DATA_DIR, MEMORY_DIR, CACHE_DIR, LOG_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Grid ───────────────────────────────────────────────────────────────────────
GRID_CELL_SIZE_M = 500
LONDON_BBOX = {
    "min_lat": 51.28,
    "max_lat": 51.70,
    "min_lon": -0.51,
    "max_lon": 0.33,
}

# ── Models ─────────────────────────────────────────────────────────────────────
GEMINI_FLASH_MODEL = "gemini-2.0-flash"
GEMINI_PRO_MODEL = "gemini-2.5-pro"

# ── Rate Limits ────────────────────────────────────────────────────────────────
@dataclass
class RateLimitConfig:
    gemini_flash_rpm: int = 60
    gemini_pro_rpm: int = 120
    tfl_rpm: int = 30
    default_rpm: int = 60

RATE_LIMITS = RateLimitConfig()

# ── Anomaly Thresholds ─────────────────────────────────────────────────────────
ANOMALY_Z_THRESHOLD = 2.0
ANOMALY_TTL_HOURS = 6

# ── Agent Intervals (seconds) ─────────────────────────────────────────────────
@dataclass
class IntervalConfig:
    tfl_jamcams: int = 300        # 5 min
    air_quality: int = 900        # 15 min
    weather: int = 1800           # 30 min
    met_office: int = 1800        # 30 min (Met Office DataHub / Open-Meteo fallback)
    metoffice_obs: int = 1800     # 30 min (Met Office DataPoint hourly obs; geo-tag canary)
    news_gdelt: int = 900         # 15 min
    financial_stocks: int = 900   # 15 min
    financial_crypto: int = 1800  # 30 min
    polymarket: int = 1800        # 30 min
    carbon_intensity: int = 1800  # 30 min
    environment: int = 300        # 5 min
    thames_water: int = 900       # 15 min (Thames Water EDM storm overflow status)

    nature: int = 3600            # 60 min (iNaturalist)
    food_hygiene: int = 86400     # daily
    tfl_digital_health: int = 600  # 10 min
    sentinel2: int = 3600         # check hourly
    sentinel5p: int = 3600        # hourly (S5P revisit ~daily, L3 products lag ~hours)
    public_events: int = 3600     # 60 min (events change slowly)
    road_disruptions: int = 300   # 5 min (realtime incidents change fast)
    tfl_traffic: int = 300        # 5 min (corridor status updates frequently)
    sensor_health: int = 900      # 15 min (sensor status changes slowly)
    system_health: int = 900      # 15 min (internal log/task monitoring)
    data_lineage: int = 900       # 15 min (per-source freshness tracking)
    waze_traffic: int = 120       # 2 min (Waze feed updates every ~2 min)
    apm: int = 60                 # 1 min (process-level resource metrics)

    data_quality: int = 900         # 15 min (validation failures, value anomalies)
    tomtom_traffic: int = 300       # 5 min (flow + incidents; free tier: 2500 req/day)
    tfl_bus_congestion: int = 300   # 5 min (bus arrival predictions as congestion proxy)
    provider_status: int = 300      # 5 min (upstream API health probes)
    police_crimes: int = 3600       # 60 min (monthly data, no need to poll frequently)
    planned_works: int = 600         # 10 min (line status changes with planned closures)
    bus_avl: int = 120                # 2 min (BODS SIRI-VM vehicle positions, updates every 10-30s)
    grid_generation: int = 300        # 5 min (Elexon FUELINST updates every 5 min)
    social_sentiment: int = 900       # 15 min (Reddit posts; respect rate limits)
    cycle_hire: int = 300              # 5 min (bike availability changes with commuter waves)
    embedded_generation: int = 1800    # 30 min (NESO forecast updates hourly)
    bus_crowding: int = 300              # 5 min (displacement onto tube-parallel bus routes)
    retail_spending: int = 3600          # 60 min (ONS data updates weekly; hourly poll is plenty)
    grid_demand: int = 1800               # 30 min (Elexon INDO updates every half-hour)
    grid_forecast: int = 1800              # 30 min (NESO day-ahead wind + demand forecasts)
    grid_status: int = 1800                 # 30 min (NESO SOP + OPMR; rate limit 2 req/min)
    ukpn_substation: int = 600             # 10 min (live faults update in near-real-time)
    ukpn_streetworks: int = 3600           # 60 min (proposed/open streetworks; daily refresh at source)
    tfl_crowding: int = 600                 # 10 min (station crowding profiles; 25 stations × 1 req each)
    rideshare_demand: int = 600              # 10 min (Uber surge as PHV demand proxy; 8 corridors)
    sensor_community: int = 900              # 15 min (citizen low-cost PM sensors; large payload)
    tfl_passenger_counts: int = 900          # 15 min (station entry/exit profiles; 19 stations × 1 req each)
    commuter_displacement: int = 600          # 10 min (TfL journey planner; 10 corridors × 1 req each)
    tube_arrivals: int = 300                   # 5 min (train arrival predictions; 11 lines × 1 req each)
    micromobility: int = 600                     # 10 min (GBFS feeds from Lime/Dott/TIER; displacement proxy)
    bluesky_transport: int = 600                   # 10 min (Bluesky public search; real-time commuter posts)
    tfl_disruption_incidents: int = 300              # 5 min (per-incident cause/impact detail from TfL disruption API)
    laqn_transport: int = 900                          # 15 min (LAQN µg/m³ concentrations at transport sites)
    river_flow: int = 300                                # 5 min (EA flow-rate sensors; redundant to level gauges)
    purpleair: int = 900                                   # 15 min (PurpleAir crowd-sourced PM2.5; API points budget)
    google_aq: int = 1800                                    # 30 min (Google AQ API; 15 sample points × 1 POST each)
    opensensemap_mobile: int = 1800                          # 30 min (mobile citizen-science boxes; no auth, sparse data)
    openaq: int = 900                                            # 15 min (OpenAQ citizen-science sensors; API key required)
    national_rail: int = 300                                       # 5 min (Darwin LDBWS departure boards; 8 terminal stations)
    tv_schedule: int = 3600                                          # 60 min (TVmaze UK schedule; data changes daily)
    nhs_syndromic: int = 3600                                           # 60 min (UKHSA syndromic surveillance; data updates daily/weekly)
    # Agent intervals
    vision_interpreter: int = 300  # 5 min — was 60s, burning Gemini quota
    numeric_interpreter: int = 30
    text_interpreter: int = 60
    financial_interpreter: int = 60
    spatial_connector: int = 30
    narrative_connector: int = 120
    statistical_connector: int = 3600
    brain: int = 300
    validator: int = 1800
    explorer_spawner: int = 120
    curiosity_engine: int = 600      # 10 min
    web_searcher: int = 60           # 1 min (reactive to investigations)
    chronicler: int = 3600           # 60 min
    discovery_engine: int = 300      # 5 min
    vam_museum: int = 3600           # 60 min (collection data, static)
    land_registry: int = 3600        # 60 min (sales data updates slowly)
    uk_parliament: int = 3600        # 60 min (bills/votes change slowly)
    companies_house: int = 3600      # 60 min (incorporations, daily pace)
    historic_england: int = 7200     # 2 hr (heritage data, very static)
    open_plaques: int = 7200         # 2 hr (plaque data, very static)
    windy_webcams: int = 540         # 9 min (image URLs expire after 10 min)

INTERVALS = IntervalConfig()

# ── API Endpoints ──────────────────────────────────────────────────────────────
ENDPOINTS = {
    "tfl_jamcams": "https://api.tfl.gov.uk/Place/Type/JamCam",
    "laqn": "https://api.erg.ic.ac.uk/AirQuality/Hourly/MonitoringIndex/GroupName=London/Json",
    "open_meteo": "https://api.open-meteo.com/v1/forecast",
    "gdelt_geo": "https://api.gdeltproject.org/api/v2/geo/geo",
    "gdelt_doc": "https://api.gdeltproject.org/api/v2/doc/doc",
    "carbon_intensity": "https://api.carbonintensity.org.uk",
    "environment_agency": "https://environment.data.gov.uk/flood-monitoring",
    "polymarket_gamma": "https://gamma-api.polymarket.com",
    "coingecko": "https://api.coingecko.com/api/v3",
    "overpass": "https://overpass-api.de/api/interpreter",
    "google_aq": "https://airquality.googleapis.com/v1/currentConditions:lookup",
    "inaturalist": "https://api.inaturalist.org/v1",
    "food_hygiene": "https://api.ratings.food.gov.uk",
    "tfl_api_base": "https://api.tfl.gov.uk",
    "planetary_computer": "https://planetarycomputer.microsoft.com/api/stac/v1",
    "predicthq": "https://api.predicthq.com/v1",
    "tfl_road_disruptions": "https://api.tfl.gov.uk/Road/all/Disruption",
    "tfl_road_status": "https://api.tfl.gov.uk/Road",
    "laqn_site_species": "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json",
    "ea_stations": "https://environment.data.gov.uk/flood-monitoring/id/stations",
    "waze_feed": "https://www.waze.com/partnerhub-api/waze-feeds/",  # requires WAZE_FEED_URL env var
    "tomtom_flow": "https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json",
    "tomtom_incidents": "https://api.tomtom.com/traffic/services/5/incidentDetails",
    "tfl_bus_arrivals": "https://api.tfl.gov.uk/Line/{lineId}/Arrivals",
    "tfl_line_stops": "https://api.tfl.gov.uk/Line/{lineId}/StopPoints",
    "police_crimes": "https://data.police.uk/api/crimes-street/all-crime",
    "police_dates": "https://data.police.uk/api/crimes-street-dates",
    "tfl_line_status": "https://api.tfl.gov.uk/Line/Mode/tube,dlr,overground,elizabeth-line,tram/Status",
    "bods_siri_vm": "https://data.bus-data.dft.gov.uk/api/v1/datafeed/",
    "elexon_fuelinst": "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST",
    "reddit_search": "https://www.reddit.com/r/{subreddit}/search.json",
    "tfl_bikepoint": "https://api.tfl.gov.uk/BikePoint",
    "neso_embedded_forecast": "https://api.neso.energy/api/3/action/datastore_search",
    "ons_datasets": "https://api.beta.ons.gov.uk/v1/datasets",
    "elexon_indo": "https://data.elexon.co.uk/bmrs/api/v1/datasets/INDO",
    "neso_grid_forecast": "https://api.neso.energy/api/3/action/datastore_search",
    "neso_grid_status": "https://api.neso.energy/api/3/action/datastore_search",
    "ukpn_open_data": "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1",
    "tfl_crowding": "https://api.tfl.gov.uk/crowding/{naptan}",
    "uber_price_estimates": "https://api.uber.com/v1.2/estimates/price",
    "sensor_community": "https://data.sensor.community/airrohr/v1/filter/area=51.509,-0.118,25",
    "tfl_stop_crowding": "https://api.tfl.gov.uk/StopPoint/{naptan}",
    "tfl_journey_planner": "https://api.tfl.gov.uk/Journey/JourneyResults/{from}/to/{to}",
    "tfl_line_arrivals": "https://api.tfl.gov.uk/Line/{lineId}/Arrivals",
    "gbfs_lime": "https://data.lime.bike/api/partners/v2/gbfs/london/gbfs.json",
    "gbfs_dott": "https://gbfs.api.ridedott.com/public/v2/london/gbfs.json",
    "gbfs_tier": "https://platform.tier-services.io/v2/gbfs/london/gbfs.json",
    "bsky_search": "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts",
    "tfl_line_disruptions": "https://api.tfl.gov.uk/Line/Mode/tube,dlr,overground,elizabeth-line,tram/Disruption",
    "laqn_data_site_species": "https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies",
    "ea_flow_stations": "https://environment.data.gov.uk/flood-monitoring/id/stations?parameter=flow",
    "s5p_pal_stac": "https://data-portal.s5p-pal.com/api/s5p-l3",
    "purpleair": "https://api.purpleair.com/v1/sensors",
    "opensensemap_boxes": "https://api.opensensemap.org/boxes",
    "openaq": "https://api.openaq.org/v3/locations",
    "windy_webcams": "https://api.windy.com/webcams/api/v3/webcams",
    "darwin_ldbws": "https://lite.realtime.nationalrail.co.uk/OpenLDBWS/ldb12.asmx",
    "tvmaze_schedule": "https://api.tvmaze.com/schedule",
    "tvmaze_web_schedule": "https://api.tvmaze.com/schedule/web",
    "ukhsa_dashboard": "https://api.ukhsa-dashboard.data.gov.uk",
}

# ── Message Board Channels ────────────────────────────────────────────────────
CHANNELS = ["#raw", "#observations", "#anomalies", "#hypotheses", "#requests", "#discoveries", "#meta", "#conversations"]

# ── Board Message TTL ──────────────────────────────────────────────────────────
DEFAULT_MESSAGE_TTL_HOURS = 6
