"""London Nervous System ingestors package."""

from .air_quality import AirQualityIngestor, ingest_air_quality
from .base import BaseIngestor
from .energy import CarbonIntensityIngestor, ingest_energy
from .environment import EnvironmentIngestor, ingest_environment
from .financial import FinancialIngestor, ingest_financial
from .misc import MiscIngestor, ingest_misc
from .nature import NatureIngestor, ingest_nature
from .news import GdeltNewsIngestor, ingest_news
from .satellite import SentinelIngestor, ingest_satellite
from .tfl import TflJamCamIngestor, ingest_tfl
from .weather import WeatherIngestor, ingest_weather
from .windy_webcams import WindyWebcamsIngestor, ingest_windy_webcams

__all__ = [
    # Base
    "BaseIngestor",
    # Ingestor classes
    "TflJamCamIngestor",
    "AirQualityIngestor",
    "WeatherIngestor",
    "GdeltNewsIngestor",
    "FinancialIngestor",
    "CarbonIntensityIngestor",
    "EnvironmentIngestor",
    "SentinelIngestor",
    "NatureIngestor",
    "MiscIngestor",
    # Standalone ingest functions
    "ingest_tfl",
    "ingest_air_quality",
    "ingest_weather",
    "ingest_news",
    "ingest_financial",
    "ingest_energy",
    "ingest_environment",
    "ingest_satellite",
    "ingest_nature",
    "ingest_misc",
    "WindyWebcamsIngestor",
    "ingest_windy_webcams",
]
