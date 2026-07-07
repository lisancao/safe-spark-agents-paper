"""Configuration dataclasses for the food-delivery test data generator.

Ported from lakehouse-stack/scripts/testdata/config.py. Determinism is preserved
(``seed`` drives both ``random`` and ``numpy``). New here vs. the source:

* ``load_chaos_config`` / ``chaos_from_dict`` — read the chaos knobs from the
  YAML file (``deploy/data/chaos.yaml``) so the rates are a deploy-time config
  rather than code.
* ``config_from_env`` — fold in the runtime parameters (Kafka bootstrap, topic,
  catalog) from environment variables for the streaming/loader paths.
"""

import os
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional


@dataclass
class ServiceTimes:
    """Gaussian distribution parameters for order lifecycle phases (in minutes)."""

    order_to_kitchen_start: tuple[float, float] = (2.0, 0.5)
    kitchen_prep: tuple[float, float] = (15.0, 5.0)
    kitchen_to_driver: tuple[float, float] = (5.0, 2.0)
    pickup_to_delivery: tuple[float, float] = (20.0, 8.0)
    driver_ping_interval_sec: int = 60


@dataclass
class DemandPattern:
    """Time-based demand weighting."""

    # Hour weights (0-23)
    hour_weights: Dict[int, float] = field(default_factory=lambda: {
        0: 0.1, 1: 0.05, 2: 0.02, 3: 0.01, 4: 0.01, 5: 0.05,
        6: 0.2, 7: 0.3, 8: 0.4, 9: 0.5, 10: 0.6,
        11: 0.9, 12: 1.0, 13: 0.9, 14: 0.5,
        15: 0.4, 16: 0.5, 17: 0.8,
        18: 1.0, 19: 1.0, 20: 0.9, 21: 0.6, 22: 0.4, 23: 0.2,
    })

    # Day of week multipliers (0=Monday, 6=Sunday)
    day_multipliers: Dict[int, float] = field(default_factory=lambda: {
        0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.25, 5: 1.35, 6: 1.15,
    })


@dataclass
class ChaosConfig:
    """Data quality issue injection settings — the 'messy' in 'real + messy data'.

    These are the knobs documented in deploy/data/chaos.yaml. Every rate is the
    per-event probability the corresponding defect is injected.
    """

    enabled: bool = True
    null_rate: float = 0.05  # 5% of events get a field nullified
    late_event_rate: float = 0.03  # 3% out-of-order / delayed events
    duplicate_rate: float = 0.02  # 2% duplicate events
    malformed_json_rate: float = 0.01  # 1% malformed body JSON
    # Seed for the streaming ChaosMonkey's isolated RNG (batch chaos is seeded
    # via the global random in generate_all_events). Keeps streaming chaos
    # deterministic: same (parquet, args, seed) => identical Kafka output.
    seed: int = 42


@dataclass
class Location:
    """A delivery location/city."""

    id: int
    city: str
    lat: float
    lon: float
    lat_range: tuple[float, float] = (0.0, 0.0)
    lon_range: tuple[float, float] = (0.0, 0.0)


@dataclass
class GeneratorConfig:
    """Main configuration for the test data generator."""

    # Time range
    start_date: date = field(default_factory=lambda: date(2024, 1, 1))
    days: int = 90
    seed: int = 42

    # Volume
    base_orders_per_day: int = 835  # ~75K over 90 days

    # Fraction of orders cancelled mid-lifecycle (emits a terminal
    # `order_cancelled` event instead of completing). Default 0 = off.
    cancel_rate: float = 0.0

    # Service times
    service_times: ServiceTimes = field(default_factory=ServiceTimes)

    # Demand patterns
    demand: DemandPattern = field(default_factory=DemandPattern)

    # Data quality chaos
    chaos: ChaosConfig = field(default_factory=ChaosConfig)

    # Locations
    locations: List[Location] = field(default_factory=lambda: [
        Location(
            id=1,
            city="San Francisco",
            lat=37.7749,
            lon=-122.4194,
            lat_range=(37.70, 37.82),
            lon_range=(-122.52, -122.35),
        ),
        Location(
            id=2,
            city="Silicon Valley",
            lat=37.3861,
            lon=-122.0839,
            lat_range=(37.30, 37.45),
            lon_range=(-122.20, -121.95),
        ),
        Location(
            id=3,
            city="Seattle",
            lat=47.6062,
            lon=-122.3321,
            lat_range=(47.50, 47.72),
            lon_range=(-122.45, -122.25),
        ),
        Location(
            id=4,
            city="Austin",
            lat=30.2672,
            lon=-97.7431,
            lat_range=(30.18, 30.40),
            lon_range=(-97.85, -97.65),
        ),
    ])

    # Output paths
    output_dir: str = "data"
    output_name: Optional[str] = None  # override events filename (default orders_{days}d.parquet)

    # Kafka settings (parameterized via env in config_from_env)
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "orders"
    stream_speed_multiplier: int = 60  # 1 real minute = 1 simulated hour


# Event types in order lifecycle
EVENT_TYPES = [
    "order_created",
    "kitchen_started",
    "kitchen_finished",
    "order_ready",
    "driver_arrived",
    "driver_picked_up",
    "driver_ping",
    "delivered",
    "order_cancelled",
]


def chaos_from_dict(data: dict) -> ChaosConfig:
    """Build a ChaosConfig from a plain dict (e.g. parsed chaos.yaml)."""
    data = data or {}
    return ChaosConfig(
        enabled=bool(data.get("enabled", True)),
        null_rate=float(data.get("null_rate", 0.05)),
        late_event_rate=float(data.get("late_event_rate", 0.03)),
        duplicate_rate=float(data.get("duplicate_rate", 0.02)),
        malformed_json_rate=float(data.get("malformed_json_rate", 0.01)),
        seed=int(data.get("seed", 42)),
    )


def load_chaos_config(path: Optional[str]) -> ChaosConfig:
    """Load chaos knobs from a YAML file. Falls back to defaults if no path.

    The file may put the rates at top level or nest them under a ``chaos:`` key.
    """
    if not path:
        return ChaosConfig()
    import yaml  # imported lazily so the generator works without pyyaml when no file is used

    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    if "chaos" in data and isinstance(data["chaos"], dict):
        data = data["chaos"]
    return chaos_from_dict(data)


def config_from_env(config: GeneratorConfig) -> GeneratorConfig:
    """Overlay runtime parameters from the environment as a FALLBACK only.

    Honours KAFKA_BOOTSTRAP, KAFKA_TOPIC and DATA_OUTPUT_DIR, but only where the
    config still holds its library default — so an explicitly-set value (e.g. a
    CLI flag) always wins. Precedence: CLI > env > default.
    """
    defaults = GeneratorConfig()
    if (config.kafka_bootstrap_servers == defaults.kafka_bootstrap_servers
            and os.environ.get("KAFKA_BOOTSTRAP")):
        config.kafka_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP"]
    if config.kafka_topic == defaults.kafka_topic and os.environ.get("KAFKA_TOPIC"):
        config.kafka_topic = os.environ["KAFKA_TOPIC"]
    if config.output_dir == defaults.output_dir and os.environ.get("DATA_OUTPUT_DIR"):
        config.output_dir = os.environ["DATA_OUTPUT_DIR"]
    return config
