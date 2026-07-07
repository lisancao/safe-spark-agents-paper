"""Food-delivery test data generator (order -> kitchen -> driver -> delivery).

Ported from lakehouse-stack/scripts/testdata. Generates realistic order
lifecycle events plus the dimension tables (items, categories, brands,
locations), with a configurable chaos engine that injects nulls, malformed JSON,
duplicates and late/out-of-order events — the 'real + messy data' the reference
architecture's bronze layer needs to exercise quarantine and cleansing.

Two output paths:
  * batch     -> parquet (exporter), loaded into Iceberg by load/load_to_iceberg.py
  * streaming -> Kafka (producer), consumed into Iceberg by load/stream_to_iceberg.py

Usage:
    from generator import GeneratorConfig, generate_dataset
    generate_dataset(GeneratorConfig(days=7))
"""

from .config import (
    GeneratorConfig,
    ChaosConfig,
    ServiceTimes,
    DemandPattern,
    chaos_from_dict,
    load_chaos_config,
    config_from_env,
)
from .dimensions import save_dimensions, get_brands, get_items, get_categories
from .events import generate_all_events, Event
from .exporter import export_events_to_parquet, get_event_stats
from .producer import stream_events, StreamingProducer

__all__ = [
    # Config
    "GeneratorConfig",
    "ChaosConfig",
    "ServiceTimes",
    "DemandPattern",
    "chaos_from_dict",
    "load_chaos_config",
    "config_from_env",
    # Dimensions
    "save_dimensions",
    "get_brands",
    "get_items",
    "get_categories",
    # Events
    "generate_all_events",
    "Event",
    # Export
    "export_events_to_parquet",
    "get_event_stats",
    # Streaming
    "stream_events",
    "StreamingProducer",
    # High-level
    "generate_dataset",
]


def generate_dataset(config: GeneratorConfig = None) -> dict:
    """Generate a complete test dataset (dimensions + events parquet)."""
    if config is None:
        config = GeneratorConfig()

    print("=" * 60)
    print("FOOD-DELIVERY TEST DATA GENERATOR")
    print("=" * 60)
    print()

    # Generate dimensions
    print("1. Generating dimension tables...")
    dim_results = save_dimensions(config)
    for table, count in dim_results.items():
        print(f"   - {table}: {count} records")
    print()

    # Generate events
    print("2. Generating event data...")
    event_results = export_events_to_parquet(config)
    print()

    print("=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print()
    print(f"Dimensions: {config.output_dir}/dimensions/")
    print(f"Events:     {event_results['path']}")
    print(f"Orders:     {event_results['orders']:,}")
    print(f"Events:     {event_results['events']:,}")
    print(f"File size:  {event_results['file_size_mb']} MB")
    print()
    print("Next steps (see deploy/data/README.md):")
    print("  Batch  load: spark-submit load/load_to_iceberg.py --bucket <bucket>")
    print("  Stream load: python -m generator stream  (+ load/stream_to_iceberg.py)")
    print()

    return {
        "dimensions": dim_results,
        "events": event_results,
    }
