"""CLI entry point for the food-delivery test data generator.

Run from deploy/data/ as a package:

    python -m generator generate --days 7 --chaos-config chaos.yaml
    python -m generator stream   --speed 60 --kafka kafka:9092 --topic orders
    python -m generator stats
    python -m generator clean

Ported from lakehouse-stack/scripts/testdata/__main__.py and adapted:
  * chaos rates can come from a YAML file (--chaos-config) instead of code,
  * Kafka bootstrap / topic / output dir fall back to env (KAFKA_BOOTSTRAP,
    KAFKA_TOPIC, DATA_OUTPUT_DIR) via config_from_env.
"""

import argparse
import os
import sys
from datetime import date
from pathlib import Path

# Allow `python -m generator` from deploy/data/ and `python generator/__main__.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generator import (  # noqa: E402
    GeneratorConfig,
    ChaosConfig,
    generate_dataset,
    stream_events,
    config_from_env,
    load_chaos_config,
)
from generator.config import chaos_from_dict  # noqa: E402


def _resolve_chaos(args) -> ChaosConfig:
    """Resolve chaos config: explicit file > --chaos-rate derived > disabled.

    Used by BOTH generate and stream so the streaming chaos rates are as
    configurable as the batch ones. The seed (--seed) is threaded onto the
    config so the streaming ChaosMonkey's RNG is deterministic.
    """
    if args.no_chaos:
        cfg = ChaosConfig(enabled=False)
    elif getattr(args, "chaos_config", None):
        cfg = load_chaos_config(args.chaos_config)
    else:
        # Derive the four rates from a single knob, matching the original behaviour.
        cfg = chaos_from_dict({
            "enabled": True,
            "null_rate": args.chaos_rate,
            "late_event_rate": args.chaos_rate * 0.6,
            "duplicate_rate": args.chaos_rate * 0.4,
            "malformed_json_rate": args.chaos_rate * 0.2,
        })
    seed = getattr(args, "seed", None)
    if seed is not None:
        cfg.seed = seed
    return cfg


def cmd_generate(args):
    """Generate test data (dimensions + events parquet)."""
    config = GeneratorConfig(
        start_date=date.fromisoformat(args.start_date),
        days=args.days,
        seed=args.seed,
        base_orders_per_day=args.orders_per_day,
        cancel_rate=args.cancel_rate,
        chaos=_resolve_chaos(args),
        output_dir=args.output,
        output_name=args.name,
    )
    config = config_from_env(config)
    generate_dataset(config)


def cmd_stream(args):
    """Stream events to Kafka."""
    config = GeneratorConfig(
        days=args.days,
        seed=args.seed,
        kafka_bootstrap_servers=args.kafka,
        kafka_topic=args.topic,
        output_dir=args.output,
        output_name=args.name,
        chaos=_resolve_chaos(args),
    )
    config = config_from_env(config)
    # CLI flags win over env when explicitly passed.
    if args.kafka:
        config.kafka_bootstrap_servers = args.kafka
    if args.topic:
        config.kafka_topic = args.topic
    stream_events(config, speed_multiplier=args.speed, start_day=args.start_day)


def cmd_clean(args):
    """Clean generated data."""
    import shutil

    output_dir = Path(args.output)
    dirs_to_clean = [output_dir / "dimensions", output_dir / "events"]

    for d in dirs_to_clean:
        if d.exists():
            shutil.rmtree(d)
            print(f"Removed: {d}")

    print("Clean complete.")


def cmd_stats(args):
    """Show statistics about generated data."""
    from generator.exporter import get_event_stats

    events_path = Path(args.output) / "events"
    parquet_files = list(events_path.glob("*.parquet"))

    if not parquet_files:
        print("No generated data found. Run 'generate' first.")
        return

    for path in parquet_files:
        print(f"\n{path.name}:")
        print("-" * 40)

        stats = get_event_stats(str(path))

        print(f"Total events:  {stats['total_events']:,}")
        print(f"Unique orders: {stats['unique_orders']:,}")
        print(f"Date range:    {stats['date_range']['min'][:10]} to {stats['date_range']['max'][:10]}")

        print("\nEvents by type:")
        for event_type, count in sorted(stats["event_types"].items()):
            print(f"  {event_type}: {count:,}")

        print("\nEvents by location:")
        for loc_id, count in sorted(stats["locations"].items()):
            print(f"  Location {loc_id}: {count:,}")


def main():
    default_kafka = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    default_topic = os.environ.get("KAFKA_TOPIC", "orders")
    default_output = os.environ.get("DATA_OUTPUT_DIR", "data")

    parser = argparse.ArgumentParser(
        description="Food-delivery test data generator (real + messy data)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 7 days of data with chaos knobs from the YAML config
  python -m generator generate --days 7 --chaos-config chaos.yaml

  # Generate 30 days, custom volume, no chaos
  python -m generator generate --days 30 --orders-per-day 500 --no-chaos

  # Stream to Kafka at 100x speed
  python -m generator stream --speed 100 --kafka kafka:9092 --topic orders
""",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Generate command
    gen_parser = subparsers.add_parser("generate", help="Generate test data")
    gen_parser.add_argument("--days", type=int, default=90, help="Number of days to generate")
    gen_parser.add_argument("--seed", type=int, default=42, help="Random seed (determinism)")
    gen_parser.add_argument("--orders-per-day", type=int, default=835, help="Base orders per day")
    gen_parser.add_argument("--cancel-rate", type=float, default=0.0,
                            help="Fraction of orders cancelled mid-lifecycle (emits order_cancelled)")
    gen_parser.add_argument("--start-date", default="2024-01-01", help="Start date (YYYY-MM-DD)")
    gen_parser.add_argument("--output", default=default_output, help="Output directory")
    gen_parser.add_argument("--name", default=None,
                            help="Override events filename (default orders_{days}d.parquet)")
    gen_parser.add_argument("--no-chaos", action="store_true", help="Disable chaos injection")
    gen_parser.add_argument("--chaos-rate", type=float, default=0.05,
                            help="Single-knob chaos rate (ignored if --chaos-config given)")
    gen_parser.add_argument("--chaos-config", default=None,
                            help="Path to chaos YAML config (e.g. chaos.yaml)")
    gen_parser.set_defaults(func=cmd_generate)

    # Stream command
    stream_parser = subparsers.add_parser("stream", help="Stream events to Kafka")
    stream_parser.add_argument("--speed", type=int, default=60, help="Speed multiplier (1=realtime)")
    stream_parser.add_argument("--start-day", type=int, default=0, help="Day to start from")
    stream_parser.add_argument("--kafka", default=default_kafka, help="Kafka bootstrap servers")
    stream_parser.add_argument("--topic", default=default_topic, help="Kafka topic")
    stream_parser.add_argument("--days", type=int, default=90, help="Days in generated file")
    stream_parser.add_argument("--output", default=default_output, help="Data directory")
    stream_parser.add_argument("--name", default=None,
                               help="Events filename to replay (default orders_{days}d.parquet)")
    stream_parser.add_argument("--seed", type=int, default=42,
                               help="Seed for the streaming chaos RNG (determinism)")
    stream_parser.add_argument("--no-chaos", action="store_true", help="Disable chaos during stream")
    stream_parser.add_argument("--chaos-rate", type=float, default=0.05,
                               help="Single-knob chaos rate (ignored if --chaos-config given)")
    stream_parser.add_argument("--chaos-config", default=None,
                               help="Path to chaos YAML config (e.g. chaos.yaml)")
    stream_parser.set_defaults(func=cmd_stream)

    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Clean generated data")
    clean_parser.add_argument("--output", default=default_output, help="Data directory")
    clean_parser.set_defaults(func=cmd_clean)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show data statistics")
    stats_parser.add_argument("--output", default=default_output, help="Data directory")
    stats_parser.set_defaults(func=cmd_stats)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
