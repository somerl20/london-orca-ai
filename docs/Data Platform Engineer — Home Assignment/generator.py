#!/usr/bin/env python3
"""
Sensor data file generator — DE Home Assignment

Continuously drops JSON files into a local directory at a configurable rate.
Each file contains an array of sensor records from one source_id + measurement type.

Usage:
    python generator.py --output-dir ./input --rate 2 --size small
    python generator.py --output-dir ./input --rate 1 --size large --sources 20

File format:
    [
        {
            "source_id": "ship-01",
            "measurement": "gps",
            "timestamp": "2026-05-23T10:14:00Z",
            "values": { ... }
        },
        ...
    ]

File naming:
    {source_id}_{measurement}_{timestamp_utc}.json
"""

import argparse
import json
import random
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Size tiers: approximate row counts per file
# ---------------------------------------------------------------------------
ROWS_BY_SIZE: dict[str, int] = {
    "small":  100,      #  ~15 KB  — quick to generate and process
    "medium": 30_000,   #  ~5  MB  — normal operating size
    "large":  250_000,  #  ~45 MB  — tests chunking and batching logic
}


# ---------------------------------------------------------------------------
# Measurement schemas
# Each returns a dict of sensor values for one reading
# ---------------------------------------------------------------------------
def _gps() -> dict:
    return {
        "lat": round(random.uniform(-90.0, 90.0), 6),
        "lon": round(random.uniform(-180.0, 180.0), 6),
        "speed_knots": round(random.uniform(0.0, 25.0), 2),
    }


MEASUREMENT_SCHEMAS: dict[str, Callable[[], dict[str, float]]] = {
    "gps": _gps,
}


# ---------------------------------------------------------------------------
# File generation
# ---------------------------------------------------------------------------
def _write_file(output_dir: Path, source_id: str, measurement: str, row_count: int) -> Path:
    """Write a JSON array file for one source_id + measurement.

    Uses streaming writes to keep memory usage flat regardless of row_count.
    """
    now = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%dT%H%M%SZ")
    filename = f"{source_id}_{measurement}_{ts_str}.json"
    filepath = output_dir / filename

    schema_fn = MEASUREMENT_SCHEMAS[measurement]
    tmp_path = filepath.with_suffix(".json.tmp")

    with open(tmp_path, "w") as f:
        f.write("[\n")
        for i in range(row_count):
            ts = (now + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            record = {
                "source_id": source_id,
                "measurement": measurement,
                "timestamp": ts,
                "values": schema_fn(),
            }
            line = json.dumps(record)
            f.write(line)
            if i < row_count - 1:
                f.write(",\n")
        f.write("\n]\n")

    # Atomic rename — consumers never see a partial file
    tmp_path.rename(filepath)
    return filepath


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sensor data file generator for the DE home assignment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        metavar="PATH",
        help="Local directory to drop JSON files into (created if missing)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=2.0,
        metavar="N",
        help="Files per minute (default: 2)",
    )
    parser.add_argument(
        "--size",
        choices=["small", "medium", "large"],
        default="small",
        help="File size tier — small (~15 KB), medium (~5 MB), large (~45 MB) (default: small)",
    )
    parser.add_argument(
        "--sources",
        type=int,
        default=10,
        metavar="N",
        help="Number of distinct source IDs to simulate (default: 10)",
    )
    parser.add_argument(
        "--measurements",
        default="gps",
        metavar="LIST",
        help="Comma-separated measurement types (default: gps)",
    )
    args = parser.parse_args()

    # --- Validate inputs ---
    measurements = [m.strip() for m in args.measurements.split(",") if m.strip()]
    unknown = [m for m in measurements if m not in MEASUREMENT_SCHEMAS]
    if unknown:
        parser.error(
            f"Unknown measurement type(s): {unknown}. "
            f"Valid options: {sorted(MEASUREMENT_SCHEMAS)}"
        )

    if args.rate <= 0:
        parser.error("--rate must be greater than 0")

    if args.sources < 1:
        parser.error("--sources must be at least 1")

    # --- Setup ---
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    source_ids = [f"ship-{i:02d}" for i in range(1, args.sources + 1)]
    row_count = ROWS_BY_SIZE[args.size]
    interval_seconds = 60.0 / args.rate

    print("=" * 60)
    print("  Sensor Data Generator")
    print("=" * 60)
    print(f"  Output dir   : {output_dir.resolve()}")
    print(f"  Rate         : {args.rate} file(s)/min  (one every {interval_seconds:.1f}s)")
    print(f"  Size tier    : {args.size}  ({row_count:,} rows/file)")
    print(f"  Sources      : {len(source_ids)}  ({source_ids[0]} … {source_ids[-1]})")
    print(f"  Measurements : {measurements}")
    print("=" * 60)
    print("Press Ctrl+C to stop.\n")

    file_count = 0
    try:
        while True:
            source_id = random.choice(source_ids)
            measurement = random.choice(measurements)

            t0 = time.monotonic()
            filepath = _write_file(output_dir, source_id, measurement, row_count)
            elapsed = time.monotonic() - t0

            file_count += 1
            size_kb = filepath.stat().st_size / 1024
            print(
                f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                f"#{file_count:04d}  {filepath.name}  "
                f"({size_kb:,.0f} KB | {row_count:,} rows | {elapsed:.2f}s)"
            )

            # Sleep for the remainder of the interval
            sleep_for = max(0.0, interval_seconds - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        print(f"\nStopped. {file_count} file(s) produced in {output_dir.resolve()}")


if __name__ == "__main__":
    main()
