"""
generator.py
============
Synthetic data generator for the Real-Time Mobility Data Platform.

Produces four event streams that mirror Uber-style operational data:
  - ride_events         : trip lifecycle records
  - driver_location_events : GPS pings from active drivers
  - payment_events      : payment processing records
  - cancellation_events : cancelled ride records

Usage:
    python generator.py --rides 100000 --output ./output
    python generator.py --rides 1000000 --format json
"""

import argparse
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import Iterator

# ---------------------------------------------------------------------------
# City bounding boxes — realistic lat/lng ranges per city
# ---------------------------------------------------------------------------
CITIES = {
    "New York":  {"lat": (40.4774, 40.9176), "lng": (-74.2591, -73.7004)},
    "San Francisco": {"lat": (37.6398, 37.9298), "lng": (-122.5548, -122.3482)},
    "Chicago":   {"lat": (41.6441, 42.0230), "lng": (-87.9402, -87.5234)},
    "Los Angeles": {"lat": (33.7037, 34.3373), "lng": (-118.6682, -118.1553)},
    "Miami":     {"lat": (25.5584, 25.9582), "lng": (-80.6734, -80.1186)},
    "Seattle":   {"lat": (47.4919, 47.7341), "lng": (-122.4360, -122.2368)},
    "Austin":    {"lat": (30.0986, 30.5168), "lng": (-97.9209, -97.5698)},
    "Boston":    {"lat": (42.2279, 42.3970), "lng": (-71.1912, -70.9234)},
}

RIDE_STATUSES       = ["completed", "cancelled", "in_progress"]
PAYMENT_METHODS     = ["card", "cash", "uber_cash", "paypal"]
PAYMENT_STATUSES    = ["success", "failed", "pending", "refunded"]
CANCEL_REASONS      = ["rider_cancelled", "driver_cancelled", "no_driver_available", "rider_no_show"]
CANCELLED_BY        = ["rider", "driver", "system"]
DRIVER_AVAIL        = ["available", "on_trip", "offline"]

# ---------------------------------------------------------------------------
# Surge multiplier by hour — peaks at morning/evening rush and late night
# ---------------------------------------------------------------------------
SURGE_BY_HOUR = {
    0: 1.5, 1: 1.8, 2: 2.0, 3: 1.5, 4: 1.2, 5: 1.0,
    6: 1.2, 7: 1.8, 8: 2.5, 9: 1.5, 10: 1.0, 11: 1.0,
    12: 1.2, 13: 1.0, 14: 1.0, 15: 1.2, 16: 1.5, 17: 2.0,
    18: 2.5, 19: 2.0, 20: 1.5, 21: 1.5, 22: 1.8, 23: 2.0,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def random_coords(city: str) -> tuple[float, float]:
    bounds = CITIES[city]
    lat = round(random.uniform(*bounds["lat"]), 6)
    lng = round(random.uniform(*bounds["lng"]), 6)
    return lat, lng


def random_timestamp(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def surge_for(ts: datetime) -> float:
    base = SURGE_BY_HOUR[ts.hour]
    jitter = random.uniform(-0.1, 0.2)
    return round(max(1.0, base + jitter), 2)


def fare_amount(distance_miles: float, surge: float) -> float:
    base_fare   = 2.50
    per_mile    = 1.25
    per_minute  = 0.25
    duration    = distance_miles / 15 * 60          # assume avg 15 mph
    raw         = base_fare + per_mile * distance_miles + per_minute * duration
    return round(raw * surge, 2)


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------

def generate_ride_events(
    n: int,
    rider_pool: list[str],
    driver_pool: list[str],
    start: datetime,
    end: datetime,
) -> Iterator[dict]:
    for _ in range(n):
        city                 = random.choice(list(CITIES))
        pickup_lat, pickup_lng   = random_coords(city)
        dropoff_lat, dropoff_lng = random_coords(city)
        request_time         = random_timestamp(start, end)
        wait_minutes         = random.uniform(1, 8)
        pickup_time          = request_time + timedelta(minutes=wait_minutes)
        distance             = round(random.uniform(0.5, 25.0), 2)
        duration_minutes     = round(distance / 15 * 60 + random.uniform(-2, 5), 1)
        dropoff_time         = pickup_time + timedelta(minutes=duration_minutes)
        surge                = surge_for(request_time)
        status               = random.choices(
            RIDE_STATUSES, weights=[0.82, 0.15, 0.03], k=1
        )[0]

        yield {
            "ride_id":          random_id("ride"),
            "rider_id":         random.choice(rider_pool),
            "driver_id":        random.choice(driver_pool),
            "city":             city,
            "pickup_lat":       pickup_lat,
            "pickup_lng":       pickup_lng,
            "dropoff_lat":      dropoff_lat,
            "dropoff_lng":      dropoff_lng,
            "request_time":     request_time.isoformat(),
            "pickup_time":      pickup_time.isoformat(),
            "dropoff_time":     dropoff_time.isoformat() if status == "completed" else None,
            "ride_status":      status,
            "fare_amount":      fare_amount(distance, surge) if status == "completed" else None,
            "surge_multiplier": surge,
            "distance_miles":   distance,
            "duration_minutes": duration_minutes,
            "payment_type":     random.choice(PAYMENT_METHODS),
            "event_timestamp":  request_time.isoformat(),
        }


def generate_driver_location_events(
    driver_pool: list[str],
    start: datetime,
    end: datetime,
    pings_per_driver: int = 50,
) -> Iterator[dict]:
    for driver_id in driver_pool:
        city = random.choice(list(CITIES))
        for _ in range(pings_per_driver):
            lat, lng = random_coords(city)
            ts = random_timestamp(start, end)
            yield {
                "event_id":           random_id("loc"),
                "driver_id":          driver_id,
                "city":               city,
                "lat":                lat,
                "lng":                lng,
                "availability_status": random.choices(
                    DRIVER_AVAIL, weights=[0.35, 0.55, 0.10], k=1
                )[0],
                "event_timestamp":    ts.isoformat(),
            }


def generate_payment_events(ride_events: list[dict]) -> Iterator[dict]:
    for ride in ride_events:
        if ride["ride_status"] != "completed":
            continue
        ts = datetime.fromisoformat(ride["dropoff_time"]) + timedelta(seconds=random.randint(5, 60))
        status = random.choices(
            PAYMENT_STATUSES, weights=[0.92, 0.04, 0.02, 0.02], k=1
        )[0]
        yield {
            "payment_id":     random_id("pay"),
            "ride_id":        ride["ride_id"],
            "payment_status": status,
            "amount":         ride["fare_amount"],
            "currency":       "USD",
            "payment_method": ride["payment_type"],
            "event_timestamp": ts.isoformat(),
        }


def generate_cancellation_events(ride_events: list[dict]) -> Iterator[dict]:
    for ride in ride_events:
        if ride["ride_status"] != "cancelled":
            continue
        ts = datetime.fromisoformat(ride["request_time"]) + timedelta(seconds=random.randint(30, 300))
        cancelled_by = random.choice(CANCELLED_BY)
        yield {
            "cancellation_id": random_id("cancel"),
            "ride_id":         ride["ride_id"],
            "cancelled_by":    cancelled_by,
            "reason":          random.choice(CANCEL_REASONS),
            "event_timestamp": ts.isoformat(),
        }


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_csv(records: list[dict], path: str) -> int:
    if not records:
        return 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    return len(records)


def write_json(records: list[dict], path: str) -> int:
    if not records:
        return 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(records, f, indent=2)
    return len(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(n_rides: int, output_dir: str, fmt: str, seed: int) -> None:
    random.seed(seed)

    end   = datetime.now().replace(second=0, microsecond=0)
    start = end - timedelta(days=90)   # 90 days of historical data

    # Build fixed rider/driver pools so IDs are reusable across event types
    n_riders  = max(1000, n_rides // 5)
    n_drivers = max(500,  n_rides // 20)
    rider_pool  = [random_id("rider")  for _ in range(n_riders)]
    driver_pool = [random_id("driver") for _ in range(n_drivers)]

    print(f"Generating {n_rides:,} ride events  ({n_riders:,} riders, {n_drivers:,} drivers)...")

    rides = list(generate_ride_events(n_rides, rider_pool, driver_pool, start, end))
    print(f"  ride_events           : {len(rides):>10,}")

    location_pings = list(generate_driver_location_events(driver_pool, start, end))
    print(f"  driver_location_events: {location_pings.__len__():>10,}")

    payments = list(generate_payment_events(rides))
    print(f"  payment_events        : {len(payments):>10,}")

    cancellations = list(generate_cancellation_events(rides))
    print(f"  cancellation_events   : {len(cancellations):>10,}")

    write_fn = write_json if fmt == "json" else write_csv
    ext      = fmt

    totals = {
        "ride_events":            write_fn(rides,         f"{output_dir}/ride_events.{ext}"),
        "driver_location_events": write_fn(location_pings, f"{output_dir}/driver_location_events.{ext}"),
        "payment_events":         write_fn(payments,       f"{output_dir}/payment_events.{ext}"),
        "cancellation_events":    write_fn(cancellations,  f"{output_dir}/cancellation_events.{ext}"),
    }

    total_records = sum(totals.values())
    print(f"\nWrote {total_records:,} total records to {output_dir}/")
    for name, count in totals.items():
        print(f"  {name:<30} {count:>10,} records")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mobility Platform — Synthetic Data Generator")
    parser.add_argument("--rides",   type=int, default=100_000, help="Number of ride events to generate")
    parser.add_argument("--output",  type=str, default="./output", help="Output directory")
    parser.add_argument("--format",  type=str, default="csv", choices=["csv", "json"], help="Output format")
    parser.add_argument("--seed",    type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    run(args.rides, args.output, args.format, args.seed)
