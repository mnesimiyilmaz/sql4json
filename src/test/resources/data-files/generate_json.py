#!/usr/bin/env python3
"""
JSON File Generator
-------------------
Generates a JSON file of approximately the target size (in MB).
Each object has ~10+ fields, with random field dropping on some objects,
and nested structures up to 3 levels deep.

Usage:
    python generate_json.py --size 10          # 10 MB
    python generate_json.py --size 100         # 100 MB
    python generate_json.py --size 500         # 500 MB
    python generate_json.py -s 50 -o data.json # 50 MB, custom filename
"""

import json
import argparse
import random
import string
import uuid
import os
import sys
import time
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rand_str(min_len=5, max_len=30):
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=random.randint(min_len, max_len)))

def rand_email():
    user = "".join(random.choices(string.ascii_lowercase, k=random.randint(4, 10)))
    domains = ["gmail.com", "outlook.com", "yahoo.com", "company.io", "example.org"]
    return f"{user}@{random.choice(domains)}"

def rand_date(start_year=2015, end_year=2026):
    start = datetime(start_year, 1, 1)
    delta = (datetime(end_year, 12, 31) - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()

def rand_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

def rand_tags(max_tags=6):
    pool = [
        "python", "data", "ml", "api", "cloud", "devops", "security",
        "frontend", "backend", "database", "analytics", "mobile", "iot",
        "networking", "testing", "ci-cd", "monitoring", "logging",
    ]
    return random.sample(pool, k=random.randint(1, min(max_tags, len(pool))))


# ---------------------------------------------------------------------------
# Nested object builders  (depth up to 3)
# ---------------------------------------------------------------------------

def build_address():
    """Depth-2 nested object."""
    return {
        "street": f"{random.randint(1, 9999)} {rand_str(6, 15)} St",
        "city": random.choice(["New York", "London", "Berlin", "Tokyo", "Sydney", "Toronto", "Mumbai", "São Paulo"]),
        "state": rand_str(2, 2).upper(),
        "zip_code": str(random.randint(10000, 99999)),
        "country": random.choice(["US", "UK", "DE", "JP", "AU", "CA", "IN", "BR"]),
        "geo": {                       # depth-3
            "lat": round(random.uniform(-90, 90), 6),
            "lng": round(random.uniform(-180, 180), 6),
            "accuracy_m": round(random.uniform(1, 500), 2),
        },
    }

def build_metadata():
    """Depth-2 nested object with its own depth-3 child."""
    return {
        "created_at": rand_date(),
        "updated_at": rand_date(),
        "version": random.randint(1, 20),
        "source": random.choice(["api", "web", "mobile", "import", "migration"]),
        "audit": {                     # depth-3
            "last_reviewed_by": rand_str(5, 12),
            "review_status": random.choice(["approved", "pending", "rejected", "needs_revision"]),
            "comments_count": random.randint(0, 50),
        },
    }

def build_preferences():
    """Depth-2 nested object."""
    return {
        "theme": random.choice(["dark", "light", "system"]),
        "language": random.choice(["en", "es", "fr", "de", "ja", "pt", "hi"]),
        "notifications": {             # depth-3
            "email": random.choice([True, False]),
            "sms": random.choice([True, False]),
            "push": random.choice([True, False]),
            "frequency": random.choice(["instant", "daily", "weekly"]),
        },
    }


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

ALL_FIELDS = [
    "id",
    "name",
    "email",
    "age",
    "is_active",
    "score",
    "ip_address",
    "tags",
    "bio",
    "registered_at",
    "address",       # nested depth 2-3
    "metadata",      # nested depth 2-3
    "preferences",   # nested depth 2-3
]

def build_record(drop_chance=0.25, min_fields=7):
    """
    Build a single JSON record.
    - Always includes 'id'.
    - Randomly drops some fields (controlled by drop_chance).
    - Guarantees at least `min_fields` fields per object.
    """
    record = {}

    # Decide which optional fields to keep
    optional = ALL_FIELDS[1:]  # everything except id
    kept = [f for f in optional if random.random() > drop_chance]

    # Ensure minimum field count
    if len(kept) + 1 < min_fields:  # +1 for id
        extras = random.sample(
            [f for f in optional if f not in kept],
            k=min(min_fields - 1 - len(kept), len(optional) - len(kept)),
        )
        kept.extend(extras)

    record["id"] = str(uuid.uuid4())

    field_builders = {
        "name": lambda: rand_str(6, 25),
        "email": rand_email,
        "age": lambda: random.randint(18, 85),
        "is_active": lambda: random.choice([True, False]),
        "score": lambda: round(random.uniform(0, 100), 4),
        "ip_address": rand_ip,
        "tags": rand_tags,
        "bio": lambda: rand_str(30, 120),
        "registered_at": rand_date,
        "address": build_address,
        "metadata": build_metadata,
        "preferences": build_preferences,
    }

    for field in kept:
        record[field] = field_builders[field]()

    return record


# ---------------------------------------------------------------------------
# Streaming writer – keeps memory low for large files
# ---------------------------------------------------------------------------

def generate_json_file(target_mb: float, output_path: str, pretty: bool = False):
    target_bytes = int(target_mb * 1024 * 1024)
    written = 0
    count = 0
    indent = 2 if pretty else None
    sep = ",\n" if pretty else ","
    start = time.time()

    print(f"Target  : {target_mb} MB ({target_bytes:,} bytes)")
    print(f"Output  : {output_path}")
    print(f"Pretty  : {pretty}")
    print(f"Generating …")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("[\n" if pretty else "[")
        written += 1

        while written < target_bytes:
            rec = build_record()
            blob = json.dumps(rec, indent=indent, ensure_ascii=False)

            if count > 0:
                chunk = sep + blob
            else:
                chunk = blob

            f.write(chunk)
            written += len(chunk.encode("utf-8"))
            count += 1

            # progress every 5 000 records
            if count % 5000 == 0:
                pct = min(written / target_bytes * 100, 100)
                print(f"  {count:>10,} records | {written / 1024 / 1024:>8.1f} MB | {pct:5.1f}%", end="\r")

        f.write("\n]" if pretty else "]")
        written += 1

    elapsed = time.time() - start
    actual_mb = os.path.getsize(output_path) / 1024 / 1024

    print(f"\n{'─' * 50}")
    print(f"Records : {count:,}")
    print(f"Size    : {actual_mb:.2f} MB")
    print(f"Time    : {elapsed:.1f}s")
    print(f"Speed   : {actual_mb / elapsed:.1f} MB/s")
    print(f"File    : {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate a JSON file of a target size.")
    parser.add_argument("-s", "--size", type=float, required=True,
                        help="Target file size in MB (e.g. 10, 100, 500)")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Output file path (default: data_<size>mb.json)")
    parser.add_argument("--pretty", action="store_true",
                        help="Pretty-print JSON (increases file size overhead)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducibility")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    output = args.output or f"data_{int(args.size)}mb.json"

    generate_json_file(
        target_mb=args.size,
        output_path=output,
        pretty=args.pretty,
    )


if __name__ == "__main__":
    main()
