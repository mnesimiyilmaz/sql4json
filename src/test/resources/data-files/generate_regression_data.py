#!/usr/bin/env python3
"""Generate deterministic JSON fixtures for SQL4Json regression tests.

The output files are committed to git and asserted exactly by RegressionQueryTest.
They MUST be reproducible byte-for-byte across Python versions, OSes, and locales.
Hence: no random module, no time, no locale-dependent formatting — every value is
derived from integer arithmetic on the row index `i`.

Usage:
    python generate_regression_data.py

Outputs (alongside this script):
    regression_users.json   — 1200 user records, top-level array
    regression_orders.json  — 1500 order records, top-level array
"""
import json
from datetime import date, timedelta
from pathlib import Path

# ───────────────────────────── vocabularies ─────────────────────────────
COUNTRIES = ["US", "UK", "DE", "JP", "FR", "BR", "IN", "CA"]
TIERS     = ["bronze", "silver", "gold", "platinum"]
DEPTS     = ["ENG", "SALES", "MKT", "HR", "FIN", "OPS", "LEGAL", "RND"]
CITIES    = ["NewYork", "London", "Berlin", "Tokyo", "Paris",
             "SaoPaulo", "Mumbai", "Toronto"]
BIOS      = ["loves chess", "writes code", "reads books",
             "cycling fan", "cooking pro", "coffee snob"]
STATUSES  = ["paid", "pending", "refunded", "cancelled", "shipped"]


# ───────────────────────── deterministic generators ─────────────────────
def make_user(i: int) -> dict:
    """Build user row from index i (0-based). All values are pure int arithmetic on i."""
    country_idx = i % len(COUNTRIES)
    tier_idx    = (i * 3) % len(TIERS)
    dept_idx    = (i * 5) % len(DEPTS)
    city_idx    = country_idx  # city aligns with country for a sensible JOIN signal

    days_since_epoch = (i * 9) % (365 * 12)  # 12-year span, predictable
    registered_at = (date(2018, 1, 1) + timedelta(days=days_since_epoch)).isoformat()

    salary = 40000 + (i * 211) % 80000
    score  = (i * 31) % 100  # 0..99, plenty of ties
    age    = 18 + (i * 7) % 60

    rec = {
        "id":            i + 1,
        "username":      f"user_{i:04d}",
        "email":         f"user_{i:04d}@dom{i % 5}.test",
        "age":           age,
        "country":       COUNTRIES[country_idx],
        "dept":          DEPTS[dept_idx],
        "tier":          TIERS[tier_idx],
        "salary":        salary,
        "score":         score,
        # is_active uses %5 (coprime to country period 8) so every country has both
        # active and inactive users — without this Q1's WHERE is_active=true would
        # silently exclude entire countries because of period-4 alignment.
        "is_active":     i % 5 != 0,
        "manager_id":    None if i % 11 == 0 else ((i * 13) % 1200) + 1,
        "registered_at": registered_at,
        "address": {
            "city":     CITIES[city_idx],
            "zip_code": str(10000 + (i * 31) % 90000),
        },
    }
    # Sparse bio: only ~85% of rows have one — exercises COALESCE / IS NOT NULL.
    if i % 7 != 0:
        rec["bio"] = BIOS[i % len(BIOS)]
    return rec


def make_order(j: int) -> dict:
    """Build order row from index j (0-based). FKs into users 1..1200."""
    user_id = (j * 7) % 1200 + 1  # some users get many orders, some get none
    # Amount in cents-style: integer * 0.01 — keeps decimals clean for ROUND.
    amount = round(((j * 23) % 50000) * 0.01 + (j % 7) * 0.25, 2)
    days_since_epoch = (j * 3) % (365 * 5)
    placed_at = (date(2022, 1, 1) + timedelta(days=days_since_epoch)).isoformat()
    return {
        "order_id":  j + 1,
        "user_id":   user_id,
        "amount":    amount,
        "status":    STATUSES[j % len(STATUSES)],
        "placed_at": placed_at,
    }


# ───────────────────────────── entry point ──────────────────────────────
def main() -> None:
    n_users  = 1200
    n_orders = 1500

    users  = [make_user(i)  for i in range(n_users)]
    orders = [make_order(j) for j in range(n_orders)]

    out_dir = Path(__file__).parent
    users_path  = out_dir / "regression_users.json"
    orders_path = out_dir / "regression_orders.json"

    # Compact JSON (no whitespace) — the file is treated as a binary fixture, not
    # human-edited. Compactness halves the on-disk size vs. indented output.
    users_path.write_text(
        json.dumps(users, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8", newline="")
    orders_path.write_text(
        json.dumps(orders, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8", newline="")

    print(f"Wrote {users_path.name:<26} {users_path.stat().st_size:>9,} bytes "
          f"({n_users} records)")
    print(f"Wrote {orders_path.name:<26} {orders_path.stat().st_size:>9,} bytes "
          f"({n_orders} records)")


if __name__ == "__main__":
    main()
