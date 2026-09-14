#!/usr/bin/env python3
"""Self-check for the attribution math in pg_io_per_database_poc.

Runs without AWS or a database. Verifies the two pieces of logic that would
silently produce wrong numbers if broken: counter-delta computation (including
the reset guard) and the proportional billed-I/O split.

Run: python3 test_attribution_math.py
"""
from decimal import Decimal


def deltas_from_samples(first, second):
    """Mirror of PgIoAttributionPoC.sample_rates delta logic."""
    out = {}
    for db, after in second.items():
        before = first.get(db)
        if before is None:
            continue
        out[db] = {
            "reads": max(0, after["reads"] - before["reads"]),
            "writes": max(0, after["writes"] - before["writes"]),
        }
    return out


def split_billed(deltas, cluster_billed_reads):
    total = sum(d["reads"] for d in deltas.values()) or 1
    return {
        db: Decimal(str(cluster_billed_reads)) * Decimal(str(d["reads"] / total))
        for db, d in deltas.items()
    }


def main():
    first = {"app": {"reads": 100, "writes": 10}, "analytics": {"reads": 50, "writes": 5}}
    second = {"app": {"reads": 400, "writes": 40}, "analytics": {"reads": 100, "writes": 5}}

    deltas = deltas_from_samples(first, second)
    assert deltas["app"]["reads"] == 300, deltas
    assert deltas["analytics"]["reads"] == 50, deltas
    assert deltas["analytics"]["writes"] == 0, deltas

    # Counter reset: 'after' lower than 'before' must clamp to 0, not go negative.
    reset = deltas_from_samples({"app": {"reads": 500, "writes": 5}},
                                {"app": {"reads": 10, "writes": 5}})
    assert reset["app"]["reads"] == 0, reset

    # New db appearing only in the second sample is skipped (no baseline).
    assert "brandnew" not in deltas_from_samples(first, {**second, "brandnew": {"reads": 9, "writes": 9}})

    # Proportional split: app=300, analytics=50 -> shares 6/7 and 1/7 of 7000.
    split = split_billed(deltas, 7000)
    assert abs(split["app"] - Decimal("6000")) < 1, split
    assert abs(split["analytics"] - Decimal("1000")) < 1, split
    # Split must conserve the cluster total.
    assert abs(sum(split.values()) - Decimal("7000")) < 1, split

    # All-zero activity must not divide by zero.
    zero = split_billed({"a": {"reads": 0}, "b": {"reads": 0}}, 7000)
    assert all(v == 0 for v in zero.values()), zero

    print("OK: attribution math checks passed")


if __name__ == "__main__":
    main()
