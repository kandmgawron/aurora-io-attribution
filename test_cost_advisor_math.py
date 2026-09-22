#!/usr/bin/env python3
"""Offline self-check for aurora_io_cost_advisor pure math. No AWS, no DB.

Verifies the logic that would silently produce wrong recommendations if broken:
delta computation, share normalisation, monthly extrapolation, per-DB pricing
under both storage modes, and the verdict direction.

Run: python3 test_cost_advisor_math.py
"""
from decimal import Decimal

import aurora_io_cost_advisor as adv


# Realistic us-east-1 pricing (from the Pricing API at build time).
PRICING = {
    "std_storage_gb_mo": Decimal("0.10"),
    "opt_storage_gb_mo": Decimal("0.225"),
    "std_io_per_request": Decimal("0.0000002"),
    "std_acu_hr": Decimal("0.12"),
    "opt_acu_hr": Decimal("0.16"),
}


def test_deltas_and_reset_guard():
    first = {"app": {"reads": 100, "writes": 10}, "an": {"reads": 50, "writes": 5}}
    second = {"app": {"reads": 400, "writes": 40}, "an": {"reads": 100, "writes": 5}}
    d = adv.deltas_from_samples(first, second)
    assert d["app"]["reads"] == 300 and d["app"]["writes"] == 30, d
    assert d["an"]["reads"] == 50 and d["an"]["writes"] == 0, d
    # counter reset clamps to 0
    r = adv.deltas_from_samples({"app": {"reads": 500, "writes": 5}},
                                {"app": {"reads": 10, "writes": 5}})
    assert r["app"]["reads"] == 0, r
    # new db with no baseline is skipped
    assert "new" not in adv.deltas_from_samples(first, {**second, "new": {"reads": 9, "writes": 9}})


def test_shares_sum_to_one():
    deltas = {"a": {"reads": 300, "writes": 30}, "b": {"reads": 100, "writes": 10}}
    s = adv.io_shares(deltas)
    assert abs(s["a"]["read_share"] - Decimal("0.75")) < Decimal("1e-9"), s
    assert abs(s["b"]["read_share"] - Decimal("0.25")) < Decimal("1e-9"), s
    # shares must each sum to 1 across dbs
    assert abs(sum(v["read_share"] for v in s.values()) - 1) < Decimal("1e-9")
    assert abs(sum(v["write_share"] for v in s.values()) - 1) < Decimal("1e-9")
    assert abs(sum(v["io_share"] for v in s.values()) - 1) < Decimal("1e-9")


def test_shares_all_zero_no_div_zero():
    s = adv.io_shares({"a": {"reads": 0, "writes": 0}, "b": {"reads": 0, "writes": 0}})
    assert all(v["read_share"] == 0 for v in s.values()), s
    assert all(v["io_share"] == 0 for v in s.values()), s


def test_scale_to_month():
    # 3600 requests in a 60s window -> per hour 216000 -> *730h
    m = adv.scale_to_month(3600, 60)
    assert abs(m - Decimal("3600") * (Decimal("730") * 3600 / Decimal("60"))) < 1, m
    assert adv.scale_to_month(100, 0) == 0  # guard


def test_normalize_period_to_month():
    # A REAL 30-day (720h) total is barely rescaled to a 730h month (~1.014x),
    # i.e. it uses the real data rather than extrapolating.
    real_30d = Decimal("218374")  # real VolumeReadIOPs total observed
    m = adv.normalize_period_to_month(real_30d, 720)
    assert m == real_30d * (Decimal("730") / Decimal("720")), m
    assert Decimal("1.01") < (m / real_30d) < Decimal("1.02"), m
    # A 730h period is a no-op (factor exactly 1).
    assert adv.normalize_period_to_month(Decimal("1000"), 730) == Decimal("1000")
    # Guard against zero-length period.
    assert adv.normalize_period_to_month(Decimal("500"), 0) == 0
    # Contrast with scale_to_month: extrapolating 60s balloons the number,
    # normalising a real month barely changes it.
    assert adv.normalize_period_to_month(real_30d, 720) < adv.scale_to_month(real_30d, 60)


def test_io_heavy_db_prefers_optimized():
    # A read-heavy DB with huge billed I/O should favour I/O-Optimized:
    # I/O charges dominate and I/O-Optimized zeroes them out.
    share = {"read_share": Decimal("1.0"), "write_share": Decimal("1.0"),
             "io_share": Decimal("1.0")}
    billed = {"reads": Decimal("2_000_000_000"), "writes": Decimal("100_000_000")}
    costs = adv.price_database(share, billed, storage_gb=Decimal("100"),
                               acu_hours_month=Decimal("2000"), pricing=PRICING)
    cheaper, pct = adv.verdict(costs["standard"]["total"], costs["optimized"]["total"])
    assert cheaper == "I/O-Optimized", costs
    assert costs["optimized"]["io"] == 0
    assert pct > 0


def test_low_io_db_prefers_standard():
    # A tiny-I/O DB should favour Standard: I/O charges are negligible, so the
    # 2.25x storage + 1.33x compute premium of I/O-Optimized loses.
    share = {"read_share": Decimal("1.0"), "write_share": Decimal("1.0"),
             "io_share": Decimal("1.0")}
    billed = {"reads": Decimal("1000"), "writes": Decimal("500")}
    costs = adv.price_database(share, billed, storage_gb=Decimal("50"),
                               acu_hours_month=Decimal("730"), pricing=PRICING)
    cheaper, _ = adv.verdict(costs["standard"]["total"], costs["optimized"]["total"])
    assert cheaper == "Standard", costs


def test_verdict_tie_prefers_standard():
    # exact tie -> Standard (no reason to pay the premium)
    cheaper, pct = adv.verdict(Decimal("100"), Decimal("100"))
    assert cheaper == "Standard" and pct == 0


def test_optimized_storage_and_compute_premium_applied():
    share = {"read_share": Decimal("0.5"), "write_share": Decimal("0.5"),
             "io_share": Decimal("0.5")}
    billed = {"reads": Decimal("0"), "writes": Decimal("0")}
    costs = adv.price_database(share, billed, storage_gb=Decimal("100"),
                               acu_hours_month=Decimal("730"), pricing=PRICING)
    # storage: std 100*0.10=10 ; opt 100*0.225=22.5
    assert costs["standard"]["storage"] == Decimal("10.0"), costs
    assert costs["optimized"]["storage"] == Decimal("22.5"), costs
    # compute: std 730*0.12=87.6 ; opt 730*0.16=116.8
    assert costs["standard"]["compute"] == Decimal("87.60"), costs
    assert costs["optimized"]["compute"] == Decimal("116.80"), costs


def main():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  ok: {name}")
    print("OK: cost advisor math checks passed")


if __name__ == "__main__":
    main()
