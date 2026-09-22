#!/usr/bin/env python3
"""
Aurora PostgreSQL per-database I/O attribution + Standard vs I/O-Optimized
cost advisor. Single tool: every run does BOTH the attribution and the pricing
analysis in one pass.

What it does
------------
Aurora storage is billed at the cluster level and the Standard / I/O-Optimized
toggle is a *cluster-wide* setting -- you cannot put one logical database on a
different storage mode from its neighbours in the same cluster. But you CAN
split databases into separate clusters for cost optimisation: park the
I/O-heavy databases on an I/O-Optimized cluster and keep the low-I/O ones on a
Standard cluster.

Every run prints two sections:

  Part 1 -- Per-database I/O attribution:
    Samples pg_stat_database (via the RDS Data API) twice to get each logical
    database's share of physical read + write activity, pulls the cluster's
    real billed I/O from CloudWatch, and splits that billed I/O across
    databases by their activity share. This is the evidence the pricing rests
    on.

  Part 2 -- Standard vs I/O-Optimized cost recommendation:
    Extrapolates the sampled window to a monthly rate, prices each database
    *as if it were its own cluster* under both Standard and I/O-Optimized
    (using live AWS Pricing API data + VolumeBytesUsed + ServerlessV2 ACU),
    and emits a per-database verdict plus the cluster total both ways and
    cluster-split guidance.

This tells you which side of the line each database falls on.

Honesty / caveats
-----------------
* Per-database billed I/O is an ESTIMATE. Aurora only exposes billed I/O at the
  cluster level; there is no per-database billed metric anywhere. We split the
  cluster total by each DB's logical-I/O share from pg_stat_database.
* Read attribution (blks_read) is reliable. Write attribution uses row activity
  (tup_inserted+updated+deleted) as a coarse proxy -- pg_stat_database has no
  block-level write counter -- so write shares are softer than read shares.
* Storage is split by each DB's on-disk size (pg_database_size), not by the
  cluster VolumeBytesUsed (which includes overhead); the two are reconciled.
* Serverless compute (ACU) is a shared cluster resource and is NOT attributable
  per database. It is reported at cluster level and, for the per-DB "as its own
  cluster" view, allocated by total-I/O share purely so the comparison includes
  a compute term. Treat per-DB compute as indicative, not billable truth.
* The per-DB verdict answers "if this DB were alone on a cluster, which mode is
  cheaper" -- which is exactly the signal you need to decide how to GROUP
  databases into Standard vs I/O-Optimized clusters.

Aurora PostgreSQL only (relies on pg_stat_database). Aurora MySQL is rejected.

Transport: RDS Data API, so it runs from anywhere with IAM creds -- no VPC path
or psql/psycopg2 needed. Cluster must have the Data API enabled and a Secrets
Manager secret with the master credentials.

Requires: python3 -m pip install boto3
"""

import re
import sys
import json
import time
import argparse
from decimal import Decimal
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError


SYSTEM_DATABASES = {"template0", "template1", "rdsadmin", "postgres"}
HOURS_PER_MONTH = Decimal("730")  # AWS billing convention

# region -> Pricing API usageType prefix (empty for us-east-1). Mirrors the
# usage-report region codes AWS uses in usageType strings.
REGION_CODES = {
    "us-east-1": "", "us-east-2": "USE2-", "us-west-1": "USW1-", "us-west-2": "USW2-",
    "af-south-1": "AFS1-", "ap-east-1": "APE1-", "ap-south-1": "APS3-",
    "ap-south-2": "APS5-", "ap-northeast-1": "APN1-", "ap-northeast-2": "APN2-",
    "ap-northeast-3": "APN3-", "ap-southeast-1": "APS1-", "ap-southeast-2": "APS2-",
    "ap-southeast-3": "APS4-", "ap-southeast-4": "APS6-", "ca-central-1": "CAN1-",
    "eu-central-1": "EUC1-", "eu-central-2": "EUC2-", "eu-west-1": "EU-",
    "eu-west-2": "EUW2-", "eu-west-3": "EUW3-", "eu-north-1": "EUN1-",
    "eu-south-1": "EUS1-", "eu-south-2": "EUS2-", "il-central-1": "ILC1-",
    "me-central-1": "MEC1-", "me-south-1": "MES1-", "sa-east-1": "SAE1-",
}


# =========================================================================
# Pure functions (unit-tested offline in test_cost_advisor_math.py)
# =========================================================================

def deltas_from_samples(first, second):
    """Per-db counter deltas between two pg_stat_database snapshots.

    Guards against counter resets (failover / stats reset) by clamping to 0,
    and skips databases with no baseline in the first sample.
    """
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


def io_shares(deltas):
    """Return per-db read/write/combined shares of total logical I/O (0..1)."""
    tot_r = sum(d["reads"] for d in deltas.values()) or 1
    tot_w = sum(d["writes"] for d in deltas.values()) or 1
    tot_io = sum(d["reads"] + d["writes"] for d in deltas.values()) or 1
    shares = {}
    for db, d in deltas.items():
        shares[db] = {
            "read_share": Decimal(d["reads"]) / Decimal(tot_r),
            "write_share": Decimal(d["writes"]) / Decimal(tot_w),
            "io_share": Decimal(d["reads"] + d["writes"]) / Decimal(tot_io),
        }
    return shares


def scale_to_month(value_in_window, window_seconds):
    """Extrapolate a windowed total to a 730-hour month."""
    if window_seconds <= 0:
        return Decimal(0)
    seconds_per_month = HOURS_PER_MONTH * Decimal(3600)
    return Decimal(str(value_in_window)) * (seconds_per_month / Decimal(str(window_seconds)))


def price_database(db_share, billed_io_month, storage_gb, acu_hours_month, pricing):
    """Cost of one database as if alone on a cluster, both storage modes.

    db_share: dict with read_share/write_share/io_share (Decimals).
    billed_io_month: dict {reads, writes} cluster billed I/O requests / month.
    storage_gb: this DB's stored GB.
    acu_hours_month: cluster ACU-hours / month allocated to this DB (indicative).
    pricing: dict from build_pricing().

    Standard: pay for I/O requests, cheaper storage + compute.
    I/O-Optimized: I/O free, but storage 2.25x and compute ~1.33x.
    """
    # I/O requests attributed to this DB (reads by read_share, writes by write_share)
    db_read_io = Decimal(str(billed_io_month["reads"])) * db_share["read_share"]
    db_write_io = Decimal(str(billed_io_month["writes"])) * db_share["write_share"]
    db_io_requests = db_read_io + db_write_io

    std = {
        "compute": pricing["std_acu_hr"] * Decimal(str(acu_hours_month)),
        "storage": pricing["std_storage_gb_mo"] * Decimal(str(storage_gb)),
        "io": pricing["std_io_per_request"] * db_io_requests,
    }
    opt = {
        "compute": pricing["opt_acu_hr"] * Decimal(str(acu_hours_month)),
        "storage": pricing["opt_storage_gb_mo"] * Decimal(str(storage_gb)),
        "io": Decimal(0),  # included in I/O-Optimized
    }
    std["total"] = std["compute"] + std["storage"] + std["io"]
    opt["total"] = opt["compute"] + opt["storage"] + opt["io"]
    return {"standard": std, "optimized": opt, "io_requests": db_io_requests}


def verdict(std_total, opt_total):
    """Which mode is cheaper, and by how much (percent of the pricier side)."""
    if std_total <= opt_total:
        cheaper = "Standard"
        pricier = opt_total
        savings = opt_total - std_total
    else:
        cheaper = "I/O-Optimized"
        pricier = std_total
        savings = std_total - opt_total
    pct = (savings / pricier * Decimal(100)) if pricier > 0 else Decimal(0)
    return cheaper, pct


# =========================================================================
# AWS-facing advisor
# =========================================================================

class CostAdvisor:
    def __init__(self, args):
        self._args = args
        session = boto3.Session(profile_name=args.profile)
        self._cw = session.client("cloudwatch", region_name=args.region)
        self._rds = session.client("rds", region_name=args.region)
        self._rds_data = session.client("rds-data", region_name=args.region)
        # Pricing API only lives in us-east-1 / ap-south-1; us-east-1 is safe.
        self._pricing = session.client("pricing", region_name="us-east-1")

    # --- engine / cluster metadata --------------------------------------

    def describe_cluster(self):
        resp = self._rds.describe_db_clusters(
            DBClusterIdentifier=self._args.cluster
        )["DBClusters"]
        if not resp:
            raise SystemExit(f"Cluster '{self._args.cluster}' not found.")
        c = resp[0]
        if c["Engine"] != "aurora-postgresql":
            raise SystemExit(
                f"Engine is '{c['Engine']}'. This tool is Aurora PostgreSQL only "
                "(it relies on pg_stat_database; Aurora MySQL has no equivalent "
                "per-database block-read counter)."
            )
        instances = self._rds.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": [self._args.cluster]}]
        )["DBInstances"]
        return c, instances

    # --- engine sampling via Data API -----------------------------------

    # Transient Data API conditions worth retrying: cold auto-pause resume, and
    # server-side/throttling blips on a busy cluster.
    _RETRYABLE = {
        "DatabaseResumingException", "InternalServerErrorException",
        "ServiceUnavailableError", "ThrottlingException",
        "TooManyRequestsException", "StatementTimeoutException",
    }

    def _execute(self, sql):
        deadline = time.time() + 120
        attempt = 0
        while True:
            try:
                return self._rds_data.execute_statement(
                    resourceArn=self._args.cluster_arn,
                    secretArn=self._args.secret_arn,
                    database=self._args.connect_db,
                    sql=sql,
                )
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                retryable = code in self._RETRYABLE or "resuming" in str(e)
                if retryable and time.time() < deadline:
                    attempt += 1
                    if code == "DatabaseResumingException" or "resuming" in str(e):
                        print("  cluster resuming from auto-pause, waiting...", file=sys.stderr)
                        wait = 5
                    else:
                        # exponential backoff (capped) for load/throttle blips
                        wait = min(2 ** attempt, 15)
                        print(f"  transient Data API error ({code or 'unknown'}), "
                              f"retrying in {wait}s...", file=sys.stderr)
                    time.sleep(wait)
                    continue
                raise

    def _sample(self):
        resp = self._execute(
            """
            SELECT datname,
                   blks_read,
                   COALESCE(tup_inserted,0)+COALESCE(tup_updated,0)
                 + COALESCE(tup_deleted,0) AS write_rows
            FROM pg_stat_database
            WHERE datname IS NOT NULL
            """
        )
        out = {}
        for row in resp["records"]:
            name = row[0].get("stringValue")
            if name is not None:
                out[name] = {"reads": _long(row[1]), "writes": _long(row[2])}
        return out

    def _database_sizes(self):
        """Per-database on-disk size in GB (excludes DBs we can't size)."""
        resp = self._execute(
            "SELECT datname, pg_database_size(datname) FROM pg_database "
            "WHERE datname IS NOT NULL"
        )
        sizes = {}
        for row in resp["records"]:
            name = row[0].get("stringValue")
            if name is not None:
                sizes[name] = Decimal(_long(row[1])) / Decimal(1024**3)
        return sizes

    def sample_rates(self):
        print(f"Sampling pg_stat_database via Data API, {self._args.interval}s apart...")
        first = self._sample()
        t0 = time.time()
        time.sleep(self._args.interval)
        second = self._sample()
        window = time.time() - t0
        return deltas_from_samples(first, second), window

    # --- CloudWatch: billed I/O + stored bytes + capacity ---------------

    def _metric_sum(self, metric, dims, start, end, period=300):
        r = self._cw.get_metric_statistics(
            Namespace="AWS/RDS", MetricName=metric, Dimensions=dims,
            StartTime=start, EndTime=end, Period=period, Statistics=["Sum"],
        )
        pts = sorted(r["Datapoints"], key=lambda d: d["Timestamp"])
        return pts

    def _metric_avg_latest(self, metric, dims, start, end, period=300):
        r = self._cw.get_metric_statistics(
            Namespace="AWS/RDS", MetricName=metric, Dimensions=dims,
            StartTime=start, EndTime=end, Period=period, Statistics=["Average"],
        )
        pts = sorted(r["Datapoints"], key=lambda d: d["Timestamp"])
        return pts

    def billed_io(self, start, end):
        """Billed I/O over [start,end]. Falls back to the most recent available
        5-min buckets if the window itself has no datapoints (Serverless v2
        publishes Volume* metrics on a lag and not at all while auto-paused)."""
        dims = [{"Name": "DBClusterIdentifier", "Value": self._args.cluster}]

        def sum_metric(metric):
            pts = self._metric_sum(metric, dims, start, end)
            if pts:
                return sum(p["Sum"] for p in pts), False, None
            # Fallback: widen lookback to catch the freshest lagged bucket.
            look_start = end - timedelta(hours=self._args.lookback_hours)
            pts = self._metric_sum(metric, dims, look_start, end)
            if not pts:
                return 0.0, True, None
            latest = pts[-1]
            age_min = (datetime.now(timezone.utc) - latest["Timestamp"]).total_seconds() / 60
            # Represent the window as one 5-min bucket's worth (rate proxy).
            return latest["Sum"], True, age_min

        reads, r_fb, r_age = sum_metric("VolumeReadIOPs")
        writes, w_fb, w_age = sum_metric("VolumeWriteIOPs")
        fell_back = r_fb or w_fb
        age = max([a for a in (r_age, w_age) if a is not None], default=None)
        return {"reads": reads, "writes": writes}, fell_back, age

    def stored_bytes(self, start, end):
        dims = [{"Name": "DBClusterIdentifier", "Value": self._args.cluster}]
        pts = self._metric_avg_latest("VolumeBytesUsed", dims, start, end)
        if not pts:
            look_start = end - timedelta(hours=self._args.lookback_hours)
            pts = self._metric_avg_latest("VolumeBytesUsed", dims, look_start, end)
        return Decimal(str(pts[-1]["Average"])) if pts else Decimal(0)

    def acu_hours(self, instances, start, end, window_seconds):
        """Cluster ServerlessV2 ACU-hours consumed in the window (sum across
        serverless instances). Provisioned instances contribute fixed hours."""
        total_acu_hours = Decimal(0)
        provisioned = []
        for inst in instances:
            iid = inst["DBInstanceIdentifier"]
            if inst["DBInstanceClass"] == "db.serverless":
                dims = [{"Name": "DBInstanceIdentifier", "Value": iid}]
                pts = self._metric_avg_latest("ServerlessDatabaseCapacity", dims, start, end)
                if not pts:
                    look_start = end - timedelta(hours=self._args.lookback_hours)
                    pts = self._metric_avg_latest("ServerlessDatabaseCapacity", dims, look_start, end)
                if pts:
                    avg_acu = Decimal(str(sum(p["Average"] for p in pts) / len(pts)))
                    total_acu_hours += avg_acu * (Decimal(str(window_seconds)) / Decimal(3600))
            else:
                provisioned.append(inst["DBInstanceClass"])
        return total_acu_hours, provisioned

    # --- pricing --------------------------------------------------------

    def _pricing_request(self, filters, engine=None):
        fl = [{"Type": "TERM_MATCH", "Field": "regionCode", "Value": self._args.region}]
        for field, value in filters:
            fl.append({"Type": "TERM_MATCH", "Field": field, "Value": value})
        if engine:
            fl.append({"Type": "TERM_MATCH", "Field": "databaseEngine", "Value": engine})
        resp = self._pricing.get_products(ServiceCode="AmazonRDS", Filters=fl, MaxResults=10)
        if not resp["PriceList"]:
            raise SystemExit(
                f"No pricing found for filters {filters} in {self._args.region}. "
                "The region may use a usageType prefix this tool doesn't know."
            )
        # Take the max USD OnDemand price dimension (matches gist behaviour).
        best = Decimal(0)
        for pj in resp["PriceList"]:
            d = json.loads(pj)
            for term in d["terms"].get("OnDemand", {}).values():
                for pd in term["priceDimensions"].values():
                    usd = pd["pricePerUnit"].get("USD")
                    if usd is not None:
                        best = max(best, Decimal(str(usd)))
        return best

    def build_pricing(self):
        return {
            "std_storage_gb_mo": self._pricing_request(
                [("usageType", "Aurora:StorageUsage"), ("operation", "CreateDBInstance")]),
            "opt_storage_gb_mo": self._pricing_request(
                [("usageType", "Aurora:IO-OptimizedStorageUsage"), ("operation", "CreateDBInstance")]),
            "std_io_per_request": self._pricing_request(
                [("usageType", "Aurora:StorageIOUsage"), ("operation", "CreateDBInstance")]),
            "std_acu_hr": self._pricing_request(
                [("usageType", "Aurora:ServerlessV2Usage")], engine="Aurora PostgreSQL"),
            "opt_acu_hr": self._pricing_request(
                [("usageType", "Aurora:ServerlessV2IOOptimizedUsage")], engine="Aurora PostgreSQL"),
        }

    # --- orchestration --------------------------------------------------

    def run(self):
        cluster, instances = self.describe_cluster()
        print(f"Cluster: {self._args.cluster} ({cluster['Engine']} {cluster['EngineVersion']})")
        for i in instances:
            print(f"  instance {i['DBInstanceIdentifier']} ({i['DBInstanceClass']})")

        start = datetime.now(timezone.utc)
        deltas, window = self.sample_rates()
        sizes = self._database_sizes()
        end = datetime.now(timezone.utc)

        pricing = self.build_pricing()

        # Cluster billed I/O over the sampling window (with lag fallback).
        billed_window, fell_back, age_min = self.billed_io(
            start - timedelta(minutes=5), end + timedelta(minutes=5)
        )
        if fell_back:
            note = f" (nearest bucket ~{age_min:.0f} min old)" if age_min is not None else ""
            print(
                f"\nNote: no billed I/O datapoints inside the sampling window; "
                f"fell back to the most recent available 5-min bucket{note}. "
                "Billed-I/O figures are approximate. For accurate figures, run "
                "against a cluster with recent sustained traffic and re-run with "
                "a longer --interval.", file=sys.stderr,
            )

        # Extrapolate billed I/O to a month.
        billed_month = {
            "reads": scale_to_month(billed_window["reads"], window),
            "writes": scale_to_month(billed_window["writes"], window),
        }

        # Compute/storage context.
        acu_hours_window, provisioned = self.acu_hours(instances, start, end, window)
        acu_hours_month = scale_to_month(acu_hours_window, window)

        # Filter to user databases with any activity or size.
        shares = io_shares(deltas)
        total_io = sum(d["reads"] + d["writes"] for d in deltas.values())
        if total_io == 0:
            print(
                "\nNo logical I/O observed during the sampling window. The cluster "
                "looks idle -- run against live traffic or use a longer --interval.",
                file=sys.stderr,
            )

        # Part 1: raw per-database I/O attribution (the estimate + evidence).
        self._attribution_report(deltas, shares, billed_window, fell_back)
        # Part 2: Standard vs I/O-Optimized cost recommendation per database.
        self._report(deltas, shares, sizes, billed_month, acu_hours_month,
                     pricing, provisioned, window)

    def _attribution_report(self, deltas, shares, billed_window, fell_back):
        """Per-database I/O attribution: how the cluster's billed I/O splits
        across logical databases (the foundation the pricing rests on)."""
        include_system = self._args.include_system
        dbs = [d for d in deltas if include_system or d not in SYSTEM_DATABASES]
        dbs.sort(key=lambda d: deltas[d]["reads"], reverse=True)

        total_logical_reads = sum(d["reads"] for d in deltas.values()) or 1

        headers = ["Database", "Logical reads", "Read share", "Est. billed reads",
                   "Logical writes", "Write share"]
        rows = []
        for db in dbs:
            d = deltas[db]
            sh = shares[db]
            est_billed_reads = Decimal(str(billed_window["reads"])) * sh["read_share"]
            label = f"{db} (system)" if db in SYSTEM_DATABASES else db
            rows.append([
                label,
                f"{d['reads']:,}",
                f"{sh['read_share']*100:.1f}%",
                f"{est_billed_reads:,.0f}",
                f"{d['writes']:,}",
                f"{sh['write_share']*100:.1f}%",
            ])

        print("\n=== Per-database I/O attribution (estimate) ===")
        _print_table(headers, rows)
        auth = " (approximate -- see billed-I/O note above)" if fell_back else ""
        print(f"\nCluster billed I/O over window (CloudWatch, authoritative{auth}):")
        print(f"- VolumeReadIOPs (sum):  {billed_window['reads']:,.0f}")
        print(f"- VolumeWriteIOPs (sum): {billed_window['writes']:,.0f}")
        print(
            f"\nSanity check -- logical reads captured across all DBs: "
            f"{total_logical_reads:,}. The per-DB billed numbers are the cluster "
            "total split by each DB's share, not measured directly. Read shares "
            "(blks_read) are reliable; write shares use row activity as a coarse "
            "proxy (pg_stat_database has no block-level write counter)."
        )

    def _report(self, deltas, shares, sizes, billed_month, acu_hours_month,
                pricing, provisioned, window):
        include_system = self._args.include_system
        dbs = [d for d in deltas
               if include_system or d not in SYSTEM_DATABASES]
        # Rank by combined I/O share, descending.
        dbs.sort(key=lambda d: shares[d]["io_share"], reverse=True)

        headers = ["Database", "Read%", "Write%", "Storage GB",
                   "Std $/mo", "IO-Opt $/mo", "Cheaper", "By"]
        rows = []
        cluster_std = Decimal(0)
        cluster_opt = Decimal(0)
        for db in dbs:
            sh = shares[db]
            gb = sizes.get(db, Decimal(0))
            # Allocate cluster ACU-hours to this DB by its combined I/O share.
            db_acu = acu_hours_month * sh["io_share"]
            costs = price_database(sh, billed_month, gb, db_acu, pricing)
            cheaper, pct = verdict(costs["standard"]["total"], costs["optimized"]["total"])
            cluster_std += costs["standard"]["total"]
            cluster_opt += costs["optimized"]["total"]
            label = f"{db} (system)" if db in SYSTEM_DATABASES else db
            rows.append([
                label,
                f"{sh['read_share']*100:.1f}%",
                f"{sh['write_share']*100:.1f}%",
                f"{gb:.2f}",
                f"${costs['standard']['total']:,.2f}",
                f"${costs['optimized']['total']:,.2f}",
                cheaper,
                f"{pct:.1f}%",
            ])

        print(f"\nSampling window: {window:.0f}s (extrapolated to a 730h month)")
        print("\n=== Per-database Standard vs I/O-Optimized cost recommendation ===")
        print("(each database priced as if alone on a cluster)")
        _print_table(headers, rows)

        # Cluster-level recommendation.
        c_cheaper, c_pct = verdict(cluster_std, cluster_opt)
        print("\nCluster totals (sum of per-DB estimates):")
        print(f"  Aurora Standard:      ${cluster_std:,.2f}/mo")
        print(f"  Aurora I/O-Optimized: ${cluster_opt:,.2f}/mo")
        print(f"  --> If kept as one cluster, {c_cheaper} is cheaper by {c_pct:.1f}%.")

        # Split recommendation.
        opt_dbs = [r[0] for r in rows if r[6] == "I/O-Optimized"]
        std_dbs = [r[0] for r in rows if r[6] == "Standard"]
        print("\nCluster-split guidance:")
        if opt_dbs and std_dbs:
            print(f"  I/O-Optimized candidates: {', '.join(opt_dbs)}")
            print(f"  Standard candidates:      {', '.join(std_dbs)}")
            print("  Splitting these into two clusters by the grouping above can beat "
                  "a single-mode cluster when the workloads are mixed.")
        elif opt_dbs:
            print(f"  All databases favour I/O-Optimized: {', '.join(opt_dbs)}")
        else:
            print(f"  All databases favour Standard: {', '.join(std_dbs)}")

        if provisioned:
            print(f"\nNote: provisioned instances present ({', '.join(provisioned)}); "
                  "their compute is a fixed cluster cost not attributable per DB.")
        print("\nReminder: per-DB billed I/O and compute are ESTIMATES derived from "
              "logical activity shares. Read shares are reliable; write shares use "
              "row activity as a proxy. Use the verdict to guide cluster grouping, "
              "then validate with a billing-console what-if before migrating.")


# =========================================================================
# helpers
# =========================================================================

def _long(field):
    if field.get("isNull"):
        return 0
    if "longValue" in field:
        return field["longValue"]
    if "stringValue" in field:
        return int(field["stringValue"])
    if "doubleValue" in field:
        return int(field["doubleValue"])
    return 0


def _print_table(headers, rows):
    cols = list(zip(*([headers] + rows))) if rows else [[h] for h in headers]
    widths = [max(len(str(c)) for c in col) for col in cols]

    def fmt(cells):
        out = []
        for i, cell in enumerate(cells):
            s = str(cell)
            right = bool(re.fullmatch(r"[\d,.\s%$-]+", s))
            out.append(s.rjust(widths[i]) if right else s.ljust(widths[i]))
        return "  ".join(out)

    print(fmt(headers))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt(row))


def _cluster_id_from_arn(arn):
    m = re.match(r"arn:aws[^:]*:rds:[^:]*:[^:]*:cluster:(.+)$", arn)
    return m.group(1) if m else None


def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--region", required=True, help="AWS region (e.g. us-east-1)")
    p.add_argument("--cluster-arn", required=True, help="Aurora cluster ARN (Data API resourceArn)")
    p.add_argument("--secret-arn", required=True, help="Secrets Manager ARN with the DB master credentials")
    p.add_argument("--cluster", help="Cluster identifier for CloudWatch (defaults to name in --cluster-arn)")
    p.add_argument("--connect-db", default="postgres", help="DB to run pg_stat queries against (default: postgres)")
    p.add_argument("--interval", default=60, type=int, help="Seconds between samples (default 60)")
    p.add_argument("--lookback-hours", default=6, type=int,
                   help="How far back to search for billed-I/O datapoints when the window is empty (default 6)")
    p.add_argument("--include-system", action="store_true",
                   help="Include system databases (postgres, template*, rdsadmin) in the table")
    p.add_argument("--profile", help="AWS profile name")
    args = p.parse_args()
    if not args.cluster:
        args.cluster = _cluster_id_from_arn(args.cluster_arn)
        if not args.cluster:
            p.error("could not derive --cluster from --cluster-arn; pass --cluster explicitly")
    return args


if __name__ == "__main__":
    args = parse_args()
    try:
        CostAdvisor(args).run()
    except ClientError as e:
        print(f"AWS error: {e}", file=sys.stderr)
        sys.exit(1)
