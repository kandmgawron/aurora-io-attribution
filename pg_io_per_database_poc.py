#!/usr/bin/env python3
"""
Proof-of-concept: attribute Aurora PostgreSQL *billed* volume I/O to individual
logical databases -- via the RDS Data API (no VPC/psql access required).

Why this exists
---------------
Aurora bills I/O at the cluster storage-volume level. CloudWatch only publishes
the billed metrics (VolumeReadIOPs / VolumeWriteIOPs) at the DBClusterIdentifier
dimension -- there is NO per-database breakdown available anywhere in CloudWatch.

The only place per-database I/O activity is visible is *inside the engine*. This
PoC reads pg_stat_database, which tracks physical block reads (blks_read) and
rows/blocks written per logical database, samples it twice over an interval to
get a rate, then uses each database's share of logical I/O to proportionally
split the cluster's real billed I/O from CloudWatch.

    db_billed_io  ~=  cluster_billed_io  x  (db_logical_io / total_logical_io)

IMPORTANT: this is an ESTIMATE. Logical counters don't map 1:1 to billed I/O
(buffer cache hits, background flushing, replication, WAL, and cluster overhead
are not cleanly attributable to a single database). The point of this PoC is to
let you eyeball how well the split tracks the real cluster total before building
it into the pricing tool.

Transport: this version talks to the cluster through the RDS Data API
(rds-data execute-statement), so it runs from anywhere with IAM creds and needs
no direct network path to the DB. The cluster must have the Data API enabled
(HttpEndpointEnabled) and a Secrets Manager secret holding the master creds.

Requires: python3 -m pip install boto3
"""

import re
import sys
import time
import argparse
from decimal import Decimal
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError


# Databases that are engine plumbing, not user workloads. Shown but flagged.
SYSTEM_DATABASES = {"template0", "template1", "rdsadmin", "postgres"}


class PgIoAttributionPoC:
    def __init__(self, args):
        self._args = args
        session = boto3.Session(profile_name=args.profile)
        self._cloudwatch = session.client("cloudwatch", region_name=args.region)
        self._rds_data = session.client("rds-data", region_name=args.region)

    # --- engine sampling via Data API ------------------------------------

    def _execute(self, sql):
        """Run one SQL statement through the Data API, with auto-pause warm-up.

        Serverless v2 can be scaled to zero; the first call then returns
        DatabaseResumingException while the instance wakes. Retry briefly.
        """
        deadline = time.time() + 90
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
                msg = str(e)
                resuming = code == "DatabaseResumingException" or "resuming" in msg
                if resuming and time.time() < deadline:
                    print("  cluster resuming from auto-pause, waiting...", file=sys.stderr)
                    time.sleep(5)
                    continue
                raise

    def _sample(self):
        """One snapshot of per-database physical I/O counters.

        blks_read  = blocks read from storage (physical reads -> read I/O proxy)
        tup_*      = rows written; summed as a coarse write-activity proxy since
                     pg_stat_database has no direct 'blocks written' column.
        """
        resp = self._execute(
            """
            SELECT datname,
                   blks_read,
                   COALESCE(tup_inserted, 0)
                 + COALESCE(tup_updated, 0)
                 + COALESCE(tup_deleted, 0) AS write_rows
            FROM pg_stat_database
            WHERE datname IS NOT NULL
            """
        )
        out = {}
        for row in resp["records"]:
            # row = [ {stringValue: name}, {longValue: reads}, {longValue: writes} ]
            name = row[0].get("stringValue")
            reads = _long(row[1])
            writes = _long(row[2])
            if name is not None:
                out[name] = {"reads": reads, "writes": writes}
        return out

    def sample_rates(self):
        """Take two samples `interval` seconds apart, return per-db deltas."""
        print(
            f"Sampling pg_stat_database via Data API, {self._args.interval}s apart..."
        )
        first = self._sample()
        time.sleep(self._args.interval)
        second = self._sample()

        deltas = {}
        for db, after in second.items():
            before = first.get(db)
            if before is None:
                continue
            # Counters reset on stat reset / failover; guard against negatives.
            reads = max(0, after["reads"] - before["reads"])
            writes = max(0, after["writes"] - before["writes"])
            deltas[db] = {"reads": reads, "writes": writes}
        return deltas

    # --- cluster billed I/O from CloudWatch ------------------------------

    def cluster_billed_io(self, start_time, end_time):
        """Sum of billed VolumeReadIOPs + VolumeWriteIOPs over the window."""
        def total(metric):
            resp = self._cloudwatch.get_metric_data(
                MetricDataQueries=[
                    {
                        "Id": "m1",
                        "MetricStat": {
                            "Metric": {
                                "Namespace": "AWS/RDS",
                                "MetricName": metric,
                                "Dimensions": [
                                    {"Name": "DBClusterIdentifier", "Value": self._args.cluster}
                                ],
                            },
                            "Period": 60,
                            "Stat": "Sum",
                        },
                        "ReturnData": True,
                    }
                ],
                StartTime=start_time.isoformat(),
                EndTime=end_time.isoformat(),
            )
            return sum(resp["MetricDataResults"][0]["Values"])

        return {"reads": total("VolumeReadIOPs"), "writes": total("VolumeWriteIOPs")}

    # --- reporting -------------------------------------------------------

    def run(self):
        start = datetime.now(timezone.utc)
        deltas = self.sample_rates()
        end = datetime.now(timezone.utc)

        # Widen the CloudWatch window by a minute each side so the sampling
        # interval is fully covered by 60s-granularity datapoints.
        billed = self.cluster_billed_io(
            start - timedelta(minutes=1), end + timedelta(minutes=1)
        )

        total_logical_reads = sum(d["reads"] for d in deltas.values()) or 1
        total_logical_writes = sum(d["writes"] for d in deltas.values()) or 1

        headers = [
            "Database",
            "Logical reads",
            "Read share",
            "Est. billed reads",
            "Logical writes",
            "Write share",
        ]
        rows = []
        for db in sorted(deltas, key=lambda d: deltas[d]["reads"], reverse=True):
            d = deltas[db]
            read_share = d["reads"] / total_logical_reads
            write_share = d["writes"] / total_logical_writes
            est_billed_reads = Decimal(str(billed["reads"])) * Decimal(str(read_share))
            label = f"{db} (system)" if db in SYSTEM_DATABASES else db
            rows.append([
                label,
                f"{d['reads']:,}",
                f"{read_share * 100:.1f}%",
                f"{est_billed_reads:,.0f}",
                f"{d['writes']:,}",
                f"{write_share * 100:.1f}%",
            ])

        print("\nPer-database I/O attribution (estimate)")
        _print_table(headers, rows)

        print("\nCluster billed I/O over window (CloudWatch, authoritative):")
        print(f"- VolumeReadIOPs (sum):  {billed['reads']:,.0f}")
        print(f"- VolumeWriteIOPs (sum): {billed['writes']:,.0f}")

        # The honesty check: does the logical read total look proportional to
        # the billed read total? A wildly different magnitude means the proxy
        # is weak for this workload.
        print(
            f"\nSanity check -- logical reads captured across all DBs: "
            f"{total_logical_reads:,}. Compare the *shares* above against your "
            "knowledge of the workload; the per-DB billed numbers are the "
            "cluster total split by those shares, not measured directly."
        )
        print(
            "Reminder: writes have no clean block-level counter in "
            "pg_stat_database, so write shares use row activity as a coarse "
            "proxy and are less reliable than read shares."
        )


def _print_table(headers, rows):
    """Minimal fixed-width table. Numeric-looking cells right-align.

    ponytail: replaces rich just to render one table; stdlib str formatting
    is plenty. Ceiling: no wrapping/colour. Upgrade path: reinstate rich if
    output ever needs styling.
    """
    cols = list(zip(*([headers] + rows))) if rows else [[h] for h in headers]
    widths = [max(len(str(c)) for c in col) for col in cols]

    def fmt(cells):
        out = []
        for i, cell in enumerate(cells):
            s = str(cell)
            # Right-align cells that look like numbers/percentages/counts.
            right = bool(re.fullmatch(r"[\d,.\s%-]+", s))
            out.append(s.rjust(widths[i]) if right else s.ljust(widths[i]))
        return "  ".join(out)

    sep = "  ".join("-" * w for w in widths)
    print(fmt(headers))
    print(sep)
    for row in rows:
        print(fmt(row))


def _long(field):
    """Pull an integer out of a Data API field, tolerating type variance."""
    if field.get("isNull"):
        return 0
    if "longValue" in field:
        return field["longValue"]
    if "stringValue" in field:
        return int(field["stringValue"])
    if "doubleValue" in field:
        return int(field["doubleValue"])
    return 0


def _cluster_id_from_arn(arn):
    """arn:aws:rds:region:acct:cluster:my-cluster -> my-cluster."""
    m = re.match(r"arn:aws[^:]*:rds:[^:]*:[^:]*:cluster:(.+)$", arn)
    return m.group(1) if m else None


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", required=True, help="AWS region (e.g. us-east-1)")
    p.add_argument(
        "--cluster-arn",
        required=True,
        help="Aurora cluster ARN (Data API resourceArn)",
    )
    p.add_argument(
        "--secret-arn",
        required=True,
        help="Secrets Manager ARN with the DB master credentials",
    )
    p.add_argument(
        "--cluster",
        help="Cluster identifier for CloudWatch (defaults to the name in --cluster-arn)",
    )
    p.add_argument(
        "--connect-db",
        default="postgres",
        help="Database to run the pg_stat query against (default: postgres)",
    )
    p.add_argument("--interval", default=60, type=int, help="Seconds between samples (default 60)")
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
        PgIoAttributionPoC(args).run()
    except ClientError as e:
        print(f"AWS error: {e}", file=sys.stderr)
        sys.exit(1)
