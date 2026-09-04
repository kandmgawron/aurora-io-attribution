#!/usr/bin/env python3
"""
Proof-of-concept: attribute Aurora PostgreSQL *billed* volume I/O to individual
logical databases.

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

Requires: python3 -m pip install boto3 psycopg2-binary rich
"""

import sys
import time
import argparse
from decimal import Decimal
from datetime import datetime, timedelta, timezone

import boto3
import psycopg2
from rich.console import Console
from rich.table import Table


# Databases that are engine plumbing, not user workloads. Shown but flagged.
SYSTEM_DATABASES = {"template0", "template1", "rdsadmin", "postgres"}


class PgIoAttributionPoC:
    def __init__(self, args):
        self._args = args
        self._console = Console()
        session = boto3.Session(profile_name=args.profile)
        self._cloudwatch = session.client("cloudwatch", region_name=args.region)

    # --- engine sampling -------------------------------------------------

    def _connect(self):
        return psycopg2.connect(
            host=self._args.host,
            port=self._args.port,
            dbname=self._args.connect_db,
            user=self._args.user,
            password=self._args.password,
            connect_timeout=10,
            # ponytail: read-only intent; we never write. sslmode=require is the
            # sane default for Aurora endpoints.
            sslmode="require",
        )

    def _sample(self, conn):
        """One snapshot of per-database physical I/O counters.

        blks_read  = blocks read from storage (physical reads -> read I/O proxy)
        tup_*      = rows written; summed as a coarse write-activity proxy since
                     pg_stat_database has no direct 'blocks written' column.
        """
        with conn.cursor() as cur:
            cur.execute(
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
            return {row[0]: {"reads": row[1], "writes": row[2]} for row in cur.fetchall()}

    def sample_rates(self):
        """Take two samples `interval` seconds apart, return per-db deltas."""
        conn = self._connect()
        try:
            self._console.print(
                f"Sampling pg_stat_database, {self._args.interval}s apart...", style="bold"
            )
            first = self._sample(conn)
            time.sleep(self._args.interval)
            second = self._sample(conn)
        finally:
            conn.close()

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

        table = Table(title="Per-database I/O attribution (estimate)", show_lines=True)
        for col in [
            "Database",
            "Logical reads",
            "Read share",
            "Est. billed reads",
            "Logical writes",
            "Write share",
        ]:
            table.add_column(col)

        for db in sorted(deltas, key=lambda d: deltas[d]["reads"], reverse=True):
            d = deltas[db]
            read_share = d["reads"] / total_logical_reads
            write_share = d["writes"] / total_logical_writes
            est_billed_reads = Decimal(str(billed["reads"])) * Decimal(str(read_share))
            label = f"{db} [dim](system)[/dim]" if db in SYSTEM_DATABASES else db
            table.add_row(
                label,
                f"{d['reads']:,}",
                f"{read_share * 100:.1f}%",
                f"{est_billed_reads:,.0f}",
                f"{d['writes']:,}",
                f"{write_share * 100:.1f}%",
            )
        self._console.print(table)

        self._console.print(
            f"\nCluster billed I/O over window (CloudWatch, authoritative):",
            style="bold",
        )
        self._console.print(f"- VolumeReadIOPs (sum):  {billed['reads']:,.0f}")
        self._console.print(f"- VolumeWriteIOPs (sum): {billed['writes']:,.0f}")

        # The honesty check: does the logical read total look proportional to
        # the billed read total? A wildly different magnitude means the proxy
        # is weak for this workload.
        self._console.print(
            "\n[bold]Sanity check[/bold] -- logical reads captured across all DBs: "
            f"{total_logical_reads:,}. Compare the *shares* above against your "
            "knowledge of the workload; the per-DB billed numbers are the "
            "cluster total split by those shares, [italic]not[/italic] measured "
            "directly."
        )
        self._console.print(
            "[yellow]Reminder:[/yellow] writes have no clean block-level counter "
            "in pg_stat_database, so write shares use row activity as a coarse "
            "proxy and are less reliable than read shares."
        )


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--region", required=True, help="AWS region (e.g. eu-west-1)")
    p.add_argument("--cluster", required=True, help="Aurora cluster identifier (for CloudWatch)")
    p.add_argument("--host", required=True, help="Cluster/writer endpoint hostname")
    p.add_argument("--port", default=5432, type=int)
    p.add_argument("--user", required=True, help="DB user with CONNECT + pg_stat access")
    p.add_argument("--password", required=True, help="DB password")
    p.add_argument("--connect-db", default="postgres", help="Database to connect to for stats")
    p.add_argument("--interval", default=60, type=int, help="Seconds between samples (default 60)")
    p.add_argument("--profile", help="AWS profile name")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        PgIoAttributionPoC(args).run()
    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
