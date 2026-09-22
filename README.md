# aurora-io-attribution

Attribute Aurora I/O and cost to individual logical databases.

Aurora bills I/O at the cluster storage-volume level, and the Standard /
I/O-Optimized toggle is a **cluster-wide** setting — you cannot put one logical
database on a different storage mode from its neighbours. But you *can* split
databases into separate clusters for cost optimisation: park the I/O-heavy
databases on an I/O-Optimized cluster and keep the low-I/O ones on Standard.

CloudWatch only exposes billed I/O at the cluster level — there is no
per-database breakdown anywhere. This repo attributes it per database by
sampling `pg_stat_database` inside the engine, then prices each database both
ways so you know how to group them.

## `aurora_io_cost_advisor.py`

A single tool. **Every run does both** the per-database I/O attribution and the
Standard vs I/O-Optimized cost recommendation, in one pass:

- **Part 1 — I/O attribution.** Samples `pg_stat_database` (via the RDS Data
  API) twice to get each database's share of physical read + write activity,
  pulls the cluster's real billed I/O from CloudWatch, and splits that billed
  I/O across databases by their activity share.
- **Part 2 — cost recommendation.** Extrapolates the sampled window to a
  monthly rate, prices each database *as if it were alone on a cluster* under
  both storage modes (live AWS Pricing API + `VolumeBytesUsed` + ServerlessV2
  ACU), and emits a per-DB verdict plus cluster totals and cluster-split
  guidance.

Talks to the cluster through the **RDS Data API**, so it runs from anywhere with
IAM creds — no VPC access or `psql`/`psycopg2` needed. The cluster must have the
Data API enabled and a Secrets Manager secret with the master credentials.

```
python3 -m pip install boto3
python3 aurora_io_cost_advisor.py --region us-east-1 \
    --cluster-arn arn:aws:rds:us-east-1:ACCT:cluster:my-cluster \
    --secret-arn arn:aws:secretsmanager:us-east-1:ACCT:secret:my-db-secret \
    --interval 60
```

Options: `--interval` (sampling seconds), `--lookback-hours` (how far back to
search for billed-I/O datapoints when the sampling window is empty),
`--include-system` (show `postgres`/`template*`/`rdsadmin`), `--connect-db`,
`--profile`. `--cluster` (the CloudWatch identifier) is derived from
`--cluster-arn` automatically; pass it explicitly only if they differ.

Aurora PostgreSQL only (engine-guarded). The economics: I/O-Optimized removes
per-request I/O charges but raises storage ~2.25x and Serverless compute ~1.33x,
so it wins for I/O-heavy databases and loses for storage-heavy/low-I/O ones.

### Caveats (see the module docstring for the full list)

Per-DB billed I/O and compute are **estimates** from logical-activity shares —
read shares (`blks_read`) are reliable, write shares use row activity as a
coarse proxy (`pg_stat_database` has no block-level write counter), and
Serverless compute is a shared cluster resource only indicatively allocated per
DB. On a cluster that has auto-paused, the billed `Volume*` metrics lag and the
tool falls back to the freshest available bucket with a warning; for accurate
figures run against a cluster with recent sustained traffic. Always validate
with a billing-console what-if before migrating.

## Tests

Two standalone offline test modules (no AWS, no database required):

```
python3 test_cost_advisor_math.py     # pricing + verdict math
python3 test_attribution_math.py      # delta + proportional-split math
```
