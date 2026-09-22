# aurora-io-attribution

Experiments in attributing Aurora I/O and cost to individual logical databases.

The starting point is a cluster-wide Aurora Standard vs I/O-Optimized cost
comparison. This repo explores whether that I/O can be broken down per logical
database, which CloudWatch alone cannot do.

## `pg_io_per_database_poc.py`

Proof-of-concept (Aurora PostgreSQL only). Samples `pg_stat_database` to get
per-database logical I/O rates, then splits the cluster's real billed I/O
(from CloudWatch) proportionally across databases.

Talks to the cluster through the **RDS Data API**, so it runs from anywhere with
IAM creds — no VPC access or `psql`/`psycopg2` needed. The cluster must have the
Data API enabled and a Secrets Manager secret with the master credentials.

```
python3 -m pip install boto3
python3 pg_io_per_database_poc.py --region us-east-1 \
    --cluster-arn arn:aws:rds:us-east-1:ACCT:cluster:my-cluster \
    --secret-arn arn:aws:secretsmanager:us-east-1:ACCT:secret:my-db-secret
```

`--cluster` (the CloudWatch identifier) is derived from `--cluster-arn`
automatically; pass it explicitly only if they differ.

Read attribution (`blks_read`) is reliable; write attribution is a coarse
row-activity proxy. See the module docstring for the full caveats.

## `aurora_io_cost_advisor.py`

The full tool built on the PoC: per-database **Aurora Standard vs
I/O-Optimized cost recommendations**. It attributes billed I/O per database
(as the PoC does), pulls live pricing from the AWS Pricing API, and prices each
database *as if it were alone on a cluster* under both storage modes — then
emits a per-DB verdict plus cluster-split guidance.

Why per-DB when the mode is cluster-wide? Because you can't toggle one DB, but
you **can split databases into separate clusters**: park the I/O-heavy ones on
an I/O-Optimized cluster and keep low-I/O ones on Standard. This tool tells you
which side of that line each database falls on.

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
`--profile`.

Aurora PostgreSQL only (engine-guarded). The economics: I/O-Optimized removes
per-request I/O charges but raises storage ~2.25x and Serverless compute ~1.33x,
so it wins for I/O-heavy databases and loses for storage-heavy/low-I/O ones.

Caveats (see the module docstring for the full list): per-DB billed I/O and
compute are **estimates** from logical-activity shares — read shares are
reliable, write shares use row activity as a proxy, and Serverless compute is a
shared cluster resource only indicatively allocated per DB. On a cluster that
has auto-paused, the billed `Volume*` metrics lag and the tool falls back to the
freshest available bucket with a warning; for accurate figures run against a
cluster with recent sustained traffic. Always validate with a billing-console
what-if before migrating.

Offline math tests: `python3 test_cost_advisor_math.py` (no AWS required).
