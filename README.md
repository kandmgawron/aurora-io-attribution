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
