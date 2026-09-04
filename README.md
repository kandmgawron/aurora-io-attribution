# aurora-io-attribution

Experiments in attributing Aurora I/O and cost to individual logical databases.

The starting point is a cluster-wide Aurora Standard vs I/O-Optimized cost
comparison. This repo explores whether that I/O can be broken down per logical
database, which CloudWatch alone cannot do.

## `pg_io_per_database_poc.py`

Proof-of-concept (Aurora PostgreSQL only). Samples `pg_stat_database` to get
per-database logical I/O rates, then splits the cluster's real billed I/O
(from CloudWatch) proportionally across databases.

```
python3 -m pip install boto3 psycopg2-binary rich
python3 pg_io_per_database_poc.py --region eu-west-1 --cluster my-cluster \
    --host my-cluster.cluster-xxxx.eu-west-1.rds.amazonaws.com \
    --user reporting --password '***'
```

Read attribution (`blks_read`) is reliable; write attribution is a coarse
row-activity proxy. See the module docstring for the full caveats.
