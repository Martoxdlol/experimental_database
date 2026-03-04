# Mental thoughts

Order of executing transactions:

- transaction management order
- client -> commit
- commit to WAL and memory pages cache (single writer) - (WAL fsync) - (only locking process)
- CRITICAL: keep a last commited transaction timestamp or something similar, it must be the lat ts before the commit
- start replication messages
- start invalidating cache and subscriptions
- await replication response
- CRITICAL: allow new timestamp (the new data becomes available to new queries) - Save to WAL (I think)
- await cache invalidation
- respond to client
- await active subscriptions invalidation

The new thing is the indicator of the newest ready timestamp to read from on incoming transactions.
This is needed to prevent the case when the primary makes data available to new transactions before the replication.
If the primary fails before replication data loss occurs.

