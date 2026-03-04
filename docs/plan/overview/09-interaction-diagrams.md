# Interaction Diagrams

## 1. Document Write Commit (Full 10-Step Protocol)

```
Client          Session(L7)    CommitCoord(L5)   WAL(L2)     DocStore(L3)  Replication(L6)  Subs(L5)
  |                 |                |               |              |              |             |
  |--commit(tx)---->|                |               |              |              |             |
  |                 |--CommitReq---->|               |              |              |             |
  |                 |                |               |              |              |             |
  |                 |          1. VALIDATE            |              |              |             |
  |                 |          OCC check read_set     |              |              |             |
  |                 |          vs commit_log           |              |              |             |
  |                 |                |               |              |              |             |
  |                 |          2. TIMESTAMP            |              |              |             |
  |                 |          commit_ts = alloc()     |              |              |             |
  |                 |                |               |              |              |             |
  |                 |          3. PERSIST              |              |              |             |
  |                 |                |--append+fsync->|              |              |             |
  |                 |                |<---lsn---------|              |              |             |
  |                 |                |               |              |              |             |
  |                 |          4. MATERIALIZE          |              |              |             |
  |                 |                |------------insert_version---->|              |             |
  |                 |                |               |   (B-tree     |              |             |
  |                 |                |               |    insert)    |              |             |
  |                 |                |<-------------------------------|              |             |
  |                 |                |               |              |              |             |
  |                 |          5. LOG                  |              |              |             |
  |                 |          commit_log.append()     |              |              |             |
  |                 |                |               |              |              |             |
  |                 |          6. INVALIDATE           |              |              |             |
  |                 |                |---------------------------------------------->|             |
  |                 |                |               |              |   check_invalidation()     |
  |                 |                |<----------------------------------------------|             |
  |                 |                |               |              |              |             |
  |                 |          7. REPLICATE            |              |              |             |
  |                 |                |---------------------------------------->|                   |
  |                 |                |               |              |    stream WAL record        |
  |                 |          8. SYNC                 |              |              |             |
  |                 |                |<----------------------------------------|                   |
  |                 |                |               |              |    ack from replicas        |
  |                 |                |               |              |              |             |
  |                 |          9. VISIBLE              |              |              |             |
  |                 |          advance latest_ts       |              |              |             |
  |                 |                |               |              |              |             |
  |                 |         10. RESPOND              |              |              |             |
  |                 |<--CommitResult--|               |              |              |             |
  |<--ok(commit_ts)-|                |               |              |              |             |
```

## 2. Point Read (Get by ID) — All Layers Top to Bottom

```
Client       Session(L7)    Query(L4)     DocStore(L3)     Storage(L2)      Disk
  |              |              |              |                |              |
  |--get(doc)--->|              |              |                |              |
  |              |--plan------->|              |                |              |
  |              |   PrimaryGet |              |                |              |
  |              |              |--get_at_ts-->|                |              |
  |              |              |              |                |              |
  |              |              |              |  key = doc_id || inv_ts       |
  |              |              |              |--btree.get---->|              |
  |              |              |              |                |              |
  |              |              |              |          fetch_page_shared    |
  |              |              |              |                |--pread------>|
  |              |              |              |                |<--page_data--|
  |              |              |              |                |              |
  |              |              |              |  (traverse internal nodes)    |
  |              |              |              |                |--pread------>|
  |              |              |              |                |<--page_data--|
  |              |              |              |                |              |
  |              |              |              |<--Option<val>--|              |
  |              |              |              |                |              |
  |              |              |              | if external:   |              |
  |              |              |              |--heap_load---->|--pread------>|
  |              |              |              |<--body---------|<-------------|
  |              |              |              |                |              |
  |              |              |<--Option<doc>-|               |              |
  |              |              |              |                |              |
  |              |              | record read_set interval      |              |
  |              |<--QueryResult|              |                |              |
  |<--ok(doc)----|              |              |                |              |
```

## 3. Index Scan Query Pipeline (Source → PostFilter → Terminal)

```
         ┌─────────────────────────────────────────────────────┐
         │                   Query Pipeline                     │
         │                                                      │
         │  ┌──────────┐    ┌──────────────┐    ┌──────────┐   │
         │  │  SOURCE   │───>│  POST-FILTER  │───>│ TERMINAL │   │
         │  │           │    │              │    │          │   │
         │  │ IndexScan │    │ filter_matches│    │ limit(N) │   │
         │  │ at read_ts│    │ per document  │    │ or first │   │
         │  └──────────┘    └──────────────┘    └──────────┘   │
         │       │                  │                  │        │
         └───────│──────────────────│──────────────────│────────┘
                 │                  │                  │
                 ▼                  ▼                  ▼
         SecondaryIndex      PrimaryIndex         Vec<Doc>
         .scan_at_ts()       .get_at_ts()         (results)
              │                    │
              ▼                    ▼
         BTreeHandle          BTreeHandle
         (L2 raw scan)        (L2 raw get)

Scan Execution Detail:
  1. Encode range → byte interval [lower, upper)
  2. Seek secondary B-tree to lower bound
  3. For each (doc_id, inv_ts):
     a. Version resolution: skip if ts > read_ts
     b. Dedup: within same doc_id, take first visible
     c. Verify against primary (skip stale entries)
  4. Fetch full doc from primary B-tree
  5. Evaluate post-filter → skip if no match
  6. Yield to terminal → check limit
  7. Record scanned interval in read set
```

## 4. Subscription Invalidation on Commit

```
CommitCoord(L5)       SubscriptionRegistry(L5)        Session(L7)         Client
     |                         |                          |                  |
     | (after step 5: LOG)     |                          |                  |
     |                         |                          |                  |
     |--check_invalidation---->|                          |                  |
     |   (index_writes from    |                          |                  |
     |    this commit)         |                          |                  |
     |                         |                          |                  |
     |   For each (coll, idx): |                          |                  |
     |     For each key_write: |                          |                  |
     |       old_key overlap?  |                          |                  |
     |       new_key overlap?  |                          |                  |
     |       → collect (sub_id,|                          |                  |
     |          query_id)      |                          |                  |
     |                         |                          |                  |
     |<--Vec<InvalidationEvent>|                          |                  |
     |                         |                          |                  |
     | For each event:         |                          |                  |
     |   if mode=Subscribe:    |                          |                  |
     |     begin new tx        |                          |                  |
     |                         |                          |                  |
     |---push_notification---->|------------------------->|                  |
     |   (invalidation msg     |                          |--invalidation--->|
     |    with query_ids,      |                          |  {tx, queries,   |
     |    new_tx, new_ts)      |                          |   new_tx, new_ts}|
```

## 5. WAL Replication Primary → Replica

```
Primary                          Network              Replica
  |                                 |                    |
  | (commit step 3: WAL record)    |                    |
  |                                 |                    |
  |---stream WAL record bytes------>|--WAL record------->|
  |   (9-byte header + payload,    |                    |
  |    exactly as stored on disk)  |                    |
  |                                 |                    |
  |                                 |          1. Verify CRC-32C
  |                                 |          2. Write to local WAL
  |                                 |          3. Apply to page store
  |                                 |             (B-tree inserts)
  |                                 |          4. Invalidate local subs
  |                                 |          5. Update applied_lsn/ts
  |                                 |                    |
  |<--ack(applied_lsn)-------------|<---ack-------------|
  |                                 |                    |
  | (wait for ack per              |                    |
  |  replication mode)             |                    |
  |                                 |                    |
  | advance latest_committed_ts    |                    |
```

## 6. Crash Recovery + Startup Sequence

```
                        Startup
                           |
                    ┌──────▼──────┐
                    │ Read meta.json│
                    │ checkpoint_lsn│
                    └──────┬──────┘
                           |
                    ┌──────▼──────┐
                    │ DWB Recovery │  data.dwb exists?
                    │              │  → restore torn pages
                    └──────┬──────┘  to data.db
                           |
                    ┌──────▼──────┐
                    │ Open data.db │
                    │ (pages at    │
                    │  checkpoint) │
                    └──────┬──────┘
                           |
                    ┌──────▼──────┐
                    │ WAL Replay   │  from checkpoint_lsn
                    │              │  forward:
                    │ TxCommit →   │  redo B-tree mutations
                    │ CreateColl → │  update catalog B-tree
                    │ CreateIdx →  │  update catalog B-tree
                    │ IndexReady → │  Building → Ready
                    │ Vacuum →     │  remove old versions
                    │ Checkpoint → │  no-op
                    └──────┬──────┘
                           |
                    ┌──────▼──────┐
                    │ Build Cache  │  scan catalog B-tree
                    │ CatalogCache │  → HashMap lookups
                    └──────┬──────┘
                           |
                    ┌──────▼──────┐
                    │ Open Indexes │  open B-tree handles
                    │ Drop Building│  for each collection
                    └──────┬──────┘
                           |
                    ┌──────▼──────┐
                    │ Start Tasks  │  commit coordinator,
                    │              │  checkpoint, vacuum
                    └──────┬──────┘
                           |
                        Ready ✓
```

## 7. Connection Lifecycle (Hello → Auth → Transactions → Disconnect)

```
Client                             Server
  |                                   |
  |<------hello (JSON text)-----------|  always JSON, first message
  |  {version, encodings,            |
  |   auth_required, node_role}      |
  |                                   |
  |---authenticate(token)------------>|
  |                                   |  validate JWT
  |<------ok-------------------------|  (or error: auth_failed)
  |                                   |
  |---begin(db, readonly, sub)------->|  acquire read_ts
  |<------ok(tx:1)-------------------|
  |                                   |
  |---get(tx:1, coll, doc_id)------->|  execute query
  |<------ok(query_id:0, doc)---------|
  |                                   |
  |---query(tx:1, coll, idx, range)->|  index scan
  |<------ok(query_id:1, docs)--------|
  |                                   |
  |---commit(tx:1)------------------>|  OCC validate + persist
  |<------ok(commit_ts:42)-----------|
  |                                   |
  |  (if subscribe: true)            |
  |<------invalidation(tx:1,---------|  server-initiated push
  |        queries:[1], new_tx:2)    |
  |                                   |
  |---get(tx:2, ...)---------------->|  re-execute in new tx
  |   ...                             |
  |                                   |
  |---[disconnect]------------------>|  rollback open txns,
  |                                   |  remove subscriptions
```

## 8. Create Collection Flow

```
Session(L7)     Database      CatalogCache    WAL(L2)     StorageEngine(L2)    CatalogBTree(L2)
   |               |               |             |               |                   |
   |--create_coll->|               |             |               |                   |
   |               |               |             |               |                   |
   |               | 1. Assign collection_id     |               |                   |
   |               |---next_id---->|             |               |                   |
   |               |<--id----------|             |               |                   |
   |               |               |             |               |                   |
   |               | 2. Allocate pages           |               |                   |
   |               |-----------------------------------create_btree()-->|             |
   |               |   (primary root = empty leaf)     |<--root1------|             |
   |               |-----------------------------------create_btree()-->|             |
   |               |   (_created_at root)              |<--root2------|             |
   |               |               |             |               |                   |
   |               | 3. WAL record               |               |                   |
   |               |------------------append---->|               |                   |
   |               |   CreateCollection          |               |                   |
   |               |<-----------------lsn--------|               |                   |
   |               |               |             |               |                   |
   |               | 4. Catalog B-tree insert    |               |                   |
   |               |------------------------------------------------------insert---->|
   |               |   by-id key + entry         |               |                   |
   |               |------------------------------------------------------insert---->|
   |               |   by-name key + id          |               |                   |
   |               |               |             |               |                   |
   |               | 5. Update in-memory cache   |               |                   |
   |               |--add_coll---->|             |               |                   |
   |               |               |             |               |                   |
   |<--ok----------|               |             |               |                   |
```

## 9. Document Insert: Layer 2 vs Layer 3 Boundary

```
                 Layer 3 (Document Store)           │     Layer 2 (Storage Engine)
                 Domain-aware: DocId, Ts,           │     Domain-agnostic: bytes,
                 tombstones, MVCC                   │     pages, B-tree
                                                    │
  Caller: insert_version(doc_id, commit_ts, body)   │
           │                                        │
           ▼                                        │
  1. Construct MVCC key:                            │
     key = doc_id[16] || inv_ts[8]                  │
     (inv_ts = u64::MAX - commit_ts)                │
                                                    │
  2. Decide inline vs external:                     │
     if body.len() > EXTERNAL_THRESHOLD:            │
       href = heap.store(body)  ────────────────────┼──→  Heap::store(bytes)
       value = flags[1] || len[4] || href[6]        │       → SlottedPage insert
     else:                                          │
       value = flags[1] || len[4] || body[var]      │
                                                    │
  3. Insert into primary B-tree:                    │
     btree.insert(key, value)  ─────────────────────┼──→  BTree::insert(&[u8], &[u8])
                                                    │       → leaf page lookup
                                                    │       → slot insert
                                                    │       → split if full
                                                    │
  4. Insert secondary index entries:                │
     for each index on collection:                  │
       scalars = extract(doc, field_paths)          │
       sec_key = encode(scalars) || doc_id || ts    │
       sec_btree.insert(sec_key, &[])  ────────────┼──→  BTree::insert(&[u8], &[u8])
                                                    │
  ─────────────────────────────────────────────────  │
  Layer 3 knows: DocId, Ts, tombstones, MVCC keys,  │  Layer 2 knows: bytes, pages,
  inline/external threshold, index field extraction  │  slotted pages, B-tree insert/split
```
