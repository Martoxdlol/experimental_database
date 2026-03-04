# Interaction Diagrams

## 1. Document Write Commit (Full 11-Step Protocol)

```
Caller          Database(L6)   CommitCoord(L5)   WAL(L2)     DocStore(L3)  Replication(L7)  Subs(L5)
  |                 |                |               |              |              |             |
  |--tx.commit()--->|                |               |              |              |             |
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
  |                 |                |<-------------------------------|              |             |
  |                 |                |               |              |              |             |
  |                 |          5. LOG                  |              |              |             |
  |                 |          commit_log.append()     |              |              |             |
  |                 |                |               |              |              |             |
  |                 |          6. CONCURRENT START     |              |              |             |
  |                 |          6a.   |---------------------------------------->|   |             |
  |                 |                |               |              | replicate    |             |
  |                 |          6b.   |---------------------------------------------->|             |
  |                 |                |               |              |   check_invalidation()     |
  |                 |                |               |              |              |             |
  |                 |          7. AWAIT REPLICATION    |              |              |             |
  |                 |                |<----------------------------------------|   |             |
  |                 |                |               |              |   ack         |             |
  |                 |                |               |              |              |             |
  |                 |          8. PERSIST VISIBLE_TS   |              |              |             |
  |                 |                |--append+fsync->|              |              |             |
  |                 |                |   VISIBLE_TS   |              |              |             |
  |                 |                |               |              |              |             |
  |                 |          9. ADVANCE visible_ts   |              |              |             |
  |                 |          (new readers can now    |              |              |             |
  |                 |           see commit_ts data)    |              |              |             |
  |                 |                |               |              |              |             |
  |                 |         10. RESPOND              |              |              |             |
  |                 |<--CommitResult--|               |              |              |             |
  |<--ok(commit_ts)-|                |               |              |              |             |
  |                 |                |               |              |              |             |
  |                 |         11. PUSH INVALIDATION (fire-and-forget to subscriber sessions)    |
```

Note: Steps 6a (replicate) and 6b (check invalidation) run concurrently. Without replication (NoReplication), 6a is a no-op. Step 11 pushes invalidation events to subscriber sessions asynchronously — the committing client does NOT wait for other clients to receive their subscription notifications.

## 2. Point Read (Get by ID) — Embedded Usage

```
Caller       Database(L6)   Query(L4)     DocStore(L3)     Storage(L2)      Disk
  |              |              |              |                |              |
  |--tx.get()--->|              |              |                |              |
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
  |<--Ok(doc)----|              |              |                |              |
```

## 3. Index Scan Query Pipeline (Source -> PostFilter -> Terminal)

```
         +---------------------------------------------------------+
         |                   Query Pipeline                         |
         |                                                          |
         |  +----------+    +--------------+    +----------+       |
         |  |  SOURCE   |--->|  POST-FILTER  |--->| TERMINAL |       |
         |  |           |    |              |    |          |       |
         |  | IndexScan |    | filter_matches|    | limit(N) |       |
         |  | at read_ts|    | per document  |    | or first |       |
         |  +----------+    +--------------+    +----------+       |
         |       |                  |                  |            |
         +-------|------------------|------------------|------------+
                 |                  |                  |
                 v                  v                  v
         SecondaryIndex      PrimaryIndex         Vec<Doc>
         .scan_at_ts()       .get_at_ts()         (results)
              |                    |
              v                    v
         BTreeHandle          BTreeHandle
         (L2 raw scan)        (L2 raw get)

Scan Execution Detail:
  1. Encode range -> byte interval [lower, upper)
  2. Seek secondary B-tree to lower bound
  3. For each (doc_id, inv_ts):
     a. Version resolution: skip if ts > read_ts
     b. Dedup: within same doc_id, take first visible
     c. Verify against primary (skip stale entries)
  4. Fetch full doc from primary B-tree
  5. Evaluate post-filter -> skip if no match
  6. Yield to terminal -> check limit
  7. Record scanned interval in read set
```

## 4. Subscription Invalidation on Commit

```
CommitCoord(L5)       SubscriptionRegistry(L5)        Database(L6)        Caller
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
     |       -> collect (sub_id,|                         |                  |
     |          query_id)      |                          |                  |
     |                         |                          |                  |
     |<--Vec<InvalidationEvent>|                          |                  |
     |                         |                          |                  |
     | For each event:         |                          |                  |
     |   if mode=Subscribe:    |                          |                  |
     |     begin new tx        |                          |                  |
     |                         |                          |                  |
     | Return invalidation events in CommitResult         |                  |
     |---CommitResult--------->|------------------------->|                  |
     |                         |                          |--callback/poll-->|
```

Note: In embedded mode, the caller receives invalidation events directly in the CommitResult or via a subscription channel. In network mode (L8), the session pushes them over the wire.

## 5. WAL Replication Primary -> Replica

```
Primary(L6)      PrimaryReplicator(L7)    Network         ReplicaClient(L7)    Replica(L6)
  |                     |                    |                   |                  |
  | (commit step 7)     |                    |                   |                  |
  |--replicate_and_wait->|                   |                   |                  |
  |   (via trait)        |                   |                   |                  |
  |                      |--WAL record------>|---WAL record----->|                  |
  |                      |                   |                   |                  |
  |                      |                   |           1. Verify CRC-32C          |
  |                      |                   |                   |--apply---------->|
  |                      |                   |                   |  Write local WAL |
  |                      |                   |                   |  Apply to pages  |
  |                      |                   |                   |  Invalidate subs |
  |                      |                   |                   |<--ok-------------|
  |                      |                   |                   |                  |
  |                      |<--ack-------------|<---ack------------|                  |
  |<--ok-----------------|                   |                   |                  |
  |                      |                   |                   |                  |
  | persist WAL_RECORD_VISIBLE_TS            |                   |                  |
  |-------------------append+fsync---------->|                   |                  |
  |<------------------lsn--------------------|                   |                  |
  |                      |                   |                   |                  |
  | advance visible_ts   |                   |                   |                  |
```

## 6. Crash Recovery + Startup Sequence

```
                        Database::open()
                           |
                    +------v------+
                    | Read FileHeader|
                    | (page 0)      |
                    | checkpoint_lsn|
                    +------+------+
                           |
                    +------v------+
                    | DWB Recovery |  data.dwb exists?
                    |              |  -> restore torn pages
                    +------+------+  to data.db
                           |
                    +------v------+
                    | Open data.db |
                    | (pages at    |
                    |  checkpoint) |
                    +------+------+
                           |
                    +------v------+
                    | WAL Replay   |  from checkpoint_lsn
                    |              |  forward:
                    | TxCommit ->  |  redo B-tree mutations
                    | CreateColl ->|  update catalog B-tree
                    | CreateIdx -> |  update catalog B-tree
                    | IndexReady ->|  Building -> Ready
                    | Vacuum ->    |  remove old versions
                    | Checkpoint ->|  no-op
                    +------+------+
                           |
                    +------v------+
                    | Build Cache  |  scan catalog B-tree
                    | CatalogCache |  -> HashMap lookups
                    +------+------+
                           |
                    +------v------+
                    | Open Indexes |  open B-tree handles
                    | Drop Building|  for each collection
                    +------+------+
                           |
                    +------v------+
                    | Start Tasks  |  commit coordinator,
                    |              |  checkpoint, vacuum
                    +------+------+
                           |
                        Ready
```

## 7. Embedded Usage Lifecycle

```
Caller                          Database(L6)
  |                                |
  |--Database::open(path, cfg)--->|
  |                                |  recovery + catalog load + start tasks
  |<--Ok(Database)----------------|
  |                                |
  |--create_collection("users")->|  WAL + catalog B-tree + cache
  |<--Ok(CollectionId)-----------|
  |                                |
  |--begin_mutation()----------->|  allocate begin_ts
  |<--Ok(MutationTransaction)----|
  |                                |
  |--tx.insert("users", doc)---->|  buffer in write set
  |<--Ok(DocId)------------------|
  |                                |
  |--tx.get("users", &id)------->|  read-your-writes from write set
  |<--Ok(Some(doc))--------------|
  |                                |
  |--tx.commit(opts)------------->|  OCC + WAL + materialize + replicate
  |<--CommitResult::Success------|
  |                                |
  |--begin_readonly()----------->|  snapshot at visible_ts
  |<--ReadonlyTransaction--------|
  |                                |
  |--tx.query("users",...) ----->|  scan + filter + read set
  |<--Ok(Vec<Doc>)---------------|
  |                                |
  |--db.close()------------------>|  checkpoint + flush + shutdown
  |<--Ok(())---------------------|
```

## 8. Network Usage Lifecycle (Hello -> Auth -> Transactions -> Disconnect)

```
Client              Session(L8)       Database(L6)
  |                     |                  |
  |<--hello-------------|                  |  always JSON, first message
  |                     |                  |
  |--authenticate------>|                  |  validate JWT (L8 auth.rs)
  |<--ok----------------|                  |
  |                     |                  |
  |--begin(db,rw)------>|                  |
  |                     |--begin_mutation->|  allocate begin_ts
  |                     |<--tx-------------|
  |<--ok(tx:1)----------|                  |
  |                     |                  |
  |--insert(tx:1,...)-->|                  |
  |                     |--tx.insert----->|  buffer in write set
  |                     |<--doc_id--------|
  |<--ok(doc_id)--------|                  |
  |                     |                  |
  |--commit(tx:1)------>|                  |
  |                     |--tx.commit----->|  OCC + WAL + materialize
  |                     |<--CommitResult--|
  |<--ok(commit_ts:42)--|                  |
  |                     |                  |
  |  (if subscribed)    |                  |
  |<--invalidation------|  (pushed from subscription channel)
  |                     |                  |
  |--[disconnect]------>|                  |  rollback open txns,
  |                     |                  |  remove subscriptions
```

## 9. Create Collection Flow

```
Caller          Database(L6)    CatalogCache    WAL(L2)     StorageEngine(L2)    CatalogBTree(L2)
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

## 10. Document Insert: Layer 2 vs Layer 3 Boundary

```
                 Layer 3 (Document Store)           |     Layer 2 (Storage Engine)
                 Domain-aware: DocId, Ts,           |     Domain-agnostic: bytes,
                 tombstones, MVCC                   |     pages, B-tree
                                                    |
  Caller: insert_version(doc_id, commit_ts, body)   |
           |                                        |
           v                                        |
  1. Construct MVCC key:                            |
     key = doc_id[16] || inv_ts[8]                  |
     (inv_ts = u64::MAX - commit_ts)                |
                                                    |
  2. Decide inline vs external:                     |
     if body.len() > EXTERNAL_THRESHOLD:            |
       href = heap.store(body)  --------------------+-->  Heap::store(bytes)
       value = flags[1] || len[4] || href[6]        |       -> SlottedPage insert
     else:                                          |
       value = flags[1] || len[4] || body[var]      |
                                                    |
  3. Insert into primary B-tree:                    |
     btree.insert(key, value)  ---------------------+-->  BTree::insert(&[u8], &[u8])
                                                    |       -> leaf page lookup
                                                    |       -> slot insert
                                                    |       -> split if full
                                                    |
  4. Insert secondary index entries:                |
     for each index on collection:                  |
       scalars = extract(doc, field_paths)          |
       sec_key = encode(scalars) || doc_id || ts    |
       sec_btree.insert(sec_key, &[])  ------------+-->  BTree::insert(&[u8], &[u8])
                                                    |
  -------------------------------------------------  |
  Layer 3 knows: DocId, Ts, tombstones, MVCC keys,  |  Layer 2 knows: bytes, pages,
  inline/external threshold, index field extraction  |  slotted pages, B-tree insert/split
```
