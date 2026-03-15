# Interaction Diagrams

## 1. Document Write Commit (Full 11-Step, Two-Task Protocol)

The commit protocol is split across two tasks: the **writer task** (CommitCoordinator, steps 1–5) handles local operations with no network dependency. The **replication task** (ReplicationRunner, steps 6–11) handles replication, visibility, subscriptions, and client response. The writer is never blocked on replication.

```
Caller       Database(L6)   CommitCoord(L5)   WAL(L2)   DocStore(L3)
  |              |                |               |           |
  |--tx.commit-->|                |               |           |
  |              |--CommitReq---->|               |           |
  |              |                |               |           |
  |              |   ┌─── WRITER TASK (steps 1–5, !Send) ────┐
  |              |   │                                        │
  |              |   │  1. VALIDATE                           │
  |              |   │  OCC check read_set vs commit_log      │
  |              |   │                                        │
  |              |   │  2. TIMESTAMP                           │
  |              |   │  commit_ts = ts_allocator.allocate()    │
  |              |   │                                        │
  |              |   │  3. PERSIST                             │
  |              |   │     |--append+fsync-->|                │
  |              |   │     |<---lsn---------|                │
  |              |   │                                        │
  |              |   │  4. MATERIALIZE                         │
  |              |   │     |----------insert_version--------->│
  |              |   │     |<---------------------------------│
  |              |   │     compute index deltas               │
  |              |   │     apply secondary index mutations    │
  |              |   │                                        │
  |              |   │  5. LOG + ENQUEUE                       │
  |              |   │  commit_log.append()                    │
  |              |   │  → enqueue ReplicationEntry to          │
  |              |   │    replication task via mpsc             │
  |              |   │  → immediately loop for next commit     │
  |              |   └────────────────────────────────────────┘
  |              |                                  mpsc
  |              |                                   │
  |              |                                   ▼
  |              |            ReplicationRunner(L5)   Replication(L7)   Subs(L5)
  |              |                  |                      |              |
  |              |   ┌─── REPLICATION TASK (steps 6–11, Send) ───────────┐
  |              |   │                                                    │
  |              |   │  6. REPLICATE                                      │
  |              |   │     |--replicate_and_wait-->|                     │
  |              |   │     |<-----ack-------------|                     │
  |              |   │                                                    │
  |              |   │  7. FENCE                                          │
  |              |   │     WAL append VISIBLE_TS record + fsync          │
  |              |   │                                                    │
  |              |   │  8. VISIBLE                                        │
  |              |   │     visible_ts.store(commit_ts)                    │
  |              |   │     (new readers can now see commit_ts data)       │
  |              |   │                                                    │
  |              |   │  9. INVALIDATE                                     │
  |              |   │     |--check_invalidation------------------------>│
  |              |   │     |<---Vec<InvalidationEvent>------------------|│
  |              |   │                                                    │
  |              |   │  10. SUBSCRIBE (if requested)                      │
  |              |   │     a. extend_for_deltas on read set              │
  |              |   │     b. register in subscription registry          │
  |              |   │                                                    │
  |              |   │  11. RESPOND + PUSH                                │
  |              |   │     push invalidation events (fire-and-forget)    │
  |              |<──│──CommitResult──────────────────────────────────────│
  |<--ok(commit_ts)  │                                                    │
  |              |   └────────────────────────────────────────────────────┘

=== FAILURE PATH (quorum lost at step 6) ===

ReplicationRunner(L5)         CommitLog(L5)
       |                           |
  6f. replicate_and_wait fails     |
       |                           |
  7f. RESPOND QuorumLost           |
       to this client              |
       |                           |
  8f. ROLLBACK COMMIT LOG          |
       |--remove_after(visible_ts)->|
       |                           |
  9f. DRAIN remaining queue entries |
       respond QuorumLost to all   |
       |                           |
  10f. STOP (drop replication_rx)  |
       Writer detects channel      |
       closed → stops accepting    |
       new commits                 |
       |                           |
  Page store: mutations for ts > visible_ts remain but are
  invisible. Rollback vacuum (L3) cleans them on restart.
```

Notes:
- The writer task processes steps 1–5 with no network I/O. After step 5, it enqueues to the replication task and immediately loops back for the next commit.
- The replication task processes entries in strict commit order. Replications can be pipelined (multiple in-flight to replicas), but `visible_ts` advances strictly in order.
- Without replication (`NoReplication`), step 6 is a no-op — `visible_ts` advances immediately.
- On replication failure, page mutations are NOT immediately rolled back. They are invisible (beyond `visible_ts`) and cleaned up by rollback vacuum on restart.
- Step 11 pushes invalidation events asynchronously — the committing client does NOT wait for other clients to receive their subscription notifications.

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

Subscription invalidation happens in the **replication task** (ReplicationRunner), AFTER `visible_ts` has advanced (step 8). This ensures clients never receive invalidation events for commits that might be rolled back due to replication failure.

```
ReplicationRunner(L5)    SubscriptionRegistry(L5)        Caller
     |                         |                            |
     | (after step 8: visible_ts advanced)                  |
     |                         |                            |
     | 9. INVALIDATE           |                            |
     |--check_invalidation---->|                            |
     |   (index_writes from    |                            |
     |    this commit)         |                            |
     |                         |                            |
     |   For each (coll, idx): |                            |
     |     For each key_write: |                            |
     |       old_key overlap?  |                            |
     |       new_key overlap?  |                            |
     |       -> collect (sub_id,|                           |
     |          query_id)      |                            |
     |                         |                            |
     |<--Vec<InvalidationEvent>|                            |
     |                         |                            |
     | 10. SUBSCRIBE           |                            |
     |   if subscription req:  |                            |
     |   extend_for_deltas()   |                            |
     |--register()------------>|                            |
     |                         |                            |
     | 11. RESPOND + PUSH      |                            |
     |   push_events() (fire-and-forget to other subs)     |
     |   respond to client via oneshot                     |
     |---CommitResult-------------------------------------------->|
```

Note: In embedded mode, the caller receives invalidation events via a subscription channel (mpsc::Receiver<InvalidationEvent>). In network mode (L8), the session pushes them over the wire.

## 5. WAL Replication Primary -> Replica

```
Primary(L6)      PrimaryReplicator(L7)    Network         ReplicaClient(L7)    Replica(L6)
  |                     |                    |                   |                  |
  | (commit step 6,     |                    |                   |                  |
  |  called by          |                    |                   |                  |
  |  ReplicationRunner) |                    |                   |                  |
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
                    |              |  + catalog mutations
                    |              |  (create/drop coll/idx)
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
                    | Rollback Vac |  if committed ts >
                    | (L3)         |  visible_ts: clean up
                    |              |  un-replicated entries
                    +------+------+
                           |
                    +------v------+
                    | Start Tasks  |  commit coordinator +
                    |              |  replication runner,
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
  |--begin(opts)---------------->|  allocate begin_ts = visible_ts
  |<--Ok(Transaction)-----------|
  |                                |
  |--tx.create_collection------->|  buffer CatalogMutation in write set,
  |      ("users")                |  return provisional CollectionId
  |<--Ok(CollectionId)-----------|
  |                                |
  |--tx.insert("users", doc)---->|  resolve name via write set (pending create),
  |                                |  buffer in write set
  |<--Ok(DocId)------------------|
  |                                |
  |--tx.get("users", &id)------->|  read-your-writes from write set
  |<--Ok(Some(doc))--------------|
  |                                |
  |--tx.commit()---------------->|  Writer: OCC + WAL + materialize
  |                                |  + commit log → enqueue
  |                                |  Replication: replicate →
  |                                |  visible_ts → subs → respond
  |<--CommitResult::Success------|
  |                                |
  |--begin(readonly: true)------>|  snapshot at visible_ts
  |<--Ok(Transaction)-----------|
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
  |                     |--begin(opts)--->|  allocate begin_ts = visible_ts
  |                     |<--tx-------------|
  |<--ok(tx:1)----------|                  |
  |                     |                  |
  |--create_collection->|                  |
  |   (tx:1,"users")   |--tx.create_coll->|  buffer CatalogMutation
  |                     |<--coll_id--------|
  |<--ok(coll_id)-------|                  |
  |                     |                  |
  |--insert(tx:1,...)-->|                  |
  |                     |--tx.insert----->|  buffer in write set
  |                     |<--doc_id--------|
  |<--ok(doc_id)--------|                  |
  |                     |                  |
  |--commit(tx:1)------>|                  |
  |                     |--tx.commit----->|  OCC + WAL + catalog +
  |                     |                  |  materialize + replicate
  |                     |<--CommitResult--|
  |<--ok(commit_ts:42)--|                  |
  |                     |                  |
  |  (if subscribed)    |                  |
  |<--invalidation------|  (pushed from subscription channel)
  |                     |                  |
  |--[disconnect]------>|                  |  rollback open txns,
  |                     |                  |  remove subscriptions
```

## 9. Create Collection Flow (Transactional)

Collection creation is a transactional operation. It goes through two phases:
**buffering** (when `tx.create_collection()` is called) and **apply** (at commit time).

### Phase 1: Buffer (tx.create_collection)
```
Caller          MutationTx(L6)    CatalogCache    WriteSet(L5)
   |               |                  |               |
   |--tx.create--->|                  |               |
   |  _collection  |                  |               |
   |  ("users")    |                  |               |
   |               | 1. Check: name not in committed catalog
   |               |---get_by_name--->|               |
   |               |<--None-----------|               |
   |               |                  |               |
   |               | 2. Check: name not in pending creates
   |               |---resolve_pending_collection---->|
   |               |<--None---------------------------|
   |               |                  |               |
   |               | 3. Allocate provisional ID       |
   |               |---next_coll_id-->|               |
   |               |<--id------------|               |
   |               |                  |               |
   |               | 4. Buffer CatalogMutation        |
   |               |---add_catalog_mutation---------->|
   |               |   CreateCollection{name, id}     |
   |               |                  |               |
   |<--Ok(id)------|                  |               |
```

### Phase 2: Apply (at commit, inside CommitCoordinator step 4a)
```
CommitCoord(L5)    StorageEngine(L2)    CatalogBTree(L2)    CatalogCache(L6)
   |                    |                    |                   |
   | For each CatalogMutation::CreateCollection:                |
   |                    |                    |                   |
   | 1. Allocate B-tree pages               |                   |
   |---create_btree()-->|                    |                   |
   |   (primary root)   |                    |                   |
   |<--root1------------|                    |                   |
   |---create_btree()-->|                    |                   |
   |   (_created_at)    |                    |                   |
   |<--root2------------|                    |                   |
   |                    |                    |                   |
   | 2. Catalog B-tree insert               |                   |
   |--------------------------------------insert--------------->|
   |   by-id key + entry                    |                   |
   |--------------------------------------insert--------------->|
   |   by-name key + id                     |                   |
   |                    |                    |                   |
   | 3. Update in-memory cache              |                   |
   |--------------------------------------------------add_coll->|
   |                    |                    |                   |
   | (then proceed to step 4b: apply data mutations)            |
```

Note: The WAL record for the entire commit (step 3 of the commit protocol) includes both catalog mutations and data mutations in a single `TxCommit` record. On recovery, WAL replay re-executes both phases.

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
