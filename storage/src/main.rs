use std::ops::Bound;
use std::path::Path;

use storage::btree::ScanDirection;
use storage::catalog_btree::{
    self, CatalogEntityType, CatalogIndexState, CollectionEntry, IndexEntry, IndexType,
};
use storage::engine::{StorageConfig, StorageEngine};
use storage::posting::{PostingEntry, PostingList};
use storage::recovery::NoOpHandler;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db_path = Path::new("tmp_data");

    // ── Open the storage engine ──
    let engine = StorageEngine::open(db_path, StorageConfig::default(), &mut NoOpHandler)?;
    println!("Storage engine opened at {:?}", db_path);

    // ── Create a B-tree and insert some data ──
    let tree = engine.create_btree()?;
    println!("Created B-tree with root page {}", tree.root_page());

    // Insert key-value pairs (raw bytes — Layer 2 is domain-agnostic).
    for i in 0u32..100 {
        let key = i.to_be_bytes(); // big-endian for sorted ordering
        let value = format!("value_{}", i);
        tree.insert(&key, value.as_bytes())?;
    }
    println!("Inserted 100 entries");

    // ── Point lookup ──
    let key_42 = 42u32.to_be_bytes();
    if let Some(val) = tree.get(&key_42)? {
        println!("GET key=42 -> {:?}", String::from_utf8_lossy(&val));
    }

    // ── Range scan [10, 20) ──
    let lower = 10u32.to_be_bytes();
    let upper = 20u32.to_be_bytes();
    let results: Vec<_> = tree
        .scan(
            Bound::Included(&lower[..]),
            Bound::Excluded(&upper[..]),
            ScanDirection::Forward,
        )
        .collect::<std::io::Result<Vec<_>>>()?;
    println!("SCAN [10..20): {} entries", results.len());
    for (k, v) in &results {
        let num = u32::from_be_bytes(k[..4].try_into().unwrap());
        println!("  {} -> {}", num, String::from_utf8_lossy(v));
    }

    // ── Delete a key ──
    let deleted = tree.delete(&key_42)?;
    println!("DELETE key=42: existed={}", deleted);
    assert!(tree.get(&key_42)?.is_none());

    // ── Heap (large blob storage) ──
    let blob = vec![0xABu8; 50_000]; // 50 KB blob
    let href = engine.heap_store(&blob)?;
    println!("Stored 50KB blob in heap: {:?}", href);
    let loaded = engine.heap_load(href)?;
    assert_eq!(loaded, blob);
    println!("Loaded blob back, size={}", loaded.len());

    // ── WAL (write-ahead log) ──
    let lsn = engine.append_wal(0x01, b"hello from WAL").await?;
    println!("Appended WAL record, LSN={}", lsn);

    // ── Catalog (use the engine's built-in catalog B-trees) ──
    let fh = engine.file_header();
    let catalog = engine.open_btree(fh.catalog_root_page.get());
    let name_idx = engine.open_btree(fh.catalog_name_root_page.get());

    // Register a collection (by ID + by name).
    let col = CollectionEntry {
        collection_id: 1,
        name: "users".to_string(),
        primary_root_page: tree.root_page(),
        doc_count: 99, // after delete
    };
    let id_key =
        catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, col.collection_id);
    catalog.insert(&id_key, &catalog_btree::serialize_collection(&col))?;

    let name_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, &col.name);
    name_idx.insert(
        &name_key,
        &catalog_btree::serialize_name_value(col.collection_id),
    )?;
    println!("Registered collection '{}'", col.name);

    // Register a GIN index (by ID + by name).
    let posting_tree = engine.create_btree()?;
    let pending_tree = engine.create_btree()?;
    let docterm_tree = engine.create_btree()?;

    let gin_idx = IndexEntry {
        index_id: 1,
        collection_id: 1,
        name: "users_tags_gin".to_string(),
        field_paths: vec![vec!["tags".to_string()]],
        root_page: posting_tree.root_page(),
        state: CatalogIndexState::Ready,
        index_type: IndexType::Gin,
        aux_root_pages: vec![pending_tree.root_page(), docterm_tree.root_page()],
        config: vec![],
    };
    let idx_id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, gin_idx.index_id);
    catalog.insert(&idx_id_key, &catalog_btree::serialize_index(&gin_idx))?;

    let idx_name_key =
        catalog_btree::make_catalog_name_key(CatalogEntityType::Index, &gin_idx.name);
    name_idx.insert(
        &idx_name_key,
        &catalog_btree::serialize_name_value(gin_idx.index_id),
    )?;
    println!(
        "Registered GIN index '{}' (posting={}, pending={}, docterm={})",
        gin_idx.name,
        posting_tree.root_page(),
        pending_tree.root_page(),
        docterm_tree.root_page()
    );

    // ── Posting list demo ──
    let list_a = PostingList::from_sorted(vec![
        PostingEntry {
            key: vec![0, 1],
            value: vec![],
        },
        PostingEntry {
            key: vec![0, 3],
            value: vec![],
        },
        PostingEntry {
            key: vec![0, 5],
            value: vec![],
        },
    ]);
    let list_b = PostingList::from_sorted(vec![
        PostingEntry {
            key: vec![0, 2],
            value: vec![],
        },
        PostingEntry {
            key: vec![0, 3],
            value: vec![],
        },
        PostingEntry {
            key: vec![0, 4],
            value: vec![],
        },
    ]);

    let union = list_a.union(&list_b);
    let intersect = list_a.intersect(&list_b);
    println!("PostingList union: {} entries", union.len()); // 5
    println!("PostingList intersect: {} entries", intersect.len()); // 1

    // Store a posting list in the B-tree (term -> encoded posting list).
    let encoded = union.encode();
    posting_tree.insert(b"rust", &encoded)?;
    let loaded = posting_tree.get(b"rust")?.unwrap();
    let decoded = PostingList::decode(&loaded)?;
    println!(
        "Stored+loaded posting list for term 'rust': {} entries",
        decoded.len()
    );

    // ── Checkpoint & close ──
    engine.checkpoint().await?;
    println!("Checkpoint complete");

    engine.close().await?;
    println!("Engine closed");

    // ── Reopen and look up collection by name ──
    let engine2 = StorageEngine::open(db_path, StorageConfig::default(), &mut NoOpHandler)?;
    let fh2 = engine2.file_header();
    let catalog2 = engine2.open_btree(fh2.catalog_root_page.get());
    let name_idx2 = engine2.open_btree(fh2.catalog_name_root_page.get());

    // Look up "users" collection by name
    let lookup_key = catalog_btree::make_catalog_name_key(CatalogEntityType::Collection, "users");
    let id_bytes = name_idx2
        .get(&lookup_key)?
        .expect("collection 'users' not found in name index");
    let collection_id = catalog_btree::deserialize_name_value(&id_bytes);
    println!(
        "Looked up 'users' by name -> collection_id={}",
        collection_id
    );

    // Fetch the full CollectionEntry by ID
    let id_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Collection, collection_id);
    let col_bytes = catalog2.get(&id_key)?.expect("collection entry not found");
    let col2 = catalog_btree::deserialize_collection(&col_bytes)?;
    println!(
        "Collection '{}': root_page={}, doc_count={}",
        col2.name, col2.primary_root_page, col2.doc_count
    );

    // Open the data B-tree and verify
    let tree2 = engine2.open_btree(col2.primary_root_page);
    let val = tree2.get(&10u32.to_be_bytes())?.unwrap();
    println!(
        "After reopen: GET key=10 -> {:?}",
        String::from_utf8_lossy(&val)
    );

    // Look up index by name too
    let idx_lookup =
        catalog_btree::make_catalog_name_key(CatalogEntityType::Index, "users_tags_gin");
    let idx_id_bytes = name_idx2
        .get(&idx_lookup)?
        .expect("index not found by name");
    let idx_id = catalog_btree::deserialize_name_value(&idx_id_bytes);
    let idx_key = catalog_btree::make_catalog_id_key(CatalogEntityType::Index, idx_id);
    let idx_bytes = catalog2.get(&idx_key)?.expect("index entry not found");
    let idx2 = catalog_btree::deserialize_index(&idx_bytes)?;
    println!(
        "Looked up index '{}' by name: root={}, aux={:?}",
        idx2.name, idx2.root_page, idx2.aux_root_pages
    );

    engine2.close().await?;
    println!("Done! Data persisted at {:?}", db_path);

    Ok(())
}
