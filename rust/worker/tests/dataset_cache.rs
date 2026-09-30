#![cfg(unix)]

use dispatch_worker::dataset_cache::CacheIndex;

#[test]
fn pinned_entries_are_never_eviction_candidates() {
    let mut cache = CacheIndex::new(100, 60).unwrap();
    cache.insert("old", 40).unwrap();
    cache.insert("new", 40).unwrap();
    cache.pin("old").unwrap();
    assert_eq!(cache.plan_eviction(40).unwrap(), vec!["new"]);
    assert!(cache.remove("old").is_err());
    cache.remove("new").unwrap();
    cache.insert("incoming", 40).unwrap();
    cache.pin("incoming").unwrap();
    assert_eq!(cache.used_bytes(), 80);
    cache.unpin("old").unwrap();
    assert_eq!(cache.plan_eviction(30).unwrap(), vec!["old"]);
}

#[test]
fn recency_and_capacity_failures_are_deterministic() {
    let mut cache = CacheIndex::new(100, 60).unwrap();
    cache.insert("a", 30).unwrap();
    cache.insert("b", 30).unwrap();
    cache.insert("c", 30).unwrap();
    cache.pin("a").unwrap();
    cache.unpin("a").unwrap();
    assert_eq!(cache.plan_eviction(20).unwrap(), vec!["b", "c"]);
    cache.pin("b").unwrap();
    cache.pin("c").unwrap();
    cache.pin("a").unwrap();
    assert!(cache.plan_eviction(20).is_err());
    assert!(cache.plan_eviction(101).is_err());
    assert!(cache.insert("duplicate", 0).is_err());
    assert!(cache.insert("a", 30).is_err());
}
