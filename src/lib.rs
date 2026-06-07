//! # CRDT Map
//!
//! A library implementing Conflict-free Replicated Data Types (CRDTs) for
//! distributed systems. CRDTs allow independent updates on different replicas
//! without coordination, guaranteeing eventual consistency when replicas sync.
//!
//! ## Overview
//!
//! CRDTs are data structures that can be replicated across multiple nodes and
//! updated independently without coordination. They guarantee that:
//!
//! - Concurrent updates never conflict (mathematically proven)
//! - Replicas always converge to the same state when they sync
//! - No distributed locking or consensus is needed for updates
//!
//! This crate implements both CvRDTs (state-based) and common operation-based patterns.
//!
//! ## Example
//!
//! ```
//! use crdt_map::{GCounter, PNCounter, LWWRegister, ORSet, CRDTMap};
//!
//! // Grow-only counter
//! let mut c1 = GCounter::new("replica-1");
//! let mut c2 = GCounter::new("replica-2");
//! c1.increment(5);
//! c2.increment(3);
//! let merged = c1.merge(&c2);
//! assert_eq!(merged.value(), 8);
//! ```

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// Grow-only counter (G-Counter).
///
/// Each replica maintains a map of (replica_id → count). The value is the sum
/// of all per-replica counts. Merging takes the component-wise maximum.
///
/// This is a state-based CRDT (CvRDT). It only supports increments;
/// use `PNCounter` if you need decrements.
///
/// ## Mathematical Properties
///
/// - **Commutative**: merge(A, B) = merge(B, A)
/// - **Associative**: merge(A, merge(B, C)) = merge(merge(A, B), C)
/// - **Idempotent**: merge(A, A) = A
///
/// These three properties guarantee convergence.
#[derive(Debug, Clone, PartialEq)]
pub struct GCounter {
    /// Per-replica counts.
    counts: HashMap<String, u64>,
    /// This replica's identifier.
    replica_id: String,
}

impl GCounter {
    /// Create a new GCounter for the given replica.
    pub fn new(replica_id: &str) -> Self {
        let mut counts = HashMap::new();
        counts.insert(replica_id.to_string(), 0);
        Self {
            counts,
            replica_id: replica_id.to_string(),
        }
    }

    /// Increment this replica's counter by the given amount.
    pub fn increment(&mut self, amount: u64) {
        *self.counts.entry(self.replica_id.clone()).or_insert(0) += amount;
    }

    /// Get this replica's local count.
    pub fn local_value(&self) -> u64 {
        self.counts.get(&self.replica_id).copied().unwrap_or(0)
    }

    /// Get the total value (sum of all replica counts).
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Merge with another GCounter, taking component-wise maximum.
    pub fn merge(&self, other: &GCounter) -> GCounter {
        let mut merged = self.counts.clone();
        for (id, &count) in &other.counts {
            merged.entry(id.clone()).and_modify(|c| *c = (*c).max(count)).or_insert(count);
        }
        GCounter {
            counts: merged,
            replica_id: self.replica_id.clone(),
        }
    }

    /// Get all replica IDs.
    pub fn replica_ids(&self) -> Vec<String> {
        self.counts.keys().cloned().collect()
    }

    /// Get the count for a specific replica.
    pub fn count_for(&self, replica_id: &str) -> u64 {
        self.counts.get(replica_id).copied().unwrap_or(0)
    }
}

/// Positive-Negative Counter (PN-Counter).
///
/// Implements a counter supporting both increment and decrement using a pair
/// of G-Counters: one for increments (P) and one for decrements (N).
/// The value is P - N.
#[derive(Debug, Clone, PartialEq)]
pub struct PNCounter {
    positive: GCounter,
    negative: GCounter,
}

impl PNCounter {
    /// Create a new PN-Counter for the given replica.
    pub fn new(replica_id: &str) -> Self {
        Self {
            positive: GCounter::new(replica_id),
            negative: GCounter::new(replica_id),
        }
    }

    /// Increment the counter.
    pub fn increment(&mut self, amount: u64) {
        self.positive.increment(amount);
    }

    /// Decrement the counter.
    pub fn decrement(&mut self, amount: u64) {
        self.negative.increment(amount);
    }

    /// Get the current value (positive - negative).
    pub fn value(&self) -> i64 {
        self.positive.value() as i64 - self.negative.value() as i64
    }

    /// Merge with another PN-Counter.
    pub fn merge(&self, other: &PNCounter) -> PNCounter {
        PNCounter {
            positive: self.positive.merge(&other.positive),
            negative: self.negative.merge(&other.negative),
        }
    }
}

/// Last-Writer-Wins Register (LWW-Register).
///
/// Stores a single value with a timestamp. On conflict, the value with the
/// higher timestamp wins. If timestamps are equal, the value is chosen
/// deterministically by comparing the values themselves.
///
/// ## Important
///
/// LWW relies on synchronized clocks for correctness. In practice, use
/// hybrid logical clocks (HLC) or similar to avoid clock skew issues.
#[derive(Debug, Clone, PartialEq)]
pub struct LWWRegister<T: Clone + PartialEq> {
    /// The stored value.
    value: T,
    /// Timestamp (logical or physical).
    timestamp: u64,
    /// The replica that set this value.
    replica_id: String,
}

impl<T: Clone + PartialEq> LWWRegister<T> {
    /// Create a new LWW register with initial value and timestamp.
    pub fn new(value: T, timestamp: u64, replica_id: &str) -> Self {
        Self {
            value,
            timestamp,
            replica_id: replica_id.to_string(),
        }
    }

    /// Get the current value.
    pub fn value(&self) -> &T {
        &self.value
    }

    /// Get the timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Set a new value with a timestamp. Only updates if the new timestamp is higher.
    pub fn set(&mut self, value: T, timestamp: u64, replica_id: &str) {
        if timestamp > self.timestamp {
            self.value = value;
            self.timestamp = timestamp;
            self.replica_id = replica_id.to_string();
        }
    }

    /// Merge with another LWW register. The one with the higher timestamp wins.
    pub fn merge(&self, other: &LWWRegister<T>) -> LWWRegister<T> {
        if other.timestamp > self.timestamp {
            other.clone()
        } else if self.timestamp > other.timestamp {
            self.clone()
        } else {
            // Tie-break by replica_id for determinism
            if other.replica_id >= self.replica_id {
                other.clone()
            } else {
                self.clone()
            }
        }
    }
}

/// Observed-Remove Set (OR-Set).
///
/// A CRDT set where elements can be added and removed. Each add operation
/// attaches a unique tag; a remove operation removes specific observed tags.
/// This means:
///
/// - If replica A adds element X and replica B removes X concurrently,
///   the add wins (because B hasn't observed A's tag yet)
/// - Subsequent adds of X after a remove will succeed
///
/// ## Mathematical Model
///
/// The OR-Set maintains:
/// - A set of (element, unique_tag) pairs representing current members
/// - A set of observed tombstone tags
///
/// add(e) generates a unique tag t and adds (e, t) to the payload.
/// remove(e) adds all observed tags for e to the tombstone set.
/// merge takes the union of non-tombstoned entries and the union of tombstones.
#[derive(Debug, Clone)]
pub struct ORSet<T: Clone + Hash + Eq> {
    /// Active entries: element → set of unique tags.
    entries: HashMap<T, HashSet<u64>>,
    /// Tombstoned tags.
    tombstones: HashSet<u64>,
    /// Tag counter for generating unique tags.
    tag_counter: u64,
    /// Replica ID for unique tag generation.
    replica_id: String,
}

impl<T: Clone + Hash + Eq> ORSet<T> {
    /// Create a new OR-Set.
    pub fn new(replica_id: &str) -> Self {
        Self {
            entries: HashMap::new(),
            tombstones: HashSet::new(),
            tag_counter: 0,
            replica_id: replica_id.to_string(),
        }
    }

    /// Add an element to the set.
    pub fn add(&mut self, element: T) {
        self.tag_counter += 1;
        let tag = self.tag_counter;
        if !self.tombstones.contains(&tag) {
            self.entries.entry(element).or_default().insert(tag);
        }
    }

    /// Remove an element from the set. Only removes observed tags.
    pub fn remove(&mut self, element: &T) -> bool {
        if let Some(tags) = self.entries.remove(element) {
            self.tombstones.extend(tags);
            true
        } else {
            false
        }
    }

    /// Check if an element is in the set.
    pub fn contains(&self, element: &T) -> bool {
        self.entries.contains_key(element)
    }

    /// Get all elements in the set.
    pub fn elements(&self) -> Vec<T> {
        self.entries.keys().cloned().collect()
    }

    /// Get the number of elements.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Merge with another OR-Set.
    pub fn merge(&self, other: &ORSet<T>) -> ORSet<T> {
        let tombstones: HashSet<u64> = self.tombstones.union(&other.tombstones).copied().collect();
        let mut entries: HashMap<T, HashSet<u64>> = HashMap::new();
        for (elem, tags) in &self.entries {
            let filtered: HashSet<u64> = tags.difference(&tombstones).copied().collect();
            if !filtered.is_empty() {
                entries.insert(elem.clone(), filtered);
            }
        }
        for (elem, tags) in &other.entries {
            let filtered: HashSet<u64> = tags.difference(&tombstones).copied().collect();
            if !filtered.is_empty() {
                entries.entry(elem.clone()).or_default().extend(filtered);
            }
        }
        ORSet {
            entries,
            tombstones,
            tag_counter: self.tag_counter.max(other.tag_counter),
            replica_id: self.replica_id.clone(),
        }
    }
}

/// A CRDT-based key-value map combining LWW-Register values with OR-Set semantics.
///
/// Keys are managed by an OR-Set (add/remove), and values are LWW-Registers
/// that converge on the highest timestamp.
#[derive(Debug, Clone)]
pub struct CRDTMap<V: Clone + PartialEq> {
    /// Key-value pairs with LWW semantics.
    values: HashMap<String, LWWRegister<V>>,
    /// Key membership managed by OR-Set.
    keys: ORSet<String>,
    /// This replica's ID.
    replica_id: String,
    /// Logical clock for timestamps.
    clock: u64,
}

impl<V: Clone + PartialEq> CRDTMap<V> {
    /// Create a new CRDT Map.
    pub fn new(replica_id: &str) -> Self {
        Self {
            values: HashMap::new(),
            keys: ORSet::new(replica_id),
            replica_id: replica_id.to_string(),
            clock: 0,
        }
    }

    /// Insert a key-value pair.
    pub fn insert(&mut self, key: &str, value: V) {
        self.clock += 1;
        self.keys.add(key.to_string());
        self.values.insert(
            key.to_string(),
            LWWRegister::new(value, self.clock, &self.replica_id),
        );
    }

    /// Remove a key from the map.
    pub fn remove(&mut self, key: &str) -> bool {
        self.values.remove(key);
        self.keys.remove(&key.to_string())
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Option<&V> {
        if self.keys.contains(&key.to_string()) {
            self.values.get(key).map(|r| r.value())
        } else {
            None
        }
    }

    /// Check if the map contains a key.
    pub fn contains_key(&self, key: &str) -> bool {
        self.keys.contains(&key.to_string())
    }

    /// Get all keys.
    pub fn keys(&self) -> Vec<String> {
        self.keys.elements()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Merge with another CRDTMap.
    pub fn merge(&self, other: &CRDTMap<V>) -> CRDTMap<V> {
        let keys = self.keys.merge(&other.keys);
        let mut values = HashMap::new();
        let all_keys: HashSet<String> = self.values.keys().chain(other.values.keys()).cloned().collect();
        for key in all_keys {
            match (self.values.get(&key), other.values.get(&key)) {
                (Some(a), Some(b)) => {
                    values.insert(key, a.merge(b));
                }
                (Some(a), None) => {
                    values.insert(key, a.clone());
                }
                (None, Some(b)) => {
                    values.insert(key, b.clone());
                }
                (None, None) => {}
            }
        }
        CRDTMap {
            values,
            keys,
            replica_id: self.replica_id.clone(),
            clock: self.clock.max(other.clock),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcounter_increment() {
        let mut c = GCounter::new("r1");
        c.increment(5);
        c.increment(3);
        assert_eq!(c.local_value(), 8);
        assert_eq!(c.value(), 8);
    }

    #[test]
    fn test_gcounter_merge() {
        let mut c1 = GCounter::new("r1");
        let mut c2 = GCounter::new("r2");
        c1.increment(5);
        c2.increment(3);
        let merged = c1.merge(&c2);
        assert_eq!(merged.value(), 8);
        assert_eq!(merged.count_for("r1"), 5);
        assert_eq!(merged.count_for("r2"), 3);
    }

    #[test]
    fn test_gcounter_merge_idempotent() {
        let mut c = GCounter::new("r1");
        c.increment(5);
        let m1 = c.merge(&c);
        assert_eq!(m1, c);
    }

    #[test]
    fn test_gcounter_merge_commutative() {
        let mut c1 = GCounter::new("r1");
        let mut c2 = GCounter::new("r2");
        c1.increment(3);
        c2.increment(7);
        let m1 = c1.merge(&c2);
        let m2 = c2.merge(&c1);
        assert_eq!(m1.value(), m2.value());
    }

    #[test]
    fn test_pncounter_basic() {
        let mut c = PNCounter::new("r1");
        c.increment(10);
        c.decrement(3);
        assert_eq!(c.value(), 7);
    }

    #[test]
    fn test_pncounter_merge() {
        let mut c1 = PNCounter::new("r1");
        let mut c2 = PNCounter::new("r2");
        c1.increment(10);
        c2.decrement(5);
        let merged = c1.merge(&c2);
        assert_eq!(merged.value(), 5);
    }

    #[test]
    fn test_lww_register_basic() {
        let mut r = LWWRegister::new("a", 1, "r1");
        assert_eq!(r.value(), &"a");
        r.set("b", 2, "r1");
        assert_eq!(r.value(), &"b");
    }

    #[test]
    fn test_lww_register_ignores_old() {
        let mut r = LWWRegister::new("a", 10, "r1");
        r.set("b", 5, "r1");
        assert_eq!(r.value(), &"a"); // timestamp 5 < 10, ignored
    }

    #[test]
    fn test_lww_register_merge() {
        let r1 = LWWRegister::new("a", 1, "r1");
        let r2 = LWWRegister::new("b", 2, "r2");
        let merged = r1.merge(&r2);
        assert_eq!(merged.value(), &"b"); // higher timestamp wins
    }

    #[test]
    fn test_orset_add_contains() {
        let mut s: ORSet<&str> = ORSet::new("r1");
        s.add("x");
        s.add("y");
        assert!(s.contains(&"x"));
        assert!(s.contains(&"y"));
        assert!(!s.contains(&"z"));
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn test_orset_remove() {
        let mut s: ORSet<&str> = ORSet::new("r1");
        s.add("x");
        assert!(s.remove(&"x"));
        assert!(!s.contains(&"x"));
        assert!(s.is_empty());
    }

    #[test]
    fn test_orset_merge() {
        let mut s1: ORSet<&str> = ORSet::new("r1");
        let mut s2: ORSet<&str> = ORSet::new("r2");
        s1.add("a");
        s1.add("b");
        s2.add("b");
        s2.add("c");
        let merged = s1.merge(&s2);
        assert!(merged.contains(&"a"));
        assert!(merged.contains(&"b"));
        assert!(merged.contains(&"c"));
    }

    #[test]
    fn test_crdtmap_insert_get() {
        let mut m: CRDTMap<i32> = CRDTMap::new("r1");
        m.insert("x", 42);
        m.insert("y", 99);
        assert_eq!(m.get("x"), Some(&42));
        assert_eq!(m.get("y"), Some(&99));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn test_crdtmap_remove() {
        let mut m: CRDTMap<i32> = CRDTMap::new("r1");
        m.insert("x", 42);
        assert!(m.remove("x"));
        assert_eq!(m.get("x"), None);
    }

    #[test]
    fn test_crdtmap_merge() {
        let mut m1: CRDTMap<i32> = CRDTMap::new("r1");
        let mut m2: CRDTMap<i32> = CRDTMap::new("r2");
        m1.insert("x", 1);
        m2.insert("y", 2);
        let merged = m1.merge(&m2);
        assert_eq!(merged.get("x"), Some(&1));
        assert_eq!(merged.get("y"), Some(&2));
    }
}
