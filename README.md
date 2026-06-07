# CRDT Map

A Rust library implementing Conflict-free Replicated Data Types (CRDTs) for distributed state management. Provides GCounter, PNCounter, LWWRegister, ORSet, and a CRDTMap for building eventually consistent distributed systems.

## Why This Matters

In distributed systems, achieving consistency without coordination is extraordinarily valuable. Traditional approaches require distributed locking, consensus rounds, or two-phase commits — all expensive and failure-prone. CRDTs provide a mathematically proven alternative: data structures that **always converge** regardless of message ordering, duplication, or delay.

CRDTs power real-world systems:
- **Riak KV** — Shopping carts, counters, sets
- **Amazon DynamoDB** — Conflict resolution for globally distributed tables
- **Automerge** — Collaborative editing (Google Docs-style)
- **Apple Notes** — Offline-first sync across devices

## Architecture

### GCounter (Grow-Only Counter)

A counter that only increases. Each replica maintains its own monotonic counter. The total value is the sum of all per-replica counters.

```
Replica A: {A: 5, B: 0}  → value = 5
Replica B: {A: 0, B: 3}  → value = 3
Merged:    {A: 5, B: 3}  → value = 8
```

Merge rule: component-wise `max` for each replica ID. This ensures:
- **Commutativity**: merge(A, B) = merge(B, A)
- **Associativity**: merge(A, merge(B, C)) = merge(merge(A, B), C)
- **Idempotence**: merge(A, A) = A

### PNCounter (Positive-Negative Counter)

Supports both increment and decrement via a pair of G-Counters:
- `P` counts increments
- `N` counts decrements
- Value = P.value() - N.value()

### LWWRegister (Last-Writer-Wins Register)

Stores a single value with a timestamp. On conflict, the value with the higher timestamp wins. Ties are broken deterministically by replica ID comparison.

**Warning**: LWW depends on clock synchronization. Use hybrid logical clocks (HLC) in production to minimize clock skew effects.

### ORSet (Observed-Remove Set)

A set supporting add and remove operations where:
- Each `add(e)` attaches a unique tag to the element
- `remove(e)` only removes tags that the calling replica has observed
- Concurrent add and remove of the same element: **add wins**

This provides intuitive semantics: if you haven't seen someone add an element, removing it won't affect their future observation of it.

### CRDTMap

A key-value store combining OR-Set (for key membership) with LWW-Register (for values). Supports insert, remove, and merge operations.

## Usage

```rust
use crdt_map::{GCounter, PNCounter, LWWRegister, ORSet, CRDTMap};

// === G-Counter ===
let mut c1 = GCounter::new("node-1");
let mut c2 = GCounter::new("node-2");
c1.increment(10);
c2.increment(20);
let merged = c1.merge(&c2);
assert_eq!(merged.value(), 30);

// === PN-Counter ===
let mut pn = PNCounter::new("node-1");
pn.increment(100);
pn.decrement(30);
assert_eq!(pn.value(), 70);

// === LWW Register ===
let mut reg = LWWRegister::new("initial", 1, "node-1");
reg.set("updated", 2, "node-1");
assert_eq!(reg.value(), &"updated");

// === OR-Set ===
let mut s: ORSet<&str> = ORSet::new("node-1");
s.add("hello");
s.add("world");
s.remove(&"hello");
assert!(!s.contains(&"hello"));
assert!(s.contains(&"world"));

// === CRDT Map ===
let mut m1: CRDTMap<String> = CRDTMap::new("node-1");
let mut m2: CRDTMap<String> = CRDTMap::new("node-2");
m1.insert("name", "Alice".to_string());
m2.insert("city", "NYC".to_string());
let merged = m1.merge(&m2);
assert_eq!(merged.get("name"), Some(&"Alice".to_string()));
assert_eq!(merged.get("city"), Some(&"NYC".to_string()));
```

## Mathematical Background

### Semilattice Theory

All state-based CRDTs (CvRDTs) form a **join-semilattice**: a partially ordered set where every pair of elements has a least upper bound (the merge operation).

For a CRDT with state space S and merge function ⊔:
1. **Commutativity**: ∀a,b ∈ S: a ⊔ b = b ⊔ a
2. **Associativity**: ∀a,b,c ∈ S: (a ⊔ b) ⊔ c = a ⊔ (b ⊔ c)
3. **Idempotence**: ∀a ∈ S: a ⊔ a = a

These three axioms guarantee that replicas converge regardless of merge order or duplication.

### GCounter Formalization

State: `S = { (r₁, c₁), (r₂, c₂), ..., (rₙ, cₙ) }` where `rᵢ` are replica IDs and `cᵢ ∈ ℕ`

```
increment(rᵢ): cᵢ ← cᵢ + 1
merge(A, B):   for all r: cᵣ ← max(A.cᵣ, B.cᵣ)
value():       Σᵢ cᵢ
```

### OR-Set Formalization

State: `(E, T)` where `E` is the set of `(element, tag)` pairs and `T` is the tombstone set.

```
add(e):        generate unique tag t; E ← E ∪ {(e, t)}
remove(e):     T ← T ∪ {t : (e, t) ∈ E}; E ← E \ {(e, t) : (e, t) ∈ E}
query(e):      ∃t : (e, t) ∈ E \ T
merge(A, B):   E ← (A.E ∪ B.E) \ (A.T ∪ B.T); T ← A.T ∪ B.T
```

The observed-remove property ensures that if replica A adds element e with tag t after replica B has already removed e (with different tags), the add wins because A's tag t is not in B's tombstone set.

### Convergence Proof Sketch

For any CvRDT, convergence is guaranteed because:
1. The merge function ⊔ is the least upper bound in the semilattice
2. State only moves "up" in the partial order (monotonicity)
3. The semilattice has a finite height (bounded state)
4. By the monotone convergence theorem, all sequences converge to the same fixed point ∎

## Performance Characteristics

| Operation | Time Complexity | Space Complexity |
|-----------|----------------|------------------|
| GCounter increment | O(1) | O(r) for r replicas |
| GCounter merge | O(r) | O(r) |
| PNCounter increment | O(1) | O(r) |
| LWW set | O(1) | O(1) |
| ORSet add | O(1) amortized | O(n) for n elements |
| ORSet remove | O(k) for k tags | O(n) |
| ORSet merge | O(n) | O(n) |
| CRDTMap insert | O(1) | O(n) |
| CRDTMap merge | O(n) | O(n) |

## License

MIT
