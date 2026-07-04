//! A tiny bounded LRU + TTL cache (feature 8).
//!
//! No new dependency: the workspace has no `lru`/`moka`, and a ~70-line struct
//! earns its keep over pulling a crate for this one use. Backed by a
//! `HashMap<K, (Instant, V)>` plus a `VecDeque<K>` recency list, guarded by a
//! `Mutex` because the handler is `Arc`-shared.
//!
//! ## The async-safety invariant (critique mustChange #4)
//!
//! The cache is SYNC. Its `Mutex` is a `std::sync::Mutex` and must NEVER be held
//! across an `.await`. The refill-on-miss pattern is therefore strictly:
//!
//! ```ignore
//! if let Some(v) = cache.get(&k) { return v; }   // lock acquired + released
//! let v = client.fetch().await?;                  // network, no lock held
//! cache.put(k, v.clone());                        // lock re-acquired + released
//! ```
//!
//! `get`/`put`/`invalidate*` each acquire and release the lock internally and
//! return owned clones, so a caller literally cannot hold the guard across an
//! await. Entries are cheap clones (`Vec<Song>`, `Vec<u8>` cover bytes).

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A bounded, TTL-expiring, LRU-evicting cache.
pub struct TtlLru<K, V> {
    inner: Mutex<Inner<K, V>>,
    cap: usize,
    ttl: Duration,
}

struct Inner<K, V> {
    map: HashMap<K, (Instant, V)>,
    /// Recency order, front = least-recently-used, back = most-recent.
    recency: VecDeque<K>,
}

impl<K, V> TtlLru<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    pub fn new(cap: usize, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                recency: VecDeque::new(),
            }),
            cap,
            ttl,
        }
    }

    /// Fetch a fresh entry. If present and within TTL, mark it most-recent and
    /// return a clone. If expired, evict it and return `None`. Lock is released
    /// before this returns.
    pub fn get(&self, k: &K) -> Option<V> {
        let mut g = self.inner.lock().unwrap();
        let fresh = match g.map.get(k) {
            Some((ts, _)) => ts.elapsed() < self.ttl,
            None => return None,
        };
        if !fresh {
            g.map.remove(k);
            g.recency.retain(|x| x != k);
            return None;
        }
        // Move to back (most-recent).
        g.recency.retain(|x| x != k);
        g.recency.push_back(k.clone());
        g.map.get(k).map(|(_, v)| v.clone())
    }

    /// Insert/replace an entry, evicting the LRU key while over capacity.
    pub fn put(&self, k: K, v: V) {
        let mut g = self.inner.lock().unwrap();
        g.recency.retain(|x| x != &k);
        g.recency.push_back(k.clone());
        g.map.insert(k, (Instant::now(), v));
        while g.map.len() > self.cap {
            if let Some(old) = g.recency.pop_front() {
                g.map.remove(&old);
            } else {
                break;
            }
        }
    }

    /// Drop one entry by key (no-op if absent).
    pub fn invalidate(&self, k: &K) {
        let mut g = self.inner.lock().unwrap();
        g.map.remove(k);
        g.recency.retain(|x| x != k);
    }
}

impl<V> TtlLru<String, V>
where
    V: Clone,
{
    /// Drop every entry whose key starts with `prefix`. Used by the star-bust
    /// invariant (invalidate all `album/*` rows when a star flips).
    pub fn invalidate_prefix(&self, prefix: &str) {
        let mut g = self.inner.lock().unwrap();
        let doomed: Vec<String> = g
            .map
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        for k in doomed {
            g.map.remove(&k);
            g.recency.retain(|x| x != &k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_then_expire() {
        let c: TtlLru<String, u32> = TtlLru::new(4, Duration::from_millis(30));
        c.put("a".into(), 1);
        assert_eq!(c.get(&"a".into()), Some(1));
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(c.get(&"a".into()), None, "expired entry must miss");
    }

    #[test]
    fn evicts_lru_over_capacity() {
        let c: TtlLru<String, u32> = TtlLru::new(2, Duration::from_secs(60));
        c.put("a".into(), 1);
        c.put("b".into(), 2);
        // touch a -> b is now LRU
        assert_eq!(c.get(&"a".into()), Some(1));
        c.put("c".into(), 3); // evicts b
        assert_eq!(c.get(&"b".into()), None);
        assert_eq!(c.get(&"a".into()), Some(1));
        assert_eq!(c.get(&"c".into()), Some(3));
    }

    #[test]
    fn invalidate_and_prefix() {
        let c: TtlLru<String, u32> = TtlLru::new(8, Duration::from_secs(60));
        c.put("artists".into(), 1);
        c.put("album/1".into(), 2);
        c.put("album/2".into(), 3);
        c.put("cover/x".into(), 4);
        c.invalidate(&"artists".into());
        assert_eq!(c.get(&"artists".into()), None);
        c.invalidate_prefix("album/");
        assert_eq!(c.get(&"album/1".into()), None);
        assert_eq!(c.get(&"album/2".into()), None);
        assert_eq!(c.get(&"cover/x".into()), Some(4), "prefix bust is scoped");
    }
}
