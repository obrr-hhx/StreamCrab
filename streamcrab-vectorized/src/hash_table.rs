//! GroupHashTable: row-group based hash table for vectorized aggregation and join.
//!
//! Each unique key combination (across one or more Arrow columns) maps to a
//! group index. Callers store accumulator or payload data in a separate
//! side-array indexed by group index. This design keeps the hash table itself
//! small and allocation-free per-row.
//!
//! Supported key column types: Int64, Float64, Utf8, Boolean.
//! Key bytes are type-tagged so that, e.g., Int64(1) != Boolean(true).

use std::collections::HashMap;

use ahash::RandomState;
use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow_schema::DataType;

/// A row-group based hash table. Each unique key combination maps to a group
/// index. Accumulator/payload data is stored externally and indexed by group
/// index.
///
/// # Usage
/// ```rust,ignore
/// let mut ht = GroupHashTable::new();
/// let group = ht.find_or_create_group(&key_cols, row_idx);
/// accumulators[group].update(value);
/// ```
#[derive(serde::Serialize, serde::Deserialize)]
pub struct GroupHashTable {
    /// The hasher state, held for computing hashes of new keys.
    ///
    /// `RandomState` is not `Serialize`/`Deserialize`, so we skip it and
    /// rebuild from a fixed seed on deserialize.
    #[serde(skip, default = "default_hasher")]
    hasher: RandomState,
    /// Maps serialized key bytes -> group index.
    map: HashMap<Vec<u8>, usize, RandomState>,
    /// Number of distinct groups inserted so far.
    num_groups: usize,
}

fn default_hasher() -> RandomState {
    RandomState::new()
}

impl GroupHashTable {
    /// Create an empty hash table.
    pub fn new() -> Self {
        let hasher = RandomState::new();
        // Clone the seed into the map so both use the same hasher state.
        let map = HashMap::with_hasher(hasher.clone());
        Self {
            hasher,
            map,
            num_groups: 0,
        }
    }

    /// Find the group index for the key composed of `columns[*][row]`, creating
    /// a new group if this key combination has not been seen before.
    ///
    /// Returns the group index (0-based).
    pub fn find_or_create_group(&mut self, key_columns: &[ArrayRef], row: usize) -> usize {
        let key_bytes = serialize_row_key(key_columns, row);
        let next_group = self.num_groups;
        let entry = self.map.entry(key_bytes).or_insert(next_group);
        if *entry == next_group {
            self.num_groups += 1;
        }
        *entry
    }

    /// Return the number of distinct groups currently stored.
    pub fn num_groups(&self) -> usize {
        self.num_groups
    }

    /// Reset the table for a new window / new partition.
    pub fn clear(&mut self) {
        self.map.clear();
        self.num_groups = 0;
    }

    /// Serialize the table to bytes using bincode.
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }

    /// Deserialize a table from bytes produced by [`serialize`].
    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        let mut table: GroupHashTable = bincode::deserialize(data)?;
        // Re-create the hasher; the map keys/values are preserved by serde.
        // We rebuild the map with a fresh RandomState so that future inserts
        // hash correctly (the existing HashMap<Vec<u8>, usize> uses byte
        // equality for lookups so any hasher works for correctness).
        table.hasher = RandomState::new();
        Ok(table)
    }
}

impl Default for GroupHashTable {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Key serialization helpers
// ---------------------------------------------------------------------------

/// Serialize the key composed of `columns[*][row]` into a canonical byte
/// string. Each value is prefixed with a 1-byte type tag so that values of
/// different types that happen to produce the same bytes are kept distinct.
///
/// Supported types: Int64, Float64, Utf8, Boolean.
/// Null values are encoded as a dedicated null tag.
pub fn serialize_row_key(columns: &[ArrayRef], row: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(columns.len() * 9);
    for col in columns {
        match col.data_type() {
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                if arr.is_null(row) {
                    buf.push(TAG_NULL);
                } else {
                    buf.push(TAG_INT64);
                    buf.extend_from_slice(&arr.value(row).to_le_bytes());
                }
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                if arr.is_null(row) {
                    buf.push(TAG_NULL);
                } else {
                    buf.push(TAG_FLOAT64);
                    // Use raw bits so NaN == NaN (all NaN variants with same
                    // bit pattern are equal, which is fine for grouping).
                    buf.extend_from_slice(&arr.value(row).to_bits().to_le_bytes());
                }
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                if arr.is_null(row) {
                    buf.push(TAG_NULL);
                } else {
                    let s = arr.value(row).as_bytes();
                    buf.push(TAG_UTF8);
                    // Length-prefix the string bytes to avoid ambiguity between
                    // ("ab", "c") and ("a", "bc").
                    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buf.extend_from_slice(s);
                }
            }
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                if arr.is_null(row) {
                    buf.push(TAG_NULL);
                } else {
                    buf.push(TAG_BOOL);
                    buf.push(arr.value(row) as u8);
                }
            }
            other => {
                // Unsupported type: encode the data-type name so we at least
                // get consistent (though not optimal) grouping.
                let type_str = format!("{other:?}");
                buf.push(TAG_UNKNOWN);
                buf.extend_from_slice(&(type_str.len() as u32).to_le_bytes());
                buf.extend_from_slice(type_str.as_bytes());
            }
        }
    }
    buf
}

/// Compute a u64 hash of the key composed of `columns[*][row]`.
pub fn hash_row_key(hasher: &RandomState, columns: &[ArrayRef], row: usize) -> u64 {
    use std::hash::{BuildHasher, Hash, Hasher};
    let key_bytes = serialize_row_key(columns, row);
    let mut h = hasher.build_hasher();
    key_bytes.hash(&mut h);
    h.finish()
}

// Type tags used in serialize_row_key.
const TAG_NULL: u8 = 0x00;
const TAG_INT64: u8 = 0x01;
const TAG_FLOAT64: u8 = 0x02;
const TAG_UTF8: u8 = 0x03;
const TAG_BOOL: u8 = 0x04;
const TAG_UNKNOWN: u8 = 0xFF;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn i64_col(values: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }

    fn str_col(values: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(values))
    }

    fn f64_col(values: Vec<f64>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    fn bool_col(values: Vec<bool>) -> ArrayRef {
        Arc::new(BooleanArray::from(values))
    }

    // -----------------------------------------------------------------------
    // Test 1: rows with the same key get the same group index
    // -----------------------------------------------------------------------

    #[test]
    fn test_same_key_same_group() {
        let mut ht = GroupHashTable::new();
        let col = i64_col(vec![42, 7, 42, 7, 42]);

        let g0 = ht.find_or_create_group(&[col.clone()], 0); // key=42
        let g1 = ht.find_or_create_group(&[col.clone()], 1); // key=7
        let g2 = ht.find_or_create_group(&[col.clone()], 2); // key=42 again
        let g3 = ht.find_or_create_group(&[col.clone()], 3); // key=7 again
        let g4 = ht.find_or_create_group(&[col.clone()], 4); // key=42 again

        assert_eq!(g0, g2, "same key=42 should map to same group");
        assert_eq!(g0, g4, "same key=42 should map to same group");
        assert_eq!(g1, g3, "same key=7 should map to same group");
        assert_ne!(g0, g1, "different keys should map to different groups");
    }

    // -----------------------------------------------------------------------
    // Test 2: different keys produce different group indices
    // -----------------------------------------------------------------------

    #[test]
    fn test_different_keys_different_groups() {
        let mut ht = GroupHashTable::new();
        let col = str_col(vec!["alpha", "beta", "gamma"]);

        let ga = ht.find_or_create_group(&[col.clone()], 0);
        let gb = ht.find_or_create_group(&[col.clone()], 1);
        let gc = ht.find_or_create_group(&[col.clone()], 2);

        assert_ne!(ga, gb);
        assert_ne!(gb, gc);
        assert_ne!(ga, gc);
        assert_eq!(ht.num_groups(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 3: serialize / deserialize round-trip preserves groups
    // -----------------------------------------------------------------------

    #[test]
    fn test_serialize_deserialize_round_trip() {
        let mut ht = GroupHashTable::new();
        let col = i64_col(vec![1, 2, 3, 1, 2]);

        let g1 = ht.find_or_create_group(&[col.clone()], 0); // key=1
        let g2 = ht.find_or_create_group(&[col.clone()], 1); // key=2
        let g3 = ht.find_or_create_group(&[col.clone()], 2); // key=3

        let bytes = ht.serialize().expect("serialize should succeed");
        let ht2 = GroupHashTable::deserialize(&bytes).expect("deserialize should succeed");

        assert_eq!(ht2.num_groups(), 3);

        // Re-query the same rows against the deserialized table.
        // Because the map is restored, inserting existing keys must return
        // the original group indices.
        let col2 = i64_col(vec![1, 2, 3]);
        // find_or_create_group on a deserialized table with existing keys:
        // we need a mutable borrow, so clone.
        let mut ht2 = ht2;
        assert_eq!(ht2.find_or_create_group(&[col2.clone()], 0), g1);
        assert_eq!(ht2.find_or_create_group(&[col2.clone()], 1), g2);
        assert_eq!(ht2.find_or_create_group(&[col2.clone()], 2), g3);
        // No new groups should have been created.
        assert_eq!(ht2.num_groups(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 4: clear resets to empty
    // -----------------------------------------------------------------------

    #[test]
    fn test_clear_resets() {
        let mut ht = GroupHashTable::new();
        let col = i64_col(vec![10, 20, 30]);

        ht.find_or_create_group(&[col.clone()], 0);
        ht.find_or_create_group(&[col.clone()], 1);
        ht.find_or_create_group(&[col.clone()], 2);
        assert_eq!(ht.num_groups(), 3);

        ht.clear();
        assert_eq!(ht.num_groups(), 0);

        // After clear, inserting the same key should start from group 0 again.
        let g_new = ht.find_or_create_group(&[col.clone()], 0);
        assert_eq!(g_new, 0);
        assert_eq!(ht.num_groups(), 1);
    }

    // -----------------------------------------------------------------------
    // Test 5: multi-column composite key (Int64 + Utf8)
    // -----------------------------------------------------------------------

    #[test]
    fn test_multi_column_composite_key() {
        let mut ht = GroupHashTable::new();

        // Rows: (1, "a"), (1, "b"), (2, "a"), (1, "a")
        let id_col = i64_col(vec![1, 1, 2, 1]);
        let name_col = str_col(vec!["a", "b", "a", "a"]);
        let key_cols = vec![id_col, name_col];

        let g00 = ht.find_or_create_group(&key_cols, 0); // (1, "a")
        let g01 = ht.find_or_create_group(&key_cols, 1); // (1, "b")
        let g02 = ht.find_or_create_group(&key_cols, 2); // (2, "a")
        let g03 = ht.find_or_create_group(&key_cols, 3); // (1, "a") again

        assert_eq!(g00, g03, "(1,a) seen twice should be same group");
        assert_ne!(g00, g01, "(1,a) != (1,b)");
        assert_ne!(g00, g02, "(1,a) != (2,a)");
        assert_ne!(g01, g02, "(1,b) != (2,a)");
        assert_eq!(ht.num_groups(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 6: Float64 and Boolean key columns
    // -----------------------------------------------------------------------

    #[test]
    fn test_float64_and_bool_keys() {
        let mut ht = GroupHashTable::new();

        // Two rows with same float, one with different bool.
        let f_col = f64_col(vec![1.5, 1.5, 2.5]);
        let b_col = bool_col(vec![true, false, true]);
        let key_cols = vec![f_col, b_col];

        let g0 = ht.find_or_create_group(&key_cols, 0); // (1.5, true)
        let g1 = ht.find_or_create_group(&key_cols, 1); // (1.5, false)
        let g2 = ht.find_or_create_group(&key_cols, 2); // (2.5, true)

        assert_ne!(g0, g1);
        assert_ne!(g0, g2);
        assert_ne!(g1, g2);
        assert_eq!(ht.num_groups(), 3);

        // Inserting (1.5, true) again should return g0.
        let f_col2 = f64_col(vec![1.5]);
        let b_col2 = bool_col(vec![true]);
        let g0_again = ht.find_or_create_group(&[f_col2, b_col2], 0);
        assert_eq!(g0_again, g0);
        assert_eq!(ht.num_groups(), 3);
    }

    // -----------------------------------------------------------------------
    // Test 7: hash_row_key is consistent with serialize_row_key
    // -----------------------------------------------------------------------

    #[test]
    fn test_hash_row_key_consistency() {
        let col = i64_col(vec![100, 200]);
        let hasher = RandomState::new();

        let h0a = hash_row_key(&hasher, &[col.clone()], 0);
        let h0b = hash_row_key(&hasher, &[col.clone()], 0);
        let h1 = hash_row_key(&hasher, &[col.clone()], 1);

        assert_eq!(h0a, h0b, "same row hashed twice must be equal");
        assert_ne!(h0a, h1, "different values should (likely) hash differently");
    }
}
