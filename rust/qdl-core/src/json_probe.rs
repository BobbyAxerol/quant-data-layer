//! Narrow, borrowed JSON inspection for routing provider frames.
//!
//! A router needs a few top-level fields of a frame, not its decoded DOM. These
//! types let a hand-written serde visitor keep only the kind of a value (and a
//! string's text, borrowed when it has no escapes) and skip everything else
//! without allocating. Both recurse through `deserialize_any`, so serde_json
//! applies the same syntax, number, string and recursion-limit checks as a
//! `serde_json::Value` parse of the same bytes: a frame a `Value` parse refuses
//! is refused here with the same error, and vice versa.

use std::borrow::Cow;
use std::fmt;

use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;

/// What routing needs to know about one JSON value.
#[derive(Debug, Default)]
pub enum Kind<'a> {
    #[default]
    Other,
    Str(Cow<'a, str>),
    U64,
    Array,
    Object,
}

impl Kind<'_> {
    pub fn non_empty_str(&self) -> Option<&str> {
        match self {
            Kind::Str(value) if !value.is_empty() => Some(value),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Kind::Str(value) => Some(value),
            _ => None,
        }
    }
}

/// A nested value whose content routing ignores. It recurses through
/// `deserialize_any`, so serde_json applies the same recursion limit and
/// number/string validation as a `Value` parse, but keeps nothing.
pub struct Skip;

impl<'de> Deserialize<'de> for Skip {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SkipVisitor)
    }
}

struct SkipVisitor;

impl<'de> Visitor<'de> for SkipVisitor {
    type Value = Skip;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any JSON value")
    }
    fn visit_bool<E>(self, _: bool) -> Result<Skip, E> {
        Ok(Skip)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Skip, E> {
        Ok(Skip)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Skip, E> {
        Ok(Skip)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Skip, E> {
        Ok(Skip)
    }
    fn visit_str<E>(self, _: &str) -> Result<Skip, E> {
        Ok(Skip)
    }
    fn visit_unit<E>(self) -> Result<Skip, E> {
        Ok(Skip)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Skip, A::Error> {
        while seq.next_element::<Skip>()?.is_some() {}
        Ok(Skip)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Skip, A::Error> {
        while map.next_entry::<Skip, Skip>()?.is_some() {}
        Ok(Skip)
    }
}

impl<'de> Deserialize<'de> for Kind<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(KindVisitor)
    }
}

struct KindVisitor;

impl<'de> Visitor<'de> for KindVisitor {
    type Value = Kind<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any JSON value")
    }
    fn visit_bool<E>(self, _: bool) -> Result<Kind<'de>, E> {
        Ok(Kind::Other)
    }
    // `Value::as_u64` is Some for a non-negative integer only; serde_json
    // reports those through `visit_u64` and negatives through `visit_i64`.
    fn visit_i64<E>(self, value: i64) -> Result<Kind<'de>, E> {
        Ok(if value >= 0 { Kind::U64 } else { Kind::Other })
    }
    fn visit_u64<E>(self, _: u64) -> Result<Kind<'de>, E> {
        Ok(Kind::U64)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Kind<'de>, E> {
        Ok(Kind::Other)
    }
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Kind<'de>, E> {
        Ok(Kind::Str(Cow::Borrowed(value)))
    }
    fn visit_str<E>(self, value: &str) -> Result<Kind<'de>, E> {
        Ok(Kind::Str(Cow::Owned(value.to_owned())))
    }
    fn visit_string<E>(self, value: String) -> Result<Kind<'de>, E> {
        Ok(Kind::Str(Cow::Owned(value)))
    }
    fn visit_unit<E>(self) -> Result<Kind<'de>, E> {
        Ok(Kind::Other)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Kind<'de>, A::Error> {
        while seq.next_element::<Skip>()?.is_some() {}
        Ok(Kind::Array)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Kind<'de>, A::Error> {
        while map.next_entry::<Skip, Skip>()?.is_some() {}
        Ok(Kind::Object)
    }
}
