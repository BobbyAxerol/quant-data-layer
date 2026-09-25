//! The market cache (KN-3 K3.3/K3.4, decisions D6-D8).
//!
//! Key layout under one prefix (`kn3:<environment>:`), every u64 as a decimal
//! string: `ptr:<lpk>` per-product ready/staging pointer and fence,
//! `l:<g>:<lpk>` latest value + source coordinate, `bm:<g>:<lpk>` BAR meta,
//! `b:<g>:<lpk>:<bucket>` BAR rows by open time, `rk:<g>:<lpk>` fact keys that
//! are not the current row, `cx:<g>:<lpk>` recent conflicts, `own:<t>:<p>`
//! partition owner fence and `ckpt:<t>:<p>` checkpoint. All writes of a batch
//! go through `apply.lua` in one atomic call (see its header).

use redis::{Connection, Script};

/// Opens per BAR bucket (listpack). 112 after the KN-3 R1 real-mix sweep:
/// 743.1 B/row vs 816.9 at the KN-1 value 116 (allocator size classes).
pub const BUCKET_OPENS: u64 = 112;

const APPLY_LUA: &str = include_str!("apply.lua");

/// The bucket of an open: `open_ms div (BUCKET_OPENS x interval_ms)`, so a
/// bucket never holds more than `BUCKET_OPENS` opens of a fixed interval.
pub fn bucket_of(open_ms: u64, interval_ms: u64) -> u64 {
    open_ms / (BUCKET_OPENS * interval_ms.max(1))
}

#[derive(Clone, Debug)]
pub struct Layout {
    prefix: String,
}

impl Layout {
    pub fn new(environment: &str) -> Self {
        Self {
            prefix: format!("kn3:{environment}:"),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn key(&self, parts: &[&str]) -> String {
        format!("{}{}", self.prefix, parts.join(":"))
    }

    pub fn pointer(&self, lpk: &str) -> String {
        self.key(&["ptr", lpk])
    }
    pub fn generation_counter(&self) -> String {
        self.key(&["gen"])
    }
    pub fn latest(&self, generation: u64, lpk: &str) -> String {
        self.key(&["l", &generation.to_string(), lpk])
    }
    pub fn bar_meta(&self, generation: u64, lpk: &str) -> String {
        self.key(&["bm", &generation.to_string(), lpk])
    }
    pub fn bar_bucket(&self, generation: u64, lpk: &str, bucket: u64) -> String {
        self.key(&["b", &generation.to_string(), lpk, &bucket.to_string()])
    }
    pub fn fact_keys(&self, generation: u64, lpk: &str) -> String {
        self.key(&["rk", &generation.to_string(), lpk])
    }
    pub fn conflicts(&self, generation: u64, lpk: &str) -> String {
        self.key(&["cx", &generation.to_string(), lpk])
    }
    pub fn owner(&self, topic: &str, partition: i32) -> String {
        self.key(&["own", topic, &partition.to_string()])
    }
    pub fn checkpoint(&self, topic: &str, partition: i32) -> String {
        self.key(&["ckpt", topic, &partition.to_string()])
    }
    /// Superseded generations waiting for reclaim: `<generation>|<lpk>`
    /// members added by `apply.lua` in the same script as the swap (D20).
    pub fn retire(&self) -> String {
        self.key(&["retire"])
    }
    /// Products staged from `topic`/`partition` (cold build registry).
    pub fn registry(&self, topic: &str, partition: i32) -> String {
        self.key(&["parts", topic, &partition.to_string()])
    }
    /// Set of LPKs an operator asked to rebuild (D17); the owner of each
    /// product takes its request.
    pub fn rebuild_requests(&self) -> String {
        self.key(&["rebuild"])
    }
}

/// A product's pointer (contract section 5).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Pointer {
    pub ready: Option<u64>,
    pub staging: Option<u64>,
    pub fence: u64,
}

impl Pointer {
    fn args(&self) -> [String; 3] {
        [
            self.ready.map(|g| g.to_string()).unwrap_or_default(),
            self.staging.map(|g| g.to_string()).unwrap_or_default(),
            self.fence.to_string(),
        ]
    }

    /// The generation a live record of this product is written to: the
    /// ready one, or the staging one of a product not yet ready. A staging
    /// generation next to a ready one belongs to a per-product rebuild, which
    /// replays the log into it itself (D17: no dual write).
    pub fn targets(&self) -> Vec<u64> {
        self.ready.or(self.staging).into_iter().collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub next: i64,
    pub fence: u64,
    pub at_ms: u64,
}

/// One operation of an atomic batch. `pointer` is what the caller read; a
/// change since then is a CAS miss and nothing of the batch is applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// Start (or restart) staging `generation` for the product.
    Stage {
        lpk: String,
        pointer: Pointer,
        generation: u64,
    },
    /// Publish the staged generation as ready (fence + 1).
    Publish { lpk: String, pointer: Pointer },
    /// The product has no state any more: drop its pointer (NOT_READY).
    Unpublish { lpk: String, pointer: Pointer },
    /// Discard the product's unpublished staging generation (retired in the
    /// same script); refused by CAS if it was published meanwhile (D23).
    Unstage { lpk: String, pointer: Pointer },
    Latest {
        lpk: String,
        generation: u64,
        pointer: Pointer,
        /// Source offset of the current entry (`None` = absent).
        expected_offset: Option<u64>,
        value: Vec<u8>,
        topic_id: String,
        partition: u32,
        offset: u64,
    },
    DeleteLatest {
        lpk: String,
        generation: u64,
        pointer: Pointer,
    },
    Bar {
        lpk: String,
        generation: u64,
        pointer: Pointer,
        bucket: u64,
        open_ms: u64,
        /// 48-byte trailer of the current row (`None` = absent).
        expected_trailer: Option<Vec<u8>>,
        row: Vec<u8>,
        is_final: bool,
        /// Fact key suffix (`f<rev>|<sha16>`) of the row this one replaces.
        superseded: Option<String>,
    },
    /// A fact that is not applied: remember its key, count a conflict.
    BarNote {
        lpk: String,
        generation: u64,
        pointer: Pointer,
        open_ms: u64,
        fact: Option<String>,
        conflict: Option<String>,
    },
    Floor {
        lpk: String,
        generation: u64,
        pointer: Pointer,
        floor_ms: u64,
        /// Buckets entirely below the floor.
        buckets: Vec<u64>,
        /// The bucket holding the floor (partially below it), if any.
        boundary: Option<u64>,
    },
}

impl Op {
    fn push_args(&self, args: &mut Vec<Vec<u8>>) {
        let text = |value: &str| value.as_bytes().to_vec();
        let pointer_args = |pointer: &Pointer, args: &mut Vec<Vec<u8>>| {
            for value in pointer.args() {
                args.push(value.into_bytes());
            }
        };
        match self {
            Op::Stage {
                lpk,
                pointer,
                generation,
            } => {
                args.push(text("S"));
                args.push(text(lpk));
                pointer_args(pointer, args);
                args.push(generation.to_string().into_bytes());
            }
            Op::Publish { lpk, pointer } => {
                args.push(text("P"));
                args.push(text(lpk));
                pointer_args(pointer, args);
            }
            Op::Unpublish { lpk, pointer } => {
                args.push(text("U"));
                args.push(text(lpk));
                pointer_args(pointer, args);
            }
            Op::Unstage { lpk, pointer } => {
                args.push(text("X"));
                args.push(text(lpk));
                pointer_args(pointer, args);
            }
            Op::Latest {
                lpk,
                generation,
                pointer,
                expected_offset,
                value,
                topic_id,
                partition,
                offset,
            } => {
                args.push(text("L"));
                args.push(text(lpk));
                args.push(generation.to_string().into_bytes());
                pointer_args(pointer, args);
                args.push(
                    expected_offset
                        .map(|o| o.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                );
                args.push(value.clone());
                args.push(text(topic_id));
                args.push(partition.to_string().into_bytes());
                args.push(offset.to_string().into_bytes());
            }
            Op::DeleteLatest {
                lpk,
                generation,
                pointer,
            } => {
                args.push(text("D"));
                args.push(text(lpk));
                args.push(generation.to_string().into_bytes());
                pointer_args(pointer, args);
            }
            Op::Bar {
                lpk,
                generation,
                pointer,
                bucket,
                open_ms,
                expected_trailer,
                row,
                is_final,
                superseded,
            } => {
                args.push(text("B"));
                args.push(text(lpk));
                args.push(generation.to_string().into_bytes());
                pointer_args(pointer, args);
                args.push(bucket.to_string().into_bytes());
                args.push(open_ms.to_string().into_bytes());
                args.push(expected_trailer.clone().unwrap_or_default());
                args.push(row.clone());
                args.push(text(if *is_final { "1" } else { "0" }));
                args.push(text(superseded.as_deref().unwrap_or_default()));
            }
            Op::BarNote {
                lpk,
                generation,
                pointer,
                open_ms,
                fact,
                conflict,
            } => {
                args.push(text("N"));
                args.push(text(lpk));
                args.push(generation.to_string().into_bytes());
                pointer_args(pointer, args);
                args.push(open_ms.to_string().into_bytes());
                args.push(text(fact.as_deref().unwrap_or_default()));
                args.push(text(conflict.as_deref().unwrap_or_default()));
            }
            Op::Floor {
                lpk,
                generation,
                pointer,
                floor_ms,
                buckets,
                boundary,
            } => {
                args.push(text("F"));
                args.push(text(lpk));
                args.push(generation.to_string().into_bytes());
                pointer_args(pointer, args);
                args.push(floor_ms.to_string().into_bytes());
                args.push(
                    buckets
                        .iter()
                        .map(u64::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                        .into_bytes(),
                );
                args.push(
                    boundary
                        .map(|b| b.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                );
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Applied {
    /// Every op applied and the checkpoint moved; one result per op.
    Ok(Vec<String>),
    /// Another owner took the partition: nothing applied.
    Zombie { current_owner: String },
    /// These op indexes (0-based) no longer match: nothing applied.
    Miss(Vec<usize>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheError {
    /// `maxmemory` reached with noeviction: typed pressure, nothing applied.
    MemoryPressure(String),
    Redis(String),
}

impl CacheError {
    fn from(error: redis::RedisError) -> Self {
        let text = error.to_string();
        if text.contains("OOM") {
            CacheError::MemoryPressure(text)
        } else {
            CacheError::Redis(text)
        }
    }
}

pub struct Cache {
    pub layout: Layout,
    client: redis::Client,
    connection: Connection,
    apply: Script,
    /// Injected faults (tests only, D23).
    #[cfg(feature = "fault-injection")]
    pub faults: std::sync::Arc<std::sync::Mutex<crate::fault::CacheFaults>>,
}

/// Bound on one Redis round trip: a hung market cache surfaces as an error
/// (and a reconnect) instead of blocking stage B forever.
const IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

fn open_connection(client: &redis::Client) -> Result<Connection, CacheError> {
    let connection = client.get_connection().map_err(CacheError::from)?;
    connection
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(CacheError::from)?;
    connection
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(CacheError::from)?;
    Ok(connection)
}

fn parse_u64(value: Option<String>) -> Option<u64> {
    value.and_then(|text| text.parse().ok())
}

impl Cache {
    pub fn connect(url: &str, layout: Layout) -> Result<Self, CacheError> {
        let client = redis::Client::open(url).map_err(CacheError::from)?;
        let connection = open_connection(&client)?;
        Ok(Self {
            layout,
            client,
            connection,
            apply: Script::new(APPLY_LUA),
            #[cfg(feature = "fault-injection")]
            faults: Default::default(),
        })
    }

    /// Consume one injected fault picked by `pick` (always `false` without
    /// the `fault-injection` feature).
    #[cfg(feature = "fault-injection")]
    fn injected(&self, pick: impl FnOnce(&mut crate::fault::CacheFaults) -> &mut usize) -> bool {
        let mut faults = self.faults.lock().unwrap();
        let count = pick(&mut faults);
        if *count > 0 {
            *count -= 1;
            true
        } else {
            false
        }
    }

    #[cfg(feature = "fault-injection")]
    fn after_lost_reply(&self) {
        let mut faults = self.faults.lock().unwrap();
        faults.fail_pointer_reads += std::mem::take(&mut faults.unreadable_after_lost_reply);
    }

    #[cfg(not(feature = "fault-injection"))]
    #[inline(always)]
    fn after_lost_reply(&self) {}

    #[cfg(not(feature = "fault-injection"))]
    #[inline(always)]
    fn injected(&self, _pick: impl FnOnce(&mut crate::fault::CacheFaults) -> &mut usize) -> bool {
        false
    }

    /// Up to `max` pending retirement members `<generation>|<lpk>` (D20).
    pub fn retirements(&mut self, max: usize) -> Result<Vec<String>, CacheError> {
        redis::cmd("SRANDMEMBER")
            .arg(self.layout.retire())
            .arg(max)
            .query(&mut self.connection)
            .map_err(CacheError::from)
    }

    /// A retirement member has been handled (reclaimed or found live).
    pub fn retired(&mut self, member: &str) -> Result<(), CacheError> {
        redis::cmd("SREM")
            .arg(self.layout.retire())
            .arg(member)
            .query::<u64>(&mut self.connection)
            .map_err(CacheError::from)?;
        Ok(())
    }

    /// Objects UNLINK has not freed yet: memory still counted (contract
    /// section 5: the next staging starts only after reclaim).
    pub fn lazyfree_pending(&mut self) -> Result<u64, CacheError> {
        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        Ok(info
            .lines()
            .find_map(|line| line.strip_prefix("lazyfree_pending_objects:"))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0))
    }

    /// Products an operator or a recovery asked to rebuild (D17/D21); a member
    /// is removed only when its product is published.
    pub fn rebuild_requests(&mut self) -> Result<Vec<String>, CacheError> {
        redis::cmd("SMEMBERS")
            .arg(self.layout.rebuild_requests())
            .query(&mut self.connection)
            .map_err(CacheError::from)
    }

    pub fn request_rebuilds(&mut self, lpks: &[String]) -> Result<(), CacheError> {
        if lpks.is_empty() {
            return Ok(());
        }
        redis::cmd("SADD")
            .arg(self.layout.rebuild_requests())
            .arg(lpks)
            .query::<u64>(&mut self.connection)
            .map_err(CacheError::from)?;
        Ok(())
    }

    pub fn rebuild_done(&mut self, lpk: &str) -> Result<(), CacheError> {
        redis::cmd("SREM")
            .arg(self.layout.rebuild_requests())
            .arg(lpk)
            .query::<u64>(&mut self.connection)
            .map_err(CacheError::from)?;
        Ok(())
    }

    /// A fresh connection after an I/O error (the market cache restarted or
    /// the link broke); the Lua script is reloaded on first use.
    pub fn reconnect(&mut self) -> Result<(), CacheError> {
        self.connection = open_connection(&self.client)?;
        Ok(())
    }

    /// The current owner fence of each partition (`None`: the key is gone,
    /// i.e. the cache lost its state).
    pub fn owners(&mut self, partitions: &[(String, i32)]) -> Result<Vec<Option<u64>>, CacheError> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let mut command = redis::cmd("MGET");
        for (topic, partition) in partitions {
            command.arg(self.layout.owner(topic, *partition));
        }
        let values: Vec<Option<String>> = command
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        Ok(values.into_iter().map(parse_u64).collect())
    }

    pub fn connection(&mut self) -> &mut Connection {
        &mut self.connection
    }

    /// Become the owner of `topic`/`partition`: returns the new owner fence;
    /// every older owner's batches are refused from now on.
    pub fn take_ownership(&mut self, topic: &str, partition: i32) -> Result<u64, CacheError> {
        redis::cmd("INCR")
            .arg(self.layout.owner(topic, partition))
            .query(&mut self.connection)
            .map_err(CacheError::from)
    }

    pub fn checkpoint(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<Checkpoint>, CacheError> {
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(self.layout.checkpoint(topic, partition))
            .arg("next")
            .arg("fence")
            .arg("at_ms")
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        let mut values = values.into_iter();
        let (next, fence, at_ms) = (values.next(), values.next(), values.next());
        Ok(
            match (
                next.flatten().and_then(|text| text.parse::<i64>().ok()),
                parse_u64(fence.flatten()),
                parse_u64(at_ms.flatten()),
            ) {
                (Some(next), Some(fence), Some(at_ms)) => Some(Checkpoint { next, fence, at_ms }),
                _ => None,
            },
        )
    }

    pub fn pointer(&mut self, lpk: &str) -> Result<Pointer, CacheError> {
        if self.injected(|faults| &mut faults.fail_pointer_reads) {
            return Err(CacheError::Redis("injected: pointer read".into()));
        }
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(self.layout.pointer(lpk))
            .arg("ready")
            .arg("staging")
            .arg("fence")
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        let mut values = values.into_iter();
        Ok(Pointer {
            ready: parse_u64(values.next().flatten()),
            staging: parse_u64(values.next().flatten()),
            fence: parse_u64(values.next().flatten()).unwrap_or(0),
        })
    }

    pub fn allocate_generation(&mut self) -> Result<u64, CacheError> {
        redis::cmd("INCR")
            .arg(self.layout.generation_counter())
            .query(&mut self.connection)
            .map_err(CacheError::from)
    }

    /// Current latest source offset of `lpk` in `generation`.
    pub fn latest_offset(&mut self, generation: u64, lpk: &str) -> Result<Option<u64>, CacheError> {
        let value: Option<String> = redis::cmd("HGET")
            .arg(self.layout.latest(generation, lpk))
            .arg("o")
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        Ok(parse_u64(value))
    }

    /// Current latest source coordinate `(topic_id, partition, offset)`.
    pub fn latest_coordinate(
        &mut self,
        generation: u64,
        lpk: &str,
    ) -> Result<Option<(String, u32, u64)>, CacheError> {
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(self.layout.latest(generation, lpk))
            .arg("t")
            .arg("p")
            .arg("o")
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        let mut values = values.into_iter();
        Ok(
            match (
                values.next().flatten(),
                values.next().flatten().and_then(|p| p.parse::<u32>().ok()),
                parse_u64(values.next().flatten()),
            ) {
                (Some(topic_id), Some(partition), Some(offset)) => {
                    Some((topic_id, partition, offset))
                }
                _ => None,
            },
        )
    }

    /// `(first, floor)` of a BAR product in `generation`.
    pub fn bar_bounds(
        &mut self,
        generation: u64,
        lpk: &str,
    ) -> Result<(Option<u64>, Option<u64>), CacheError> {
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(self.layout.bar_meta(generation, lpk))
            .arg("first")
            .arg("floor")
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        let mut values = values.into_iter();
        Ok((
            parse_u64(values.next().flatten()),
            parse_u64(values.next().flatten()),
        ))
    }

    /// Products staged from `topic`/`partition`.
    pub fn registry(&mut self, topic: &str, partition: i32) -> Result<Vec<String>, CacheError> {
        redis::cmd("SMEMBERS")
            .arg(self.layout.registry(topic, partition))
            .query(&mut self.connection)
            .map_err(CacheError::from)
    }

    /// Current BAR row of `lpk` at `open_ms` in `generation`.
    pub fn bar_row(
        &mut self,
        generation: u64,
        lpk: &str,
        bucket: u64,
        open_ms: u64,
    ) -> Result<Option<Vec<u8>>, CacheError> {
        redis::cmd("HGET")
            .arg(self.layout.bar_bucket(generation, lpk, bucket))
            .arg(open_ms.to_string())
            .query(&mut self.connection)
            .map_err(CacheError::from)
    }

    /// Apply a batch atomically and move the checkpoint of `topic`/`partition`
    /// to `next_offset`.
    #[allow(clippy::too_many_arguments)]
    pub fn apply(
        &mut self,
        topic: &str,
        partition: i32,
        owner_fence: u64,
        next_offset: i64,
        now_ms: u64,
        ops: &[Op],
    ) -> Result<Applied, CacheError> {
        let mut args: Vec<Vec<u8>> = vec![
            self.layout.prefix().as_bytes().to_vec(),
            topic.as_bytes().to_vec(),
            partition.to_string().into_bytes(),
            owner_fence.to_string().into_bytes(),
            next_offset.to_string().into_bytes(),
            now_ms.to_string().into_bytes(),
            ops.len().to_string().into_bytes(),
        ];
        for op in ops {
            op.push_args(&mut args);
        }
        let publishes = ops.iter().any(|op| matches!(op, Op::Publish { .. }));
        if publishes && self.injected(|faults| &mut faults.fail_before_publish) {
            return Err(CacheError::Redis("injected: before publish".into()));
        }
        let mut invocation = self.apply.prepare_invoke();
        for arg in &args {
            invocation.arg(arg.as_slice());
        }
        let reply: redis::Value = invocation
            .invoke(&mut self.connection)
            .map_err(CacheError::from)?;
        if publishes && self.injected(|faults| &mut faults.lose_publish_reply) {
            self.after_lost_reply();
            return Err(CacheError::Redis("injected: publish reply lost".into()));
        }
        parse_reply(reply)
    }

    /// Remove every key of a superseded generation of `lpk` (bounded by the
    /// product's BAR meta range). Returns the number of keys removed.
    /// `(rows in the meta, rows counted in the buckets first..=last)` of a
    /// BAR product in `generation` (rebuild verification, D17).
    pub fn bar_row_count(
        &mut self,
        generation: u64,
        lpk: &str,
        interval_ms: u64,
    ) -> Result<(u64, u64), CacheError> {
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(self.layout.bar_meta(generation, lpk))
            .arg("rows")
            .arg("first")
            .arg("last")
            .query(&mut self.connection)
            .map_err(CacheError::from)?;
        let mut values = values.into_iter();
        let rows = parse_u64(values.next().flatten()).unwrap_or(0);
        let (Some(first), Some(last)) = (
            parse_u64(values.next().flatten()),
            parse_u64(values.next().flatten()),
        ) else {
            return Ok((rows, 0));
        };
        let buckets: Vec<u64> =
            (bucket_of(first, interval_ms)..=bucket_of(last, interval_ms)).collect();
        let mut counted = 0u64;
        for chunk in buckets.chunks(64) {
            let mut pipe = redis::pipe();
            for bucket in chunk {
                pipe.cmd("HLEN")
                    .arg(self.layout.bar_bucket(generation, lpk, *bucket));
            }
            let lengths: Vec<u64> = pipe.query(&mut self.connection).map_err(CacheError::from)?;
            counted += lengths.iter().sum::<u64>();
        }
        Ok((rows, counted))
    }

    pub fn reclaim(
        &mut self,
        generation: u64,
        lpk: &str,
        interval_ms: Option<u64>,
    ) -> Result<u64, CacheError> {
        if self.injected(|faults| &mut faults.fail_reclaim) {
            return Err(CacheError::Redis("injected: reclaim".into()));
        }
        let mut keys = vec![
            self.layout.latest(generation, lpk),
            self.layout.fact_keys(generation, lpk),
            self.layout.conflicts(generation, lpk),
        ];
        if let Some(interval_ms) = interval_ms {
            let meta = self.layout.bar_meta(generation, lpk);
            let bounds: Vec<Option<String>> = redis::cmd("HMGET")
                .arg(&meta)
                .arg("first")
                .arg("last")
                .query(&mut self.connection)
                .map_err(CacheError::from)?;
            let mut bounds = bounds.into_iter();
            if let (Some(first), Some(last)) = (
                parse_u64(bounds.next().flatten()),
                parse_u64(bounds.next().flatten()),
            ) {
                for bucket in bucket_of(first, interval_ms)..=bucket_of(last, interval_ms) {
                    keys.push(self.layout.bar_bucket(generation, lpk, bucket));
                }
            }
            keys.push(meta);
        }
        let mut removed = 0u64;
        for chunk in keys.chunks(500) {
            let count: u64 = redis::cmd("UNLINK")
                .arg(chunk)
                .query(&mut self.connection)
                .map_err(CacheError::from)?;
            removed += count;
        }
        Ok(removed)
    }
}

fn text(value: &redis::Value) -> String {
    match value {
        redis::Value::Data(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        redis::Value::Status(status) => status.clone(),
        redis::Value::Int(number) => number.to_string(),
        _ => String::new(),
    }
}

fn parse_reply(reply: redis::Value) -> Result<Applied, CacheError> {
    let redis::Value::Bulk(items) = reply else {
        return Err(CacheError::Redis(format!(
            "unexpected apply reply {reply:?}"
        )));
    };
    match items.first().map(text).as_deref() {
        Some("OK") => {
            let results = match items.get(1) {
                Some(redis::Value::Bulk(results)) => results.iter().map(text).collect(),
                _ => Vec::new(),
            };
            Ok(Applied::Ok(results))
        }
        Some("ZOMBIE") => Ok(Applied::Zombie {
            current_owner: items.get(1).map(text).unwrap_or_default(),
        }),
        Some("MISS") => Ok(Applied::Miss(
            items
                .get(1)
                .map(text)
                .unwrap_or_default()
                .split(',')
                .filter_map(|index| index.parse::<usize>().ok())
                .map(|index| index - 1)
                .collect(),
        )),
        other => Err(CacheError::Redis(format!(
            "unexpected apply status {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_never_holds_more_than_bucket_opens() {
        for interval_ms in [60_000u64, 900_000, 86_400_000, 604_800_000] {
            // Opens on any grid offset (OKX calendar bars follow the venue).
            for offset in [0u64, 1, 28_800_000 % interval_ms] {
                let mut counts = std::collections::BTreeMap::new();
                for index in 0..1_000u64 {
                    let open = 1_700_000_000_000 - (1_700_000_000_000 % interval_ms)
                        + offset
                        + index * interval_ms;
                    *counts.entry(bucket_of(open, interval_ms)).or_insert(0u64) += 1;
                }
                assert!(counts.values().all(|&count| count <= BUCKET_OPENS));
            }
        }
    }

    #[test]
    fn ops_serialize_to_the_script_widths() {
        let pointer = Pointer {
            ready: Some(7),
            staging: None,
            fence: 3,
        };
        let cases: Vec<(Op, usize)> = vec![
            (
                Op::Stage {
                    lpk: "p".into(),
                    pointer: pointer.clone(),
                    generation: 9,
                },
                6,
            ),
            (
                Op::Publish {
                    lpk: "p".into(),
                    pointer: pointer.clone(),
                },
                5,
            ),
            (
                Op::Unstage {
                    lpk: "p".into(),
                    pointer: pointer.clone(),
                },
                5,
            ),
            (
                Op::Latest {
                    lpk: "p".into(),
                    generation: 7,
                    pointer: pointer.clone(),
                    expected_offset: Some(u64::MAX >> 1),
                    value: vec![0, 1, 2],
                    topic_id: "t".into(),
                    partition: 5,
                    offset: (1 << 62) + 1,
                },
                11,
            ),
            (
                Op::DeleteLatest {
                    lpk: "p".into(),
                    generation: 7,
                    pointer: pointer.clone(),
                },
                6,
            ),
            (
                Op::Bar {
                    lpk: "p".into(),
                    generation: 7,
                    pointer: pointer.clone(),
                    bucket: 1,
                    open_ms: 60_000,
                    expected_trailer: None,
                    row: vec![1; 60],
                    is_final: true,
                    superseded: None,
                },
                12,
            ),
            (
                Op::BarNote {
                    lpk: "p".into(),
                    generation: 7,
                    pointer: pointer.clone(),
                    open_ms: 60_000,
                    fact: Some("f1|abcd".into()),
                    conflict: None,
                },
                9,
            ),
            (
                Op::Floor {
                    lpk: "p".into(),
                    generation: 7,
                    pointer,
                    floor_ms: 60_000,
                    buckets: vec![0, 1],
                    boundary: Some(2),
                },
                9,
            ),
        ];
        for (op, width) in cases {
            let mut args = Vec::new();
            op.push_args(&mut args);
            assert_eq!(args.len(), width, "{op:?}");
        }
    }
}
