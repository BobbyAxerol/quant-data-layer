//! Injected market-cache faults for the crate's own tests (KN-3 Astra R3,
//! D23). The plan is attached to a `Cache` only with the `fault-injection`
//! feature, which the crate enables for its tests through a self
//! dev-dependency; a production build has no hook.

/// Counts of upcoming calls that fail.
#[derive(Debug, Default)]
pub struct CacheFaults {
    /// `apply` of a batch holding a publish fails before the script runs.
    pub fail_before_publish: usize,
    /// `apply` of a batch holding a publish runs the script (the server
    /// commits) and then reports an error, as a lost reply.
    pub lose_publish_reply: usize,
    /// `reclaim` fails before deleting anything.
    pub fail_reclaim: usize,
    /// `pointer` reads fail (the outcome of a swap cannot be read back).
    pub fail_pointer_reads: usize,
    /// Pointer reads that start failing when a publish reply is lost.
    pub unreadable_after_lost_reply: usize,
}
