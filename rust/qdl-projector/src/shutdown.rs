//! Cooperative stop between atomic processing steps; no cancellation of commits.
use std::sync::{Condvar, Mutex};
use std::time::Duration;

#[derive(Default)]
pub struct Stop {
    requested: Mutex<bool>,
    changed: Condvar,
}

impl Stop {
    pub fn request(&self) {
        *self.requested.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }

    pub fn requested(&self) -> bool {
        *self.requested.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// True when interrupted. The predicate also handles spurious wakeups.
    pub fn wait(&self, duration: Duration) -> bool {
        let guard = self.requested.lock().unwrap_or_else(|e| e.into_inner());
        let (guard, _) = self
            .changed
            .wait_timeout_while(guard, duration, |stop| !*stop)
            .unwrap_or_else(|e| e.into_inner());
        *guard
    }
}

/// A timeout is a failed drain. Handles remain owned by the caller on failure.
pub async fn drain_threads(
    threads: &mut Vec<std::thread::JoinHandle<()>>,
    budget: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + budget;
    while threads.iter().any(|thread| !thread.is_finished()) {
        if tokio::time::Instant::now() >= deadline {
            return Err("shutdown deadline exhausted; unfinished workers remain".into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut panicked = false;
    for thread in threads.drain(..) {
        panicked |= thread.join().is_err();
    }
    if panicked {
        Err("task panicked during drain".into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc};
    use std::time::Instant;

    #[tokio::test]
    async fn blocked_drain_times_out_without_discarding_the_worker() {
        let (release, released) = mpsc::channel();
        let mut threads = vec![std::thread::spawn(move || {
            released.recv().unwrap();
        })];
        assert!(drain_threads(&mut threads, Duration::from_millis(20))
            .await
            .is_err());
        assert_eq!(threads.len(), 1);
        release.send(()).unwrap();
        drain_threads(&mut threads, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(threads.is_empty());
    }

    #[tokio::test]
    async fn worker_panic_cannot_be_reported_as_successful_drain() {
        let mut threads = vec![std::thread::spawn(|| panic!("test-only worker fault"))];
        assert!(drain_threads(&mut threads, Duration::from_secs(1))
            .await
            .is_err());
        assert!(threads.is_empty());
    }

    #[test]
    fn maintenance_wait_wakes_and_stop_is_sticky() {
        let stop = Arc::new(Stop::default());
        let other = stop.clone();
        let worker = std::thread::spawn(move || other.wait(Duration::from_secs(60)));
        stop.request();
        assert!(worker.join().unwrap());
        let start = Instant::now();
        assert!(stop.wait(Duration::from_secs(60)));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn ordinary_wait_does_not_request_stop() {
        let stop = Stop::default();
        assert!(!stop.wait(Duration::from_millis(2)));
        assert!(!stop.requested());
    }

    #[test]
    fn admitted_step_finishes_but_no_next_step_is_admitted() {
        let stop = Arc::new(Stop::default());
        let other = stop.clone();
        let (entered, observed) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut committed = 0;
            while !other.requested() {
                entered.send(()).unwrap();
                released.recv().unwrap();
                committed += 1;
            }
            committed
        });
        observed.recv_timeout(Duration::from_secs(1)).unwrap();
        stop.request();
        assert!(!worker.is_finished());
        release.send(()).unwrap();
        assert_eq!(worker.join().unwrap(), 1);
    }
}
