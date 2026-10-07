#[path = "book.rs"]
mod book;
use book::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
struct CountAlloc;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(p, l, n)
    }
}
#[global_allocator]
static ALLOC: CountAlloc = CountAlloc;
fn main() {
    let depth: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let count: u64 = std::env::args().nth(2).unwrap().parse().unwrap();
    let id = BookIdentity::new("TEST_ONLY", "TEST_BOOK", "TEST_CHANNEL").unwrap();
    let mut core = L2BookCore::new(
        BookConfig::new(
            id.clone(),
            SequencePolicy::PreviousSequence,
            ChecksumPolicy::RequireVerified,
            20,
        )
        .unwrap(),
    );
    let levels: Vec<_> = (0..depth)
        .flat_map(|i| {
            [
                BookLevelInput {
                    side: BookSide::Bid,
                    price: format!("{}", 10000 - i),
                    quantity: "1.25".into(),
                    order_count: None,
                },
                BookLevelInput {
                    side: BookSide::Ask,
                    price: format!("{}", 10001 + i),
                    quantity: "2.5".into(),
                    order_count: None,
                },
            ]
        })
        .collect();
    assert_eq!(
        core.apply_snapshot(&BookSnapshot {
            identity: id.clone(),
            generation: 1,
            sequence_end: 1,
            checksum: ChecksumEvidence::Verified,
            origin: SnapshotOrigin::WebSocket,
            levels
        }),
        BookOutcome::SnapshotApplied
    );
    let mut delta = BookDelta {
        identity: id,
        generation: 1,
        sequence_start: None,
        previous_sequence: Some(1),
        sequence_end: 2,
        checksum: ChecksumEvidence::Verified,
        updates: (0..5)
            .flat_map(|i| {
                [
                    BookLevelInput {
                        side: BookSide::Bid,
                        price: format!("{}", 10000 - i),
                        quantity: "3.25".into(),
                        order_count: None,
                    },
                    BookLevelInput {
                        side: BookSide::Ask,
                        price: format!("{}", 10001 + i),
                        quantity: "4.5".into(),
                        order_count: None,
                    },
                ]
            })
            .collect(),
    };
    let start_alloc = ALLOCS.load(Ordering::Relaxed);
    let start = std::time::Instant::now();
    for seq in 2..count + 2 {
        delta.previous_sequence = Some(seq - 1);
        delta.sequence_end = seq;
        assert_eq!(
            std::hint::black_box(core.apply_delta(&delta)),
            BookOutcome::DeltaApplied
        );
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.;
    let allocations = ALLOCS.load(Ordering::Relaxed) - start_alloc;
    println!("{{\"provenance\":\"TEST_ONLY_SYNTHETIC_MICROBENCH\",\"depth_per_side\":{depth},\"deltas\":{count},\"elapsed_ms\":{elapsed},\"allocations\":{allocations}}}");
    eprintln!("FINAL {:?}", core.view());
}
