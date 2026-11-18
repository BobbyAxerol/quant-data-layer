//! DL v2.3.2: the measured cost of one session-alive frame on the gateway.
//!
//! Per tick a subscription does: the cached session status read (directory
//! listing + one `stat` per lane file; the JSON is parsed only when a file
//! changed), the frame rules, building the `StreamRecord` / `SubscribeResponse`
//! and the prost encode tonic performs. This measures those steps in-process
//! (no network, no HTTP/2 framing, no TLS) and prints the cost per frame and
//! the vCPU it implies at the design's 226 frames/s. Ignored by default:
//!
//! `cargo test -p qdl-stream-gateway --release --test session_alive_cost -- --ignored --nocapture`

use prost::Message as _;
use qdl_stream_gateway::generated::query_v2 as query;
use qdl_stream_gateway::session_liveness::{
    session_alive, session_alive_record, EventSession, SessionLivenessReader,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const NOW: i64 = 1_800_000_000_000_000_000;
const DESIGN_FRAMES_PER_SECOND: f64 = 226.0;

fn lane_dir(lanes: usize) -> PathBuf {
    let base = std::env::var_os("QDL_TEST_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = base.join(format!("qdl-alive-cost-{lanes}-{}", std::process::id()));
    let dir = root.join("okx-swap");
    std::fs::create_dir_all(&dir).unwrap();
    for lane in 0..lanes {
        let body = serde_json::json!({
            "schema": "qdl.provider-session-liveness.v1",
            "source_session_id": format!("okx-public-{lane}"),
            "connection_generation": 3,
            "state": "LIVE",
            "last_transport_at_ns": NOW - 200_000_000,
            "updated_at_ns": NOW - 200_000_000,
            "config_revision": 22,
        });
        std::fs::write(dir.join(format!("public-{lane}.json")), body.to_string()).unwrap();
    }
    root
}

fn measure<F: FnMut()>(iterations: u32, mut work: F) -> f64 {
    for _ in 0..iterations / 10 {
        work();
    }
    let started = Instant::now();
    for _ in 0..iterations {
        work();
    }
    started.elapsed().as_nanos() as f64 / f64::from(iterations)
}

#[test]
#[ignore = "measurement; run with --release --ignored --nocapture"]
fn session_alive_frame_cost() {
    let last = EventSession {
        venue: "OKX".into(),
        market: "SWAP".into(),
        source_session_id: "okx-public-0".into(),
        connection_generation: 3,
        config_revision: 21,
    };
    let token = "x".repeat(300); // a signed cursor v3 token is ~300 bytes
    for lanes in [4usize, 16] {
        let root = lane_dir(lanes);
        let reader = SessionLivenessReader::with_clock(root.clone(), Arc::new(|| NOW));
        let interval = Duration::from_millis(500);
        let status_ns = measure(50_000, || {
            let frame = session_alive(&reader, Some(&last), interval, Some(2_000), 7, 9, NOW);
            assert!(frame.is_ok());
        });
        let frame = session_alive(&reader, Some(&last), interval, Some(2_000), 7, 9, NOW).unwrap();
        let mut size = 0;
        let encode_ns = measure(200_000, || {
            let response = query::SubscribeResponse {
                record: Some(session_alive_record(frame.clone(), &token)),
                records: Vec::new(),
            };
            let bytes = response.encode_to_vec();
            size = bytes.len();
            std::hint::black_box(bytes);
        });
        let total = status_ns + encode_ns;
        println!(
            "{}",
            serde_json::json!({
                "event": "qdl_session_alive_frame_cost",
                "lane_files": lanes,
                "status_and_rules_ns_per_frame": status_ns.round(),
                "build_and_encode_ns_per_frame": encode_ns.round(),
                "total_ns_per_frame": total.round(),
                "encoded_response_bytes": size,
                "design_frames_per_second": DESIGN_FRAMES_PER_SECOND,
                "vcpu_at_design_rate": total * DESIGN_FRAMES_PER_SECOND / 1e9,
                "not_measured": "HTTP/2 framing, TLS, tokio wake-ups, the ingestor's extra writes",
            })
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
