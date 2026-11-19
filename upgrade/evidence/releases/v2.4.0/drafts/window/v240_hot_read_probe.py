"""DL v2.4.0 window gate G-hot (READ-ONLY): Query -> Stream hot reads, run inside a live Query container.

Purpose: the window 1 RCA (2026-11-19) showed that a Stream replica on another catalog revision refuses Query's hot
reads (gateway hot_view.rs HOT_AUTHORITY_MISMATCH), which made the TS writer's quiet-QUOTE probes DATA_NOT_READY.
This probe builds the Query's own hot client (same mTLS identity, signing key and expectation, from the container
environment) and reads the latest record of a few OKX SWAP QUOTE bindings, first through the client exactly as the
Query configures it ("ALL", the gate), then through each target alone (diagnostic).
Boundary: gRPC reads only; prints target, symbol and outcome code, never key material. No writes.
Usage: docker exec -i <query container> python3 -B - [canonical symbols...] < this file
"""
import os, sys, grpc
from qdl.runtime.stable import StableRuntimeConfig
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.kn_query_backend import KnCursorSettings
from qdl.runtime import kn_hot_view as hv
from qdl.projection.state_contract import LogicalProductKey
config = StableRuntimeConfig.from_environment("query_v2")
catalog = StableSourceCatalog.load(config.source_bindings_path)
settings = KnCursorSettings.from_environment(os.environ, environment=config.environment)
syms = sys.argv[1:] or ["BTC-USDT", "BAND-USDT", "ADA-USDT"]
picked = [b for b in catalog.bindings for i in [b.instrument.identity] if b.feed.value == "QUOTE" and i.venue == "OKX"
          and i.market == "SWAP" and i.canonical_symbol in syms]
print("query catalog_revision", catalog.catalog_revision, "bindings", len(picked))
configured = os.environ["QDL_KN_HOT_READ_TARGETS"]
for target in ["ALL", *configured.split(",")]:
    env = dict(os.environ, QDL_KN_HOT_READ_TARGETS=configured if target == "ALL" else target)
    client = hv.hot_client_from_environment(env, settings=settings, catalog=catalog, config=config)
    for b in picked:
        i = b.instrument.identity
        lpk = LogicalProductKey.for_product(environment=config.environment, venue=i.venue, market=i.market,
                                            instrument_uid=i.instrument_uid, feed=b.feed.value, interval=None)
        try:
            client.latest(b, lpk); out = "OK"
        except hv.HotViewUnavailable as e:
            cause = e.__cause__
            code = cause.code().name if isinstance(cause, grpc.RpcError) else ""
            det = cause.details() if isinstance(cause, grpc.RpcError) else ""
            out = f"{type(e).__name__}:{e} {code} {det}"
        print(target, i.canonical_symbol, out)
    client.close()
