"""DL v2.4.2 r10 gate, READ-ONLY, run inside a Query container (`docker exec -i <query> python3 -B - < this`).

Checks, on the deployed image and the deployed files (not an authenticated HTTP request: the window has no alpha
signing key): (1) the mounted catalogue (QDL_STABLE_SOURCE_BINDINGS) holds no RVN-USDT-SWAP instrument or binding;
(2) no loaded consumer manifest (QDL_STABLE_CONSUMER_MANIFESTS) names its uid; (3) the image's V2QueryService turns an
RVN BAR 1d requirement into the typed INSTRUMENT_NOT_FOUND per-item refusal (snapshot, and a mixed snapshot batch keeps
the other item), the DL v2.4.2 slice 2b fix. Prints one line per check; exit 0 only if all pass.
"""
import asyncio, os, sys
from pathlib import Path
from qdl.consumer.manifest import ConsumerManifestLoader
from qdl.consumer.selectors import SelectorContext
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.query.contracts import CanonicalErrorCode, DataRequirement, QueryProblem
from qdl.query.entitlement import AccessPurpose, EntitlementPolicy
from qdl.query.results import InstrumentQuery, QueryBackendError
from qdl.query.service import QueryServiceError, V2QueryService
from qdl.domain.instrument import InstrumentRegistry

RVN = "2f110c43-e43d-5784-8ceb-9970d2d16742"
ok = True
catalog_path = Path(os.environ["QDL_STABLE_SOURCE_BINDINGS"])
catalog = StableSourceCatalog.load(catalog_path)
bad = [b.binding_id for b in catalog.bindings if b.instrument.identity.instrument_uid == RVN]
print(f"{'PASS' if not bad else 'FAIL'} catalog r{catalog.catalog_revision}: RVN bindings={len(bad)}"); ok &= not bad
ctx = SelectorContext.from_catalog(catalog, universe_dir=catalog_path.parent / "universes")
named = []
for p in os.environ["QDL_STABLE_CONSUMER_MANIFESTS"].split(":"):
    m = ConsumerManifestLoader.load(Path(p), selector_context=ctx)
    if any(r.instrument_uid == RVN for r in m.requirements):
        named.append(f"{m.consumer_id}-r{m.manifest_revision}")
print(f"{'PASS' if not named else 'FAIL'} manifests naming RVN: {named or 'none'}"); ok &= not named
row = {"instrument_uid": RVN, "feed": "BAR", "consumer_grade": "ALPHA", "source_policy_id": "crypto_primary_v2",
       "interval": "1d", "warmup_limit": 10, "max_freshness_ms": 259200000, "require_full_coverage": True,
       "require_final_bars": True, "stale_policy": "BLOCK", "gap_policy": "BLOCK", "recovery": "SNAPSHOT_AND_REPLAY",
       "bar_revision_policy": "EMIT_REVISIONS"}
rvn = DataRequirement.from_mapping(row)
other_binding = next(b for b in catalog.bindings if b.feed.value == "BAR" and b.interval == "1d")
other = DataRequirement.from_mapping({**row, "instrument_uid": other_binding.instrument.identity.instrument_uid,
                                      "source_policy_id": other_binding.source_policy_id})
class Backend:
    def __init__(self): self.catalog = catalog
    def latest(self, requirement):
        catalog.binding_for(requirement)
        raise QueryBackendError(QueryProblem(CanonicalErrorCode.DATA_NOT_READY, "probe", True))
service = V2QueryService(instruments=InstrumentQuery(InstrumentRegistry()), backend=Backend(), entitlements=EntitlementPolicy(()))
try:
    service.snapshot(rvn, purpose=AccessPurpose.INTERNAL_ALPHA); code = "NO_ERROR"
except QueryServiceError as error:
    code = error.problem.code.value
except Exception as error:  # noqa: BLE001
    code = f"UNTYPED:{type(error).__name__}"
print(f"{'PASS' if code == 'INSTRUMENT_NOT_FOUND' else 'FAIL'} RVN snapshot refusal: {code}"); ok &= code == "INSTRUMENT_NOT_FOUND"
result = asyncio.run(service.snapshot_batch_async((other, rvn), purpose=AccessPurpose.INTERNAL_ALPHA, consumer_id="probe"))
codes = [i.error.problem.code.value if i.error else "OK" for i in result.results]
good = codes == ["DATA_NOT_READY", "INSTRUMENT_NOT_FOUND"]
print(f"{'PASS' if good else 'FAIL'} mixed snapshot batch per-item: {codes}"); ok &= good
sys.exit(0 if ok else 1)
