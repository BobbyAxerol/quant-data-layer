"""Bar-edge checkpoint crossing from the pair the live bar_edge runs (out6, not recreated in stage B) to stage C r8."""
import sys, json
sys.path.insert(0, "/src")
from pathlib import Path
from scripts.dl_declaration import _bar_edge_set
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.runtime.bar_edge_checkpoint import plan_crossing
O6 = Path("/src/upgrade/evidence/releases/v2.3.0/declaration-out6/runtime")
C8 = Path("/s/outC8/runtime")
bc = StableSourceCatalog.load(O6 / "bar-edge-source-bindings.yaml")
ba = StableAcquisitionPlan.load(O6 / "bar-edge-acquisition-bindings.yaml", catalog=bc)
nc = StableSourceCatalog.load(C8 / "bar-edge-source-bindings.yaml")
na = StableAcquisitionPlan.load(C8 / "bar-edge-acquisition-bindings.yaml", catalog=nc)
before, after = _bar_edge_set(bc, ba), _bar_edge_set(nc, na)
crossing = plan_crossing(
    checkpoint={"catalog_revision": bc.catalog_revision, "acquisition_revision": ba.revision,
                "binding_ids": sorted(before), "binding_digests": before},
    runtime={"catalog_revision": nc.catalog_revision, "acquisition_revision": na.revision, "binding_ids": sorted(after)},
    current_digests=after, migration=None)
print(json.dumps({"from": [bc.catalog_revision, ba.revision], "to": [nc.catalog_revision, na.revision],
                  "summary": crossing.summary()}))
