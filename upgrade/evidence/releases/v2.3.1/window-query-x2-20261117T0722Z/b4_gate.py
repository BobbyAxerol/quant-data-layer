"""B4 smoke gate for the DL v2.3.1 Query window: PASS iff every snapshot_batch without a BAR item passed and every
batch with a BAR item was refused INVALID_ARGUMENT (router.py: a snapshot batch item carries no warmup). Prints 'pass total'."""
import csv, json, sys
d = sys.argv[1]
rows = [r for r in csv.DictReader(open(d + "/report.csv")) if r["operation"] == "snapshot_batch"]
res = [e for e in json.load(open(d + "/report.json"))[0]["results"] if e["operation"] == "snapshot_batch"]
ok = 0
for r, e in zip(rows, res):
    bar = any(p.split(":")[2] == "BAR" for p in r["products"].split(";"))
    codes = {s.get("error_code") for s in e["samples"]}
    good = (codes == {"INVALID_ARGUMENT"}) if bar else (e["status"] == "PASS")
    ok += good
    if not good:
        print("BAD", "BAR" if bar else "-", e["status"], sorted(map(str, codes)), file=sys.stderr)
print(ok, len(res))
