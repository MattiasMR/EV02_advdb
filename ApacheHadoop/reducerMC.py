#!/usr/bin/env python3
import sys
from collections import defaultdict

def to_float(s):
    try:
        return float(s)
    except:
        return None

# Acumuladores
total_by_decade = defaultdict(float)         # decade -> total unique revenue
rev_by_company  = defaultdict(float)         # (decade, cid, cname) -> revenue

decades = set()

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    key, val = line.split("\t", 1)

    if key.startswith("MC|TOTAL|"):
        # MC|TOTAL|<decade> \t <revenue>
        parts = key.split("|", 3)
        if len(parts) >= 3:
            dec = parts[2]
            r = to_float(val)
            if r is not None and r > 0:
                total_by_decade[dec] += r
                decades.add(dec)
        continue

    if key.startswith("MC|"):
        parts = key.split("|", 4)
        if len(parts) < 4:
            continue
        _, dec, cid, cname = parts[:4]
        r = to_float(val)
        if r is None:
            continue
        rev_by_company[(dec, cid, cname)] += r
        decades.add(dec)
        continue

# Emitir por década
for dec in sorted(decades):
    total = total_by_decade.get(dec, 0.0)
    rows = [(cid, cname, rev) for (d, cid, cname), rev in rev_by_company.items() if d == dec]
    rows.sort(key=lambda x: (-x[2], x[1], x[0]))  # por revenue desc, luego nombre

    if total <= 0.0 or not rows:
        print(f"{dec}\tTOP5|0.0000|TOTAL|0.00")
        continue

    top5 = rows[:5]
    top5_sum = sum(rv for _, _, rv in top5)
    top5_share_pct = (top5_sum / total) * 100.0
    print(f"{dec}\tTOP5|{top5_share_pct:.4f}|TOTAL|{total:.2f}")

    for i, (cid, cname, rv) in enumerate(top5, start=1):
        share_pct = (rv / total) * 100.0
        cname_safe = cname.replace("\t"," ").replace("\n"," ").replace("|"," ").strip()
        print(f"{dec}|TOP{i}\t{cid}|{cname_safe}|{rv:.2f}|{share_pct:.4f}")
