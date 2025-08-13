#!/usr/bin/env python3
import sys, os, math, bisect
from collections import defaultdict

# Umbrales (puedes ajustarlos por -cmdenv) así al final de cuando se ejecuta la query
"""
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar" \
  -input ./hadoop/input/movies_metadata_final.csv \
  -output ./hadoop/roi \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducerROI.py" \
  -cmdenv MIN_MOVIES_ROI=30 -cmdenv MIN_BUDGETS=30
"""
MIN_MOVIES_ROI = int(os.getenv("MIN_MOVIES_ROI", "20"))  
MIN_BUDGETS    = int(os.getenv("MIN_BUDGETS", "20"))     

budgets = defaultdict(list)     
movies  = defaultdict(list)     

def to_float(x):
    try: return float(x)
    except: return None

def decile_cuts(vals):
    """[p10,...,p90] por nearest-rank."""
    if not vals:
        return []
    v = sorted(vals)
    n = len(v)
    cuts = []
    for p in range(10, 100, 10):
        k = int(math.ceil(p/100.0 * n)) - 1
        k = min(max(k,0), n-1)
        cuts.append(v[k])
    return cuts

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    key, val = line.split("\t", 1)
    if not key.startswith("ROI|"):
        continue
    parts = key.split("|")
    if len(parts) < 3:
        continue
    tag = parts[1]
    dec = parts[2]

    if tag == "BUDGET":
        b = to_float(val)
        if b is not None and b > 0:
            budgets[dec].append(b)

    elif tag == "MOV":
        try:
            b_str, r_str = val.split("|", 1)
        except ValueError:
            continue
        b = to_float(b_str); r = to_float(r_str)
        if b is None or b <= 0 or r is None:
            continue
        movies[dec].append((b, r))

all_decs = sorted(set(list(budgets.keys()) + list(movies.keys())))
for dec in all_decs:
    n_mov = len(movies[dec])
    n_bud = len(budgets[dec])

    if n_mov < MIN_MOVIES_ROI or n_bud < MIN_BUDGETS:
        sys.stderr.write(f"[INFO] ROI: skip decade={dec} (n_movies={n_mov} < {MIN_MOVIES_ROI} "
                         f"or n_budgets={n_bud} < {MIN_BUDGETS})\n")
        continue

    cuts = decile_cuts(budgets[dec])  
    if not cuts:
        sys.stderr.write(f"[INFO] ROI: no cuts decade={dec}\n")
        continue

    agg = defaultdict(lambda: [0.0, 0])  
    for b, r in movies[dec]:
        roi = (r - b) / b
        d = bisect.bisect_right(cuts, b) + 1  # 1..10
        agg[d][0] += roi
        agg[d][1] += 1

    for d in range(1, 11):
        s, n = agg[d]
        avg = (s/n) if n > 0 else 0.0
        print(f"{dec}|{d}\t{avg:.6f}|{n}")
