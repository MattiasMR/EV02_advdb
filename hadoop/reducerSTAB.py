#!/usr/bin/env python3
import sys, math
from collections import defaultdict

n_titles = defaultdict(int)     
sum_w    = defaultdict(float)   
sum_wx   = defaultdict(float)   
sum_wx2  = defaultdict(float)   

def to_float(s):
    try: return float(s)
    except: return None

def to_int(s):
    try: return int(float(s))
    except: return 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    key, val = line.split("\t", 1)
    if not key.startswith("STAB|"):
        continue

    try:
        _, dec, bucket = key.split("|", 2)
    except ValueError:
        continue
    k = f"{dec}|{bucket}"

    parts = val.split("|")
    if len(parts) < 2:
        continue
    va = to_float(parts[0])
    vc = to_int(parts[1])
    if va is None or vc <= 0:
        continue

    n_titles[k] += 1
    sum_w[k]    += vc
    sum_wx[k]   += vc * va
    sum_wx2[k]  += vc * va * va

# Emitir resultados (poblacional)
for k in sorted(n_titles.keys()):
    w  = sum_w[k]
    wx = sum_wx[k]
    wx2= sum_wx2[k]
    n  = n_titles[k]

    if w <= 0:
        mean = 0.0
        std  = 0.0
    else:
        mean = wx / w
        num = wx2 - (wx * wx) / w
        var = max(0.0, num / w)
        std = math.sqrt(var)

    print(f"{k}\t{mean:.6f}|{std:.6f}|{n}")