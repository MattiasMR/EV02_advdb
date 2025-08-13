import sys, os, math
from collections import defaultdict

# Umbrales (puedes ajustarlos por -cmdenv) así al final de cuando se ejecuta la query
"""
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar" \
  -input ./hadoop/input/movies_metadata_final.csv \
  -output ./hadoop/hg \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducerHG.py" \
  -cmdenv MIN_VOTES=10
""" 
MIN_VOTES = int(os.getenv("MIN_VOTES", "100"))

va = defaultdict(list)     # decade -> [vote_average,...]
pop = defaultdict(list)    # decade -> [popularity,...]
mov = defaultdict(list)    # decade -> [(id, title, va, pop, votes), ...]

def to_float(x):
    try: return float(x)
    except: return None

def to_int(x):
    try: return int(float(x))
    except: return 0

def percentile(vals, p):
    """Nearest-rank p in [0,100]."""
    if not vals:
        return None
    v = sorted(vals)
    n = len(v)
    if p <= 0:   return v[0]
    if p >= 100: return v[-1]
    k = int(math.ceil(p/100.0 * n)) - 1
    k = min(max(k,0), n-1)
    return v[k]

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    key, val = line.split("\t", 1)
    if not key.startswith("HG|"):
        continue

    parts = key.split("|")
    if len(parts) < 3:
        continue
    tag = parts[1]
    dec = parts[2]

    if tag == "VA":
        x = to_float(val)
        if x is not None:
            va[dec].append(x)

    elif tag == "POP":
        x = to_float(val)
        if x is not None:
            pop[dec].append(x)

    elif tag == "MOV":
        # Formatos soportados:
        #   <id>|<title>|<va>|<pop>                (sin votos)  -> votes=0
        #   <id>|<title>|<va>|<pop>|<vote_count>  (con votos)
        parts = val.split("|")
        if len(parts) < 4:
            continue
        mid   = parts[0]
        title = parts[1]
        va_s  = parts[2]
        pop_s = parts[3]
        vct   = to_int(parts[4]) if len(parts) >= 5 else 0

        v = to_float(va_s)
        p = to_float(pop_s)
        if v is None or p is None:
            continue
        mov[dec].append((mid, title, v, p, vct))

# Calcular umbrales y filtrar
for dec in sorted(set(list(va.keys()) + list(pop.keys()) + list(mov.keys()))):
    thr_va = percentile(va[dec], 90)
    thr_p  = percentile(pop[dec], 10)
    if thr_va is None or thr_p is None:
        continue

    for (mid, title, v, p, votes) in mov[dec]:
        if votes >= MIN_VOTES and v >= thr_va and p <= thr_p:
            # Mantener el formato de salida original (sin votos)
            print(f"{dec}\t{mid}|{title}|{v:.3f}|{p:.3f}|{votes}")