#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
from collections import defaultdict
from datetime import datetime

def to_float(x):
    try: return float(x)
    except: return None

def safe_name(s):
    return (s or "").replace("\t"," ").replace("\n"," ").replace("|"," ").strip()

# (cid,cname) -> [(date, roi|None, pop|None)]
data = defaultdict(list)

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue
    key, val = line.split("\t", 1)
    if not key.startswith("DEBUT|"):
        continue

    try:
        _, cid, cname = key.split("|", 2)
    except ValueError:
        continue

    try:
        ds, rs, ps = val.split("|", 2)
    except ValueError:
        continue

    # fecha debut / fechas resto (usamos date para ordenar)
    try:
        d = datetime.strptime(ds, "%Y-%m-%d").date()
    except Exception:
        # si no hay fecha válida, ignoramos el registro
        continue

    r = None if rs == "" else to_float(rs)
    p = None if ps == "" else to_float(ps)
    data[(cid, cname)].append((d, r, p))


for (cid, cname), items in sorted(data.items(), key=lambda kv: (kv[0][0], kv[0][1])):
    if not items:
        continue

    # ordenar por fecha ascendente
    items.sort(key=lambda t: t[0])
    debut = items[0]
    rest  = items[1:]

    debut_date = debut[0].isoformat()
    debut_roi  = debut[1]   # None si mapper no pudo calcular
    debut_pop  = debut[2]   # puede ser None

    # *** EXCLUSIÓN: si el debut no tiene ROI válido, saltamos esta productora
    if debut_roi is None:
        continue

    # promedios del resto
    roi_vals = [x[1] for x in rest if x[1] is not None]
    pop_vals = [x[2] for x in rest if x[2] is not None]
    post_avg_roi = (sum(roi_vals)/len(roi_vals)) if roi_vals else 0.0
    post_avg_pop = (sum(pop_vals)/len(pop_vals)) if pop_vals else 0.0
    n_post = len(rest)

    # ΔROI y ΔPop (debut - promedio resto)
    delta_roi = debut_roi - post_avg_roi
    delta_pop = (debut_pop - post_avg_pop) if (debut_pop is not None) else None

    # salida:
    # <cid>|<cname> \t <debut_date>|<debut_roi>|<debut_pop>|<post_avg_roi>|<post_avg_pop>|<n_post>|<delta_roi>|<delta_pop>
    cname_safe = safe_name(cname)
    debut_roi_s = f"{debut_roi:.6f}"
    debut_pop_s = "" if debut_pop is None else f"{debut_pop:.6f}"
    post_roi_s  = f"{post_avg_roi:.6f}"
    post_pop_s  = f"{post_avg_pop:.6f}"
    delta_roi_s = f"{delta_roi:.6f}"
    delta_pop_s = "" if delta_pop is None else f"{delta_pop:.6f}"

    print(f"{cid}|{cname_safe}\t{debut_date}|{debut_roi_s}|{debut_pop_s}|{post_roi_s}|{post_pop_s}|{n_post}|{delta_roi_s}|{delta_pop_s}")
#   idProductora|nombreProductora|fechaEstreno|debutROI|debutPopularidad|noDebutPromedioROI|noDebutPromedioPopularidad|nPeliculasNoDebut|deltaROI|deltaPopularidad
