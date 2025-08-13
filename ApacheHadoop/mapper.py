#!/usr/bin/env python3
"""
Emite claves tipadas para 5 consultas:
  MC    -> Concentración de mercado por década (Top-5 productoras)
  ROI   -> ROI vs decil de presupuesto por década
  DEBUT -> "Efecto debut" de productoras
  HG    -> "Hidden gems" por década
  STAB  -> Estabilidad de calificación según nº de géneros por década
"""
import sys, csv, ast

def fnum(x):
    try:
        if x is None: return None
        s = str(x).strip()
        if s == "" or s.lower() == "nan": return None
        return float(s)
    except:
        return None

def decade_from_date(s):
    if not s or len(s) < 4: 
        return None
    try:
        y = int(s[:4])
        return (y // 10) * 10
    except:
        return None

def parse_list_of_dicts(s):
    if not s or not s.strip(): 
        return []
    try:
        v = ast.literal_eval(s)
        return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []
    except:
        return []

def companies(row):
    out = []
    for d in parse_list_of_dicts(row.get("production_companies")):
        cid = d.get("id")
        name = d.get("name")
        if name:
            out.append((str(cid) if cid is not None else "", str(name)))
    return out

def genre_bucket(row):
    n = len(parse_list_of_dicts(row.get("genres")))
    if n <= 0: return None
    return "1" if n == 1 else ("2" if n == 2 else "3+")

def safe_txt(s):
    # además de tabs/newlines, elimina '|' para no romper el split del reducer
    return (s or "").replace("\t"," ").replace("\n"," ").replace("|"," ").strip()

reader = csv.DictReader(sys.stdin)

for r in reader:
    dec = decade_from_date(r.get("release_date"))
    if dec is None:
        continue

    bud = fnum(r.get("budget"))
    rev = fnum(r.get("revenue"))
    pop = fnum(r.get("popularity"))
    va  = fnum(r.get("vote_average"))
    mid = (r.get("id") or "").strip()
    title = safe_txt(r.get("original_title"))
    votes = fnum(r.get("vote_count"))
    vct = int(votes) if votes is not None else 0
    comps = companies(r)

    # 1) MC — revenue por (década, productora) prorrateado + TOTAL único
    if rev is not None and rev > 0:
        print(f"MC|TOTAL|{dec}\t{rev}")
        if comps:
            share = rev / len(comps)
            for cid, cname in comps:
                print(f"MC|{dec}|{cid}|{safe_txt(cname)}\t{share}")

    # 2) ROI — deciles y ROI promedio por decil
    if bud is not None and bud > 0:
        print(f"ROI|BUDGET|{dec}\t{bud}")
        if rev is not None:
            print(f"ROI|MOV|{dec}\t{bud}|{rev}")

    # 3) DEBUT — por productora (fecha, ROI, popularidad)
    if comps:
        roi = None if (bud is None or bud <= 0 or rev is None) else (rev - bud) / bud
        rs = "" if roi is None else str(roi)
        ps = "" if pop is None else str(pop)
        ds = (r.get("release_date") or "")[:10]
        if ds:
            for cid, cname in comps:
                print(f"DEBUT|{cid}|{safe_txt(cname)}\t{ds}|{rs}|{ps}")

    # 4) HG — valores para umbrales y candidatos
    if va is not None:
        print(f"HG|VA|{dec}\t{va}")
    if pop is not None:
        print(f"HG|POP|{dec}\t{pop}")
    if va is not None and pop is not None and mid:
        print(f"HG|MOV|{dec}\t{mid}|{title}|{va}|{pop}|{vct}")

    # 5) STAB — bucket géneros vs vote_average
    b = genre_bucket(r)
    if b is not None and va is not None:
        print(f"STAB|{dec}|{b}\t{va}|{vct}")
