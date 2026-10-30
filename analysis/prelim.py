# 權證盤中紀錄初步分析：各發行商價差、買進成本(委賣IV)、委買張數、報價崩壞、開盤、報價跟隨
import csv, gzip, json, os, sys, collections, math
from datetime import date
import numpy as np

ROOT = sys.argv[1]
DAYS = sorted(d for d in os.listdir(os.path.join(ROOT, "data")) if os.path.exists(os.path.join(ROOT, "data", d, "done.txt")))
FULL = [d for d in DAYS if d not in ("2026-10-12", "2026-10-28", "2026-10-29", "2026-10-30")]


def lv(s):
    x = [v for v in s.split("_") if v not in ("", "-")]
    return float(x[0]) if x else None


def ncdf(x):
    return 0.5 * (1 + np.vectorize(math.erf)(x / math.sqrt(2)))


def bs(S, K, v, t, put):
    d1 = (np.log(S / K) + 0.5 * v * v * t) / (v * np.sqrt(t)); d2 = d1 - v * np.sqrt(t)
    c = S * ncdf(d1) - K * ncdf(d2)
    return np.where(put, c - S + K, c)


def iv(p, S, K, t, put):
    lo, hi = np.full_like(p, 0.01), np.full_like(p, 3.0)
    for _ in range(40):
        mid = (lo + hi) / 2
        hi_ = bs(S, K, mid, t, put) > p
        hi = np.where(hi_, mid, hi); lo = np.where(hi_, lo, mid)
    v = (lo + hi) / 2
    return np.where((v > 0.02) & (v < 2.95), v, np.nan)


spread = collections.defaultdict(list)      # issuer -> 相對價差(扣同組中位數)
spread_raw = collections.defaultdict(list)
bidv = collections.defaultdict(list)
broken = collections.defaultdict(lambda: [0, 0])
open_ratio = collections.defaultdict(list)
ivrows = []                                  # (issuer, group, askIV)
IV_SNAPS = {"09:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "13:00"}

for d in FULL:
    p = os.path.join(ROOT, "data", d)
    sel = json.load(open(os.path.join(p, "selection.json"), encoding="utf-8"))
    W = {w["code"]: w for w in sel["main"]}
    snaps = collections.defaultdict(dict)
    for r in list(csv.reader(gzip.open(os.path.join(p, "quotes.csv.gz"), "rt", encoding="utf-8")))[1:]:
        snaps[r[0]][r[1]] = (lv(r[5]), lv(r[6]), lv(r[7]), lv(r[8]))
    today = date.fromisoformat(d)
    for snap, q in snaps.items():
        if snap == "13:31":
            continue
        grp_sp = collections.defaultdict(list); items = []
        for c, (b, bv, a, av) in q.items():
            w = W.get(c)
            if not w:
                continue
            u = q.get(w["und"])
            if not u or not u[0] or not u[2]:
                continue
            S = (u[0] + u[2]) / 2
            otm = ((S - w["K"]) if w["put"] else (w["K"] - S)) / S * 100
            days = (date.fromisoformat(w["exp"]) - today).days
            g = (w["und"], w["put"], int(max(otm, 0) // 5), 0 if days < 90 else 1)
            ok = b and a and a > 0 and b > 0
            if snap >= "09:10":
                broken[w["issuer"]][1] += 1
                if not ok or (a - b) / a > 0.10:
                    broken[w["issuer"]][0] += 1
            if not ok:
                continue
            sp = (a - b) / a * 100
            if snap == "09:05":
                items.append(("open", w["issuer"], c, sp)); continue
            items.append(("sp", w["issuer"], g, sp)); grp_sp[g].append((w["issuer"], sp))
            if snap >= "09:10":
                bidv[w["issuer"]].append(bv or 0)
            if snap in IV_SNAPS and days > 10:
                ivrows.append((w["issuer"], (d, snap) + g, a / w["ratio"], S, w["K"], days / 365, w["put"]))
        for kind, iss, g, sp in items:
            if kind == "sp" and snap >= "09:10":
                spread_raw[iss].append(sp)
                others = [x for i2, x in grp_sp[g] if i2 != iss]
                if len(others) >= 2:
                    spread[iss].append(sp - np.median(others))
    # 開盤 09:05 價差 ÷ 當天 09:10 以後該檔中位數
    per = collections.defaultdict(list)
    for snap, q in snaps.items():
        if "09:10" <= snap <= "13:25":
            for c, (b, bv, a, av) in q.items():
                if c in W and b and a:
                    per[c].append((a - b) / a)
    q0 = snaps.get("09:05", {})
    for c, (b, bv, a, av) in q0.items():
        if c in W and per.get(c):
            base = np.median(per[c])
            if base > 0:
                open_ratio[W[c]["issuer"]].append(((a - b) / a if b and a else 0.5) / base)

# 委賣 IV 相對同組
arr = list(zip(*ivrows))
ivs = iv(np.array(arr[2]), np.array(arr[3]), np.array(arr[4]), np.array(arr[5]), np.array(arr[6]))
grp = collections.defaultdict(list)
for i, g in enumerate(arr[1]):
    if not np.isnan(ivs[i]):
        grp[g].append((arr[0][i], ivs[i]))
ivrel = collections.defaultdict(list)
for i, g in enumerate(arr[1]):
    if not np.isnan(ivs[i]):
        others = [x for i2, x in grp[g] if i2 != arr[0][i]]
        if len(others) >= 2:
            ivrel[arr[0][i]].append((ivs[i] - np.median(others)) * 100)

# 報價跟隨：快速樣本，標的中價變動時，同一輪(約10秒)內權證委買有沒有往同方向動
follow = collections.defaultdict(lambda: [0, 0])
for d in FULL:
    p = os.path.join(ROOT, "data", d)
    sel = json.load(open(os.path.join(p, "selection.json"), encoding="utf-8"))
    W = {w["code"]: w for w in sel["main"]}
    rows = collections.defaultdict(dict)
    for r in list(csv.reader(gzip.open(os.path.join(p, "fast.csv.gz"), "rt", encoding="utf-8")))[1:]:
        if r[0] >= "09:10":
            rows[r[0]][r[1]] = (lv(r[5]), lv(r[7]))
    ts = sorted(rows)
    for t0, t1 in zip(ts, ts[1:]):
        for c in sel["fast"]:
            w = W.get(c)
            if not w:
                continue
            u0, u1 = rows[t0].get(w["und"]), rows[t1].get(w["und"])
            x0, x1 = rows[t0].get(c), rows[t1].get(c)
            if not (u0 and u1 and x0 and x1 and all(u0) and all(u1) and x0[0] and x1[0]):
                continue
            du = (u1[0] + u1[1]) / 2 - (u0[0] + u0[1]) / 2
            if du == 0:
                continue
            want = du if not w["put"] else -du
            follow[w["issuer"]][1] += 1
            if (x1[0] - x0[0]) * want > 0:
                follow[w["issuer"]][0] += 1

iss_all = sorted(set(spread) | set(ivrel), key=lambda k: -len(spread.get(k, [])))
print("完整天數", FULL)
print(f"{'發行商':<4}{'樣本':>8}{'價差中位%':>9}{'相對同組':>8}{'委賣IV相對':>10}{'IV樣本':>7}{'委買≥100張%':>8}{'崩壞%':>7}{'開盤價差倍數':>10}{'跟隨率%':>8}{'跟隨樣本':>8}")
for k in iss_all:
    sp = spread.get(k, []); ivr = ivrel.get(k, []); bv = bidv.get(k, []); br = broken[k]; op = open_ratio.get(k, []); fo = follow.get(k, [0, 0])
    print(f"{k:<4}{len(sp):>8}{np.median(spread_raw[k]) if sp else float('nan'):>9.2f}{np.median(sp) if sp else float('nan'):>8.2f}"
          f"{np.median(ivr) if ivr else float('nan'):>10.2f}{len(ivr):>7}{np.mean(np.array(bv) >= 100) * 100 if bv else float('nan'):>8.0f}"
          f"{br[0] / br[1] * 100 if br[1] else float('nan'):>7.1f}{np.median(op) if op else float('nan'):>10.2f}"
          f"{fo[0] / fo[1] * 100 if fo[1] else float('nan'):>8.1f}{fo[1]:>8}")
