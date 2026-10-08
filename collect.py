# 權證盤中紀錄：每個交易日由 GitHub Actions 啟動一次，跑到收盤後結束
#   1. 挑權證：熱門標的、價外 0~20%、剩 30~240 天，約 1,500 檔
#   2. 主紀錄：09:05~13:25 每 5 分鐘抓一次五檔（權證＋標的），13:31 再抓一次收盤
#   3. 快速樣本：約 50 檔（5 個標的 × 各發行商一檔）每 10 秒抓一次，看報價跟得快不快
#   4. 收盤後：抓流通在外張數（元大權證網，統計到前一交易日）
# 資料存在 data/日期/，由 workflow 收尾時 commit
import csv, gzip, json, os, re, ssl, sys, threading, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter

TZ = ZoneInfo("Asia/Taipei")
now = lambda: datetime.now(TZ)
TODAY = now().date()
OUT = os.path.join("data", "test" if "--test" in sys.argv else TODAY.isoformat())
TARGET_N, PER_UND, FAST_UND, MAIN_EVERY, FAST_EVERY = 1500, 60, 5, 300, 10
URLS = {
    "twse_w": "https://openapi.twse.com.tw/v1/opendata/t187ap37_L",
    "tpex_w": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap37_O",
    "twse_q": "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
    "tpex_q": "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
}
MIS_LOCK = threading.Lock()
FIELDS = ["c", "tlong", "z", "y", "b", "g", "a", "f"]   # 代號、交易所時間(ms)、成交、昨收、五檔買價/買量/賣價/賣量


def log(*a):
    print(f"[{now():%H:%M:%S}]", *a, flush=True)


def wait_until(hh, mm, ss=0):
    t = now().replace(hour=hh, minute=mm, second=ss, microsecond=0)
    while now() < t:
        time.sleep(min(30, (t - now()).total_seconds()))


def get_json(url, tries=4, **kw):
    for i in range(tries):
        try:
            return requests.get(url, timeout=300, headers={"User-Agent": "Mozilla/5.0"}, **kw).json()
        except Exception as e:
            log("重試", url, type(e).__name__)
            time.sleep(10 * (i + 1))
    raise RuntimeError(f"抓不到 {url}")


def mis(items, session):
    """items: [(市場, 代號)]，回傳 MIS 原始列（只留 FIELDS）"""
    q = "|".join(f"{mk}_{c}.tw" for mk, c in items)
    best = []
    for i in range(4):                           # 回傳少於九成就重抓，留最多的那次
        try:
            with MIS_LOCK:                       # 主紀錄和快速樣本輪流送，不同時打 MIS
                r = session.get("https://mis.twse.com.tw/stock/api/getStockInfo.jsp",
                                params={"ex_ch": q, "json": "1", "delay": "0"}, timeout=30).json()
            got = [{k: m.get(k, "") for k in FIELDS} for m in r.get("msgArray", []) if m.get("c")]
            if len(got) > len(best):
                best = got
            if len(best) >= len(items) * 0.9:
                break
        except Exception:
            pass
        time.sleep(3)
    return best


def new_session():
    s = requests.Session(); s.headers["User-Agent"] = "Mozilla/5.0"
    return s


def is_trading_day():
    """台積電報價的日期是今天＝有開盤"""
    d = None
    for i in range(5):
        try:
            d = new_session().get("https://mis.twse.com.tw/stock/api/getStockInfo.jsp",
                                  params={"ex_ch": "tse_2330.tw", "json": "1", "delay": "0"},
                                  timeout=30).json()["msgArray"][0].get("d")
            break
        except Exception:
            time.sleep(20)
    log("台積電報價日期", d, "今天", TODAY)
    if d is None:                                # 連不上證交所 → 當失敗處理，讓 GitHub 寄通知信
        raise SystemExit("連不上證交所即時報價（可能被擋），請檢查")
    return d == TODAY.strftime("%Y%m%d")


# ── 1. 挑權證 ──
def select():
    log("抓權證基本資料…")
    with ThreadPoolExecutor(4) as ex:
        got = dict(zip(URLS, ex.map(get_json, URLS.values())))
    clean = lambda n: re.sub(r"[（(]原名.*?[）)]", "", n).strip()
    num = lambda x: float(str(x).replace(",", "")) if re.match(r"^[\d,.]+$", str(x).strip() or "x") else None
    und = {}                                     # 名稱 -> (代號, 市場, 收盤)
    for x in got["twse_q"]:
        und.setdefault(clean(x["Name"]), (x["Code"].strip(), "tse", num(x.get("ClosingPrice"))))
    for x in got["tpex_q"]:
        und.setdefault(clean(x["CompanyName"]), (x["SecuritiesCompanyCode"].strip(), "otc", num(x.get("Close"))))
    roc = lambda s: datetime(int(s[:3]) + 1911, int(s[3:5]), int(s[5:])).date()
    pool = []
    for src, market in (("twse_w", "tse"), ("tpex_w", "otc")):
        for x in got[src]:
            if x["類別"] != "一般型":
                continue
            u = und.get(clean(x["標的證券/指數"]))
            K, ratio = num(x["最新履約價格(元)/履約指數"]), num(x["最新標的履約配發數量(每仟單位權證)"])
            if not u or not u[2] or not K or not ratio:
                continue
            try:
                last, exp = roc(x["最後交易日"].strip()), roc(x["履約截止日"].strip())
            except Exception:
                continue
            put = "售" in x["權證類型"]
            S = u[2]
            otm = (S - K) / S if put else (K - S) / S
            days = (last - TODAY).days
            if not (0 <= otm <= 0.20 and 30 <= days <= 240):
                continue
            name = x["權證簡稱"].strip()
            m = re.search(r"([^\x00-\x7f]{2})[0-9A-Z]{2}[購售][0-9A-Z]{2}$", name)
            pool.append(dict(code=x["權證代號"].strip(), market=market, name=name, issuer=m.group(1) if m else "?",
                             put=put, und=u[0], und_market=u[1], und_close=S, K=K, ratio=ratio / 1000,
                             last=last.isoformat(), exp=exp.isoformat(), otm=round(otm * 100, 2),
                             issue=num(x.get("發行單位數量(仟單位)", ""))))
    by_und = {}
    for w in pool:
        by_und.setdefault(w["und"], []).append(w)
    ranked = sorted(by_und.values(), key=len, reverse=True)
    chosen = []
    for ws in ranked:                            # 從權證最多的標的開始收，每個標的最多 60 檔、各發行商輪流挑
        if len(chosen) >= TARGET_N:
            break
        by_iss = {}
        for w in sorted(ws, key=lambda w: w["otm"]):
            by_iss.setdefault(w["issuer"], []).append(w)
        queues = list(by_iss.values()); pick = []
        while queues and len(pick) < PER_UND:
            for q in list(queues):
                if q and len(pick) < PER_UND:
                    pick.append(q.pop(len(q) // 2))   # 每家從價外程度中間往外挑，價外深淺都有
                if not q:
                    queues.remove(q)
        chosen += pick
    # 快速樣本：前 5 大標的，每家發行商挑一檔最接近「認購、價外 8%、剩 60~180 天」的
    fast = []
    for ws in ranked[:FAST_UND]:
        best = {}
        for w in (x for x in chosen if x["und"] == ws[0]["und"]):
            if w["put"] or not (60 <= (datetime.fromisoformat(w["last"]).date() - TODAY).days <= 180):
                continue
            if w["issuer"] not in best or abs(w["otm"] - 8) < abs(best[w["issuer"]]["otm"] - 8):
                best[w["issuer"]] = w
        fast += [w["code"] for w in best.values()]
    log(f"候選 {len(pool)} 檔、標的 {len(by_und)} 個；主紀錄 {len(chosen)} 檔（{len({w['und'] for w in chosen})} 個標的）、快速樣本 {len(fast)} 檔")
    return chosen, fast


# ── 2、3. 盤中紀錄 ──
class Recorder:
    def __init__(self, path, header):
        self.f = gzip.open(path, "at", encoding="utf-8", newline="")
        self.w = csv.writer(self.f)
        if self.f.tell() == 0:
            self.w.writerow(header)
        self.lock = threading.Lock()

    def rows(self, tag, rows):
        with self.lock:
            for r in rows:
                self.w.writerow([tag] + [r[k] for k in FIELDS])
            self.f.flush()

    def close(self):
        self.f.close()


def snapshot(items, workers=1):          # MIS 同時多個請求會漏資料，一律依序抓
    chunks = [items[i:i + 50] for i in range(0, len(items), 50)]
    local = threading.local()
    def one(ch):
        if not hasattr(local, "s"):
            local.s = new_session()
        return mis(ch, local.s)
    with ThreadPoolExecutor(workers) as ex:
        return [r for rs in ex.map(one, chunks) for r in rs]


def fast_loop(items, rec, stop):
    s = new_session()
    while not stop.is_set():
        t0 = time.time()
        rows = []
        for i in range(0, len(items), 50):
            rows += mis(items[i:i + 50], s)
        rec.rows(now().strftime("%H:%M:%S"), rows)
        stop.wait(max(0, FAST_EVERY - (time.time() - t0)))


# ── 4. 流通在外 ──
def outstanding(codes):
    class Relaxed(HTTPAdapter):
        def init_poolmanager(self, *a, **k):
            ctx = ssl.create_default_context(); ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
            k["ssl_context"] = ctx; return super().init_poolmanager(*a, **k)
    s = requests.Session(); s.mount("https://", Relaxed()); s.headers["User-Agent"] = "Mozilla/5.0"
    def one(c):
        try:
            r = s.get("https://www.warrantwin.com.tw/eyuanta/ws/GetWarHistory.ashx",
                      params={"type": "outstanding", "symbol": c}, timeout=15).json()
            res = r.get("result") or []
            return c, (res[-1].get("Date"), res[-1].get("Volume")) if res else None
        except Exception:
            return c, None
    with ThreadPoolExecutor(8) as ex:
        return dict(ex.map(one, codes))


def main():
    test = "--test" in sys.argv                  # 測試模式：不等時間、只抓幾輪
    if os.path.exists(os.path.join(OUT, "done.txt")) and not test:
        log("今天已經紀錄完了"); return
    if not test and now().hour >= 14:
        log("已經收盤，今天不紀錄"); return
    os.makedirs(OUT, exist_ok=True)
    sel_path = os.path.join(OUT, "selection.json")
    if os.path.exists(sel_path):                 # 同一天重跑（前一次中斷）就沿用
        sel = json.load(open(sel_path, encoding="utf-8"))
        chosen, fast = sel["main"], sel["fast"]
    else:
        chosen, fast = select()
        json.dump({"main": chosen, "fast": fast}, open(sel_path, "w", encoding="utf-8"), ensure_ascii=False)
    unds = sorted({(w["und_market"], w["und"]) for w in chosen})
    main_items = unds + [(w["market"], w["code"]) for w in chosen]
    fast_set = set(fast)
    fast_items = sorted({(w["und_market"], w["und"]) for w in chosen if w["code"] in fast_set}) + \
                 [(w["market"], w["code"]) for w in chosen if w["code"] in fast_set]

    if not test:
        wait_until(9, 2)
        if not is_trading_day():
            log("今天沒開盤，結束")
            for p in os.listdir(OUT):
                os.remove(os.path.join(OUT, p))
            os.rmdir(OUT); return
    main_rec = Recorder(os.path.join(OUT, "quotes.csv.gz"), ["snap"] + FIELDS)
    fast_rec = Recorder(os.path.join(OUT, "fast.csv.gz"), ["ts"] + FIELDS)
    stop = threading.Event()
    th = threading.Thread(target=fast_loop, args=(fast_items, fast_rec, stop), daemon=True)
    tried = good = 0                             # 健康檢查：每輪抓到八成以上才算正常

    if test:
        th.start()
        for k in range(2):
            t0 = time.time(); rows = snapshot(main_items)
            main_rec.rows(now().strftime("%H:%M"), rows)
            log(f"主紀錄第 {k+1} 輪：{len(rows)}/{len(main_items)} 筆，{time.time()-t0:.0f} 秒")
            time.sleep(20)
    else:
        wait_until(9, 5)
        th.start()
        t = now().replace(hour=9, minute=5, second=0, microsecond=0)
        end = now().replace(hour=13, minute=25, second=0, microsecond=0)
        while t <= end:
            if now() < t + timedelta(minutes=4):     # 錯過太久的那輪就跳過
                wait_until(t.hour, t.minute)
                t0 = time.time(); rows = snapshot(main_items)
                main_rec.rows(t.strftime("%H:%M"), rows)
                tried += 1; good += len(rows) >= len(main_items) * 0.8
                log(f"{t:%H:%M} 主紀錄 {len(rows)}/{len(main_items)} 筆，{time.time()-t0:.0f} 秒")
            t += timedelta(seconds=MAIN_EVERY)
        wait_until(13, 25, 30)
    stop.set(); th.join(30)
    if not test:
        wait_until(13, 31)
        rows = snapshot(main_items)
        main_rec.rows("13:31", rows)                  # 收盤後一筆，日後拿來跟盤中對照
        log(f"13:31 收盤紀錄 {len(rows)} 筆")
    main_rec.close(); fast_rec.close()
    log("抓流通在外…")
    ov = outstanding([w["code"] for w in chosen])
    json.dump(ov, open(os.path.join(OUT, "outstanding.json"), "w", encoding="utf-8"))
    log(f"流通在外 {sum(v is not None for v in ov.values())}/{len(ov)} 檔")
    open(os.path.join(OUT, "done.txt"), "w").write(now().isoformat())
    if not test and (tried == 0 or good < tried * 0.8):
        raise SystemExit(f"今天資料不完整：正常 {good}/{tried} 輪，請檢查")   # 已抓到的照樣存檔，但標成失敗寄信
    log(f"完成（正常 {good}/{tried} 輪）")


if __name__ == "__main__":
    main()
