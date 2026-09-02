#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""bar_cache.py — 진단(d)(e)용 15분봉 디스크 캐시.

audit_v1.py 는 봉을 캐시하지 않는다(동결된 감사 경로). 결측봉 민감도(d)와
이벤트 시점 σ 실측(e)은 같은 봉을 다시 읽어야 하므로 여기서 한 번 받아 캐시한다.
캐시는 '원본 봉'만 저장한다 — 보정봉은 저장하지 않으므로 (d)에서 보정 전/후를
모두 재구성할 수 있다.
"""
import concurrent.futures as cf, csv, gzip, os, sys, threading, time
import requests
import audit_v1 as A

CACHE   = "bar_cache"
WORKERS = 4
RPS     = 7.0                      # 업비트 공지 10 req/s 대비 보수적

class Pacer:
    def __init__(self, rps):
        self.gap = 1.0/rps; self.lock = threading.Lock(); self.next = 0.0
    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next); self.next = t + self.gap
        if t > now: time.sleep(t-now)

PACER = Pacer(RPS)

def get(path, params):
    for i in range(6):
        PACER.wait()
        try:
            r = requests.get(A.UPBIT_BASE+path, params=params, timeout=20)
            if r.status_code == 200: return r.json()
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 5))); continue
            if r.status_code in (403, 451): A._geoblock(r.status_code, A.UPBIT_BASE)
            time.sleep(2)
        except SystemExit: raise
        except Exception:
            time.sleep(3)
    raise RuntimeError(f"업비트 요청 반복 실패: {path} {params}")

def upbit_bars(market, days):
    """audit_v1.upbit_bars 와 동일 로직 — 페이서만 공유 버킷으로 교체."""
    cutoff = A.utcnow_ms() - days*86400*1000
    to, bars, seen = None, [], set()
    while True:
        p = {"market": market, "count": 200}
        if to: p["to"] = to
        c = get("/v1/candles/minutes/15", p)
        if not c: break
        oldest = None
        for x in c:
            ts = int(A.datetime.strptime(x["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
                     .replace(tzinfo=A.timezone.utc).timestamp()*1000)
            oldest = ts if oldest is None else min(oldest, ts)
            if ts in seen or ts + A.BAR_MS > A.utcnow_ms(): continue
            seen.add(ts); bars.append((ts, float(x["trade_price"]),
                                       float(x["candle_acc_trade_price"])))
        if oldest is None or oldest <= cutoff: break
        to = A.datetime.fromtimestamp(oldest/1000, A.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bars = sorted(b for b in bars if b[0] >= cutoff)
    return bars

def path_for(venue, sym, days): return f"{CACHE}/{venue}/{sym}_{days}d.csv.gz"

def load(venue, sym, days):
    p = path_for(venue, sym, days)
    if not os.path.exists(p): return None
    with gzip.open(p, "rt") as f:
        return [(int(a), float(b), float(c)) for a, b, c in csv.reader(f)]

def save(venue, sym, days, bars):
    p = path_for(venue, sym, days); os.makedirs(os.path.dirname(p), exist_ok=True)
    with gzip.open(p, "wt") as f:
        w = csv.writer(f)
        for b in bars: w.writerow(b)

def ensure(venue, syms, days):
    todo = [s for s in syms if load(venue, s, days) is None]
    A.log(f"{venue}: 캐시 필요 {len(todo)}/{len(syms)}종목 ({days}일)")
    if not todo: return
    fetch = upbit_bars if venue == "upbit" else (lambda s, d: A.vision_bars(s, d))
    done = [0]; lock = threading.Lock()
    def one(s):
        try:
            b = fetch(s, days); save(venue, s, days, b); n = len(b)
        except SystemExit: raise
        except Exception as e:
            n = -1; A.log(f"{s} 캐시 실패({e})")
        with lock:
            done[0] += 1
            if done[0] % 10 == 0 or n < 0:
                A.log(f"  {venue} 캐시 {done[0]}/{len(todo)} (최근 {s}: {n}봉)")
    with cf.ThreadPoolExecutor(WORKERS if venue == "upbit" else A.VISION_WORKERS) as ex:
        list(ex.map(one, todo))
    A.log(f"{venue} 캐시 완료")

if __name__ == "__main__":
    import json
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 180
    up = json.load(open("universe_upbit.json"))
    if "KRW-BTC" not in up: up = ["KRW-BTC"] + up          # (c) BTC 기준봉
    ensure("upbit", up, days)
    vi = json.load(open("universe_vision.json"))
    if "BTCUSDT" not in vi: vi = ["BTCUSDT"] + vi
    ensure("vision", vi, days)
