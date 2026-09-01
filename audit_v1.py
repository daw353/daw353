#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_v1.py — H1/H2 사전등록 부속 감사 (수익률 미조회 = 확증 예산 미소모)

무엇을 측정하는가:
  --mode freq : 트리거 A(청산 캐스케이드 프록시), B(거래량 폭발+양의 수익률)의
                발화 빈도, 일별 분포, 무신호 최장 구간, 월별 F5 체크, 동시발화(군집) 구조
  --mode cost : 유니버스 전 종목 실시간 스프레드 샘플링 → 왕복비용 하한 추정
  --source synthetic : 파이프라인 자가점검(합성 데이터). 증거가 아님. 판정에 사용 금지.

데이터 현실 고지(사전등록 수정 사항, 측정 전 확정):
  과거 청산(강제 체결) 내역은 무료 REST로 제공되지 않음(바이낸스 allForceOrders 폐지).
  따라서 백테스트/감사 구간의 트리거 A는 프록시로 정의한다:
      A_proxy := [15분 수익률 < -1.5σ(직전 7일, 15분 수익률)] AND [15분 quote 거래대금 > 직전 30일 P99]
  라이브 단계에서 forceOrder 웹소켓 로깅으로 프록시-실제 청산 대응률을 별도 검증한다.

원칙:
  - 이 스크립트는 트리거 이후의 미래 수익률을 절대 계산하지 않는다.
  - 파라미터는 아래 CONFIG에 동결. 실행 중 재량 조정 금지.

실행 예 (Google Colab 셀에서):
  !python audit_v1.py --mode freq --days 180
  !python audit_v1.py --mode cost --rounds 20 --interval 60
"""

import argparse, calendar, json, math, os, random, statistics, sys, time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

try:
    import requests
except ImportError:
    requests = None

# ========================= CONFIG (동결 — 사전등록 §3) =========================
BASE_FAPI      = "https://fapi.binance.com"
RANK_MIN, RANK_MAX = 20, 150          # 30일 중위 일거래대금 순위 (양끝 포함, 1-indexed)
BAR            = "15m"                # 기본 봉 (트리거 정의 단위)
BARS_PER_DAY   = 96
SIGMA_DAYS     = 7                    # σ 추정 윈도우 (7일 = 672봉)
PCTL_DAYS      = 30                   # 거래대금 분포 윈도우 (30일 = 2880봉)
RET_K          = 1.5                  # A: 수익률 < -1.5σ
VOL_PCTL       = 99.0                 # A: 거래대금 > P99
VOLZ_B         = 3.0                  # B: 거래대금 z > 3, ret > 0
COOLDOWN_MIN   = 240                  # 종목당 트리거별 쿨다운 4시간
F5_MIN_MONTHLY = 10                   # F5: 트리거 A 월 10건 미만 → 폐기
STABLE_SKIP    = {"USDCUSDT","FDUSDUSDT","TUSDUSDT","BUSDUSDT","EURUSDT","USDPUSDT","DAIUSDT"}
OUT_DIR        = "audit_out"
# ==============================================================================

def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

def http_get(path, params=None, retries=5):
    url = BASE_FAPI + path
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (418, 429):          # rate limit
                wait = int(r.headers.get("Retry-After", 30))
                log(f"rate limit {r.status_code}, {wait}s 대기"); time.sleep(wait); continue
            if r.status_code in (403, 451):          # 지역 차단 — 재시도 무의미
                raise SystemExit(
                    f"HTTP {r.status_code}: 현재 IP에서 {BASE_FAPI} 접근 차단(지역 제한). "
                    "차단되지 않은 위치/프록시에서 실행하거나 BASE_FAPI를 해당 리전 엔드포인트로 교체할 것. "
                    "(주의: 구글 Colab의 미국 리전 IP도 흔히 차단됨)")
            log(f"HTTP {r.status_code} {path} {params}"); time.sleep(2)
        except Exception as e:
            log(f"요청 실패({e}), 재시도 {i+1}/{retries}"); time.sleep(3)
    raise RuntimeError(f"요청 반복 실패: {path}")

# ---------------------------- 유니버스 ----------------------------------------
def build_universe(cache="universe.json"):
    if os.path.exists(cache):
        u = json.load(open(cache))
        log(f"유니버스 캐시 사용: {len(u)}종목"); return u
    info = http_get("/fapi/v1/exchangeInfo")
    syms = [s["symbol"] for s in info["symbols"]
            if s.get("contractType")=="PERPETUAL" and s.get("quoteAsset")=="USDT"
            and s.get("status")=="TRADING" and s["symbol"] not in STABLE_SKIP]
    log(f"후보 {len(syms)}종목 → 30일 일봉으로 중위 거래대금 랭킹 산출 중")
    med = {}
    for i, s in enumerate(syms):
        try:
            kl = http_get("/fapi/v1/klines", {"symbol": s, "interval": "1d", "limit": 31})
            qv = [float(k[7]) for k in kl[:-1]]      # 마지막(미완성 일봉) 제외, quote volume
            if len(qv) >= 20: med[s] = statistics.median(qv)
        except Exception as e:
            log(f"{s} 랭킹 스킵({e})")
        if i % 50 == 0: log(f"  랭킹 진행 {i}/{len(syms)}")
        time.sleep(0.12)
    ranked = sorted(med, key=med.get, reverse=True)
    universe = ranked[RANK_MIN-1:RANK_MAX]           # 순위 20~150
    json.dump(universe, open(cache, "w"))
    log(f"유니버스 확정: {len(universe)}종목 (순위 {RANK_MIN}~{RANK_MAX}) → {cache} 저장")
    return universe

# ---------------------------- 15m 캔들 수집 ------------------------------------
def fetch_15m(symbol, days):
    end = int(time.time()*1000)
    start = end - days*86400*1000
    bars = []
    while start < end:
        kl = http_get("/fapi/v1/klines",
                      {"symbol": symbol, "interval": BAR, "startTime": start, "limit": 1500})
        if not kl: break
        # 미완성(진행 중) 봉 제외: closeTime(k[6])이 현재 시각을 넘는 봉은 버린다.
        now_ms = int(time.time()*1000)
        bars += [(int(k[0]), float(k[4]), float(k[7])) for k in kl if int(k[6]) < now_ms]
        nxt = kl[-1][0] + 1
        if nxt <= start: break
        start = nxt
        time.sleep(0.12)
    return bars

def detect_window(bars):
    """트리거 판정이 실제로 가능한 구간 (워밍업 이후 첫 봉 ~ 마지막 봉). 없으면 None."""
    warm = PCTL_DAYS * BARS_PER_DAY
    if len(bars) <= warm + 1: return None
    return (bars[warm][0], bars[-1][0])

# ---------------------------- 트리거 판정 --------------------------------------
def detect_events(symbol, bars):
    """bars: [(ts, close, quoteVol)] 시간순. 미래 정보 미사용(모든 통계는 직전 구간)."""
    n = len(bars)
    warm = PCTL_DAYS * BARS_PER_DAY
    if n <= warm + 1: return []
    ts   = [b[0] for b in bars]
    ret  = [0.0]+[(bars[i][1]/bars[i-1][1]-1) if bars[i-1][1]>0 else 0.0 for i in range(1,n)]
    vol  = [b[2] for b in bars]

    events, last_fire = [], {"A": -10**18, "B": -10**18}
    sig_win = SIGMA_DAYS * BARS_PER_DAY
    day_idx, p99, vmean, vstd = -1, None, None, None
    for i in range(warm, n):
        d = ts[i] // 86400000
        if d != day_idx:                              # 임계값은 일 1회 갱신(직전 30일 분포)
            day_idx = d
            w = vol[i-warm:i]
            sw = sorted(w); p99 = sw[min(len(sw)-1, int(len(sw)*VOL_PCTL/100))]
            vmean = sum(w)/len(w)
            vstd  = (sum((x-vmean)**2 for x in w)/len(w))**0.5 or 1e-12
        rw = ret[i-sig_win:i]
        mu = sum(rw)/len(rw)
        sd = (sum((x-mu)**2 for x in rw)/len(rw))**0.5 or 1e-12
        volz = (vol[i]-vmean)/vstd
        cd = COOLDOWN_MIN*60000
        if ret[i] < -RET_K*sd and vol[i] > p99 and ts[i]-last_fire["A"] >= cd:
            events.append((ts[i], symbol, "A", round(ret[i],5), round(volz,2))); last_fire["A"]=ts[i]
        if volz > VOLZ_B and ret[i] > 0 and ts[i]-last_fire["B"] >= cd:
            events.append((ts[i], symbol, "B", round(ret[i],5), round(volz,2))); last_fire["B"]=ts[i]
    return events

# ---------------------------- 집계 --------------------------------------------
def summarize(events, days_covered, n_symbols, label, window=None):
    """window: (first_ts, last_ts) — 판정 가능 구간. 주어지면 무신호 스트릭/부분월 판별에 사용."""
    os.makedirs(OUT_DIR, exist_ok=True)
    events.sort()
    with open(f"{OUT_DIR}/events_{label}.csv","w") as f:
        f.write("utc,symbol,trigger,ret15,volz\n")
        for ts,s,t,r,z in events:
            f.write(f"{datetime.fromtimestamp(ts/1000,timezone.utc).isoformat()},{s},{t},{r},{z}\n")

    w0_day = w1_day = None
    if window:
        w0_day = datetime.fromtimestamp(window[0]/1000, timezone.utc).date()
        w1_day = datetime.fromtimestamp(window[1]/1000, timezone.utc).date()
        days_covered = (w1_day - w0_day).days + 1     # 실제 관측일로 대체

    daily, monthly, simult = defaultdict(lambda: defaultdict(int)), defaultdict(int), defaultdict(int)
    for ts,s,t,r,z in events:
        day = datetime.fromtimestamp(ts/1000,timezone.utc).strftime("%Y-%m-%d")
        daily[day][t]+=1
        if t=="A": monthly[day[:7]]+=1
        simult[(ts,t)]+=1

    all_days = sorted(daily)
    # 무신호 최장 스트릭(달력일 기준). window가 있으면 관측 구간 전체, 없으면 첫~마지막 발화일.
    def streaks(trig):
        if w0_day and w1_day:
            cur, end = w0_day, w1_day
        elif all_days:
            cur = datetime.strptime(all_days[0], "%Y-%m-%d").date()
            end = datetime.strptime(all_days[-1], "%Y-%m-%d").date()
        else:
            return days_covered
        run = best = 0
        while cur <= end:
            run = 0 if daily.get(cur.isoformat(), {}).get(trig, 0) > 0 else run + 1
            best = max(best, run)
            cur += timedelta(days=1)
        return best

    # 부분월(관측 구간에 절단된 달)은 F5 판정에서 분리한다 — 절단 때문에 건수가 낮게 나옴.
    def month_complete(m):
        if not (w0_day and w1_day): return None
        y, mo = (int(x) for x in m.split("-"))
        return date(y, mo, 1) >= w0_day and date(y, mo, calendar.monthrange(y, mo)[1]) <= w1_day
    complete_months  = [m for m in sorted(monthly) if month_complete(m) is not False]
    partial_months   = [m for m in sorted(monthly) if month_complete(m) is False]

    counts = {t:[daily[d].get(t,0) for d in all_days] for t in ("A","B")}
    both   = [daily[d].get("A",0)+daily[d].get("B",0) for d in all_days]
    def stats(xs):
        if not xs: return {}
        xs2=sorted(xs)
        return {"mean":round(sum(xs)/max(1,days_covered),2),
                "median_activeday":xs2[len(xs2)//2],
                "p90_activeday":xs2[int(len(xs2)*0.9)],
                "max_day":max(xs)}
    clus = sorted(simult.values(), reverse=True)
    summary = {
        "label":label, "symbols":n_symbols, "days_covered":days_covered,
        "window_utc": None if not window else
            [datetime.fromtimestamp(window[0]/1000,timezone.utc).isoformat(),
             datetime.fromtimestamp(window[1]/1000,timezone.utc).isoformat()],
        "total_events":{"A":sum(counts["A"]),"B":sum(counts["B"])},
        "per_day":{"A":stats(counts["A"]),"B":stats(counts["B"]),"A+B":stats(both)},
        "zero_streak_days":{"A":streaks("A"),"B":streaks("B")},
        "share_days_with_any_signal":round(len([x for x in both if x>0])/max(1,days_covered,len(all_days)),2),
        "monthly_A_counts":dict(sorted(monthly.items())),
        "F5_check(A_month<10)":[m for m in complete_months if monthly[m]<F5_MIN_MONTHLY],
        "F5_partial_months_excluded":partial_months,
        "max_simultaneous_same_bar":clus[0] if clus else 0,
        "note":"수익률은 계산하지 않음(확증 예산 미소모)."
    }
    if not window:
        summary["F5_warning"] = "관측 구간 미지정 — 경계월이 부분월일 수 있어 F5 판정 신뢰 불가."
    json.dump(summary, open(f"{OUT_DIR}/summary_{label}.json","w"), ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    log(f"저장: {OUT_DIR}/events_{label}.csv, {OUT_DIR}/summary_{label}.json")

# ---------------------------- 비용(스프레드) 모드 -------------------------------
def run_cost(rounds, interval, fee, universe):
    os.makedirs(OUT_DIR, exist_ok=True)
    uni = set(universe)
    samples = defaultdict(list)   # sym -> [(hour, rel_spread)]
    for r in range(rounds):
        book = http_get("/fapi/v1/ticker/bookTicker")
        now_h = datetime.now(timezone.utc).hour
        for b in book:
            s=b["symbol"]
            if s not in uni: continue
            bid,ask=float(b["bidPrice"]),float(b["askPrice"])
            if bid<=0 or ask<=bid: continue
            samples[s].append((now_h,(ask-bid)/((ask+bid)/2)))
        log(f"스프레드 라운드 {r+1}/{rounds}")
        if r<rounds-1: time.sleep(interval)
    with open(f"{OUT_DIR}/cost_matrix.csv","w") as f:
        f.write("symbol,n,median_spread,p75_spread,roundtrip_lower_bound,hours_utc\n")
        rows=[]
        for s,v in samples.items():
            sp=sorted(x for _,x in v)
            med=sp[len(sp)//2]; p75=sp[int(len(sp)*0.75)]
            rt=2*fee+med
            hrs="|".join(str(h) for h in sorted({h for h,_ in v}))
            rows.append((s,len(sp),med,p75,rt))
            f.write(f"{s},{len(sp)},{med:.6f},{p75:.6f},{rt:.6f},{hrs}\n")
    rows.sort(key=lambda x:x[4])
    print("\n왕복비용 하한(수수료 2회+중위 스프레드, 슬리피지 제외) — 상/하위 10")
    for r in rows[:10]: print(f"  BEST {r[0]:14s} {r[4]*100:.3f}%")
    for r in rows[-10:]: print(f"  WORST {r[0]:14s} {r[4]*100:.3f}%")
    print(f"\n주의: 수수료 {fee*100:.3f}%/편도는 가정값 — 본인 계정 실제 수수료로 교체 필수.")
    print("주의: 이 값은 하한. 실제 비용 = 하한 + 슬리피지(체결 규모/호가 깊이 의존).")
    print("주의: 시간대 매트릭스를 위해 서로 다른 시간대(한국 새벽/저녁, 미국 개장)에 반복 실행할 것.")
    log(f"저장: {OUT_DIR}/cost_matrix.csv")

# ---------------------------- 합성 자가점검 -------------------------------------
def synthetic_bars(days, seed):
    rnd=random.Random(seed)
    n=days*BARS_PER_DAY; price=1.0; bars=[]
    sig=0.003
    t0=int(time.time()*1000)-n*900000
    for i in range(n):
        sig=max(0.001,min(0.02, sig*0.995 + (0.004 if rnd.random()<0.02 else 0)))
        r=rnd.gauss(0,sig)
        vol=rnd.lognormvariate(0,0.6)
        if rnd.random()<0.004:                       # 인위적 캐스케이드
            r-= 4*sig; vol*=12
        price*=math.exp(r)
        bars.append((t0+i*900000, price, vol*1e6))
    return bars

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--mode",choices=["freq","cost"],default="freq")
    ap.add_argument("--days",type=int,default=180)
    ap.add_argument("--rounds",type=int,default=20)
    ap.add_argument("--interval",type=int,default=60)
    ap.add_argument("--fee",type=float,default=0.0005)
    ap.add_argument("--source",choices=["binance","synthetic"],default="binance")
    a=ap.parse_args()

    def widen(win, w):
        if not w: return win
        if not win: return [w[0], w[1]]
        return [min(win[0], w[0]), max(win[1], w[1])]

    if a.source=="synthetic":
        print("="*70+"\nSYNTHETIC MODE — 파이프라인 자가점검 전용. 증거 아님. 판정 사용 금지.\n"+"="*70)
        evs=[]; win=None
        n_sym=30
        for k in range(n_sym):
            b=synthetic_bars(a.days, seed=k)
            evs+=detect_events(f"SYN{k:02d}USDT", b)
            win=widen(win, detect_window(b))
        summarize(evs, a.days-PCTL_DAYS, n_sym, "SYNTHETIC", window=win)
        return

    if requests is None:
        sys.exit("requests 미설치: pip install requests")
    uni=build_universe()
    if a.mode=="cost":
        run_cost(a.rounds,a.interval,a.fee,uni); return
    evs=[]; win=None
    for i,s in enumerate(uni):
        try:
            b=fetch_15m(s,a.days)
            e=detect_events(s,b)
            evs+=e
            win=widen(win, detect_window(b))
            log(f"[{i+1}/{len(uni)}] {s}: 봉 {len(b)}, 이벤트 {len(e)}")
        except Exception as ex:
            log(f"{s} 실패({ex}) — 스킵(결측 기록)")
    summarize(evs, a.days-PCTL_DAYS, len(uni), f"binance_{a.days}d", window=win)

if __name__=="__main__":
    main()
