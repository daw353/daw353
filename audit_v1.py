#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_v1.py — H1/H2 사전등록 부속 감사 (수익률 미조회 = 확증 예산 미소모)

무엇을 측정하는가:
  --mode freq : 트리거 A(청산 캐스케이드 프록시), B(거래량 폭발+양의 수익률)의
                발화 빈도, 일별 분포, 무신호 최장 구간, 월별 F5 체크, 동시발화(군집) 구조
  --mode cost : 유니버스 전 종목 실시간 스프레드 샘플링 → 왕복비용 하한 추정

데이터 출처(venue/source):
  --venue upbit                 정본. 업비트 KRW 마켓 현물 (REST /v1/candles, /v1/orderbook)
  --venue binance --source vision   진단용. Binance USDT-M 공개 아카이브(data.binance.vision)
  --venue binance --source binance  진단용. Binance USDT-M REST (지역 차단 시 즉시 종료)
  --source synthetic            파이프라인 자가점검(합성 데이터). 증거가 아님. 판정 사용 금지.

데이터 현실 고지(사전등록 수정 사항, 측정 전 확정):
  과거 청산(강제 체결) 내역은 무료 REST로 제공되지 않음(바이낸스 allForceOrders 폐지).
  따라서 백테스트/감사 구간의 트리거 A는 프록시로 정의한다:
      A_proxy := [15분 수익률 < -1.5σ(직전 7일, 15분 수익률)] AND [15분 quote 거래대금 > 직전 30일 P99]
  라이브 단계에서 forceOrder 웹소켓 로깅으로 프록시-실제 청산 대응률을 별도 검증한다.
  업비트 현물에는 청산 자체가 없다 — A는 순수 가격/거래대금 프록시로만 해석할 것.

원칙:
  - 이 스크립트는 트리거 이후의 미래 수익률을 절대 계산하지 않는다.
  - 파라미터는 아래 CONFIG에 동결. 실행 중 재량 조정 금지.
  - 지역 차단(451/403) 시 우회하지 않는다. 즉시 종료하고 보고한다.

실행 예:
  python audit_v1.py --venue upbit  --mode freq --days 180        # 정본
  python audit_v1.py --venue binance --source vision --mode freq --days 180   # 진단
  python audit_v1.py --venue upbit  --mode cost --rounds 20 --interval 60
  python audit_v1.py --source synthetic --days 45                 # 자가점검
"""

import argparse, calendar, concurrent.futures as cf, json, math, os, random, statistics, sys, time, zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

try:
    import requests
except ImportError:
    requests = None

# ========================= CONFIG (동결 — 사전등록 §3) =========================
RANK_MIN, RANK_MAX = 20, 150          # 30일 중위 일거래대금 순위 (양끝 포함, 1-indexed)
BAR            = "15m"                # 기본 봉 (트리거 정의 단위)
BAR_MS         = 900000
BARS_PER_DAY   = 96
SIGMA_DAYS     = 7                    # σ 추정 윈도우 (7일 = 672봉)
PCTL_DAYS      = 30                   # 거래대금 분포 윈도우 (30일 = 2880봉)
RET_K          = 1.5                  # A: 수익률 < -1.5σ
VOL_PCTL       = 99.0                 # A: 거래대금 > P99
VOLZ_B         = 3.0                  # B: 거래대금 z > 3, ret > 0
COOLDOWN_MIN   = 240                  # 종목당 트리거별 쿨다운 4시간
F5_MIN_MONTHLY = 10                   # F5: 트리거 A 월 10건 미만 → 폐기
STABLE_SKIP    = {"USDCUSDT","FDUSDUSDT","TUSDUSDT","BUSDUSDT","EURUSDT","USDPUSDT","DAIUSDT",
                  "KRW-USDT","KRW-USDC","KRW-DAI","KRW-BUSD","KRW-TUSD","KRW-USDS"}
OUT_DIR        = "audit_out"
# ================ 엔드포인트/운영 상수 (연구 파라미터 아님 — 교체 가능) ================
BASE_FAPI      = "https://fapi.binance.com"
VISION_BASE    = "https://data.binance.vision"
VISION_S3      = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
VISION_PREFIX  = "data/futures/um/monthly/klines/"
VISION_CACHE   = "vision_cache"
VISION_WORKERS = 8                    # 아카이브 동시 다운로드 (CDN)
UPBIT_BASE     = "https://api.upbit.com"
UPBIT_SLEEP    = 0.15                 # 공지값(2026-09 확인): 그룹당 10 req/s, 600 req/min
                                      # → 보수적 ~6.7 req/s 로 운용
UPBIT_OB_BATCH = 100                  # /v1/orderbook markets 배치 (287개 동시도 200 확인됨)
# 수수료 기본값 — 가정값. 반드시 본인 계정 실제값으로 교체(--fee).
FEE_DEFAULT    = {"upbit": 0.0005,    # 업비트 KRW 마켓 공지 기준 0.05%/편도 (요율표 확인 필요)
                  "binance": 0.0005}  # 바이낸스 USDT-M taker 기준 0.05%/편도 (VIP/BNB 할인 미반영)
# ==============================================================================

def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)

def utcnow_ms(): return int(time.time()*1000)

# ---------------------------- HTTP ---------------------------------------------
def _geoblock(status, url):
    raise SystemExit(
        f"HTTP {status}: 현재 IP에서 {url} 접근 차단(지역 제한). 우회 시도 금지 — 보고 후 종료. "
        "차단되지 않은 위치에서 실행하거나 접근 가능한 리전 엔드포인트로 교체할 것. "
        "(주의: 구글 Colab의 미국 리전 IP도 흔히 차단됨)")

def http_json(url, params=None, retries=5, pace=0.0):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 200:
                if pace: time.sleep(pace)
                return r.json()
            if r.status_code in (418, 429):          # rate limit
                wait = int(r.headers.get("Retry-After", 30))
                log(f"rate limit {r.status_code}, {wait}s 대기 ({r.headers.get('Remaining-Req','')})")
                time.sleep(wait); continue
            if r.status_code in (403, 451):          # 지역 차단 — 재시도 무의미
                _geoblock(r.status_code, url)
            log(f"HTTP {r.status_code} {url} {params}"); time.sleep(2)
        except SystemExit:
            raise
        except Exception as e:
            log(f"요청 실패({e}), 재시도 {i+1}/{retries}"); time.sleep(3)
    raise RuntimeError(f"요청 반복 실패: {url} {params}")

def http_get(path, params=None, retries=5):        # Binance fapi 전용 래퍼
    return http_json(BASE_FAPI + path, params, retries)

def upbit_get(path, params=None):
    return http_json(UPBIT_BASE + path, params, pace=UPBIT_SLEEP)

# ---------------------------- 공통: 결측봉 보정 ---------------------------------
def fill_gaps(bars):
    """결측 15분봉을 (직전 종가, 거래대금 0)으로 채운다.

    이유: 임계값 윈도우는 사전등록에서 '직전 7일 / 직전 30일'로 정의되어 있는데
    구현은 봉 개수(672 / 2880)로 센다. 거래가 없어 봉이 누락되는 마켓(업비트 저유동
    종목)에서는 봉 개수 윈도우가 달력 기준을 초과해 정의와 어긋난다.
    채운 봉은 ret=0, vol=0 이므로 그 자신은 절대 트리거를 발화시키지 못하고
    (A는 ret<0 및 vol>P99, B는 volz>3 및 ret>0 필요), 임계값 윈도우를 달력 기준으로
    유지하는 역할만 한다. 다만 임계값 자체는 낮아지므로 보정 비율을 summary에 기록한다.
    """
    if len(bars) < 2: return bars, 0
    out=[bars[0]]; filled=0
    for ts, close, vol in bars[1:]:
        prev_ts, prev_close, _ = out[-1]
        gap = ts - prev_ts
        if gap > BAR_MS and gap % BAR_MS == 0:
            for k in range(1, gap//BAR_MS):
                out.append((prev_ts + k*BAR_MS, prev_close, 0.0)); filled += 1
        out.append((ts, close, vol))
    return out, filled

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
def summarize(events, days_covered, n_symbols, label, window=None, meta=None):
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
    if meta: summary.update(meta)
    json.dump(summary, open(f"{OUT_DIR}/summary_{label}.json","w"), ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    log(f"저장: {OUT_DIR}/events_{label}.csv, {OUT_DIR}/summary_{label}.json")

# ============================ 소스 1: Binance REST =============================
def binance_universe(cache="universe_binance.json"):
    if os.path.exists(cache):
        u = json.load(open(cache)); log(f"유니버스 캐시 사용: {len(u)}종목"); return u
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
    universe = sorted(med, key=med.get, reverse=True)[RANK_MIN-1:RANK_MAX]
    json.dump(universe, open(cache, "w"))
    log(f"유니버스 확정: {len(universe)}종목 (순위 {RANK_MIN}~{RANK_MAX}) → {cache} 저장")
    return universe

def binance_bars(symbol, days):
    end = utcnow_ms(); start = end - days*86400*1000
    bars = []
    while start < end:
        kl = http_get("/fapi/v1/klines",
                      {"symbol": symbol, "interval": BAR, "startTime": start, "limit": 1500})
        if not kl: break
        now_ms = utcnow_ms()                          # 미완성(진행 중) 봉 제외
        bars += [(int(k[0]), float(k[4]), float(k[7])) for k in kl if int(k[6]) < now_ms]
        nxt = kl[-1][0] + 1
        if nxt <= start: break
        start = nxt
        time.sleep(0.12)
    return bars

# ====================== 소스 2: data.binance.vision (아카이브) ==================
def vision_symbols():
    """S3 리스팅 XML로 USDT-M 심볼 목록. 상장폐지 심볼도 아카이브에 남아 있다."""
    ns   = "{http://s3.amazonaws.com/doc/2006-03-01/}"
    syms, marker = [], ""
    while True:
        for i in range(5):
            try:
                r = requests.get(VISION_S3, params={"delimiter":"/","prefix":VISION_PREFIX,
                                                    "marker":marker}, timeout=40)
                if r.status_code in (403,451): _geoblock(r.status_code, VISION_S3)
                if r.status_code == 200: break
                log(f"S3 리스팅 HTTP {r.status_code}, 재시도"); time.sleep(2)
            except SystemExit: raise
            except Exception as e:
                log(f"S3 리스팅 실패({e}), 재시도 {i+1}/5"); time.sleep(3)
        else:
            raise RuntimeError("S3 리스팅 반복 실패")
        root = ET.fromstring(r.content)
        page = [cp.findtext(ns+"Prefix")[len(VISION_PREFIX):].strip("/")
                for cp in root.iter(ns+"CommonPrefixes")]
        syms += page
        if root.findtext(ns+"IsTruncated") != "true": break
        marker = root.findtext(ns+"NextMarker") or (VISION_PREFIX + page[-1] + "/")
    out = [s for s in syms if s.endswith("USDT") and "_" not in s and s not in STABLE_SKIP]
    log(f"vision 심볼 {len(syms)}개 → USDT 무기한 {len(out)}개")
    return sorted(out)

def vision_rows(key):
    """아카이브 zip 1개 → CSV 행 리스트. 없으면 None. 디스크 캐시."""
    os.makedirs(VISION_CACHE, exist_ok=True)
    fn = os.path.join(VISION_CACHE, key.replace("/", "_"))
    if os.path.exists(fn + ".miss"): return None
    if not os.path.exists(fn):
        for i in range(4):
            try:
                r = requests.get(f"{VISION_BASE}/{key}", timeout=90)
                if r.status_code == 404:
                    open(fn + ".miss", "w").close(); return None
                if r.status_code in (403, 451): _geoblock(r.status_code, VISION_BASE)
                if r.status_code == 200:
                    with open(fn, "wb") as f: f.write(r.content)
                    break
                time.sleep(2)
            except SystemExit: raise
            except Exception as e:
                log(f"vision 다운로드 실패({e}) {key}, 재시도 {i+1}/4"); time.sleep(3)
        else:
            raise RuntimeError(f"vision 다운로드 반복 실패: {key}")
    try:
        with zipfile.ZipFile(fn) as z:
            data = z.read(z.namelist()[0]).decode()
    except zipfile.BadZipFile:
        os.remove(fn); return vision_rows(key)        # 손상 캐시 1회 재시도
    lines = data.splitlines()
    if lines and not lines[0][:1].isdigit():          # 헤더 행 유무 스니핑
        lines = lines[1:]
    return [l.split(",") for l in lines if l]

def _vision_key(sym, interval, kind, stamp):
    return (f"data/futures/um/{kind}/klines/{sym}/{interval}/"
            f"{sym}-{interval}-{stamp}.zip")

def _months_between(d0, d1):
    y, m, out = d0.year, d0.month, []
    while (y, m) <= (d1.year, d1.month):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y+1, 1) if m == 12 else (y, m+1)
    return out

def vision_bars(sym, days, interval=BAR):
    """월별 zip으로 범위를 덮고, 당월(및 월별 파일 미발행 구간)은 일별 zip으로 보충."""
    today  = datetime.now(timezone.utc).date()
    cutoff = utcnow_ms() - days*86400*1000
    start  = today - timedelta(days=days)
    rows   = []
    for m in _months_between(start, today):
        y, mo = (int(x) for x in m.split("-"))
        is_current = (y, mo) == (today.year, today.month)
        r = None if is_current else vision_rows(_vision_key(sym, interval, "monthly", m))
        if r is not None:
            rows += r; continue
        # 월별 파일 없음(당월/발행 지연) → 최근 2개월 이내면 일별로 폴백,
        # 그보다 과거면 데이터 없음으로 간주(상장폐지 심볼의 404 폭주 방지).
        age = (today.year - y)*12 + (today.month - mo)
        if age > 1: continue
        lo = max(start, date(y, mo, 1))               # 요청 범위 밖의 날은 받지 않는다
        hi = min(today, date(y, mo, calendar.monthrange(y, mo)[1]))
        day = lo
        while day <= hi:
            dr = vision_rows(_vision_key(sym, interval, "daily", day.isoformat()))
            if dr: rows += dr
            day += timedelta(days=1)
    now_ms = utcnow_ms()
    seen, bars = set(), []
    for k in rows:
        try:
            ot, close, ct, qv = int(k[0]), float(k[4]), int(k[6]), float(k[7])
        except (ValueError, IndexError):
            continue
        if ot < cutoff or ct >= now_ms or ot in seen: continue
        seen.add(ot); bars.append((ot, close, qv))
    bars.sort()
    return bars

def vision_prefetch(universe, days):
    """본 루프 전에 아카이브 zip을 병렬로 내려받아 디스크 캐시에 채운다(순차 실행 시 80분+)."""
    log(f"vision 아카이브 사전 다운로드: {len(universe)}종목 (동시 {VISION_WORKERS})")
    done = [0]
    def one(s):
        try:
            vision_bars(s, days)
        except SystemExit:
            raise
        except Exception as e:
            log(f"{s} 사전 다운로드 실패({e}) — 본 루프에서 재시도")
        done[0] += 1
        if done[0] % 10 == 0: log(f"  사전 다운로드 {done[0]}/{len(universe)}")
    with cf.ThreadPoolExecutor(VISION_WORKERS) as ex:
        list(ex.map(one, universe))

def vision_universe(cache="universe_vision.json"):
    if os.path.exists(cache):
        u = json.load(open(cache)); log(f"유니버스 캐시 사용: {len(u)}종목"); return u
    syms = vision_symbols()
    today = datetime.now(timezone.utc).date()
    m1 = date(today.year, today.month, 1) - timedelta(days=1)      # 직전 완결월
    m0 = date(m1.year, m1.month, 1) - timedelta(days=1)            # 그 전 달
    months = [f"{m0.year:04d}-{m0.month:02d}", f"{m1.year:04d}-{m1.month:02d}"]
    log(f"vision 랭킹: {len(syms)}종목 × 1d 아카이브 {months} (동시 {VISION_WORKERS})")

    def one(s):
        qv = []
        for m in months:
            r = vision_rows(_vision_key(s, "1d", "monthly", m))
            if not r: continue
            for k in r:
                try: qv.append((int(k[0]), float(k[7])))
                except (ValueError, IndexError): pass
        qv.sort()
        vals = [v for _, v in qv][-30:]                            # 직전 30 완결일
        return (s, statistics.median(vals)) if len(vals) >= 20 else (s, None)

    med, done = {}, 0
    with cf.ThreadPoolExecutor(VISION_WORKERS) as ex:
        for s, v in ex.map(one, syms):
            done += 1
            if v: med[s] = v
            if done % 100 == 0: log(f"  랭킹 진행 {done}/{len(syms)}")
    universe = sorted(med, key=med.get, reverse=True)[RANK_MIN-1:RANK_MAX]
    json.dump(universe, open(cache, "w"))
    log(f"유니버스 확정: {len(universe)}종목 (랭킹 대상 {len(med)}) → {cache} 저장")
    return universe

# ============================ 소스 3: Upbit (정본) =============================
def upbit_universe(cache="universe_upbit.json"):
    if os.path.exists(cache):
        u = json.load(open(cache)); log(f"유니버스 캐시 사용: {len(u)}종목"); return u
    mk = upbit_get("/v1/market/all", {"isDetails": "false"})
    syms = [m["market"] for m in mk
            if m["market"].startswith("KRW-") and m["market"] not in STABLE_SKIP]
    log(f"KRW 마켓 {len(syms)}종목 → 30일 일봉 candle_acc_trade_price 중위값 랭킹 산출 중")
    med = {}
    for i, s in enumerate(syms):
        try:
            c = upbit_get("/v1/candles/days", {"market": s, "count": 31})
            # 응답은 최신순. 첫 항목(진행 중인 오늘)을 제외한 30 완결일.
            qv = [float(x["candle_acc_trade_price"]) for x in c[1:]]
            if len(qv) >= 20: med[s] = statistics.median(qv)
        except Exception as e:
            log(f"{s} 랭킹 스킵({e})")
        if i % 50 == 0: log(f"  랭킹 진행 {i}/{len(syms)}")
    ranked = sorted(med, key=med.get, reverse=True)
    universe = ranked[RANK_MIN-1:RANK_MAX]            # 마켓 수 부족 시 자동으로 20~끝
    json.dump(universe, open(cache, "w"))
    log(f"유니버스 확정: {len(universe)}종목 (랭킹 대상 {len(ranked)}, 순위 {RANK_MIN}~"
        f"{min(RANK_MAX, len(ranked))}) → {cache} 저장")
    return universe

def upbit_bars(market, days):
    """/v1/candles/minutes/15 역방향 페이지네이션. to는 배타, 응답은 최신순."""
    cutoff = utcnow_ms() - days*86400*1000
    to, bars, seen = None, [], set()
    while True:
        p = {"market": market, "count": 200}
        if to: p["to"] = to
        c = upbit_get("/v1/candles/minutes/15", p)
        if not c: break
        oldest = None
        for x in c:
            ts = int(datetime.strptime(x["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S")
                     .replace(tzinfo=timezone.utc).timestamp()*1000)
            oldest = ts if oldest is None else min(oldest, ts)
            if ts in seen: continue
            if ts + BAR_MS > utcnow_ms(): continue    # 진행 중 봉 제외
            seen.add(ts); bars.append((ts, float(x["trade_price"]),
                                       float(x["candle_acc_trade_price"])))
        if oldest is None or oldest <= cutoff: break
        to = datetime.fromtimestamp(oldest/1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bars = [b for b in bars if b[0] >= cutoff]
    bars.sort()
    return bars

# ---------------------------- 비용(스프레드) 모드 -------------------------------
def _cost_write(new_samples, venue, fee):
    """샘플을 누적 CSV에 append 하고, 누적 전체로 cost_matrix.csv 재생성.
       서로 다른 시간대에 여러 번 실행해 시간대 매트릭스를 모으기 위한 구조."""
    os.makedirs(OUT_DIR, exist_ok=True)
    raw = f"{OUT_DIR}/cost_samples_{venue}.csv"
    if not os.path.exists(raw):
        with open(raw, "w") as f: f.write("utc_iso,hour_utc,hour_kst,symbol,rel_spread\n")
    with open(raw, "a") as f:
        for iso, hu, hk, s, sp in new_samples:
            f.write(f"{iso},{hu},{hk},{s},{sp:.8f}\n")

    acc = defaultdict(list)
    with open(raw) as f:
        next(f)
        for line in f:
            p = line.rstrip("\n").split(",")
            if len(p) == 5: acc[p[3]].append((int(p[1]), int(p[2]), float(p[4])))
    rows = []
    with open(f"{OUT_DIR}/cost_matrix.csv", "w") as f:
        f.write("symbol,n,median_spread,p75_spread,roundtrip_lower_bound,hours_utc,hours_kst\n")
        for s, v in acc.items():
            sp = sorted(x for _, _, x in v)
            med, p75 = sp[len(sp)//2], sp[int(len(sp)*0.75)]
            rt = 2*fee + med
            hu = "|".join(str(h) for h in sorted({h for h, _, _ in v}))
            hk = "|".join(str(h) for h in sorted({k for _, k, _ in v}))
            rows.append((s, len(sp), med, p75, rt))
            f.write(f"{s},{len(sp)},{med:.6f},{p75:.6f},{rt:.6f},{hu},{hk}\n")
    rows.sort(key=lambda x: x[4])
    print(f"\n왕복비용 하한(수수료 2회+중위 스프레드, 슬리피지 제외) — {venue} 상/하위 10")
    for r in rows[:10]:  print(f"  BEST  {r[0]:14s} {r[4]*100:.3f}%  (n={r[1]})")
    for r in rows[-10:]: print(f"  WORST {r[0]:14s} {r[4]*100:.3f}%  (n={r[1]})")
    print(f"\n주의: 수수료 {fee*100:.3f}%/편도는 가정값 — 본인 계정 실제 수수료로 교체 필수.")
    print("주의: 이 값은 하한. 실제 비용 = 하한 + 슬리피지(체결 규모/호가 깊이 의존).")
    print("주의: 시간대 매트릭스는 누적된다. 서로 다른 시간대(한국 새벽/저녁, 미국 개장)에")
    print(f"      --mode cost 를 반복 실행하면 {raw} 에 쌓이고 매트릭스가 갱신된다.")
    log(f"저장: {raw}, {OUT_DIR}/cost_matrix.csv")
    return rows

def run_cost_binance(rounds, interval, fee, universe):
    uni, new = set(universe), []
    for r in range(rounds):
        book = http_get("/fapi/v1/ticker/bookTicker")
        now = datetime.now(timezone.utc)
        for b in book:
            s = b["symbol"]
            if s not in uni: continue
            bid, ask = float(b["bidPrice"]), float(b["askPrice"])
            if bid <= 0 or ask <= bid: continue
            new.append((now.isoformat(), now.hour, (now.hour+9) % 24, s, (ask-bid)/((ask+bid)/2)))
        log(f"스프레드 라운드 {r+1}/{rounds}")
        if r < rounds-1: time.sleep(interval)
    _cost_write(new, "binance", fee)

def run_cost_upbit(rounds, interval, fee, universe):
    new = []
    for r in range(rounds):
        now = datetime.now(timezone.utc)
        for i in range(0, len(universe), UPBIT_OB_BATCH):
            batch = universe[i:i+UPBIT_OB_BATCH]
            ob = upbit_get("/v1/orderbook", {"markets": ",".join(batch)})
            for o in ob:
                u = o.get("orderbook_units") or []
                if not u: continue
                bid, ask = float(u[0]["bid_price"]), float(u[0]["ask_price"])
                if bid <= 0 or ask <= bid: continue
                new.append((now.isoformat(), now.hour, (now.hour+9) % 24,
                            o["market"], (ask-bid)/((ask+bid)/2)))
        log(f"스프레드 라운드 {r+1}/{rounds} (누적 샘플 {len(new)})")
        if r < rounds-1: time.sleep(interval)
    _cost_write(new, "upbit", fee)

# ---------------------------- 합성 자가점검 -------------------------------------
def synthetic_bars(days, seed):
    rnd=random.Random(seed)
    n=days*BARS_PER_DAY; price=1.0; bars=[]
    sig=0.003
    t0=utcnow_ms()-n*BAR_MS
    for i in range(n):
        sig=max(0.001,min(0.02, sig*0.995 + (0.004 if rnd.random()<0.02 else 0)))
        r=rnd.gauss(0,sig)
        vol=rnd.lognormvariate(0,0.6)
        if rnd.random()<0.004:                       # 인위적 캐스케이드
            r-= 4*sig; vol*=12
        price*=math.exp(r)
        bars.append((t0+i*BAR_MS, price, vol*1e6))
    return bars

# ---------------------------- 실행 -------------------------------------------
SURVIVORSHIP = {
 "upbit":  "생존편향 있음: /v1/market/all 은 현재 상장 마켓만 반환한다. 감사 구간 중 "
           "상장폐지된 마켓은 유니버스에서 자동 누락되며, 폐지 직전의 극단 변동(트리거가 "
           "가장 많이 발화했을 구간)이 통째로 빠진다. 발화 빈도는 상향이 아니라 하향 편의로 "
           "볼 수 없다 — 방향은 불명이며 이 감사로는 보정 불가.",
 "vision": "부분적 생존편향: 아카이브에는 상장폐지 심볼도 남아 있으나, 랭킹을 직전 완결월 "
           "거래대금으로 산출하므로 그 시점에 이미 폐지된 심볼은 유니버스에 들지 못한다.",
 "binance":"생존편향 있음: exchangeInfo 의 status=TRADING 심볼만 사용한다.",
 "synthetic":"해당 없음(합성 데이터)."}

def run_freq(get_bars, universe, days, label, meta):
    evs, win, nb, nf, failed = [], None, 0, 0, []
    for i, s in enumerate(universe):
        try:
            b = get_bars(s, days)
            b, filled = fill_gaps(b)
            e = detect_events(s, b)
            evs += e; nb += len(b); nf += filled
            w = detect_window(b)
            if w: win = [w[0], w[1]] if not win else [min(win[0], w[0]), max(win[1], w[1])]
            log(f"[{i+1}/{len(universe)}] {s}: 봉 {len(b)} (보정 {filled}), 이벤트 {len(e)}")
        except SystemExit:
            raise
        except Exception as ex:
            failed.append(s); log(f"{s} 실패({ex}) — 스킵(결측 기록)")
    meta = dict(meta)
    meta.update({"bars_total": nb,
                 "filled_bar_ratio": round(nf/nb, 5) if nb else None,
                 "filled_bars": nf,
                 "symbols_failed": failed})
    summarize(evs, days-PCTL_DAYS, len(universe), label, window=win, meta=meta)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--mode",choices=["freq","cost"],default="freq")
    ap.add_argument("--venue",choices=["upbit","binance"],default="binance")
    ap.add_argument("--source",choices=["binance","vision","synthetic"],default="binance")
    ap.add_argument("--days",type=int,default=180)
    ap.add_argument("--rounds",type=int,default=20)
    ap.add_argument("--interval",type=int,default=60)
    ap.add_argument("--fee",type=float,default=None)
    ap.add_argument("--limit-symbols",type=int,default=0,help="스모크 테스트용 상위 N종목만")
    a=ap.parse_args()

    if a.source=="synthetic":
        print("="*70+"\nSYNTHETIC MODE — 파이프라인 자가점검 전용. 증거 아님. 판정 사용 금지.\n"+"="*70)
        evs, win = [], None
        n_sym=30
        for k in range(n_sym):
            b=synthetic_bars(a.days, seed=k)
            evs+=detect_events(f"SYN{k:02d}USDT", b)
            w=detect_window(b)
            if w: win=[w[0],w[1]] if not win else [min(win[0],w[0]),max(win[1],w[1])]
        summarize(evs, a.days-PCTL_DAYS, n_sym, "SYNTHETIC", window=win,
                  meta={"venue":"synthetic","source":"synthetic",
                        "survivorship":SURVIVORSHIP["synthetic"],
                        "EVIDENCE":"NO — 자가점검 전용. 판정 사용 금지."})
        return

    if requests is None: sys.exit("requests 미설치: pip install requests")
    if a.venue=="upbit" and a.source=="vision":
        sys.exit("--venue upbit 는 --source vision 과 함께 쓸 수 없다 (vision 은 Binance 아카이브).")

    src   = "upbit" if a.venue=="upbit" else a.source
    fee   = a.fee if a.fee is not None else FEE_DEFAULT[a.venue]
    uni   = {"upbit": upbit_universe, "vision": vision_universe, "binance": binance_universe}[src]()
    if a.limit_symbols: uni = uni[:a.limit_symbols]
    getb  = {"upbit": upbit_bars, "vision": vision_bars, "binance": binance_bars}[src]

    if a.mode=="cost":
        (run_cost_upbit if a.venue=="upbit" else run_cost_binance)(a.rounds,a.interval,fee,uni)
        return
    if src == "vision": vision_prefetch(uni, a.days)

    meta = {"venue": a.venue, "source": src,
            "universe_rule": f"30일 중위 일거래대금 순위 {RANK_MIN}~{RANK_MAX} (1-indexed, 양끝 포함)",
            "universe_n_actual": len(uni),
            "quote_ccy": "KRW" if a.venue=="upbit" else "USDT",
            "survivorship": SURVIVORSHIP[src],
            "trigger_A_caveat": ("업비트 현물에는 청산이 없음 — A는 가격/거래대금 프록시일 뿐 "
                                 "청산 캐스케이드 대리변수로 해석 불가"
                                 if a.venue=="upbit" else
                                 "A는 청산 캐스케이드 프록시 (allForceOrders 폐지로 실제 청산 미조회)"),
            "EVIDENCE": "정본" if a.venue=="upbit" else "진단용(참조 시장)"}
    label = f"{a.venue}_{src}_{a.days}d" + (f"_top{a.limit_symbols}" if a.limit_symbols else "")
    run_freq(getb, uni, a.days, label, meta)

if __name__=="__main__":
    main()
