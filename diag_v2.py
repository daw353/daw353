#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""diag_v2.py — H1/H2 부속 진단 (a)~(e). 전부 수익률 미조회 = 확증 예산 미소모.

  (a) 전이 가설: 업비트 A-이벤트 ↔ 바이낸스 A-이벤트 동일자산·동일봉 일치율 (±1봉 병기)
  (b) 이벤트 시각 분포 (KST 시간대별, A/B 분리)
  (c) 군집 재집계: 동일 15분봉 A-이벤트 = 1군집. 유효 N을 군집 기준으로 보고.
      BTC 15분 수익률 병기 → |BTC ret|>1% 봉에 걸린 군집 비율 (V2 예상 제거율)
  (d) 결측봉 민감도: 임계값 계산에서 보정봉 제외 후 재집계, 이벤트 수 변화율
  (e) 이벤트 시점 σ15·σ60 실측 분포 → 비용/σ 배수, TP/SL 산수 실측치 교체

미래 정보 미사용 원칙:
  (e)의 σ는 전부 '이벤트 봉 직전까지의 후행 창'으로 계산한다. 진입 이후 실현변동성은
  조회하지 않는다 — 그것이 확증 예산이다. 따라서 (e)는 진입 후 σ의 '대리변수'이며,
  이 치환 자체가 가정임을 결과 JSON에 명시한다.
"""
import argparse, csv, json, math, os, statistics as st
from collections import Counter, defaultdict
from datetime import datetime, timezone

import audit_v1 as A
import bar_cache as BC

OUT     = "diag_out"
EV_UP   = "audit_out/events_upbit_upbit_180d.csv"
EV_BN   = "audit_out/events_binance_vision_180d.csv"
COSTCSV = "audit_out/cost_matrix.csv"
DAYS    = 180
SIG_WIN = A.SIGMA_DAYS * A.BARS_PER_DAY          # 672
PCT_WIN = A.PCTL_DAYS  * A.BARS_PER_DAY          # 2880
NO_FWD  = "이벤트 이후 구간 미조회 — 모든 통계는 이벤트 봉 직전까지의 후행 창."

def q(xs, p):
    if not xs: return None
    s = sorted(xs); return s[min(len(s)-1, int(len(s)*p))]

def load_events(path):
    out = []
    with open(path) as f:
        for r in csv.DictReader(f):
            ts = int(datetime.fromisoformat(r["utc"]).timestamp()*1000)
            out.append((ts, r["symbol"], r["trigger"], float(r["ret15"]), float(r["volz"])))
    return out

def dump(name, obj):
    os.makedirs(OUT, exist_ok=True)
    json.dump(obj, open(f"{OUT}/{name}.json", "w"), ensure_ascii=False, indent=2)
    print(json.dumps(obj, ensure_ascii=False, indent=2))
    A.log(f"저장: {OUT}/{name}.json")

# ------------------------------- (a) 전이 가설 --------------------------------
def map_upbit_to_binance(market, bset):
    base = market.split("-", 1)[1]
    for c in (base+"USDT", "1000"+base+"USDT", "1000000"+base+"USDT"):
        if c in bset: return c
    return None


def _kst(ts): return (datetime.fromtimestamp(ts/1000, timezone.utc).hour + 9) % 24

def match_by_hour(upA, bn_by, pairs):
    num, den = Counter(), Counter()
    for ts, m, *_ in upA:
        if m not in pairs: continue
        h = _kst(ts); den[h] += 1
        if any(ts+d*A.BAR_MS in bn_by.get(pairs[m], set()) for d in (-1,0,1)): num[h] += 1
    return {str(h): {"n": den[h], "pm1_rate": round(num[h]/den[h], 3)} for h in sorted(den)}

def lag_hist(upA, bn_by, pairs, span=4):
    """가장 가까운 바이낸스 A-이벤트의 오프셋(봉). 음수 = 바이낸스가 먼저(전이 방향)."""
    c = Counter()
    for ts, m, *_ in upA:
        if m not in pairs: continue
        bts = bn_by.get(pairs[m], set())
        cand = [d for d in range(-span, span+1) if ts+d*A.BAR_MS in bts]
        c[str(min(cand, key=abs)) if cand else "no_match_within_4bars"] += 1
    lead  = sum(v for k, v in c.items() if k.lstrip("-").isdigit() and int(k) < 0)
    lag   = sum(v for k, v in c.items() if k.lstrip("-").isdigit() and int(k) > 0)
    return dict(sorted(c.items(), key=lambda kv: (not kv[0].lstrip("-").isdigit(),
                                                  int(kv[0]) if kv[0].lstrip("-").isdigit() else 0))), \
           {"binance_leads": lead, "upbit_leads": lag, "same_bar": c.get("0", 0)}

def step_a2():
    """(a) 커버리지 확장 — 바이낸스 측을 동결 유니버스가 아니라 아카이브 전체에서 페어링.
       진단 전용: 여기서 쓰는 바이낸스 심볼 집합은 사전등록 유니버스(순위 20~150) 밖이다."""
    uni_u = json.load(open("universe_upbit.json"))
    arch  = set(A.vision_symbols())
    pairs = {m: b for m, b in ((m, map_upbit_to_binance(m, arch)) for m in uni_u) if b}
    tgt   = sorted(set(pairs.values()))
    A.log(f"(a2) 업비트 {len(uni_u)}종목 중 아카이브 페어링 {len(pairs)}종목 → 바이낸스 {len(tgt)}심볼")
    A.vision_prefetch(tgt, DAYS)
    bnA, rows = [], []
    for i, b in enumerate(tgt):
        try:
            bars, _ = A.fill_gaps(A.vision_bars(b, DAYS))
            ev = [e for e in A.detect_events(b, bars) if e[2] == "A"]
            bnA += [(e[0], b) for e in ev]; rows.append((b, len(bars), len(ev)))
        except Exception as e:
            A.log(f"(a2) {b} 실패({e})"); rows.append((b, 0, -1))
        if (i+1) % 20 == 0: A.log(f"  (a2) {i+1}/{len(tgt)}")
    with open(f"{OUT}/a2_binance_A_events.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["utc","symbol"])
        for ts, b in sorted(bnA):
            w.writerow([datetime.fromtimestamp(ts/1000, timezone.utc).isoformat(), b])

    up = load_events(EV_UP)
    lo = max(min(e[0] for e in up), min(ts for ts, _ in bnA))
    hi = min(max(e[0] for e in up), max(ts for ts, _ in bnA))
    upA = [e for e in up if e[2] == "A" and lo <= e[0] <= hi]
    bn_by = defaultdict(set)
    for ts, b in bnA:
        if lo <= ts <= hi: bn_by[b].add(ts)
    nbars = (hi-lo)//A.BAR_MS + 1
    scope = [e for e in upA if e[1] in pairs]
    ex = sum(1 for e in scope if e[0] in bn_by.get(pairs[e[1]], set()))
    p1 = sum(1 for e in scope if any(e[0]+d*A.BAR_MS in bn_by.get(pairs[e[1]], set())
                                     for d in (-1,0,1)))
    ch = sum(len(bn_by.get(pairs[e[1]], set()))/nbars for e in scope)/max(1, len(scope))
    lh, ld = lag_hist(scope, bn_by, pairs)
    dump("a2_transition_extended", {
        "diagnostic":"(a) 확장 — 바이낸스 측을 아카이브 전체에서 페어링한 전이 일치율",
        "no_forward_returns": NO_FWD,
        "SCOPE_NOTE":"바이낸스 심볼 집합이 동결 유니버스(순위 20~150) 밖이다. 진단 전용, "
                     "사전등록 감사 결과가 아님.",
        "overlap_window_utc":[datetime.fromtimestamp(lo/1000,timezone.utc).isoformat(),
                              datetime.fromtimestamp(hi/1000,timezone.utc).isoformat()],
        "upbit_universe":len(uni_u), "assets_paired":len(pairs),
        "assets_unpaired":sorted(set(uni_u)-set(pairs)),
        "binance_symbols":len(tgt), "binance_A_events":len(bnA),
        "upbit_A_events_total":len([e for e in up if e[2]=="A"]),
        "upbit_A_events_in_scope":len(scope),
        "coverage_of_upbit_A":round(len(scope)/max(1,len([e for e in up if e[2]=="A"])),3),
        "match_rate_exact_bar":round(ex/max(1,len(scope)),4),
        "match_rate_pm1_bar":round(p1/max(1,len(scope)),4),
        "chance_baseline_exact":round(ch,4),
        "lift_over_chance_exact":round((ex/max(1,len(scope)))/ch,2) if ch else None,
        "lag_histogram_bars":lh, "lag_direction":ld,
        "match_rate_by_kst_hour":match_by_hour(scope, bn_by, pairs),
        "per_symbol_csv":f"{OUT}/a2_binance_A_events.csv"})

def step_a():
    up, bn = load_events(EV_UP), load_events(EV_BN)
    uni_b  = set(json.load(open("universe_vision.json")))
    uni_u  = json.load(open("universe_upbit.json"))
    # 두 관측 구간의 교집합으로 한정 (vision 은 아카이브 발행 지연으로 약 1일 짧다)
    lo = max(min(e[0] for e in up), min(e[0] for e in bn))
    hi = min(max(e[0] for e in up), max(e[0] for e in bn))
    upA = [e for e in up if e[2]=="A" and lo<=e[0]<=hi]
    bnA = [e for e in bn if e[2]=="A" and lo<=e[0]<=hi]

    mapping  = {m: map_upbit_to_binance(m, uni_b) for m in uni_u}
    paired   = {m: b for m, b in mapping.items() if b}
    bn_by    = defaultdict(set)
    for ts, s, *_ in bnA: bn_by[s].add(ts)
    nbars    = (hi - lo)//A.BAR_MS + 1

    rows, tot = [], {"exact":0, "pm1":0, "n":0}
    for m in sorted(paired):
        b = paired[m]
        ev = [e for e in upA if e[1]==m]
        if not ev: continue
        bts = bn_by.get(b, set())
        ex  = sum(1 for e in ev if e[0] in bts)
        p1  = sum(1 for e in ev if any(e[0]+d*A.BAR_MS in bts for d in (-1,0,1)))
        p_chance = len(bts)/nbars if nbars else 0.0
        rows.append({"upbit":m, "binance":b, "upbit_A":len(ev), "binance_A":len(bts),
                     "exact":ex, "pm1":p1,
                     "exact_rate":round(ex/len(ev),4), "pm1_rate":round(p1/len(ev),4),
                     "chance_exact":round(p_chance,4), "chance_pm1":round(min(1,3*p_chance),4)})
        tot["exact"]+=ex; tot["pm1"]+=p1; tot["n"]+=len(ev)

    ch_ex  = sum(r["chance_exact"]*r["upbit_A"] for r in rows)/max(1,tot["n"])
    ch_p1  = sum(r["chance_pm1"]*r["upbit_A"] for r in rows)/max(1,tot["n"])
    # 역방향
    up_by = defaultdict(set)
    for ts, s, *_ in upA: up_by[s].add(ts)
    inv = {b: m for m, b in paired.items()}
    rev_n = rev_ex = rev_p1 = 0
    for ts, s, *_ in bnA:
        if s not in inv: continue
        uts = up_by.get(inv[s], set()); rev_n += 1
        if ts in uts: rev_ex += 1
        if any(ts+d*A.BAR_MS in uts for d in (-1,0,1)): rev_p1 += 1

    with open(f"{OUT}/a_transition_by_asset.csv","w",newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    dist = Counter()
    for r in rows: dist[round(r["pm1_rate"],1)] += 1
    dump("a_transition", {
        "diagnostic":"(a) 전이 가설 — 업비트 A ↔ 바이낸스 A 동일자산·동일봉 일치율",
        "no_forward_returns": NO_FWD,
        "overlap_window_utc":[datetime.fromtimestamp(lo/1000,timezone.utc).isoformat(),
                              datetime.fromtimestamp(hi/1000,timezone.utc).isoformat()],
        "universe_upbit": len(uni_u), "universe_binance": len(uni_b),
        "assets_paired": len(paired), "assets_paired_with_upbit_A": len(rows),
        "assets_unpaired": sorted(m for m,b in mapping.items() if not b),
        "upbit_A_events_in_scope": tot["n"],
        "match_rate_exact_bar": round(tot["exact"]/max(1,tot["n"]),4),
        "match_rate_pm1_bar":   round(tot["pm1"]/max(1,tot["n"]),4),
        "chance_baseline_exact": round(ch_ex,4),
        "chance_baseline_pm1":   round(ch_p1,4),
        "lift_over_chance_exact": round((tot["exact"]/max(1,tot["n"]))/ch_ex,2) if ch_ex else None,
        "lift_over_chance_pm1":   round((tot["pm1"]/max(1,tot["n"]))/ch_p1,2) if ch_p1 else None,
        "reverse_binance_to_upbit": {"n":rev_n,
            "exact":round(rev_ex/max(1,rev_n),4), "pm1":round(rev_p1/max(1,rev_n),4)},
        "per_asset_pm1_rate_histogram": {str(k):v for k,v in sorted(dist.items())},
        "match_rate_by_kst_hour": match_by_hour(
            [e for e in upA if e[1] in paired], bn_by, paired),
        "lag_histogram_bars": lag_hist([e for e in upA if e[1] in paired], bn_by, paired)[0],
        "lag_direction": lag_hist([e for e in upA if e[1] in paired], bn_by, paired)[1],
        "per_asset_csv": f"{OUT}/a_transition_by_asset.csv"})

# ------------------------------- (b) 시각 분포 --------------------------------
def step_b():
    res = {"diagnostic":"(b) 이벤트 발생 시각 분포 (KST 시간대별, A/B 분리)",
           "no_forward_returns": NO_FWD}
    for tag, path in (("upbit", EV_UP), ("binance_vision", EV_BN)):
        ev = load_events(path)
        h = {t: Counter() for t in ("A","B")}
        for ts, s, t, *_ in ev:
            kst = (datetime.fromtimestamp(ts/1000, timezone.utc).hour + 9) % 24
            h[t][kst] += 1
        block = lambda t, rng: sum(h[t][x] for x in rng)
        res[tag] = {
            "A_by_kst_hour":[h["A"][i] for i in range(24)],
            "B_by_kst_hour":[h["B"][i] for i in range(24)],
            "A_total":sum(h["A"].values()), "B_total":sum(h["B"].values()),
            "A_share_kst_09_18_국내주간":round(block("A",range(9,18))/max(1,sum(h["A"].values())),3),
            "A_share_kst_18_24_국내저녁":round(block("A",range(18,24))/max(1,sum(h["A"].values())),3),
            "A_share_kst_22_02_미국개장":round((block("A",range(22,24))+block("A",range(0,2)))
                                              /max(1,sum(h["A"].values())),3),
            "A_share_kst_02_09_새벽":round(block("A",range(2,9))/max(1,sum(h["A"].values())),3),
            "A_peak_kst_hour":max(range(24), key=lambda i: h["A"][i]),
            "B_peak_kst_hour":max(range(24), key=lambda i: h["B"][i])}
    with open(f"{OUT}/b_hour_hist.csv","w",newline="") as f:
        w=csv.writer(f); w.writerow(["kst_hour","upbit_A","upbit_B","binance_A","binance_B"])
        for i in range(24):
            w.writerow([i, res["upbit"]["A_by_kst_hour"][i], res["upbit"]["B_by_kst_hour"][i],
                        res["binance_vision"]["A_by_kst_hour"][i],
                        res["binance_vision"]["B_by_kst_hour"][i]])
    # 비용 샘플 시간대 커버리지
    if os.path.exists(COSTCSV):
        hrs=set()
        for r in csv.DictReader(open(COSTCSV)): hrs |= set(int(x) for x in r["hours_kst"].split("|"))
        res["cost_sample_kst_hours_covered"]=sorted(hrs)
        res["cost_sample_gap_vs_event_peak"]=(
            "이벤트 피크 시간대가 비용 샘플에 포함됨" if res["upbit"]["A_peak_kst_hour"] in hrs
            else f"경고: 이벤트 피크(KST {res['upbit']['A_peak_kst_hour']}시)의 비용 샘플 없음")
    dump("b_hour_distribution", res)

# ------------------------------- (c) 군집 재집계 ------------------------------
def btc_ret(venue, sym):
    b = BC.load(venue, sym, DAYS)
    if not b: return None
    b, _ = A.fill_gaps(b)
    return {b[i][0]: (b[i][1]/b[i-1][1]-1) for i in range(1, len(b))}

def step_c():
    res = {"diagnostic":"(c) 군집 재집계 — 동일 15분봉 A-이벤트 = 1군집",
           "no_forward_returns": NO_FWD,
           "why":"유효 표본은 발화 건수가 아니라 군집 수. 건수 기준 검정력은 과대평가."}
    for tag, path, bsym, bven in (("upbit", EV_UP, "KRW-BTC", "upbit"),
                                  ("binance_vision", EV_BN, "BTCUSDT", "vision")):
        ev = [e for e in load_events(path) if e[2]=="A"]
        cl = defaultdict(set)
        for ts, s, *_ in ev: cl[ts].add(s)
        sizes = sorted(len(v) for v in cl.values())
        daily = Counter(datetime.fromtimestamp(ts/1000,timezone.utc).strftime("%Y-%m-%d")
                        for ts in cl)
        blk = {"A_events":len(ev), "clusters":len(cl),
               "events_per_cluster_mean":round(len(ev)/max(1,len(cl)),2),
               "cluster_size":{"p50":q(sizes,.5),"p90":q(sizes,.9),"max":max(sizes) if sizes else 0,
                               "share_size1":round(sum(1 for x in sizes if x==1)/max(1,len(sizes)),3),
                               "share_size_ge10":round(sum(1 for x in sizes if x>=10)/max(1,len(sizes)),3)},
               "clusters_per_day":{"mean":round(len(cl)/max(1,len(daily)),2),
                                   "p90":q(sorted(daily.values()),.9),
                                   "max":max(daily.values()) if daily else 0},
               "effective_N_event_basis":len(ev), "effective_N_cluster_basis":len(cl),
               "power_overstatement_factor":round(len(ev)/max(1,len(cl)),2)}
        r = btc_ret(bven, bsym)
        if r:
            hit  = [ts for ts in cl if abs(r.get(ts,0.0))>0.01]
            hev  = sum(len(cl[ts]) for ts in hit)
            blk["V2_btc_filter"]={"btc_ref":bsym,
                "clusters_with_abs_btc_ret_gt_1pct":len(hit),
                "cluster_removal_rate":round(len(hit)/max(1,len(cl)),4),
                "event_removal_rate":round(hev/max(1,len(ev)),4),
                "note":"V2가 |BTC 15분 수익률|>1% 봉을 제외할 경우의 예상 제거율"}
        else:
            blk["V2_btc_filter"]={"error":f"{bven}/{bsym} 봉 캐시 없음 — bar_cache.py 먼저 실행"}
        res[tag]=blk
        with open(f"{OUT}/c_clusters_{tag}.csv","w",newline="") as f:
            w=csv.writer(f); w.writerow(["utc","cluster_size","btc_ret15","symbols"])
            for ts in sorted(cl):
                w.writerow([datetime.fromtimestamp(ts/1000,timezone.utc).isoformat(),
                            len(cl[ts]), round(r.get(ts,float('nan')),5) if r else "",
                            "|".join(sorted(cl[ts]))])
    dump("c_clusters", res)

# --------------------- (d) 결측봉 민감도 / (e) 이벤트 시점 σ -------------------
def detect(symbol, bars, real, thresholds_real_only):
    """audit_v1.detect_events 와 동일 판정. thresholds_real_only=True 면
       임계값 창(σ7d, P99, z)에서 보정봉을 제외한다. 판정 대상 봉은 항상 실봉."""
    n = len(bars); warm = PCT_WIN
    if n <= warm+1: return []
    ts  = [b[0] for b in bars]
    ret = [0.0]+[(bars[i][1]/bars[i-1][1]-1) if bars[i-1][1]>0 else 0.0 for i in range(1,n)]
    vol = [b[2] for b in bars]
    ev, last = [], {"A":-10**18,"B":-10**18}
    day, p99, vmean, vstd = -1, None, None, None
    for i in range(warm, n):
        d = ts[i]//86400000
        if d != day:
            day = d
            w = [vol[j] for j in range(i-warm,i) if real[j]] if thresholds_real_only \
                else vol[i-warm:i]
            if len(w) < 100: p99=vmean=vstd=None
            else:
                sw=sorted(w); p99=sw[min(len(sw)-1,int(len(sw)*A.VOL_PCTL/100))]
                vmean=sum(w)/len(w)
                vstd=(sum((x-vmean)**2 for x in w)/len(w))**0.5 or 1e-12
        if p99 is None or not real[i]: continue
        rw = [ret[j] for j in range(i-SIG_WIN,i) if real[j]] if thresholds_real_only \
             else ret[i-SIG_WIN:i]
        if len(rw) < 50: continue
        mu = sum(rw)/len(rw)
        sd = (sum((x-mu)**2 for x in rw)/len(rw))**0.5 or 1e-12
        volz = (vol[i]-vmean)/vstd
        cd = A.COOLDOWN_MIN*60000
        if ret[i] < -A.RET_K*sd and vol[i] > p99 and ts[i]-last["A"] >= cd:
            ev.append((ts[i],symbol,"A")); last["A"]=ts[i]
        if volz > A.VOLZ_B and ret[i] > 0 and ts[i]-last["B"] >= cd:
            ev.append((ts[i],symbol,"B")); last["B"]=ts[i]
    return ev

def prep(venue, sym):
    raw = BC.load(venue, sym, DAYS)
    if not raw: return None
    rawset = {b[0] for b in raw}
    bars, _ = A.fill_gaps(raw)
    return bars, [b[0] in rawset for b in bars]

def step_d(venue="upbit", uni_file="universe_upbit.json"):
    uni = json.load(open(uni_file))
    base_n, alt_n, per, miss = Counter(), Counter(), [], []
    for i, s in enumerate(uni):
        p = prep(venue, s)
        if not p: miss.append(s); continue
        bars, real = p
        b0 = detect(s, bars, real, False)
        b1 = detect(s, bars, real, True)
        c0 = Counter(t for _,_,t in b0); c1 = Counter(t for _,_,t in b1)
        for t in "AB": base_n[t]+=c0[t]; alt_n[t]+=c1[t]
        per.append({"symbol":s, "fill_ratio":round(1-sum(real)/len(real),4),
                    "A_base":c0["A"], "A_real_only":c1["A"],
                    "B_base":c0["B"], "B_real_only":c1["B"]})
        if (i+1) % 20 == 0: A.log(f"  (d) {i+1}/{len(uni)}")
    with open(f"{OUT}/d_fill_sensitivity_by_symbol.csv","w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(per[0].keys())); w.writeheader(); w.writerows(per)
    hi = [r for r in per if r["fill_ratio"] >= 0.10]
    chg = lambda t: round((alt_n[t]-base_n[t])/max(1,base_n[t]),4)
    dump("d_fill_sensitivity", {
        "diagnostic":"(d) 결측봉 민감도 — 임계값 창에서 보정봉 제외 후 재집계",
        "no_forward_returns": NO_FWD, "venue":venue,
        "symbols":len(per), "symbols_missing_cache":miss,
        "baseline_events":dict(base_n), "real_only_threshold_events":dict(alt_n),
        "change_rate":{"A":chg("A"), "B":chg("B")},
        "high_fill_symbols_ge10pct":len(hi),
        "high_fill_change_rate":{
            "A":round((sum(r["A_real_only"] for r in hi)-sum(r["A_base"] for r in hi))
                      /max(1,sum(r["A_base"] for r in hi)),4),
            "B":round((sum(r["B_real_only"] for r in hi)-sum(r["B_base"] for r in hi))
                      /max(1,sum(r["B_base"] for r in hi)),4)} if hi else None,
        "interpretation":"보정봉은 vol=0·ret=0 이므로 포함 시 임계값(P99·평균·σ)을 낮춘다. "
                         "변화율이 크면 발화 건수의 상당 부분이 보정 인공물이라는 뜻.",
        "per_symbol_csv":f"{OUT}/d_fill_sensitivity_by_symbol.csv"})

def step_e(venue="upbit", uni_file="universe_upbit.json", ev_path=None):
    uni = json.load(open(uni_file))
    ev_by = defaultdict(list)
    for ts, s, t, *_ in load_events(ev_path or EV_UP): ev_by[s].append((ts,t))
    cost = {}
    if os.path.exists(COSTCSV):
        for r in csv.DictReader(open(COSTCSV)):
            cost[r["symbol"]] = float(r["roundtrip_lower_bound"])
    rec = {"A":{"s15":[],"s60":[]}, "B":{"s15":[],"s60":[]}}
    ratio = {"A":[], "B":[]}
    for s in uni:
        p = prep(venue, s)
        if not p or s not in ev_by: continue
        bars, _ = p
        idx = {b[0]: i for i, b in enumerate(bars)}
        close = [b[1] for b in bars]
        ret = [0.0]+[(close[i]/close[i-1]-1) if close[i-1]>0 else 0.0 for i in range(1,len(close))]
        for ts, t in ev_by[s]:
            i = idx.get(ts)
            if i is None or i < SIG_WIN: continue
            rw = ret[i-SIG_WIN:i]                     # 후행 7일 15분 수익률
            s15 = st.pstdev(rw) if len(rw)>2 else None
            hc  = close[i-SIG_WIN:i:4]                # 후행 7일 1시간 종가 (168점)
            hr  = [hc[k]/hc[k-1]-1 for k in range(1,len(hc)) if hc[k-1]>0]
            s60 = st.pstdev(hr) if len(hr)>2 else None
            if s15: rec[t]["s15"].append(s15)
            if s60: rec[t]["s60"].append(s60)
            if s15 and s in cost: ratio[t].append(cost[s]/s15)
    def blk(t):
        a,b = rec[t]["s15"], rec[t]["s60"]
        o = {"n":len(a),
             "sigma15_pct":{"p10":round(q(a,.1)*100,3),"p50":round(q(a,.5)*100,3),
                            "p90":round(q(a,.9)*100,3)} if a else None,
             "sigma60_pct":{"p10":round(q(b,.1)*100,3),"p50":round(q(b,.5)*100,3),
                            "p90":round(q(b,.9)*100,3)} if b else None}
        if ratio[t]:
            o["roundtrip_cost_in_sigma15"]={"p10":round(q(ratio[t],.1),2),
                "p50":round(q(ratio[t],.5),2),"p90":round(q(ratio[t],.9),2),
                "share_cost_gt_1sigma":round(sum(1 for x in ratio[t] if x>1)/len(ratio[t]),3),
                "share_cost_gt_2sigma":round(sum(1 for x in ratio[t] if x>2)/len(ratio[t]),3)}
        if a:
            m15 = q(a,.5)
            o["tp_sl_grid_pct_at_median_sigma15"]={f"{k}sigma":round(k*m15*100,3)
                                                   for k in (1,1.5,2,3)}
        return o
    dump("e_sigma_at_event", {
        "diagnostic":"(e) 이벤트 시점 σ15·σ60 실측 분포 → TP/SL·비용 산수 실측치 교체",
        "no_forward_returns": NO_FWD, "venue":venue,
        "sigma_definition":"σ15 = 후행 7일(672봉) 15분 수익률 표준편차, "
                           "σ60 = 같은 창의 1시간 종가 168점 수익률 표준편차",
        "PROXY_WARNING":"이 σ는 이벤트 '직전'까지의 실측치다. 진입 이후 실현 σ는 "
                        "확증 예산이라 조회하지 않았다. TP/SL 산수에 쓰는 순간 "
                        "'후행 σ ≈ 선행 σ' 가정을 얹는 것이며, 그 가정은 여기서 검증되지 않았다.",
        "A":blk("A"), "B":blk("B")})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default="a,a2,b,c,d,e")
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    for s in a.steps.split(","):
        A.log(f"=== step {s} ===")
        {"a":step_a,"a2":step_a2,"b":step_b,"c":step_c,"d":step_d,"e":step_e}[s.strip()]()

if __name__ == "__main__":
    main()
