#!/bin/bash
# 완료 후 1회만 실행 — 요약 JSON에서 판정에 필요한 필드만 압축 출력
cd /home/user/daw353
for f in audit_out/summary_*.json; do
  python3 - "$f" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
k=["label","symbols","days_covered","window_utc","total_events","zero_streak_days",
   "share_days_with_any_signal","F5_check(A_month<10)","F5_partial_months_excluded",
   "max_simultaneous_same_bar","filled_bar_ratio","symbols_failed","EVIDENCE"]
print(json.dumps({x:d[x] for x in k if x in d},ensure_ascii=False,separators=(",",":")))
print("  per_day:",json.dumps(d.get("per_day",{}),separators=(",",":")))
print("  monthly_A:",json.dumps(d.get("monthly_A_counts",{}),separators=(",",":")))
PY
done
echo "--- cost_matrix (n, 시간대 커버리지, 분위)"
python3 - <<'PY'
import csv,statistics
r=list(csv.DictReader(open("audit_out/cost_matrix.csv")))
rt=sorted(float(x["roundtrip_lower_bound"]) for x in r)
q=lambda p: rt[int(len(rt)*p)]
print(f"symbols={len(r)} n_per_sym={r[0]['n']} hours_utc={r[0]['hours_utc']} hours_kst={r[0]['hours_kst']}")
print(f"roundtrip_lower_bound  p10={q(.10)*100:.3f}%  median={q(.50)*100:.3f}%  p90={q(.90)*100:.3f}%  max={rt[-1]*100:.3f}%")
best=sorted(r,key=lambda x:float(x["roundtrip_lower_bound"]))[:5]
worst=sorted(r,key=lambda x:float(x["roundtrip_lower_bound"]))[-5:]
print("best :",", ".join(f"{x['symbol']} {float(x['roundtrip_lower_bound'])*100:.3f}%" for x in best))
print("worst:",", ".join(f"{x['symbol']} {float(x['roundtrip_lower_bound'])*100:.3f}%" for x in worst))
PY
echo "--- 산출물"; ls -la audit_out/
