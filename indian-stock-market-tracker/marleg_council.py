"""
marleg_council.py — the Trade Cockpit's ENGINE COUNCIL.

Routes a candidate trade through every engine, lets them VOTE, surfaces the CONFLICTS (who argues against),
and returns a CONSENSUS go/no-go. Honest design:
  • It is a VALIDATED-EDGE-WEIGHTED vote — NOT equal democracy, NOT LLMs literally debating. Macro/timing/
    don't-chase carry the heaviest weight and can VETO; folklore signals (ICT/FVG/fade) get NO vote.
  • Engines are DIVERSE (timing · regime · trend · momentum · don't-chase · levels · R:R · vol), so consensus
    means real agreement, not the same signal echoed. Correlated agreement would overstate conviction.

Read-only decision-support. The council decides WHETHER; the cockpit gives the plan; YOU place the trade.

  python marleg_council.py DRREDDY LONG
"""
import datetime as dt
import time as _time

import marleg_data as md
import marleg_session_gate as sg
import marleg_cockpit as cp

# Shareholding (promoter/institutional/retail) via yfinance — quarterly, lumps FII+DII into "institutions"
# (no clean FII-vs-DII split without a BSE scrape). Memoised 1h (it only changes quarterly).
_OWN = {}


def _ownership(sym):
    now = _time.time()
    if sym in _OWN and (now - _OWN[sym][0]) < 3600:
        return _OWN[sym][1]
    out = {"promoter": None, "institutional": None, "retail": None, "src": None}
    try:
        import yfinance as yf
        info = yf.Ticker(sym + ".NS").info
        ins = info.get("heldPercentInsiders"); inst = info.get("heldPercentInstitutions")
        if ins is not None and inst is not None:
            out["promoter"] = round(ins * 100, 1); out["institutional"] = round(inst * 100, 1)
            out["retail"] = round(max(0.0, 1 - ins - inst) * 100, 1)
            out["src"] = "yfinance · promoter≈insiders, institutional≈FII+DII (quarterly)"
    except Exception:
        pass
    _OWN[sym] = (now, out)
    return out


def _rsi(c, n=14):
    d = c.diff(); up = d.clip(lower=0); dn = -d.clip(upper=0)
    ag = up.ewm(alpha=1 / n, adjust=False).mean(); al = dn.ewm(alpha=1 / n, adjust=False).mean()
    rs = ag / al.replace(0, 1e-9)
    return float((100 - 100 / (1 + rs)).iloc[-1])


def _v(name, score, weight, reason, tag):
    return {"engine": name, "score": score, "weight": weight, "reason": reason, "tag": tag}


def council(sym, side="LONG", capital=100000.0, risk=1.0, profile="normal"):
    sym = (sym or "").upper().strip(); side = (side or "LONG").upper(); long = side == "LONG"
    daily = md.candles(sym, 1440, 260)
    if daily is None or len(daily) < 60:
        return {"ok": False, "sym": sym, "error": "insufficient history"}
    c = daily["close"].dropna(); spot = float(c.iloc[-1])
    ma20 = float(c.iloc[-20:].mean()); ma50 = float(c.iloc[-50:].mean())
    rsi = _rsi(c); hi52 = float(c.iloc[-252:].max()) if len(c) >= 252 else float(c.max())
    dist = (spot / hi52 - 1) * 100
    ret20 = (spot / float(c.iloc[-21]) - 1) * 100 if len(c) > 21 else 0.0
    g = sg.gate(); sess = g.get("session", {}); reg = g.get("regime", {})
    ck = cp.cockpit(sym, side, None, capital, risk, profile=profile)
    eng = []

    # 1 — GATE / timing (hard)
    gate_state = sess.get("gate")
    gv = 2 if gate_state == "GREEN" else (-1 if gate_state == "AMBER" else -2)
    eng.append(_v("Gate · timing", gv, 3.0, sess.get("label", ""),
                  "GO" if gate_state == "GREEN" else ("WAIT" if gate_state == "AMBER" else "NO")))

    # 2 — MACRO regime (NIFTY vs 50DMA) — bull-gating is validated
    nf = md.candles("NIFTY", 1440, 80); nfc = nf["close"].dropna() if nf is not None else c
    bull = float(nfc.iloc[-1]) > float(nfc.iloc[-50:].mean())
    eng.append(_v("Macro regime", (2 if bull else -2) * (1 if long else -1), 3.0,
                  "NIFTY > 50DMA — bull, deploy" if bull else "NIFTY < 50DMA — bear, stand aside",
                  "GO" if bull == long else "NO"))

    # 3 — TREND (name vs 20/50DMA) — momentum is validated
    up = spot > ma20 and spot > ma50; dnt = spot < ma20 and spot < ma50
    eng.append(_v("Trend", (2 if up else (-2 if dnt else 0)) * (1 if long else -1), 2.5,
                  f"{'above' if up else 'below' if dnt else 'mixed'} 20/50DMA (₹{round(ma20)}/₹{round(ma50)})",
                  "GO" if (up and long) else ("NO" if (up and not long) else "·")))

    # 4 — DON'T-CHASE (validated veto)
    extended = (rsi >= 75) or (dist >= -4)
    eng.append(_v("Don't-chase", (-2 if (extended and long) else (1 if long else 0)), 3.0,
                  f"RSI {round(rsi)} · {round(dist,1)}% from 52w-high" + (" — EXTENDED" if extended else " — room"),
                  "NO" if (extended and long) else "GO"))

    # 5 — OVERNIGHT bias (US + VIX, validated)
    sig = reg.get("signal", "NEUTRAL"); ob = 1 if "CALL" in sig else (-1 if "PUT" in sig else 0)
    ob = ob if long else -ob
    eng.append(_v("Overnight bias", ob * 2, 2.0, f"{sig} ({reg.get('bucket','—')})",
                  "GO" if ob > 0 else ("NO" if ob < 0 else "·")))

    # 6 — MOMENTUM health
    healthy = (50 <= rsi <= 72) and ret20 > 0
    eng.append(_v("Momentum", (2 if healthy else (-1 if ret20 < 0 else 0)) * (1 if long else -1), 2.0,
                  f"20d {round(ret20,1)}% · RSI {round(rsi)}", "GO" if (healthy and long) else "·"))

    # 7 — LEVELS / entry (cockpit pivots)
    lvl = (ck.get("levels") or {}) if ck.get("ok") else {}
    pp = lvl.get("PP", spot); s2 = lvl.get("S2", 0)
    near_sup = bool(lvl) and (s2 < spot <= pp)
    eng.append(_v("Levels/entry", (1 if near_sup else 0), 1.5,
                  "in the lower half vs pivot — better long entry" if near_sup else "mid/upper range vs pivot",
                  "GO" if near_sup else "·"))

    # 8 — RISK:REWARD (cockpit)
    rr = ck.get("blended_rr", 0) if ck.get("ok") else 0
    eng.append(_v("Risk:Reward", (2 if rr >= 2 else (1 if rr >= 1.3 else -1)), 1.5, f"blended {rr}:1",
                  "GO" if rr >= 2 else ("·" if rr >= 1.3 else "NO")))

    # 9 — VIX band
    vix = reg.get("vix"); vb = 1 if (vix and vix < 20) else (-1 if (vix and vix >= 28) else 0)
    eng.append(_v("VIX band", vb, 1.0, f"VIX {vix}", "GO" if vb > 0 else ("NO" if vb < 0 else "·")))

    # 10 — MOVE-POTENTIAL (amplitude — can it actually move intraday? the 3–8% filter)
    atr_pct = (ck.get("atr", 0) / spot * 100) if (ck.get("ok") and spot) else 0.0
    mp = 1 if atr_pct >= 2.5 else (0 if atr_pct >= 1.2 else -1)
    eng.append(_v("Move-potential", mp, 1.5,
                  f"ATR {round(atr_pct,1)}%/day — " + ("can run (3–8% reach)" if atr_pct >= 2.5 else
                  "moderate amplitude" if atr_pct >= 1.2 else "too quiet for an intraday move"),
                  "GO" if mp > 0 else ("NO" if mp < 0 else "·")))

    # 11 — PATTERN (validated: bullish reversal / 5-day breakout work; bearish patterns are folklore → no vote)
    h = daily["high"]
    brk = spot >= float(h.iloc[-6:-1].max())
    rev = len(c) >= 3 and float(c.iloc[-1]) > float(c.iloc[-2]) and float(c.iloc[-2]) < float(c.iloc[-3])
    psc = (2 if brk else (1 if rev else 0)) * (1 if long else -1)
    eng.append(_v("Pattern", psc, 1.5, ("5-day breakout" if brk else "bullish reversal" if rev else "no setup"),
                  "GO" if (psc > 0 and long) else "·"))

    # 12 — CAN SLIM (O'Neil composite: N/S/L/I/M computable; C/A earnings sparse → flagged unverified)
    own = _ownership(sym)
    inst_pct = own.get("institutional") or 0
    nf3 = (float(nfc.iloc[-1]) / float(nfc.iloc[-64]) - 1) if len(nfc) >= 64 else 0.0
    st3 = (float(c.iloc[-1]) / float(c.iloc[-64]) - 1) if len(c) >= 64 else 0.0
    try:
        vol = daily["volume"]; demand = float(c.iloc[-1]) > float(c.iloc[-2]) and float(vol.iloc[-1]) > float(vol.iloc[-20:].mean())
    except Exception:
        demand = False
    cs = {"N": dist >= -8, "S": demand, "L": (ret20 > 0 and st3 > nf3), "I": inst_pct >= 25, "M": bull}
    npass = sum(1 for v in cs.values() if v)
    cs_sc = (2 if npass >= 4 else (1 if npass >= 3 else (0 if npass >= 2 else -1))) * (1 if long else -1)
    eng.append(_v("CAN SLIM", cs_sc, 1.5,
                  f"{npass}/5 legs [{','.join(k for k, v in cs.items() if v) or 'none'}] · C/A earnings unverified",
                  "GO" if (npass >= 4 and long) else ("·" if npass >= 2 else "NO")))

    # 13 — CORP EVENTS / SURVEILLANCE (ASM/GSM/default = distress VETO; fresh deal/order = weak catalyst)
    ce_distress = False; cev = None
    try:
        import marleg_corp_events as _ce
        cev = _ce.gate(sym); cesc = cev.get("score", 0)
        ce_distress = cesc <= -2
        eng.append(_v("Corp-events", cesc, 2.5, (cev.get("verdict", "") + " · " + cev.get("why", ""))[:95],
                      "NO" if cesc <= -2 else ("GO" if cesc > 0 else "·")))
    except Exception:
        pass

    short_note = None
    if not long:
        eng.append(_v("Edge prior", -2, 2.5, "shorting is a validated ANTI-edge in India (market drifts up)", "NO"))
        short_note = "Short side carries a structural penalty — shorting backtests negative at every horizon."

    W = sum(e["weight"] for e in eng) or 1.0
    sc = sum(e["score"] * e["weight"] for e in eng) / W

    # vetoes (the validated hierarchy: regime/timing > don't-chase > everything)
    veto = None
    if gate_state == "RED":
        veto = "market closed — no live session"
    elif ce_distress:
        veto = "corporate distress / exchange surveillance (ASM/GSM) flag"
    elif extended and long:
        veto = "name is EXTENDED — don't-chase veto"
    if veto:
        verdict, tone = "NO-GO", "bad"; sc = min(sc, -0.5)
    elif sc >= 1.0:
        verdict, tone = "STRONG GO", "good"
    elif sc >= 0.4:
        verdict, tone = "GO", "good"
    elif sc >= -0.3:
        verdict, tone = "SPLIT — WAIT", "warn"
    else:
        verdict, tone = "NO-GO", "bad"

    sign = 1 if sc >= 0 else -1
    conflicts = [e["engine"] for e in eng if (e["score"] * sign) < 0 and e["weight"] >= 1.5]

    # ── INVESTMENT-COMMITTEE NOTE (hedge-fund-style synthesis — honest, risk-first) ──
    go_eng = [e["engine"] for e in eng if e["score"] > 0 and e["weight"] >= 2]
    no_eng = [e["engine"] for e in eng if e["score"] < 0 and e["weight"] >= 1.5]
    drivers = [e["engine"] for e in eng if e["score"] > 0 and e["engine"] in
               ("Macro regime", "Trend", "Momentum", "Overnight bias", "Don't-chase")]
    conv = "LOW" if abs(sc) < 0.5 else ("MODERATE" if abs(sc) < 1.0 else "HIGH")
    if veto:
        bottom = f"Stand aside — {veto}. Bullish engines do NOT override a validated veto."
    elif verdict == "STRONG GO":
        bottom = f"Constructive — {len(go_eng)} weighted engines align with validated edges. Size per plan, respect the stop."
    elif verdict == "GO":
        bottom = "Mild positive — take it small; the edge is modest, not a conviction bet."
    elif "SPLIT" in verdict:
        bottom = "No consensus — WAIT. Forcing a split-decision trade is how a thin edge becomes a loss."
    else:
        bottom = "Pass — the weighted committee does not support the trade."
    ck_ok = ck.get("ok")
    memo = {
        "title": f"INVESTMENT-COMMITTEE NOTE — {sym} {side}",
        "recommendation": f"{verdict}  ·  conviction {conv}  ·  score {round(sc,2):+}",
        "thesis": (", ".join(go_eng) + f" support the {side.lower()}." if go_eng else "No engine builds a constructive case."),
        "case_against": (f"VETO — {veto}." if veto else (", ".join(no_eng) + " argue against." if no_eng else "No material objection.")),
        "edge_source": ("Return source: " + ", ".join(drivers) + " (validated edges)." if (drivers and not veto)
                        else "No clean edge here — a discipline / stand-aside call, not alpha."),
        "risk": (f"Stop {ck.get('stop')} (−1R ≈ ₹{ck.get('risk_rupees'):,}); {ck.get('qty')} sh = {risk}% of ₹{int(capital):,}. Max loss = the stop."
                 if ck_ok else "Cockpit plan unavailable."),
        "levels": (f"entry {ck.get('entry')} · stop {ck.get('stop')} · targets {'/'.join(str(t['px']) for t in ck.get('tps',[]))} · R:R {ck.get('blended_rr')}:1"
                   if ck_ok else "—"),
        "bottom_line": bottom, "conviction": conv,
    }
    memo["text"] = (memo["title"] + "\n" + "=" * 54 +
                    f"\nRECOMMENDATION : {memo['recommendation']}"
                    f"\nTHESIS         : {memo['thesis']}"
                    f"\nEDGE           : {memo['edge_source']}"
                    f"\nCASE AGAINST   : {memo['case_against']}"
                    f"\nLEVELS         : {memo['levels']}"
                    f"\nRISK / SIZING  : {memo['risk']}"
                    f"\nBOTTOM LINE    : {memo['bottom_line']}"
                    "\n(decision-support · edges modest & probabilistic · the PM — you — places the trade)")

    return {
        "memo": memo,
        "ok": True, "sym": sym, "side": side, "spot": round(spot, 2),
        "consensus_score": round(sc, 2), "verdict": verdict, "tone": tone, "veto": veto, "short_note": short_note,
        "engines": eng, "conflicts": conflicts,
        "n_go": sum(1 for e in eng if e["score"] > 0), "n_no": sum(1 for e in eng if e["score"] < 0),
        "n_neutral": sum(1 for e in eng if e["score"] == 0),
        "atr_pct": round(atr_pct, 1), "move_potential": ("HIGH" if atr_pct >= 2.5 else "MED" if atr_pct >= 1.2 else "LOW"),
        "ownership": own, "canslim": {"pass": npass, "legs": cs}, "corp_events": cev,
        "cockpit": {"entry": ck.get("entry"), "stop": ck.get("stop"), "rr": rr,
                    "tps": [t["px"] for t in (ck.get("tps") or [])]} if ck.get("ok") else None,
        "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S IST"),
        "note": "VALIDATED-EDGE-WEIGHTED consensus — macro/timing/don't-chase carry the weight + can VETO; folklore "
                "(ICT/FVG/fade) gets no vote. Diverse engines, so agreement = real, not one signal echoed.",
        "caveat": "Decision-support, not advice. Consensus cuts single-signal error but correlated engines can overstate "
                  "conviction — treat SPLIT as WAIT, a veto as final. The cockpit gives the plan; YOU place the trade.",
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sym = sys.argv[1] if len(sys.argv) > 1 else "DRREDDY"
    side = sys.argv[2] if len(sys.argv) > 2 else "LONG"
    r = council(sym, side)
    if not r.get("ok"):
        print(r); raise SystemExit
    mk = {"GO": "✓", "NO": "✗", "WAIT": "!", "·": "·"}
    print(f"\n  {r['sym']} {r['side']} · spot {r['spot']} · score {r['consensus_score']:+}")
    print(f"  ⮞ VERDICT: {r['verdict']}" + (f"  (veto: {r['veto']})" if r.get("veto") else ""))
    print(f"  votes: {r['n_go']} GO · {r['n_no']} NO · {r['n_neutral']} neutral" +
          (f"  | conflicts: {', '.join(r['conflicts'])}" if r["conflicts"] else ""))
    for e in r["engines"]:
        print(f"    [{mk.get(e['tag'],'·')}] {e['engine']:<16} w{e['weight']:<4} {e['score']:+}  — {e['reason']}")
