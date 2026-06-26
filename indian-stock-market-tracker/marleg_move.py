"""
marleg_move.py — MOVE-ATTRIBUTION "story" engine.

When a name spikes, this tells the STORY: how big the move was, on how much volume, and — the key
question the user asked ("was the AMBER spike foreign or retail?") — whether it was REAL accumulation
or just intraday churn.

HONEST DATA TRUTH (read this): India offers NO free per-stock intraday FII-vs-retail tape. You cannot
attribute a single intraday spike to "foreign vs retail". What you CAN do, and what this engine does:

  • DELIVERY %  (NSE EOD, the honest proxy) — deliverable qty / traded qty. HIGH delivery = buyers TOOK
    delivery → sticky, positional/investor money (often institutional-style). LOW delivery = positions
    squared off same day → intraday churn → speculative / trader-driven, NOT sticky. This is the single
    best free signal for "real accumulation vs a pop that fades".
  • VOLUME SURGE (rvol = today's vol / 20-day avg) — the fuel; >2× = something happened.
  • U/D ratio — 20-day up-volume / down-volume → accumulation vs distribution backdrop.
  • CATALYST — corp-events gate (deal / order / result / distress / ASM-GSM). Was there news, or is it pure flow?
  • OWNERSHIP — quarterly promoter / institutional / retail split (latest filing — context, not today's flow).
  • FII/DII — the WHOLE MARKET's net institutional flow that day (risk-on/off backdrop, not this stock).

story(sym) → a verdict + a plain-English narrative + the raw signals, for the cockpit "move story" box.
Read-only decision-support. Honest: delivery% is a proxy, not a true FII/retail tag — labelled as such.
"""
import time as _time
import datetime as dt
import io
import csv

import numpy as np
import requests

import marleg_data as md

_H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36", "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}

_sess = None
_sess_t = 0.0
_BHAV = {"t": 0.0, "data": None, "date": None}     # whole-market delivery map (one CSV, all stocks)
_FD = {"t": 0.0, "data": None}


def _session():
    global _sess, _sess_t
    if _sess is None or (_time.time() - _sess_t) > 900:
        s = requests.Session(); s.headers.update(_H)
        try:
            s.get("https://www.nseindia.com", timeout=10)
        except Exception:
            pass
        _sess, _sess_t = s, _time.time()
    return _sess


def _bhav():
    """The NSE security-wise bhavcopy (sec_bhavdata_full) for the latest available session — ONE CSV that
    carries DELIV_PER (delivery %) for every stock. The quote-equity API is Akamai-blocked from here, but
    this archive isn't. Parsed into {SYMBOL: {...}} and memoised 1h (it's EOD data)."""
    now = _time.time()
    if _BHAV["data"] is not None and (now - _BHAV["t"]) < 3600:
        return _BHAV["data"]
    s = _session()
    out, used = {}, None
    d = dt.date.today()
    for _ in range(6):                       # walk back to the latest session that has a file
        url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_" + d.strftime("%d%m%Y") + ".csv"
        try:
            r = s.get(url, headers={**_H, "Referer": "https://www.nseindia.com/"}, timeout=15)
            if r.status_code == 200 and "csv" in r.headers.get("content-type", "").lower():
                for row in csv.DictReader(io.StringIO(r.text)):
                    rr = {k.strip(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
                    if rr.get("SERIES") not in ("EQ", "BE", "BZ"):
                        continue
                    sym = rr.get("SYMBOL")
                    if sym:
                        out[sym.upper()] = rr
                used = d.isoformat()
                break
        except Exception:
            pass
        d -= dt.timedelta(days=1)
    _BHAV.update(t=now, data=out, date=used)
    return out


def delivery(sym):
    """Delivery % for the last completed session (deliverable qty / traded qty) from the bhavcopy map."""
    rr = _bhav().get((sym or "").upper())
    if not rr:
        return None
    try:
        pct = float(rr.get("DELIV_PER"))
    except Exception:
        return None
    try:
        prev, close = float(rr.get("PREV_CLOSE")), float(rr.get("CLOSE_PRICE"))
        chg = (close / prev - 1) * 100 if prev else None
    except Exception:
        close = chg = None
    return {"deliv_pct": round(pct, 1), "traded_qty": rr.get("TTL_TRD_QNTY"), "deliv_qty": rr.get("DELIV_QTY"),
            "date": rr.get("DATE1") or _BHAV.get("date"), "close": close,
            "session_chg": round(chg, 2) if chg is not None else None}


def _live(sym):
    """Today's live intraday move from Groww (candles/bhavcopy are last-session). Best-effort."""
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        p = (g.quote(sym, segment="CASH", exchange="NSE").json().get("payload") or {})
        lp = p.get("last_price")
        return {"last": round(float(lp), 2) if lp is not None else None,
                "chg_pct": p.get("day_change_perc"), "vol": p.get("volume")}
    except Exception:
        return {}


def fii_dii():
    """Market-wide FII + DII net cash flow for the latest day (₹ Cr). Memoised 1h. The whole tape, not per-stock."""
    now = _time.time()
    if _FD["data"] is not None and (now - _FD["t"]) < 3600:
        return _FD["data"]
    out, s = None, _session()
    try:
        rows = s.get("https://www.nseindia.com/api/fiidiiTradeReact",
                     headers={**_H, "Referer": "https://www.nseindia.com/"}, timeout=12).json()
        fii = dii = None
        date = None
        for r in (rows or []):
            cat = (r.get("category") or "").upper()
            date = r.get("date") or date
            try:
                net = float(str(r.get("netValue")).replace(",", ""))
            except Exception:
                net = None
            if "FII" in cat or "FPI" in cat:
                fii = net
            elif "DII" in cat:
                dii = net
        if fii is not None or dii is not None:
            out = {"fii_net": fii, "dii_net": dii, "date": date}
    except Exception:
        pass
    _FD.update(t=now, data=out)
    return out


def _vol_stats(sym):
    """Last-session move + volume context from the daily panel (Groww/yfinance)."""
    df = md.candles(sym, 1440, 60)
    if df is None or len(df) < 21:
        return None
    c = df["close"].astype(float)
    v = df["volume"].astype(float)
    chg = float(c.iloc[-1] / c.iloc[-2] - 1) * 100 if len(c) > 1 else 0.0
    ret5 = float(c.iloc[-1] / c.iloc[-6] - 1) * 100 if len(c) > 5 else 0.0
    avg20 = float(v.iloc[-21:-1].mean()) if len(v) >= 21 else float(v.mean())   # prior-20d avg, excl. last bar
    rvol = float(v.iloc[-1] / avg20) if avg20 > 0 else None
    d = np.sign(c.diff())
    upv = float(v.where(d > 0, 0.0).rolling(20).sum().iloc[-1])
    dnv = float(v.where(d < 0, 0.0).rolling(20).sum().iloc[-1])
    ud = (upv / dnv) if dnv > 0 else None
    return {"price": round(float(c.iloc[-1]), 2), "chg": round(chg, 2), "ret5": round(ret5, 2),
            "rvol": round(rvol, 2) if rvol else None, "ud": round(ud, 2) if ud else None,
            "last_vol": int(v.iloc[-1]), "avg20_vol": int(avg20)}


def story(sym):
    sym = (sym or "").upper().strip()
    vs = _vol_stats(sym)
    if not vs:
        return {"ok": False, "sym": sym, "error": "insufficient history for " + sym}
    dl = delivery(sym)
    fd = fii_dii()
    cat = None
    try:
        import marleg_corp_events as ce
        cat = ce.gate(sym)
    except Exception:
        pass
    own = None
    try:
        import marleg_council as cc
        own = cc._ownership(sym)
    except Exception:
        pass

    live = _live(sym)
    live_chg = live.get("chg_pct")
    sess_chg = vs["chg"]
    chg = float(live_chg) if live_chg is not None else sess_chg     # lead with today's live move when we have it
    rvol = vs["rvol"]; deliv = (dl or {}).get("deliv_pct")
    deliv_date = (dl or {}).get("date")
    up = chg >= 0

    # magnitude
    if rvol is None:
        vol_tag = "volume n/a"
    elif rvol >= 2.5:
        vol_tag = f"a huge {rvol}× volume surge"
    elif rvol >= 1.5:
        vol_tag = f"{rvol}× above-average volume"
    elif rvol >= 0.8:
        vol_tag = f"{rvol}× ~normal volume"
    else:
        vol_tag = f"thin {rvol}× volume"

    # delivery quality → the honest "who" proxy
    if deliv is None:
        who = "delivery % unavailable (NSE)"; who_lean = "unknown"
    elif deliv >= 60:
        who = f"{deliv}% delivery — buyers TOOK delivery (sticky, positional/investor money, often institutional-style)"
        who_lean = "delivery-based (sticky)"
    elif deliv >= 45:
        who = f"{deliv}% delivery — healthy real participation, not pure intraday"
        who_lean = "real participation"
    elif deliv >= 30:
        who = f"{deliv}% delivery — mixed intraday + delivery"
        who_lean = "mixed"
    else:
        who = f"{deliv}% delivery — LOW, mostly intraday churn (squared off same day → speculative / trader-driven, not sticky)"
        who_lean = "intraday churn (speculative)"

    # catalyst
    cs = (cat or {}).get("score", 0)
    if cat and cs < 0:
        catalyst = f"⚠ {cat['verdict']} — {cat['why']}"; ctone = "bad"
    elif cat and cs > 0:
        catalyst = f"📣 {cat['verdict']} — {cat['why']}"; ctone = "good"
    elif cat:
        catalyst = "no fresh exchange filing — the move is FLOW-driven (positioning/sentiment), not a news event"; ctone = "normal"
    else:
        catalyst = None; ctone = "normal"

    # verdict
    if cs < 0:
        verdict = "Surveillance / distress flag — be careful"
    elif up and who_lean.startswith("delivery"):
        verdict = "Real accumulation — delivery-backed up-move"
    elif up and who_lean == "intraday churn (speculative)":
        verdict = "Speculative pop — low delivery, likely to fade"
    elif up and who_lean in ("real participation", "mixed"):
        verdict = "Genuine demand — decent delivery behind the move"
    elif (not up) and who_lean.startswith("delivery"):
        verdict = "Real distribution — delivery-backed selling"
    elif not up:
        verdict = "Intraday selling — low conviction"
    else:
        verdict = "Mixed — no clear conviction"

    if cs < 0:
        tone = "bad"
    elif up and (deliv or 0) >= 50:
        tone = "good"
    elif (not up) and (deliv or 0) >= 50:
        tone = "bad"
    elif abs(chg) >= 2 and (deliv if deliv is not None else 100) < 35:
        tone = "warn"
    else:
        tone = "normal"

    # narrative
    if live_chg is not None:
        lead = f"{sym} is {chg:+.1f}% TODAY (live). Last completed session ({deliv_date or 'prior'}) was {sess_chg:+.1f}% on {vol_tag}"
    else:
        lead = f"{sym} moved {sess_chg:+.1f}% last session ({deliv_date or ''}) on {vol_tag}"
    lead += f" (5-day {vs['ret5']:+.1f}%)." if vs.get("ret5") is not None else "."
    bits = [lead,
            who + f" [from {deliv_date}]." if deliv else who + ".",
            catalyst or ""]
    if vs.get("ud") is not None:
        backdrop = "accumulation" if vs["ud"] >= 1.1 else "distribution" if vs["ud"] <= 0.9 else "balanced"
        bits.append(f"20-day up/down-volume {vs['ud']} → {backdrop} backdrop.")
    if own and own.get("institutional") is not None:
        bits.append(f"Latest filing: institutions {own['institutional']}%, retail {own['retail']}%, promoter {own['promoter']}% "
                    f"(quarterly — ownership context, not today's flow).")
    if fd and (fd.get("fii_net") is not None or fd.get("dii_net") is not None):
        ff = fd.get("fii_net") or 0; dd = fd.get("dii_net") or 0
        bits.append(f"Whole-market that day: FII net ₹{ff:,.0f} Cr, DII net ₹{dd:,.0f} Cr ({fd.get('date')}) — "
                    f"foreign risk-{'ON' if ff > 0 else 'OFF'} (entire market, not this stock).")
    story_text = " ".join(b for b in bits if b)

    return {
        "ok": True, "sym": sym, "verdict": verdict, "tone": tone, "headline": f"{sym} {chg:+.1f}% · {vol_tag}",
        "story": story_text, "who_lean": who_lean,
        "signals": {"chg": round(chg, 2), "live_chg": round(float(live_chg), 2) if live_chg is not None else None,
                    "session_chg": sess_chg, "ret5": vs.get("ret5"), "rvol": rvol, "ud": vs.get("ud"),
                    "deliv_pct": deliv, "deliv_date": deliv_date,
                    "price": live.get("last") or vs["price"], "last_vol": vs["last_vol"], "avg20_vol": vs["avg20_vol"]},
        "catalyst": cat, "ownership": own, "fii_dii": fd,
        "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
        "caveat": "India has NO free per-stock intraday FII-vs-retail tape — you cannot label a single spike "
                  "'foreign' or 'retail'. DELIVERY % is the honest proxy: high delivery = sticky/positional "
                  "(investor) money, low delivery = intraday churn (speculative). FII/DII shown is the whole "
                  "market that day. Read-only decision-support.",
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    for u in (sys.argv[1:] or ["AMBER"]):
        r = story(u)
        if not r.get("ok"):
            print(f"\n  {u}: {r.get('error')}"); continue
        print(f"\n═══ {r['sym']} · {r['verdict']} ({r['tone']}) ═══")
        print(f"  {r['headline']}")
        s = r["signals"]
        print(f"  chg {s['chg']:+}%  rvol {s['rvol']}×  U/D {s['ud']}  delivery {s['deliv_pct']}% ({s['deliv_date']})")
        print(f"\n  {r['story']}")
        print(f"\n  ⓘ {r['caveat']}")
