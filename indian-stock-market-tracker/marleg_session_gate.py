"""
marleg_session_gate.py — the desk's FIRST stop before any trade: the GO / NO-GO session gate,
the overnight-regime read, the opening-range "daily volatility" the pod waits for, and a shared
pre-trade discipline CHECKLIST (the items the pod can verify + the items YOU confirm).

The gate (a background colour, by design):
  • RED   — NO-GO   : market closed / weekend / NSE holiday / pre-market / after-hours.
  • AMBER — WAIT    : pre-open auction, the first 15–30 min DATA-COLLECTION window (pod is still
                      calibrating — no fresh entries), and the closing hour (manage/exit only).
  • GREEN — GO      : collection window passed, mid-session — edges are active, trade per checklist.

Overnight regime — the validated signal (1,017 sessions, MON100 = the India-listed Nasdaq-100 ETF as the
prior-US proxy, since it prices the prior US close):
  US-down>0.5% + VIX-up>4%  -> NIFTY mean -1.20% · 64% chance of a >1% drop (vs 9% baseline) · 14% >2%  [strong PUT-bias]
  US-down    + VIX-up        -> mean -0.83% · 43% chance of a >1% drop                                   [mild PUT-bias]
  US-up      + VIX-down      -> mean +0.62% · 0% chance of a >1% drop (worst -0.7%)                       [clean CALL-bias]
On an EXPIRY day the US cue washes out (max-pain pin dominates) — fade toward the pin, don't chase a breakout.

Read-only, decision-support — NOT advice. The gate is discipline, not a prediction.

  python marleg_session_gate.py
"""
import datetime as dt
import time as _time

import marleg_data as md
import marleg_vol as mv


IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# The prior US session ("the day before in the US market") via yfinance — the actual US cash close (final
# hours before NSE opens) + live US futures (where it's pointing NOW). MON100 (the India-listed Nasdaq ETF)
# LAGS a full session intraday and is illiquid, so it is a stale last-resort fallback only. Memoised 90s.
_US_MEMO = {"t": 0.0, "data": None}


def _us_overnight():
    now = _time.time()
    if _US_MEMO["data"] is not None and (now - _US_MEMO["t"]) < 90:
        return _US_MEMO["data"]
    data = {"ndx": None, "spx": None, "ixic": None, "nq_fut": None, "es_fut": None, "src": None, "stale": False}
    try:
        import yfinance as yf

        def _ret(t):
            try:
                d = yf.Ticker(t).history(period="6d")
                c = d["Close"].dropna() if d is not None else None
                if c is None or len(c) < 2:
                    return None
                return round(float(c.iloc[-1] / c.iloc[-2] - 1) * 100, 2)
            except Exception:
                return None

        data["ndx"] = _ret("^NDX")          # Nasdaq-100 = MON100's underlying → matches the backtest's bucket thresholds
        data["spx"] = _ret("^GSPC")         # S&P 500 = the broad "US market"
        data["ixic"] = _ret("^IXIC")        # Nasdaq Composite (context)
        data["nq_fut"] = _ret("NQ=F")       # Nasdaq futures — real-time pointer
        data["es_fut"] = _ret("ES=F")       # S&P futures — real-time pointer
        if data["ndx"] is not None or data["spx"] is not None:
            data["src"] = "yfinance · US cash close (last session) + futures (live)"
    except Exception:
        pass
    if data["ndx"] is None and data["spx"] is None:        # yfinance unreachable → stale MON100 daily fallback
        try:
            us = md.candles("MON100", 1440, 14)
            uc = us["close"].dropna() if us is not None else None
            if uc is not None and len(uc) > 1:
                data["ndx"] = round(float(uc.pct_change().iloc[-1] * 100), 2)
                data["src"] = "MON100 daily (yfinance down) — may LAG one session"
                data["stale"] = True
        except Exception:
            pass
    _US_MEMO.update(t=now, data=data)
    return data


# Live US equity FUTURES (CME Globex) — these trade overnight in US terms, which is exactly India's session
# (≈23:45–06:00 ET = 09:15–15:30 IST). So ES/NQ tick LIVE while NIFTY trades: a real-time read of where the
# US is pointing RIGHT NOW, not a stale overnight close. Memoised 30s (it's live).
_FUT_MEMO = {"t": 0.0, "data": None}


def _sp_futures():
    now = _time.time()
    if _FUT_MEMO["data"] is not None and (now - _FUT_MEMO["t"]) < 30:
        return _FUT_MEMO["data"]
    out = {"items": [], "headline": None, "es": None, "nq": None, "ok": False, "src": None,
           "note": "US equity futures trade overnight on CME Globex — LIVE through India's session "
                   "(≈23:45–06:00 ET = 09:15–15:30 IST). The real-time US risk read while NIFTY trades."}
    try:
        import yfinance as yf
        for sym, name in [("ES=F", "S&P 500"), ("NQ=F", "Nasdaq 100"), ("YM=F", "Dow"), ("RTY=F", "Russell 2000")]:
            try:
                h = yf.Ticker(sym).history(period="2d")["Close"].dropna()
                if len(h) >= 2:
                    last = float(h.iloc[-1]); prev = float(h.iloc[-2])
                    chg = (last / prev - 1) * 100 if prev else 0.0
                    out["items"].append({"sym": sym, "name": name, "last": round(last, 2), "chg_pct": round(chg, 2)})
                    if sym == "ES=F":
                        out["es"] = round(chg, 2)
                    if sym == "NQ=F":
                        out["nq"] = round(chg, 2)
            except Exception:
                pass
        if out["items"]:
            out["headline"] = out["es"] if out["es"] is not None else out["items"][0]["chg_pct"]
            out["ok"] = True
            out["src"] = "yfinance · CME Globex (live)"
    except Exception:
        pass
    _FUT_MEMO.update(t=now, data=out)
    return out

PREOPEN_MIN = 9 * 60            # 09:00  pre-open auction begins
OPEN_MIN    = 9 * 60 + 15       # 09:15  continuous session
COLLECT_MIN = 30               # first 30 min = data-collection window (15-min interim checkpoint)
CLOSING_MIN = 15 * 60          # 15:00  closing hour begins
CLOSE_MIN   = 15 * 60 + 30      # 15:30  session ends


def _now_ist():
    return dt.datetime.now(dt.timezone.utc).astimezone(IST)


def _next_session(d):
    nd = d + dt.timedelta(days=1)
    for _ in range(12):
        if mv._is_trading_day(nd):
            return nd
        nd += dt.timedelta(days=1)
    return nd


def _session(now):
    d = now.date()
    mn = now.hour * 60 + now.minute
    trading = mv._is_trading_day(d)
    holiday = (d.weekday() < 5) and (d.isoformat() in mv.NSE_HOLIDAYS)
    mso = mn - OPEN_MIN
    if not trading:
        nxt = _next_session(d)
        return {"phase": "HOLIDAY" if holiday else "WEEKEND", "gate": "RED",
                "label": ("NSE holiday — market closed" if holiday else "weekend — market closed"),
                "mins_since_open": None, "next_session": nxt.isoformat()}
    if mn < PREOPEN_MIN:
        return {"phase": "PRE", "gate": "RED", "label": "pre-market — NSE opens 09:15 IST",
                "mins_since_open": mso}
    if mn < OPEN_MIN:
        return {"phase": "PREOPEN", "gate": "AMBER", "label": "pre-open auction (09:00–09:15) — no live trades yet",
                "mins_since_open": mso}
    if mn < OPEN_MIN + COLLECT_MIN:
        return {"phase": "COLLECT", "gate": "AMBER",
                "label": "data-collection window — pod is calibrating, NO fresh entries", "mins_since_open": mso}
    if mn < CLOSING_MIN:
        return {"phase": "GO", "gate": "GREEN", "label": "session live — edges active (trade per the checklist)",
                "mins_since_open": mso}
    if mn < CLOSE_MIN:
        return {"phase": "CLOSING", "gate": "AMBER",
                "label": "closing hour (15:00–15:30) — manage / exit, no fresh lottery entries", "mins_since_open": mso}
    nxt = _next_session(d)
    return {"phase": "POST", "gate": "RED", "label": "after-hours — market closed (15:30)",
            "mins_since_open": mso, "next_session": nxt.isoformat()}


def _live_and_vix():
    """One Groww client: live NIFTY/BANKNIFTY tape (last + today's O/H/L from the quote — the intraday CANDLE
    feed lags a session, but the live quote carries today's real range) + India VIX (level)."""
    live, vix = {}, None
    try:
        import groww_client as gc
        g = gc.GrowwClient(); g.token()
        for sym in ("NIFTY", "BANKNIFTY"):
            try:
                p = (g.quote(sym, segment="CASH", exchange="NSE").json().get("payload") or {})
                lp = p.get("last_price")
                ch = p.get("day_change_perc")
                oh = p.get("ohlc") if isinstance(p.get("ohlc"), dict) else {}
                prev = oh.get("close")                       # ohlc.close = PREVIOUS close (for day-change)
                if ch is None and lp and prev:
                    ch = (float(lp) / float(prev) - 1) * 100
                if lp is not None:
                    row = {"last": round(float(lp), 2), "chg_pct": None if ch is None else round(float(ch), 2)}
                    for k in ("open", "high", "low"):         # today's real-time O/H/L
                        if oh.get(k) is not None:
                            row[k] = round(float(oh[k]), 2)
                    if prev is not None:
                        row["prev_close"] = round(float(prev), 2)
                    live[sym] = row
            except Exception:
                pass
        try:
            p = (g.ltp("INDIAVIX", exchange="NSE").json().get("payload") or {})
            for v in p.values():
                if v:
                    vix = float(v); break
        except Exception:
            pass
    except Exception:
        pass
    if vix is None:
        vix = mv.india_vix()
    return live, vix


def _regime(vix):
    out = {"signal": "NEUTRAL", "tone": "normal", "strength": "—", "bucket": "—"}
    us = _us_overnight()
    broad = us.get("spx")                       # S&P 500 = the broad "US market"
    tech = us.get("ixic")                        # Nasdaq Composite = the tech/risk read
    if broad is None:                            # stale-fallback path returns only ndx
        broad = us.get("ndx")
    spf = _sp_futures()                          # LIVE futures (ticking through India's session)
    fut = spf.get("es") if spf.get("es") is not None else spf.get("nq")
    # VIX change vs prior close
    vix_chg = None
    try:
        vx = md.candles("INDIAVIX", 1440, 14)
        vc = vx["close"].dropna() if vx is not None else None
        if vc is not None and len(vc) > 1:
            prev = float(vc.iloc[-2])
            base = vix if vix is not None else float(vc.iloc[-1])
            if prev:
                vix_chg = (base / prev - 1) * 100
    except Exception:
        pass
    out.update({"us_prior_pct": None if broad is None else round(broad, 2),     # headline = broad market
                "us_tech_pct": tech, "us_spx_pct": broad, "us_ixic_pct": tech,
                "us_fut_pct": spf.get("nq"), "us_fut_es_pct": spf.get("es"),
                "us_src": us.get("src") or "US data unavailable", "us_stale": bool(us.get("stale")),
                "vix": None if vix is None else round(vix, 2),
                "vix_chg_pct": None if vix_chg is None else round(vix_chg, 1)})
    if broad is None or vix_chg is None:
        out["stat"] = "overnight inputs unavailable (need US close + VIX history)."
        return out
    # robust direction: broad-mild-down WITH tech-hard-down = risk-off (don't lean on one index)
    t = tech if tech is not None else broad
    down_hard = (broad < -0.5) or (t < -1.0)
    down = (broad < -0.2) or (t < -0.5)
    up_clean = (broad > 0.3 and t > 0.3)
    up_lean = broad > 0.2
    vix_up = vix_chg > 2
    vix_up_strong = vix_chg > 4
    # live-futures amplifier note
    fnote = ""
    if fut is not None:
        if fut < -0.8:
            fnote = f" US futures {fut:+.1f}% NOW — risk-off deepening into the US open (overnight weakness extending)."
        elif fut > 0.8:
            fnote = f" US futures {fut:+.1f}% NOW — bid building back (overnight weakness stabilising)."
    if down_hard and vix_up_strong:
        out.update(signal="PUT-BIAS", strength="strong", tone="bad", bucket="US-down (broad/tech) + VIX-up>4%",
                   stat="Historically NIFTY mean -1.20% · 64% chance of a >1% drop (vs 9% baseline) · 14% >2%. "
                        "The fat-tail day a small OTM PUT is built for — but you're paying up (VIX rich)." + fnote)
    elif down and vix_up:
        out.update(signal="PUT-BIAS", strength="mild", tone="warn", bucket="US-down + VIX-up",
                   stat="Historically NIFTY mean -0.83% · 43% chance of a >1% drop. Downside respected; small OTM "
                        "put viable — most of the move is the open GAP, so size it as a lottery." + fnote)
    elif up_clean and vix_chg < -2:
        out.update(signal="CALL-BIAS", strength="clean", tone="good", bucket="US-up + VIX-down",
                   stat="Historically NIFTY mean +0.62% · 0% chance of a >1% drop (worst -0.7%). The clean call "
                        "day — near-zero upside tail risk." + fnote)
    elif up_lean:
        out.update(signal="CALL-LEAN", strength="mild", tone="good", bucket="US-up (VIX not falling)",
                   stat="US-up days: NIFTY mean +0.31%. Mild call lean — a VIX-up would flip it "
                        "(US-up + VIX-up = -0.13%). Watch the VIX print." + fnote)
    elif down:
        out.update(signal="PUT-LEAN", strength="mild", tone="warn", bucket="US-down (VIX not rising)",
                   stat="US-down days: NIFTY mean -0.28% (down 61%). Mild put lean — the edge sharpens to -0.83% "
                        "only once VIX also rises." + fnote)
    else:
        out.update(signal="NEUTRAL", strength="—", bucket="mixed / quiet",
                   stat="No clean overnight edge. The US lead is strongest on the open GAP (corr 0.50) — "
                        "flat/mixed here, so let the setup decide." + fnote)
    return out


def _opening(sess, live):
    """Today's volatility read from the LIVE quote O/H/L (the intraday candle feed lags a session)."""
    if sess["phase"] not in ("COLLECT", "GO", "CLOSING"):
        return None
    r = live.get("NIFTY") or {}
    o, hi, lo, last = r.get("open"), r.get("high"), r.get("low"), r.get("last")
    if not all(isinstance(x, (int, float)) for x in (o, hi, lo, last)) or not o:
        return None
    rng = (hi - lo) / o * 100
    pos = (last - lo) / (hi - lo) * 100 if hi > lo else 50.0     # 100 = at day-high, 0 = at day-low
    if rng >= 1.2:
        vread = "wide range — volatile tape; widen stops, the lottery has room but whippy"
    elif rng >= 0.6:
        vread = "normal range — tradeable; standard sizing"
    else:
        vread = "tight range — coiled / quiet; small moves, range-bound risk for naked options"
    return {"open": o, "last": last, "since_open_pct": round((last / o - 1) * 100, 2),
            "range_pct": round(rng, 2), "high": hi, "low": lo,
            "pos_in_range_pct": round(pos, 0), "read": vread}


def _vix_band(v):
    if v is None:
        return {"state": "warn", "detail": "VIX unavailable"}
    if v >= 28:
        return {"state": "fail", "detail": f"VIX {round(v,2)} — SHOCK band; stand aside or tiny lottery only"}
    if v >= 20:
        return {"state": "warn", "detail": f"VIX {round(v,2)} — elevated; options pricey, widen stops, smaller size"}
    if v <= 11:
        return {"state": "warn", "detail": f"VIX {round(v,2)} — very low; cheap premium but small moves (range risk)"}
    return {"state": "pass", "detail": f"VIX {round(v,2)} — normal, tradeable band"}


def _expiry_today(d):
    if not mv._is_trading_day(d):
        return {"is_expiry": False, "kind": None, "note": "non-trading day"}
    monthly = mv.monthly_expiry(d.year, d.month)
    if d == monthly:
        return {"is_expiry": True, "kind": "MONTHLY",
                "note": "MONTHLY F&O expiry — max-pain pin dominates; the US/overnight cue washes out. "
                        "Fade toward the pin, don't chase a breakout."}
    if d.weekday() == 1:                                     # Tuesday = NIFTY weekly expiry (post-SEBI)
        return {"is_expiry": True, "kind": "WEEKLY",
                "note": "NIFTY WEEKLY expiry (Tue) — pin/theta day. Lottery options decay fast into the close; "
                        "respect max-pain over the overnight signal."}
    return {"is_expiry": False, "kind": None, "note": "not an expiry day — directional setups behave normally."}


USER_CHECKLIST = [
    {"key": "u_dir",  "label": "Direction matches the overnight regime",
     "hint": "put on US-down+VIX-up · call on US-up+VIX-down · stand aside if NEUTRAL"},
    {"key": "u_edge", "label": "Setup is a VALIDATED edge — not a hunch",
     "hint": "call: 20d-high break in LOW vol (+1.90%) · put: strong US-down+VIX-up lottery · NOT blind shorting (-100% median)"},
    {"key": "u_chase", "label": "Not chasing an extended name at its ceiling",
     "hint": "beaten-down-recovering with room beats 52w-high froth (the overextension guard)"},
    {"key": "u_liq",  "label": "Liquid F&O instrument · tight spread",
     "hint": "NIFTY / BANKNIFTY / large-cap; thin options eat the edge on both entry and exit"},
    {"key": "u_size", "label": "Size = LOTTERY (small; full premium is a loss I can take)",
     "hint": "convex bet — capped, affordable loss · fat-tail upside · never a core position"},
    {"key": "u_stop", "label": "Stop + target written BEFORE entry",
     "hint": "stop ≈ max(prem×0.6, intrinsic) · target ≈ +1σ / next level · exit rule pre-committed"},
    {"key": "u_pin",  "label": "Expiry / pin checked",
     "hint": "on expiry day fade toward max-pain; lottery options decay fastest into the close"},
    {"key": "u_calm", "label": "I'm calm — not revenge-trading or chasing FOMO",
     "hint": "the gate is discipline; if you're forcing the trade, stand aside"},
]


def _auto_checklist(sess, reg, vixband, exp):
    items = []
    items.append({"key": "session", "label": "Session is GO (past the data-collection window)",
                  "state": "pass" if sess["phase"] == "GO" else ("warn" if sess["gate"] == "AMBER" else "fail"),
                  "detail": sess["label"]})
    mso = sess.get("mins_since_open")
    if mso is None or mso < 0:
        st, det = "fail", "market not open yet"
    elif mso >= COLLECT_MIN:
        st, det = "pass", f"{int(mso)} min since open — opening volatility captured"
    elif mso >= 15:
        st, det = "warn", f"{int(mso)} min — 15-min read in, 30-min confirm pending"
    else:
        st, det = "fail", f"{int(mso)} min — still in the opening-auction noise"
    items.append({"key": "data", "label": "Opening volatility collected (first 15–30 min)", "state": st, "detail": det})
    sig = reg.get("signal", "NEUTRAL")
    items.append({"key": "regime", "label": "Overnight regime identified (direction set)",
                  "state": "pass" if sig not in ("NEUTRAL",) else "warn",
                  "detail": f"{sig} · {reg.get('bucket','—')}"})
    items.append({"key": "vix", "label": "VIX in a tradeable band (not shock)", "state": vixband["state"],
                  "detail": vixband["detail"]})
    items.append({"key": "expiry", "label": "Expiry-pin awareness",
                  "state": "warn" if exp.get("is_expiry") else "pass",
                  "detail": exp.get("note")})
    return items


def _sr_sentiment(sym, r):
    """How the asset is behaving vs its support/resistance THROUGH the day -> a bull/bear read.
    Prior-day Classic pivots (from DAILY candles) + PDH/PDL as the levels; today's LIVE O/H/L + where price
    sits in the day range as the tell (the intraday candle feed lags, so today's range comes from the quote)."""
    try:
        if not r or r.get("open") is None or r.get("high") is None or r.get("low") is None:
            return None
        daily = md.candles(sym, 1440, 10)
        if daily is None or len(daily) < 2:
            return None
        today = _now_ist().date()
        prior = daily[[t.date() < today for t in daily.index]]      # strictly the last COMPLETE prior session
        if not len(prior):
            prior = daily
        pv = prior.iloc[-1]
        ph, pl, pc = float(pv["high"]), float(pv["low"]), float(pv["close"])
        PP = (ph + pl + pc) / 3.0
        R1 = 2 * PP - pl; S1 = 2 * PP - ph
        R2 = PP + (ph - pl); S2 = PP - (ph - pl)
        o, hi, lo, spot = r["open"], r["high"], r["low"], r["last"]
        broke_pdh, broke_pdl = hi > ph, lo < pl
        pos = (spot - lo) / (hi - lo) if hi > lo else 0.5          # 1 = at day-high (bullish), 0 = at day-low
        levels = [("S2", S2), ("S1", S1), ("PP", PP), ("R1", R1), ("R2", R2)]
        nearest = min(levels, key=lambda x: abs(spot - x[1]))
        if spot >= R1:
            zone = "above R1 — breakout zone"
        elif spot >= PP:
            zone = "PP→R1 — bullish half"
        elif spot >= S1:
            zone = "S1→PP — bearish half"
        else:
            zone = "below S1 — breakdown zone"
        score = 0
        score += 1 if spot > PP else -1
        score += 1 if spot > o else -1
        score += 1 if broke_pdh else 0
        score -= 1 if broke_pdl else 0
        score += 1 if pos > 0.6 else (-1 if pos < 0.4 else 0)      # holding near the high vs leaking to the low
        sent = "BULLISH" if score >= 2 else ("BEARISH" if score <= -2 else "NEUTRAL")
        tone = "good" if score >= 2 else ("bad" if score <= -2 else "normal")
        bits = [("above" if spot > PP else "below") + f" PP ₹{round(PP,1)}"]
        if broke_pdh:
            bits.append("broke prior-day HIGH")
        if broke_pdl:
            bits.append("broke prior-day LOW")
        bits.append(("holding near day-high" if pos > 0.6 else
                     "leaking to day-low" if pos < 0.4 else "mid-range"))
        return {"sym": sym, "sentiment": sent, "tone": tone, "score": score,
                "spot": round(spot, 2), "open": round(o, 2), "pos_in_range_pct": round(pos * 100, 0),
                "zone": zone, "nearest": nearest[0], "nearest_px": round(nearest[1], 1),
                "levels": {"S2": round(S2, 1), "S1": round(S1, 1), "PP": round(PP, 1),
                           "R1": round(R1, 1), "R2": round(R2, 1), "PDH": round(ph, 1), "PDL": round(pl, 1)},
                "broke_pdh": broke_pdh, "broke_pdl": broke_pdl, "read": " · ".join(bits)}
    except Exception:
        return None


def _next_day(nifty_chg, fut_es):
    """Validated next-day continuation tilt (backtest: 973 sessions). A risk-off day (NIFTY + US-fut both down)
    tilts NIFTY mildly down tomorrow (58-63% down); a risk-ON day tilts up more cleanly (65% up). The 'US itself
    down tomorrow' link is FALSE (US next-day is a coin-flip) — the tilt is India's own modest persistence."""
    if nifty_chg is None or fut_es is None:
        return None
    bd = nifty_chg < -0.1 and fut_es < -0.1
    bu = nifty_chg > 0.1 and fut_es > 0.1
    sd = nifty_chg < -0.5 and fut_es < -0.5
    su = nifty_chg > 0.5 and fut_es > 0.5
    if bd:
        if sd:
            return {"bias": "DOWN-LEAN", "tone": "warn",
                    "stat": "Risk-off day (NIFTY + S&P-fut both down >0.5%) → next-day NIFTY historically -0.33% "
                            "(63% down). A modest continuation tilt — small, not a lock."}
        return {"bias": "DOWN-LEAN", "tone": "warn",
                "stat": "Risk-off day (NIFTY + S&P-fut both down) → next-day NIFTY historically -0.19% (58% down). "
                        "Mild down tilt. NOTE: US itself is a coin-flip tomorrow — the tilt is India's own persistence."}
    if bu:
        if su:
            return {"bias": "UP-LEAN", "tone": "good",
                    "stat": "Risk-on day (both up >0.5%) → next-day NIFTY historically +0.24% (65% up). The cleaner "
                            "continuation — India's structural up-drift."}
        return {"bias": "UP-LEAN", "tone": "good",
                "stat": "Risk-on day (NIFTY + S&P-fut both up) → next-day NIFTY historically +0.21% (65% up). The "
                        "cleaner side of the thesis — up-continuation beats down-continuation in India."}
    return {"bias": "NEUTRAL", "tone": "normal",
            "stat": "Mixed US-fut / India today → no validated next-day continuation tilt. Let tomorrow's gate decide."}


def _put_analysis(reg, sr, live):
    """When NIFTY looks bearish, the mirror of the call read: a directional lottery PUT, put TARGETS (the
    supports it profits to), and the HEDGE framing for the user's long-dated calls. Honest: puts are folklore
    UNCONDITIONALLY in India (the up-drift) — they only earn their keep gated on the US-down+VIX-up signal."""
    sig = (reg or {}).get("signal", "") or ""
    bearish = "PUT" in sig
    r = (live or {}).get("NIFTY") or {}
    spot = r.get("last")
    if not spot:
        return None
    lv = (sr or {}).get("levels") or {}
    k = int(round(spot * 0.995 / 50.0) * 50)                    # ~0.5% OTM weekly put, NIFTY 50-pt strikes
    strength = (reg or {}).get("strength", "") or ""
    out = {"und": "NIFTY", "spot": round(spot, 2), "bearish": bearish, "signal": sig,
           "lottery": {"strike": k, "side": "PE", "otm_pct": round((spot - k) / spot * 100, 2),
                       "stat": (reg or {}).get("stat", ""),
                       "note": "slightly-OTM weekly PUT — size as a LOTTERY (full premium is the loss). Most of the "
                               "move is the overnight gap; on a strong US-down+VIX-up day the fat tail pays."}}
    # put targets = supports BELOW spot (it profits as price falls to them); when price has broken below the
    # pivots, reach for the lower levels — S3, today's low, round numbers — so there's always a target map.
    pp, pdh, pdl = lv.get("PP"), lv.get("PDH"), lv.get("PDL")
    cands = [(nm, lv.get(nm)) for nm in ("S1", "S2", "PDL") if lv.get(nm)]
    if pp and pdh and pdl:
        cands.append(("S3", round(pp - 2 * (pdh - pdl), 1)))
    if r.get("low"):
        cands.append(("day-low", r.get("low")))
    base = int(spot // 100 * 100)
    for rn in (base, base - 100, base - 200):
        cands.append((str(rn), float(rn)))
    seen, tg = set(), []
    for nm, px in sorted(cands, key=lambda x: -(x[1] or 0)):
        if not px or px >= spot:
            continue
        key = round(px / 10)
        if key in seen:
            continue
        seen.add(key)
        tg.append({"level": nm, "px": round(px, 1), "move_pct": round((px / spot - 1) * 100, 2),
                   "put_itm": round(max(k - px, 0), 1)})
        if len(tg) >= 4:
            break
    out["targets"] = tg
    s2 = lv.get("S2") or spot * 0.985
    out["hedge_long_calls"] = {
        "structure": "long-dated CALL (your bullish core) + short-dated ATM PUT  ≈  a long straddle (delta ≈ neutral)",
        "pays": "profits on a BIG move either way; the short put cushions the call's mark-to-market on a drop",
        "cost": "theta on BOTH legs, and VIX-up = the put is RICH today — it bleeds if the drop is small or quiet",
        "verdict": ("JUSTIFIED right now — strong PUT-BIAS gives the put +EV (the fat tail covers the premium)"
                    if "strong" in strength else
                    "usually a COST in India's up-drift — hedge ONLY on a strong US-down+VIX-up signal; otherwise just TRIM the calls"),
        "cheaper": f"a PUT SPREAD (buy {k} PE / sell ~{int(round(s2/50.0)*50)} PE) caps the hedge cost vs a naked put — defined, cheaper, but capped payoff"}
    out["caveat"] = ("Decision-support, not advice. Puts don't pay UNCONDITIONALLY (shorting never pays in India) — "
                     "they earn their keep only gated on the validated US-down+VIX-up signal.")
    return out


def gate():
    now = _now_ist()
    sess = _session(now)
    live, vix = _live_and_vix()
    reg = _regime(vix)
    vixband = _vix_band(reg.get("vix"))
    exp = _expiry_today(now.date())
    spf = _sp_futures()
    opening = _opening(sess, live)
    sr = _sr_sentiment("NIFTY", live.get("NIFTY"))
    nextday = _next_day((live.get("NIFTY") or {}).get("chg_pct"), spf.get("es"))
    put_an = _put_analysis(reg, sr, live)
    auto = _auto_checklist(sess, reg, vixband, exp)
    n_fail = sum(1 for i in auto if i["state"] == "fail")
    n_warn = sum(1 for i in auto if i["state"] == "warn")
    if sess["gate"] == "GREEN" and n_fail == 0:
        verdict = "CLEAR" if n_warn == 0 else "CLEAR (with cautions)"
    elif sess["gate"] == "RED":
        verdict = "NO-GO — market closed"
    else:
        verdict = "WAIT — pod still calibrating"
    return {
        "ok": True,
        "asof_ist": now.strftime("%Y-%m-%d %H:%M:%S"),
        "date_ist": now.date().isoformat(),
        "weekday": now.strftime("%A"),
        "is_trading_day": mv._is_trading_day(now.date()),
        "session": sess,
        "regime": reg,
        "us_futures": spf,
        "sr_sentiment": sr,
        "next_day": nextday,
        "put_analysis": put_an,
        "opening": opening,
        "live": live,
        "vix_band": vixband,
        "expiry": exp,
        "checklist_auto": auto,
        "checklist_user": USER_CHECKLIST,
        "auto_fail": n_fail, "auto_warn": n_warn,
        "verdict": verdict,
        "holidays": sorted(mv.NSE_HOLIDAYS),
        "boundaries": {"preopen": PREOPEN_MIN, "open": OPEN_MIN, "collect_min": COLLECT_MIN,
                       "closing": CLOSING_MIN, "close": CLOSE_MIN},
        "caveat": "Decision-support, not advice. The gate is a discipline tool: it tells you WHEN the session is "
                  "tradeable and what the overnight tilt is — it does not predict the day. You own the trade.",
    }


if __name__ == "__main__":
    import sys, json
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = gate()
    s = r["session"]; reg = r["regime"]
    print(f"\n  {r['asof_ist']} IST · {r['weekday']}")
    print(f"  GATE: {s['gate']}  [{s['phase']}]  — {s['label']}")
    print(f"  REGIME: {reg.get('signal')} ({reg.get('bucket')})")
    print(f"    US last night: SPX(broad) {reg.get('us_spx_pct')}% · Nasdaq(tech) {reg.get('us_tech_pct')}% · "
          f"ES-fut(now) {reg.get('us_fut_es_pct')}% · VIX {reg.get('vix')} ({reg.get('vix_chg_pct')}%)  [{reg.get('us_src')}]")
    print(f"  {reg.get('stat','')}")
    if r["opening"]:
        o = r["opening"]; print(f"  OPENING: NIFTY {o['since_open_pct']:+}% since open · range {o['range_pct']}% — {o['read']}")
    if r["live"]:
        print("  LIVE:", " · ".join(f"{k} {v['last']} ({v['chg_pct']:+}%)" for k, v in r["live"].items()))
    fu = r.get("us_futures") or {}
    if fu.get("items"):
        print("  US FUTURES (live):", " · ".join(f"{x['name']} {x['chg_pct']:+}%" for x in fu["items"]))
    sr = r.get("sr_sentiment")
    if sr:
        print(f"  S/R SENTIMENT: {sr['sentiment']} (score {sr['score']:+}) · {sr['zone']} · {sr['read']}")
    nd = r.get("next_day")
    if nd:
        print(f"  NEXT-DAY BIAS: {nd['bias']} — {nd['stat']}")
    print(f"  VERDICT: {r['verdict']}")
    print("  AUTO CHECKLIST:")
    for i in r["checklist_auto"]:
        mark = {"pass": "✓", "warn": "!", "fail": "✗", "na": "·"}.get(i["state"], "·")
        print(f"    [{mark}] {i['label']} — {i['detail']}")
