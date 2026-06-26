"""
marleg_notify_engine.py — the alert brain behind the notification system.

Produces SCORED alert cards. Every alert carries a CONVICTION (0-100), noise-adjusted, which sets its
priority + whether it makes a sound:
    HIGH >=65  -> red    + sound
    MED  40-64 -> amber  + chime
    LOW  <40   -> grey   + silent

Conviction = signal strength (move in sigmas) × confirmations (volume, trend) ÷ NOISE (illiquidity, erraticness).
That's why a +8% rip in a thin, whippy, retail-driven name (TejasNet) scores LOW even though it "moved a lot" —
the move is mostly noise — while the same % in a liquid, trend-aligned, high-volume name scores HIGH.

Sources (v1):
  • hard-exits from the option guardian (always HIGH)
  • movers across the canonical panel, split: on a user list (watchlist) vs NOT (missed-the-boat 🟡)
Real footprints only (price/volume/vol). No fabricated "who bought." Cascade-funnel + gate-change = next.
"""
import datetime as dt

import numpy as np
import pandas as pd

import marleg_panel_build as pb
import marleg_userlists as ul
import marleg_config as cfg

try:
    import marleg_opt_guardian as og
except Exception:
    og = None


def _ist():
    return (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")


def conviction(move1, vol20, vol_surge, above_sma50, liq_rank, vol_rank):
    """0-100 noise-adjusted conviction for a move. liq_rank/vol_rank are 0..1 percentiles in the universe."""
    C = cfg.CONVICTION                                    # weights live in the config registry, not hardcoded here
    factors = []
    z = min(abs(move1) / max(vol20, 0.005), 5.0)          # how many daily-sigmas is the move
    score = z / 5.0 * C["signal_max"]                     # raw signal strength
    if vol_surge >= 2.0:
        score += C["vol2x"]; factors.append("volume 2x+")
    elif vol_surge >= 1.3:
        score += C["volup"]; factors.append("volume up")
    if (move1 > 0) == bool(above_sma50):
        score += C["trend"]; factors.append("trend-aligned")
    else:
        factors.append("counter-trend")
    noise_mult = C["noise_floor"] + C["noise_span"] * liq_rank   # illiquid -> liquid: the TejasNet silencer
    if vol_rank > C["erratic_pct"]:
        noise_mult *= C["erratic_mult"]; factors.append("erratic")
    score = max(0.0, min(100.0, score * noise_mult))
    tier = "HIGH" if score >= C["high"] else "MED" if score >= C["med"] else "LOW"
    return round(score), tier, factors, round(noise_mult, 2)


def _tone(tier, hard_exit=False):
    if hard_exit:
        return "exit", True
    return ({"HIGH": "high", "MED": "med", "LOW": "low"}[tier]), (tier == "HIGH")


def scan(move1_thresh=None, move5_thresh=None, top=None):
    move1_thresh = cfg.MOVERS["move1_thresh"] if move1_thresh is None else move1_thresh
    move5_thresh = cfg.MOVERS["move5_thresh"] if move5_thresh is None else move5_thresh
    top = cfg.MOVERS["top"] if top is None else top
    P = pb.load()
    if not P or "close" not in P:
        return {"ok": False, "error": "no canonical panel — run marleg_panel_build.py"}
    close, vol = P["close"], P["volume"]
    rets = close.pct_change()
    vol20 = rets.tail(20).std()
    turnover = (close * vol).tail(60).median()
    liq_rank = turnover.rank(pct=True)
    vol_rank = vol20.rank(pct=True)
    sma50 = close.tail(50).mean()
    last, prev = close.iloc[-1], close.iloc[-2]
    prev5 = close.iloc[-6] if len(close) >= 6 else close.iloc[0]
    volsurge = vol.iloc[-1] / vol.tail(20).mean().replace(0, np.nan)
    watched = set(ul.effective_symbols())

    alerts = []
    for s in close.columns:
        m1 = last.get(s); p = prev.get(s)
        if pd.isna(m1) or pd.isna(p) or p <= 0:
            continue
        m1 = m1 / p - 1.0
        p5 = prev5.get(s)
        m5 = (last[s] / p5 - 1.0) if (p5 and p5 > 0) else 0.0
        if abs(m1) < move1_thresh and abs(m5) < move5_thresh:
            continue
        vs = volsurge.get(s)
        vs = float(vs) if (vs is not None and not pd.isna(vs)) else 1.0
        sc, tier, factors, nm = conviction(m1, float(vol20.get(s) or 0.02), vs,
                                           bool(last[s] > (sma50.get(s) or last[s])),
                                           float(liq_rank.get(s) or 0.5), float(vol_rank.get(s) or 0.5))
        on_list = s in watched
        up = m1 > 0
        tone, sound = _tone(tier)
        if on_list:
            kind = "watchlist"
            why = f"{'+' if up else ''}{m1*100:.1f}% ({m5*100:+.1f}% 5d) — on your list · {', '.join(factors)}"
        elif up:
            kind = "missed_boat"
            why = f"🟡 not on any list — {m1*100:+.1f}% ({m5*100:+.1f}% 5d) · {', '.join(factors)}"
        else:
            kind = "mover"
            why = f"{m1*100:+.1f}% ({m5*100:+.1f}% 5d) · {', '.join(factors)}"
        alerts.append({"sym": s, "kind": kind, "direction": "up" if up else "down",
                       "move1d_pct": round(m1 * 100, 1), "move5d_pct": round(m5 * 100, 1),
                       "conviction": sc, "tier": tier, "tone": tone, "sound": sound,
                       "noise_mult": nm, "factors": factors, "on_list": on_list, "why": why})

    # hard-exits from the guardian — always top priority
    if og is not None:
        try:
            for pos in (og.status().get("positions") or []):
                if pos.get("tone") == "exit":
                    alerts.append({"sym": pos["sym"], "kind": "hard_exit", "direction": "down",
                                   "move1d_pct": pos.get("off_peak_pct"), "move5d_pct": None,
                                   "conviction": 92, "tier": "HIGH", "tone": "exit", "sound": True,
                                   "noise_mult": 1.0, "factors": ["trailing-stop"], "on_list": True,
                                   "why": "🔴 EXIT — " + pos.get("why", "trailing stop hit")})
        except Exception:
            pass

    alerts.sort(key=lambda a: -a["conviction"])
    counts = {"HIGH": 0, "MED": 0, "LOW": 0}
    for a in alerts:
        counts[a["tier"]] = counts.get(a["tier"], 0) + 1
    return {"ok": True, "asof": _ist(), "freshness": cfg.freshness("eod", built=P.get("built")),
            "n": len(alerts), "tiers": counts, "alerts": alerts[:top]}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    r = scan()
    if not r.get("ok"):
        print(r.get("error")); raise SystemExit
    print(f"\n═══ NOTIFY FEED · {r['asof']} · {r['n']} alerts (HIGH {r['tiers']['HIGH']} / MED {r['tiers']['MED']} / LOW {r['tiers']['LOW']}) ═══")
    icon = {"exit": "🔴", "high": "🔴", "med": "🟡", "low": "⚪"}
    for a in r["alerts"][:25]:
        snd = "🔊" if a["sound"] else "  "
        print(f"  {icon.get(a['tone'],'·')}{snd} [{a['conviction']:>3}] {a['sym']:<12} {a['kind']:<11} {a['why']}")
    print("\n  conviction = signal × confirmation ÷ noise. HIGH🔴+sound / MED🟡+chime / LOW⚪+silent. Real footprints only.")
