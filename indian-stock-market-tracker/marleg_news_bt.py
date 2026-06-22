"""
marleg_news_bt.py — backtest the OUTCOME of news/rating events on the FULL Indian universe.

The honest constraint: there is NO free, structured history of Indian analyst calls (who said what, when,
at what target). yfinance returns ZERO upgrades/downgrades for .NS names and Groww's API has no news feed.
So we CANNOT rank "which house is reliable" for India — and I won't fake a leaderboard from data we don't
have. What we CAN test rigorously is the GENERIC question the user actually cares about: when a stock makes
a sharp news-driven move, does reacting to it pay — does the move CONTINUE (trust the news) or FADE (it's
already priced / over-reaction)?

Proxy for a "news/rating day" = an opening GAP of >= min_gap% (a gap is the market repricing on fresh
information overnight — earnings, ratings, orders). For every gap event across the panel we measure forward
close-to-close returns at +1/+5/+20 sessions, split by up-gap (good news) vs down-gap (bad news), and
compare to the all-day baseline drift. That tells you empirically whether to chase a news pop or fade it.

Read-only, pure compute on the canonical panel.
  python marleg_news_bt.py
"""
import sys
import numpy as np


def study(min_gap=3.0, horizons=(1, 5, 20)):
    import marleg_panel_build as pb
    P = pb.load()
    O, C = P["open"], P["close"]
    cols = list(C.columns)
    up = {h: [] for h in horizons}
    dn = {h: [] for h in horizons}
    base = {h: [] for h in horizons}
    n_names = 0
    for s in cols:
        c = C[s].dropna()
        if len(c) < 80:
            continue
        n_names += 1
        o = O[s].reindex(c.index)
        gap = (o / c.shift(1) - 1.0) * 100.0
        for h in horizons:
            fwd = (c.shift(-h) / c - 1.0) * 100.0
            valid = fwd.notna() & gap.notna()
            base[h].extend(fwd[valid].values.tolist())
            um = valid & (gap >= min_gap)
            dm = valid & (gap <= -min_gap)
            up[h].extend(fwd[um].values.tolist())
            dn[h].extend(fwd[dm].values.tolist())

    def agg(xs):
        if not xs:
            return None
        a = np.array(xs, dtype=float)
        return {"n": int(a.size), "mean": round(float(a.mean()), 2), "median": round(float(np.median(a)), 2),
                "win": round(float((a > 0).mean()) * 100, 1)}

    rows = []
    for h in horizons:
        b = agg(base[h]); u = agg(up[h]); d = agg(dn[h])
        rows.append({"horizon": h, "baseline": b, "up_gap": u, "down_gap": d,
                     "up_edge": round(u["mean"] - b["mean"], 2) if (u and b) else None,
                     "down_edge": round(d["mean"] - b["mean"], 2) if (d and b) else None})

    # verdicts
    v = {}
    for r in rows:
        h = r["horizon"]
        if r["up_gap"]:
            ue = r["up_edge"]
            v[f"up_{h}d"] = ("CONTINUES — chasing a good-news pop beat baseline" if ue and ue > 0.3
                             else "FADES — the good-news pop gives back vs baseline" if ue and ue < -0.3
                             else "≈ baseline — no edge in chasing the pop")
        if r["down_gap"]:
            de = r["down_edge"]
            v[f"down_{h}d"] = ("KEEPS FALLING — bad-news drift down (don't catch it)" if de and de < -0.3
                               else "BOUNCES vs baseline — over-reaction snaps back" if de and de > 0.3
                               else "≈ baseline — no edge")
    return {"ok": True, "min_gap_pct": min_gap, "names": n_names, "rows": rows, "verdicts": v,
            "note": "Gap = open vs prior close, a proxy for an overnight news/rating repricing. Forward = "
                    "close-to-close from the event day. 'edge' = event mean − all-day baseline mean. NO "
                    "analyst-house ranking is possible (no free Indian ratings history) — this tests whether "
                    "reacting to the news MOVE pays, which is the tradeable question. Decision-support, not advice."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    gap = float(sys.argv[1]) if len(sys.argv) > 1 else 3.0
    r = study(gap)
    print(f"\n  NEWS-EVENT OUTCOME STUDY — gap>={r['min_gap_pct']}% · {r['names']} names")
    print(f"  {'horizon':>8} {'baseline':>22} {'UP-gap (good news)':>30} {'DOWN-gap (bad news)':>30}")
    for x in r["rows"]:
        b, u, d = x["baseline"], x["up_gap"], x["down_gap"]
        bs = f"{b['mean']:+.2f}% w{b['win']}% (n{b['n']:,})" if b else "—"
        us = f"{u['mean']:+.2f}% w{u['win']}% edge{x['up_edge']:+.2f} (n{u['n']:,})" if u else "—"
        ds = f"{d['mean']:+.2f}% w{d['win']}% edge{x['down_edge']:+.2f} (n{d['n']:,})" if d else "—"
        print(f"  {x['horizon']:>6}d {bs:>22} {us:>30} {ds:>30}")
    print("\n  verdicts:")
    for k, val in r["verdicts"].items():
        print(f"    {k:<9} {val}")
    print(f"\n  {r['note']}")
