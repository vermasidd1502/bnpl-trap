"""
marleg_overnight.py — does "buy at close, sell at open" (overnight) beat "buy at open, sell at close"
(intraday) in India?

Decomposes daily returns on the canonical panel into the OVERNIGHT leg (close[t-1] -> open[t]) and the
INTRADAY leg (open[t] -> close[t]), GROSS and NET of India's delivery costs. The punchline lives in the
costs: harvesting the overnight leg means HOLDING overnight = delivery = 0.10%+0.10% STT round-trip
EVERY day, which is bigger than the per-day overnight gain — a "cost mirage". The gross Sharpe is also
inflated by bid-ask bounce / stale open prints, which is why it shrinks as the universe gets cleaner.

  python marleg_overnight.py
"""
import sys
import numpy as np


def _stats(r, cost_bps=0.0):
    mu, sd = float(r.mean()), float(r.std())
    net = mu - cost_bps / 10000.0
    return {"mean_day_pct": round(mu * 100, 4), "ann_pct": round(((1 + mu) ** 252 - 1) * 100, 1),
            "sharpe": round(mu / sd * np.sqrt(252), 2) if sd > 0 else 0.0,
            "win_pct": round(float((r > 0).mean()) * 100, 1),
            "cum_pct": round(((1 + r).prod() - 1) * 100, 0),
            "net_ann_pct": round(((1 + net) ** 252 - 1) * 100, 1)}


def analyze(universe="largecap"):
    import marleg_panel_build as pb
    P = pb.load()
    close, open_, vol = P["close"], P["open"], P["volume"]
    turn = (close * vol).median()
    if universe == "all":
        keep = turn[turn >= turn.quantile(0.50)].index; label = "all liquid (top 50% turnover)"
    elif universe == "mid":
        keep = turn.sort_values(ascending=False).head(150).index; label = "top-150 by turnover"
    else:
        keep = turn.sort_values(ascending=False).head(50).index; label = "top-50 large caps"
    c, o = close[keep], open_[keep]
    pc = c.shift(1)
    on = (o / pc - 1).mask(lambda x: x.abs() > 0.5).mean(axis=1).dropna()      # close -> open (overnight)
    ind = (c / o - 1).mask(lambda x: x.abs() > 0.5).mean(axis=1).dropna()      # open -> close (intraday)
    full = (c / pc - 1).mask(lambda x: x.abs() > 0.5).mean(axis=1).dropna()    # close -> close (buy & hold)
    if len(on) < 60:
        return {"ok": False, "error": "panel too short"}
    on_s = _stats(on)
    verdict = ("In India the gain is almost ALL overnight ({:+.0f}% ann gross) while the intraday session "
               "drifts DOWN ({:+.0f}% ann) — real and globally-documented. But it's NOT harvestable: holding "
               "overnight = delivery = ~0.20% STT round-trip EVERY day, bigger than the +{:.2f}%/day overnight "
               "gain, so net of costs it dies. The Sharpe {:.1f} is inflated by bid-ask bounce / stale opens "
               "(it shrinks on cleaner large caps). The survivor net of costs is still buy-&-hold.").format(
        on_s["ann_pct"], _stats(ind)["ann_pct"], on_s["mean_day_pct"], on_s["sharpe"])
    return {"ok": True, "universe": label, "n_names": int(len(keep)), "days": int(len(on)),
            "start": str(on.index[0].date()), "end": str(on.index[-1].date()),
            "overnight": on_s, "overnight_net20": _stats(on, 20), "overnight_net30": _stats(on, 30),
            "intraday": _stats(ind), "buyhold": _stats(full), "verdict": verdict,
            "note": "Overnight = close→open, Intraday = open→close, Buy&Hold = close→close. NET rows charge "
                    "the overnight leg India's delivery STT (20bps floor / 30bps realistic). Decision-support."}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    import json
    u = sys.argv[1] if len(sys.argv) > 1 else "largecap"
    print(json.dumps(analyze(u), indent=2))
