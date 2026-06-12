"""
Marle-G Cascade BACKTEST — event-study validation of the cascade "painting".

For each real historical event date, we build the cascade legs (industry + predicted
side) and measure each leg's CAR = cumulative ABNORMAL return vs NIFTY over the next
H trading days. A leg "hits" if sign(CAR) matches the prediction. We also form the
market-neutral long-short basket (mean long-member CAR - mean short-member CAR) per
event and t-test it across events.

Method = standard event study (MacKinlay 1997): abnormal return = stock return -
market return (market-model with beta=1, the common simplification for baskets).

HONEST CAVEATS (printed): few events (low power), survivorship (uses TODAY's industry
members; some didn't trade then -> skipped), India structural long-bias (short legs
fight a rising tape), no costs. Treat as evidence of directionality, not a P&L promise.

  python marleg_cascade_backtest.py --event oil_shock_up --h 10
"""
import os, sys, json, argparse, collections
import numpy as np, pandas as pd, yfinance as yf
import marleg_cascade as casc

HERE = os.path.dirname(os.path.abspath(__file__))

# Real event dates (NSE trading-day or nearest). Sources: well-known oil/macro shocks.
EVENT_DATES = {
    "oil_shock_up": [
        "2019-09-16",   # Saudi Aramco Abqaiq attack, Brent +15%
        "2022-02-24",   # Russia invades Ukraine, crude spikes
        "2022-03-07",   # crude blow-off ~$130
        "2023-04-03",   # surprise OPEC+ output cut, crude gaps up
    ],
    "oil_shock_down": [
        "2020-03-09",   # Saudi-Russia price war, crude crash
        "2018-11-13",   # Q4-2018 oil slide
        "2014-12-01",   # OPEC no-cut, oil collapse
    ],
    "rate_hike": [
        "2022-05-04",   # RBI surprise off-cycle +40bps
        "2022-06-08",   # +50bps
        "2018-06-06",   # RBI hike
    ],
    "war_escalation": [
        "2019-09-16", "2022-02-24",
    ],
    "iran_war": [
        "2019-09-16",   # Aramco Abqaiq attack (Iran-linked), Brent +15%
        "2020-01-03",   # US kills Soleimani — Iran escalation, oil spike
        "2024-04-15",   # Iran's first direct missile/drone strike on Israel
        "2024-10-01",   # Iran ballistic-missile barrage on Israel
    ],
}


def yftk(s):
    return "^NSEI" if s == "^NSEI" else s + ".NS"


def fetch_history(syms, start="2014-06-01", end="2024-06-01"):
    tks = [yftk(s) for s in syms]
    px = {}
    for i in range(0, len(tks), 40):
        chunk = tks[i:i + 40]
        try:
            d = yf.download(chunk, start=start, end=end, interval="1d",
                            progress=False, group_by="ticker", threads=True, auto_adjust=True)
            for s, tk in zip(syms[i:i + 40], chunk):
                try:
                    c = (d[tk]["Close"] if len(chunk) > 1 else d["Close"]).dropna()
                    if len(c) > 50:
                        px[s] = c
                except Exception:
                    pass
        except Exception:
            pass
    return px


def fwd_ret(close, date, h):
    """Return over [first trading day >= date, +h trading days], or None."""
    idx = close.index[close.index >= pd.Timestamp(date, tz=close.index.tz)]
    if len(idx) < 2:
        return None
    t0 = idx[0]
    pos = close.index.get_loc(t0)
    if pos + h >= len(close):
        return None
    return float(close.iloc[pos + h] / close.iloc[pos] - 1.0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", default="oil_shock_up")
    ap.add_argument("--h", type=int, default=10, help="holding window in trading days")
    a = ap.parse_args()
    if a.event not in EVENT_DATES:
        print(f"no historical dates for '{a.event}'. have: {', '.join(EVENT_DATES)}"); return

    cascade = casc.build_cascade(a.event)
    legs = cascade["legs"]
    members = sorted({m for lg in legs for m in lg["members"]})
    print(f"event-study: {a.event}  |  {len(EVENT_DATES[a.event])} events  |  H={a.h}d  |  {len(members)} member stocks")
    print("downloading history (yfinance) ...")
    px = fetch_history(members + ["^NSEI"])
    nifty = px.get("^NSEI")
    if nifty is None:
        print("no NIFTY data"); return

    # per (event, leg) CAR
    leg_hits = collections.defaultdict(lambda: [0, 0])     # industry -> [hits, total]
    basket_rets = []                                        # per-event long-short CAR
    per_event = []
    for dt in EVENT_DATES[a.event]:
        mret = fwd_ret(nifty, dt, a.h)
        if mret is None:
            continue
        long_cars, short_cars = [], []
        ev_legs = 0
        for lg in legs:
            cars = []
            for s in lg["members"]:
                if s not in px:
                    continue
                r = fwd_ret(px[s], dt, a.h)
                if r is None:
                    continue
                cars.append(r - mret)                       # abnormal return
            if not cars:
                continue
            leg_car = float(np.mean(cars))
            pred = 1 if lg["side"] == "LONG" else -1
            hit = (leg_car > 0) == (pred > 0)
            leg_hits[lg["industry"]][0] += int(hit)
            leg_hits[lg["industry"]][1] += 1
            ev_legs += 1
            (long_cars if pred > 0 else short_cars).append(leg_car)
        # market-neutral basket: long legs' CAR minus short legs' CAR
        ls = (np.mean(long_cars) if long_cars else 0.0) - (np.mean(short_cars) if short_cars else 0.0)
        basket_rets.append(ls)
        per_event.append((dt, ls, ev_legs))

    print(f"\n{'EVENT':<13}{'L/S basket CAR':>16}{'legs':>7}")
    print("-" * 40)
    for dt, ls, n in per_event:
        print(f"{dt:<13}{ls*100:>15.2f}%{n:>7}")
    print("-" * 40)
    if basket_rets:
        arr = np.array(basket_rets)
        t = arr.mean() / (arr.std(ddof=1) / np.sqrt(len(arr))) if len(arr) > 1 and arr.std() > 0 else float("nan")
        print(f"mean L/S basket CAR over {a.h}d: {arr.mean()*100:+.2f}%   "
              f"(t={t:.2f}, n={len(arr)}, win-rate={np.mean(arr>0)*100:.0f}%)")
    # leg directional hit-rates (which industries the cascade calls best)
    print(f"\n{'INDUSTRY':<40}{'hit-rate':>10}")
    print("-" * 52)
    for ind, (h, tot) in sorted(leg_hits.items(), key=lambda kv: -(kv[1][0]/kv[1][1] if kv[1][1] else 0)):
        if tot:
            print(f"{ind:<40}{h}/{tot} ({h/tot*100:.0f}%)")
    print("\nCAVEATS: few events (low power); uses TODAY's industry members (survivorship; "
          "names not trading then are skipped); India long-bias works against short legs; no costs/slippage.")


if __name__ == "__main__":
    main()
