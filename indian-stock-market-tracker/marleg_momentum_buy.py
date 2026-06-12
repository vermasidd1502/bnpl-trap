"""
Marle-G momentum ENTRY script — buys volume-accumulation longs with a strict,
volatility-scaled (ATR) stop and fixed-fractional risk sizing.

SAFETY (anti-Knight-Capital): DRY-RUN by default — prints the order PLAN and sends
NOTHING. Live placement needs BOTH `--live` AND env MARLEG_ALLOW_LIVE_ORDERS=1; each
order also crosses sanity checks (positive qty, stop strictly below entry, dedupe vs
holdings, deploy cap). Trailing the stop AFTER entry is handled by marleg_stop_engine.py
/ the Groww Cloud script — this script only opens the position + sets the initial SL.

SIGNAL (validated train/test): gated accumulation long —
  sector top-40% relative strength  +  U/D > 50d MA & rising  +  price > 0.618 Fib,
  ranked by RSI entry-timing: EARLY (RSI<45) first, EXTENDED (60-70) ok, CHASING (>=70)
  last, and the DEAD-ZONE (RSI 45-60, the bucket that lost in & out of sample) SKIPPED.

RISK MODEL:
  stop   = entry - k*ATR(14)                     (volatility-scaled, never a guessed %)
  target = entry + RR*(entry - stop)             (default 2:1)
  qty    = floor(capital * risk% / (entry-stop)) (fixed-fractional: each trade risks risk%)
  caps   : --max positions, --deploy % of capital, skip names already held.

  python marleg_momentum_buy.py
  python marleg_momentum_buy.py --capital 500000 --risk 1 --k 2 --rr 2 --max 5
  python marleg_momentum_buy.py --live        # also needs MARLEG_ALLOW_LIVE_ORDERS=1
"""
import os, json, argparse
import numpy as np, pandas as pd, yfinance as yf
import marleg_volume_scan as v

HERE = os.path.dirname(os.path.abspath(__file__))
SECT = json.load(open(os.path.join(HERE, "marleg_sectors.json"), encoding="utf-8"))


def screen():
    U = v.SEED
    data = yf.download([s + ".NS" for s in U], period="1y", interval="1d",
                       group_by="ticker", progress=False, threads=True)
    close, volume, high, low = {}, {}, {}, {}
    for s in U:
        t = s + ".NS"
        try:
            c = data[t]["Close"].dropna()
            if len(c) > 200:
                close[s] = c; volume[s] = data[t]["Volume"]; high[s] = data[t]["High"]; low[s] = data[t]["Low"]
        except Exception:
            pass
    close = pd.DataFrame(close); volume = pd.DataFrame(volume).reindex(close.index)
    high = pd.DataFrame(high).reindex(close.index); low = pd.DataFrame(low).reindex(close.index)

    ret20 = close.pct_change(20, fill_method=None)
    secmap = {s: (SECT.get(s, {}).get("sector") or "Others") for s in close.columns}
    members = {}
    for s in close.columns: members.setdefault(secmap[s], []).append(s)
    sec_ret = pd.DataFrame({sec: ret20[m].mean(axis=1) for sec, m in members.items()})
    srank = pd.DataFrame({s: sec_ret.rank(axis=1, ascending=False, pct=True)[secmap[s]] for s in close.columns})
    d = np.sign(close.diff()); upv = volume.where(d > 0, 0.0); dnv = volume.where(d < 0, 0.0)
    ud = upv.rolling(20).sum() / dnv.rolling(20).sum().replace(0, np.nan)
    hh = close.rolling(120).max(); ll = close.rolling(120).min(); fib = (close - ll) / (hh - ll).replace(0, np.nan)
    longsig = (srank <= 0.40) & (ud > ud.rolling(50).mean()) & (ud > ud.shift(10)) & (fib > 0.618)
    delta = close.diff(); gain = delta.clip(lower=0).rolling(14).mean(); loss = (-delta.clip(upper=0)).rolling(14).mean()
    rsi = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    t = close.index[-1]; out = []
    for s in close.columns:
        if not longsig.loc[t, s]:
            continue
        r = float(rsi.loc[t, s])
        if 45 <= r < 60:                                  # DEAD-ZONE — validated worst bucket, skip
            continue
        c, h, l = close[s], high[s], low[s]; pc = c.shift(1)
        tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        a = float(tr.rolling(14).mean().iloc[-1])
        if not a or a <= 0 or pd.isna(a):
            continue
        tag = "EARLY" if r < 45 else "EXTENDED" if r < 70 else "CHASING"
        out.append({"s": s, "name": SECT.get(s, {}).get("name", s), "sector": secmap[s],
                    "entry": round(float(c.iloc[-1]), 1), "atr": round(a, 2), "rsi": round(r, 1),
                    "ud": round(float(ud.loc[t, s]), 2), "fib": round(float(fib.loc[t, s]), 2), "tag": tag})
    rank = {"EARLY": 0, "EXTENDED": 1, "CHASING": 2}
    out.sort(key=lambda x: (rank[x["tag"]], -x["ud"]))
    return out, str(t.date())


def build_plan(cands, capital, risk_pct, k, rr, maxpos, deploy_pct, held):
    orders, deployed = [], 0.0
    cap_deploy = capital * deploy_pct / 100.0
    for c in cands:
        if len(orders) >= maxpos:
            break
        if c["s"] in held:
            continue
        entry = c["entry"]; stop = round(entry - k * c["atr"], 1)
        if stop <= 0 or stop >= entry:                    # SANITY: wrong-side / nonsensical stop
            continue
        risk_ps = entry - stop
        qty = int((capital * risk_pct / 100.0) // risk_ps)
        if qty <= 0:
            continue
        if deployed + qty * entry > cap_deploy:           # deploy cap -> size down to fit
            qty = int((cap_deploy - deployed) // entry)
            if qty <= 0:
                continue
        cost = qty * entry
        orders.append({**c, "qty": qty, "stop": stop, "target": round(entry + rr * risk_ps, 1),
                       "risk_rs": round(qty * risk_ps), "cost": round(cost), "rr": rr})
        deployed += cost
    return orders, deployed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital", type=float, default=1_000_000)
    ap.add_argument("--risk", type=float, default=1.0, help="%% of capital risked per trade")
    ap.add_argument("--k", type=float, default=2.0, help="stop = entry - k*ATR")
    ap.add_argument("--rr", type=float, default=2.0, help="target reward:risk")
    ap.add_argument("--max", type=int, default=5, help="max new positions")
    ap.add_argument("--deploy", type=float, default=60.0, help="max %% of capital deployed")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    live_ok = args.live and os.environ.get("MARLEG_ALLOW_LIVE_ORDERS") == "1"
    mode = "LIVE" if live_ok else ("LIVE requested but BLOCKED — set MARLEG_ALLOW_LIVE_ORDERS=1" if args.live else "DRY-RUN (paper)")
    print(f"Marle-G momentum buy | mode = {mode}")
    print(f"capital Rs{args.capital:,.0f} | risk {args.risk}%/trade | stop {args.k}xATR | target 1:{args.rr} | max {args.max} | deploy<= {args.deploy}%\n")

    held = set()
    try:
        import groww_client as gc
        cl = gc.GrowwClient()
        hd = cl.holdings_data() or {}
        for h in (hd.get("holdings") or (hd.get("payload") or {}).get("holdings") or []):
            sym = h.get("trading_symbol") or h.get("symbol")
            if sym: held.add(sym)
        if held: print(f"skipping {len(held)} names already held.")
    except Exception:
        cl = None

    print("screening universe for accumulation longs (this downloads price history)...")
    cands, asof = screen()
    print(f"as of {asof} | {len(cands)} candidates outside the RSI 45-60 dead zone\n")
    orders, deployed = build_plan(cands, args.capital, args.risk, args.k, args.rr, args.max, args.deploy, held)
    if not orders:
        print("No qualifying entries right now (no accumulation longs outside the dead zone / sizing).")
        return

    print(f"{'SYMBOL':<12}{'tag':<10}{'entry':>9}{'stop':>9}{'target':>9}{'qty':>7}{'risk Rs':>9}{'cost Rs':>11}  R:R")
    print("-" * 86)
    for o in orders:
        print(f"{o['s']:<12}{o['tag']:<10}{o['entry']:>9}{o['stop']:>9}{o['target']:>9}{o['qty']:>7}"
              f"{o['risk_rs']:>9}{o['cost']:>11}  1:{o['rr']:.0f}")
    tot_risk = sum(o["risk_rs"] for o in orders)
    print("-" * 86)
    print(f"{len(orders)} entries | deployed Rs{deployed:,.0f} ({deployed/args.capital*100:.0f}%) | "
          f"total risk Rs{tot_risk:,.0f} ({tot_risk/args.capital*100:.2f}% of capital if every stop hits)")
    print("Initial SL set at entry; ratchet/trailing handed to marleg_stop_engine.py / Groww Cloud after fill.")
    json.dump({"asof": asof, "mode": mode, "orders": orders},
              open(os.path.join(HERE, "marleg_buy_plan.json"), "w"), indent=1, default=str)

    if not live_ok:
        print("\nDRY-RUN: nothing sent. To go live (your call): set MARLEG_ALLOW_LIVE_ORDERS=1 and pass --live.")
        return
    print("\nLIVE — placing BUY + resting SL-M for each (each crosses sanity checks):")
    for o in orders:
        if o["qty"] <= 0 or o["stop"] >= o["entry"]:
            print(f"  SKIP {o['s']}: sanity"); continue
        b = cl.place_order(o["s"], "BUY", o["qty"], product="CNC", order_type="MARKET", confirm=True)
        print(f"  BUY {o['qty']:>4} {o['s']:<12} -> {b.get('groww_order_id') or b.get('status_code') or b.get('note')}")
        sl = cl.place_order(o["s"], "SELL", o["qty"], product="CNC", order_type="SL-M",
                            trigger_price=o["stop"], confirm=True)
        print(f"  SL  {o['qty']:>4} {o['s']:<12} @ {o['stop']} -> {sl.get('groww_order_id') or sl.get('status_code') or sl.get('note')}")
    print("\nDone. Now trail the stops:  python marleg_stop_engine.py --ticks 0 --interval 5")


if __name__ == "__main__":
    main()
