"""
Marle-G — INTRADAY FADE BACKTEST (5-minute bars).

Daily test said shorting the fade fails (capitulation bounce). But intraday mean-reversion
is a different beast. This tests: when a stock spikes INTRADAY (5m RSI>72, stretched above
VWAP) and starts ticking down — does shorting it for the next ~30-60 min pay, net of cost?
Two variants: plain fade, and fade WITH volume fading (the user's specific idea). Symmetric
long-dip test included to see if mean-reversion is two-sided or drift-biased.

DATA: official Groww 5-min candles (auto-chunked). Deeper + cleaner than yfinance.
  python marleg_intraday_fade_bt.py [days]
"""
import sys
import numpy as np
import pandas as pd
import groww_client as gc

UNIV = ["RELIANCE", "TEJASNET", "ADANIPORTS", "TATASTEEL", "JSWSTEEL", "SBIN",
        "AXISBANK", "BHARTIARTL", "QUESS", "ICICIBANK", "COALINDIA", "HINDALCO"]
RSI_HI, RSI_LO = 72, 28
EXT = 0.008          # stretched > 0.8% above/below VWAP
HOLD = 6             # exit after 6 bars (~30 min)
COST = 0.0008        # intraday round-trip incl slippage


def _rsi(c, n=14):
    d = c.diff(); up = d.clip(lower=0).rolling(n).mean(); dn = (-d.clip(upper=0)).rolling(n).mean().replace(0, np.nan)
    return 100 - 100 / (1 + up / dn)


def run(days=75):
    c_api = gc.GrowwClient(); c_api.token()
    fade, fade_volfade, dip = [], [], []
    names = 0; span = [None, None]
    for s in UNIV:
        try:
            d = c_api.candles(s, 5, days)
        except Exception:
            d = None
        if d is None or len(d) < 300:
            continue
        names += 1
        d = d.rename(columns={"open": "Open", "high": "High", "low": "Low",
                              "close": "Close", "volume": "Volume"})
        if span[0] is None or d.index[0] < span[0]:
            span[0] = d.index[0]
        if span[1] is None or d.index[-1] > span[1]:
            span[1] = d.index[-1]
        d["date"] = d.index.date
        d["rsi"] = _rsi(d["Close"])
        c = d["Close"].values; h = d["High"].values; v = d["Volume"].values; rsi = d["rsi"].values
        # per-day VWAP + volume MA
        d["tpv"] = d["Close"] * d["Volume"]
        d["vwap"] = d.groupby("date")["tpv"].cumsum() / d.groupby("date")["Volume"].cumsum()
        vwap = d["vwap"].values
        vma = pd.Series(v).rolling(10).mean().values
        dates = d["date"].values
        n = len(c)
        for i in range(20, n - HOLD - 1):
            if dates[i] != dates[i + HOLD]:        # don't hold across the close
                continue
            entry = c[i + 1]
            if not np.isfinite(entry) or entry <= 0:
                continue
            # FADE-SHORT: overbought + stretched above VWAP + ticking down
            if rsi[i] > RSI_HI and c[i] > vwap[i] * (1 + EXT) and c[i] < c[i - 1]:
                exitp = c[i + HOLD]
                ret = (entry - exitp) / entry - COST
                fade.append(ret * 100)
                if np.isfinite(vma[i]) and v[i] < vma[i]:    # volume fading at the signal
                    fade_volfade.append(ret * 100)
            # LONG-DIP (symmetry check)
            if rsi[i] < RSI_LO and c[i] < vwap[i] * (1 - EXT) and c[i] > c[i - 1]:
                exitp = c[i + HOLD]
                ret = (exitp - entry) / entry - COST
                dip.append(ret * 100)

    def stat(a, label):
        a = np.array(a)
        if len(a) < 20:
            return f"{label:<26} n={len(a)} (thin)"
        return (f"{label:<26} n={len(a):<5} mean {a.mean():+.3f}% median {np.median(a):+.3f}% "
                f"win {(a>0).mean()*100:.0f}% sum {a.sum():+.1f}%")
    return {"names": names, "span": [str(span[0]), str(span[1])], "lines": [
        stat(fade, "FADE-SHORT (RSI>72)"),
        stat(fade_volfade, "FADE-SHORT + vol fading"),
        stat(dip, "LONG-DIP (RSI<28) [symmetry]"),
    ], "raw": {"fade": fade, "fade_volfade": fade_volfade, "dip": dip}}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    days = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 75
    r = run(days)
    print(f"\nINTRADAY FADE BACKTEST — Groww 5-min, {r['span'][0][:10]}..{r['span'][1][:10]}, {r['names']} names, hold {HOLD} bars (~30min), net {COST*1e4:.0f}bps\n")
    for ln in r["lines"]:
        print("  " + ln)
    fa = np.array(r["raw"]["fade"]); fv = np.array(r["raw"]["fade_volfade"])
    works = len(fa) > 50 and fa.mean() > 0.02 and (fa > 0).mean() > 0.5
    works_v = len(fv) > 50 and fv.mean() > 0.02 and (fv > 0).mean() > 0.5
    print("\nVERDICT: intraday fade-short %s | with volume-fading filter %s" % (
        "PAYS (worth a paper test)" if works else "does NOT pay net of cost",
        "PAYS" if works_v else "does NOT pay"))
    print("(Official Groww 5-min, chunked ~2.5 months. Directional, not decade-grade.)")


if __name__ == "__main__":
    main()
