"""
marleg_fib_target.py — FIB-AWARE target: don't chase past resistance, target it; extend only on the break.

Backtested (marleg_fib_target_study, 5y): a target set 2*ATR PAST the prior high (fib 1.0) is reached only
~52% of the time, while the prior high itself is reached ~71% — and once the high breaks, price continues to
the extension ~73%. So:
  • price BELOW the prior 120d high  → first target = the prior high (the resistance).  ~71% hit.
  • price AT/THROUGH the high (broke)→ next target = the fib 1.272 extension, then 1.618.   ~73% continues.
The market watches fib=1 too — so we book/trail at it and EXTEND only on a confirmed hold, never auto-chase.

  target(price, hi120, lo120, atr) -> dict
"""


def target(price, hi120, lo120, atr):
    price = float(price); atr = float(atr or 0)
    hi120 = float(hi120 or price); lo120 = float(lo120 or price)
    swing = (hi120 - lo120) if hi120 > lo120 else (2 * atr if atr else price * 0.05)
    ext1272 = hi120 + 0.272 * swing
    ext1618 = hi120 + 0.618 * swing
    if price < hi120 * 0.999:                      # below the resistance
        first = hi120
        status = "below resistance"
        at_res = price >= hi120 * 0.985            # within ~1.5% of the high
    else:                                          # already broke the high → aim at the extension
        first = ext1272 if price < ext1272 else ext1618
        status = "broke resistance"
        at_res = False
    stop = price - atr if atr else price * 0.97
    return {"first_target": round(first, 1), "resistance": round(hi120, 1),
            "ext_1272": round(ext1272, 1), "ext_1618": round(ext1618, 1),
            "stop": round(stop, 1), "status": status, "at_resistance": bool(at_res),
            "tgtpct": round((first / price - 1) * 100, 1) if price else None,
            "note": ("at the resistance — book partial / trail; extends to ₹%.1f only on a confirmed hold above ₹%.1f"
                     % (ext1272, hi120)) if at_res else
                    ("targeting the prior high ₹%.1f (resistance, ~71%% hit); then ₹%.1f / ₹%.1f on a confirmed break"
                     % (hi120, ext1272, ext1618)) if status == "below resistance" else
                    ("broke the high — riding to the ₹%.1f / ₹%.1f extensions (continues ~73%% once broken)" % (ext1272, ext1618))}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    # demo: price 595, 120d high 609, low 470, atr 18
    print(target(595, 609, 470, 18))
    print(target(612, 609, 470, 18))   # broke the high
