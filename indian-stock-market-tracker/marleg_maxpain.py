"""
marleg_maxpain.py — the NIFTY (index) OPTION BOARD: max-pain + OI walls from the LIVE chain.

This is the "form structure" behind the expiry-day pin the user keeps seeing (NIFTY rises to a strike,
reverses, the call bleeds). The mechanics, made explicit:
  MAX-PAIN  = the strike where the TOTAL value of expiring options is minimized (most expire worthless).
              Option writers (net-short institutions/MMs) delta-hedge toward it → price gets pinned there.
  CALL WALL = the strike with the heaviest CALL OI = RESISTANCE (writers defend it, sell into rallies).
  PUT WALL  = the strike with the heaviest PUT OI = SUPPORT (writers defend it, buy dips).
On expiry day price gravitates to max-pain and reverses at the walls → directional option BUYERS get
chopped by theta + the pin (writers win). Works for NIFTY / BANKNIFTY / FINNIFTY / any index + expiry.
"""
from __future__ import annotations
import marleg_options_monitor as om

INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]


def _levels(rows):
    data = []
    for r in rows:
        k = r.get("strike")
        if k is None:
            continue
        ce, pe = (r.get("ce") or {}), (r.get("pe") or {})
        data.append({"strike": float(k), "ce_oi": float(ce.get("oi") or 0), "pe_oi": float(pe.get("oi") or 0),
                     "ce_ltp": ce.get("ltp"), "pe_ltp": pe.get("ltp"), "is_atm": r.get("is_atm")})
    if not data or sum(d["ce_oi"] + d["pe_oi"] for d in data) == 0:
        return None
    strikes = [d["strike"] for d in data]

    def pain(K):
        return sum(d["ce_oi"] * max(0, K - d["strike"]) + d["pe_oi"] * max(0, d["strike"] - K) for d in data)

    maxpain = min(strikes, key=pain)
    cw = max(data, key=lambda d: d["ce_oi"])
    pw = max(data, key=lambda d: d["pe_oi"])
    tot_ce = sum(d["ce_oi"] for d in data) or 1
    tot_pe = sum(d["pe_oi"] for d in data)
    return {"data": data, "maxpain": maxpain, "call_wall": cw["strike"], "call_wall_oi": cw["ce_oi"],
            "put_wall": pw["strike"], "put_wall_oi": pw["pe_oi"], "pcr": round(tot_pe / tot_ce, 2)}


def _healthy(L):
    """Usable chain = real OI across several strikes + NON-DEGENERATE walls + a sane PCR. A contract
    expiring TODAY (DTE 0) can come back thin — SENSEX weeklies especially — with call wall == put wall
    and an absurd PCR. Those must be rolled past, not reported as real walls."""
    if not L:
        return False
    if L["call_wall"] == L["put_wall"]:                       # degenerate (dying/thin contract)
        return False
    live = sum(1 for d in L["data"] if (d["ce_oi"] + d["pe_oi"]) > 0)
    return live >= 5 and 0.1 <= L["pcr"] <= 12


def board(index="NIFTY", expiry=None):
    index = (index or "NIFTY").upper()
    c = om.chain(index, n=18, expiry=expiry)
    L = _levels(c.get("rows") or [])
    # Roll to the nearest LIVE weekly with a healthy chain. A contract expiring TODAY (DTE 0) is the
    # "dying" one — its OI thins into the close and can go degenerate (SENSEX weeklies especially) — so we
    # skip it and show the next live weekly, which is what actually frames the ongoing range. Fall back to
    # any healthy chain (even DTE 0) only if nothing ahead is usable. NIFTY on a normal day already sits on
    # a live weekly, so this is a no-op there.
    if not expiry:
        if not _healthy(L) or (c.get("days_to_expiry") or 0) < 1:
            picked = fallback = None
            for e in (c.get("expiries") or [])[:6]:
                ce = c if e == c.get("expiry") else om.chain(index, n=18, expiry=e)
                Le = L if e == c.get("expiry") else _levels(ce.get("rows") or [])
                if not _healthy(Le):
                    continue
                fallback = fallback or (ce, Le)
                if (ce.get("days_to_expiry") or 0) >= 1:       # the next LIVE weekly (skip today's expiry)
                    picked = (ce, Le)
                    break
            if picked:
                c, L = picked
            elif fallback:
                c, L = fallback
    if not L:
        return {"ok": False, "error": "no chain OI for %s" % index, "index": index}
    thin = not _healthy(L)                                     # flag if even the best chain is degenerate
    spot = c.get("spot")
    mp, cw, pw = L["maxpain"], L["call_wall"], L["put_wall"]

    if spot is None:
        pos = "spot unknown"
    elif spot >= cw * 0.999:
        pos = "AT/ABOVE the call wall — capped, reversal risk"
    elif spot <= pw * 1.001:
        pos = "AT/BELOW the put wall — support / bounce zone"
    else:
        pos = "inside the pin zone %d–%d" % (int(pw), int(cw))
    # distance to each wall
    up_room = round((cw / spot - 1) * 100, 2) if spot else None
    dn_room = round((pw / spot - 1) * 100, 2) if spot else None

    # top-3 resistance (CE OI) + support (PE OI) ladders
    d = L["data"]
    res = [{"strike": int(x["strike"]), "oi": int(x["ce_oi"])} for x in sorted(d, key=lambda z: -z["ce_oi"])[:3]]
    sup = [{"strike": int(x["strike"]), "oi": int(x["pe_oi"])} for x in sorted(d, key=lambda z: -z["pe_oi"])[:3]]

    return {
        "ok": True, "index": index, "spot": spot, "expiry": c.get("expiry"), "expiries": c.get("expiries"),
        "dte": c.get("days_to_expiry"), "vix": c.get("india_vix"), "thin": thin,
        "maxpain": mp, "call_wall": cw, "call_wall_oi": int(L["call_wall_oi"]),
        "put_wall": pw, "put_wall_oi": int(L["put_wall_oi"]), "pcr": L["pcr"],
        "position": pos, "pin_to": int(mp), "up_room_pct": up_room, "dn_room_pct": dn_room,
        "resistance": res, "support": sup, "rows": d,
        "note": ("Max-pain = the pin magnet (writers hedge toward it). Call wall = resistance (heaviest call "
                 "OI, writers defend). Put wall = support. On expiry day price gravitates to max-pain and "
                 "reverses at the walls → directional option BUYERS get chopped by theta + the pin; the WRITERS "
                 "win. So near a wall, don't BUY the breakout — fade it or trade the underlying. PCR is context "
                 "only — backtested, it does NOT predict direction."),
    }


if __name__ == "__main__":
    import sys, json
    try: sys.stdout.reconfigure(encoding="utf-8")
    except Exception: pass
    b = board("NIFTY")
    print(json.dumps({k: v for k, v in b.items() if k != "rows"}, default=str, indent=1))
