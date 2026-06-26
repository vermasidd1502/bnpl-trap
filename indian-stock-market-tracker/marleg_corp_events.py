"""
marleg_corp_events.py — corporate-events & distress gate for the cockpit.

Two RELIABLE, fast NSE sources (verified reachable):
  • Corporate ANNOUNCEMENTS (deals / orders / M&A / fund-raising / rating / default) — near-real-time JSON.
  • ASM + GSM SURVEILLANCE lists — the exchange's OWN risk flag (Additional / Graded Surveillance Measure).
    A name on ASM/GSM = the exchange itself warning of speculative/abnormal/distressed activity → a hard CAUTION.

gate(sym) → a verdict the council can use:
  • on ASM/GSM, or a fresh DEFAULT/insolvency/downgrade filing  → NEGATIVE (veto-grade distress flag)
  • a fresh DEAL / big ORDER / fund-raise                        → small POSITIVE catalyst (honest: often priced-in)
  • nothing material                                             → neutral

Read-only, decision-support. Honest: deals are a weak edge (priced in / PEAD = react to the gap); the DISTRESS
side (ASM/GSM/default) is the genuinely valuable gate — it keeps you OUT of manipulated/distressed names.
"""
import time as _time
import datetime as dt

import requests

_H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36", "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}
_REF = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"

_sess = None
_sess_t = 0.0
_ASM = {"t": 0.0, "data": None}
_ANN = {}                      # per-symbol announcement cache {sym: (t, list)}

DISTRESS = ("default", "delinquen", "insolven", "nclt", "ibc", " cirp", "winding up", "downgrade",
            "rating ... d", "auditor resign", "fraud", "liquidation", "lenders", "one-time settlement",
            "sdr", "wilful default", "pledge invoc")
DEAL = ("acquisition", "acquir", "merger", "amalgamat", "joint venture", " jv ", "stake", "mou ",
        "memorandum of understanding", "collaboration", "partnership", "agreement", "definitive")
ORDER = ("order", "contract", "bagged", "letter of award", " loa", "work order", "won ", "secures", "awarded")
FUNDRAISE = ("qip", "preferential", "rights issue", "fund rais", "fundrais", "allotment", "ncd ",
             "debenture", "warrants", "capital raise")
RESULT = ("financial result", "board meeting", "quarterly", "audited")


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


def surveillance():
    """ASM + GSM lists → {SYMBOL: {'asm': stage, 'gsm': stage}}. Memoised 1h."""
    now = _time.time()
    if _ASM["data"] is not None and (now - _ASM["t"]) < 3600:
        return _ASM["data"]
    out, s = {}, _session()
    try:
        j = s.get("https://www.nseindia.com/api/reportASM", headers={**_H, "Referer": "https://www.nseindia.com/"},
                  timeout=12).json()
        for seg in ("longterm", "shortterm"):
            for x in ((j.get(seg) or {}).get("data") or []):
                sym = x.get("symbol")
                if sym:
                    out.setdefault(sym.upper(), {})["asm"] = x.get("asmSurvIndicator") or (seg + " ASM")
    except Exception:
        pass
    try:
        j = s.get("https://www.nseindia.com/api/reportGSM", headers={**_H, "Referer": "https://www.nseindia.com/"},
                  timeout=12).json()
        rows = j.get("data") if isinstance(j, dict) else (j if isinstance(j, list) else [])
        for x in (rows or []):
            sym = x.get("symbol")
            if sym:
                out.setdefault(sym.upper(), {})["gsm"] = x.get("gsmStage") or x.get("gsmIndicator") or "GSM"
    except Exception:
        pass
    _ASM.update(t=now, data=out)
    return out


def _categorize(text):
    t = (text or "").lower()
    if any(k in t for k in DISTRESS):
        return "DISTRESS"
    if any(k in t for k in DEAL):
        return "DEAL"
    if any(k in t for k in ORDER):
        return "ORDER"
    if any(k in t for k in FUNDRAISE):
        return "FUNDRAISE"
    if any(k in t for k in RESULT):
        return "RESULT"
    return "OTHER"


def announcements(sym, n=12):
    sym = (sym or "").upper()
    now = _time.time()
    if sym in _ANN and (now - _ANN[sym][0]) < 300:
        return _ANN[sym][1]
    out, s = [], _session()
    try:
        rows = s.get("https://www.nseindia.com/api/corporate-announcements?index=equities&symbol=" + sym,
                     headers={**_H, "Referer": _REF}, timeout=12).json()
        if isinstance(rows, list):
            for x in rows[:n]:
                subj = x.get("desc") or x.get("subject") or ""
                body = x.get("attchmntText") or x.get("smText") or ""
                cat = _categorize(subj + " " + body)
                out.append({"date": x.get("an_dt", ""), "subject": subj[:120], "cat": cat})
    except Exception:
        pass
    _ANN[sym] = (now, out)
    return out


def gate(sym):
    sym = (sym or "").upper()
    surv = surveillance().get(sym, {})
    anns = announcements(sym)
    # distress filing in the last ~10 days?
    distress_recent = next((a for a in anns if a["cat"] == "DISTRESS"), None)
    positive = [a for a in anns if a["cat"] in ("DEAL", "ORDER", "FUNDRAISE")]
    if surv.get("gsm"):
        verdict, tone, score = "AVOID — GSM surveillance", "bad", -3
        why = f"on the GSM list ({surv['gsm']}) — exchange flags it speculative/illiquid; circuit-bound, manipulation risk."
    elif surv.get("asm"):
        verdict, tone, score = "CAUTION — ASM surveillance", "warn", -2
        why = f"on the ASM list ({surv['asm']}) — abnormal activity; higher margins, tighter circuits."
    elif distress_recent:
        verdict, tone, score = "AVOID — distress filing", "bad", -3
        why = f"recent distress disclosure: {distress_recent['subject']}"
    elif positive:
        verdict, tone, score = "CATALYST — fresh deal/order", "good", 1
        why = positive[0]["cat"] + ": " + positive[0]["subject"]
    else:
        verdict, tone, score = "clean — no flag", "normal", 0
        why = "no surveillance flag, no material deal/distress filing in the recent window."
    return {"ok": True, "sym": sym, "verdict": verdict, "tone": tone, "score": score,
            "surveillance": surv or None, "recent": anns[:6], "why": why,
            "asof": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST"),
            "caveat": "Distress/ASM/GSM = a genuine AVOID gate (exchange's own risk flag). Deals/orders are a WEAK "
                      "edge (priced in; PEAD says react to the gap, not the headline). Read-only decision-support."}


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    surv = surveillance()
    print(f"\n  ASM/GSM universe: {len(surv)} flagged names (sample: {list(surv.items())[:3]})")
    for u in (sys.argv[1:] or ["AMBER", "RELIANCE"]):
        g = gate(u)
        print(f"\n  {g['sym']}: {g['verdict']}  (score {g['score']})")
        print(f"    {g['why']}")
        for a in g["recent"][:4]:
            print(f"    · [{a['cat']}] {a['date']} — {a['subject'][:70]}")
