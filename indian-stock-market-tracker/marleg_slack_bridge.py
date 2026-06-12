"""
Marle-G — SLACK BRIDGE (two-way). Type a command in Slack, the pod does the work and
replies. Polls the channel (no public URL / webhook server needed — works behind NAT).

SAFETY (hard line): this bridge READS, COMPUTES, MONITORS, and PAPER-trades on command.
It NEVER places, modifies, or cancels a real order. Order intents ("buy/sell/place/stop")
get a PREPARED TICKET back — you place it in Groww, or let the armed cloud guardian do it.
Real-order automation lives ONLY in the cloud guardian (mechanical, you-armed), never on
free-text chat commands (that would be an injection/finance hole).

Commands:
  status | book          live positions + P&L + protection coverage
  coverage | stops       which positions are UNPROTECTED + the exact SL ticket
  target <SYM>           fib targets + diffusion ETA
  gate <SYM>             7-gate pre-trade verdict + size
  vol <SYM>              volume/ud read + zone
  radar | gated          short-radar / gated-longs top picks
  paper <SYM>            try the momentum strategy as a PAPER trade
  help

Setup (one-time, YOUR side):
  1. api.slack.com/apps -> Create App -> add a Bot. Scopes: chat:write, channels:history
     (or groups:history for a private channel). Install to workspace.
  2. Invite the bot to your channel. Copy the Bot token (xoxb-...) and channel ID.
  3. Put in env / secrets (NEVER in chat):  MARLEG_SLACK_BOT_TOKEN, MARLEG_SLACK_CHANNEL
  4. python marleg_slack_bridge.py     # leave running; talk to it in Slack
"""
import os, sys, time, json
import requests

BOT = os.environ.get("MARLEG_SLACK_BOT_TOKEN", "")
CHAN = os.environ.get("MARLEG_SLACK_CHANNEL", "")
API = "https://slack.com/api"
POLL = 8
ORDER_WORDS = ("buy", "sell", "place", "short", "exit", "square", "order", "modify", "cancel")


def post(text):
    if not BOT or not CHAN:
        print("[bridge:no-token]", text[:200]); return
    try:
        requests.post(API + "/chat.postMessage",
                      headers={"Authorization": "Bearer " + BOT},
                      json={"channel": CHAN, "text": text}, timeout=8)
    except Exception as e:
        print("post err", str(e)[:80])


# ---------------- command handlers (read / compute / paper only) ----------------
def cmd_status():
    try:
        from marleg_check_stops import load_positions
        from marleg_stop_guardian import live_sl_coverage
        pos, src = load_positions()
        cover = live_sl_coverage()
        lines = [f"*Book* ({src.split('(')[0].strip()}):"]
        naked = 0
        for p in pos:
            if p.get("type") == "OPT":
                lines.append(f"• {p['symbol']} OPT {p.get('side','')} {p.get('qty')}")
                continue
            q = p.get("qty") or 0
            prot = "✓" if cover.get(p["symbol"], 0) >= q * 0.9 else "⛔ NAKED"
            if "⛔" in prot:
                naked += 1
            lines.append(f"• {p['symbol']} {p.get('type','')} {q} @ {p.get('entry')} — {prot}")
        if naked:
            lines.append(f"\n⛔ *{naked} position(s) UNPROTECTED* — send `coverage` for tickets.")
        return "\n".join(lines)
    except Exception as e:
        return "status error: " + str(e)[:120]


def cmd_coverage():
    try:
        from marleg_check_stops import load_positions
        from marleg_stop_guardian import protection_report, _load_state, assess, _load_state as _ls
        import marleg_stop_guardian as g
        pos, src = load_positions()
        st = g._load_state()
        px = g._live_px(list({p.get("underlying") or p["symbol"] for p in pos if p.get("type") != "OPT"}))
        rows = []
        for p in pos:
            if p.get("type") == "OPT":
                continue
            r = assess(p, st, g.K_DEFAULT, px)
            if r:
                rows.append(r)
        rep = protection_report(rows, st)
        if not rep["naked"]:
            return "✓ All positions have a live stop order at the broker."
        out = ["⛔ *UNPROTECTED — place these SL-M SELL tickets:*"]
        for n in rep["naked"]:
            out.append(f"• {n['sym']} {n['qty']} → trigger {n['suggest_stop']}")
        out.append("\n(Place in Groww, or arm the cloud guardian to auto-place.)")
        return "\n".join(out)
    except Exception as e:
        return "coverage error: " + str(e)[:120]


def cmd_target(sym):
    try:
        import marleg_fibmap as fm
        d = fm.fibmap(sym)
        if d.get("error"):
            return d["error"]
        eta = d.get("upside_eta", [])[:4]
        levels = "  ".join(f"{e['level'].split()[-1]} {e['price']} (+{e['dist_pct']}% {e['eta']})" for e in eta)
        conf = d.get("confluence", [])
        cl = ("\nconfluence: " + "; ".join(f"{c['macro_level']}≈{c['micro_level']} @ {c['price']}" for c in conf[:2])) if conf else ""
        return f"*{sym}* {d['live']} · targets: {levels or 'none above'}{cl}"
    except Exception as e:
        return "target error: " + str(e)[:120]


def cmd_gate(sym):
    try:
        import marleg_pretrade as pt
        g = pt.gate_data(sym)
        if g.get("error"):
            return g["error"]
        flags = f"T{'✓' if g['g1'] else '✗'} V{'✓' if g['g2'] else '✗'} S{'✓' if g['g3'] else '✗' if g['g3'] is not None else '?'}"
        return (f"*{sym}* gate: *{g['verdict']}* ({flags}) · ud {g['ud']:.2f} · f* {g['f_star']:.2f} · "
                f"ATR {g['atr_pct']:.1f}%/d · stop {g['stop_px']} · size {g['qty']}\n"
                + ("; ".join(g['cautions']) if g['cautions'] else "clean"))
    except Exception as e:
        return "gate error: " + str(e)[:120]


def cmd_vol(sym):
    try:
        import marleg_pretrade as pt
        g = pt.gate_data(sym)
        if g.get("error"):
            return g["error"]
        zone = ("go-to" if 2.0 <= g["ud"] < 3.6 else "ok" if g["ud"] < 2.0 else "hot" if g["ud"] < 6 else "EXTREME")
        return f"*{sym}* ud {g['ud']:.2f} ({zone}) · vs50 {g['px']/g['sma50']-1:+.0%} vs200 {g['px']/g['sma200']-1:+.0%} · px {g['px']:.1f}"
    except Exception as e:
        return "vol error: " + str(e)[:120]


def cmd_radar():
    try:
        import marleg_short_radar as sr
        r = sr.scan()
        cs = r.get("candidates", [])[:5]
        if not cs:
            return "🌑 short radar: no eclipse candidates right now."
        return "🌑 *short radar:*\n" + "\n".join(f"• {c['sym']} {c['verdict']} (rsi {c['rsi_y']:.0f}, ud {c['ud']})" for c in cs)
    except Exception as e:
        return "radar error: " + str(e)[:120]


def cmd_paper(sym):
    try:
        import marleg_strategies as s
        r = s.paper_trade("momentum_breakout", sym)
        if r.get("opened"):
            p = r["position"]
            return f"📄 PAPER bought {p['qty']} {sym} @ {p['entry']} (stop {p['stop']}, tgt {p['target']})"
        return f"📄 {sym}: setup not confirmed — {('; '.join((r.get('eval') or {}).get('reasons', []))) or r.get('msg','')}"
    except Exception as e:
        return "paper error: " + str(e)[:120]


HELP = ("*Marle-G Slack bridge* — I read/compute/paper, never place real orders.\n"
        "`status` book+coverage · `coverage` SL tickets · `target X` · `gate X` · `vol X` · "
        "`radar` · `paper X` · `help`")


def handle(text):
    t = text.strip().lower()
    parts = t.split()
    if not parts:
        return None
    cmd = parts[0]
    arg = parts[1].upper() if len(parts) > 1 else None
    if any(w in t for w in ORDER_WORDS) and cmd not in ("paper",):
        sym = arg or "<SYM>"
        return ("🚫 I won't place real orders from chat (safety). Send `coverage` for the exact "
                "SL ticket to tap in Groww, or `gate " + sym + "` to size a new trade. "
                "Real auto-placement = arm the cloud guardian.")
    if cmd in ("status", "book"):
        return cmd_status()
    if cmd in ("coverage", "stops", "protect"):
        return cmd_coverage()
    if cmd in ("target", "fib", "fibmap") and arg:
        return cmd_target(arg)
    if cmd == "gate" and arg:
        return cmd_gate(arg)
    if cmd in ("vol", "volume", "ud") and arg:
        return cmd_vol(arg)
    if cmd in ("radar", "short"):
        return cmd_radar()
    if cmd == "paper" and arg:
        return cmd_paper(arg)
    if cmd in ("help", "?", "commands"):
        return HELP
    return None    # ignore chatter that isn't a command


def run():
    if not BOT or not CHAN:
        print("Slack bridge: set MARLEG_SLACK_BOT_TOKEN and MARLEG_SLACK_CHANNEL (see header). "
              "Running in OFFLINE test mode — type commands here:")
        for line in sys.stdin:
            r = handle(line)
            print(r or "(not a command)")
        return
    print("Slack bridge live — polling every", POLL, "s. READ/PAPER only; never places orders.", flush=True)
    post("🟢 Marle-G bridge online. `help` for commands. (I never place real orders.)")
    last_ts = str(time.time())
    while True:
        try:
            r = requests.get(API + "/conversations.history",
                             headers={"Authorization": "Bearer " + BOT},
                             params={"channel": CHAN, "oldest": last_ts, "limit": 20}, timeout=10)
            msgs = sorted((r.json() or {}).get("messages", []), key=lambda m: float(m.get("ts", 0)))
            for m in msgs:
                if m.get("bot_id") or m.get("subtype"):
                    continue
                last_ts = m.get("ts", last_ts)
                reply = handle(m.get("text", ""))
                if reply:
                    post(reply)
        except Exception as e:
            print("poll err", str(e)[:80], flush=True)
        time.sleep(POLL)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    run()


if __name__ == "__main__":
    main()
