"""
Marle-G Slack bridge — posts alerts / reports / approval requests to a Slack
Incoming Webhook. No MCP needed; works headless and on Groww Cloud (stdlib only).

SAFE BY DEFAULT: if MARLEG_SLACK_WEBHOOK is not set, every call is a no-op that
just prints locally — nothing leaves the machine until you opt in.

Activate:
  1. Slack -> create an app -> enable "Incoming Webhooks" -> add to a channel.
  2. Copy the webhook URL.
  3. set MARLEG_SLACK_WEBHOOK=<url>   (env var)

Use:
  from marleg_slack import notify
  notify("🟢 Cascade fired: oil-shock basket", fields={"legs": "6", "risk": "Rs 9,800"})
"""
import os, json, urllib.request
from datetime import datetime, timezone, timedelta

WEBHOOK = os.environ.get("MARLEG_SLACK_WEBHOOK", "")
_IST = timezone(timedelta(hours=5, minutes=30))


def _stamp():
    """Dual clock — the pod rule: always show IST + US-Central."""
    ist = datetime.now(_IST); ct = ist - timedelta(hours=10, minutes=30)   # CDT
    return f"{ist:%a %d %b %H:%M} IST · {ct:%H:%M} CT"


def build(headline, lines=None, action=None, foot=True):
    """Assemble a COMPLETE message: headline + context bullets + an explicit action + dual-clock
    footer. Use this everywhere so every alert is self-contained, not a cryptic one-liner."""
    out = headline
    if lines:
        out += "\n" + "\n".join("• " + str(l) for l in lines if l)
    if action:
        out += "\n\n➤ *Do:* " + action
    if foot:
        out += "\n_" + _stamp() + " · Marle-G · monitor-only, your call_"
    return out


def card(headline, lines=None, fields=None, action=None, foot=True):
    """build() + send. Returns True if sent."""
    return notify(build(headline, lines, action, foot), fields)


def enabled():
    return bool(WEBHOOK)


def _p(*a):
    """Console print that never dies on Windows cp1252 (alerts carry emoji)."""
    s = " ".join(str(x) for x in a)
    try:
        print(s)
    except UnicodeEncodeError:
        print(s.encode("ascii", "replace").decode())


def notify(text, fields=None):
    """Post a message. Returns True if sent, False if no-op/failed. Never raises."""
    if not WEBHOOK:
        _p("[slack:off]", text, ("| " + json.dumps(fields)) if fields else "")
        return False
    payload = {"text": text}
    if fields:
        payload["blocks"] = [
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
            {"type": "section", "fields": [{"type": "mrkdwn", "text": f"*{k}*\n{v}"} for k, v in fields.items()]},
        ]
    try:
        req = urllib.request.Request(WEBHOOK, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=8)
        return True
    except Exception as e:
        _p("[slack:error]", str(e)[:100])
        return False


if __name__ == "__main__":
    ok = notify("🧪 Marle-G Slack bridge test",
                fields={"status": "wired", "mode": "live" if enabled() else "no-op (set MARLEG_SLACK_WEBHOOK)"})
    _p("sent to Slack" if ok else "no webhook set — printed locally only (safe).")
