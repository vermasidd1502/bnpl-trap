"""
Groww trading API client for Marle-G.

- Resolves credentials in order: env vars -> groww-secrets plaintext file ->
  backup file -> encrypted .gpg (needs GROWW_VAULT_PASSWORD or interactive).
- Mints + caches the short-lived access token via the SHA256(secret+ts) checksum flow.
- Live data:  ltp(), quote(), ohlc()
- Account:    holdings(), positions(), orders(), margin()
- Trading:    place_order()  -- DRY-RUN by default; pass confirm=True to actually send.

Never prints the key / secret / minted token.

CLI:
  python groww_client.py --check                 # auth + probe every endpoint
  python groww_client.py --ltp RELIANCE,TCS      # live LTP
"""
import os, sys, time, json, hashlib, base64, subprocess, getpass
import requests

BASE = "https://api.groww.in"
API_VERSION = "1.0"
SECRETS_DIR = os.environ.get("GROWW_SECRETS_DIR", r"C:\Users\siddh\groww-secrets")
GPG_EXE = os.environ.get("GPG_EXE", r"C:\Program Files\Git\usr\bin\gpg.exe")


# --------------------------------------------------------------- credentials
def _parse_kv(text):
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'").strip()
    return out


def _from_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return _parse_kv(f.read())
    except OSError:
        return {}


def _from_gpg(path, password):
    try:
        p = subprocess.run(
            [GPG_EXE, "--batch", "--yes", "--pinentry-mode", "loopback",
             "--passphrase-fd", "0", "--decrypt", path],
            input=password.encode(), capture_output=True)
        if p.returncode == 0:
            return _parse_kv(p.stdout.decode("utf-8", "replace"))
    except OSError:
        pass
    return {}


def resolve_credentials(interactive=False):
    k, s = os.environ.get("GROWW_API_KEY"), os.environ.get("GROWW_API_SECRET")
    if k and s:
        return k, s, "env"
    for fname, tag in [("groww_credentials.txt", "file"),
                       ("groww_credentials.prev.txt", "backup")]:
        kv = _from_file(os.path.join(SECRETS_DIR, fname))
        if kv.get("GROWW_API_KEY") and kv.get("GROWW_API_SECRET"):
            return kv["GROWW_API_KEY"], kv["GROWW_API_SECRET"], tag
    gpg_path = os.path.join(SECRETS_DIR, "groww_credentials.txt.gpg")
    if os.path.exists(gpg_path):
        pw = os.environ.get("GROWW_VAULT_PASSWORD")
        if not pw and interactive:
            pw = getpass.getpass("Vault password: ")
        if pw:
            kv = _from_gpg(gpg_path, pw)
            if kv.get("GROWW_API_KEY") and kv.get("GROWW_API_SECRET"):
                return kv["GROWW_API_KEY"], kv["GROWW_API_SECRET"], "gpg"
    raise RuntimeError("No Groww credentials found (env / file / backup / gpg).")


def _jwt_exp(token):
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return int(json.loads(base64.urlsafe_b64decode(payload)).get("exp", 0))
    except Exception:
        return 0


def _safe_json(r):
    try:
        return r.json()
    except Exception:
        return {"_raw": r.text[:300]}


# --------------------------------------------------------------- client
class GrowwClient:
    def __init__(self, api_key=None, api_secret=None, interactive=False):
        if api_key and api_secret:
            self.api_key, self.api_secret, self.src = api_key, api_secret, "explicit"
        else:
            self.api_key, self.api_secret, self.src = resolve_credentials(interactive)
        self._token, self._token_exp = None, 0
        self._sess = requests.Session()

    # ---- auth ----
    def _mint(self):
        ts = str(int(time.time()))
        checksum = hashlib.sha256((self.api_secret + ts).encode("utf-8")).hexdigest()
        r = self._sess.post(
            f"{BASE}/v1/token/api/access",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json={"key_type": "approval", "checksum": checksum, "timestamp": ts}, timeout=15)
        r.raise_for_status()
        tok = (r.json() or {}).get("token")
        if not tok:
            raise RuntimeError(f"token mint returned no token: {r.text[:200]}")
        self._token = tok
        exp = _jwt_exp(tok)
        self._token_exp = exp if exp else int(time.time()) + 6 * 3600
        return tok

    def token(self):
        if not self._token or time.time() > self._token_exp - 60:
            self._mint()
        return self._token

    def _headers(self, post=False):
        h = {"Authorization": f"Bearer {self.token()}", "Accept": "application/json",
             "X-API-VERSION": API_VERSION}
        if post:
            h["Content-Type"] = "application/json"
        return h

    def _get(self, path, params=None):
        r = self._sess.get(f"{BASE}{path}", headers=self._headers(), params=params, timeout=15)
        if r.status_code == 401:
            self._token = None
            r = self._sess.get(f"{BASE}{path}", headers=self._headers(), params=params, timeout=15)
        return r

    def _post(self, path, body):
        r = self._sess.post(f"{BASE}{path}", headers=self._headers(post=True), json=body, timeout=15)
        if r.status_code == 401:
            self._token = None
            r = self._sess.post(f"{BASE}{path}", headers=self._headers(post=True), json=body, timeout=15)
        return r

    # ---- symbol helper ----
    @staticmethod
    def sym(s, exchange="NSE"):
        s = s.upper().strip()
        return s if "_" in s else f"{exchange}_{s}"

    # ---- live data ----
    def ltp(self, symbols, segment="CASH", exchange="NSE"):
        if isinstance(symbols, str):
            symbols = [x for x in symbols.split(",") if x.strip()]
        ex = ",".join(self.sym(s, exchange) for s in symbols)
        return self._get("/v1/live-data/ltp", {"segment": segment, "exchange_symbols": ex})

    def quote(self, symbol, segment="CASH", exchange="NSE"):
        return self._get("/v1/live-data/quote",
                         {"exchange": exchange, "segment": segment,
                          "trading_symbol": symbol.upper()})

    def quote_table(self, symbols, segment="CASH", exchange="NSE"):
        """Real-time {SYMBOL: {price, prev, chg}} built from ltp + ohlc (one call each)."""
        if isinstance(symbols, str):
            symbols = [x for x in symbols.split(",") if x.strip()]
        lr, orr = self.ltp(symbols, segment, exchange), self.ohlc(symbols, segment, exchange)
        lp = (_safe_json(lr).get("payload") or {}) if lr.status_code == 200 else {}
        oh = (_safe_json(orr).get("payload") or {}) if orr.status_code == 200 else {}
        out = {}
        for s in symbols:
            es = self.sym(s, exchange)
            price = lp.get(es)
            prev = (oh.get(es) or {}).get("close")
            row = {}
            if price is not None:
                row["price"] = round(float(price), 2)
            if prev:
                row["prev"] = round(float(prev), 2)
                if price is not None:
                    row["chg"] = round((float(price) / float(prev) - 1) * 100, 2)
            out[s.upper()] = row or {"error": "no data"}
        return out

    def ohlc(self, symbols, segment="CASH", exchange="NSE"):
        if isinstance(symbols, str):
            symbols = [x for x in symbols.split(",") if x.strip()]
        ex = ",".join(self.sym(s, exchange) for s in symbols)
        return self._get("/v1/live-data/ohlc", {"segment": segment, "exchange_symbols": ex})

    # ---- account ----
    def holdings(self):
        return self._get("/v1/holdings/user")

    def positions(self):
        return self._get("/v1/positions/user")

    def orders(self):
        return self._get("/v1/order/list")

    def margin(self):
        return self._get("/v1/margins/detail/user")

    # ---- parsed payload accessors (None on failure) ----
    @staticmethod
    def _payload(r):
        if r.status_code == 200:
            j = _safe_json(r)
            if j.get("status") == "SUCCESS":
                return j.get("payload")
        return None

    def holdings_data(self):
        return self._payload(self.holdings())

    def positions_data(self):
        return self._payload(self.positions())

    def orders_data(self):
        return self._payload(self.orders())

    def margin_data(self):
        return self._payload(self.margin())

    # ---- trading (guarded) ----
    def place_order(self, trading_symbol, transaction_type, quantity, *, exchange="NSE",
                    segment="CASH", product="CNC", order_type="MARKET", price=0,
                    trigger_price=0, validity="DAY", order_reference_id=None, confirm=False):
        transaction_type = transaction_type.upper()
        order_type = order_type.upper()
        if transaction_type not in ("BUY", "SELL"):
            raise ValueError("transaction_type must be BUY or SELL")
        if order_type not in ("MARKET", "LIMIT", "SL", "SL-M", "SL_M"):
            raise ValueError("order_type must be MARKET/LIMIT/SL/SL-M")
        body = {"trading_symbol": trading_symbol.upper(), "quantity": int(quantity),
                "validity": validity, "exchange": exchange, "segment": segment,
                "product": product, "order_type": order_type,
                "transaction_type": transaction_type}
        if order_type in ("LIMIT", "SL"):
            body["price"] = price
        if order_type in ("SL", "SL-M", "SL_M"):
            body["trigger_price"] = trigger_price
        if order_reference_id:
            body["order_reference_id"] = order_reference_id
        if not confirm:
            return {"dry_run": True, "would_send": body,
                    "note": "pass confirm=True to actually place this order"}
        r = self._post("/v1/order/create", body)
        return {"dry_run": False, "status_code": r.status_code, "response": _safe_json(r)}

    def modify_order(self, groww_order_id, *, trigger_price=None, price=None, quantity=None,
                     order_type=None, segment="CASH", confirm=False):
        body = {"groww_order_id": groww_order_id, "segment": segment}
        if trigger_price is not None:
            body["trigger_price"] = trigger_price
        if price is not None:
            body["price"] = price
        if quantity is not None:
            body["quantity"] = int(quantity)
        if order_type is not None:
            body["order_type"] = order_type.upper()
        if not confirm:
            return {"dry_run": True, "would_modify": body}
        r = self._post("/v1/order/modify", body)
        return {"dry_run": False, "status_code": r.status_code, "response": _safe_json(r)}


# --------------------------------------------------------------- CLI
def _check():
    c = GrowwClient(interactive=True)
    print(f"credentials source : {c.src}")
    print(f"api key {len(c.api_key)} chars / secret {len(c.api_secret)} chars (hidden)")
    try:
        c.token()
        print(f"token mint         : OK ({len(c._token)} chars, exp in "
              f"{int(c._token_exp - time.time())}s)")
    except Exception as e:
        print(f"token mint         : FAILED {e}")
        return
    probes = [
        ("ltp RELIANCE", lambda: c.ltp("RELIANCE")),
        ("quote RELIANCE", lambda: c.quote("RELIANCE")),
        ("ohlc RELIANCE", lambda: c.ohlc("RELIANCE")),
        ("holdings", c.holdings),
        ("positions", c.positions),
        ("orders", c.orders),
        ("margin", c.margin),
    ]
    for name, fn in probes:
        try:
            r = fn()
            body = json.dumps(_safe_json(r))
            print(f"  {name:<16} HTTP {r.status_code}  {body[:160]}")
        except Exception as e:
            print(f"  {name:<16} ERROR {str(e)[:120]}")


def _ltp(arg):
    c = GrowwClient(interactive=True)
    r = c.ltp(arg)
    print(f"HTTP {r.status_code}")
    print(json.dumps(_safe_json(r), indent=2))


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--check":
        _check()
    elif len(sys.argv) >= 3 and sys.argv[1] == "--ltp":
        _ltp(sys.argv[2])
    else:
        print(__doc__)
