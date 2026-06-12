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
    """Merge creds from gpg (lowest) -> files -> env (highest). Returns
    (api_key, api_secret, access_token, totp_secret, src). Valid if either
    (key+secret) for minting, or a ready access_token, is present."""
    merged = {}
    for fname in ("groww_credentials.prev.txt", "groww_credentials.txt"):
        merged.update(_from_file(os.path.join(SECRETS_DIR, fname)))
    for k in ("GROWW_API_KEY", "GROWW_API_SECRET", "GROWW_ACCESS_TOKEN", "GROWW_TOTP_SECRET"):
        if os.environ.get(k):
            merged[k] = os.environ[k]

    def _usable(m):
        return (m.get("GROWW_API_KEY") and m.get("GROWW_API_SECRET")) or m.get("GROWW_ACCESS_TOKEN")

    # gpg vault only as a fallback, so a working plaintext file never triggers a prompt
    gpg_path = os.path.join(SECRETS_DIR, "groww_credentials.txt.gpg")
    if not _usable(merged) and os.path.exists(gpg_path):
        pw = os.environ.get("GROWW_VAULT_PASSWORD")
        if not pw and interactive:
            pw = getpass.getpass("Vault password: ")
        if pw:
            for kk, vv in _from_gpg(gpg_path, pw).items():
                merged.setdefault(kk, vv)
    key = merged.get("GROWW_API_KEY", "")
    sec = merged.get("GROWW_API_SECRET", "")
    tok = merged.get("GROWW_ACCESS_TOKEN", "")
    totp = merged.get("GROWW_TOTP_SECRET", "")
    if not ((key and sec) or tok or (key and totp)):
        raise RuntimeError(
            "No usable Groww credentials. Need GROWW_API_KEY + (GROWW_API_SECRET for the "
            "approval flow OR GROWW_TOTP_SECRET for the TOTP flow), or a ready "
            f"GROWW_ACCESS_TOKEN, in env or {os.path.join(SECRETS_DIR, 'groww_credentials.txt')}.")
    return key, sec, tok, totp, "env/file"


def _jwt_exp(token):
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return int(json.loads(base64.urlsafe_b64decode(payload)).get("exp", 0))
    except Exception:
        return 0


def _totp(seed, digits=6, step=30):
    """RFC-6238 TOTP from a base32 seed (stdlib only; for Groww TOTP-flow keys)."""
    import hmac, struct
    s = seed.strip().replace(" ", "").upper()
    key = base64.b32decode(s + "=" * (-len(s) % 8))
    msg = struct.pack(">Q", int(time.time()) // step)
    h = hmac.new(key, msg, hashlib.sha1).digest()
    o = h[-1] & 0x0F
    code = (struct.unpack(">I", h[o:o + 4])[0] & 0x7FFFFFFF) % (10 ** digits)
    return str(code).zfill(digits)


def _safe_json(r):
    try:
        return r.json()
    except Exception:
        return {"_raw": r.text[:300]}


# --------------------------------------------------------------- client
class GrowwClient:
    def __init__(self, api_key=None, api_secret=None, interactive=False):
        if api_key and api_secret:
            self.api_key, self.api_secret = api_key, api_secret
            self.access_token, self.totp_secret, self.src = "", "", "explicit"
        else:
            (self.api_key, self.api_secret, self.access_token,
             self.totp_secret, self.src) = resolve_credentials(interactive)
        self._token, self._token_exp = None, 0
        self._sess = requests.Session()

    @property
    def auth_mode(self):
        if self.access_token:
            return "access-token (direct)"
        if self.totp_secret:
            return "TOTP-flow mint"
        return "approval-flow mint"

    # ---- auth ----
    def _token_cache_path(self):
        return os.path.join(SECRETS_DIR, "token_cache.json")

    def _mint(self):
        # 1) ready-made daily access token -> use directly, no mint call
        if self.access_token:
            self._token = self.access_token
            exp = _jwt_exp(self.access_token)
            self._token_exp = exp if exp else int(time.time()) + 6 * 3600
            return self._token
        # 1b) disk-cached session token (so loops/scripts never re-mint needlessly),
        #     plus a 429 backoff stamp so NOBODY hammers the daily mint rate-limit.
        try:
            d = json.load(open(self._token_cache_path()))
        except Exception:
            d = {}
        if d.get("token") and time.time() < d.get("exp", 0) - 120:
            self._token, self._token_exp = d["token"], d["exp"]
            return self._token
        if time.time() < d.get("backoff_until", 0):
            until = time.strftime("%H:%M", time.localtime(d["backoff_until"]))
            raise RuntimeError(f"mint backoff active until {until} (429 earlier — not retrying)")
        # 2) TOTP flow (Bearer = TOTP token, body carries the 6-digit code) -> self-
        #    renews, NO api_secret/manual approval; else approval flow (key+secret checksum)
        if self.totp_secret:
            if not self.api_key:
                raise RuntimeError("TOTP flow needs GROWW_API_KEY (the TOTP token)")
            body = {"key_type": "totp", "totp": _totp(self.totp_secret)}
        else:
            if not (self.api_key and self.api_secret):
                raise RuntimeError("need API key+secret to mint, or set GROWW_ACCESS_TOKEN / GROWW_TOTP_SECRET")
            ts = str(int(time.time()))
            checksum = hashlib.sha256((self.api_secret + ts).encode("utf-8")).hexdigest()
            body = {"key_type": "approval", "checksum": checksum, "timestamp": ts}
        r = self._sess.post(
            f"{BASE}/v1/token/api/access",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json=body, timeout=15)
        if r.status_code == 429:
            d["backoff_until"] = int(time.time()) + 6 * 3600
            try:
                json.dump(d, open(self._token_cache_path(), "w"))
            except Exception:
                pass
            raise RuntimeError("429 rate-limited; backoff set for 6h (no further mint attempts)")
        r.raise_for_status()
        tok = (r.json() or {}).get("token")
        if not tok:
            raise RuntimeError(f"token mint returned no token: {r.text[:200]}")
        self._token = tok
        exp = _jwt_exp(tok)
        self._token_exp = exp if exp else int(time.time()) + 6 * 3600
        try:
            json.dump({"token": tok, "exp": self._token_exp}, open(self._token_cache_path(), "w"))
        except Exception:
            pass
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

    def candles(self, symbol, interval_min=5, days=60, exchange="NSE", segment="CASH"):
        """Official Groww OHLCV (IST-indexed DataFrame). interval_min: 1/5/10/15/60/1440.
        Intraday intervals cap at ~15 days/request, so this AUTO-CHUNKS deeper windows.
        Returns columns open/high/low/close/volume, or None."""
        import pandas as pd, time as _t
        from datetime import datetime, timedelta
        chunk = 15 if interval_min < 60 else 365
        end = datetime.now(); start_all = end - timedelta(days=days)
        frames, cur = [], end
        while cur > start_all:
            s = max(start_all, cur - timedelta(days=chunk))
            try:
                r = self._get("/v1/historical/candle/range", params={
                    "exchange": exchange, "segment": segment, "trading_symbol": symbol.upper(),
                    "start_time": s.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": cur.strftime("%Y-%m-%d %H:%M:%S"),
                    "interval_in_minutes": str(interval_min)})
                if r.status_code == 200:
                    rows = ((r.json().get("payload") or {}).get("candles")) or []
                    if rows:
                        frames.append(pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"]))
            except Exception:
                pass
            cur = s - timedelta(seconds=1)
            if days > chunk:
                _t.sleep(0.4)
        if not frames:
            return None
        df = pd.concat(frames).drop_duplicates(subset="ts").sort_values("ts")
        df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
        return df.set_index("dt")[["open", "high", "low", "close", "volume"]]

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
    print(f"auth mode          : {c.auth_mode}")
    print(f"api key {len(c.api_key or '')} chars / secret {len(c.api_secret or '')} chars / "
          f"token {len(c.access_token or '')} chars / totp-seed {'set' if c.totp_secret else 'no'} (hidden)")
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
