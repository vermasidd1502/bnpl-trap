"""
Synthetic Options Monitor -- complete simulated options book
============================================================

Lets you paper-trade options positions with full pricing, Greeks, IV solver,
mark-to-market, and P&L tracking. Designed to express the BSI thesis with
asymmetric payoffs (long puts on fired names) rather than equity shorts,
and to maintain Taleb-style convex hedges (long OTM calls on positions).

Why this matters: an equity short loses unbounded money on a short squeeze;
a long put loses at most the premium. For high-conviction BSI fires on
crowded shorts (KLAR, WRLD), options are the right instrument.

Components
----------
  BlackScholes      -- pricing + Greeks (delta, gamma, theta, vega)
  implied_vol       -- Newton-Raphson IV solver from market mid
  OptionsBook       -- position tracking, mark, P&L per position
  monitor_book()    -- daily mark-to-market + Greeks aggregation report

Schema (auto-created in apollo.db)
----------------------------------
  options_journal           one row per opened position
  options_position_sizing   daily mark with Greeks per position

CLI
---
  python synthetic_options.py --price CRMT 10 2026-07-18 PUT
  python synthetic_options.py --chain CRMT --expiry 2026-07-18
  python synthetic_options.py --fire CRMT PUT 10 2026-07-18 5
  python synthetic_options.py --monitor --username sid
  python synthetic_options.py --status

Academic anchors
----------------
  Black & Scholes (1973) "The Pricing of Options and Corporate Liabilities"
  Merton (1973) "Theory of Rational Option Pricing"
  Taleb (2009) on convex tail hedges
"""
from __future__ import annotations

import argparse
import math
import os
import sqlite3
import sys
import warnings
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import yfinance as yf
from scipy.stats import norm

warnings.filterwarnings("ignore")

DEFAULT_DB = os.environ.get(
    "APOLLO_DB",
    str(Path(__file__).resolve().parents[1] / "data" / "apollo.db"),
)

# Risk-free rate (short Treasury). Tune via env var.
DEFAULT_R = float(os.environ.get("OPTIONS_RISK_FREE_RATE", 0.045))

# Schema -------------------------------------------------------------------

SCHEMA_DDL = [
    """CREATE TABLE IF NOT EXISTS options_journal (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        username      TEXT NOT NULL,
        ts            TEXT NOT NULL,
        ticker        TEXT NOT NULL,
        option_type   TEXT NOT NULL,   -- 'CALL' | 'PUT'
        side          TEXT NOT NULL,   -- 'LONG' | 'SHORT'
        strike        REAL NOT NULL,
        expiry        TEXT NOT NULL,   -- YYYY-MM-DD
        contracts     INTEGER NOT NULL,
        entry_premium REAL NOT NULL,
        entry_underlying REAL,
        entry_iv      REAL,
        status        TEXT NOT NULL DEFAULT 'OPEN',
        exit_premium  REAL,
        exit_ts       TEXT,
        verdict       TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS options_position_sizing (
        journal_id        INTEGER NOT NULL,
        ts                TEXT NOT NULL,
        underlying_price  REAL,
        mark_premium      REAL,
        iv                REAL,
        delta             REAL,
        gamma             REAL,
        theta             REAL,
        vega              REAL,
        days_to_expiry    INTEGER,
        pnl_usd           REAL,
        PRIMARY KEY (journal_id, ts)
    )""",
    """CREATE INDEX IF NOT EXISTS idx_opts_journal_user ON options_journal(username, status)""",
    """CREATE INDEX IF NOT EXISTS idx_opts_sizing_ts ON options_position_sizing(ts)""",
]


def migrate(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    for stmt in SCHEMA_DDL:
        cur.execute(stmt)
    con.commit()


# ============================================================================
# Black-Scholes pricing + Greeks
# ============================================================================

@dataclass
class Greeks:
    price: float
    delta: float
    gamma: float
    theta: float   # per CALENDAR day
    vega: float    # per 1% change in vol (absolute, not %)
    rho: float

    def to_dict(self) -> dict:
        return asdict(self)


class BlackScholes:
    """Pricing + Greeks for European-style options.

    Treats US single-name options as European for pricing purposes (the early-
    exercise premium is small for puts on dividend-paying stocks and zero for
    calls on non-dividend names; good enough for paper-book tracking).
    """

    @staticmethod
    def _d1_d2(S: float, K: float, T: float, sigma: float, r: float) -> tuple[float, float]:
        if T <= 0 or sigma <= 0:
            # Edge: at expiry, d1/d2 are degenerate; downstream code handles
            return (float("inf"), float("inf"))
        d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
        d2 = d1 - sigma * math.sqrt(T)
        return d1, d2

    @classmethod
    def price(cls, S: float, K: float, T: float, sigma: float, r: float,
              option_type: str) -> float:
        """T in years, sigma annualized."""
        if T <= 0:
            # Intrinsic value at expiry
            if option_type.upper() == "CALL":
                return max(0.0, S - K)
            return max(0.0, K - S)
        d1, d2 = cls._d1_d2(S, K, T, sigma, r)
        if option_type.upper() == "CALL":
            return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

    @classmethod
    def greeks(cls, S: float, K: float, T: float, sigma: float, r: float,
               option_type: str) -> Greeks:
        if T <= 0 or sigma <= 0:
            return Greeks(
                price=cls.price(S, K, max(T, 1e-9), max(sigma, 1e-9), r, option_type),
                delta=1.0 if (option_type.upper() == "CALL" and S > K)
                      else -1.0 if (option_type.upper() == "PUT" and S < K) else 0.0,
                gamma=0.0, theta=0.0, vega=0.0, rho=0.0,
            )
        d1, d2 = cls._d1_d2(S, K, T, sigma, r)
        npdf_d1 = norm.pdf(d1)
        sqrt_T = math.sqrt(T)
        price = cls.price(S, K, T, sigma, r, option_type)
        if option_type.upper() == "CALL":
            delta = norm.cdf(d1)
            theta_yr = (-(S * npdf_d1 * sigma) / (2 * sqrt_T)
                        - r * K * math.exp(-r * T) * norm.cdf(d2))
            rho = K * T * math.exp(-r * T) * norm.cdf(d2) / 100.0
        else:
            delta = norm.cdf(d1) - 1.0
            theta_yr = (-(S * npdf_d1 * sigma) / (2 * sqrt_T)
                        + r * K * math.exp(-r * T) * norm.cdf(-d2))
            rho = -K * T * math.exp(-r * T) * norm.cdf(-d2) / 100.0
        gamma = npdf_d1 / (S * sigma * sqrt_T)
        vega = S * npdf_d1 * sqrt_T / 100.0   # per 1% vol
        theta = theta_yr / 365.0              # per calendar day
        return Greeks(price=price, delta=delta, gamma=gamma, theta=theta,
                      vega=vega, rho=rho)


def implied_vol(market_price: float, S: float, K: float, T: float, r: float,
                option_type: str, *, max_iter: int = 50, tol: float = 1e-5) -> float:
    """Newton-Raphson IV solver. Returns NaN if no solution."""
    if market_price <= 0 or T <= 0:
        return float("nan")
    sigma = 0.30  # initial guess
    for _ in range(max_iter):
        bs_price = BlackScholes.price(S, K, T, sigma, r, option_type)
        g = BlackScholes.greeks(S, K, T, sigma, r, option_type)
        vega = g.vega * 100.0  # vega is per 1%; convert to per unit
        if vega < 1e-8:
            break
        diff = bs_price - market_price
        if abs(diff) < tol:
            return float(sigma)
        sigma -= diff / vega
        sigma = max(0.001, min(5.0, sigma))   # clamp
    return float(sigma)


# ============================================================================
# Market data -- yfinance options chain wrapper
# ============================================================================

_OPT_CHAIN_CACHE: dict[tuple, "pd.DataFrame"] = {}


def fetch_option_chain(ticker: str, expiry: str, option_type: str) -> "pd.DataFrame":
    """Returns DataFrame with strike, bid, ask, lastPrice, impliedVolatility, etc.

    expiry: YYYY-MM-DD; option_type: 'CALL' or 'PUT'.
    """
    key = (ticker, expiry, option_type.upper())
    if key in _OPT_CHAIN_CACHE:
        return _OPT_CHAIN_CACHE[key]
    try:
        chain = yf.Ticker(ticker).option_chain(expiry)
        df = chain.calls if option_type.upper() == "CALL" else chain.puts
    except Exception:
        import pandas as pd
        df = pd.DataFrame()
    _OPT_CHAIN_CACHE[key] = df
    return df


def available_expiries(ticker: str) -> list[str]:
    try:
        return list(yf.Ticker(ticker).options)
    except Exception:
        return []


def last_underlying_price(ticker: str) -> Optional[float]:
    try:
        h = yf.Ticker(ticker).history(period="5d", auto_adjust=True)
        if h.empty:
            return None
        return float(h["Close"].iloc[-1])
    except Exception:
        return None


def lookup_option_quote(
    ticker: str, strike: float, expiry: str, option_type: str,
) -> Optional[dict]:
    """Returns {bid, ask, last, mid, iv_market} or None if not found."""
    df = fetch_option_chain(ticker, expiry, option_type)
    if df.empty:
        return None
    row = df[(df["strike"] == strike)]
    if row.empty:
        # try nearest strike
        nearest = df.iloc[(df["strike"] - strike).abs().argsort()[:1]]
        if nearest.empty:
            return None
        row = nearest
    r = row.iloc[0]
    bid = float(r.get("bid", 0) or 0)
    ask = float(r.get("ask", 0) or 0)
    last = float(r.get("lastPrice", 0) or 0)
    iv = float(r.get("impliedVolatility", 0) or 0)
    mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else last
    return {"strike": float(r["strike"]), "bid": bid, "ask": ask, "last": last,
            "mid": mid, "iv_market": iv}


# ============================================================================
# OptionsBook -- position tracking
# ============================================================================

class OptionsBook:
    def __init__(self, db_path: str, username: str = "default"):
        self.con = sqlite3.connect(db_path)
        self.username = username
        migrate(self.con)

    # ---------- entries ----------

    def fire_position(
        self,
        ticker: str,
        option_type: str,
        side: str,
        strike: float,
        expiry: str,
        contracts: int,
        *,
        verdict: str = "",
        urgency: str = "normal",
    ) -> int:
        """Open a synthetic option position. Returns journal_id."""
        if option_type.upper() not in ("CALL", "PUT"):
            raise ValueError("option_type must be CALL or PUT")
        if side.upper() not in ("LONG", "SHORT"):
            raise ValueError("side must be LONG or SHORT")

        q = lookup_option_quote(ticker, strike, expiry, option_type)
        if q is None:
            raise RuntimeError(f"no option chain quote for {ticker} {strike}{option_type} {expiry}")
        # Realistic fill: long pays ask, short receives bid; urgency tightens
        from execution_gates import simulate_fill  # piggyback bundle
        if side.upper() == "LONG":
            premium = simulate_fill("LONG", q["mid"], 0.0 if q["ask"] == 0
                                     else 10_000 * (q["ask"] - q["bid"]) / max(q["mid"], 1e-9),
                                     urgency=urgency)
        else:
            premium = simulate_fill("SHORT", q["mid"], 0.0 if q["ask"] == 0
                                     else 10_000 * (q["ask"] - q["bid"]) / max(q["mid"], 1e-9),
                                     urgency=urgency)
        # Above is a paper-trade simulated premium; real fills would be better
        underlying = last_underlying_price(ticker)
        ts = datetime.now().isoformat(timespec="seconds")
        cur = self.con.cursor()
        cur.execute(
            """INSERT INTO options_journal
               (username, ts, ticker, option_type, side, strike, expiry,
                contracts, entry_premium, entry_underlying, entry_iv, status, verdict)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (self.username, ts, ticker, option_type.upper(), side.upper(),
             float(strike), expiry, int(contracts), float(premium),
             float(underlying) if underlying else None,
             float(q.get("iv_market", 0)) if q.get("iv_market") else None,
             "OPEN", verdict),
        )
        self.con.commit()
        return cur.lastrowid

    def close_position(self, journal_id: int, urgency: str = "normal") -> dict:
        """Close an open position at current market mid (with simulated slippage)."""
        cur = self.con.cursor()
        row = cur.execute(
            """SELECT ticker, option_type, side, strike, expiry, contracts, entry_premium
               FROM options_journal WHERE id=? AND username=? AND status='OPEN'""",
            (journal_id, self.username),
        ).fetchone()
        if not row:
            raise RuntimeError(f"no open position with id={journal_id}")
        ticker, otype, side, strike, expiry, contracts, entry = row
        q = lookup_option_quote(ticker, strike, expiry, otype)
        if q is None:
            raise RuntimeError(f"no quote to close {ticker} {strike}{otype}")
        # Closing reverses side: a LONG sells (hits bid), a SHORT buys back (pays ask)
        close_side = "SHORT" if side == "LONG" else "LONG"
        from execution_gates import simulate_fill
        spread_bps = 10_000 * (q["ask"] - q["bid"]) / max(q["mid"], 1e-9) if q["ask"] > 0 else 0
        exit_premium = simulate_fill(close_side, q["mid"], spread_bps, urgency=urgency)
        # PNL per contract = (exit - entry) for LONG, (entry - exit) for SHORT, × 100 shares
        sign = 1 if side == "LONG" else -1
        pnl = sign * (exit_premium - entry) * contracts * 100
        ts = datetime.now().isoformat(timespec="seconds")
        cur.execute(
            """UPDATE options_journal SET status='CLOSED', exit_premium=?, exit_ts=?
               WHERE id=?""",
            (float(exit_premium), ts, journal_id),
        )
        self.con.commit()
        return {"journal_id": journal_id, "ticker": ticker, "entry_premium": entry,
                "exit_premium": exit_premium, "contracts": contracts, "pnl_usd": pnl}

    # ---------- mark / monitor ----------

    def mark_position(
        self,
        row: dict,
        *,
        risk_free_rate: float = DEFAULT_R,
    ) -> dict:
        """Mark one position to current market and compute Greeks. Returns extended row."""
        ticker = row["ticker"]
        S = last_underlying_price(ticker)
        if S is None:
            return {**row, "underlying_price": None, "mark_premium": None,
                    "greeks": None, "pnl_usd": None, "days_to_expiry": None}
        expiry_d = date.fromisoformat(row["expiry"])
        dte = (expiry_d - date.today()).days
        T_years = max(dte / 365.0, 1e-9)
        # Try market quote first; fall back to model price using entry IV (or default 50%)
        q = lookup_option_quote(ticker, row["strike"], row["expiry"], row["option_type"])
        if q and q.get("mid", 0) > 0:
            mark = q["mid"]
            iv_used = q.get("iv_market") or row.get("entry_iv") or 0.5
        else:
            iv_used = row.get("entry_iv") or 0.5
            mark = BlackScholes.price(S, row["strike"], T_years, iv_used,
                                       risk_free_rate, row["option_type"])
        greeks = BlackScholes.greeks(S, row["strike"], T_years, iv_used,
                                      risk_free_rate, row["option_type"])
        sign = 1 if row["side"] == "LONG" else -1
        pnl = sign * (mark - row["entry_premium"]) * row["contracts"] * 100
        return {**row, "underlying_price": S, "mark_premium": mark,
                "iv_used": iv_used, "greeks": greeks, "pnl_usd": pnl,
                "days_to_expiry": dte}

    def list_open_positions(self) -> list[dict]:
        cur = self.con.cursor()
        rows = cur.execute(
            """SELECT id, ts, ticker, option_type, side, strike, expiry, contracts,
                      entry_premium, entry_underlying, entry_iv, verdict
               FROM options_journal WHERE username=? AND status='OPEN'
               ORDER BY id""",
            (self.username,),
        ).fetchall()
        cols = ["id", "ts", "ticker", "option_type", "side", "strike", "expiry",
                "contracts", "entry_premium", "entry_underlying", "entry_iv", "verdict"]
        return [dict(zip(cols, r)) for r in rows]

    def monitor_book(self, *, journal: bool = True) -> dict:
        """Mark every open position, aggregate Greeks, return report dict."""
        open_pos = self.list_open_positions()
        marked = [self.mark_position(p) for p in open_pos]
        # aggregate
        agg = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0,
               "pnl_usd": 0.0, "n_positions": len(marked)}
        for m in marked:
            if m.get("greeks") is None:
                continue
            sign = 1 if m["side"] == "LONG" else -1
            mult = m["contracts"] * 100 * sign
            agg["delta"] += m["greeks"].delta * mult
            agg["gamma"] += m["greeks"].gamma * mult
            agg["theta"] += m["greeks"].theta * mult
            agg["vega"]  += m["greeks"].vega  * mult
            agg["pnl_usd"] += m.get("pnl_usd") or 0
        if journal:
            ts = datetime.now().isoformat(timespec="seconds")
            cur = self.con.cursor()
            for m in marked:
                if m.get("greeks") is None:
                    continue
                g = m["greeks"]
                cur.execute(
                    """INSERT OR REPLACE INTO options_position_sizing
                       (journal_id, ts, underlying_price, mark_premium, iv,
                        delta, gamma, theta, vega, days_to_expiry, pnl_usd)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (m["id"], ts, m.get("underlying_price"), m.get("mark_premium"),
                     m.get("iv_used"), g.delta, g.gamma, g.theta, g.vega,
                     m.get("days_to_expiry"), m.get("pnl_usd")),
                )
            self.con.commit()
        return {"positions": marked, "aggregate": agg}


# ============================================================================
# Presentation
# ============================================================================

def print_chain(ticker: str, expiry: str, *, option_type: str = "BOTH", n: int = 10) -> None:
    """Print a slice of the chain centered on the at-the-money strike."""
    S = last_underlying_price(ticker)
    if S is None:
        print(f"  (no underlying price for {ticker})")
        return
    print(f"\n  {ticker} @ ${S:.2f}    expiry {expiry}    risk-free {DEFAULT_R*100:.1f}%")
    expiry_d = date.fromisoformat(expiry)
    dte = (expiry_d - date.today()).days
    T_years = max(dte / 365.0, 1e-9)
    print(f"  days-to-expiry {dte}  (T={T_years:.3f} yrs)\n")

    for otype in (["CALL", "PUT"] if option_type.upper() == "BOTH"
                  else [option_type.upper()]):
        df = fetch_option_chain(ticker, expiry, otype)
        if df.empty:
            print(f"  {otype}: no chain available\n")
            continue
        # take N strikes nearest to spot
        df = df.iloc[(df["strike"] - S).abs().argsort()[:n]].sort_values("strike")
        print(f"  {otype}S near ATM (showing {len(df)} strikes):")
        print(f"  {'STRIKE':>8} {'BID':>7} {'ASK':>7} {'LAST':>7} {'MID':>7} "
              f"{'IV_MKT':>7} {'BS_PX':>7} {'IV_BS':>7} {'DELTA':>7}")
        for _, r in df.iterrows():
            K = float(r["strike"])
            bid = float(r.get("bid", 0) or 0)
            ask = float(r.get("ask", 0) or 0)
            last = float(r.get("lastPrice", 0) or 0)
            mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else last
            iv_mkt = float(r.get("impliedVolatility", 0) or 0)
            iv_bs = implied_vol(mid, S, K, T_years, DEFAULT_R, otype) if mid > 0 else float("nan")
            if not math.isnan(iv_bs) and iv_bs > 0:
                g = BlackScholes.greeks(S, K, T_years, iv_bs, DEFAULT_R, otype)
                bs_px = g.price; delta = g.delta
            else:
                bs_px = float("nan"); delta = float("nan")
            print(f"  ${K:>6.2f} ${bid:>5.2f} ${ask:>5.2f} ${last:>5.2f} ${mid:>5.2f} "
                  f"{iv_mkt*100:>5.1f}% ${bs_px:>5.2f} {iv_bs*100:>5.1f}% {delta:>+6.2f}")
        print()


def print_monitor_report(report: dict) -> None:
    a = report["aggregate"]
    n = a["n_positions"]
    print("=" * 86)
    print(f"  SYNTHETIC OPTIONS BOOK MONITOR   |   {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 86)
    print(f"  Open positions   : {n}")
    print(f"  Aggregate P&L    : ${a['pnl_usd']:+,.2f}")
    print(f"  Aggregate Greeks (per 1 share notional):")
    print(f"    Delta  = {a['delta']:+,.2f}    (book-equivalent share exposure)")
    print(f"    Gamma  = {a['gamma']:+,.4f}")
    print(f"    Theta  = ${a['theta']:+,.2f}/day  (calendar)")
    print(f"    Vega   = ${a['vega']:+,.2f}/vol-pt")
    print("-" * 86)
    if not report["positions"]:
        print("  (no open positions)")
        print("=" * 86)
        return
    print(f"  {'ID':>3} {'TKR':<6} {'TYPE':<5} {'SIDE':<5} {'STRIKE':>7} {'EXPIRY':<11} "
          f"{'DTE':>4} {'CT':>3} {'ENTRY':>7} {'MARK':>7} {'P&L':>9} {'DELTA':>7}")
    for p in report["positions"]:
        g = p.get("greeks")
        mark = p.get("mark_premium")
        pnl = p.get("pnl_usd")
        print(f"  {p['id']:>3} {p['ticker']:<6} {p['option_type']:<5} {p['side']:<5} "
              f"${p['strike']:>5.2f} {p['expiry']:<11} {p.get('days_to_expiry', 0):>4} "
              f"{p['contracts']:>3} ${p['entry_premium']:>5.2f} "
              f"${mark:>5.2f} ${pnl:>+8.2f} {(g.delta if g else 0):>+7.2f}")
    print("=" * 86)


# ============================================================================
# CLI
# ============================================================================

def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description="Synthetic options book.")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--username", default="sid")
    sub = p.add_subparsers(dest="cmd")

    # --price TICKER STRIKE EXPIRY TYPE
    p_price = sub.add_parser("price", help="Price + Greeks for one contract")
    p_price.add_argument("ticker")
    p_price.add_argument("strike", type=float)
    p_price.add_argument("expiry", help="YYYY-MM-DD")
    p_price.add_argument("option_type", choices=["CALL", "PUT", "call", "put"])
    p_price.add_argument("--vol", type=float, default=None,
                          help="override volatility (decimal)")

    # --chain TICKER --expiry YYYY-MM-DD
    p_chain = sub.add_parser("chain", help="Print options chain slice")
    p_chain.add_argument("ticker")
    p_chain.add_argument("--expiry", default=None)
    p_chain.add_argument("--type", default="BOTH", choices=["CALL", "PUT", "BOTH"])
    p_chain.add_argument("-n", type=int, default=10)

    # --expiries TICKER
    p_exp = sub.add_parser("expiries", help="List available expiries for a ticker")
    p_exp.add_argument("ticker")

    # --fire TICKER TYPE STRIKE EXPIRY CONTRACTS [--side LONG|SHORT]
    p_fire = sub.add_parser("fire", help="Open a synthetic position")
    p_fire.add_argument("ticker")
    p_fire.add_argument("option_type", choices=["CALL", "PUT", "call", "put"])
    p_fire.add_argument("strike", type=float)
    p_fire.add_argument("expiry")
    p_fire.add_argument("contracts", type=int)
    p_fire.add_argument("--side", default="LONG", choices=["LONG", "SHORT"])
    p_fire.add_argument("--verdict", default="BSI_THESIS")

    # --close JOURNAL_ID
    p_close = sub.add_parser("close", help="Close an open position")
    p_close.add_argument("journal_id", type=int)

    # --monitor
    sub.add_parser("monitor", help="Mark all open positions + aggregate Greeks")

    # --status
    sub.add_parser("status", help="Open positions summary (no re-mark)")

    args = p.parse_args(argv)

    if args.cmd == "price":
        S = last_underlying_price(args.ticker)
        if S is None:
            print(f"no price for {args.ticker}", file=sys.stderr)
            return 1
        expiry_d = date.fromisoformat(args.expiry)
        dte = (expiry_d - date.today()).days
        T = max(dte / 365.0, 1e-9)
        # solve for IV from market mid; if no market data, use vol override or 50%
        q = lookup_option_quote(args.ticker, args.strike, args.expiry, args.option_type)
        if args.vol is not None:
            iv = args.vol
            source = "user-override"
        elif q and q.get("mid", 0) > 0:
            iv = implied_vol(q["mid"], S, args.strike, T, DEFAULT_R, args.option_type)
            source = f"solved from market mid ${q['mid']:.2f}"
        else:
            iv = 0.50
            source = "default 50%"
        g = BlackScholes.greeks(S, args.strike, T, iv, DEFAULT_R, args.option_type)
        print(f"\n  {args.ticker} {args.option_type.upper()} ${args.strike} exp {args.expiry}")
        print(f"  Spot ${S:.2f}   DTE {dte}   r {DEFAULT_R*100:.1f}%")
        print(f"  IV {iv*100:.2f}%   ({source})")
        print(f"  Black-Scholes price: ${g.price:.4f}")
        if q:
            print(f"  Market quote:        bid ${q['bid']:.2f}  ask ${q['ask']:.2f}  "
                  f"mid ${q['mid']:.2f}  last ${q['last']:.2f}")
        print(f"\n  Greeks (per 1 share):")
        print(f"    Delta = {g.delta:+.4f}")
        print(f"    Gamma = {g.gamma:+.4f}")
        print(f"    Theta = ${g.theta:+.4f}/day")
        print(f"    Vega  = ${g.vega:+.4f}/vol-pt")
        print(f"    Rho   = ${g.rho:+.4f}/r-pt")
        return 0

    if args.cmd == "expiries":
        ex = available_expiries(args.ticker)
        if not ex:
            print(f"no expiries for {args.ticker}", file=sys.stderr)
            return 1
        print(f"\n  Available expiries for {args.ticker}:")
        for e in ex[:30]:
            d = date.fromisoformat(e)
            dte = (d - date.today()).days
            print(f"    {e}    DTE {dte}")
        return 0

    if args.cmd == "chain":
        expiry = args.expiry
        if not expiry:
            ex = available_expiries(args.ticker)
            if not ex:
                print(f"no expiries for {args.ticker}", file=sys.stderr); return 1
            expiry = ex[0]
        print_chain(args.ticker, expiry, option_type=args.type, n=args.n)
        return 0

    if args.cmd == "fire":
        book = OptionsBook(args.db, args.username)
        jid = book.fire_position(
            args.ticker, args.option_type, args.side, args.strike, args.expiry,
            args.contracts, verdict=args.verdict,
        )
        print(f"opened position #{jid} ({args.username})")
        # Show immediate mark
        rep = book.monitor_book(journal=False)
        print_monitor_report(rep)
        return 0

    if args.cmd == "close":
        book = OptionsBook(args.db, args.username)
        result = book.close_position(args.journal_id)
        print(f"closed position #{result['journal_id']}: "
              f"entry ${result['entry_premium']:.2f} -> exit ${result['exit_premium']:.2f}  "
              f"contracts {result['contracts']}  P&L ${result['pnl_usd']:+,.2f}")
        return 0

    if args.cmd == "monitor":
        book = OptionsBook(args.db, args.username)
        rep = book.monitor_book(journal=True)
        print_monitor_report(rep)
        return 0

    if args.cmd == "status":
        book = OptionsBook(args.db, args.username)
        open_ = book.list_open_positions()
        print(f"\n  {args.username}: {len(open_)} open option positions")
        for p in open_:
            print(f"    #{p['id']:>3} {p['ticker']:<6} {p['option_type']:<4} {p['side']:<5} "
                  f"${p['strike']:>6.2f} {p['expiry']}  {p['contracts']:>2}ct  "
                  f"@ ${p['entry_premium']:.2f}  ({p.get('verdict','')})")
        return 0

    # default: show help
    p.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
