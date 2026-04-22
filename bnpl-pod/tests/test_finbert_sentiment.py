"""Offline tests for nlp.finbert_sentiment — no HF network calls."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from data.schema import DDL
from nlp import finbert_sentiment as fb


@pytest.fixture()
def tmp_duckdb(tmp_path: Path, monkeypatch):
    db = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    for stmt in DDL:
        con.execute(stmt)
    con.close()
    monkeypatch.setattr(fb.settings, "duckdb_path", db)
    return db


def _fake_scorer(texts):
    # Deterministic: "late" / "collections" / "default" → negative-heavy.
    out = []
    for t in texts:
        low = (t or "").lower()
        if any(k in low for k in ("late", "collections", "default", "declined")):
            out.append({"negative": 0.80, "neutral": 0.15, "positive": 0.05})
        elif any(k in low for k in ("love", "great", "easy")):
            out.append({"negative": 0.05, "neutral": 0.15, "positive": 0.80})
        else:
            out.append({"negative": 0.20, "neutral": 0.70, "positive": 0.10})
    return out


# --- credibility ----------------------------------------------------------
def test_credibility_missing_defaults_to_neutral():
    assert fb.credibility(None, None) == fb.CRED_DEFAULT


def test_credibility_monotone_in_age_and_karma():
    c_young = fb.credibility(10, 10)
    c_old   = fb.credibility(1000, 10_000)
    assert 0.0 < c_young < c_old <= 1.0


def test_credibility_floor_and_cap():
    c = fb.credibility(0, 0)
    assert c == pytest.approx(fb.CRED_FLOOR)
    c_max = fb.credibility(10_000, 1_000_000)
    assert 0.99 <= c_max <= 1.0


# --- Reddit ---------------------------------------------------------------
def _insert_reddit(db: Path, rows: list[tuple]):
    con = duckdb.connect(str(db))
    con.executemany(
        """INSERT INTO reddit_posts
           (post_id, subreddit, created_at, title, body, score, num_comments, url,
            author, author_age_days, author_karma)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    con.close()


def test_score_reddit_writes_and_is_idempotent(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(fb, "_score_text", _fake_scorer)
    ts = datetime(2024, 6, 1, tzinfo=timezone.utc)
    _insert_reddit(tmp_duckdb, [
        ("p1", "povertyfinance", ts, "Affirm late fee trap", "charged me again", 10, 2,
         "u1", "alice", 400, 2000),
        ("p2", "debt", ts, "I love Klarna easy checkout", "great service", 5, 0,
         "u2", "bob", 30, 5),   # low credibility — young low-karma
        ("p3", "personalfinance", ts, "Affirm collections notice", "help", 3, 1,
         "u3", None, None, None),  # missing metadata
    ])

    n1 = fb.score_reddit()
    assert n1 == 3
    # Second run is a no-op: finbert_neg is now non-null.
    n2 = fb.score_reddit()
    assert n2 == 0

    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT post_id, finbert_neg, finbert_pos, credibility "
        "FROM reddit_posts ORDER BY post_id"
    ).fetchall()
    con.close()
    d = {pid: (neg, pos, cr) for pid, neg, pos, cr in rows}
    assert d["p1"][0] > 0.5     # negative
    assert d["p2"][1] > 0.5     # positive
    assert d["p3"][0] > 0.5     # negative narrative
    # Credibility: alice (aged/karma) > bob (young) > neutral prior.
    assert d["p1"][2] > d["p2"][2]
    assert d["p3"][2] == pytest.approx(fb.CRED_DEFAULT)


# --- CFPB -----------------------------------------------------------------
def test_score_cfpb_skips_empty_narrative(monkeypatch, tmp_duckdb):
    monkeypatch.setattr(fb, "_score_text", _fake_scorer)
    con = duckdb.connect(str(tmp_duckdb))
    con.executemany(
        """INSERT INTO cfpb_complaints
           (complaint_id, received_at, product, company, narrative)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ("c1", "2024-06-01", "BNPL", "AFFIRM, INC.",
             "Affirm sent my account to collections for a default I already paid."),
            ("c2", "2024-06-02", "BNPL", "AFFIRM, INC.", ""),          # skipped
            ("c3", "2024-06-03", "BNPL", "AFFIRM, INC.", None),        # skipped
            ("c4", "2024-06-04", "BNPL", "AFFIRM, INC.",
             "I was declined and charged a late fee on the same day."),
        ],
    )
    con.close()

    n = fb.score_cfpb()
    assert n == 2
    con = duckdb.connect(str(tmp_duckdb))
    rows = con.execute(
        "SELECT complaint_id, finbert_neg FROM cfpb_complaints ORDER BY complaint_id"
    ).fetchall()
    con.close()
    d = dict(rows)
    assert d["c1"] is not None and d["c1"] > 0.5
    assert d["c2"] is None
    assert d["c3"] is None
    assert d["c4"] is not None and d["c4"] > 0.5
