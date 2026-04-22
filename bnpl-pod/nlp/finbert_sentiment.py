"""
FinBERT sentiment scoring for Reddit posts and CFPB complaint narratives.

Implements MASTERPLAN v4.1 §3: ProsusAI/finbert three-way classification
(negative / neutral / positive) with a Bot-Filter Credibility Score (§3.1)
applied to Reddit authors before the signal is aggregated into BSI.

Behavioural-framing note (paper §4/§5)
--------------------------------------
FinBERT is a generic financial-sentiment model. On BNPL narratives it picks
up BOTH financial-credit distress ("I can't pay", "wages garnished",
"collections") AND technical friction ("app crashes", "autopay failed",
"Apple Pay declined"). For the BSI we only want the former — the BNPL-
specific top-of-funnel panic signal the paper positions as a
"psychological sensor". We therefore tag every scored row with a coarse
``distress_kind`` label (``financial`` | ``technical`` | ``mixed`` |
``other``) derived from keyword presence; downstream BSI aggregation can
elect to drop or down-weight the technical-only subset. See
``classify_distress_kind`` below. The raw FinBERT probabilities are left
untouched so the data is not destroyed — a v2 ablation can always re-run
the unfiltered composite.

Design
------
- Model loads lazily (first scoring call). Offline tests inject a dummy scorer
  via monkeypatch on ``_score_text`` — zero HuggingFace network calls.
- Batching in fixed chunks; the HF pipeline handles tokenization.
- Idempotent writes: ``UPDATE ... SET finbert_*`` on PRIMARY KEY — re-running
  over the same post_id / complaint_id rewrites the same row.
- Credibility score C(u) ∈ [0, 1] combines account age and karma, capped at 1.
  Posts with no author metadata default to credibility = 0.5 (neutral prior).

The credibility score is stored on the row but does NOT alter the raw FinBERT
probabilities. Downstream aggregation (signals/bsi.py) multiplies the
negative-class probability by credibility when producing the daily Reddit
component, so the raw scores stay source-of-truth.

Run with:  python -m nlp.finbert_sentiment
"""
from __future__ import annotations

import logging
import math
from typing import Callable, Iterable

import duckdb

from data.settings import settings

log = logging.getLogger(__name__)

MODEL_NAME = "ProsusAI/finbert"
BATCH_SIZE = 32
MAX_CHARS = 1200  # FinBERT is 512-token; ~1200 chars is a safe truncation.

# --- Bot-filter credibility (v4.1 §3.1) -----------------------------------
# Credibility = min(1, 0.5*age_score + 0.5*karma_score)
# age_score = 1 - exp(-age_days / 180)     # half-life ~125 days
# karma_score = 1 - exp(-max(karma,0) / 500)
CRED_AGE_TAU = 180.0
CRED_KARMA_TAU = 500.0
CRED_DEFAULT = 0.5    # used when author metadata is missing
CRED_FLOOR = 0.05     # even unknown authors contribute a little


def credibility(age_days: int | None, karma: int | None) -> float:
    """Bot-filter credibility C(u) ∈ [0,1]. Higher = more likely a real user."""
    if age_days is None and karma is None:
        return CRED_DEFAULT
    age_score = 0.0 if age_days is None else 1.0 - math.exp(-max(age_days, 0) / CRED_AGE_TAU)
    karma_score = 0.0 if karma is None else 1.0 - math.exp(-max(karma, 0) / CRED_KARMA_TAU)
    c = 0.5 * age_score + 0.5 * karma_score
    return max(CRED_FLOOR, min(1.0, c))


# --- Financial-vs-technical distress classifier ---------------------------
# Coarse keyword filter. The point is NOT to replace FinBERT — it is to
# separate the rows where the consumer is expressing financial-credit panic
# ("I can't afford", "debt collector", "wages garnished") from rows where
# they are frustrated about plumbing ("app crashes", "cannot log in",
# "autopay failed"). The former is what the Subprime-2.0 thesis positions
# as the BSI signal; the latter is noise we want to be able to drop
# downstream. We return a label, not a probability, to keep the
# classifier trivially auditable.
_FIN_KW = (
    "can't pay", "cant pay", "cannot pay", "unable to pay",
    "garnish", "collections", "debt collector", "collection agency",
    "credit report", "credit score", "hit my credit", "reported to",
    "late fee", "overdraft", "overdrew", "overdrawn", "nsf",
    "bankruptcy", "foreclosure", "default",
    "can't afford", "cant afford", "lost my job", "laid off",
    "scam", "fraud", "charged twice", "charged me twice", "unauthorized",
    "predatory", "usury",
)

_TECH_KW = (
    "crash", "freeze", "won't load", "wont load", "glitch",
    "can't log in", "cant log in", "can't login", "cant login",
    "password reset", "verification code", "two-factor", "2fa",
    "error code", "error message", "spinning", "blank screen",
    "autopay fail", "autopay didn't", "autopay didnt",
    "apple pay", "google pay", "payment method",
    "declined at checkout", "checkout failed", "keeps declining",
    "support won't", "cant reach support", "can't reach support",
    "app update", "ios update", "android update",
)


def classify_distress_kind(text: str | None) -> str:
    """Return ``financial`` | ``technical`` | ``mixed`` | ``other``.

    Very coarse — a 40-keyword dictionary, not a model. The goal is to let
    the BSI layer drop technical-only rows from the top-of-funnel signal
    without losing the underlying FinBERT probabilities. False positives on
    both sides are expected; the right response when precision matters is a
    v2 with a small labelled set and a fine-tuned classifier head, not
    more keywords.
    """
    if not text:
        return "other"
    t = text.lower()
    has_fin = any(kw in t for kw in _FIN_KW)
    has_tech = any(kw in t for kw in _TECH_KW)
    if has_fin and has_tech:
        return "mixed"
    if has_fin:
        return "financial"
    if has_tech:
        return "technical"
    return "other"


# --- Model loader (lazy, mockable) ----------------------------------------
_PIPELINE = None


def _load_pipeline():
    """Return a HF pipeline. Separated so tests monkeypatch ``_score_text``."""
    global _PIPELINE
    if _PIPELINE is not None:
        return _PIPELINE
    from transformers import pipeline   # local import, heavy
    _PIPELINE = pipeline(
        task="text-classification",
        model=MODEL_NAME,
        top_k=None,
        truncation=True,
        max_length=512,
    )
    return _PIPELINE


def _score_text(texts: list[str]) -> list[dict[str, float]]:
    """Return [{'negative': p, 'neutral': p, 'positive': p}, ...] per input."""
    if not texts:
        return []
    pipe = _load_pipeline()
    out: list[dict[str, float]] = []
    for raw in pipe(texts, batch_size=BATCH_SIZE):
        # HF returns either a list of label/score dicts (top_k=None) or a single dict.
        labels = raw if isinstance(raw, list) else [raw]
        d = {lbl["label"].lower(): float(lbl["score"]) for lbl in labels}
        out.append({
            "negative": d.get("negative", 0.0),
            "neutral":  d.get("neutral",  0.0),
            "positive": d.get("positive", 0.0),
        })
    return out


# --- Reddit ---------------------------------------------------------------
def score_reddit(batch_size: int = 500, limit: int | None = None) -> int:
    """Score unscored Reddit rows. Returns number of rows updated."""
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        q = """
            SELECT post_id, title, body, author_age_days, author_karma
            FROM reddit_posts
            WHERE finbert_neg IS NULL
        """
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = con.execute(q).fetchall()
        if not rows:
            return 0
        n = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            texts = [_compose(title, body) for _, title, body, _, _ in chunk]
            scores = _score_text(texts)
            payload = []
            for (pid, _t, _b, age, karma), sc in zip(chunk, scores):
                payload.append((
                    sc["negative"], sc["neutral"], sc["positive"],
                    credibility(age, karma),
                    pid,
                ))
            con.executemany(
                """
                UPDATE reddit_posts
                   SET finbert_neg = ?, finbert_neu = ?, finbert_pos = ?,
                       credibility = ?
                 WHERE post_id = ?
                """,
                payload,
            )
            n += len(payload)
        log.info("finbert | reddit | scored %d posts", n)
        return n
    finally:
        con.close()


# --- CFPB -----------------------------------------------------------------
def score_cfpb(batch_size: int = 500, limit: int | None = None,
               companies: list[str] | None = None) -> int:
    """Score unscored CFPB complaint narratives. Returns rows updated.

    ``companies`` filters to a subset of the ``company`` field. Pass the
    BSI-treated list to score only the 70k BNPL narratives we actually
    feed into the composite; pass None to score every narrative in the
    warehouse (~195k rows, 3-5 hours on CPU).
    """
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        base_q = """
            SELECT complaint_id, narrative
            FROM cfpb_complaints
            WHERE finbert_neg IS NULL
              AND narrative IS NOT NULL AND length(narrative) > 10
        """
        params: list = []
        if companies:
            placeholders = ",".join(["?"] * len(companies))
            base_q += f" AND company IN ({placeholders})"
            params.extend(companies)
        q = base_q + (f" LIMIT {int(limit)}" if limit else "")
        rows = con.execute(q, params).fetchall()
        if not rows:
            return 0
        n = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            texts = [(narr or "")[:MAX_CHARS] for _, narr in chunk]
            scores = _score_text(texts)
            payload = [
                (sc["negative"], sc["neutral"], sc["positive"], cid)
                for (cid, _), sc in zip(chunk, scores)
            ]
            con.executemany(
                """
                UPDATE cfpb_complaints
                   SET finbert_neg = ?, finbert_neu = ?, finbert_pos = ?
                 WHERE complaint_id = ?
                """,
                payload,
            )
            n += len(payload)
        log.info("finbert | cfpb | scored %d complaints", n)
        return n
    finally:
        con.close()


def _compose(title: str | None, body: str | None) -> str:
    t = (title or "").strip()
    b = (body or "").strip()
    text = f"{t}\n\n{b}".strip()
    return text[:MAX_CHARS] if text else "(empty)"


# --- App Store reviews ----------------------------------------------------
def score_app_store(batch_size: int = 500, limit: int | None = None) -> int:
    """Score unscored App Store reviews. Returns rows updated.

    Reviews are already short by construction (App Store caps at ~6k chars
    but medians ~100), so truncation is rare. We score title+body together
    because the title frequently carries the affective punch ("SCAM!",
    "Stole my money") while the body supplies context — FinBERT benefits
    from both.
    """
    con = duckdb.connect(str(settings.duckdb_path))
    try:
        q = """
            SELECT review_id, title, body
            FROM app_store_reviews
            WHERE finbert_neg IS NULL
              AND (
                    (body  IS NOT NULL AND length(body)  > 3)
                 OR (title IS NOT NULL AND length(title) > 3)
                  )
        """
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = con.execute(q).fetchall()
        if not rows:
            return 0
        n = 0
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            texts = [_compose(t, b) for _, t, b in chunk]
            scores = _score_text(texts)
            payload = [
                (sc["negative"], sc["neutral"], sc["positive"], rid)
                for (rid, _, _), sc in zip(chunk, scores)
            ]
            con.executemany(
                """
                UPDATE app_store_reviews
                   SET finbert_neg = ?, finbert_neu = ?, finbert_pos = ?
                 WHERE review_id = ?
                """,
                payload,
            )
            n += len(payload)
        log.info("finbert | app_store | scored %d reviews", n)
        return n
    finally:
        con.close()


def score_all() -> dict[str, int]:
    return {
        "reddit":    score_reddit(),
        "cfpb":      score_cfpb(),
        "app_store": score_app_store(),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    summary = score_all()
    print("\nFinBERT scoring summary:")
    for k, v in summary.items():
        print(f"  {k:8s} {v:>6d} rows")
