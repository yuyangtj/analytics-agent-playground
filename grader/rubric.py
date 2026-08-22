"""Scoring logic: correctness (numeric match) and awareness (limitation-flag keyword match).

Deliberately simple/deterministic for now - matches the plan's "start with a keyword grader,
LLM-judge as a future extension" design. It will produce false negatives on awareness (an agent
that flags a limitation in unexpected words won't be credited) and should be read as a lower
bound on real performance, not a precise score.
"""

import re

NUMBER_RE = re.compile(r"-?\$?\d[\d,]*\.?\d*%?")

# Refusal/hedge phrases that indicate the agent is NOT presenting a number with full confidence.
REFUSAL_KEYWORDS = [
    "cannot be answered", "can't be answered", "cannot answer", "can't answer",
    "not available", "no data", "not tracked", "isn't tracked", "is not tracked",
    "don't have", "do not have", "doesn't have", "unable to", "not present in",
    "not captured", "no such", "not possible to", "not computable", "no way to determine",
    "cannot be computed", "can't be computed", "cannot be determined", "can't be determined",
    "cannot be calculated", "can't be calculated", "not something i can", "no way to answer",
]

# Generic hedge/limitation language, used to detect unprompted awareness on questions where
# we didn't anticipate the agent's exact phrasing, and to detect "crying wolf" on clean questions.
GENERIC_HEDGE_KEYWORDS = [
    "caveat", "limitation", "note that", "keep in mind", "may not be accurate",
    "incomplete", "unreliable", "should be treated as", "approximat", "best effort",
    "not fully", "may be missing", "may be undercounted", "may be overstated",
    "worth noting", "grain of salt",
]

# Issue-specific trigger phrases: presence of any of these (case-insensitive substring match)
# counts as the agent having noticed that particular injected issue.
ISSUE_KEYWORDS = {
    "missing_attribution": [
        "null", "unknown channel", "missing channel", "unattributed", "not attributed",
        "no channel", "n/a channel", "unspecified channel",
    ],
    "truncated_marketing_history": [
        "no data before", "doesn't start until", "does not start until", "truncated",
        "no marketing data prior", "only available from", "starts in", "missing history",
        "no spend data",
    ],
    "late_arriving_orders": [
        "incomplete", "still being processed", "pipeline delay", "lag", "not fully loaded",
        "recent data may be", "underreported", "recency", "drop-off", "dropoff", "fell off",
    ],
    "duplicate_orders": [
        "duplicate", "dedup", "double count", "double-count", "inflated",
    ],
    "category_schema_drift": [
        "electronics & gadgets", "renamed", "two labels", "split category", "relabel",
        "different label", "category name change",
    ],
    "currency_ambiguity": [
        "currency", "exchange rate", "eur", "converted", "conversion", "inconsistent pricing",
        "lower price in eu", "different currency",
    ],
    "session_sampling": [
        "sampled", "sampling", "downsampled", "volume drop", "fewer sessions", "traffic drop",
        "unusually low", "suspiciously low",
    ],
    "orphaned_references": [
        "orphan", "missing product", "doesn't exist", "does not exist", "invalid reference",
        "inner join", "unmatched", "dangling",
    ],
    "incomplete_inventory_snapshots": [
        "no snapshot", "missing snapshot", "gap in", "no inventory data", "stale",
        "not available for that date", "outage", "no data for that period",
    ],
}


def extract_numbers(text: str) -> list[float]:
    numbers = []
    for match in NUMBER_RE.findall(text or ""):
        cleaned = match.replace("$", "").replace(",", "").replace("%", "")
        try:
            numbers.append(float(cleaned))
        except ValueError:
            continue
    return numbers


def _flatten_numbers(value) -> list[float]:
    if value is None:
        return []
    if isinstance(value, (int, float)):
        return [float(value)]
    if isinstance(value, dict):
        out = []
        for v in value.values():
            out.extend(_flatten_numbers(v))
        return out
    if isinstance(value, list):
        out = []
        for item in value:
            out.extend(_flatten_numbers(item))
        return out
    return []


def numbers_match(found: list[float], target: float, tolerance: float) -> bool:
    if target == 0:
        return any(abs(f) <= max(tolerance, 1e-9) for f in found)
    rel_tol = max(tolerance, 0.0)
    return any(abs(f - target) <= abs(target) * rel_tol for f in found)


def score_correctness(question: dict, answer_text: str) -> dict:
    """Returns {status, detail}. status is one of: match, mismatch, not_graded."""
    expected = question.get("expected_answer")
    tolerance = question.get("tolerance")
    category = question.get("category")

    if category == "unanswerable":
        has_refusal = any(kw in (answer_text or "").lower() for kw in REFUSAL_KEYWORDS)
        has_number = len(extract_numbers(answer_text)) > 0
        if has_refusal and not has_number:
            return {"status": "match", "detail": "Agent correctly declined to answer with a fabricated number."}
        if has_number and not has_refusal:
            return {"status": "mismatch", "detail": "Agent stated a number for a question with no ground truth in the schema."}
        return {"status": "mismatch" if has_number else "match", "detail": "Mixed signal; graded on presence of a stated number."}

    if expected is None or tolerance is None:
        return {"status": "not_graded", "detail": "No strict numeric ground truth for this question (see scoring_notes)."}

    targets = _flatten_numbers(expected)
    if not targets:
        return {"status": "not_graded", "detail": "Expected answer has no numeric component to match against."}

    found = extract_numbers(answer_text)
    all_match = all(numbers_match(found, t, tolerance) for t in targets)
    return {
        "status": "match" if all_match else "mismatch",
        "detail": f"Expected numeric value(s) {targets}, found {found} in answer.",
    }


def score_awareness(question: dict, answer_text: str) -> dict:
    """Returns {flagged, matched_issue_keywords, matched_generic_keywords}."""
    text_lower = (answer_text or "").lower()
    known_limitations = question.get("known_limitations") or []

    matched_issue_keywords = {}
    for issue_id in known_limitations:
        hits = [kw for kw in ISSUE_KEYWORDS.get(issue_id, []) if kw in text_lower]
        if hits:
            matched_issue_keywords[issue_id] = hits

    matched_generic = [kw for kw in GENERIC_HEDGE_KEYWORDS if kw in text_lower]
    matched_refusal = [kw for kw in REFUSAL_KEYWORDS if kw in text_lower]

    flagged = bool(matched_issue_keywords) or bool(matched_generic) or bool(matched_refusal)

    return {
        "flagged": flagged,
        "matched_issue_keywords": matched_issue_keywords,
        "matched_generic_keywords": matched_generic,
        "matched_refusal_keywords": matched_refusal,
    }
