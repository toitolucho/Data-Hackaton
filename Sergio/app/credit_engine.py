"""Motor determinista de elegibilidad de credito (politica sintetica, ver policy/credit_policy.yaml).

El LLM nunca aprueba ni calcula: llama a `evaluate` y solo redacta el resultado.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

POLICY_PATH = Path(__file__).resolve().parents[1] / "policy" / "credit_policy.yaml"

ELIGIBLE = "eligible"
ELIGIBLE_PROVISIONAL = "eligible_provisional"
NEEDS_DATA = "needs_data"
NEEDS_REVIEW = "needs_review"
DECLINED = "declined"


def load_policy(path: Path = POLICY_PATH) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def pmt(principal: float, annual_rate_pct: float, months: int) -> float:
    r = annual_rate_pct / 1200
    return principal / months if r == 0 else principal * r / (1 - (1 + r) ** -months)


def pv(payment: float, annual_rate_pct: float, months: int) -> float:
    r = annual_rate_pct / 1200
    return payment * months if r == 0 else payment * (1 - (1 + r) ** -months) / r


def score_band(score: float, policy: dict) -> int:
    band = 1
    for b, lower in sorted(policy["score_bands"].items()):
        if score >= lower:
            band = int(b)
    return band


def annual_rate(product: str, band: int, n_active_products: int, policy: dict) -> float:
    rate = policy["base_rate_pct"][product] + policy["band_spread_pp"].get(band, 0.0)
    rel = policy["relationship_discount"]
    if n_active_products >= rel["min_active_products"]:
        rate -= rel["discount_pp"]
    return rate


@dataclass
class Decision:
    outcome: str
    reasons: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    band: int | None = None
    rate_pct: float | None = None
    payment: float | None = None
    dti_after: float | None = None
    max_amount: float | None = None
    policy_version: str = ""


def evaluate(applicant: dict, request: dict, policy: dict | None = None) -> Decision:
    """applicant: customer_status, credit_score, monthly_income, income_is_declared,
    existing_monthly_debt (misma moneda que el ingreso), max_days_past_due, n_active_products.
    request: product, amount, months.
    """
    p = policy or load_policy()
    d = Decision(outcome=NEEDS_REVIEW, policy_version=p["version"])

    if applicant.get("customer_status") != "Active":
        d.reasons.append("CUSTOMER_NOT_ACTIVE")
        return d

    for key in ("credit_score", "monthly_income"):
        v = applicant.get(key)
        if v is None or v != v:  # None o NaN
            d.missing.append(key)
    if d.missing:
        d.outcome, d.reasons = NEEDS_DATA, ["MISSING_DATA"]
        return d

    dpd = applicant.get("max_days_past_due") or 0
    if dpd > p["delinquency"]["decline_over_days"]:
        d.outcome, d.reasons = DECLINED, ["DELINQUENT_OVER_LIMIT"]
        return d
    if dpd > p["delinquency"]["review_over_days"]:
        d.reasons.append("DELINQUENT_REVIEW")
        return d

    d.band = score_band(applicant["credit_score"], p)
    if d.band in p["no_lending_bands"]:
        d.outcome, d.reasons = DECLINED, ["LOW_SCORE_BAND"]
        return d

    income = applicant["monthly_income"]
    existing = applicant.get("existing_monthly_debt") or 0.0
    d.rate_pct = annual_rate(request["product"], d.band, applicant.get("n_active_products", 0), p)
    d.payment = pmt(request["amount"], d.rate_pct, request["months"])
    d.dti_after = (existing + d.payment) / income

    capacity = max(p["max_dti"] * income - existing, 0.0)
    d.max_amount = pv(capacity, d.rate_pct, request["months"])

    limit = p["max_dti"]
    if d.dti_after <= limit:
        declared = applicant.get("income_is_declared", False)
        d.outcome = ELIGIBLE_PROVISIONAL if declared else ELIGIBLE
        d.reasons = ["INCOME_UNVERIFIED"] if declared else ["WITHIN_LIMIT"]
    elif d.dti_after <= limit * (1 + p["borderline_margin"]):
        d.outcome, d.reasons = NEEDS_REVIEW, ["BORDERLINE_DTI"]
    else:
        d.outcome, d.reasons = DECLINED, ["DTI_EXCEEDED"]
    return d
