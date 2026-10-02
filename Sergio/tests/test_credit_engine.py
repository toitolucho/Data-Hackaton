import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import credit_engine as ce  # noqa: E402

P = ce.load_policy()
REQ = {"product": "personal_loan", "amount": 9000.0, "months": 36}


def applicant(**kw):
    base = dict(customer_status="Active", credit_score=700, monthly_income=3000.0,
                income_is_declared=False, existing_monthly_debt=0.0,
                max_days_past_due=0, n_active_products=1)
    base.update(kw)
    return base


def test_pmt_pv_roundtrip():
    pay = ce.pmt(10_000, 18, 24)
    assert math.isclose(ce.pv(pay, 18, 24), 10_000, rel_tol=1e-9)


def test_eligible_within_limit():
    d = ce.evaluate(applicant(), REQ, P)
    assert d.outcome == ce.ELIGIBLE and d.dti_after <= P["max_dti"]


def test_low_score_declined():
    assert ce.evaluate(applicant(credit_score=450), REQ, P).outcome == ce.DECLINED


def test_missing_income_needs_data_not_guess():
    d = ce.evaluate(applicant(monthly_income=float("nan")), REQ, P)
    assert d.outcome == ce.NEEDS_DATA and "monthly_income" in d.missing


def test_dti_exceeded_declined_and_reports_max_amount():
    d = ce.evaluate(applicant(monthly_income=1000.0), REQ, P)
    assert d.outcome == ce.DECLINED and d.max_amount < REQ["amount"]


def test_declared_income_is_provisional():
    d = ce.evaluate(applicant(income_is_declared=True), REQ, P)
    assert d.outcome == ce.ELIGIBLE_PROVISIONAL


def test_delinquency_rules():
    assert ce.evaluate(applicant(max_days_past_due=45), REQ, P).outcome == ce.NEEDS_REVIEW
    assert ce.evaluate(applicant(max_days_past_due=120), REQ, P).outcome == ce.DECLINED


def test_inactive_customer_goes_to_human():
    assert ce.evaluate(applicant(customer_status="Suspended"), REQ, P).outcome == ce.NEEDS_REVIEW


def test_borderline_goes_to_review():
    base = ce.evaluate(applicant(), REQ, P)
    income = base.payment / (P["max_dti"] * 1.05)      # dti = 1.05 * limite
    d = ce.evaluate(applicant(monthly_income=income), REQ, P)
    assert d.outcome == ce.NEEDS_REVIEW and d.reasons == ["BORDERLINE_DTI"]


def test_relationship_discount_lowers_rate():
    a = ce.evaluate(applicant(n_active_products=1), REQ, P).rate_pct
    b = ce.evaluate(applicant(n_active_products=3), REQ, P).rate_pct
    assert math.isclose(a - b, P["relationship_discount"]["discount_pp"])
