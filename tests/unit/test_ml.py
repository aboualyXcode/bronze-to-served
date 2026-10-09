"""The model, the promotion policy and drift, trained and checked on the reference implementation's features."""
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest

from bronze_to_served.ml.drift import feature_drift, psi, status
from bronze_to_served.ml.model import FEATURES, TrainParams, numeric_frame, predict_proba, prepare, risk_band, \
    train_and_evaluate
from bronze_to_served.ml.policy import Decision, PromotionPolicy, decide
from reference.worlds import reference


def _training_frames():
    ref = reference("ml")
    data = pd.DataFrame(ref.features).merge(pd.DataFrame(ref.labels), on=["customer_id", "as_of_date"])
    data["revenue_90d"] = data.pop("revenue_90d_cents").map(lambda c: Decimal(c) / 100)
    data["revenue_365d"] = data.pop("revenue_365d_cents").map(lambda c: Decimal(c) / 100)
    data["marketing_opt_in"] = data["marketing_opt_in"].map(lambda v: None if v is None else bool(v))
    dates = sorted(data["as_of_date"].unique())
    test = data["as_of_date"].isin(dates[-max(1, round(len(dates) * 0.2)):])
    return data[~test], data[test]


def test_the_model_learns_churn_out_of_time():
    train, test = _training_frames()
    result = train_and_evaluate(train, test)
    assert result.metrics["roc_auc"] > 0.72, result.metrics
    assert result.metrics["top_decile_lift"] > 1.0 and 0 < result.metrics["brier"] < 0.25
    again = train_and_evaluate(train, test)
    assert np.allclose(predict_proba(result.model, test), predict_proba(again.model, test))   # deterministic


def test_prepare_turns_spark_types_into_model_input():
    pdf = pd.DataFrame([{f: None for f in FEATURES}, {f: None for f in FEATURES}])
    pdf.loc[0, ["revenue_90d", "marketing_opt_in", "loyalty_tier"]] = [Decimal("12.50"), True, "gold"]
    x = prepare(pdf)
    assert list(x.columns) == FEATURES and x.loc[0, "revenue_90d"] == 12.5 and x.loc[0, "marketing_opt_in"] == 1.0
    assert np.isnan(x.loc[1, "revenue_90d"]) and x.loc[1, "loyalty_tier"] == "unknown"
    assert numeric_frame(pdf, ["revenue_90d"]).dtypes.iloc[0] == np.float64


def test_hyperparameters_from_json_are_typed():
    assert TrainParams.from_dict({"max_iter": "50", "learning_rate": "0.1"}) == TrainParams(max_iter=50, learning_rate=0.1)


def test_risk_bands():
    assert list(risk_band([0.1, 0.3, 0.59, 0.6, 0.95])) == ["low", "medium", "medium", "high", "high"]


GOOD = {"roc_auc": 0.80, "pr_auc": 0.85, "brier": 0.18, "log_loss": 0.5}


def test_first_model_is_promoted_when_it_clears_the_floors():
    assert decide(GOOD, None).promote
    assert not decide({**GOOD, "roc_auc": 0.65}, None).promote


def test_challenger_must_beat_the_champion_without_breaking_guardrails():
    assert decide({**GOOD, "pr_auc": 0.86}, GOOD).promote
    assert not decide({**GOOD, "pr_auc": 0.84}, GOOD).promote                      # worse primary metric
    assert not decide({**GOOD, "pr_auc": 0.87, "roc_auc": 0.78}, GOOD).promote     # roc_auc guardrail
    assert not decide({**GOOD, "pr_auc": 0.87, "brier": 0.19}, GOOD).promote       # calibration guardrail
    strict = PromotionPolicy(min_improvement=0.02)
    assert not decide({**GOOD, "pr_auc": 0.86}, GOOD, strict).promote
    decision = decide({**GOOD, "pr_auc": 0.86}, GOOD)
    assert isinstance(decision, Decision) and any("pr_auc" in r for r in decision.reasons)


def test_policy_from_json():
    p = PromotionPolicy.from_json('{"primary_metric": "roc_auc", "min_improvement": 0.01, "floors": {"pr_auc": 0.5}}')
    assert p.primary_metric == "roc_auc" and p.floors == {"pr_auc": 0.5} and p.guardrails


def test_psi_is_zero_for_the_same_distribution_and_large_for_a_shift():
    rng = np.random.default_rng(1)
    base = rng.normal(0, 1, 20_000)
    assert psi(base, rng.normal(0, 1, 20_000)) < 0.01
    assert status(psi(base, rng.normal(1.0, 1, 20_000))) == "significant"
    assert 0.0 < psi(base, rng.normal(0.25, 1, 20_000)) < 0.25


def test_psi_detects_a_feature_going_missing():
    base = np.arange(1000, dtype=float)
    shifted = base.copy()
    shifted[:400] = np.nan
    assert status(psi(base, shifted)) == "significant"
    rows = feature_drift(pd.DataFrame({"a": base}), pd.DataFrame({"a": shifted}), ["a"])
    assert rows[0]["feature"] == "a" and rows[0]["status"] == "significant"
