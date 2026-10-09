"""Churn model training and evaluation with scikit-learn. Pure Python: no Spark, no MLflow."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

NUMERIC = ["recency_days", "orders_30d", "orders_90d", "orders_365d", "revenue_90d", "revenue_365d", "refunds_365d",
           "avg_ticket_price_365d", "genres_365d", "events_7d", "events_30d", "cart_adds_30d", "engagement_ratio",
           "tenure_days", "marketing_opt_in"]
CATEGORICAL = ["loyalty_tier", "country"]
FEATURES = NUMERIC + CATEGORICAL
LABEL = "churned"


@dataclass(frozen=True)
class TrainParams:
    learning_rate: float = 0.06
    max_iter: int = 300
    max_leaf_nodes: int = 31
    min_samples_leaf: int = 40
    l2_regularization: float = 0.1
    random_state: int = 7

    @classmethod
    def from_dict(cls, values: dict) -> "TrainParams":
        defaults = cls()
        return cls(**{k: type(getattr(defaults, k))(v) for k, v in values.items()})


@dataclass
class TrainingResult:
    model: Any
    metrics: dict
    train_rows: int
    test_rows: int


def _to_float(v) -> float:
    return np.nan if pd.isna(v) else float(v)      # Decimal, bool, int and None all become floats


def numeric_frame(pdf: pd.DataFrame, columns: list[str] = NUMERIC) -> pd.DataFrame:
    return pd.DataFrame({c: pdf[c].astype("object").map(_to_float).astype("float64") for c in columns}, index=pdf.index)


def prepare(pdf: pd.DataFrame) -> pd.DataFrame:
    """Model input: numeric features as float64 (NaN allowed), categoricals as strings."""
    x = numeric_frame(pdf)
    for c in CATEGORICAL:
        x[c] = pdf[c].fillna("unknown").astype(str)
    return x[FEATURES]


def build_pipeline(params: TrainParams):
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    encode = ColumnTransformer([("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL)],
                               remainder="passthrough", verbose_feature_names_out=False)
    model = HistGradientBoostingClassifier(learning_rate=params.learning_rate, max_iter=params.max_iter,
                                           max_leaf_nodes=params.max_leaf_nodes, min_samples_leaf=params.min_samples_leaf,
                                           l2_regularization=params.l2_regularization, early_stopping=True,
                                           validation_fraction=0.15, n_iter_no_change=20,
                                           random_state=params.random_state)
    return Pipeline([("encode", encode), ("model", model)])


def predict_proba(model, pdf: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(prepare(pdf))[:, 1]


def evaluation_metrics(y_true, p) -> dict:
    """Ranking (ROC AUC, PR AUC, lift) and calibration (log loss, Brier) on the same scores."""
    from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

    y = np.asarray(y_true).astype(int)
    p = np.clip(np.asarray(p, dtype=float), 1e-6, 1 - 1e-6)
    top = np.argsort(-p, kind="stable")[: max(1, int(np.ceil(len(p) * 0.1)))]
    base = float(y.mean())
    return {"roc_auc": float(roc_auc_score(y, p)), "pr_auc": float(average_precision_score(y, p)),
            "log_loss": float(log_loss(y, p, labels=[0, 1])), "brier": float(brier_score_loss(y, p)),
            "top_decile_lift": float(y[top].mean() / base) if base > 0 else 0.0, "base_rate": base, "rows": int(len(y))}


def train_and_evaluate(train_pdf: pd.DataFrame, test_pdf: pd.DataFrame, params: TrainParams = TrainParams()) -> TrainingResult:
    model = build_pipeline(params).fit(prepare(train_pdf), train_pdf[LABEL].astype(int))
    metrics = evaluation_metrics(test_pdf[LABEL], predict_proba(model, test_pdf))
    return TrainingResult(model, metrics, len(train_pdf), len(test_pdf))


def risk_band(p, high: float = 0.6, medium: float = 0.3) -> np.ndarray:
    p = np.asarray(p, dtype=float)
    return np.where(p >= high, "high", np.where(p >= medium, "medium", "low"))
