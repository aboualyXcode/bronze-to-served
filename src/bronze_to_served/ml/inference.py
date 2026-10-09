"""Distributed batch scoring: the fitted pipeline travels to the executors inside the pandas function."""
from __future__ import annotations

SCORED_SCHEMA = ("customer_id string, as_of_date date, churn_probability double, risk_band string, "
                 "model_name string, model_version string")


def score_frame(features_df, model, model_name: str, version: str):
    def score(batches):
        import pandas as pd
        from bronze_to_served.ml.model import predict_proba, risk_band
        for pdf in batches:
            if pdf.empty:
                continue
            p = predict_proba(model, pdf)
            yield pd.DataFrame({"customer_id": pdf["customer_id"].values, "as_of_date": pdf["as_of_date"].values,
                                "churn_probability": p, "risk_band": risk_band(p), "model_name": model_name,
                                "model_version": str(version)})
    return features_df.mapInPandas(score, SCORED_SCHEMA)
