"""Feature drift with the population stability index (PSI). Pure numpy.

PSI < 0.1 stable, 0.1-0.25 moderate, > 0.25 significant. Bins are deciles of the baseline; missing values
are their own bin, so a feature that suddenly goes missing is drift too.
"""
from __future__ import annotations

import numpy as np


def psi(expected, actual, bins: int = 10, eps: float = 1e-4) -> float:
    e, a = np.asarray(expected, dtype=float), np.asarray(actual, dtype=float)
    e_missing, a_missing = np.isnan(e), np.isnan(a)
    observed = e[~e_missing]
    edges = np.unique(np.quantile(observed, np.linspace(0, 1, bins + 1))) if observed.size else np.array([])

    def shares(values, missing):
        present = values[~missing]
        if edges.size > 1:
            counts = np.histogram(np.clip(present, edges[0], edges[-1]), bins=edges)[0]
        else:
            counts = np.array([present.size])
        counts = np.append(counts, missing.sum())
        return counts / max(values.size, 1)

    pe, pa = np.clip(shares(e, e_missing), eps, None), np.clip(shares(a, a_missing), eps, None)
    return float(np.sum((pa - pe) * np.log(pa / pe)))


def status(value: float, moderate: float = 0.1, significant: float = 0.25) -> str:
    return "significant" if value > significant else "moderate" if value > moderate else "stable"


def feature_drift(baseline, current, features: list[str]) -> list[dict]:
    """PSI and status per feature for two pandas DataFrames of numeric columns."""
    out = []
    for f in features:
        value = psi(baseline[f].to_numpy(dtype=float), current[f].to_numpy(dtype=float))
        out.append({"feature": f, "psi": round(value, 6), "status": status(value)})
    return out
