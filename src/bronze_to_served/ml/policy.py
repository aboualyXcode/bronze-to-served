"""Champion/challenger promotion rules. Pure Python, so every rule is unit tested.

A challenger is promoted when it clears absolute floors, beats the champion on the primary metric by at
least `min_improvement`, and does not degrade any guardrail metric by more than its tolerance. With no
champion yet, clearing the floors is enough.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

LOWER_IS_BETTER = {"log_loss", "brier"}


@dataclass(frozen=True)
class PromotionPolicy:
    primary_metric: str = "pr_auc"
    min_improvement: float = 0.0
    guardrails: dict = field(default_factory=lambda: {"roc_auc": 0.01, "brier": 0.005})
    floors: dict = field(default_factory=lambda: {"roc_auc": 0.70})

    @classmethod
    def from_json(cls, text: str) -> "PromotionPolicy":
        return cls(**json.loads(text or "{}"))


@dataclass(frozen=True)
class Decision:
    promote: bool
    reasons: list


def _gain(metric: str, challenger: float, champion: float) -> float:
    """Positive when the challenger is better."""
    return champion - challenger if metric in LOWER_IS_BETTER else challenger - champion


def decide(challenger: dict, champion: dict | None, policy: PromotionPolicy = PromotionPolicy()) -> Decision:
    reasons, ok = [], True
    for metric, floor in policy.floors.items():
        value = challenger[metric]
        passed = value <= floor if metric in LOWER_IS_BETTER else value >= floor
        ok &= passed
        reasons.append(f"{metric} {value:.4f} {'meets' if passed else 'misses'} the floor {floor:.4f}")
    if champion is None:
        reasons.append("there is no champion yet")
        return Decision(ok, reasons)
    m = policy.primary_metric
    gain = _gain(m, challenger[m], champion[m])
    passed = gain >= policy.min_improvement
    ok &= passed
    reasons.append(f"{m} {challenger[m]:.4f} vs champion {champion[m]:.4f} "
                   f"({'+' if gain >= 0 else ''}{gain:.4f}, needs >= {policy.min_improvement:.4f})")
    for metric, tolerance in policy.guardrails.items():
        loss = -_gain(metric, challenger[metric], champion[metric])
        passed = loss <= tolerance
        ok &= passed
        reasons.append(f"guardrail {metric}: {challenger[metric]:.4f} vs {champion[metric]:.4f} "
                       f"({'within' if passed else 'beyond'} tolerance {tolerance:.4f})")
    return Decision(ok, reasons)
