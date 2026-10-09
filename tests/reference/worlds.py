"""Generated landing folders shared by the tests, built once per test session."""
from __future__ import annotations

import tempfile
from functools import lru_cache

from bronze_to_served.stagedoor.datagen import GeneratorSettings, write_days

WORLDS = {
    # small and quick: contracts, invariants, reconciliation
    "small": GeneratorSettings(customers=600, days=300, clickstream_scale=0.5),
    # enough customers and history for the churn model to have stable metrics
    "ml": GeneratorSettings(customers=1500, days=360, clickstream_scale=0.4),
}


@lru_cache(maxsize=None)
def landing(name: str = "small") -> str:
    settings = WORLDS[name]
    root = tempfile.mkdtemp(prefix=f"b2s-{name}-")
    write_days(settings, root, 0, settings.days - 1)
    return root


@lru_cache(maxsize=None)
def reference(name: str = "small"):
    from reference.oracle import build
    return build(landing(name))
