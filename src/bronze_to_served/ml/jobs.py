"""ML tasks: train -> evaluate -> (validation notebook + condition) -> promote -> deploy; batch scoring; drift."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone

from pyspark.sql import functions as F

from ..core.contracts import conform
from ..core.delta_ops import append, date_list_predicate, replace_where
from ..stagedoor.gold import data_as_of
from ..stagedoor.tables import spec
from ..tasks.context import TaskContext
from . import registry
from .drift import feature_drift
from .inference import score_frame
from .model import FEATURES, LABEL, NUMERIC, TrainParams, evaluation_metrics, numeric_frame, predict_proba, prepare, \
    train_and_evaluate
from .policy import Decision, PromotionPolicy, decide

EVALUATIONS_SCHEMA = ("run_id string, model_name string, model_version string, role string, test_rows bigint, "
                      "roc_auc double, pr_auc double, log_loss double, brier double, top_decile_lift double, "
                      "promote boolean, reasons array<string>, evaluated_at timestamp")


def _model(ctx: TaskContext) -> str:
    return ctx.cfg.model_name(ctx.param("model_name", "churn_model"))


def train(ctx: TaskContext) -> dict:
    data = ctx.spark.read.table(ctx.table("ml.churn_training_set"))
    train_sdf = data.where("split = 'train'")
    train_pdf, test_pdf = train_sdf.toPandas(), data.where("split = 'test'").toPandas()
    params = TrainParams.from_dict(json.loads(ctx.param("hyperparameters", "{}")))
    result = train_and_evaluate(train_pdf, test_pdf, params)
    registry.configure(ctx.param("experiment_path"))
    name = _model(ctx)
    run_id, version = registry.log_and_register(
        result.model, prepare(train_pdf).head(200), result.metrics, asdict(params), name, run_name=f"churn-{ctx.run_id}",
        training_df=train_sdf, training_table=ctx.table("ml.churn_training_set").replace("`", ""),
        tags={"job_run_id": ctx.run_id, "train_rows": str(result.train_rows)})
    registry.set_alias(name, registry.CHALLENGER, version)
    return {"model": name, "version": version, "mlflow_run_id": run_id,
            **{k: round(v, 4) for k, v in result.metrics.items() if isinstance(v, float)}}


def evaluate(ctx: TaskContext) -> dict:
    """Score challenger and champion on the same out-of-time test set; record metrics and the decision."""
    registry.configure()
    name = _model(ctx)
    challenger, champion = registry.version_for(name, registry.CHALLENGER), registry.version_for(name, registry.CHAMPION)
    if challenger is None:
        raise RuntimeError(f"{name} has no challenger; run ml.train first")
    test = ctx.spark.read.table(ctx.table("ml.churn_training_set")).where("split = 'test'").toPandas()
    y = test[LABEL].astype(int)
    scores = {"challenger": (challenger, evaluation_metrics(y, predict_proba(registry.load(name, challenger), test)))}
    if champion and champion != challenger:
        scores["champion"] = (champion, evaluation_metrics(y, predict_proba(registry.load(name, champion), test)))
    if champion == challenger:
        decision = Decision(False, ["the challenger is already the champion"])
    else:
        decision = decide(scores["challenger"][1], scores.get("champion", (None, None))[1],
                          PromotionPolicy.from_json(ctx.param("policy", "{}")))
    now = datetime.now(timezone.utc)
    rows = [(ctx.run_id, name, version, role, m["rows"], m["roc_auc"], m["pr_auc"], m["log_loss"], m["brier"],
             m["top_decile_lift"], decision.promote if role == "challenger" else None,
             decision.reasons if role == "challenger" else [], now) for role, (version, m) in scores.items()]
    append(ctx.spark.createDataFrame(rows, EVALUATIONS_SCHEMA), ctx.table("ml.model_evaluations"))
    return {"promote": decision.promote, "challenger": challenger, "champion": champion, "reasons": decision.reasons}


def promote(ctx: TaskContext) -> dict:
    registry.configure()
    name = _model(ctx)
    challenger, champion = registry.version_for(name, registry.CHALLENGER), registry.version_for(name, registry.CHAMPION)
    if challenger is None:
        raise RuntimeError(f"{name} has no challenger to promote")
    if challenger == champion:
        return {"unchanged": challenger}
    if champion:
        registry.set_alias(name, registry.PREVIOUS, champion)
    registry.set_alias(name, registry.CHAMPION, challenger)
    return {"champion": challenger, "previous_champion": champion}


def rollback(ctx: TaskContext) -> dict:
    """Swap champion and previous_champion: one step back, and one step forward again if run twice."""
    registry.configure()
    name = _model(ctx)
    previous, current = registry.version_for(name, registry.PREVIOUS), registry.version_for(name, registry.CHAMPION)
    if previous is None:
        raise RuntimeError(f"{name} has no previous_champion to roll back to")
    registry.set_alias(name, registry.CHAMPION, previous)
    if current:
        registry.set_alias(name, registry.PREVIOUS, current)
    return {"champion": previous, "previous_champion": current}


def deploy_endpoint(ctx: TaskContext) -> dict:
    if not ctx.bool_param("enabled", False):
        return {"skipped": "endpoint deployment is disabled (enabled=false)"}
    registry.configure()
    name = _model(ctx)
    version = registry.version_for(name, registry.CHAMPION)
    if version is None:
        raise RuntimeError(f"{name} has no champion to serve")
    from .serving import deploy
    endpoint = ctx.param("endpoint_name", "stagedoor-churn")
    outcome = deploy(endpoint, name, version, workload_size=ctx.param("workload_size", "Small"))
    return {"endpoint": endpoint, "version": version, "outcome": outcome}


def batch_inference(ctx: TaskContext) -> dict:
    registry.configure()
    name = _model(ctx)
    version = registry.version_for(name, registry.CHAMPION)
    if version is None:
        return {"skipped": "no champion model yet: run the training job first"}
    as_of = ctx.date_param("as_of_date") or data_as_of(ctx.spark, ctx.cfg)
    features = ctx.spark.read.table(ctx.table("ml.customer_features")).where(F.col("as_of_date") == F.lit(as_of))
    scored = score_frame(features, registry.load(name, version), name, version).withColumn("scored_at", F.current_timestamp())
    replace_where(conform(scored, spec("ml.churn_predictions")), ctx.table("ml.churn_predictions"),
                  date_list_predicate("as_of_date", [as_of]))
    return {"model_version": version, "as_of_date": as_of.isoformat()}


def drift(ctx: TaskContext) -> dict:
    as_of = ctx.date_param("as_of_date") or data_as_of(ctx.spark, ctx.cfg)
    baseline = ctx.spark.read.table(ctx.table("ml.churn_training_set")).where("split = 'train'").select(*NUMERIC).toPandas()
    current = (ctx.spark.read.table(ctx.table("ml.customer_features")).where(F.col("as_of_date") == F.lit(as_of))
               .select(*NUMERIC).toPandas())
    if baseline.empty or current.empty:
        return {"skipped": "need a training set and features for the scoring date"}
    results = feature_drift(numeric_frame(baseline), numeric_frame(current), NUMERIC)
    now = datetime.now(timezone.utc)
    rows = ctx.spark.createDataFrame([(as_of, r["feature"], r["psi"], r["status"], now) for r in results],
                                     "as_of_date date, feature string, psi double, status string, computed_at timestamp")
    replace_where(rows, ctx.table("ml.feature_drift"), date_list_predicate("as_of_date", [as_of]))
    significant = [r["feature"] for r in results if r["status"] == "significant"]
    if significant and ctx.bool_param("fail_on_drift", False):
        raise RuntimeError(f"significant drift in {significant}")
    return {"as_of_date": as_of.isoformat(), "significant": significant,
            "moderate": [r["feature"] for r in results if r["status"] == "moderate"]}


__all__ = ["train", "evaluate", "promote", "rollback", "deploy_endpoint", "batch_inference", "drift", "FEATURES"]
