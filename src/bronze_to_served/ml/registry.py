"""MLflow tracking and the Unity Catalog model registry: log, register, alias, load.

Aliases carry the deployment state: `challenger` (just trained), `champion` (serving and batch scoring)
and `previous_champion` (one-step rollback). Code always loads models by alias, never by version number.
"""
from __future__ import annotations

import inspect
import logging

log = logging.getLogger(__name__)
CHALLENGER, CHAMPION, PREVIOUS = "challenger", "champion", "previous_champion"


def configure(experiment_path: str | None = None) -> None:
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    if experiment_path:
        mlflow.set_experiment(experiment_path)


def log_and_register(model, sample, metrics: dict, params: dict, model_name: str, *, run_name: str,
                     training_df=None, training_table: str | None = None, tags: dict | None = None) -> tuple[str, str]:
    """Log the fitted pipeline with its signature and lineage, register it, and return (run_id, version)."""
    import mlflow
    from mlflow.models import infer_signature

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_params(params)
        mlflow.log_metrics({k: float(v) for k, v in metrics.items() if isinstance(v, (int, float))})
        mlflow.set_tags(tags or {})
        if training_df is not None:
            try:      # dataset lineage: the model links back to the Unity Catalog table it was trained on
                mlflow.log_input(mlflow.data.from_spark(training_df, table_name=training_table), context="training")
            except Exception as exc:
                log.warning("could not log the training dataset: %s", exc)
        signature = infer_signature(sample, model.predict_proba(sample))
        # MLflow 3 names logged models with `name`; MLflow 2 uses `artifact_path`.
        where = {"name": "model"} if "name" in inspect.signature(mlflow.sklearn.log_model).parameters \
            else {"artifact_path": "model"}
        info = mlflow.sklearn.log_model(model, signature=signature, input_example=sample.head(5),
                                        pyfunc_predict_fn="predict_proba", **where)
        version = mlflow.register_model(info.model_uri, model_name).version
    return run.info.run_id, str(version)


def set_alias(model_name: str, alias: str, version: str) -> None:
    from mlflow.tracking import MlflowClient
    MlflowClient().set_registered_model_alias(model_name, alias, str(version))
    log.info("%s@%s -> version %s", model_name, alias, version)


def version_for(model_name: str, alias: str) -> str | None:
    from mlflow.exceptions import MlflowException
    from mlflow.tracking import MlflowClient
    try:
        return str(MlflowClient().get_model_version_by_alias(model_name, alias).version)
    except MlflowException:
        return None


def load(model_name: str, version: str):
    import mlflow
    return mlflow.sklearn.load_model(f"models:/{model_name}/{version}")
