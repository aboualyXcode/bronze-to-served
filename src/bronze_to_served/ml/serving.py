"""Model Serving: create or update a real-time endpoint for a model version (Databricks SDK)."""
from __future__ import annotations


def deploy(endpoint_name: str, model_name: str, version: str, workload_size: str = "Small",
           scale_to_zero: bool = True) -> str:
    """Point `endpoint_name` at `model_name` version `version`. Returns created, updated or unchanged."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

    w = WorkspaceClient()
    entity = ServedEntityInput(entity_name=model_name, entity_version=str(version), workload_size=workload_size,
                               scale_to_zero_enabled=scale_to_zero)
    try:
        existing = w.serving_endpoints.get(endpoint_name)
    except NotFound:
        w.serving_endpoints.create_and_wait(name=endpoint_name, config=EndpointCoreConfigInput(served_entities=[entity]))
        return "created"
    served = (existing.config.served_entities or []) if existing.config else []
    if any(e.entity_name == model_name and str(e.entity_version) == str(version) for e in served):
        return "unchanged"
    w.serving_endpoints.update_config_and_wait(name=endpoint_name, served_entities=[entity])
    return "updated"
