# The guide

| Chapter | Covers |
|---|---|
| [1. Architecture](01-architecture.md) | The platform end to end, how components connect, the Unity Catalog layout, where to start reading |
| [2. Unity Catalog and governance](02-unity-catalog.md) | Catalogs and schemas per environment and layer, volumes, tables as contracts, grants, tags, masking, lineage |
| [3. Ingestion](03-ingestion.md) | Auto Loader, COPY INTO, Kafka, CDC files, and what to do when upstream changes its schema |
| [4. The medallion](04-medallion.md) | What each layer promises; SCD1, SCD2, exactly-once, quarantine, the quality gate, the star schema, late data |
| [5. Streaming](05-streaming.md) | Triggers, checkpoints, idempotent micro-batches, watermarks, continuous jobs, serverless and classic compute |
| [6. Declarative pipelines](06-declarative-pipelines.md) | The same flows as a Lakeflow Declarative Pipeline, and when to choose which |
| [7. The ML lifecycle](07-ml-lifecycle.md) | Point-in-time features, out-of-time evaluation, champion/challenger, aliases, serving, scoring, drift |
| [8. Orchestration and CI/CD](08-orchestration-and-cicd.md) | The seven jobs, task types, parameters, targets, GitHub Actions |
| [9. The pipeline designer](09-pipeline-designer.md) | Designing jobs visually, the checks, how designs become bundle YAML |
| [10. Testing](10-testing.md) | What each suite proves, the differential oracle, what needs a workspace |
| [Scenario catalog](scenarios.md) | Every scenario, where it lives, where it runs and what tests it |
| [Data dictionary](data-dictionary.md) | Every table and column, generated from the contracts |
