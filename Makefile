# Common tasks: `make help` lists them.
PY ?= python3
ROOT ?= .local

.PHONY: help install install-local test test-unit test-spark test-designer generate check data local designer validate deploy clean

help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' Makefile | sed 's/:.*## /\t/'

install: ## the package with ML and dev extras (unit tests)
	$(PY) -m pip install -e ".[ml,dev]"

install-local: ## also local Spark and Delta Lake (needs Java 17)
	$(PY) -m pip install -e ".[ml,local,dev]"

test: test-unit test-designer ## unit and designer tests (no Spark)

test-unit: ## pytest tests/unit
	$(PY) -m pytest tests/unit -q

test-spark: ## Spark and Delta tests: MERGE patterns, differential, incremental
	$(PY) -m pytest tests/spark -q

test-designer: ## designer rules and generated bundle files
	node designer/tests/run.js

generate: ## regenerate the palette, the data dictionary and the bundle jobs
	PYTHONPATH=src $(PY) -m bronze_to_served.tasks.catalog designer/js/catalog.js
	PYTHONPATH=src $(PY) -m bronze_to_served.stagedoor.tables docs/data-dictionary.md
	node designer/scripts/build.js

check: ## fail if generated files are out of date (CI runs this)
	node designer/scripts/build.js --check

data: ## generate a small simulated landing folder for local runs
	PYTHONPATH=src $(PY) -m bronze_to_served.stagedoor.datagen --out $(ROOT)/volumes/landing/raw --customers 800 --days 240 --clickstream-scale 0.5

local: ## create the tables and run the daily job's DAG locally (after `make data`)
	PYTHONPATH=src $(PY) -m bronze_to_served.tasks.cli --task setup.uc_objects --runtime local --local_root $(ROOT)
	PYTHONPATH=src $(PY) -m bronze_to_served.tasks.local designer/templates/platform_daily.json --root $(ROOT) \
	  --skip land.sample_data --skip ml.batch_inference

designer: ## serve the pipeline designer on http://localhost:8000
	$(PY) -m http.server 8000 --directory designer

validate: ## databricks bundle validate
	databricks bundle validate

deploy: ## deploy to your dev target
	databricks bundle deploy -t dev

clean: ## remove local runs, builds and caches
	rm -rf $(ROOT) build dist designer/dist .pytest_cache spark-warehouse metastore_db derby.log
	find . -name __pycache__ -prune -exec rm -rf {} +
