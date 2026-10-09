/* bundle.js: turn a design into a Databricks Asset Bundle job (resources/<job>.job.yml) and a Mermaid diagram. */
(function (root) {
  'use strict';
  const B2S = root.B2S = root.B2S || {};
  const WHEEL = '../dist/*.whl';
  const ML_LIBRARIES = ['scikit-learn>=1.4,<1.8', 'mlflow>=2.16,<4', 'databricks-sdk>=0.30'];
  const COMMON = { catalog: '{{job.parameters.catalog}}', schema_prefix: '{{job.parameters.schema_prefix}}', run_id: '{{job.run_id}}' };

  const strings = obj => Object.fromEntries(Object.entries(obj || {}).filter(([, v]) => v !== '' && v !== undefined).map(([k, v]) => [k, String(v)]));

  function compute(node, comp, design, task) {
    if (comp.kind === 'condition' || comp.kind === 'pipeline') return;
    if (design.job.compute === 'job_cluster') {
      task.job_cluster_key = 'main';
      return;
    }
    if (comp.kind === 'python') task.environment_key = comp.environment;   // notebooks run on serverless by default
  }

  function body(node, comp, design) {
    const task = {};
    compute(node, comp, design, task);
    if (comp.kind === 'python') {
      task.python_wheel_task = { package_name: 'bronze_to_served', entry_point: 'b2s',
        named_parameters: { task: comp.id, ...COMMON, ...strings(node.params) } };
      if (design.job.compute === 'job_cluster') {
        task.libraries = [{ whl: WHEEL }].concat(comp.environment === 'ml' ? ML_LIBRARIES.map(p => ({ pypi: { package: p } })) : []);
      }
    } else if (comp.kind === 'notebook') {
      task.notebook_task = { notebook_path: `../${comp.path}.py`, base_parameters: { ...COMMON, ...strings(node.params) } };
    } else if (comp.kind === 'condition') {
      const c = node.condition || {};
      task.condition_task = { op: c.op || 'EQUAL_TO', left: String(c.left || ''), right: String(c.right || '') };
    } else if (comp.kind === 'pipeline') {
      task.pipeline_task = { pipeline_id: `\${resources.pipelines.${comp.path}.id}`, full_refresh: (node.params || {}).full_refresh === 'true' };
    }
    return task;
  }

  function task(node, comp, design, nodes) {
    const t = { task_key: node.task_key, description: comp.summary };
    const deps = design.edges.filter(e => e.to === node.id).map(e => {
      const d = { task_key: nodes.get(e.from).task_key };
      if (e.outcome) d.outcome = e.outcome;
      return d;
    });
    if (deps.length) t.depends_on = deps;
    if (node.run_if) t.run_if = node.run_if;
    if (node.max_retries) Object.assign(t, { max_retries: node.max_retries, min_retry_interval_millis: 60000, retry_on_timeout: false });
    if (node.timeout_seconds) t.timeout_seconds = node.timeout_seconds;
    const inner = body(node, comp, design);
    if (node.for_each) {
      t.for_each_task = { inputs: node.for_each.inputs, concurrency: node.for_each.concurrency || 1,
        task: { task_key: `${node.task_key}_iteration`, ...inner } };
    } else {
      Object.assign(t, inner);
    }
    return t;
  }

  function jobResource(design, catalog) {
    const comps = B2S.graph.byId(catalog), job = design.job, nodes = new Map(design.nodes.map(n => [n.id, n]));
    const order = B2S.graph.stages(design).stages.flat().concat(B2S.graph.stages(design).cyclic);
    const r = { name: job.name, description: job.description };
    if (job.tags && Object.keys(job.tags).length) r.tags = job.tags;
    r.max_concurrent_runs = job.max_concurrent_runs || 1;
    if (job.timeout_seconds) r.timeout_seconds = job.timeout_seconds;
    const trig = job.trigger || { type: 'manual' };
    if (trig.type === 'schedule') r.schedule = { quartz_cron_expression: trig.cron, timezone_id: trig.timezone || 'UTC', pause_status: 'UNPAUSED' };
    if (trig.type === 'file_arrival') r.trigger = { pause_status: 'UNPAUSED', file_arrival: { url: trig.url, min_time_between_triggers_seconds: trig.min_seconds || 60 } };
    if (trig.type === 'continuous') r.continuous = { pause_status: 'PAUSED' };
    if (trig.type !== 'continuous') r.queue = { enabled: true };
    if (job.notifications && (job.notifications.on_failure || []).length) r.email_notifications = { on_failure: job.notifications.on_failure };
    if ((job.parameters || []).length) r.parameters = job.parameters.map(p => ({ name: p.name, default: String(p.default ?? '') }));
    const used = new Set(design.nodes.map(n => comps.get(n.component)).filter(c => c && c.kind === 'python').map(c => c.environment));
    if (job.compute === 'job_cluster') {
      r.job_clusters = [{ job_cluster_key: 'main', new_cluster: { spark_version: '${var.classic_spark_version}', node_type_id: '${var.node_type_id}',
        num_workers: 1, data_security_mode: 'SINGLE_USER', spark_conf: { 'spark.sql.session.timeZone': 'UTC' } } }];
    } else if (used.size) {
      r.environments = ['default', 'ml'].filter(k => used.has(k)).map(k => ({ environment_key: k,
        spec: { environment_version: '${var.serverless_environment_version}', dependencies: [WHEEL].concat(k === 'ml' ? ML_LIBRARIES : []) } }));
    }
    r.tasks = order.map(id => nodes.get(id)).filter(n => comps.has(n.component)).map(n => task(n, comps.get(n.component), design, nodes));
    return r;
  }

  function toYaml(design, catalog, source) {
    const header = [
      '# Generated by the Bronze to Served pipeline designer' + (source ? ` from ${source}` : '') + '.',
      '# Change the design in designer/index.html (or the template JSON), then run: node designer/scripts/build.js',
      '# CI fails when this file and its template disagree.',
    ].join('\n');
    return header + '\n' + B2S.yaml.dump({ resources: { jobs: { [design.job.key]: jobResource(design, catalog) } } });
  }

  function toMermaid(design, catalog) {
    const comps = B2S.graph.byId(catalog), nodes = new Map(design.nodes.map(n => [n.id, n]));
    const lines = ['flowchart LR'];
    catalog.layers.forEach(l => lines.push(`  classDef ${l.id} stroke:${l.color},stroke-width:2px`));
    design.nodes.forEach(n => {
      const c = comps.get(n.component) || { title: n.component, layer: 'ops' };
      const shape = c.kind === 'condition' ? [`{"`, `"}`] : [`["`, `"]`];
      lines.push(`  ${n.id}${shape[0]}${n.task_key}<br/><small>${c.title}</small>${shape[1]}:::${c.layer}`);
    });
    design.edges.forEach(e => {
      if (!nodes.has(e.from) || !nodes.has(e.to)) return;
      lines.push(e.outcome ? `  ${e.from} -- ${e.outcome} --> ${e.to}` : `  ${e.from} --> ${e.to}`);
    });
    return lines.join('\n') + '\n';
  }

  B2S.bundle = { jobResource, toYaml, toMermaid };
})(typeof window !== 'undefined' ? window : globalThis);
