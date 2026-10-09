#!/usr/bin/env node
/* Designer tests: YAML writer, graph rules, validation and the golden bundle files. node designer/tests/run.js */
'use strict';
const assert = require('assert');
const path = require('path');
const { generated, templates, staleJobFiles } = require('../scripts/build.js');
const fs = require('fs');
const B2S = globalThis.B2S;
const ROOT = path.resolve(__dirname, '..', '..');
let passed = 0, failed = 0;
function test(name, fn) {
  try { fn(); passed += 1; console.log(`  ok   ${name}`); } catch (e) { failed += 1; console.log(`  FAIL ${name}\n       ${e.message}`); }
}
const design = (nodes, edges, job) => ({ job: Object.assign({ key: 'j', name: 'j', compute: 'serverless', trigger: { type: 'manual' },
  parameters: [{ name: 'catalog' }, { name: 'schema_prefix' }] }, job || {}), nodes, edges });
const node = (id, component, extra) => Object.assign({ id, component, task_key: id, params: {} }, extra || {});
const codes = d => B2S.graph.validate(d, B2S.CATALOG).map(p => p.code);

test('YAML scalars are quoted only when needed', () => {
  const s = B2S.yaml.scalar;
  assert.strictEqual(s('bronze.orders'), 'bronze.orders');
  assert.strictEqual(s('${var.catalog}'), '"${var.catalog}"');
  assert.strictEqual(s('0 0 2 * * ?'), '"0 0 2 * * ?"');
  assert.strictEqual(s('true'), '"true"');
  assert.strictEqual(s('3'), '"3"');
  assert.strictEqual(s(''), '""');
  assert.strictEqual(s(false), 'false');
  assert.strictEqual(s('{{job.run_id}}'), '"{{job.run_id}}"');
});

test('YAML lists of mappings are indented under their key', () => {
  const text = B2S.yaml.dump({ tasks: [{ task_key: 'a', depends_on: [{ task_key: 'b' }] }], tags: {} });
  assert.strictEqual(text, 'tasks:\n  - task_key: a\n    depends_on:\n      - task_key: b\ntags: {}\n');
});

test('stages group tasks that can run in parallel; cycles are detected', () => {
  const d = design([node('a', 'bronze.orders'), node('b', 'bronze.customers_cdc'), node('c', 'quality.gate')],
    [{ from: 'a', to: 'c' }, { from: 'b', to: 'c' }]);
  assert.deepStrictEqual(B2S.graph.stages(d).stages, [['a', 'b'], ['c']]);
  assert.ok(B2S.graph.wouldCycle(d, 'c', 'a'));
  d.edges.push({ from: 'c', to: 'a' });
  assert.ok(codes(d).includes('cycle'));
});

test('auto-wire connects tasks by table lineage, without redundant edges', () => {
  const d = design([node('bo', 'bronze.orders'), node('so', 'silver.orders'), node('gate', 'quality.gate'), node('gs', 'gold.sales'),
    node('gd', 'gold.dimensions'), node('sc', 'silver.customers'), node('bc', 'bronze.customers_cdc'), node('sr', 'silver.reference'),
    node('br', 'bronze.reference')], []);
  const { design: wired, added } = B2S.graph.autoWire(d, B2S.CATALOG);
  assert.ok(added >= 7, `added ${added}`);
  assert.ok(B2S.graph.reachable(wired, 'bo', 'gs') && B2S.graph.reachable(wired, 'so', 'gate'));
  assert.ok(!codes(wired).includes('missing-dependency'));
  assert.strictEqual(B2S.graph.autoWire(wired, B2S.CATALOG).added, 0);
});

test('validation catches the mistakes that break real jobs', () => {
  assert.ok(codes(design([node('a', 'silver.orders'), node('b', 'bronze.orders')], [])).includes('missing-dependency'));
  assert.ok(codes(design([node('x', 'bronze.orders'), node('x2', 'bronze.orders', { task_key: 'x' })], [])).includes('duplicate-key'));
  assert.ok(codes(design([node('s', 'silver.clickstream', { params: { trigger: 'processingTime=1 minute' } })], [])).includes('serverless-trigger'));
  assert.ok(codes(design([node('a', 'bronze.orders'), node('b', 'silver.orders')], [{ from: 'a', to: 'b', outcome: 'true' }])).includes('outcome'));
  assert.ok(codes(design([node('p', 'ml.promote')], [])).includes('ungated-promotion'));
  assert.ok(codes(design([node('a', 'bronze.orders')], [], { parameters: [] })).includes('job-params'));
  assert.ok(codes(design([node('g', 'control.condition', { condition: { op: 'EQUAL_TO', left: '{{tasks.nope.values.x}}', right: 'true' } })], [])).includes('task-value'));
  assert.ok(codes(design([node('m', 'ops.maintain_table', { for_each: { inputs: 'not json' }, params: { table: '{{input}}' } })], [])).includes('for-each'));
  assert.ok(codes(design([node('a', 'silver.orders', { params: { x: '{{job.parameters.nope}}' } })], [])).includes('job-param-ref'));
  assert.ok(codes(design([], [], { key: 'stagedoor_declarative' })).includes('job-key-clash'));
});

test('every template is valid, with no warnings', () => {
  templates().forEach(t => {
    const problems = B2S.graph.validate(t.design, B2S.CATALOG);
    assert.deepStrictEqual(problems, [], `${t.file}: ${JSON.stringify(problems)}`);
  });
});

test('layout places every task after its dependencies', () => {
  templates().forEach(t => {
    const laid = B2S.graph.layout(t.design), pos = new Map(laid.nodes.map(n => [n.id, n]));
    laid.edges.forEach(e => assert.ok(pos.get(e.from).x < pos.get(e.to).x, `${t.file}: ${e.from} -> ${e.to}`));
  });
});

test('the committed bundle jobs are exactly what the templates generate', () => {
  Object.entries(generated()).forEach(([rel, text]) => {
    const file = path.join(ROOT, rel);
    assert.ok(fs.existsSync(file), `${rel} is missing: run node designer/scripts/build.js`);
    assert.strictEqual(fs.readFileSync(file, 'utf8'), text, `${rel} is out of date: run node designer/scripts/build.js`);
  });
});

test('no generated job file outlives its template, and job keys never reuse a pipeline key', () => {
  assert.deepStrictEqual(staleJobFiles(), [], 'run node designer/scripts/build.js to remove them');
  const keys = templates().map(t => t.design.job.key);
  const pipelines = B2S.CATALOG.components.filter(c => c.kind === 'pipeline').map(c => c.path);
  assert.strictEqual(new Set(keys).size, keys.length);
  keys.forEach(k => assert.ok(!pipelines.includes(k), `${k} is also a pipeline key`));
});

test('Mermaid output names every task and branch', () => {
  const t = templates().find(x => x.design.job.key === 'stagedoor_ml_training');
  const m = B2S.bundle.toMermaid(t.design, B2S.CATALOG);
  assert.ok(m.startsWith('flowchart LR') && m.includes('gate -- true --> promote') && m.includes('promotion_gate'));
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
