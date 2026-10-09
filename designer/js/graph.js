/* graph.js: the design as a graph. Order, cycles, lineage-based wiring, layout and validation.
   A design is { job: {...}, nodes: [{ id, component, task_key, params, ... }], edges: [{ from, to, outcome? }] }. */
(function (root) {
  'use strict';
  const B2S = root.B2S = root.B2S || {};
  const TASK_KEY = /^[A-Za-z0-9_-]{1,100}$/;
  const EARLY = new Set(['landing', 'bronze']);
  const LATE = new Set(['gold', 'ml', 'serving']);

  const byId = catalog => new Map(catalog.components.map(c => [c.id, c]));
  const soft = ref => ref.endsWith('?');
  const bare = ref => ref.replace(/\?$/, '');
  const layerOf = ref => ref.startsWith('landing/') ? 'landing' : ref.split('.')[0];

  function successors(design) {
    const out = new Map(design.nodes.map(n => [n.id, []]));
    design.edges.forEach(e => { if (out.has(e.from) && out.has(e.to)) out.get(e.from).push(e.to); });
    return out;
  }

  function reachable(design, from, to) {
    const next = successors(design), seen = new Set([from]), queue = [from];
    while (queue.length) {
      const id = queue.shift();
      if (id === to) return true;
      for (const n of next.get(id) || []) if (!seen.has(n)) { seen.add(n); queue.push(n); }
    }
    return false;
  }

  const wouldCycle = (design, from, to) => from === to || reachable(design, to, from);

  /* Stages: tasks in the same stage can run in parallel. Nodes left over are on a cycle. */
  function stages(design) {
    const indeg = new Map(design.nodes.map(n => [n.id, 0]));
    design.edges.forEach(e => { if (indeg.has(e.to)) indeg.set(e.to, indeg.get(e.to) + 1); });
    const next = successors(design), out = [];
    let frontier = design.nodes.filter(n => indeg.get(n.id) === 0).map(n => n.id);
    const done = new Set();
    while (frontier.length) {
      out.push(frontier);
      frontier.forEach(id => done.add(id));
      const following = [];
      frontier.forEach(id => (next.get(id) || []).forEach(m => {
        indeg.set(m, indeg.get(m) - 1);
        if (indeg.get(m) === 0) following.push(m);
      }));
      frontier = following;
    }
    return { stages: out, cyclic: design.nodes.map(n => n.id).filter(id => !done.has(id)) };
  }

  function upstream(design, id) {
    const prev = new Map(design.nodes.map(n => [n.id, []]));
    design.edges.forEach(e => { if (prev.has(e.to)) prev.get(e.to).push(e.from); });
    const seen = new Set(), queue = [...(prev.get(id) || [])];
    while (queue.length) {
      const n = queue.shift();
      if (seen.has(n)) continue;
      seen.add(n);
      queue.push(...(prev.get(n) || []));
    }
    return seen;
  }

  function writers(design, catalog) {
    const comps = byId(catalog), out = new Map();
    design.nodes.forEach(n => ((comps.get(n.component) || {}).writes || []).forEach(ref => {
      if (!out.has(ref)) out.set(ref, []);
      out.get(ref).push(n.id);
    }));
    return out;
  }

  /* Add an edge wherever a task reads what another task in the design writes, unless it is already
     ordered after it (directly or transitively) or the edge would close a cycle. */
  function autoWire(design, catalog) {
    const comps = byId(catalog), w = writers(design, catalog);
    const result = { ...design, edges: design.edges.slice() };
    let added = 0;
    const order = stages(design).stages.flat();
    const rank = new Map(order.map((id, i) => [id, i]));
    const nodes = design.nodes.slice().sort((a, b) => (rank.get(a.id) ?? 1e9) - (rank.get(b.id) ?? 1e9));
    nodes.forEach(n => ((comps.get(n.component) || {}).reads || []).filter(r => !soft(r)).forEach(ref => {
      (w.get(ref) || []).forEach(src => {
        if (src === n.id || reachable(result, src, n.id) || wouldCycle(result, src, n.id)) return;
        result.edges.push({ from: src, to: n.id });
        added += 1;
      });
    }));
    return { design: result, added };
  }

  /* Columns by dependency depth (edges always flow left to right), rows by the barycentre of parents. */
  function layout(design) {
    const { stages: cols, cyclic } = stages(design);
    if (cyclic.length) cols.push(cyclic);
    const row = {}, pos = {};
    cols.forEach((col, ci) => {
      const scored = col.map((id, i) => {
        const parents = design.edges.filter(e => e.to === id && row[e.from] !== undefined).map(e => row[e.from]);
        return { id, i, score: parents.length ? parents.reduce((a, b) => a + b, 0) / parents.length : i };
      }).sort((a, b) => a.score - b.score || a.i - b.i);
      scored.forEach((s, ri) => { row[s.id] = ri; pos[s.id] = { x: 32 + ci * 256, y: 64 + ri * 96 }; });
    });
    return { ...design, nodes: design.nodes.map(n => ({ ...n, ...pos[n.id] })) };
  }

  function taskValueRefs(text) {
    return [...String(text || '').matchAll(/\{\{tasks\.([A-Za-z0-9_-]+)\.values\.([A-Za-z0-9_]+)\}\}/g)].map(m => m[1]);
  }

  function jobParamRefs(node) {
    const texts = Object.values(node.params || {}).concat(node.condition ? [node.condition.left, node.condition.right] : []);
    return texts.flatMap(t => [...String(t || '').matchAll(/\{\{job\.parameters\.([A-Za-z0-9_]+)\}\}/g)].map(m => m[1]));
  }

  function validate(design, catalog) {
    const comps = byId(catalog), problems = [];
    const add = (level, code, message, nodes) => problems.push({ level, code, message, nodes: nodes || [] });
    const job = design.job || {}, params = new Set((job.parameters || []).map(p => p.name));
    const nodes = new Map(design.nodes.map(n => [n.id, n]));
    const keyToId = new Map(design.nodes.map(n => [n.task_key, n.id]));
    const continuous = (job.trigger || {}).type === 'continuous';

    if (!TASK_KEY.test(job.key || '')) add('error', 'job-key', 'The job needs a resource key of letters, digits, - and _.');
    if (catalog.components.some(c => c.kind === 'pipeline' && c.path === job.key)) {
      add('error', 'job-key-clash', `The job key ${job.key} is already a pipeline's key: bundle resource keys must be unique across all resource types.`);
    }
    if (!design.nodes.length) add('warning', 'empty', 'Add components from the palette to start a pipeline.');
    const seen = new Map();
    design.nodes.forEach(n => {
      const c = comps.get(n.component);
      if (!c) { add('error', 'unknown-component', `${n.task_key}: unknown component ${n.component}.`, [n.id]); return; }
      if (!TASK_KEY.test(n.task_key || '')) add('error', 'task-key', `${n.task_key || '(empty)'}: task keys use letters, digits, - and _.`, [n.id]);
      if (seen.has(n.task_key)) add('error', 'duplicate-key', `Two tasks are called ${n.task_key}.`, [seen.get(n.task_key), n.id]);
      seen.set(n.task_key, n.id);
      if (c.kind === 'python' && !(params.has('catalog') && params.has('schema_prefix'))) {
        add('error', 'job-params', 'Python tasks need the job parameters catalog and schema_prefix.', [n.id]);
      }
      jobParamRefs(n).filter(p => !params.has(p)).forEach(p => add('error', 'job-param-ref', `${n.task_key} uses {{job.parameters.${p}}}, which the job does not define.`, [n.id]));
      if (c.kind === 'condition') {
        const cond = n.condition || {};
        if (!cond.op || cond.left === undefined || cond.left === '' || cond.right === undefined) add('error', 'condition', `${n.task_key}: a condition needs an operator, a left value and a right value.`, [n.id]);
        const outgoing = design.edges.filter(e => e.from === n.id);
        if (outgoing.some(e => e.outcome !== 'true' && e.outcome !== 'false')) add('error', 'outcome', `${n.task_key}: each branch must follow the true or the false outcome.`, [n.id]);
      }
      const ups = upstream(design, n.id);
      Object.values(n.params || {}).concat(n.condition ? [n.condition.left, n.condition.right] : []).forEach(text => taskValueRefs(text).forEach(key => {
        const src = keyToId.get(key);
        if (!src) add('error', 'task-value', `${n.task_key} reads a value from ${key}, which is not in this job.`, [n.id]);
        else if (!ups.has(src)) add('error', 'task-value-order', `${n.task_key} reads a value from ${key}, so it must run after it.`, [n.id, src]);
      }));
      (c.params || []).filter(p => p.name === 'table' && c.id === 'ops.maintain_table').forEach(() => {
        if (!(n.params || {}).table) add('error', 'required-param', `${n.task_key}: set the table to maintain (or {{input}} in a for-each).`, [n.id]);
      });
      if (n.for_each) {
        let ok = false;
        try { ok = Array.isArray(JSON.parse(n.for_each.inputs)); } catch (e) { ok = false; }
        if (!ok) add('error', 'for-each', `${n.task_key}: for-each inputs must be a JSON array.`, [n.id]);
      }
      const trigger = (n.params || {}).trigger || '';
      if (c.streaming && trigger.startsWith('processingTime') && job.compute !== 'job_cluster') {
        add('error', 'serverless-trigger', `${n.task_key}: serverless compute only runs streams with availableNow. Use job cluster compute for processing-time triggers.`, [n.id]);
      }
      if (c.layer === 'setup' && c.id === 'setup.uc_objects' && (job.trigger || {}).type === 'schedule') {
        add('warning', 'setup-scheduled', `${n.task_key}: setup is one-off; keep it in a manually triggered job.`, [n.id]);
      }
      if (c.id === 'ml.promote' && ![...ups].some(id => (comps.get((nodes.get(id) || {}).component) || {}).kind === 'condition')) {
        add('warning', 'ungated-promotion', `${n.task_key}: promotion runs without a validation gate (add a condition before it).`, [n.id]);
      }
      (c.reads || []).filter(r => !soft(r)).forEach(ref => {
        if (LATE.has(c.layer) && EARLY.has(layerOf(ref))) add('warning', 'layer-skip', `${n.task_key} reads ${ref}: ${c.layer} should build on Silver or Gold, not raw data.`, [n.id]);
      });
    });
    design.edges.forEach(e => {
      if (!nodes.has(e.from) || !nodes.has(e.to)) { add('error', 'dangling-edge', 'A dependency points at a missing task.'); return; }
      const c = comps.get(nodes.get(e.from).component) || {};
      if (e.outcome && c.kind !== 'condition') add('error', 'outcome', `${nodes.get(e.to).task_key} follows an outcome of ${nodes.get(e.from).task_key}, which is not a condition.`, [e.from, e.to]);
    });
    const { cyclic } = stages(design);
    if (cyclic.length) add('error', 'cycle', `These tasks depend on each other in a loop: ${cyclic.map(id => nodes.get(id).task_key).join(', ')}.`, cyclic);
    const w = writers(design, catalog);
    design.nodes.forEach(n => {
      const c = comps.get(n.component);
      if (!c) return;
      const ups = upstream(design, n.id);
      (c.reads || []).filter(r => !soft(r)).forEach(ref => (w.get(ref) || []).forEach(src => {
        if (src === n.id || ups.has(src) || cyclic.length) return;
        const sc = comps.get(nodes.get(src).component);
        if (continuous && c.streaming && sc && sc.streaming) return;   // always-on streams feed each other
        add('warning', 'missing-dependency', `${n.task_key} reads ${ref}, which ${nodes.get(src).task_key} writes, but does not run after it.`, [n.id, src]);
      }));
    });
    w.forEach((ids, ref) => {
      if (ids.length < 2 || ref.startsWith('ops.') || ref.startsWith('landing/')) return;
      for (let i = 0; i < ids.length; i++) for (let j = i + 1; j < ids.length; j++) {
        if (!reachable(design, ids[i], ids[j]) && !reachable(design, ids[j], ids[i])) {
          add('warning', 'concurrent-writers', `${nodes.get(ids[i]).task_key} and ${nodes.get(ids[j]).task_key} both write ${ref} and may run at the same time.`, [ids[i], ids[j]]);
        }
      }
    });
    if (design.nodes.length > 1 && !continuous) {
      design.nodes.filter(n => !design.edges.some(e => e.from === n.id || e.to === n.id))
        .forEach(n => add('warning', 'orphan', `${n.task_key} is not connected to anything.`, [n.id]));
    }
    return problems;
  }

  B2S.graph = { stages, reachable, wouldCycle, upstream, writers, autoWire, layout, validate, byId };
})(typeof window !== 'undefined' ? window : globalThis);
