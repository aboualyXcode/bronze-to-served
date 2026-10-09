/* app.js: the designer's browser front end. The rules (validation, lineage wiring, layout, YAML) live in
   graph.js and bundle.js, shared with the Node tests and the bundle build; this file draws and edits. */
(function () {
  'use strict';
  const B2S = window.B2S, G = B2S.graph, CAT = B2S.CATALOG;
  const comps = G.byId(CAT);
  const layers = new Map(CAT.layers.map(l => [l.id, l]));
  const NODE_W = 208, NODE_H = 72, COL = 256, GRID = 8;
  const STORE = 'bronze-to-served-designer:v1', THEME = 'bronze-to-served-designer:theme';
  const RUN_IF = [['', 'All dependencies succeeded (default)'], ['ALL_DONE', 'All dependencies finished'],
    ['AT_LEAST_ONE_SUCCESS', 'At least one succeeded'], ['NONE_FAILED', 'None failed'],
    ['AT_LEAST_ONE_FAILED', 'At least one failed'], ['ALL_FAILED', 'All failed']];
  const OPS = [['EQUAL_TO', 'equals'], ['NOT_EQUAL', 'does not equal'], ['GREATER_THAN', 'is greater than'],
    ['GREATER_THAN_OR_EQUAL', 'is at least'], ['LESS_THAN', 'is less than'], ['LESS_THAN_OR_EQUAL', 'is at most']];
  const $ = id => document.getElementById(id);
  const clone = o => JSON.parse(JSON.stringify(o));
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const rgba = (hex, a) => { const n = parseInt(hex.slice(1), 16); return `rgba(${(n >> 16) & 255},${(n >> 8) & 255},${n & 255},${a})`; };

  function el(tag, attrs, ...kids) {
    const e = document.createElement(tag);
    Object.entries(attrs || {}).forEach(([k, v]) => {
      if (v === null || v === undefined || v === false) return;
      if (k === 'class') e.className = v;
      else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
      else e.setAttribute(k, v === true ? '' : v);
    });
    kids.flat().forEach(k => { if (k !== null && k !== undefined && k !== false) e.append(k.nodeType ? k : document.createTextNode(k)); });
    return e;
  }

  /* ---------- state, persistence, undo ---------- */
  const blankJob = () => ({ key: 'my_pipeline', name: 'my-pipeline', description: 'Designed with the Bronze to Served pipeline designer.',
    trigger: { type: 'manual' }, compute: 'serverless', max_concurrent_runs: 1, tags: { project: 'bronze-to-served' },
    notifications: { on_failure: ['${var.alert_email}'] },
    parameters: [{ name: 'catalog', default: '${var.catalog}' }, { name: 'schema_prefix', default: '${var.schema_prefix}' }] });
  function prepare(d) {
    d = clone(d);
    d.nodes.forEach(n => { n.params = n.params || {}; });
    return d.nodes.some(n => typeof n.x !== 'number' || typeof n.y !== 'number') ? G.layout(d) : d;
  }
  const defaultTemplate = () => B2S.TEMPLATES.find(t => t.design.job.key === 'stagedoor_platform') || B2S.TEMPLATES[0];
  function load() {
    try {
      const d = JSON.parse(localStorage.getItem(STORE) || 'null');
      if (d && d.job && Array.isArray(d.nodes) && Array.isArray(d.edges)) return prepare(d);
    } catch (e) { /* storage unavailable or unreadable: start from a template */ }
    return prepare(defaultTemplate().design);
  }
  let design = load(), selected = null, tab = 'yaml', pending = null, drag = null, toastTimer = null;
  const undoStack = [], redoStack = [];
  const save = () => { try { localStorage.setItem(STORE, JSON.stringify(design)); } catch (e) { /* private browsing */ } };
  const nodeById = id => design.nodes.find(n => n.id === id);
  function settle() {
    if (selected && selected.type === 'node' && !nodeById(selected.id)) selected = null;
    if (selected && selected.type === 'edge' && !design.edges[selected.index]) selected = null;
  }
  function commit(next) {
    undoStack.push(JSON.stringify(design));
    if (undoStack.length > 200) undoStack.shift();
    redoStack.length = 0;
    design = next; settle(); save(); render();
  }
  function change(mutator) { const next = clone(design); mutator(next); commit(next); }
  function editStart() { pending = JSON.stringify(design); }
  function edit(mutator) {
    if (pending !== null) { undoStack.push(pending); redoStack.length = 0; pending = null; }
    mutator(design); save(); refresh();
  }
  function undo() { if (!undoStack.length) return; redoStack.push(JSON.stringify(design)); design = JSON.parse(undoStack.pop()); settle(); save(); render(); }
  function redo() { if (!redoStack.length) return; undoStack.push(JSON.stringify(design)); design = JSON.parse(redoStack.pop()); settle(); save(); render(); }
  function pick(sel) { selected = sel; render(); }
  function toast(message) {
    const t = $('toast');
    t.textContent = message; t.classList.add('show');
    clearTimeout(toastTimer); toastTimer = setTimeout(() => t.classList.remove('show'), 2800);
  }

  /* ---------- editing operations ---------- */
  const colOf = x => Math.max(0, Math.round((x - 32) / COL));
  function uniqueKey(base, except) {
    let key = base, i = 2;
    while (design.nodes.some(n => n.task_key === key && n.id !== except)) key = `${base}_${i++}`;
    return key;
  }
  function placeFor(c) {
    const w = G.writers(design, CAT);
    const sources = (c.reads || []).map(r => r.replace(/\?$/, '')).flatMap(r => w.get(r) || []).map(nodeById).filter(Boolean);
    const col = sources.length ? Math.max(...sources.map(n => colOf(n.x))) + 1 : 0;
    let row = 0;
    while (design.nodes.some(n => colOf(n.x) === col && Math.abs(n.y - (64 + row * 96)) < 48)) row += 1;
    return { x: 32 + col * COL, y: 64 + row * 96 };
  }
  function addNode(componentId, at) {
    const c = comps.get(componentId);
    const node = Object.assign({ id: 'n' + Date.now().toString(36) + Math.floor(Math.random() * 1296).toString(36),
      component: componentId, task_key: uniqueKey(componentId.replace(/\./g, '_')), params: {} }, at || placeFor(c));
    if (c.kind === 'condition') node.condition = { op: 'EQUAL_TO', left: '', right: 'true' };
    selected = { type: 'node', id: node.id };
    change(d => { d.nodes.push(node); });
    toast(`Added ${c.title}. Drag from its right edge to the task that should run next, or use Wire by lineage.`);
  }
  function connect(from, to) {
    if (from === to) return;
    if (design.edges.some(e => e.from === from && e.to === to)) { toast('These tasks are already connected.'); return; }
    if (G.wouldCycle(design, from, to)) { toast(`That would make a loop: ${nodeById(from).task_key} already runs after ${nodeById(to).task_key}.`); return; }
    const edge = { from, to };
    if ((comps.get(nodeById(from).component) || {}).kind === 'condition') {
      edge.outcome = design.edges.some(e => e.from === from && e.outcome === 'true') ? 'false' : 'true';
    }
    change(d => { d.edges.push(edge); });
  }
  function removeNode(id) {
    change(d => { d.nodes = d.nodes.filter(n => n.id !== id); d.edges = d.edges.filter(e => e.from !== id && e.to !== id); });
  }
  function removeSelected() {
    if (!selected) return;
    if (selected.type === 'node') { removeNode(selected.id); return; }
    const i = selected.index;
    selected = null;
    change(d => { d.edges.splice(i, 1); });
  }

  /* ---------- palette ---------- */
  function badge(c) {
    const label = c.kind === 'notebook' ? 'Notebook' : c.kind === 'condition' ? 'Condition' : c.kind === 'pipeline' ? 'Pipeline' : c.streaming ? 'Stream' : null;
    return label ? el('span', { class: 'badge' }, label) : null;
  }
  function renderPalette() {
    const q = $('search').value.trim().toLowerCase();
    const box = $('groups');
    box.replaceChildren();
    CAT.layers.forEach(layer => {
      const items = CAT.components.filter(c => c.layer === layer.id && (!q ||
        [c.title, c.id, c.summary, ...c.capabilities, ...c.reads, ...c.writes].join(' ').toLowerCase().includes(q)));
      if (!items.length) return;
      box.append(el('section', { class: 'group' }, el('h3', { style: `--c:${layer.color}` }, layer.title),
        ...items.map(c => el('button', { class: 'item', type: 'button', draggable: 'true', title: c.summary,
          onclick: () => addNode(c.id),
          ondragstart: ev => { ev.dataTransfer.setData('text/b2s-component', c.id); ev.dataTransfer.effectAllowed = 'copy'; } },
        el('span', {}, c.title), badge(c)))));
    });
    if (!box.children.length) box.append(el('p', { class: 'hint' }, 'Nothing matches. Try a table name such as silver.orders.'));
  }

  /* ---------- canvas ---------- */
  function stageSize() {
    let w = 0, h = 0;
    design.nodes.forEach(n => { w = Math.max(w, n.x + NODE_W); h = Math.max(h, n.y + NODE_H); });
    return { w: w + 240, h: h + 140 };
  }
  function renderBands(h) {
    const cols = new Map();
    design.nodes.forEach(n => {
      const c = colOf(n.x), l = (comps.get(n.component) || {}).layer || 'ops';
      if (!cols.has(c)) cols.set(c, new Map());
      cols.get(c).set(l, (cols.get(c).get(l) || 0) + 1);
    });
    const box = $('bands');
    box.replaceChildren(...[...cols.entries()].sort((a, b) => a[0] - b[0]).map(([c, counts]) => {
      const ordered = [...counts.entries()].sort((a, b) => b[1] - a[1]);
      const main = layers.get(ordered[0][0]) || { color: '#57534e' };
      return el('div', { class: 'band', style: `left:${32 + c * COL - 12}px;width:${NODE_W + 24}px;height:${h - 16}px;background:${rgba(main.color, 0.08)};--c:${main.color}` },
        el('span', {}, ordered.map(([l]) => (layers.get(l) || { title: l }).title).join(' and ')));
    }));
  }
  function renderNodes(levels) {
    const box = $('nodes');
    box.replaceChildren(...design.nodes.map(n => {
      const c = comps.get(n.component) || { title: n.component, layer: 'ops', kind: 'python', capabilities: [] };
      const color = (layers.get(c.layer) || { color: '#57534e' }).color;
      const level = levels.get(n.id);
      const isSel = selected && selected.type === 'node' && selected.id === n.id;
      const node = el('div', { class: `node kind-${c.kind}${isSel ? ' sel' : ''}${level ? ' ' + level : ''}`, 'data-id': n.id,
        style: `left:${n.x}px;top:${n.y}px;--layer:${color}`, tabindex: '0', role: 'button', 'aria-pressed': String(!!isSel),
        'aria-label': `${n.task_key}: ${c.title}${level ? ` (${level})` : ''}` },
      el('div', { class: 't' }, c.title), el('div', { class: 'k' }, n.task_key),
      el('div', { class: 'meta' }, badge(c), n.for_each ? el('span', { class: 'badge' }, 'For each') : null,
        level ? el('span', { class: `dot ${level}`, title: level === 'error' ? 'Has errors' : 'Has warnings' }) : null),
      el('span', { class: 'port in', 'aria-hidden': 'true' }),
      el('span', { class: 'port out', 'data-port': 'out', title: 'Drag to the task that should run after this one' }));
      node.addEventListener('pointerdown', ev => startPointer(ev, n));
      node.addEventListener('keydown', ev => { if (ev.key === 'Enter' || ev.key === ' ') { ev.preventDefault(); pick({ type: 'node', id: n.id }); } });
      return node;
    }));
  }
  function curve(x1, y1, x2, y2) {
    const dx = Math.max(40, Math.abs(x2 - x1) / 2);
    return `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`;
  }
  function renderEdges(temp) {
    const { w, h } = stageSize(), svg = $('edges'), pos = new Map(design.nodes.map(n => [n.id, n]));
    svg.setAttribute('width', w); svg.setAttribute('height', h); svg.setAttribute('viewBox', `0 0 ${w} ${h}`);
    let out = '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" class="arrowhead"/></marker></defs>';
    design.edges.forEach((e, i) => {
      const a = pos.get(e.from), b = pos.get(e.to);
      if (!a || !b) return;
      const d = curve(a.x + NODE_W + 7, a.y + NODE_H / 2, b.x - 9, b.y + NODE_H / 2);
      const isSel = selected && selected.type === 'edge' && selected.index === i;
      out += `<g class="edge${isSel ? ' sel' : ''}${e.outcome ? ' ' + esc(e.outcome) : ''}" data-index="${i}"><path class="hit" d="${d}"/><path class="line" d="${d}" marker-end="url(#arrow)"/>`;
      if (e.outcome) out += `<text class="outcome" x="${(a.x + NODE_W + b.x) / 2 - 10}" y="${(a.y + b.y) / 2 + NODE_H / 2 - 8}">${esc(e.outcome)}</text>`;
      out += '</g>';
    });
    if (temp) { const a = pos.get(temp.from); out += `<path class="temp" d="${curve(a.x + NODE_W + 7, a.y + NODE_H / 2, temp.x, temp.y)}"/>`; }
    svg.innerHTML = out;
  }
  function renderCanvas(levels) {
    const { w, h } = stageSize();
    $('stage').style.width = w + 'px';
    $('stage').style.height = h + 'px';
    renderBands(h); renderEdges(); renderNodes(levels);
    $('empty').hidden = design.nodes.length > 0;
  }
  function startPointer(ev, n) {
    if (ev.button !== 0) return;
    ev.preventDefault();
    const r = $('stage').getBoundingClientRect();
    if (ev.target.closest('[data-port="out"]')) {
      drag = { mode: 'connect', from: n.id, x: ev.clientX - r.left, y: ev.clientY - r.top };
    } else {
      drag = { mode: 'move', id: n.id, dx: ev.clientX - r.left - n.x, dy: ev.clientY - r.top - n.y, moved: false, before: JSON.stringify(design) };
      if (!(selected && selected.id === n.id)) pick({ type: 'node', id: n.id });
    }
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp, { once: true });
  }
  function onMove(ev) {
    if (!drag) return;
    const r = $('stage').getBoundingClientRect(), x = ev.clientX - r.left, y = ev.clientY - r.top;
    if (drag.mode === 'connect') { drag.x = x; drag.y = y; renderEdges(drag); return; }
    const n = nodeById(drag.id);
    n.x = Math.max(0, Math.round((x - drag.dx) / GRID) * GRID);
    n.y = Math.max(40, Math.round((y - drag.dy) / GRID) * GRID);
    drag.moved = true;
    const node = document.querySelector(`.node[data-id="${drag.id}"]`);
    if (node) { node.style.left = n.x + 'px'; node.style.top = n.y + 'px'; }
    renderEdges();
  }
  function onUp(ev) {
    window.removeEventListener('pointermove', onMove);
    const d = drag;
    drag = null;
    if (!d) return;
    if (d.mode === 'move') {
      if (d.moved) { undoStack.push(d.before); redoStack.length = 0; save(); render(); }
      return;
    }
    renderEdges();
    const target = document.elementFromPoint(ev.clientX, ev.clientY);
    const node = target && target.closest('.node');
    if (node) connect(d.from, node.dataset.id);
  }

  /* ---------- inspector ---------- */
  function field(label, control, hint) {
    return el('label', { class: 'field' }, el('span', { class: 'label' }, label), control, hint ? el('span', { class: 'hint' }, hint) : null);
  }
  function input(value, onInput, attrs) {
    const i = el('input', Object.assign({ type: 'text', spellcheck: 'false', autocomplete: 'off' }, attrs || {}));
    i.value = value ?? '';
    i.addEventListener('focus', editStart);
    i.addEventListener('input', () => onInput(i.value));
    return i;
  }
  function area(value, onInput, rows) {
    const t = el('textarea', { rows: String(rows || 3), spellcheck: 'false' });
    t.value = value ?? '';
    t.addEventListener('focus', editStart);
    t.addEventListener('input', () => onInput(t.value));
    return t;
  }
  function choice(options, value, onChange, label) {
    const s = el('select', label ? { 'aria-label': label } : {}, ...options.map(([v, text]) => {
      const o = el('option', { value: v }, text);
      o.selected = v === value;
      return o;
    }));
    s.addEventListener('change', () => onChange(s.value));
    return s;
  }
  const num = v => Math.max(0, parseInt(v, 10) || 0);
  const nodeIn = (d, id) => d.nodes.find(n => n.id === id);

  function jobForm() {
    const j = design.job, trig = j.trigger || { type: 'manual' };
    const wrap = el('div', {}, el('h2', {}, 'Job settings'),
      el('p', { class: 'lead' }, 'Select a task to edit it. These settings apply to the whole job.'),
      field('Resource key', input(j.key, v => edit(d => { d.job.key = v; }), { class: 'code' }), 'Names the job in the bundle and its YAML file.'),
      field('Name', input(j.name, v => edit(d => { d.job.name = v; }))),
      field('Description', area(j.description, v => edit(d => { d.job.description = v; }))),
      field('Trigger', choice([['manual', 'Run manually'], ['schedule', 'On a schedule'], ['file_arrival', 'When files arrive'], ['continuous', 'Continuously']],
        trig.type, v => change(d => {
          d.job.trigger = v === 'schedule' ? { type: v, cron: '0 0 2 * * ?', timezone: 'UTC' }
            : v === 'file_arrival' ? { type: v, url: '/Volumes/${var.catalog}/${var.schema_prefix}landing/raw/', min_seconds: 300 } : { type: v };
        }))));
    if (trig.type === 'schedule') {
      wrap.append(field('Quartz cron expression', input(trig.cron, v => edit(d => { d.job.trigger.cron = v; }), { class: 'code' }), 'Seconds, minutes, hours, day of month, month, day of week.'),
        field('Time zone', input(trig.timezone || 'UTC', v => edit(d => { d.job.trigger.timezone = v; }))));
    }
    if (trig.type === 'file_arrival') {
      wrap.append(field('Volume path to watch', input(trig.url, v => edit(d => { d.job.trigger.url = v; }), { class: 'code' })),
        field('Minimum seconds between runs', input(String(trig.min_seconds || 60), v => edit(d => { d.job.trigger.min_seconds = num(v); }), { inputmode: 'numeric' })));
    }
    if (trig.type === 'continuous') wrap.append(el('p', { class: 'hint' }, 'A continuous job restarts as soon as it finishes. Processing-time streams need job cluster compute.'));
    wrap.append(
      field('Compute', choice([['serverless', 'Serverless'], ['job_cluster', 'Job cluster (classic compute)']], j.compute || 'serverless', v => change(d => { d.job.compute = v; }))),
      field('Maximum concurrent runs', input(String(j.max_concurrent_runs || 1), v => edit(d => { d.job.max_concurrent_runs = Math.max(1, num(v)); }), { inputmode: 'numeric' })),
      field('Timeout in seconds', input(String(j.timeout_seconds || 0), v => edit(d => { d.job.timeout_seconds = num(v) || undefined; }), { inputmode: 'numeric' }), '0 means no timeout.'),
      field('Email on failure', input(((j.notifications || {}).on_failure || []).join(', '), v => edit(d => {
        d.job.notifications = { on_failure: v.split(',').map(s => s.trim()).filter(Boolean) };
      })), 'Comma separated. ${var.alert_email} uses the bundle variable.'),
      paramsEditor());
    return wrap;
  }
  function paramsEditor() {
    const fs = el('fieldset', {}, el('legend', {}, 'Job parameters'),
      el('p', { class: 'hint' }, 'Tasks read them as {{job.parameters.name}}. Python tasks need catalog and schema_prefix.'));
    (design.job.parameters || []).forEach((p, i) => fs.append(el('div', { class: 'param-row' },
      input(p.name, v => edit(d => { d.job.parameters[i].name = v; }), { class: 'code', 'aria-label': 'Parameter name' }),
      input(p.default, v => edit(d => { d.job.parameters[i].default = v; }), { class: 'code', 'aria-label': `Default value of ${p.name}` }),
      el('button', { type: 'button', class: 'quiet', 'aria-label': `Remove ${p.name}`, onclick: () => change(d => { d.job.parameters.splice(i, 1); }) }, 'Remove'))));
    fs.append(el('button', { type: 'button', onclick: () => change(d => {
      d.job.parameters = d.job.parameters || [];
      d.job.parameters.push({ name: `param_${d.job.parameters.length + 1}`, default: '' });
    }) }, 'Add parameter'));
    return fs;
  }
  function conditionEditor(n) {
    const c = n.condition || {};
    return el('fieldset', {}, el('legend', {}, 'Condition'),
      field('Left value', input(c.left, v => edit(d => { nodeIn(d, n.id).condition.left = v; }), { class: 'code', list: 'refs' }), 'A task value such as {{tasks.validation_report.values.promote}} or a job parameter.'),
      field('Operator', choice(OPS, c.op || 'EQUAL_TO', v => change(d => { nodeIn(d, n.id).condition.op = v; }))),
      field('Right value', input(c.right, v => edit(d => { nodeIn(d, n.id).condition.right = v; }), { class: 'code' })),
      el('p', { class: 'hint' }, 'Connect the tasks that follow, then pick the true or false branch for each by selecting its arrow.'));
  }
  function depsEditor(n) {
    const fs = el('fieldset', { class: 'deps' }, el('legend', {}, 'Runs after'));
    const runsAfter = m => design.edges.some(e => e.from === m.id && e.to === n.id);
    const others = design.nodes.filter(m => m.id !== n.id).sort((a, b) => runsAfter(b) - runsAfter(a));   // current ones first
    if (!others.length) fs.append(el('p', { class: 'hint' }, 'Add more tasks to set dependencies.'));
    others.forEach(m => {
      const has = runsAfter(m);
      const blocked = !has && G.wouldCycle(design, m.id, n.id);
      const box = el('input', { type: 'checkbox' });
      box.checked = has; box.disabled = blocked;
      box.addEventListener('change', () => (box.checked ? connect(m.id, n.id)
        : change(d => { d.edges = d.edges.filter(e => !(e.from === m.id && e.to === n.id)); })));
      fs.append(el('label', { title: blocked ? `${m.task_key} already runs after ${n.task_key}` : null }, box, m.task_key));
    });
    return fs;
  }
  function forEachEditor(n) {
    const box = el('input', { type: 'checkbox' });
    box.checked = !!n.for_each;
    box.addEventListener('change', () => change(d => {
      const m = nodeIn(d, n.id);
      if (box.checked) m.for_each = { inputs: '["silver.orders", "gold.fct_sales"]', concurrency: 2 }; else delete m.for_each;
    }));
    const wrap = el('div', {}, el('label', { class: 'toggle' }, box, 'Run once for each item in a list'));
    if (n.for_each) {
      wrap.append(field('Items (JSON array)', area(n.for_each.inputs, v => edit(d => { nodeIn(d, n.id).for_each.inputs = v; }), 2), 'Each run gets the item as {{input}}.'),
        field('Run at most this many at once', input(String(n.for_each.concurrency || 1), v => edit(d => { nodeIn(d, n.id).for_each.concurrency = Math.max(1, num(v)); }), { inputmode: 'numeric' })));
    }
    return wrap;
  }
  function lineage(c) {
    const part = (title, refs) => (refs.length ? [el('h3', { class: 'sub' }, title),
      el('ul', { class: 'refs' }, ...refs.map(r => el('li', {}, r.endsWith('?') ? `${r.slice(0, -1)} (if present)` : r)))] : []);
    return el('div', {}, ...part('Reads', c.reads || []), ...part('Writes', c.writes || []));
  }
  function nodeForm(n) {
    const c = comps.get(n.component);
    if (!c) return el('div', {}, el('h2', {}, n.task_key), el('p', { class: 'lead' }, `Unknown component ${n.component}.`),
      el('button', { type: 'button', class: 'danger', onclick: () => removeNode(n.id) }, 'Remove task'));
    const layer = layers.get(c.layer);
    const wrap = el('div', {}, el('span', { class: 'chip', style: `--c:${layer.color}` }, layer.title), el('h2', {}, c.title), el('p', { class: 'lead' }, c.summary));
    if (c.capabilities.length) wrap.append(el('p', { class: 'caps' }, `Uses ${c.capabilities.join(', ')}.`));
    wrap.append(field('Task key', input(n.task_key, v => edit(d => { nodeIn(d, n.id).task_key = v; }), { class: 'code' }), 'Unique in the job: letters, digits, - and _.'));
    if (c.kind === 'condition') wrap.append(conditionEditor(n));
    (c.params || []).forEach(p => wrap.append(field(p.name, input((n.params || {})[p.name] ?? '', v => edit(d => {
      const m = nodeIn(d, n.id);
      if (v === '') delete m.params[p.name]; else m.params[p.name] = v;
    }), { class: 'code', list: 'refs', placeholder: p.default ? `default: ${p.default}` : '' }), p.description || null)));
    wrap.append(depsEditor(n), el('details', { class: 'more' }, el('summary', {}, 'Retries, timeout and when to run'),
      field('Retries', input(String(n.max_retries || 0), v => edit(d => { nodeIn(d, n.id).max_retries = num(v) || undefined; }), { inputmode: 'numeric' })),
      field('Timeout in seconds', input(String(n.timeout_seconds || 0), v => edit(d => { nodeIn(d, n.id).timeout_seconds = num(v) || undefined; }), { inputmode: 'numeric' })),
      field('Run when', choice(RUN_IF, n.run_if || '', v => change(d => { nodeIn(d, n.id).run_if = v || undefined; }))),
      c.kind === 'python' ? forEachEditor(n) : null),
    lineage(c), el('button', { type: 'button', class: 'danger', onclick: () => removeNode(n.id) }, 'Remove task'));
    return wrap;
  }
  function edgeForm(e, i) {
    const a = nodeById(e.from), b = nodeById(e.to), src = comps.get(a.component) || {};
    const wrap = el('div', {}, el('h2', {}, 'Dependency'), el('p', { class: 'lead' }, `${b.task_key} runs after ${a.task_key}.`));
    if (src.kind === 'condition') {
      wrap.append(field('Follow this arrow when the condition is', choice([['true', 'true'], ['false', 'false']], e.outcome || 'true', v => change(d => { d.edges[i].outcome = v; }))));
    }
    wrap.append(el('button', { type: 'button', class: 'danger', onclick: removeSelected }, 'Remove dependency'));
    return wrap;
  }
  function renderInspector() {
    const box = $('inspector');
    let form = null;
    if (selected && selected.type === 'node' && nodeById(selected.id)) form = nodeForm(nodeById(selected.id));
    else if (selected && selected.type === 'edge' && design.edges[selected.index]) form = edgeForm(design.edges[selected.index], selected.index);
    box.replaceChildren(form || jobForm());
  }

  /* ---------- checks, output, toolbar ---------- */
  function renderChecks(problems) {
    const errors = problems.filter(p => p.level === 'error').length, warnings = problems.length - errors;
    $('checks-title').textContent = !problems.length ? 'Checks: ready to deploy'
      : `Checks: ${errors} ${errors === 1 ? 'error' : 'errors'}, ${warnings} ${warnings === 1 ? 'warning' : 'warnings'}`;
    $('checks').replaceChildren(...(problems.length ? problems.map(p => el('li', { class: p.level },
      el('button', { type: 'button', onclick: () => { if (p.nodes[0]) pick({ type: 'node', id: p.nodes[0] }); } },
        el('span', { class: 'lvl' }, p.level === 'error' ? 'Error' : 'Warning'), ` ${p.message}`)))
      : [el('li', { class: 'ok' }, 'No problems found. The bundle YAML is ready to commit under resources/.')]));
  }
  function runOrder() {
    const { stages, cyclic } = G.stages(design), key = id => nodeById(id).task_key;
    const lines = stages.map((s, i) => `${i + 1}. ${s.map(key).join(', ')}${s.length > 1 ? `   (${s.length} run in parallel)` : ''}`);
    if (cyclic.length) lines.push(`Never runs, because of a loop: ${cyclic.map(key).join(', ')}`);
    return (lines.length ? lines.join('\n') : 'Add tasks to see the order they run in.') + '\n';
  }
  function outputText() {
    if (tab === 'order') return { file: 'Run order', text: runOrder() };
    if (tab === 'mermaid') return { file: `${design.job.key || 'pipeline'}.mmd`, text: B2S.bundle.toMermaid(design, CAT) };
    if (tab === 'json') return { file: `${design.job.key || 'design'}.json`, text: JSON.stringify(design, null, 2) + '\n' };
    return { file: `resources/${design.job.key || 'job'}.job.yml`, text: B2S.bundle.toYaml(design, CAT) };
  }
  function highlight(text) {
    const safe = esc(text);
    if (tab !== 'yaml') return safe;
    return safe.split('\n').map(line => (line.startsWith('#') ? `<span class="c">${line}</span>`
      : line.replace(/^(\s*(?:- )?)([A-Za-z_][\w.-]*)(:)/, '$1<span class="k">$2</span>$3').replace(/(&quot;.*?&quot;)/g, '<span class="s">$1</span>'))).join('\n');
  }
  function renderOutput() {
    document.querySelectorAll('.tab').forEach(b => b.setAttribute('aria-selected', String(b.dataset.tab === tab)));
    const { file, text } = outputText();
    $('file').textContent = file;
    $('code').innerHTML = highlight(text);
  }
  function renderRefs() {
    const options = (design.job.parameters || []).map(p => `{{job.parameters.${p.name}}}`).concat(['{{job.run_id}}', '{{input}}']);
    design.nodes.forEach(n => ((comps.get(n.component) || {}).writes || []).filter(w => w.startsWith('taskvalue:'))
      .forEach(w => options.push(`{{tasks.${n.task_key}.values.${w.slice(10)}}}`)));
    $('refs').replaceChildren(...options.map(o => el('option', { value: o })));
  }
  function renderBar() {
    const { stages } = G.stages(design);
    $('summary').textContent = `${design.job.name || design.job.key}: ${design.nodes.length} ${design.nodes.length === 1 ? 'task' : 'tasks'} in ${stages.length} ${stages.length === 1 ? 'stage' : 'stages'}`;
    $('undo').disabled = !undoStack.length;
    $('redo').disabled = !redoStack.length;
  }
  function worst(problems) {
    const levels = new Map();
    problems.forEach(p => p.nodes.forEach(id => { if (levels.get(id) !== 'error') levels.set(id, p.level); }));
    return levels;
  }
  function refresh() {
    const problems = G.validate(design, CAT);
    renderCanvas(worst(problems)); renderChecks(problems); renderOutput(); renderRefs(); renderBar();
  }
  function render() { refresh(); renderInspector(); }

  async function copy() {
    const { text } = outputText();
    try {
      await navigator.clipboard.writeText(text);
      toast('Copied.');
    } catch (e) {
      const ta = el('textarea', { 'aria-hidden': 'true', style: 'position:fixed;opacity:0' });
      ta.value = text; document.body.append(ta); ta.select();
      let ok = false;
      try { ok = document.execCommand('copy'); } catch (err) { ok = false; }
      ta.remove();
      toast(ok ? 'Copied.' : 'Copying is blocked here: select the text in the output and copy it.');
    }
  }
  function download() {
    const { file, text } = outputText();
    const a = el('a', { href: URL.createObjectURL(new Blob([text], { type: 'text/plain' })), download: file.split('/').pop().replace(/\s+/g, '-').toLowerCase() });
    document.body.append(a); a.click(); a.remove();
  }
  function importFile(ev) {
    const file = ev.target.files[0];
    ev.target.value = '';
    if (!file) return;
    file.text().then(text => {
      const d = JSON.parse(text);
      if (!d || !d.job || !Array.isArray(d.nodes) || !Array.isArray(d.edges)) throw new Error('it has no job, nodes and edges');
      const unknown = d.nodes.filter(n => !comps.has(n.component)).map(n => n.component);
      selected = null;
      commit(prepare(d));
      toast(unknown.length ? `Opened, but these components are unknown: ${unknown.join(', ')}.` : `Opened ${d.job.name || d.job.key}.`);
    }).catch(err => toast(`That file is not a pipeline design: ${err.message}.`));
  }
  function toggleTheme() {
    const root = document.documentElement;
    const current = root.dataset.theme || (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    root.dataset.theme = current === 'dark' ? 'light' : 'dark';
    try { localStorage.setItem(THEME, root.dataset.theme); } catch (e) { /* not persisted */ }
  }

  function init() {
    try { const t = localStorage.getItem(THEME); if (t) document.documentElement.dataset.theme = t; } catch (e) { /* default theme */ }
    $('template').replaceChildren(el('option', { value: '' }, 'Choose a template'),
      ...B2S.TEMPLATES.map((t, i) => el('option', { value: String(i) }, t.design.job.name)), el('option', { value: 'blank' }, 'A blank job'));
    $('template').addEventListener('change', ev => {
      const v = ev.target.value;
      ev.target.value = '';
      if (!v) return;
      selected = null;
      commit(v === 'blank' ? { version: 1, job: blankJob(), nodes: [], edges: [] } : prepare(B2S.TEMPLATES[+v].design));
      toast(v === 'blank' ? 'Started a blank job.' : `Opened ${design.job.name}.`);
    });
    renderPalette();
    $('search').addEventListener('input', renderPalette);
    $('autowire').addEventListener('click', () => {
      const { design: wired, added } = G.autoWire(design, CAT);
      if (!added) { toast('Every dependency the table lineage implies is already there.'); return; }
      commit(G.layout(wired));
      toast(`Added ${added} ${added === 1 ? 'dependency' : 'dependencies'} from table lineage.`);
    });
    $('tidy').addEventListener('click', () => commit(G.layout(design)));
    $('undo').addEventListener('click', undo);
    $('redo').addEventListener('click', redo);
    $('import').addEventListener('click', () => $('import-file').click());
    $('import-file').addEventListener('change', importFile);
    $('theme').addEventListener('click', toggleTheme);
    document.querySelectorAll('.tab').forEach(b => b.addEventListener('click', () => { tab = b.dataset.tab; renderOutput(); }));
    $('copy').addEventListener('click', copy);
    $('download').addEventListener('click', download);
    try { if (window.self !== window.top) $('download').hidden = true; } catch (e) { $('download').hidden = true; }
    const canvas = $('canvas');
    canvas.addEventListener('pointerdown', ev => { if (!ev.target.closest('.node, .edge') && selected) pick(null); });
    canvas.addEventListener('dragover', ev => {
      if ([...ev.dataTransfer.types].includes('text/b2s-component')) { ev.preventDefault(); ev.dataTransfer.dropEffect = 'copy'; }
    });
    canvas.addEventListener('drop', ev => {
      const id = ev.dataTransfer.getData('text/b2s-component');
      if (!id || !comps.has(id)) return;
      ev.preventDefault();
      const r = $('stage').getBoundingClientRect();
      addNode(id, { x: Math.max(0, Math.round((ev.clientX - r.left - NODE_W / 2) / GRID) * GRID),
        y: Math.max(40, Math.round((ev.clientY - r.top - 24) / GRID) * GRID) });
    });
    $('edges').addEventListener('click', ev => {
      const g = ev.target.closest('.edge');
      if (g) pick({ type: 'edge', index: +g.dataset.index });
    });
    document.addEventListener('keydown', ev => {
      const typing = /^(INPUT|TEXTAREA|SELECT)$/.test((document.activeElement || {}).tagName || '');
      const mod = ev.ctrlKey || ev.metaKey;
      if (typing) return;
      if (mod && ev.key.toLowerCase() === 'z') { ev.preventDefault(); if (ev.shiftKey) redo(); else undo(); }
      else if (mod && ev.key.toLowerCase() === 'y') { ev.preventDefault(); redo(); }
      else if ((ev.key === 'Delete' || ev.key === 'Backspace') && selected) { ev.preventDefault(); removeSelected(); }
      else if (ev.key === 'Escape' && selected) pick(null);
    });
    render();
  }
  init();
})();
