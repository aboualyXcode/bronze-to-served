/* yaml.js: a small, deterministic YAML writer for bundle files (block style, quoting only when needed). */
(function (root) {
  'use strict';
  const B2S = root.B2S = root.B2S || {};
  const SPECIAL = /[:#{}\[\],&*!|>'"%@`]/;
  const RESERVED = /^(true|false|yes|no|null|on|off|~)$/i;
  const NUMBER = /^[-+]?(\d[\d_]*)(\.\d*)?([eE][-+]?\d+)?$/;

  function scalar(v) {
    if (v === null || v === undefined) return 'null';
    if (typeof v === 'boolean' || typeof v === 'number') return String(v);
    const s = String(v);
    if (s === '' || /^\s|\s$/.test(s) || SPECIAL.test(s) || /^[-?]/.test(s) || RESERVED.test(s) || NUMBER.test(s) || /\n/.test(s)) {
      return JSON.stringify(s);
    }
    return s;
  }

  function dump(value, indent) {
    indent = indent || 0;
    const pad = ' '.repeat(indent);
    if (Array.isArray(value)) {
      return value.map(item => {
        if (item !== null && typeof item === 'object' && !Array.isArray(item)) {
          return pad + '- ' + dump(item, indent + 2).slice(indent + 2);
        }
        return pad + '- ' + scalar(item) + '\n';
      }).join('');
    }
    if (value !== null && typeof value === 'object') {
      return Object.keys(value).filter(k => value[k] !== undefined).map(k => {
        const v = value[k];
        const key = scalar(k);
        if (Array.isArray(v)) return v.length ? `${pad}${key}:\n${dump(v, indent + 2)}` : `${pad}${key}: []\n`;
        if (v !== null && typeof v === 'object') return Object.keys(v).length ? `${pad}${key}:\n${dump(v, indent + 2)}` : `${pad}${key}: {}\n`;
        return `${pad}${key}: ${scalar(v)}\n`;
      }).join('');
    }
    return pad + scalar(value) + '\n';
  }

  B2S.yaml = { scalar, dump };
})(typeof window !== 'undefined' ? window : globalThis);
