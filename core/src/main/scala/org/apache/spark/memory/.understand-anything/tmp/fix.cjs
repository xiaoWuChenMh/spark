#!/usr/bin/env node
// Fix missing complexity on concept nodes and missing direction on all edges
const fs = require('fs');
const path = require('path');

const root = path.join('f:\\code\\workpace\\study\\intellij idea\\source\\spark\\core\\src\\main\\scala\\org\\apache\\spark\\memory', '.understand-anything');
const kgPath = path.join(root, 'knowledge-graph.json');
const dashPath = path.join(root, 'dashboard-dist', 'knowledge-graph.json');

const kg = JSON.parse(fs.readFileSync(kgPath, 'utf8'));

// 1) Add complexity to concept nodes (nodes 12, 13, 14 by original index)
let fixedComplexity = 0;
kg.nodes.forEach((n, i) => {
  if (n.type === 'concept' && !n.complexity) {
    n.complexity = 'moderate';
    fixedComplexity++;
  }
});

// 2) Add direction to all edges (default to "forward")
let fixedDirection = 0;
kg.edges.forEach((e, i) => {
  if (!e.direction) {
    e.direction = 'forward';
    fixedDirection++;
  }
});

const out = JSON.stringify(kg, null, 2);
fs.writeFileSync(kgPath, out);
fs.writeFileSync(dashPath, out);

console.log('Fixed complexity on', fixedComplexity, 'concept nodes');
console.log('Fixed direction on', fixedDirection, 'edges');
console.log('Total nodes:', kg.nodes.length, '| Total edges:', kg.edges.length);

// Quick re-validation
const issues = [];
const nodeIds = new Set(kg.nodes.map(n => n.id));
kg.edges.forEach((e, i) => {
  if (!nodeIds.has(e.source)) issues.push(`Edge[${i}] source missing`);
  if (!nodeIds.has(e.target)) issues.push(`Edge[${i}] target missing`);
  if (!e.direction) issues.push(`Edge[${i}] no direction`);
});
kg.nodes.forEach((n, i) => {
  if (n.type === 'concept' && !n.complexity) issues.push(`Node[${i}] concept missing complexity`);
});
console.log('Remaining issues:', issues.length, issues);
