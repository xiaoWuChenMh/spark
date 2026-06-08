#!/usr/bin/env node
// Assemble and validate the knowledge graph
const fs = require('fs');
const path = require('path');

const root = path.join('f:\\code\\workpace\\study\\intellij idea\\source\\spark\\core\\src\\main\\scala\\org\\apache\\spark\\memory', '.understand-anything');
const inter = path.join(root, 'intermediate');

const batch0 = JSON.parse(fs.readFileSync(path.join(inter, 'batch-0.json'), 'utf8'));
const scan = JSON.parse(fs.readFileSync(path.join(inter, 'scan-result.json'), 'utf8'));

const allNodes = batch0.nodes;
const allEdges = batch0.edges;

// ---- Build Layers ----
const layers = [
  {
    id: 'layer:abstractions',
    name: '抽象基类层 (Abstractions)',
    description: '为内存管理子系统定义抽象契约的基类（MemoryPool、MemoryManager）。这一层的类不直接实例化使用，定义子类的扩展点和同步模型。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala',
      'file:core/src/main/scala/org/apache/spark/memory/MemoryManager.scala',
      'class:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool',
      'class:core/src/main/scala/org/apache/spark/memory/MemoryManager.scala:MemoryManager'
    ]
  },
  {
    id: 'layer:pool-implementations',
    name: '内存池实现层 (Pool Implementations)',
    description: 'MemoryPool 的具体子类，分别实现执行内存（按任务公平共享）和存储内存（支持块驱逐）的簿记。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala',
      'file:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala',
      'class:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:ExecutionMemoryPool',
      'class:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala:StorageMemoryPool',
      'function:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:ExecutionMemoryPool.acquireMemory',
      'function:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:ExecutionMemoryPool.releaseMemory',
      'function:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:ExecutionMemoryPool.releaseAllMemoryForTask',
      'function:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala:StorageMemoryPool.acquireMemory',
      'function:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala:StorageMemoryPool.freeSpaceToShrinkPool'
    ]
  },
  {
    id: 'layer:unified-strategy',
    name: '统一内存策略层 (Unified Memory Strategy)',
    description: 'MemoryManager 的具体实现 UnifiedMemoryManager，实现软边界：执行和存储可互相借用空闲空间，必要时通过驱逐缓存块回收。伴生对象提供配置驱动的工厂方法。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala',
      'class:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager',
      'class:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager$',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager.acquireExecutionMemory',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager.acquireStorageMemory',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager$.getMaxMemory',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager$.apply'
    ]
  },
  {
    id: 'layer:package-documentation',
    name: '包文档层 (Package Documentation)',
    description: 'package.scala 中的包对象提供子系统的整体设计文档、组件关系图（MemoryManager / TaskMemoryManager / MemoryConsumer / MemoryPool）。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/package.scala'
    ]
  },
  {
    id: 'layer:concepts',
    name: '核心概念层 (Core Concepts)',
    description: '贯穿整个内存子系统的关键设计概念：统一内存软边界、任务公平共享、Tungsten 内存模式。',
    nodeIds: [
      'concept:unified-memory-soft-boundary',
      'concept:task-fair-sharing',
      'concept:tungsten-memory-mode'
    ]
  }
];

// ---- Build Tour ----
const tour = [
  {
    order: 1,
    title: '整体架构概览',
    description: '从 package.scala 开始，理解 Spark 内存子系统的整体设计：MemoryManager（JVM 级）、TaskMemoryManager（任务级）、MemoryConsumer、MemoryPool 四大组件的关系。',
    nodeIds: ['file:core/src/main/scala/org/apache/spark/memory/package.scala'],
    languageLesson: 'package object 是 Scala 中将包级别文档、类型别名、隐式转换集中到单一文件的惯用模式。'
  },
  {
    order: 2,
    title: '内存池抽象：MemoryPool',
    description: '阅读 MemoryPool 基类，掌握 _poolSize 字段、lock 同步对象、poolSize/memoryFree/incrementPoolSize/decrementPoolSize/memoryUsed(abstract) 这套通用簿记接口。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala',
      'class:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool'
    ]
  },
  {
    order: 3,
    title: '执行内存池：任务公平共享',
    description: '阅读 ExecutionMemoryPool。理解核心算法：保证每个任务至少能获得 1/(2N) 的内存，上限 1/N。通过 memoryForTask 映射跟踪每个 taskAttemptId 的占用，使用 lock.wait()/notifyAll() 协调。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala',
      'class:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:ExecutionMemoryPool',
      'function:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:ExecutionMemoryPool.acquireMemory',
      'concept:task-fair-sharing'
    ],
    languageLesson: '@GuardedBy 注解是 JCIP（Java Concurrency in Practice）的约定，告诉代码阅读者哪个锁保护该字段，但 Scala 编译器不会强制检查。'
  },
  {
    order: 4,
    title: '存储内存池：缓存与驱逐',
    description: '阅读 StorageMemoryPool。理解 _memoryUsed 跟踪、acquireMemory 通过 MemoryStore.evictBlocksToFreeSpace 在内存不足时驱逐块、freeSpaceToShrinkPool 为统一策略层提供回收能力。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala',
      'class:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala:StorageMemoryPool',
      'function:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala:StorageMemoryPool.acquireMemory',
      'function:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala:StorageMemoryPool.freeSpaceToShrinkPool'
    ]
  },
  {
    order: 5,
    title: 'JVM 级内存管理器：MemoryManager',
    description: '阅读 MemoryManager 抽象类。掌握它如何组合四个池（on/off-heap × execution/storage）、如何根据 spark.memory.offHeap.enabled 决定 Tungsten 模式（ON_HEAP / OFF_HEAP）、如何根据 numCores 和 maxTungstenMemory 计算默认页大小（注意 G1GC 下的 LONG_ARRAY_OFFSET 调整）。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/MemoryManager.scala',
      'class:core/src/main/scala/org/apache/spark/memory/MemoryManager.scala:MemoryManager',
      'function:core/src/main/scala/org/apache/spark/memory/MemoryManager.scala:MemoryManager.acquireStorageMemory',
      'function:core/src/main/scala/org/apache/spark/memory/MemoryManager.scala:MemoryManager.acquireExecutionMemory',
      'concept:tungsten-memory-mode'
    ]
  },
  {
    order: 6,
    title: '统一内存策略：UnifiedMemoryManager',
    description: '阅读 UnifiedMemoryManager 及其伴生对象。核心要点：maybeGrowExecutionPool 回调让执行池通过驱逐存储块扩张；acquireStorageMemory 让存储池从执行池借用空闲空间（但不会驱逐执行）；伴生对象的 getMaxMemory 校验系统内存下限（reservedMemory * 1.5）。',
    nodeIds: [
      'file:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala',
      'class:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager',
      'class:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager$',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager.acquireExecutionMemory',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager.acquireStorageMemory',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager$.getMaxMemory',
      'function:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager$.apply',
      'concept:unified-memory-soft-boundary'
    ],
    languageLesson: 'computeMaxExecutionPoolSize() 使用闭包形式 () => Long，可在每次 acquireMemory 内部重算以反映 storagePool.memoryUsed 的最新值。'
  }
];

// ---- Build the final KnowledgeGraph ----
const knowledgeGraph = {
  version: '1.0.0',
  project: {
    name: scan.projectName,
    languages: scan.languages,
    frameworks: scan.frameworks,
    description: scan.projectDescription,
    analyzedAt: new Date().toISOString(),
    gitCommitHash: '88f727c7e51933e741941a1c845070e47451aed5'
  },
  nodes: allNodes,
  edges: allEdges,
  layers,
  tour
};

// ---- Inline validation ----
const issues = [];
const warnings = [];
const nodeIds = new Set();
const seen = new Map();
allNodes.forEach((n, i) => {
  if (!n.id) { issues.push(`Node[${i}] missing id`); return; }
  if (!n.type) issues.push(`Node[${i}] '${n.id}' missing type`);
  if (!n.name) issues.push(`Node[${i}] '${n.id}' missing name`);
  if (!n.summary) issues.push(`Node[${i}] '${n.id}' missing summary`);
  if (!n.tags || !n.tags.length) issues.push(`Node[${i}] '${n.id}' missing tags`);
  if (seen.has(n.id)) issues.push(`Duplicate node ID '${n.id}'`);
  else seen.set(n.id, i);
  nodeIds.add(n.id);
});
const baseEdges = [
  // 修复：incrementPoolSize/decrementPoolSize 是 MemoryPool 继承方法，重定向到基类
  { id: 26, target: 'function:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool.decrementPoolSize' },
  { id: 27, target: 'function:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool.incrementPoolSize' },
  { id: 28, target: 'function:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool.decrementPoolSize' },
  { id: 29, target: 'function:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool.incrementPoolSize' }
];
allEdges.forEach(e => {
  const fix = baseEdges.find(b => b.id === allEdges.indexOf(e));
  if (fix) e.target = fix.target;
});

const allEdgesFinal = allEdges.concat([
  // 补充：MemoryPool 三个方法归属于 MemoryPool 类
  { source: 'class:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool', target: 'function:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool.memoryFree', type: 'contains', weight: 1.0, label: '方法' },
  { source: 'class:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool', target: 'function:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool.incrementPoolSize', type: 'contains', weight: 1.0, label: '方法' },
  { source: 'class:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool', target: 'function:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool.decrementPoolSize', type: 'contains', weight: 1.0, label: '方法' },
  // 补充：package.scala 文档关联所有核心组件
  { source: 'file:core/src/main/scala/org/apache/spark/memory/package.scala', target: 'class:core/src/main/scala/org/apache/spark/memory/MemoryManager.scala:MemoryManager', type: 'documents', weight: 0.5, label: '子系统文档' },
  { source: 'file:core/src/main/scala/org/apache/spark/memory/package.scala', target: 'class:core/src/main/scala/org/apache/spark/memory/MemoryPool.scala:MemoryPool', type: 'documents', weight: 0.5, label: '子系统文档' },
  { source: 'file:core/src/main/scala/org/apache/spark/memory/package.scala', target: 'class:core/src/main/scala/org/apache/spark/memory/ExecutionMemoryPool.scala:ExecutionMemoryPool', type: 'documents', weight: 0.5, label: '子系统文档' },
  { source: 'file:core/src/main/scala/org/apache/spark/memory/package.scala', target: 'class:core/src/main/scala/org/apache/spark/memory/StorageMemoryPool.scala:StorageMemoryPool', type: 'documents', weight: 0.5, label: '子系统文档' },
  { source: 'file:core/src/main/scala/org/apache/spark/memory/package.scala', target: 'class:core/src/main/scala/org/apache/spark/memory/UnifiedMemoryManager.scala:UnifiedMemoryManager', type: 'documents', weight: 0.5, label: '子系统文档' }
]);

allEdgesFinal.forEach((e, i) => {
  if (!nodeIds.has(e.source)) issues.push(`Edge[${i}] source '${e.source}' not found`);
  if (!nodeIds.has(e.target)) issues.push(`Edge[${i}] target '${e.target}' not found`);
});
const fileLevelTypes = new Set(['file', 'config', 'document', 'service', 'pipeline', 'table', 'schema', 'resource', 'endpoint']);
const fileNodes = allNodes.filter(n => fileLevelTypes.has(n.type)).map(n => n.id);
const assigned = new Map();
layers.forEach(layer => {
  (layer.nodeIds || []).forEach(id => {
    if (!nodeIds.has(id)) issues.push(`Layer '${layer.id}' refs missing node '${id}'`);
    if (assigned.has(id)) issues.push(`Node '${id}' appears in multiple layers`);
    assigned.set(id, layer.id);
  });
});
fileNodes.forEach(id => {
  if (!assigned.has(id)) warnings.push(`File node '${id}' not in any layer`);
});
tour.forEach((step, i) => {
  (step.nodeIds || []).forEach(id => {
    if (!nodeIds.has(id)) issues.push(`Tour step[${i}] refs missing node '${id}'`);
  });
});
const withEdges = new Set([
  ...allEdgesFinal.map(e => e.source),
  ...allEdgesFinal.map(e => e.target)
]);
allNodes.forEach(n => {
  if (!withEdges.has(n.id)) warnings.push(`Node '${n.id}' has no edges (orphan)`);
});

const stats = {
  totalNodes: allNodes.length,
  totalEdges: allEdgesFinal.length,
  totalLayers: layers.length,
  tourSteps: tour.length,
  nodeTypes: allNodes.reduce((a, n) => { a[n.type] = (a[n.type]||0)+1; return a; }, {}),
  edgeTypes: allEdgesFinal.reduce((a, e) => { a[e.type] = (a[e.type]||0)+1; return a; }, {})
};

fs.writeFileSync(path.join(root, 'knowledge-graph.json'), JSON.stringify({ ...knowledgeGraph, edges: allEdgesFinal }, null, 2));
fs.writeFileSync(path.join(inter, 'review.json'), JSON.stringify({ issues, warnings, stats }, null, 2));

console.log('Stats:', JSON.stringify(stats, null, 2));
console.log('Issues:', issues.length, 'Warnings:', warnings.length);
if (issues.length) {
  console.log('Issue details:', issues);
}
if (warnings.length) {
  console.log('Warning details:', warnings);
}
process.exit(issues.length > 0 ? 1 : 0);
