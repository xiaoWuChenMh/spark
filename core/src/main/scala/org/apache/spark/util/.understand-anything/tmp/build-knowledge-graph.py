#!/usr/bin/env python3
"""build-knowledge-graph.py — assemble the final knowledge-graph.json."""
import json
import os
from datetime import datetime, timezone

ROOT = r"F:\code\workpace\study\intellij idea\source\spark\core\src\main\scala\org\apache\spark\util"
INTER = os.path.join(ROOT, ".understand-anything", "intermediate")
OUT = os.path.join(ROOT, ".understand-anything", "knowledge-graph.json")

with open(os.path.join(INTER, "assembled-graph.json"), "r", encoding="utf-8") as fh:
    asm = json.load(fh)
with open(os.path.join(INTER, "layers.json"), "r", encoding="utf-8") as fh:
    layers = json.load(fh)
with open(os.path.join(INTER, "tour.json"), "r", encoding="utf-8") as fh:
    tour = json.load(fh)

# Normalize: every layer needs id, name, description, nodeIds
node_ids = {n["id"] for n in asm["nodes"]}
for L in layers:
    L["nodeIds"] = [nid for nid in L.get("nodeIds", []) if nid in node_ids]
for s in tour:
    s["nodeIds"] = [nid for nid in s.get("nodeIds", []) if nid in node_ids]

# Try to get git commit hash, fallback to unknown
git_hash = "unknown"
try:
    import subprocess
    r = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=os.path.join(ROOT, "..", "..", "..", "..", "..", ".."),
        capture_output=True, text=True, timeout=10,
    )
    if r.returncode == 0:
        git_hash = r.stdout.strip()
except Exception:
    pass

graph = {
    "version": "1.0.0",
    "project": {
        "name": "org.apache.spark.util",
        "languages": ["scala", "java", "markdown", "json"],
        "frameworks": ["spark"],
        "description": "Apache Spark 内核的 util 工具包：通用工具方法、并发原语、事件总线、可序列化包装、IO/日志/采样/Shuffle 数据结构，是 Spark 引擎各层都会依赖的基础设施。",
        "analyzedAt": datetime.now(timezone.utc).isoformat(),
        "gitCommitHash": git_hash,
    },
    "nodes": asm["nodes"],
    "edges": asm["edges"],
    "layers": layers,
    "tour": tour,
}

with open(OUT, "w", encoding="utf-8") as fh:
    json.dump(graph, fh, ensure_ascii=False, indent=2)

print(f"Knowledge graph written to {OUT}")
print(f"  Nodes: {len(graph['nodes'])}")
print(f"  Edges: {len(graph['edges'])}")
print(f"  Layers: {len(graph['layers'])}")
print(f"  Tour steps: {len(graph['tour'])}")
