#!/usr/bin/env python3
"""merge-batch-graphs.py — merge all batch-N.json into a single assembled graph."""
import json
import os
import sys

ROOT = r"F:\code\workpace\study\intellij idea\source\spark\core\src\main\scala\org\apache\spark\util"
INTER = os.path.join(ROOT, ".understand-anything", "intermediate")

nodes_by_id = {}
edges = []
edge_set = set()
errors = []

# Find all batch files
batch_files = sorted(
    [f for f in os.listdir(INTER) if f.startswith("batch-") and f.endswith(".json")],
    key=lambda s: int(s[6:-5]),
)

for bf in batch_files:
    path = os.path.join(INTER, bf)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as e:
        errors.append(f"{bf}: {e}")
        continue

    for n in data.get("nodes", []):
        if n["id"] in nodes_by_id:
            # Merge tags
            existing = nodes_by_id[n["id"]]
            existing.setdefault("tags", [])
            for t in n.get("tags", []):
                if t not in existing["tags"]:
                    existing["tags"].append(t)
            continue
        nodes_by_id[n["id"]] = dict(n)

    for e in data.get("edges", []):
        key = (e["source"], e["target"], e["type"])
        if key in edge_set:
            continue
        edge_set.add(key)
        edges.append(e)

# Build full graph
graph = {
    "metadata": {
        "projectName": "org.apache.spark.util",
        "projectRoot": ROOT,
        "language": "zh",
        "generatedAt": "2026-06-08",
        "version": "1.0",
        "schemaVersion": "1.0",
        "nodeCount": len(nodes_by_id),
        "edgeCount": len(edges),
        "errors": errors,
    },
    "nodes": list(nodes_by_id.values()),
    "edges": edges,
}

out_path = os.path.join(INTER, "assembled-graph.json")
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(graph, fh, ensure_ascii=False, indent=2)

print(f"Merged {len(batch_files)} batch files")
print(f"Nodes: {len(nodes_by_id)}")
print(f"Edges: {len(edges)}")
print(f"Output: {out_path}")
if errors:
    print("Errors:")
    for e in errors:
        print("  -", e)
