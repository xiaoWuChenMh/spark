#!/usr/bin/env python3
"""validate-graph.py — sanity check the assembled-graph.json"""
import json
import os

ROOT = r"F:\code\workpace\study\intellij idea\source\spark\core\src\main\scala\org\apache\spark\util"
path = os.path.join(ROOT, ".understand-anything", "intermediate", "assembled-graph.json")

with open(path, "r", encoding="utf-8") as fh:
    g = json.load(fh)

nodes = g["nodes"]
edges = g["edges"]
node_ids = {n["id"] for n in nodes}

# Orphan edges
orphans = [e for e in edges if e["source"] not in node_ids or e["target"] not in node_ids]

# Edge type distribution
from collections import Counter
edge_types = Counter(e["type"] for e in edges)
node_types = Counter(n["type"] for n in nodes)

# File nodes vs class nodes
files = [n for n in nodes if n["type"] == "file"]
classes = [n for n in nodes if n["type"] == "class"]

# Tag distribution
all_tags = Counter()
for n in nodes:
    for t in n.get("tags", []):
        all_tags[t] += 1

# Summary coverage
with_summary = sum(1 for n in nodes if n.get("summary"))
print(f"Total nodes: {len(nodes)}")
print(f"Total edges: {len(edges)}")
print(f"Orphan edges (missing source/target): {len(orphans)}")
print()
print("Node types:")
for t, c in node_types.most_common():
    print(f"  {t}: {c}")
print()
print("Edge types:")
for t, c in edge_types.most_common():
    print(f"  {t}: {c}")
print()
print(f"Files: {len(files)}")
print(f"Classes: {len(classes)}")
print()
print("Top tags:")
for t, c in all_tags.most_common(20):
    print(f"  {t}: {c}")
print()
print(f"Nodes with summary: {with_summary}/{len(nodes)}")
print()
# Some sample nodes
print("Sample file nodes:")
for n in files[:5]:
    print(f"  {n['id']}: {n['summary'][:80]}")
print()
print("Sample class nodes:")
for n in classes[:5]:
    print(f"  {n['id']}: {n['summary'][:80]}")
