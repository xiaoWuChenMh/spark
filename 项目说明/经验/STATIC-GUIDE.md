# Dashboard 静态化指南

> 基于 2026-06-08 实际验证的流程。适用于 Windows 环境 + understand-dashboard 技能 + 已生成的 knowledge-graph.json。

---

## 一、为什么需要静态化

| 问题 | Vite 开发模式 | 静态文件模式 |
|------|--------------|-------------|
| 启动复杂度 | 需要 Node.js spawn + GRAPH_DIR 环境变量 | 双击 .bat 或一条命令 |
| Token 门禁 | 每次随机生成，必须带 `?token=xxx` | DEMO_MODE 跳过，无需输入 |
| 端口冲突 | 5173-5179 常被占用 | 自定义端口（如 9000） |
| 依赖 | Vite、pnpm、core 包构建 | 仅需 Python（或 Node.js serve） |
| 出错风险 | 路径解析、环境变量传递等 | 几乎为零 |

---

## 二、前提

本指南假设你已经通过 `/understand` 技能生成了知识图谱，且 `knowledge-graph.json` 的 Schema 已经合规（complexity/edge type/weight/direction 等字段均已就绪）。如果还没到这一步，请先完成图谱生成和 Schema 修复。

**目录结构说明**：`.understand-anything/` 不一定在项目根目录。在你的项目中，每个代码组件可以有自己的 `.understand-anything/` 目录。下文用 `{UA_DIR}` 代指这个目录的实际路径，执行时替换为真实路径即可。

```
{UA_DIR}/                          ← 组件对应的 .understand-anything 目录
├── knowledge-graph.json           ← 必须存在（/understand 生成的产物）
├── meta.json                      ← 必须存在
└── config.json                    ← 必须存在
```

---

## 三、静态化步骤

### Step 1: 用 DEMO_MODE 构建 Dashboard

```powershell
# 进入 Dashboard 源码目录
Set-Location "$env:USERPROFILE\.understand-anything\repo\understand-anything-plugin\packages\dashboard"

# 启用 DEMO_MODE（跳过 Token 门禁）
$env:VITE_DEMO_MODE = "true"

# 构建（--base ./ 确保相对路径正确）
npx vite build --base ./
```

构建产物输出在源码目录下的 `dist/` 文件夹中。

### Step 2: 复制到目标目录

将 Dashboard 构建产物 + 知识图谱数据复制到 `{UA_DIR}/dashboard-dist/`：

```powershell
$src = "$env:USERPROFILE\.understand-anything\repo\understand-anything-plugin\packages\dashboard\dist"
$dst = "{UA_DIR}\dashboard-dist"    # ← 替换为实际路径

New-Item -ItemType Directory -Path $dst -Force | Out-Null

# 复制 Dashboard 构建产物（index.html + assets/ 等）
Copy-Item -Path "$src\*" -Destination $dst -Recurse -Force

# 复制知识图谱数据（3 个必需文件）
Copy-Item -Path "{UA_DIR}\knowledge-graph.json" -Destination "$dst\knowledge-graph.json" -Force
Copy-Item -Path "{UA_DIR}\meta.json"           -Destination "$dst\meta.json"           -Force
Copy-Item -Path "{UA_DIR}\config.json"         -Destination "$dst\config.json"         -Force
```

### Step 3: 创建启动脚本（可选）

在 `dashboard-dist/` 目录下放一个启动脚本，方便以后双击即可查看。脚本只需做一件事：**以该目录为根启动一个 HTTP 静态文件服务器**。

具体要求：
- 监听 `127.0.0.1`，端口自定（如 9000）
- 优先用 Python `http.server`，没有则 fallback 到 `npx serve`
- 可以同时提供 `.bat` 和 `.ps1` 两个版本

让 AI 根据以上描述生成即可，无需在此贴完整代码。

### Step 4: 启动并验证

```powershell
cd "{UA_DIR}\dashboard-dist"
python -m http.server 9000 --bind 127.0.0.1
```

浏览器打开 **http://127.0.0.1:9000**

**成功标志**：
- 不显示 "Access Token Required" 页面，直接进入图谱视图
- 终端日志中 `knowledge-graph.json`、`meta.json`、`config.json` 均返回 **200**
- `diff-overlay.json` / `domain-graph.json` 返回 404 是正常的（可选数据）

---

## 四、最终目录结构

```
{UA_DIR}/                           ← 组件的 .understand-anything 目录
├── knowledge-graph.json            ← 原始知识图谱
├── meta.json                       ← 项目元信息
├── config.json                     ← 配置
├── dashboard-dist/                 ← ★ 静态化输出目录
│   ├── index.html                  ← 入口页面
│   ├── knowledge-graph.json        ← 副本
│   ├── meta.json                   ← 副本
│   ├── config.json                 ← 副本
│   ├── assets/                     ← JS/CSS 资源（哈希文件名）
│   │   ├── index-xxxxxx.js
│   │   ├── index-xxxxxx.css
│   │   └── ...
│   ├── favicon.ico / favicon.svg
│   └── start-dashboard.xxx         ← 启动脚本（可选）
└── ...
```

---

## 五、故障排查

| 问题 | 原因 | 解决 |
|------|------|------|
| 显示 "Access Token Required" | 未用 DEMO_MODE 构建 | 重新执行 Step 1（确保 `$env:VITE_DEMO_MODE="true"`） |
| 页面空白 / JS 报错 | `assets/` 资源未复制完整 | 确保 `dist/assets/*` 全部复制到了目标目录 |
| knowledge-graph.json 404 | 文件未复制或路径错误 | 检查 `dashboard-dist/knowledge-graph.json` 是否存在 |
| meta.json / config.json 404 | 同上，这两个也是必需的 | 从 `{UA_DIR}/` 复制过去 |
| 端口被占用 | 上次服务未关闭 | 换端口或关闭占用进程 |
| `@vite/client` 404 | 正常现象 | 静态模式下不需要，忽略即可 |


---

*创建时间: 2026-06-08*
*适用环境: Windows 11 + Node.js 20 + Python 3 + understand-anything-plugin*
*依赖: Vite 4.5.9, pnpm 8*
