# Understand Anything Dashboard 故障排除指南

> 基于 2026-06-08 实际排查经验的总结文档。适用于 Windows 环境 + Spark 项目 + understand-dashboard 技能。

---

## 一、环境信息（关键前提）

| 项目 | 值 |
|------|-----|
| 操作系统 | Windows (win32) / PowerShell |
| Shell | `C:\WINDOWS\System32\WindowsPowerShell\v1` |
| 包管理器 | pnpm 8.15.9, npm |
| Node.js | v20.20.2 |
| 插件路径 A | `C:\Users\Zz\.understand-attention-plugin\` (**不完整，无 node_modules**) |
| 插件路径 B | `C:\Users\Zz\.understand-anything\repo\understand-anything-plugin\` (**正确路径，含完整依赖**) |
| Dashboard 路径 | `{插件根}\packages\dashboard\` |
| Core 包路径 | `{插件根}\packages\core\` |
| 知识图谱 | `项目根\.understand-attention\knowledge-graph.json` |

### ⚠️ 关键发现：双副本问题

系统中存在 **两份** understand-anything-plugin 副本：

```
~/.understand-attention-plugin/          ← 不完整（缺少 node_modules 和 dist/）
~/.understand-anything/repo/             ← 完整（pnpm install 的实际位置）
    └── understand-attention-plugin/     ← ✅ 正确的工作目录
```

**Vite 启动时通过 node_modules 解析的路径指向 `repo/` 副本**，因此必须从该目录启动服务。

---

## 二、启动命令（最终可用版本）

```powershell
# 方式一：直接命令（需在 PowerShell 中执行）
$env:GRAPH_DIR = "f:\code\workpace\study\intellij idea\source\spark"
cd "C:\Users\Zz\.understand-anything\repo\understand-attention-plugin\packages\dashboard"
npx vite --host 127.0.0.1

# 方式二：Node.js spawn（推荐，确保环境变量正确传递）
node -e "const {spawn} = require('child_process'); const p = spawn('npx', ['vite', '--host', '127.0.0.1'], {cwd: 'C:\\\\Users\\\\Zz\\\\.understand-anything\\\\repo\\\\understand-attention-plugin\\\\packages\\\\dashboard', shell: true, env: {...process.env, GRAPH_DIR: '你的项目绝对路径'}}); p.stdout.on('data', d => process.stdout.write(d.toString())); p.stderr.on('data', d => process.stderr.write(d.toString()));"
```

**输出示例**：
```
Port 5173 is in use, trying another one...
...
  🔑  Dashboard URL: http://127.0.0.1:5177/?token=xxxxxxxxxxxxxxxx
  VITE v4.5.9 ready in 286 ms
  ➜  Local:   http://127.0.0.1:5177/
```

---

## 三、故障树与解决方案

### 问题 1：浏览器显示"无法访问此页面" / ERR_CONNECTION_REFUSED

**现象**：Vite 服务未运行或启动后立即崩溃退出。

**排查步骤**：

```bash
# 1. 检查端口是否在监听
netstat -ano | findstr "5177"

# 2. 如果没有监听，说明 Vite 进程已退出，需要查看崩溃原因
```

**可能原因及修复**：

| 子原因 | 症状 | 修复方法 |
|--------|------|----------|
| **1A. Core 包未构建** | 终端报错 `Cannot find module '@understand-attention/core/schema'` 或类似 alias 解析失败 | 见 [问题 2](#问题-2core-包未构建) |
| **1B. 从错误的插件目录启动** | 报错 `Failed to load url /src/main.tsx ... Does the file exist?` | 使用正确的 `repo/` 目录启动 |
| **1C. GRAPH_DIR 未传递** | API 返回 404 `"No knowledge graph found"` 或 `"Missing or invalid project metadata"` | 用 Node.js spawn 注入环境变量（见上方启动命令） |

---

### 问题 2：Core 包未构建

**现象**：
- Vite 启动时报错：模块找不到（`@understand-attention/core/schema`, `search`, `types`）
- 文件 `packages/core/dist/schema.js` 不存在

**验证方法**：
```bash
node -e "const fs=require('fs'); console.log(fs.existsSync('C:/Users/Zz/.understand-attention/repo/understand-attention-plugin/packages/core/dist/schema.js') ? 'EXISTS' : 'MISSING');"
```

**修复步骤**：

```bash
# 1. 安装 workspace 依赖
cd "C:\Users\Zz\.understand-anything\repo\understand-attention-plugin"
pnpm install

# 2. 构建 core 包
cd packages\core
npm run build   # 或 npx tsc

# 3. 验证构建产物
node -e "const fs=require('fs'); console.log(fs.existsSync('dist/schema.js') ? 'OK' : 'FAIL');"
```

**注意**：如果 `npm run build` 报错 "tsc command you are looking for"，说明 core 包缺少 TypeScript devDependency，需要先执行 `npm install`。

---

### 问题 3：Vite 解析到错误的文件路径

**现象**：
```
Failed to load url /src/main.tsx (resolved id: C:/Users/Zz/.understand-attention/repo/.../src/main.tsx). Does the file exist?
TypeError: Cannot read properties of undefined (reading 'imports')
```

**根因**：系统中有两份插件副本，Vite 的 `node_modules/.pnpm/` 内部符号链接指向 `repo/` 路径。从 `.understand-attention-plugin/` 启动时，Vite 能找到依赖包但解析源码路径时会跳转到 `repo/` 副本。

**修复**：始终从以下目录启动 Vite：
```
C:\Users\Zz\.understand-anything\repo\understand-attention-plugin\packages\dashboard
```

---

### 问题 4：API 返回 404 — 知识图谱文件未找到

**现象**：Dashboard 显示 `"Missing or invalid project metadata"`，API `/knowledge-graph.json?token=xxx` 返回 404 或空响应。

**根因**：`GRAPH_DIR` 环境变量未正确传递给 Vite 进程。

**验证方法**：
```bash
# 直接测试 API
node -e "const http=require('http'); http.get('http://127.0.0.1:5178/knowledge-graph.json?token=TOKEN_HERE', r => { let d=''; r.on('data',c=>d+=c); r.on('end',()=>console.log('Status:', r.statusCode)); }).on('error', e=>console.error(e.message));"
```

**为什么 `set GRAPH_DIR=... && npx vite...` 在 Windows 中不生效？**

Windows CMD 的 `set` 变量作用域限制 + `&&` 链式执行的变量继承问题，导致子进程（npx → node → vite）可能无法读取 `GRAPH_DIR`。

**修复**：使用 Node.js `spawn({ env: {...process.env, GRAPH_DIR: '...' } })` 显式注入环境变量（见[启动命令](#二启动命令最终可用版本)）。

---

## 四、Schema 兼容性修正（knowledge-graph.json）

Dashboard 对 knowledge-graph.json 有严格的 schema 校验。以下是常见的不合规字段及其修正方式：

### 4.1 nodes[].complexity 可选值

| ❌ 无效值 | ✅ 有效值 | 适用场景 |
|-----------|-----------|----------|
| `"trivial"` | `"simple"` | 极简工具类、异常类、数据容器 |
| *(空缺)* | `"moderate"` | 中等复杂度的组件 |
| *(空缺)* | `"complex"` | 核心架构组件 |

**批量替换脚本**（Node.js）：
```javascript
const fs = require('fs');
const path = '项目根/.understand-attention/knowledge-graph.json';
const data = JSON.parse(fs.readFileSync(path, 'utf8'));
data.nodes.forEach(n => {
  if (n.complexity === 'trivial') n.complexity = 'simple';
});
fs.writeFileSync(path, JSON.stringify(data, null, 2));
console.log('Fixed complexity values');
```

### 4.2 edges[].type 可选值（完整白名单）

```
imports | exports | contains | inherits | implements | calls | subscribes | publishes
middleware | reads_from | writes_to | transforms | validates | depends_on | tested_by
configures | related | similar_to | deploys | serves | provisions | triggers | migrates
documents | routes | defines_schema | contains_flow | flow_step | cross_domain | cites
contradicts | builds_on | exemplifies | categorized_under | authored_by
```

**常见无效值映射**：

| ❌ 原始值 | → ✅ 替换为 | 语义说明 |
|-----------|------------|----------|
| `"delegates"` | `"calls"` | 委托调用关系 |
| `"bridges"` | `"depends_on"` | 桥接适配依赖 |
| `"references"` | `"reads_from"` | 引用读取关系 |
| `"processes"` | `"transforms"` | 处理转换消息 |
| `"uses"` | `"depends_on"` | 一般使用依赖 |

### 4.3 edges[].direction 字段（必填）

每条 edge **必须包含** `direction` 字段，可选值：`"forward" | "backward" | "bidirectional"`

缺失时 Dashboard 会自动默认为 `"forward"` 并显示 Auto-corrected 警告。

**批量补充**：
```javascript
data.edges.forEach(e => {
  if (!e.direction) e.direction = 'forward';
});
```

---

## 五、完整启动检查清单（Checklist）

首次启动或遇到问题时，按顺序执行以下检查：

```
□ 1. Core 包是否已构建？
     → 检查: {plugin-root}/packages/core/dist/schema.js 是否存在
     → 若否: cd packages/core && npm install && npm run build

□ 2. 依赖是否安装？
     → 检查: {plugin-root}/node_modules 是否存在且非空
     → 若否: cd {plugin-root} && pnpm install

□ 3. 知识图谱文件是否存在？
     → 检查: {project}/.understand-attention/knowledge-graph.json 是否存在
     → 若否: 先运行 /understand 分析项目

□ 4. Schema 是否合规？
     → 检查: 无 "trivial" complexity, 所有 edges 有 direction, type 在白名单内
     → 参考 [四、Schema 兼容性修正](#四schema-兼容性修正knowledge-graphjson)

□ 5. 从正确目录启动？
     → 必须使用: ~/.understand-attention/repo/understand-attention-plugin/packages/dashboard

□ 6. 环境变量是否传递？
     → GRAPH_DIR 必须指向项目根目录（绝对路径）
     → 推荐用 Node.js spawn 注入

□ 7. 端口是否被占用？
     → 默认 5173，自动递增到 5174/5175/...
     → netstat -ano | findstr "端口号" 检查

□ 8. Token 是否正确？
     → URL 格式: http://127.0.0.1:{port}/?token={终端输出的token}
     → 缺少 token 会显示 403 Forbidden
```

---

## 六、常见错误信息速查表

| 错误信息 | 含义 | 解决方案 |
|----------|------|----------|
| `ERR_CONNECTION_REFUSED` | Vite 未在运行 | 重新启动（按启动命令） |
| `403 Forbidden: missing or invalid token` | URL 缺少 token 参数 | 从终端复制带 token 的完整 URL |
| `404 No knowledge graph found` | 找不到知识图谱文件 | 设置 `GRAPH_DIR` 环境变量 |
| `Missing or invalid project metadata` | meta.json 缺失或格式错误 | 确保 meta.json 存在且有效 |
| `Failed to load /src/main.tsx` | 从错误的插件目录启动 | 切换到 `repo/` 路径 |
| `Cannot read properties of undefined (reading 'imports')` | Vite 初始化阶段崩溃 | 同上（路径问题导致） |
| `Auto-corrected (N): missing "direction"` | edges 缺少 direction 字段 | 补充 `"direction": "forward"` |
| `Dropped (N): Invalid option: complexity` | 节点使用 `"trivial"` | 改为 `"simple"` |
| `Dropped (N): Invalid option: type` | 边使用不在白名单的 type | 映射为合法类型 |

---

## 七、端口占用处理

如果需要完全重启服务（释放所有端口）：

```powershell
# 查找并终止所有 vite 相关进程
tasklist | findstr "node"
# 记录 PID 后:
taskkill /PID <pid> /F

# 或者一次性杀掉指定端口
for /f "tokens=5" %a in ('netstat -ano ^| findstr ":5173 :5174 :5175 :5176 :5177 :5178"') do taskkill /PID %a /F
```

---

*最后更新: 2026-06-08*
*适用环境: Windows 11 + Node.js 20 + pnpm 8 + understand-anything-plugin*
