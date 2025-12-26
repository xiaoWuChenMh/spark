# StagesResource 类分析文档

## 类的概述和定义

`StagesResource` 是 Spark REST API v1 版本中的核心资源类，专门用于管理 Spark 应用程序的阶段（Stage）和任务（Task）信息。该类继承自 `BaseAppResource`，提供了完整的阶段生命周期管理和任务监控功能。

**功能定位**:
- 阶段列表查询和状态管理
- 单个阶段详细信息获取
- 阶段尝试（Attempt）数据访问
- 任务列表查询和分页管理
- 任务指标统计和分布分析
- 数据表格 API 支持

**架构特点**:
- 多层次资源嵌套设计（阶段→尝试→任务）
- 支持复杂查询条件和过滤逻辑
- 集成 DataTables 前端框架支持
- 完整的异常处理机制

## 构造函数参数说明

该类没有显式定义的构造函数，继承自 `BaseAppResource` 基类。通过继承获得以下关键属性：
- `uiRoot`: Spark UI 根对象访问权限
- `ui`: 当前应用程序的 UI 上下文
- `store`: 数据存储接口

## 核心属性分析

### 注解属性
- `@Produces(Array(MediaType.APPLICATION_JSON))`: 默认返回 JSON 格式数据
- `private[v1]`: 包级私有访问权限

### 路径参数模式
- `{stageId: \\d+}`: 阶段ID，必须为数字
- `{stageAttemptId: \\d+}`: 阶段尝试ID，必须为数字

### 查询参数常量
- 默认分位数：`0.0,0.25,0.5,0.75,1.0`
- 默认分页大小：20条记录
- 默认排序字段：ID

## 主要方法分类和说明

### 1. 阶段列表查询 - stageList 方法

**功能**: 获取应用程序的阶段列表，支持状态过滤和详细模式

**参数说明**:
- `statuses`: 阶段状态过滤（多选）
- `details`: 是否返回详细信息（默认false）
- `withSummaries`: 是否包含统计摘要（默认false）
- `quantileString`: 分位数配置字符串
- `taskStatus`: 任务状态过滤条件

**过滤逻辑**:
- 根据任务状态动态过滤阶段
- 支持 FAILED、KILLED、RUNNING、SUCCESS、UNKNOWN 状态
- 仅在详细模式下应用任务状态过滤

### 2. 阶段数据查询 - stageData 方法

**功能**: 获取特定阶段的详细信息

**路径参数**: `{stageId}` - 阶段ID

**查询参数**:
- `details`: 是否包含详细信息（默认true）
- `taskStatus`: 任务状态过滤
- `withSummaries`: 统计摘要标志
- `quantileString`: 分位数配置

**异常处理**:
- 阶段不存在时抛出 `NotFoundException`

### 3. 阶段尝试数据 - oneAttemptData 方法

**功能**: 获取特定阶段尝试的详细信息

**路径参数**:
- `{stageId}`: 阶段ID
- `{stageAttemptId}`: 尝试ID

**智能错误处理**:
- 尝试不存在时检查阶段是否存在
- 提供详细的错误信息（找到的尝试ID列表）

### 4. 任务统计摘要 - taskSummary 方法

**功能**: 获取阶段尝试的任务指标分布统计

**特殊参数**:
- `quantileString`: 默认使用 `0.05,0.25,0.5,0.75,0.95`

**业务逻辑**:
- 计算任务指标的分位数分布
- 无任务数据时抛出异常

### 5. 任务列表查询 - taskList 方法

**功能**: 获取阶段尝试的任务列表，支持分页和排序

**分页参数**:
- `offset`: 起始位置（默认0）
- `length`: 返回数量（默认20）
- `sortBy`: 排序字段（默认ID）
- `statuses`: 任务状态过滤

### 6. 数据表格 API - taskTable 方法

**功能**: 为 DataTables 前端提供任务表格数据

**特殊说明**:
- **格式要求严格**: 注释明确要求保持现有格式
- **DataTables 兼容**: 遵循 DataTables 服务器端协议

**参数处理**:
- 双重 URL 解码避免编码问题
- 支持搜索和过滤功能
- 服务器端分页实现

### 7. 辅助方法

#### doPagination - 分页处理
- 处理 DataTables 分页参数
- 支持列排序和方向控制
- 特殊处理 "Logs" 列排序

#### filterTaskList - 任务搜索过滤
- 基于用户输入进行全文搜索
- 支持任务属性和指标字段搜索
- 不区分大小写的匹配

#### parseQuantileString - 分位数解析
- 将字符串转换为双精度数组
- 参数验证和异常处理

## 设计特点总结

### 1. RESTful 资源嵌套设计
- **层次结构**: 应用程序 → 阶段 → 尝试 → 任务
- **路径参数**: 清晰的资源标识符
- **HTTP 方法**: 统一的 GET 操作

### 2. 灵活的查询能力
- **多条件过滤**: 状态、时间、数量等多维度过滤
- **分页支持**: 支持大数据集的分批获取
- **排序功能**: 多字段排序支持

### 3. 性能优化设计
- **懒加载**: 按需获取详细信息
- **分页查询**: 避免一次性加载大量数据
- **缓存友好**: 合理的默认参数设置

### 4. 前端集成友好
- **DataTables 兼容**: 完整的服务器端 API 支持
- **搜索功能**: 全文搜索和过滤
- **响应格式**: 标准化的 JSON 结构

### 5. 错误处理机制
- **精确异常**: 区分不同层次的错误
- **用户友好**: 详细的错误消息和解决方案
- **健壮性**: 参数验证和边界检查

## 配置参数说明

### 查询参数配置

#### 状态参数
- `status`: 阶段状态过滤（StageStatus 枚举）
- `taskStatus`: 任务状态过滤（TaskStatus 枚举）

#### 显示模式参数
- `details`: 布尔值，控制详细信息级别
- `withSummaries`: 布尔值，控制统计摘要

#### 分页和排序参数
- `offset`/`length`: 分页控制
- `sortBy`: 排序字段（TaskSorting 枚举）

#### 统计参数
- `quantiles`: 分位数配置字符串

### DataTables 特定参数

#### 客户端参数
- `search[value]`: 搜索关键词
- `start`/`length`: 分页参数
- `order[0][dir]`: 排序方向

#### 响应格式
- `aaData`: 实际数据数组
- `recordsTotal`: 总记录数
- `recordsFiltered`: 过滤后记录数

## 使用场景和最佳实践

### 典型使用场景

#### 1. 阶段监控面板
```javascript
// 获取所有运行中的阶段
GET /api/v1/applications/{appId}/stages?status=RUNNING
```

#### 2. 阶段详情分析
```javascript
// 获取特定阶段的详细信息
GET /api/v1/applications/{appId}/stages/{stageId}?details=true
```

#### 3. 任务性能分析
```javascript
// 分析任务执行时间分布
GET /api/v1/applications/{appId}/stages/{stageId}/{attemptId}/taskSummary
```

#### 4. Web UI 数据展示
```javascript
// DataTables 数据加载
GET /api/v1/applications/{appId}/stages/{stageId}/{attemptId}/taskTable
```

### 最佳实践建议

#### 1. 查询优化
- 合理使用 `details` 参数避免不必要的数据传输
- 使用状态过滤减少返回数据量
- 设置合适的分页大小平衡性能和用户体验

#### 2. 错误处理
- 处理 `NotFoundException` 提供友好的用户界面
- 验证分位数参数的合法性
- 处理大数据集时的超时问题

#### 3. 性能考虑
- 避免频繁调用高开销操作（如任务统计）
- 使用缓存机制减少重复计算
- 监控 API 响应时间

### 扩展性考虑

#### 1. 新过滤条件
- 可以添加基于执行器、主机等条件的过滤
- 支持更复杂的时间范围查询

#### 2. 统计功能增强
- 添加更多指标类型的分位数分析
- 支持自定义聚合函数

#### 3. 实时性改进
- 添加流式数据更新支持
- 实现增量数据获取机制