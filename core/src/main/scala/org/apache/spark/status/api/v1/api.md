# API 数据模型分析文档

## 文件概述和定义

`api.scala` 是 Spark REST API v1 版本的核心数据模型定义文件，包含了所有 REST API 接口的数据传输对象（DTO）定义。该文件采用 case class 和普通 class 结合的方式，提供了完整的类型安全数据模型。

**功能定位**:
- 定义 REST API 的请求和响应数据结构
- 提供 JSON 序列化和反序列化的类型定义
- 封装 Spark 内部状态和指标数据
- 支持复杂的统计分析和监控功能

**架构特点**:
- **类型安全**: 强类型定义避免运行时错误
- **Jackson 集成**: 完整的 JSON 序列化支持
- **模块化设计**: 分层的数据模型结构
- **向后兼容**: 支持版本演进和字段弃用

## 核心数据模型分类

### 1. 应用程序相关模型

#### ApplicationInfo - 应用程序信息
**功能**: 封装 Spark 应用程序的基本信息

**关键字段**:
- `id`: 应用程序唯一标识
- `name`: 应用程序名称
- `coresGranted`: 已分配的核心数
- `maxCores`: 最大核心数限制
- `attempts`: 应用程序尝试列表

#### ApplicationAttemptInfo - 应用程序尝试信息
**功能**: 记录应用程序的每次执行尝试

**时间字段**:
- `startTime/endTime`: 开始和结束时间
- `lastUpdated`: 最后更新时间
- `duration`: 执行持续时间

**序列化特性**:
- `@JsonIgnoreProperties`: 忽略内部时间戳字段
- 提供 epoch 时间戳的 getter 方法

### 2. 资源管理模型

#### ResourceProfileInfo - 资源配置文件
**功能**: 定义执行器和任务的资源分配策略

**资源类型**:
- `executorResources`: 执行器资源请求
- `taskResources`: 任务资源请求

#### ExecutorSummary - 执行器摘要
**功能**: 执行器的完整状态和性能指标

**性能指标**:
- 任务统计：活跃、失败、完成、总数
- 资源使用：内存、磁盘、CPU
- I/O 统计：输入输出、Shuffle 操作

**弃用字段处理**:
- `isBlacklisted` → `isExcluded`
- `blacklistedInStages` → `excludedInStages`

### 3. 作业和阶段模型

#### JobData - 作业数据
**功能**: 作业执行状态和统计信息

**统计字段**:
- 任务计数：总数、活跃、完成、跳过、失败、被杀
- 阶段计数：活跃、完成、跳过、失败
- 时间信息：提交时间、完成时间

#### StageData - 阶段数据
**功能**: 阶段的详细执行信息

**性能指标**:
- **时间指标**: 反序列化时间、运行时间、GC时间
- **内存指标**: 内存溢出、峰值内存
- **I/O指标**: 输入输出字节和记录
- **Shuffle指标**: 远程读取、本地读取、写入统计

**高级特性**:
- `taskMetricsDistributions`: 任务指标分布
- `executorMetricsDistributions`: 执行器指标分布
- `isShufflePushEnabled`: Shuffle Push 功能状态

### 4. 任务相关模型

#### TaskData - 任务数据
**功能**: 单个任务的执行详情

**关键信息**:
- `taskId/index/attempt`: 任务标识
- `executorId/host`: 执行位置
- `status/taskLocality`: 状态和本地性
- `taskMetrics`: 性能指标

#### TaskMetrics - 任务指标
**功能**: 任务的详细性能统计

**指标分类**:
- **时间指标**: 反序列化、运行、序列化时间
- **资源指标**: 内存溢出、磁盘溢出
- **I/O指标**: 输入输出统计
- **Shuffle指标**: 读写操作统计

### 5. 统计分布模型

#### TaskMetricDistributions - 任务指标分布
**功能**: 任务指标的分位数分布统计

**分布类型**:
- `quantiles`: 分位数配置（如0.05,0.25,0.5,0.75,0.95）
- 各指标的分位数数值序列

#### ExecutorMetricsDistributions - 执行器指标分布
**功能**: 执行器性能指标的分位数分布

**峰值内存指标**:
- `peakMemoryMetrics`: 峰值内存使用分布
- 支持多种内存类型的统计

### 6. 序列化工具类

#### ExecutorMetricsJsonSerializer/Deserializer
**功能**: ExecutorMetrics 的 JSON 序列化处理

**序列化逻辑**:
- 将 ExecutorMetrics 转换为 Map 格式
- 使用 metricToOffset 映射关系
- 支持空值处理

## 设计特点总结

### 1. 类型安全设计
- **强类型定义**: 所有字段都有明确的类型
- **Option 类型**: 可选字段使用 Option 包装
- **集合类型**: 使用 Seq、Map 等标准集合

### 2. JSON 序列化优化
- **Jackson 注解**: 控制序列化行为
- `@JsonIgnoreProperties`: 忽略内部字段
- `@JsonSerialize/Deserializer`: 自定义序列化器
- `@JsonDeserialize`: 类型转换控制

### 3. 版本兼容性设计
- **字段弃用**: 使用 `@deprecated` 注解标记旧字段
- **向后兼容**: 保留旧字段但推荐使用新字段
- **迁移路径**: 提供清晰的字段替换方案

### 4. 性能监控完整性
- **全面指标**: 覆盖所有关键性能维度
- **分层统计**: 应用程序→作业→阶段→任务
- **分布分析**: 支持分位数统计和分析

### 5. 资源管理精细化
- **资源配置文件**: 支持细粒度资源分配
- **执行器资源**: 内存、CPU、磁盘等资源统计
- **任务资源**: 任务级别的资源使用情况

## 配置参数说明

### Jackson 序列化配置

#### 注解使用规范
- `@JsonIgnoreProperties`: 控制序列化字段可见性
- `@JsonSerialize(using = ...)`: 指定自定义序列化器
- `@JsonDeserialize(using = ...)`: 指定自定义反序列化器

#### 序列化器实现
- **ExecutorMetricsJsonSerializer**: 指标 Map 转换
- **ExecutorPeakMetricsDistributionsJsonSerializer**: 峰值指标分布序列化

### 数据类型映射

#### 时间类型处理
- `Date` 类型: 自动转换为时间戳
- `Option[Date]`: 支持可选时间字段
- epoch 时间戳: 提供 getter 方法

#### 数值类型优化
- `Long` 类型: 大数值支持
- `Option[Long]`: 可选数值字段
- `IndexedSeq[Double]`: 分位数数据序列

### 弃用字段管理

#### 字段迁移策略
- **黑名单字段**: `isBlacklisted` → `isExcluded`
- **阶段排除**: `blacklistedInStages` → `excludedInStages`
- **版本标注**: 明确弃用版本和替代方案

## 使用场景和最佳实践

### 典型数据流场景

#### 1. 应用程序监控
```scala
// 获取应用程序列表
val appList: Seq[ApplicationInfo] = api.getApplications()

// 获取单个应用程序详情
val appDetail: ApplicationInfo = api.getApplication(appId)
```

#### 2. 作业和阶段分析
```scala
// 获取作业列表
val jobs: Seq[JobData] = api.getJobs(appId)

// 获取阶段详情
val stages: Seq[StageData] = api.getStages(appId)
```

#### 3. 性能监控
```scala
// 获取任务指标分布
val distributions: TaskMetricDistributions = api.getTaskDistributions(stageId)

// 获取执行器性能
val executorMetrics: ExecutorMetricsDistributions = api.getExecutorMetrics()
```

### 序列化最佳实践

#### 1. JSON 处理
```scala
// 使用 Jackson 进行序列化
val mapper = new ObjectMapper()
val json = mapper.writeValueAsString(appInfo)

// 反序列化
val appInfo = mapper.readValue(json, classOf[ApplicationInfo])
```

#### 2. 自定义序列化器
```scala
// 注册自定义序列化器
mapper.registerModule(new SimpleModule()
  .addSerializer(classOf[ExecutorMetrics], new ExecutorMetricsJsonSerializer))
```

### 性能优化建议

#### 1. 数据查询优化
- 按需获取字段，避免不必要的数据传输
- 使用分页查询大数据集
- 合理设置查询时间范围

#### 2. 序列化优化
- 使用 Jackson 的流式 API 处理大对象
- 配置合适的序列化特性
- 使用压缩格式减少网络传输

#### 3. 内存管理
- 注意大集合对象的内存使用
- 及时释放不再使用的对象
- 使用合适的数据结构

### 扩展性设计

#### 1. 新指标添加
- 在相应的数据模型中添加新字段
- 更新序列化器支持新字段
- 保持向后兼容性

#### 2. 统计功能扩展
- 添加新的分布统计类型
- 支持更多的分位数配置
- 扩展监控维度

#### 3. 资源类型扩展
- 添加新的资源类型定义
- 更新资源配置文件结构
- 支持动态资源分配

## 技术实现细节

### 时间处理机制
- 使用 Java `Date` 类型表示时间
- 提供 epoch 时间戳的便捷访问
- 支持可选时间字段的序列化

### 指标统计算法
- 分位数计算基于排序后的数据分布
- 支持自定义分位数配置
- 指标分布提供完整的统计视图

### 内存管理优化
- 使用值类型减少对象开销
- 懒加载大字段数据
- 支持数据分片和流式处理

这个数据模型文件为 Spark REST API 提供了完整、类型安全的数据定义，支持复杂的监控、分析和统计功能，是 Spark 监控体系的核心组成部分。