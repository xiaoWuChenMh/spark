# StoreTypes 存储类型分析文档

## 文件概述和定义

`storeTypes.scala` 是 Spark 状态监控系统的存储类型定义文件，包含了所有用于 KVStore 存储的数据包装器类。这些类负责将 API 数据模型转换为适合存储的格式，并配置索引和查询参数。

**功能定位**:
- **数据映射**: 将 API 数据模型映射到存储格式
- **索引配置**: 定义存储索引和查询参数
- **序列化支持**: 提供 JSON 序列化注解
- **版本管理**: 支持存储格式的版本兼容性

**架构特点**:
- **包装器模式**: 使用包装器类封装原始数据
- **类型安全**: 强类型的存储接口定义
- **索引优化**: 多级索引支持高效查询
- **内存优化**: 紧凑的数据结构设计

## 核心包装器类分析

### 1. 应用程序相关包装器

#### ApplicationInfoWrapper - 应用程序信息包装器
```scala
private[spark] class ApplicationInfoWrapper(val info: ApplicationInfo)
```

**索引配置**:
- `@KVIndex` 主键索引：应用程序ID
- 使用 `info.id` 作为存储键

**设计特点**:
- **简单映射**: 直接包装 ApplicationInfo 对象
- **唯一标识**: 使用应用程序ID作为主键
- **最小开销**: 不添加额外字段

#### ApplicationEnvironmentInfoWrapper - 应用程序环境信息包装器
```scala
private[spark] class ApplicationEnvironmentInfoWrapper(val info: ApplicationEnvironmentInfo)
```

**特殊设计**:
- **固定ID**: 使用类名作为存储键（每个应用只有一个环境信息）
- **单例模式**: 确保每个应用只有一个环境信息实例

### 2. 执行器相关包装器

#### ExecutorSummaryWrapper - 执行器摘要包装器
```scala
private[spark] class ExecutorSummaryWrapper(val info: ExecutorSummary)
```

**多索引配置**:
- **主键索引**: `id` - 执行器ID
- **状态索引**: `active` - 活跃状态过滤
- **主机索引**: `host` - 主机名过滤

**索引优化**:
```scala
@JsonIgnore @KVIndex("host")
val host: String = Utils.parseHostPort(info.hostPort)._1
```

**功能**:
- 提取主机名用于索引
- 支持按主机进行执行器查询
- 提高集群监控的查询效率

### 3. 作业相关包装器

#### JobDataWrapper - 作业数据包装器
```scala
private[spark] class JobDataWrapper(
    val info: JobData,
    val skippedStages: Set[Int],
    val sqlExecutionId: Option[Long])
```

**扩展属性**:
- `skippedStages`: 跳过的阶段集合
- `sqlExecutionId`: SQL执行ID关联

**索引配置**:
- **主键索引**: `id` - 作业ID
- **完成时间索引**: `completionTime` - 按完成时间排序

**业务逻辑**:
- 支持作业与SQL执行的关联
- 跟踪跳过的阶段信息
- 提供准确的作业统计

### 4. 阶段相关包装器

#### StageDataWrapper - 阶段数据包装器
```scala
private[spark] class StageDataWrapper(
    val info: StageData,
    val jobIds: Set[Int],
    @JsonDeserialize(contentAs = classOf[JLong])
    val locality: Map[String, Long])
```

**复合主键**:
```scala
@JsonIgnore @KVIndex
private[this] val id: Array[Int] = Array(info.stageId, info.attemptId)
```

**多维度索引**:
- **阶段ID索引**: `stageId` - 按阶段ID查询
- **活跃状态索引**: `active` - 过滤活跃阶段
- **完成时间索引**: `completionTime` - 按完成时间排序

**本地性统计**:
- 存储任务本地性分布信息
- 支持本地性分析功能

### 5. 任务相关包装器

#### TaskDataWrapper - 任务数据包装器

**设计目标**:
- **内存优化**: 避免存储完整的 TaskData 对象
- **索引丰富**: 支持多种查询和排序方式
- **性能优先**: 针对大规模任务数据优化

**内存优化策略**:
```scala
// 不存储完整的 TaskData 对象以节省内存
// 使用展开的指标字段替代嵌套对象
```

## 索引系统设计

### TaskIndexNames - 任务索引名称常量

#### 索引命名规范
```scala
final val ACCUMULATORS = "acc"
final val ATTEMPT = "att"
final val DESER_TIME = "des"
final val DURATION = "dur"
```

**设计原则**:
- **短名称**: 使用2-3字符的缩写
- **语义明确**: 名称反映索引用途
- **一致性**: 统一的命名规范

#### 完整索引列表

**基本属性索引**:
- `ACCUMULATORS`: 累加器信息
- `ATTEMPT`: 任务尝试次数
- `DURATION`: 任务执行时间
- `EXECUTOR`: 执行器ID

**性能指标索引**:
- `DESER_TIME`: 反序列化时间
- `GC_TIME`: GC时间
- `INPUT_SIZE`: 输入数据大小
- `OUTPUT_SIZE`: 输出数据大小

**Shuffle操作索引**:
- `SHUFFLE_LOCAL_BLOCKS`: 本地块读取
- `SHUFFLE_REMOTE_BLOCKS`: 远程块读取
- `SHUFFLE_READ_RECORDS`: Shuffle读取记录数
- `SHUFFLE_WRITE_SIZE`: Shuffle写入大小

**高级索引**:
- `SHUFFLE_PUSH_CORRUPT_MERGED_BLOCK_CHUNKS`: 损坏的合并块
- `SHUFFLE_PUSH_MERGED_REMOTE_BLOCKS`: 合并的远程块
- `SHUFFLE_REMOTE_REQS_DURATION`: 远程请求持续时间

### 索引层次结构

#### 父级索引设计
```scala
@KVIndexParam(parent = TaskIndexNames.STAGE)
val taskId: JLong
```

**层次关系**:
- **阶段级索引**: 所有任务索引都挂载在阶段下
- **范围查询**: 支持按阶段查询任务
- **性能优化**: 减少索引扫描范围

#### 复合索引支持
```scala
@KVIndexParam(value = TaskIndexNames.TASK_INDEX, parent = TaskIndexNames.STAGE)
val index: Int
```

**多字段索引**:
- 支持阶段ID + 任务索引的复合查询
- 提高特定任务的查找效率

## 数据转换和序列化

### JSON序列化配置

#### 注解使用
```scala
@JsonIgnore @KVIndex
private def id: String = info.id
```

**序列化控制**:
- `@JsonIgnore`: 忽略序列化字段
- `@KVIndex`: 标记存储索引字段
- `@JsonDeserialize`: 控制反序列化行为

#### 类型适配器
```scala
@JsonDeserialize(contentAs = classOf[JLong])
val locality: Map[String, Long]
```

**类型安全**:
- 确保 Map 值的正确类型
- 支持泛型类型的序列化
- 避免运行时类型错误

### 数据转换方法

#### toApi - API数据转换
```scala
def toApi: TaskData
```

**转换逻辑**:
- 从存储格式重建 API 数据模型
- 处理特殊值（如负值指标）
- 重建嵌套对象结构

#### 指标值恢复
```scala
private def getMetricValue(metric: Long): Long = {
  if (status != "SUCCESS") {
    math.abs(metric + 1)
  } else {
    metric
  }
}
```

**特殊处理**:
- **失败任务**: 使用负值存储指标
- **恢复算法**: `math.abs(metric + 1)`
- **状态判断**: 根据任务状态决定恢复方式

## 内存优化技术

### 1. 对象结构优化

#### 扁平化设计
```scala
// 替代嵌套的 TaskMetrics 对象
val executorDeserializeTime: Long
val executorRunTime: Long
val resultSize: Long
// ... 其他30+个指标字段
```

**优势**:
- **减少对象开销**: 避免嵌套对象的额外内存
- **估算节省**: 每个任务节省约80字节
- **批量处理**: 提高序列化/反序列化效率

#### 懒加载策略
```scala
val hasMetrics: Boolean
```

**条件处理**:
- 仅当有指标数据时才进行复杂计算
- 避免无指标任务的开销
- 支持增量更新

### 2. 字符串优化

#### 字符串池使用
```scala
weakIntern(info.executorId)
```

**内存优化**:
- 复用相同的字符串对象
- 减少重复字符串的内存占用
- 支持垃圾回收的弱引用

#### 主机名提取
```scala
val host: String = Utils.parseHostPort(info.hostPort)._1
```

**存储优化**:
- 只存储主机名而非完整主机端口
- 减少存储空间占用
- 支持主机级别的聚合查询

### 3. 集合优化

#### 高效集合类
```scala
val skippedStages: Set[Int]
val jobIds: Set[Int]
```

**选择理由**:
- `Set` 类型避免重复元素
- 支持高效的包含性检查
- 内存占用相对较小

#### 数组存储
```scala
private[this] val id: Array[Int] = Array(stageId, attemptId)
```

**性能优势**:
- 数组访问速度快
- 连续内存布局
- 支持复合键查询

## 特殊数据类型处理

### 1. RDD操作图包装器

#### RDDOperationGraphWrapper
```scala
private[spark] class RDDOperationGraphWrapper(
    @KVIndexParam val stageId: Int,
    val edges: collection.Seq[RDDOperationEdge],
    val rootCluster: RDDOperationClusterWrapper)
```

**图结构存储**:
- **边关系**: 存储操作之间的依赖关系
- **集群结构**: 层次化的操作集群
- **阶段关联**: 与特定阶段关联

#### 转换方法
```scala
def toRDDOperationGraph(): RDDOperationGraph
```

**图重建**:
- 从存储格式重建操作图
- 恢复边和集群的层次结构
- 支持可视化展示

### 2. 资源配置文件包装器

#### ResourceProfileWrapper
```scala
private[spark] class ResourceProfileWrapper(val rpInfo: ResourceProfileInfo)
```

**资源配置**:
- 执行器和任务的资源请求
- 资源分配策略信息
- 支持动态资源管理

### 3. 流数据块包装器

#### StreamBlockData
```scala
private[spark] class StreamBlockData(
    val name: String,
    val executorId: String,
    val storageLevel: String,
    val memSize: Long,
    val diskSize: Long)
```

**流处理支持**:
- 流数据块的存储信息
- 内存和磁盘使用情况
- 存储级别配置

**复合键设计**:
```scala
@JsonIgnore @KVIndex
def key: Array[String] = Array(name, executorId)
```

## 统计和缓存类型

### 1. 分位数缓存

#### CachedQuantile - 分位数缓存
```scala
private[spark] class CachedQuantile(
    val stageId: Int,
    val stageAttemptId: Int,
    val quantile: String,
    val taskCount: Long,
    // ... 大量指标字段)
```

**缓存策略**:
- **预计算**: 缓存常用的分位数结果
- **性能优化**: 避免重复计算
- **存储效率**: 只存储必要的统计信息

**复合键设计**:
```scala
@KVIndex @JsonIgnore
def id: Array[Any] = Array(stageId, stageAttemptId, quantile)
```

### 2. 应用摘要

#### AppSummary - 应用摘要
```scala
private[spark] class AppSummary(
    val numCompletedJobs: Int,
    val numCompletedStages: Int)
```

**摘要统计**:
- 已完成作业和阶段数量
- 应用级别的聚合信息
- 快速状态查询支持

**单例设计**:
```scala
@KVIndex
def id: String = classOf[AppSummary].getName()
```

## 设计模式和应用

### 1. 包装器模式（Wrapper Pattern）

#### 模式实现
```scala
class XWrapper(val info: X) {
  // 添加存储特定的元数据和索引
  @KVIndex def id: String = info.id
}
```

**优势**:
- **关注点分离**: 存储逻辑与业务逻辑分离
- **可扩展性**: 易于添加新的存储特性
- **兼容性**: 不影响原有的API数据模型

### 2. 装饰器模式（Decorator Pattern）

#### 功能增强
```scala
class JobDataWrapper(val info: JobData, val skippedStages: Set[Int])
```

**功能扩展**:
- 在原有数据基础上添加新功能
- 保持接口一致性
- 支持渐进式增强

### 3. 工厂模式（Factory Pattern）

#### 对象创建
```scala
def toApi(): TaskData = {
  // 根据存储数据重建API对象
  new TaskData(...)
}
```

**封装创建逻辑**:
- 隐藏复杂的对象构建过程
- 提供统一的创建接口
- 支持多种数据源

## 性能优化策略

### 1. 存储空间优化

#### 字段选择策略
- **必要字段**: 只存储查询所需的字段
- **派生字段**: 避免存储可计算的数据
- **索引字段**: 精心选择索引字段减少存储开销

#### 数据类型优化
- **原始类型**: 优先使用基本类型而非对象
- **数组存储**: 使用数组存储复合键
- **枚举优化**: 使用字符串常量而非枚举对象

### 2. 查询性能优化

#### 索引设计原则
- **选择性**: 高选择性的字段优先索引
- **复合索引**: 支持常见查询模式的复合索引
- **覆盖索引**: 尽可能让索引覆盖查询需求

#### 查询模式分析
- **阶段查询**: 按阶段ID查询相关任务
- **时间范围**: 支持按时间范围查询
- **状态过滤**: 按任务状态进行过滤

### 3. 内存使用优化

#### 对象池技术
- **字符串池**: 复用常用字符串
- **缓存重用**: 重用计算结果
- **懒加载**: 延迟初始化复杂对象

#### 数据结构选择
- **集合类型**: 根据使用场景选择合适集合
- **数组应用**: 对固定大小数据使用数组
- **映射优化**: 使用特化的映射实现

## 扩展性和维护性

### 1. 版本兼容性

#### 向后兼容策略
- **字段添加**: 新字段设置为可选
- **默认值处理**: 为缺失字段提供默认值
- **迁移脚本**: 支持数据格式迁移

#### 向前兼容考虑
- **注解忽略**: 使用 `@JsonIgnore` 忽略未知字段
- **灵活解析**: 支持部分数据的解析
- **错误恢复**: 提供数据损坏的恢复机制

### 2. 监控和调试

#### 存储统计
- **索引使用**: 监控各索引的使用频率
- **存储大小**: 跟踪各类型数据的存储占用
- **查询性能**: 分析查询响应时间

#### 调试支持
- **序列化日志**: 记录序列化/反序列化过程
- **索引验证**: 验证索引的正确性
- **数据一致性**: 检查存储数据的完整性

StoreTypes 文件为 Spark 状态监控系统提供了完整的数据存储定义框架，通过精心的类型设计和优化策略，实现了高效、可靠的状态数据管理。其模块化设计和扩展性支持为 Spark 生态系统的监控能力奠定了坚实基础。