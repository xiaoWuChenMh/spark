# TaskSetExcludeList.scala 分析文档

## 概述
`TaskSetExcludeList` 是Spark调度系统中负责处理任务集内执行器和节点排除逻辑的核心类。它管理特定任务集级别的排除策略，跟踪任务失败情况，并根据配置的阈值自动排除有问题的执行器和节点。

## 类定义和构造函数
```scala
private[scheduler] class TaskSetExcludelist(
    private val listenerBus: LiveListenerBus,
    val conf: SparkConf,
    val stageId: Int,
    val stageAttemptId: Int,
    val clock: Clock) extends Logging
```

### 构造函数参数说明
- `listenerBus`: 事件监听总线，用于发送排除事件
- `conf`: Spark配置对象
- `stageId`: 阶段ID
- `stageAttemptId`: 阶段尝试ID
- `clock`: 时钟对象，用于时间跟踪

## 核心配置参数
类从Spark配置中读取以下关键参数：

- `MAX_TASK_ATTEMPTS_PER_EXECUTOR`: 每个执行器上单个任务的最大尝试次数
- `MAX_TASK_ATTEMPTS_PER_NODE`: 每个节点上单个任务的最大尝试次数
- `MAX_FAILURES_PER_EXEC_STAGE`: 阶段内执行器被排除前的最大失败次数
- `MAX_FAILED_EXEC_PER_NODE_STAGE`: 阶段内节点被排除前的最大失败执行器数量

## 核心数据结构

### 1. 执行器失败跟踪
```scala
val execToFailures = new HashMap[String, ExecutorFailuresInTaskSet]()
```
记录每个执行器上的任务失败情况，用于任务集内排除和应用级排除。

### 2. 节点失败跟踪
```scala
private val nodeToExecsWithFailures = new HashMap[String, HashSet[String]]()
private val nodeToExcludedTaskIndexes = new HashMap[String, HashSet[Int]]()
```
跟踪节点上的执行器失败情况和被排除的任务索引。

### 3. 排除集合
```scala
private val excludedExecs = new HashSet[String]()  // 被排除的执行器
private val excludedNodes = new HashSet[String]()  // 被排除的节点
```

## 主要方法分析

### 1. 排除检查方法

#### `isExecutorExcludedForTask(executorId: String, index: Int): Boolean`
检查特定执行器是否对给定任务被排除。仅检查任务集级别的排除，不检查应用级排除。

#### `isNodeExcludedForTask(node: String, index: Int): Boolean`
检查特定节点是否对给定任务被排除。

#### `isExecutorExcludedForTaskSet(executorId: String): Boolean`
检查执行器是否对整个任务集被排除。

#### `isNodeExcludedForTaskSet(node: String): Boolean`
检查节点是否对整个任务集被排除。

### 2. 失败更新方法

#### `updateExcludedForFailedTask(host: String, exec: String, index: Int, failureReason: String)`
核心方法，处理任务失败后的排除逻辑：

1. **更新执行器失败记录**：记录任务在特定执行器上的失败
2. **检查节点级排除**：统计同一节点上不同执行器的任务失败总数
3. **执行器级排除**：当执行器失败次数达到阈值时排除整个执行器
4. **节点级排除**：当节点上被排除的执行器数量达到阈值时排除整个节点
5. **事件通知**：通过监听总线发送排除事件

## 设计特点

### 1. 分层排除策略
- **任务级别排除**：特定任务在特定执行器/节点上的排除
- **执行器级别排除**：整个执行器对任务集的排除
- **节点级别排除**：整个节点对任务集的排除

### 2. 线程安全设计
- 设计为`TaskSetManager`的辅助类
- 只能在持有TaskScheduler锁的代码中调用
- 通过事件处理机制确保线程安全

### 3. 性能优化
- 使用快速检查方法避免不必要的全局排除检查
- 延迟计算节点级排除，只在必要时构建节点-执行器映射

## 使用场景

### 1. 任务调度时的排除检查
在`TaskSetManager.resourceOffer`方法中调用，确保不将任务调度到被排除的资源上。

### 2. 失败处理时的状态更新
在任务失败时更新排除状态，防止重复失败。

### 3. 应用级排除信息传递
成功完成的任务集将其排除信息传递给`HealthTracker`，用于应用级排除决策。

## 配置参数说明

### 排除阈值配置
- `spark.task.maxFailures.per.executor`: 每个执行器上任务的最大失败次数
- `spark.task.maxFailures.per.node`: 每个节点上任务的最大失败次数
- `spark.stage.max.failures.per.executor`: 阶段内执行器的最大失败次数
- `spark.stage.max.failed.executors.per.node`: 阶段内节点的最大失败执行器数

## 补充分析

### 性能影响
排除检查在任务调度的关键路径上，但通过分层检查和缓存机制最小化性能开销。

### 容错机制
与`HealthTracker`协同工作，提供任务集级别和应用级别的双重容错保护。

### 扩展性
支持动态配置调整，可根据集群规模和工作负载特性优化排除策略。