# DAGScheduler 类分析

## 类的概述和定义

`DAGScheduler` 是 Spark 的高层调度层，实现了面向阶段的调度。它是 Spark 作业调度系统的核心组件，负责将用户的作业转换为有向无环图（DAG）并调度执行。

**类定义特征：**
- 继承自 `Logging`，提供日志功能
- 被标记为 `private[spark]`，主要在 Spark 内部使用
- 采用事件驱动架构，通过 `DAGSchedulerEventProcessLoop` 处理事件

## 构造函数参数说明

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `sc` | `SparkContext` | Spark 上下文对象 |
| `taskScheduler` | `TaskScheduler` | 底层任务调度器 |
| `listenerBus` | `LiveListenerBus` | 事件监听器总线 |
| `mapOutputTracker` | `MapOutputTrackerMaster` | 映射输出跟踪器 |
| `blockManagerMaster` | `BlockManagerMaster` | 块管理器主节点 |
| `env` | `SparkEnv` | Spark 环境 |
| `clock` | `Clock` | 时钟对象（默认 SystemClock） |

## 核心属性分析

### 1. 作业和阶段管理
- `nextJobId`: 作业ID生成器
- `nextStageId`: 阶段ID生成器
- `jobIdToStageIds`: 作业到阶段ID的映射
- `stageIdToStage`: 阶段ID到阶段对象的映射
- `shuffleIdToMapStage`: Shuffle依赖ID到ShuffleMapStage的映射
- `jobIdToActiveJob`: 作业ID到活动作业的映射

### 2. 阶段状态跟踪
- `waitingStages`: 等待运行的阶段（父阶段未完成）
- `runningStages`: 正在运行的阶段
- `failedStages`: 需要重新提交的失败阶段
- `activeJobs`: 活动作业集合

### 3. 缓存和位置管理
- `cacheLocs`: RDD分区缓存位置映射
- `executorFailureEpoch`: 执行器失败纪元跟踪
- `shuffleFileLostEpoch`: Shuffle文件丢失纪元跟踪

### 4. 配置和资源管理
- `outputCommitCoordinator`: 输出提交协调器
- `closureSerializer`: 闭包序列化器
- 各种配置参数（动态分配、屏障作业检查等）

## 主要方法分类和说明

### 1. 作业提交方法

#### submitJob() 方法
```scala
def submitJob[T, U](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    callSite: CallSite,
    resultHandler: (Int, U) => Unit,
    properties: Properties): JobWaiter[U]
```
- 提交动作作业到调度器
- 验证分区有效性
- 创建 JobWaiter 对象用于异步等待
- 通过事件循环提交 JobSubmitted 事件

#### runJob() 方法
```scala
def runJob[T, U](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    callSite: CallSite,
    resultHandler: (Int, U) => Unit,
    properties: Properties): Unit
```
- 运行动作作业并等待完成
- 内部调用 submitJob 并同步等待结果
- 提供作业执行时间统计

### 2. 任务生命周期管理

#### taskStarted() / taskEnded() 方法
- 处理任务开始和结束事件
- 通过事件循环异步处理

#### executorLost() / workerRemoved() 方法
- 处理执行器和Worker节点失效
- 触发相应的恢复机制

### 3. 阶段创建和管理

#### createShuffleMapStage() 方法
- 创建 ShuffleMapStage 生成Shuffle依赖的分区
- 检查屏障作业限制
- 注册Shuffle到MapOutputTracker

#### createResultStage() 方法
- 创建 ResultStage 执行最终动作
- 处理资源配置文件合并
- 检查各种约束条件

#### getOrCreateShuffleMapStage() 方法
- 获取或创建ShuffleMapStage
- 处理祖先Shuffle依赖
- 确保阶段共享和重用

### 4. 资源管理

#### mergeResourceProfilesForStage() 方法
- 合并阶段的资源配置文件
- 处理多个资源配置文件的冲突
- 支持资源配置文件的最大值合并策略

## 设计特点总结

### 1. 事件驱动架构
- 使用 `DAGSchedulerEventProcessLoop` 处理所有事件
- 异步事件处理避免阻塞主线程
- 支持高并发作业调度

### 2. 阶段化调度
- 将作业分解为多个阶段（Stage）
- 阶段间通过Shuffle边界分隔
- 支持阶段的重用和共享

### 3. 容错机制
- 完善的失败检测和恢复机制
- 支持阶段重试和任务重试
- 处理Shuffle文件丢失等复杂场景

### 4. 资源管理
- 支持动态资源分配
- 屏障作业的特殊处理
- 资源配置文件的智能合并

### 5. 性能优化
- 缓存位置感知的任务调度
- 阶段和任务的智能重用
- 避免不必要的数据重新计算

## 配置参数说明

### 1. 调度相关配置
- `spark.stage.maxConsecutiveAttempts`: 阶段最大连续尝试次数
- `spark.scheduler.listenerbus.eventqueue.capacity`: 事件队列容量

### 2. 屏障作业配置
- `spark.dynamicAllocation.enabled`: 动态资源分配开关
- `spark.stage.ignoreDecommissionFetchFailure`: 忽略退役执行器的获取失败

### 3. Shuffle相关配置
- `spark.shuffle.push.enabled`: 推送式Shuffle开关
- `spark.shuffle.push.minPushRatio`: 最小推送比例

## 补充分析

### 1. 在Spark架构中的核心地位
DAGScheduler 是连接用户作业和底层任务执行的桥梁：
- 将高级的RDD操作转换为具体的任务调度
- 管理作业的依赖关系和执行顺序
- 提供容错和性能优化功能

### 2. 复杂的状态管理
- 管理作业、阶段、任务的完整生命周期
- 处理各种异常情况和恢复场景
- 维护复杂的依赖关系图

### 3. 性能优化策略
- 阶段边界优化减少Shuffle操作
- 数据本地性优化提高任务执行效率
- 缓存机制避免重复计算

### 4. 扩展性设计
- 支持自定义调度策略
- 可扩展的事件处理机制
- 灵活的资源配置管理

### 5. 实际应用场景
- 批处理作业的调度执行
- 流处理作业的微批调度
- 机器学习作业的迭代计算