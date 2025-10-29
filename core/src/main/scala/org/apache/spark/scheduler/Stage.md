# Stage.scala 分析文档

## 概述
`Stage` 是Spark调度系统中阶段概念的抽象基类，继承自`Logging`。它定义了Spark作业执行DAG中阶段的基本属性和行为，为`ShuffleMapStage`和`ResultStage`等具体阶段类型提供了统一的接口和基础功能。Stage是Spark任务调度和依赖管理的核心抽象，负责管理阶段的执行状态、尝试次数、失败跟踪和度量信息。

## 抽象类定义
```scala
private[scheduler] abstract class Stage(
    val id: Int,
    val rdd: RDD[_],
    val numTasks: Int,
    val parents: List[Stage],
    val firstJobId: Int,
    val callSite: CallSite,
    val resourceProfileId: Int)
  extends Logging
```

## 构造函数参数

### 必需参数
- `id: Int` - 阶段唯一标识符
- `rdd: RDD[_]` - 目标RDD（shuffle map阶段为map任务RDD，结果阶段为action目标RDD）
- `numTasks: Int` - 阶段总任务数
- `parents: List[Stage]` - 父阶段列表（通过shuffle依赖连接）
- `firstJobId: Int` - 第一个提交该阶段的作业ID（用于FIFO调度）
- `callSite: CallSite` - 调用位置信息（RDD创建位置或action调用位置）
- `resourceProfileId: Int` - 资源配置文件ID

## 核心属性

### numPartitions: Int
```scala
val numPartitions = rdd.partitions.length
```
- **访问权限**: 公共只读
- **描述**: RDD的分区数量
- **计算方式**: 直接从关联RDD的分区数组长度获取

### jobIds: HashSet[Int]
```scala
val jobIds = new HashSet[Int]
```
- **访问权限**: 公共可变集合
- **描述**: 包含该阶段的所有作业ID集合
- **用途**: 跟踪阶段与作业的关联关系

### nextAttemptId: Int
```scala
private var nextAttemptId: Int = 0
```
- **访问权限**: 私有变量
- **描述**: 下一个阶段尝试的ID
- **初始值**: 0（首次尝试）

### name: String
```scala
val name: String = callSite.shortForm
```
- **访问权限**: 公共只读
- **描述**: 阶段名称（调用位置的简短形式）
- **用途**: 显示和日志记录

### details: String
```scala
val details: String = callSite.longForm
```
- **访问权限**: 公共只读
- **描述**: 阶段详细信息（调用位置的完整形式）
- **用途**: 详细日志和调试信息

### _latestInfo: StageInfo
```scala
private var _latestInfo: StageInfo = StageInfo.fromStage(this, nextAttemptId, resourceProfileId = resourceProfileId)
```
- **访问权限**: 私有变量
- **描述**: 最新阶段尝试的StageInfo对象
- **初始化**: 在阶段创建时初始化，确保DAGScheduler有信息传递给监听器

### failedAttemptIds: HashSet[Int]
```scala
val failedAttemptIds = new HashSet[Int]
```
- **访问权限**: 公共可变集合
- **描述**: 失败的阶段尝试ID集合
- **用途**: 避免无限重试，记录重复失败（SPARK-5945）

## 主要方法

### clearFailures方法
```scala
private[scheduler] def clearFailures(): Unit = {
  failedAttemptIds.clear()
}
```
- **功能**: 清除失败尝试记录
- **访问权限**: 调度器包内私有
- **使用场景**: 阶段重试或重新调度时

### makeNewStageAttempt方法
```scala
def makeNewStageAttempt(
    numPartitionsToCompute: Int,
    taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit
```

**功能**: 创建新的阶段尝试

**参数：**
- `numPartitionsToCompute: Int` - 需要计算的分区数量
- `taskLocalityPreferences: Seq[Seq[TaskLocation]]` - 任务位置偏好（默认空序列）

**执行流程：**
1. 创建新的TaskMetrics对象并注册到SparkContext
2. 使用StageInfo.fromStage创建新的StageInfo
3. 更新_latestInfo为新的尝试信息
4. 递增nextAttemptId

### increaseAttemptIdOnFirstSkip方法
```scala
def increaseAttemptIdOnFirstSkip(): Unit = {
  if (nextAttemptId == 0) {
    nextAttemptId = 1
  }
}
```
- **功能**: 在首次跳过时递增尝试ID
- **条件**: 仅当nextAttemptId为0时生效
- **用途**: 处理阶段跳过但需要记录尝试的情况

### latestInfo方法
```scala
def latestInfo: StageInfo = _latestInfo
```
- **功能**: 获取最新阶段尝试的StageInfo
- **返回值**: 当前最新的StageInfo对象

### hashCode方法
```scala
override final def hashCode(): Int = id
```
- **功能**: 基于阶段ID计算哈希值
- **特性**: final方法，确保一致性
- **算法**: 直接返回阶段ID

### equals方法
```scala
override final def equals(other: Any): Boolean = other match {
  case stage: Stage => stage != null && stage.id == id
  case _ => false
}
```
- **功能**: 基于阶段ID判断相等性
- **逻辑**: 检查是否为Stage实例且ID相同
- **特性**: final方法，确保一致性

## 抽象方法

### findMissingPartitions方法
```scala
def findMissingPartitions(): Seq[Int]
```
- **功能**: 查找需要计算的分区ID
- **实现要求**: 必须由子类实现
- **返回值**: 需要计算的分区ID序列

## 辅助方法

### isIndeterminate方法
```scala
def isIndeterminate: Boolean = {
  rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
}
```
- **功能**: 判断阶段是否具有不确定性输出
- **判断依据**: RDD的outputDeterministicLevel属性
- **用途**: 影响任务重试和结果确定性

## 设计特点

### 1. 抽象基类设计
- 为具体阶段类型提供统一接口
- 封装通用阶段属性和行为
- 支持多态处理和扩展

### 2. 尝试管理机制
- 支持多阶段尝试（容错和重试）
- 跟踪失败尝试避免无限重试
- 维护最新尝试状态信息

### 3. 作业关联管理
- 记录阶段所属的作业集合
- 支持FIFO调度优先级
- 提供作业级别的阶段管理

### 4. 状态信息维护
- 实时更新StageInfo对象
- 支持监听器事件传递
- 提供UI展示所需信息

## 使用场景

### 1. 调度器集成
- DAGScheduler的阶段依赖管理
- TaskScheduler的任务调度
- 阶段执行状态跟踪

### 2. 容错和重试
- 阶段失败检测和重试
- 尝试次数限制和记录
- 失败模式分析和优化

### 3. 监控和调试
- 阶段执行进度监控
- 性能度量和分析
- 调试信息收集

### 4. UI展示
- Web UI的阶段状态显示
- 作业执行进度可视化
- 历史作业查看

## 配置参数

### 资源管理配置
- **resourceProfileId**: 资源配置文件标识
- **影响**: 执行器分配和资源限制
- **版本**: Spark 3.1.0+引入

### 调度策略配置
- **firstJobId**: FIFO调度优先级
- **影响**: 作业执行顺序和资源分配
- **用途**: 确保早期作业优先执行

### 位置偏好配置
- **taskLocalityPreferences**: 任务位置偏好
- **影响**: 数据本地化调度优化
- **优化**: 减少网络传输开销

## 补充分析

### 系统集成
- 与DAGScheduler紧密集成，管理阶段依赖
- 通过StageInfo与SparkListeners通信
- 与TaskSetManager协同管理任务执行

### 性能影响
- 阶段尝试管理增加调度开销
- 失败跟踪避免不必要的重试
- 位置偏好优化提升执行效率

### 容错机制
- 支持阶段级别的重试
- 失败尝试记录防止无限循环
- 与检查点机制协同工作

### 扩展建议
- 可以添加更细粒度的阶段状态管理
- 支持动态资源调整和弹性伸缩
- 增强阶段执行预测和优化

## 总结

`Stage` 抽象类是Spark调度系统中阶段管理的核心组件，为各种阶段类型提供了统一的接口和基础功能。其设计充分考虑了容错性、状态管理和性能优化等关键需求，通过合理的尝试机制和作业关联管理，确保了Spark作业的可靠执行和高效调度。作为Spark DAG执行模型的基础构建块，Stage在任务分解、依赖管理和执行优化中发挥着重要作用。