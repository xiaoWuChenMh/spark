# StageInfo.scala 分析文档

## 概述
`StageInfo` 是Spark调度系统中用于存储和传递阶段信息的核心数据类，使用`@DeveloperApi`注解标记。它封装了阶段的元数据、执行状态、度量指标和配置信息，为调度器、监听器和UI组件之间的信息传递提供了标准化的接口。StageInfo在Spark的事件系统和监控体系中扮演着关键角色。

## 类定义
```scala
@DeveloperApi
class StageInfo(
    val stageId: Int,
    private val attemptId: Int,
    val name: String,
    val numTasks: Int,
    val rddInfos: Seq[RDDInfo],
    val parentIds: Seq[Int],
    val details: String,
    val taskMetrics: TaskMetrics = null,
    private[spark] val taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
    private[spark] val shuffleDepId: Option[Int] = None,
    val resourceProfileId: Int,
    private[spark] var isShufflePushEnabled: Boolean = false,
    private[spark] var shuffleMergerCount: Int = 0)
```

## 构造函数参数

### 必需参数
- `stageId: Int` - 阶段唯一标识符
- `attemptId: Int` - 阶段尝试ID（私有访问）
- `name: String` - 阶段名称
- `numTasks: Int` - 任务数量
- `rddInfos: Seq[RDDInfo]` - 相关RDD信息序列
- `parentIds: Seq[Int]` - 父阶段ID序列
- `details: String` - 阶段详细信息
- `resourceProfileId: Int` - 资源配置文件ID

### 可选参数
- `taskMetrics: TaskMetrics = null` - 任务度量指标（默认null）
- `taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty` - 任务位置偏好（私有）
- `shuffleDepId: Option[Int] = None` - shuffle依赖ID（私有）
- `isShufflePushEnabled: Boolean = false` - shuffle push是否启用（私有可变）
- `shuffleMergerCount: Int = 0` - shuffle合并器数量（私有可变）

## 可变属性

### submissionTime: Option[Long]
```scala
var submissionTime: Option[Long] = None
```
- **访问权限**: 公共可变
- **描述**: 阶段提交到TaskScheduler的时间
- **用途**: 记录调度开始时间

### completionTime: Option[Long]
```scala
var completionTime: Option[Long] = None
```
- **访问权限**: 公共可变
- **描述**: 阶段完成或取消的时间
- **用途**: 记录执行结束时间

### failureReason: Option[String]
```scala
var failureReason: Option[String] = None
```
- **访问权限**: 公共可变
- **描述**: 阶段失败的原因
- **用途**: 错误诊断和重试决策

### accumulables: HashMap[Long, AccumulableInfo]
```scala
val accumulables = HashMap[Long, AccumulableInfo]()
```
- **访问权限**: 公共可变映射
- **描述**: 累加器终值映射（包括用户自定义累加器）
- **键类型**: 累加器ID（Long）
- **值类型**: 累加器信息（AccumulableInfo）

## 主要方法

### stageFailed方法
```scala
def stageFailed(reason: String): Unit = {
  failureReason = Some(reason)
  completionTime = Some(System.currentTimeMillis)
}
```
- **功能**: 标记阶段失败状态
- **参数**: `reason: String` - 失败原因描述
- **操作**: 设置失败原因和完成时间

### attemptNumber方法
```scala
def attemptNumber(): Int = attemptId
```
- **功能**: 获取阶段尝试次数（兼容性方法）
- **返回值**: 私有attemptId的值
- **设计**: 保持括号语法兼容性

### getStatusString方法
```scala
private[spark] def getStatusString: String = {
  if (completionTime.isDefined) {
    if (failureReason.isDefined) {
      "failed"
    } else {
      "succeeded"
    }
  } else {
    "running"
  }
}
```
- **功能**: 获取阶段状态字符串
- **访问权限**: Spark包内私有
- **状态判断**: 基于完成时间和失败原因

### setShuffleMergerCount方法
```scala
private[spark] def setShuffleMergerCount(mergers: Int): Unit = {
  shuffleMergerCount = mergers
}
```
- **功能**: 设置shuffle合并器数量
- **访问权限**: Spark包内私有
- **参数**: `mergers: Int` - 合并器数量

### setPushBasedShuffleEnabled方法
```scala
private[spark] def setPushBasedShuffleEnabled(pushBasedShuffleEnabled: Boolean): Unit = {
  isShufflePushEnabled = pushBasedShuffleEnabled
}
```
- **功能**: 设置push-based shuffle启用状态
- **访问权限**: Spark包内私有
- **参数**: `pushBasedShuffleEnabled: Boolean` - 是否启用

## 伴生对象方法

### fromStage工厂方法
```scala
private[spark] object StageInfo {
  def fromStage(
      stage: Stage,
      attemptId: Int,
      numTasks: Option[Int] = None,
      taskMetrics: TaskMetrics = null,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty,
      resourceProfileId: Int
    ): StageInfo
}
```

**功能**: 从Stage对象创建StageInfo实例

**参数说明：**
- `stage: Stage` - 源Stage对象
- `attemptId: Int` - 尝试ID
- `numTasks: Option[Int]` - 任务数量（可选）
- `taskMetrics: TaskMetrics` - 任务度量指标
- `taskLocalityPreferences: Seq[Seq[TaskLocation]]` - 任务位置偏好
- `resourceProfileId: Int` - 资源配置文件ID

**创建逻辑：**
1. **获取RDD信息**: 
   ```scala
   val ancestorRddInfos = stage.rdd.getNarrowAncestors.map(RDDInfo.fromRdd)
   val rddInfos = Seq(RDDInfo.fromRdd(stage.rdd)) ++ ancestorRddInfos
   ```
   - 获取目标RDD和所有窄依赖祖先RDD的信息
   - 使用RDDInfo.fromRdd转换RDD为RDDInfo

2. **确定shuffle依赖ID**:
   ```scala
   val shuffleDepId = stage match {
     case sms: ShuffleMapStage => Option(sms.shuffleDep).map(_.shuffleId)
     case _ => None
   }
   ```
   - 仅ShuffleMapStage有shuffle依赖ID
   - 其他阶段类型返回None

3. **创建StageInfo实例**:
   - 使用收集的信息构建完整的StageInfo对象
   - 设置shuffle push相关参数为默认值（false, 0）

## 设计特点

### 1. 信息封装设计
- 封装阶段的所有关键元数据
- 支持状态信息的动态更新
- 提供标准化的信息传递接口

### 2. 状态管理机制
- 支持阶段生命周期的状态跟踪
- 记录失败原因和完成时间
- 提供状态字符串表示

### 3. 度量指标集成
- 集成TaskMetrics支持性能监控
- 维护累加器终值映射
- 支持用户自定义度量

### 4. 高级功能支持
- Shuffle push配置管理
- 合并器数量动态调整
- 位置偏好优化支持

## 使用场景

### 1. 事件系统集成
- SparkListener事件的数据载体
- 阶段状态变化的事件传递
- UI组件的数据源

### 2. 监控和调试
- 实时阶段执行状态监控
- 性能度量和分析
- 故障诊断和重试决策

### 3. 调度优化
- 任务位置偏好调度
- 资源分配和配置管理
- Shuffle优化策略应用

### 4. 历史记录
- 事件日志的持久化存储
- 历史作业的查看和分析
- 性能基准和趋势分析

## 配置参数

### 资源管理配置
- **resourceProfileId**: 资源配置文件标识
- **影响**: 执行器分配和资源限制
- **版本**: Spark 3.1.0+引入

### Shuffle优化配置
- **isShufflePushEnabled**: push-based shuffle启用状态
- **shuffleMergerCount**: shuffle合并器数量
- **优化效果**: 减少网络传输和磁盘I/O

### 调度优化配置
- **taskLocalityPreferences**: 任务位置偏好
- **shuffleDepId**: shuffle依赖标识
- **调度策略**: 数据本地化和机架感知

## 补充分析

### 系统集成
- 与Stage类紧密集成，提供信息转换
- 通过SparkListenerBus进行事件分发
- 与Web UI组件协同工作

### 性能影响
- 信息封装增加内存使用
- 状态更新引入同步开销
- 事件传递影响系统响应

### 容错机制
- 支持阶段失败状态记录
- 提供失败原因诊断信息
- 与重试机制协同工作

### 扩展建议
- 可以添加更细粒度的性能指标
- 支持动态配置参数调整
- 增强跨阶段的数据共享

## 总结

`StageInfo` 是Spark调度系统中阶段信息管理的核心组件，为阶段的生命周期监控、状态跟踪和性能分析提供了标准化的数据接口。其设计充分考虑了信息完整性、状态管理和优化支持等关键需求，通过合理的属性封装和方法设计，确保了Spark阶段信息的可靠传递和有效利用。作为Spark事件系统和监控体系的重要数据载体，StageInfo在作业执行的可观测性和可管理性中发挥着关键作用。