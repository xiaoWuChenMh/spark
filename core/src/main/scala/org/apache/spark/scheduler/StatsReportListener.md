# StatsReportListener.scala 分析文档

## 概述
`StatsReportListener` 是Spark调度系统中一个功能强大的统计报告监听器，继承自`SparkListener`和`Logging`。它专门用于收集和分析任务执行的性能指标，在阶段完成时生成详细的统计报告，包括任务运行时间、shuffle操作、I/O性能等各方面的分布统计和百分比分析。该监听器为性能调优和故障诊断提供了重要的数据支持。

## 类定义
```scala
@DeveloperApi
class StatsReportListener extends SparkListener with Logging
```

## 核心属性

### taskInfoMetrics: Buffer[(TaskInfo, TaskMetrics)]
```scala
private val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()
```
- **访问权限**: 私有可变缓冲区
- **类型**: 任务信息和度量指标的元组序列
- **用途**: 存储阶段内所有任务的执行信息

## 主要方法

### onTaskEnd方法
```scala
override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
  val info = taskEnd.taskInfo
  val metrics = taskEnd.taskMetrics
  if (info != null && metrics != null) {
    taskInfoMetrics += ((info, metrics))
  }
}
```

**功能**: 处理任务结束事件

**执行逻辑：**
1. 从任务结束事件中提取任务信息和度量指标
2. 检查信息有效性（非空检查）
3. 将有效信息添加到taskInfoMetrics缓冲区

### onStageCompleted方法
```scala
override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit
```

**功能**: 处理阶段完成事件，生成详细的统计报告

**执行流程：**

1. **阶段状态日志**
   ```scala
   this.logInfo(s"Finished stage: ${getStatusDetail(stageCompleted.stageInfo)}")
   ```
   - 记录阶段完成的基本状态信息

2. **任务运行时间分布**
   ```scala
   showMillisDistribution("task runtime:", (info, _) => info.duration, taskInfoMetrics.toSeq)
   ```
   - 分析任务执行时间的分布情况

3. **Shuffle写入统计**
   ```scala
   showBytesDistribution("shuffle bytes written:",
     (_, metric) => metric.shuffleWriteMetrics.bytesWritten, taskInfoMetrics.toSeq)
   ```
   - 统计shuffle写入的数据量分布

4. **Fetch和I/O统计**
   ```scala
   showMillisDistribution("fetch wait time:",
     (_, metric) => metric.shuffleReadMetrics.fetchWaitTime, taskInfoMetrics.toSeq)
   showBytesDistribution("remote bytes read:",
     (_, metric) => metric.shuffleReadMetrics.remoteBytesRead, taskInfoMetrics.toSeq)
   showBytesDistribution("task result size:",
     (_, metric) => metric.resultSize, taskInfoMetrics.toSeq)
   ```
   - 获取等待时间分布
   - 远程读取数据量分布
   - 任务结果大小分布

5. **运行时分解分析**
   ```scala
   val runtimePcts = taskInfoMetrics.map { case (info, metrics) =>
     RuntimePercentage(info.duration, metrics)
   }
   showDistribution("executor (non-fetch) time pct: ",
     Distribution(runtimePcts.map(_.executorPct * 100)), "%2.0f %%")
   showDistribution("fetch wait time pct: ",
     Distribution(runtimePcts.flatMap(_.fetchPct.map(_ * 100))), "%2.0f %%")
   showDistribution("other time pct: ", Distribution(runtimePcts.map(_.other * 100)), "%2.0f %%")
   ```
   - 计算执行器时间、获取等待时间和其他时间的百分比分布
   - 分析任务执行时间的组成结构

6. **数据清理**
   ```scala
   taskInfoMetrics.clear()
   ```
   - 清空当前阶段的数据，准备下一个阶段

### getStatusDetail方法
```scala
private def getStatusDetail(info: StageInfo): String
```

**功能**: 生成阶段状态的详细描述字符串

**信息包含：**
- 阶段ID和尝试号
- 阶段名称
- 状态字符串和失败原因（如果有）
- 任务数量
- 执行时间（毫秒）

## 伴生对象功能

### 统计配置常量
```scala
val percentiles = Array[Int](0, 5, 10, 25, 50, 75, 90, 95, 100)
val probabilities = percentiles.map(_ / 100.0)
val percentilesHeader = "\t" + percentiles.mkString("%\t") + "%"
```
- **百分位点**: 0%, 5%, 10%, 25%, 50%, 75%, 90%, 95%, 100%
- **概率值**: 对应的概率值数组
- **表头格式**: 用于统计输出的表头

### 时间单位常量
```scala
val seconds = 1000L
val minutes = seconds * 60
val hours = minutes * 60
```
- **时间转换**: 毫秒到秒、分钟、小时的转换因子

### 分布统计方法

#### extractDoubleDistribution方法
```scala
def extractDoubleDistribution(
  taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
  getMetric: (TaskInfo, TaskMetrics) => Double): Option[Distribution]
```
- **功能**: 从任务度量中提取双精度值分布
- **返回值**: Distribution对象的Option

#### extractLongDistribution方法
```scala
def extractLongDistribution(
  taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)],
  getMetric: (TaskInfo, TaskMetrics) => Long): Option[Distribution]
```
- **功能**: 从任务度量中提取长整型值分布
- **实现**: 通过extractDoubleDistribution转换实现

### 显示统计方法

#### showDistribution方法（多个重载版本）
```scala
def showDistribution(heading: String, d: Distribution, formatNumber: Double => String): Unit
def showDistribution(heading: String, dOpt: Option[Distribution], formatNumber: Double => String): Unit
def showDistribution(heading: String, dOpt: Option[Distribution], format: String): Unit
def showDistribution(heading: String, format: String, getMetric: (TaskInfo, TaskMetrics) => Double, taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit
```

**功能**: 显示分布统计信息

**输出格式：**
1. **统计标题**
2. **基本统计信息**（计数、均值、标准差、最大值、最小值）
3. **百分位表头**
4. **百分位数值**

#### showBytesDistribution方法
```scala
def showBytesDistribution(heading: String, getMetric: (TaskInfo, TaskMetrics) => Long, taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit
def showBytesDistribution(heading: String, dOpt: Option[Distribution]): Unit
def showBytesDistribution(heading: String, dist: Distribution): Unit
```
- **功能**: 显示字节数据分布
- **格式化**: 使用Utils.bytesToString进行人类可读格式转换

#### showMillisDistribution方法
```scala
def showMillisDistribution(heading: String, dOpt: Option[Distribution]): Unit
def showMillisDistribution(heading: String, getMetric: (TaskInfo, TaskMetrics) => Long, taskInfoMetrics: Seq[(TaskInfo, TaskMetrics)]): Unit
```
- **功能**: 显示毫秒时间分布
- **格式化**: 使用millisToString进行人类可读格式转换

### millisToString方法
```scala
def millisToString(ms: Long): String
```

**功能**: 将毫秒时间转换为可读格式

**转换规则：**
- > 小时: 显示为小时单位
- > 分钟: 显示为分钟单位
- > 秒: 显示为秒单位
- 其他: 显示为毫秒单位

## 辅助类

### RuntimePercentage类
```scala
private case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)
```

**功能**: 表示任务运行时间的百分比组成

**属性：**
- `executorPct: Double` - 执行器时间百分比
- `fetchPct: Option[Double]` - 获取等待时间百分比（可选）
- `other: Double` - 其他时间百分比

### RuntimePercentage伴生对象
```scala
private object RuntimePercentage {
  def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage
}
```

**计算逻辑：**
1. 获取总时间作为分母
2. 计算获取等待时间百分比
3. 计算执行器时间百分比（减去获取时间）
4. 计算其他时间百分比

## 设计特点

### 1. 全面的性能分析
- 覆盖任务执行的全方位指标
- 支持时间、数据量、百分比等多种统计维度
- 提供详细的分布统计和百分位分析

### 2. 智能的数据格式化
- 自动选择合适的时间单位
- 字节数据的可读格式转换
- 百分比数据的标准化显示

### 3. 模块化的统计方法
- 可重用的分布统计组件
- 支持不同类型数据的统一处理
- 灵活的指标提取和显示

### 4. 内存效率优化
- 使用缓冲区收集阶段内任务数据
- 阶段完成后及时清理数据
- 避免内存泄漏和过度占用

## 使用场景

### 1. 性能调优
- 识别任务执行瓶颈
- 分析shuffle操作性能
- 优化数据本地化和网络传输

### 2. 故障诊断
- 检测异常任务执行模式
- 分析资源使用不均衡
- 识别数据倾斜问题

### 3. 容量规划
- 评估系统资源需求
- 预测作业执行时间
- 优化集群资源配置

### 4. 基准测试
- 比较不同配置的性能差异
- 验证优化措施的效果
- 建立性能基准

## 配置参数

### 统计精度配置
- **percentiles**: 百分位点选择
- **影响**: 统计结果的精度和详细程度
- **默认**: 9个关键百分位点

### 输出格式配置
- **时间格式**: 自动选择合适的时间单位
- **字节格式**: 人类可读的字节单位转换
- **百分比格式**: 标准化百分比显示

## 补充分析

### 系统集成
- 与SparkListener事件系统紧密集成
- 利用TaskMetrics提供的丰富指标
- 与日志系统协同工作

### 性能影响
- 统计计算增加少量CPU开销
- 数据收集增加内存使用
- 日志输出增加I/O负载

### 扩展建议
- 可以添加自定义统计指标
- 支持统计结果的持久化存储
- 增强可视化报告生成

### 最佳实践
- 在生产环境中谨慎使用，避免日志过载
- 结合其他监控工具综合分析
- 定期分析统计结果进行优化

## 总结

`StatsReportListener` 是Spark调度系统中一个功能强大的性能分析工具，通过收集和分析任务执行的各种指标，为性能调优和故障诊断提供了详细的数据支持。其设计充分考虑了统计分析的全面性、数据格式的友好性和系统性能的影响，通过模块化的统计方法和智能的数据处理，确保了统计报告的质量和实用性。作为Spark开发者API的重要组成部分，StatsReportListener在性能优化和系统监控中发挥着关键作用。