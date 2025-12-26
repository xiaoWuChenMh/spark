# Partitioner 源码分析

## 类的概述和定义

`Partitioner` 是 Apache Spark 中负责数据分区策略的核心组件。它定义了如何将键值对 RDD 中的元素按照键进行分区，确保相同键的数据被分配到同一个分区中，这对于 Shuffle 操作和聚合计算至关重要。

### 主要组件结构

1. **Partitioner抽象类**：分区器的基类，定义基本接口
2. **Partitioner伴生对象**：提供默认分区器选择和工具方法
3. **HashPartitioner类**：基于哈希的分区器实现
4. **PartitionIdPassthrough类**：分区ID直通分区器
5. **ConstantPartitioner类**：常量分区器（所有数据到一个分区）
6. **RangePartitioner类**：基于范围的分区器，支持排序
7. **RangePartitioner伴生对象**：提供范围分区器的工具方法

## 构造函数参数说明

### Partitioner 抽象类
```scala
abstract class Partitioner extends Serializable
```
- 无显式构造函数参数
- 继承 `Serializable` 确保可序列化

### HashPartitioner 构造函数
```scala
class HashPartitioner(partitions: Int) extends Partitioner
```
- `partitions`：分区数量，必须为非负数

### RangePartitioner 构造函数
```scala
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20)
```
- `partitions`：目标分区数量
- `rdd`：用于采样确定范围的RDD
- `ascending`：排序方向，默认为升序
- `samplePointsPerPartitionHint`：每个分区的采样点数提示，默认为20

### PartitionIdPassthrough 构造函数
```scala
private[spark] class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner
```
- `numPartitions`：分区数量

### ConstantPartitioner 构造函数
```scala
private[spark] class ConstantPartitioner extends Partitioner
```
- 无参数，固定返回1个分区

## 核心属性分析

### Partitioner 抽象类核心属性

1. **numPartitions**: `Int`
   - 抽象属性，必须由子类实现
   - 返回分区器的分区总数

2. **getPartition**: `(key: Any) => Int`
   - 抽象方法，必须由子类实现
   - 根据键计算分区ID（0到numPartitions-1）

### HashPartitioner 核心属性

1. **partitions**: `Int`
   - 分区数量，通过构造函数传入
   - 使用 `require` 验证非负性

2. **哈希算法**：
   - 使用 `Utils.nonNegativeMod(key.hashCode, numPartitions)`
   - 处理 null 键的特殊情况（返回分区0）

### RangePartitioner 核心属性

1. **rangeBounds**: `Array[K]`
   - 存储前 `numPartitions-1` 个分区的上界
   - 通过采样和权重计算确定

2. **ordering**: `Ordering[K]`
   - 键的排序规则，通过隐式参数获取

3. **binarySearch**: `(Array[K], K) => Int`
   - 二分查找函数，根据分区数量动态选择
   - 小分区使用线性搜索，大分区使用二分搜索

4. **ascending**: `Boolean`
   - 排序方向，影响分区分配顺序

## 主要方法分类和说明

### 分区器选择方法

#### defaultPartitioner 方法
```scala
def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner
```
**功能**：为cogroup-like操作选择合适的分区器

**算法步骤**：
1. 收集所有RDD并检查已有分区器
2. 选择具有最多分区的现有分区器
3. 计算默认分区数（spark.default.parallelism或最大分区数）
4. 根据条件选择使用现有分区器或创建新的HashPartitioner

**关键逻辑**：
```scala
if (hasMaxPartitioner.nonEmpty && 
    (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
     defaultNumPartitions <= hasMaxPartitioner.get.getNumPartitions)) {
  hasMaxPartitioner.get.partitioner.get
} else {
  new HashPartitioner(defaultNumPartitions)
}
```

#### isEligiblePartitioner 方法
```scala
private def isEligiblePartitioner(hasMaxPartitioner: RDD[_], rdds: Seq[RDD[_]]): Boolean
```
**功能**：判断现有分区器是否合格

**算法**：比较最大分区数与现有分区器分区数的对数差是否小于1

### 哈希分区方法

#### HashPartitioner.getPartition
```scala
def getPartition(key: Any): Int = key match {
  case null => 0
  case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
}
```
**特点**：
- 简单高效的哈希分配
- 处理null键的特殊情况
- 使用非负模运算确保结果在有效范围内

### 范围分区方法

#### RangePartitioner 初始化
**采样阶段**：
1. 计算采样大小：`min(samplePointsPerPartitionHint * partitions, 1e6)`
2. 每个分区采样：`ceil(3.0 * sampleSize / rdd.partitions.length)`
3. 使用蓄水池采样算法获取代表性样本

**范围确定阶段**：
1. 计算总权重和步长
2. 按权重累积确定分区边界
3. 跳过重复值避免空分区

#### RangePartitioner.getPartition
```scala
def getPartition(key: Any): Int
```
**搜索策略**：
- 分区数 ≤ 128：使用线性搜索
- 分区数 > 128：使用二分搜索
- 根据排序方向调整分区分配

### 工具方法

#### RangePartitioner.sketch
```scala
def sketch[K : ClassTag](rdd: RDD[K], sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])])
```
**功能**：通过蓄水池采样对输入RDD进行草图绘制

#### RangePartitioner.determineBounds
```scala
def determineBounds[K : Ordering : ClassTag](candidates: ArrayBuffer[(K, Float)], partitions: Int): Array[K]
```
**功能**：根据带权重的候选键确定分区边界

## 设计特点总结

### 1. 抽象层次设计
- `Partitioner` 抽象类定义统一接口
- 具体实现类针对不同场景优化
- 支持用户自定义分区器

### 2. 性能优化策略

#### 动态搜索算法选择
```scala
if (rangeBounds.length <= 128) {
  // 线性搜索：小数据量更高效
  while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
    partition += 1
  }
} else {
  // 二分搜索：大数据量更高效
  partition = binarySearch(rangeBounds, k)
}
```

#### 采样优化
- 自适应采样大小控制
- 处理数据倾斜的分区重采样
- 权重计算确保代表性

### 3. 容错性设计
- null键安全处理
- 分区数验证（非负检查）
- 空RDD边界情况处理

### 4. 序列化支持
- 所有分区器实现Serializable
- RangePartitioner支持自定义序列化
- 兼容不同序列化器（JavaSerializer vs Kryo）

## 算法实现细节

### 哈希分区算法
**核心公式**：`partition = hash(key) % numPartitions`

**优化点**：
- 使用 `Utils.nonNegativeMod` 避免负数结果
- 简单的模运算确保均匀分布

### 范围分区算法

#### 蓄水池采样算法
```scala
val (sample, n) = SamplingUtils.reservoirSampleAndCount(iter, sampleSizePerPartition, seed)
```
**特点**：
- 单次遍历即可完成采样
- 保证每个元素被选中的概率相等
- 适合大数据流场景

#### 边界确定算法
```scala
val step = sumWeights / partitions
var cumWeight = 0.0
var target = step
```
**过程**：
1. 按键排序候选样本
2. 累积权重直到达到目标步长
3. 设置分区边界并跳过重复值

### 默认分区器选择算法
**决策树**：
1. 是否存在现有分区器？
2. 现有分区器是否合格？（分区数在合理范围内）
3. 默认分区数是否小于等于现有分区器分区数？
4. 根据条件选择重用或创建新分区器

## 配置参数说明

### 核心配置参数

1. **spark.default.parallelism**
   - 默认并行度（分区数）
   - 影响默认分区器的选择

2. **采样相关参数**
   - `samplePointsPerPartitionHint`：每个分区的采样点提示数
   - 影响范围分区器的精度和性能

### 性能调优参数

1. **分区数量选择**
   - 太少：可能导致数据倾斜
   - 太多：增加Shuffle开销
   - 理想值：集群核心数的2-3倍

2. **采样大小权衡**
   - 大样本：更准确的分区边界，但计算开销大
   - 小样本：计算快，但可能导致数据倾斜

## 使用场景分析

### HashPartitioner 适用场景
- 键分布相对均匀
- 不需要排序功能
- 性能要求高，计算简单
- 大多数常规Shuffle操作

### RangePartitioner 适用场景
- 需要按键排序
- 键分布不均匀，存在数据倾斜
- 支持范围查询操作
- 如：sortByKey、repartitionAndSortWithinPartitions

### 特殊分区器场景

#### PartitionIdPassthrough
- 分区ID已预计算的情况
- 避免重复的分区计算
- 如：某些优化后的Shuffle操作

#### ConstantPartitioner
- 所有数据需要集中处理
- 小数据量全局聚合
- 测试和调试场景

## 错误处理和边界情况

### 键处理特殊情况
1. **null键处理**：分配到分区0
2. **数组键警告**：Java数组hashCode基于身份而非内容
3. **自定义对象**：需要正确实现hashCode和equals

### 分区数边界情况
1. **分区数为0**：空RDD的特殊情况
2. **分区数为1**：ConstantPartitioner的简化版
3. **超大分区数**：可能影响性能需要监控

## 性能优化建议

### 1. 分区器选择策略
- 根据数据特性和操作需求选择合适的分区器
- 重用现有分区器减少Shuffle开销
- 考虑数据倾斜程度选择哈希或范围分区

### 2. 参数调优
- 合理设置默认并行度
- 根据数据量调整采样大小
- 监控分区均匀性及时调整策略

### 3. 自定义分区器
- 针对特定数据模式优化
- 实现高效的hashCode和equals方法
- 考虑数据本地性优化

## 总结

`Partitioner` 是Spark数据分布策略的核心，通过精心的设计实现了：

1. **灵活性**：支持多种分区策略满足不同场景需求
2. **高性能**：动态算法选择和优化实现
3. **可扩展性**：清晰的抽象层次支持自定义扩展
4. **健壮性**：完善的错误处理和边界情况处理

该组件的设计体现了Spark在大规模数据处理中对性能、灵活性和可靠性的平衡考虑，是学习分布式计算数据分布策略的优秀案例。