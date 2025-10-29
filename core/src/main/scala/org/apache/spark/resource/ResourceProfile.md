# ResourceProfile 类分析

## 类的概述和定义

`ResourceProfile` 是 Spark 资源管理系统的核心类，用于定义和管理 RDD 的资源需求配置。它允许用户在阶段级别为 RDD 指定 Executor 和 Task 的资源需求，实现不同阶段间的资源需求变化。

**类定义签名：**
```scala
@Evolving
@Since("3.1.0")
class ResourceProfile(
    val executorResources: Map[String, ExecutorResourceRequest],
    val taskResources: Map[String, TaskResourceRequest]) extends Serializable with Logging
```

**设计原则：**
- **不可变性**：构建后不可修改，确保配置一致性
- **构建器模式**：用户应使用 `ResourceProfileBuilder` 构建实例
- **序列化支持**：支持分布式环境传输

## 构造函数参数说明

### 1. executorResources: Map[String, ExecutorResourceRequest]
- **作用**：Executor 级别的资源请求映射
- **键**：资源名称（如 cores, memory, GPU）
- **值**：`ExecutorResourceRequest` 对象
- **重要性**：定义每个 Executor 的资源配额

### 2. taskResources: Map[String, TaskResourceRequest]
- **作用**：Task 级别的资源请求映射
- **键**：资源名称（如 cpus, GPU）
- **值**：`TaskResourceRequest` 对象
- **重要性**：定义每个 Task 的资源需求

## 核心属性分析

### 内部状态属性
```scala
private var _id = ResourceProfile.getNextProfileId
private var _executorResourceSlotsPerAddr: Option[Map[String, Int]] = None
private var _limitingResource: Option[String] = None
private var _maxTasksPerExecutor: Option[Int] = None
private var _coresLimitKnown: Boolean = false
```

**属性说明：**
- `_id`：资源配置文件的唯一标识符（测试目的可修改）
- `_executorResourceSlotsPerAddr`：每个资源地址的插槽数（延迟计算）
- `_limitingResource`：限制性资源名称（延迟计算）
- `_maxTasksPerExecutor`：每个 Executor 的最大任务数（延迟计算）
- `_coresLimitKnown`：CPU 核心限制是否已知的标志

### 公共访问属性
```scala
def id: Int = _id
def taskResourcesJMap: JMap[String, TaskResourceRequest] = taskResources.asJava
def executorResourcesJMap: JMap[String, ExecutorResourceRequest] = executorResources.asJava
```

**设计特点：**
- **只读访问**：确保不可变性
- **Java 兼容性**：提供 Java Map 接口
- **延迟计算**：复杂属性按需计算

## 资源访问方法分析

### Executor 资源获取方法
```scala
private[spark] def getExecutorCores: Option[Int]
private[spark] def getPySparkMemory: Option[Long]
private[spark] def getExecutorMemory: Option[Long]
```

**方法特点：**
- **内部使用**：`private[spark]` 限制为 Spark 内部使用
- **Option 返回**：处理资源可能不存在的情况
- **类型转换**：自动进行数值类型转换

### 自定义资源过滤
```scala
private[spark] def getCustomTaskResources(): Map[String, TaskResourceRequest]
protected[spark] def getCustomExecutorResources(): Map[String, ExecutorResourceRequest]
```

**过滤逻辑：**
- **任务资源**：过滤掉 CPU 资源，只返回自定义资源
- **Executor 资源**：过滤掉所有支持的 Executor 资源，返回自定义资源
- **扩展性**：支持任意类型的自定义资源

## 核心算法分析

### 资源调度数量计算
```scala
private[spark] def getSchedulerTaskResourceAmount(resource: String): Int
private[spark] def getNumSlotsPerAddress(resource: String, sparkConf: SparkConf): Int
```

**小数资源处理：**
- **amount < 1**：表示多个任务共享同一资源地址
- **转换规则**：0.25 → 4个任务共享，调度时amount=1
- **地址分配**：`["0", "0", "0", "0"]` 表示地址0被4个任务共享

### 最大任务数计算
```scala
private[spark] def maxTasksPerExecutor(sparkConf: SparkConf): Int
private[spark] def limitingResource(sparkConf: SparkConf): String
```

**计算逻辑：**
- **限制性资源**：资源配额与任务需求比值最小的资源
- **示例**：Executor有4CPU和2GPU，Task需要1CPU和1GPU → GPU是限制性资源，最大任务数=2
- **延迟计算**：使用 `getOrElse` 触发首次计算

### 核心限制检查
```scala
private def shouldCheckExecutorCores(sparkConf: SparkConf): Boolean
```

**集群管理器适配：**
- **YARN/K8s**：必须检查 Executor 核心配置
- **Standalone**：可能不设置核心配置，使用所有可用核心
- **默认值**：仅对 YARN 和 K8s 应用默认配置

## 核心算法：calculateTasksAndLimitingResource

### 算法流程概述
```scala
private def calculateTasksAndLimitingResource(sparkConf: SparkConf): Unit = synchronized
```

**同步保护：**使用 `synchronized` 确保线程安全，避免重复计算

### CPU 资源计算阶段
```scala
val shouldCheckExecCores = shouldCheckExecutorCores(sparkConf)
var (taskLimit, limitingResource) = if (shouldCheckExecCores) {
  val cpusPerTask = // 从配置或Profile获取
  val coresPerExecutor = // 从配置或Profile获取
  val tasksBasedOnCores = coresPerExecutor / cpusPerTask
  (tasksBasedOnCores, ResourceProfile.CPUS)
} else {
  (-1, "")
}
```

**验证逻辑：**
- **CPU 验证**：`ResourceUtils.validateTaskCpusLargeEnough` 确保配置合理
- **默认处理**：未知时设为-1，后续基于其他资源计算

### 自定义资源计算阶段
```scala
execResourceToCheck.foreach { case (rName, execReq) =>
  val taskReq = taskResources.get(rName).map(_.amount).getOrElse(0.0)
  if (taskReq > 0.0) {
    val (numPerTask, parts) = ResourceUtils.calculateAmountAndPartsForFraction(taskReq)
    val numTasks = ((execReq.amount * parts) / numPerTask).toInt
    if (taskLimit == -1 || numTasks < taskLimit) {
      limitingResource = rName
      taskLimit = numTasks
    }
  }
}
```

**小数资源处理：**
- **parts 计算**：将小数转换为整数比例（0.25 → parts=4）
- **任务数计算**：`(executor_amount × parts) / task_amount`
- **限制性更新**：选择最小的任务数作为限制

### 验证和错误处理
```scala
if (taskResourcesToCheck.nonEmpty) {
  throw new SparkException("No executor resource configs were specified...")
}
ResourceUtils.warnOnWastedResources(this, sparkConf)
```

**完整性检查：**确保所有任务资源都有对应的 Executor 配置
**资源浪费警告：**检测可能存在的资源分配浪费

## TaskResourceProfile 子类分析

### 类定义和用途
```scala
@Evolving
@Since("3.4.0")
private[spark] class TaskResourceProfile(
    override val taskResources: Map[String, TaskResourceRequest])
  extends ResourceProfile(Map.empty, taskResources)
```

**设计目的：**
- **动态分配禁用**：基于任务资源调度到默认配置的 Executor
- **动态分配启用**：为Profile创建新的Executor，仅在此类Executor上调度任务
- **简化配置**：只关注任务资源，Executor使用默认或动态配置

### 自定义Executor资源适配
```scala
override protected[spark] def getCustomExecutorResources(): Map[String, ExecutorResourceRequest] = {
  if (SparkEnv.get == null) {
    return super.getCustomExecutorResources()
  }
  val sparkConf = SparkEnv.get.conf
  if (!Utils.isDynamicAllocationEnabled(sparkConf)) {
    ResourceProfile.getOrCreateDefaultProfile(sparkConf).getCustomExecutorResources()
  } else {
    super.getCustomExecutorResources()
  }
}
```

**环境适配逻辑：**
- **Standalone Master**：SparkEnv未初始化时使用父类逻辑
- **动态分配禁用**：使用默认Profile的Executor资源
- **动态分配启用**：使用空的Executor资源（触发新Executor创建）

## 伴生对象分析

### 资源常量定义
```scala
// Task resources
val CPUS = "cpus"

// Executor resources  
val CORES = "cores"
val MEMORY = "memory"
val OFFHEAP_MEM = "offHeap"
val OVERHEAD_MEM = "memoryOverhead"
val PYSPARK_MEM = "pyspark.memory"
```

**资源分类：**
- **任务资源**：CPUS（CPU核心数）
- **Executor资源**：5种内置资源类型
- **扩展性**：`allSupportedExecutorResources` 返回所有内置资源

### 配置文件标识符
```scala
val UNKNOWN_RESOURCE_PROFILE_ID = -1
val DEFAULT_RESOURCE_PROFILE_ID = 0
private lazy val nextProfileId = new AtomicInteger(0)
```

**ID管理：**
- **默认Profile**：ID=0
- **未知Profile**：ID=-1
- **ID生成**：原子计数器确保唯一性

### 默认Profile管理
```scala
@GuardedBy("DEFAULT_PROFILE_LOCK")
private var defaultProfile: Option[ResourceProfile] = None

private[spark] def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile
```

**单例模式：**
- **线程安全**：使用同步锁保护
- **延迟创建**：首次访问时创建
- **配置驱动**：基于Spark配置构建默认Profile

### 默认资源配置构建
```scala
private def getDefaultTaskResources(conf: SparkConf): Map[String, TaskResourceRequest]
private def getDefaultExecutorResources(conf: SparkConf): Map[String, ExecutorResourceRequest]
```

**构建逻辑：**
- **任务资源**：基于 `spark.task.cpus` 配置
- **Executor资源**：复杂的集群管理器适配逻辑
- **自定义资源**：解析 `spark.executor.resource.*` 配置

### 集群管理器适配
```scala
private[spark] def getResourcesForClusterManager(
    rpId: Int,
    execResources: Map[String, ExecutorResourceRequest],
    overheadFactor: Double,
    conf: SparkConf,
    isPythonApp: Boolean,
    resourceMappings: Map[String, String]): ExecutorResourcesOrDefaults
```

**资源映射：**
- **默认Profile**：使用默认资源配置
- **自定义Profile**：覆盖默认值，保留未指定资源的默认值
- **Python应用**：特殊处理PySpark内存需求
- **资源重命名**：支持集群管理器的资源名称映射

## 设计特点总结

### 1. 不可变性和线程安全
**设计原则：**
- 所有公共属性为val，确保不可变性
- 复杂计算使用同步保护
- 延迟初始化避免不必要的计算

**并发控制：**
- `synchronized` 保护核心算法
- `@GuardedBy` 注解明确锁依赖
- 原子计数器管理ID生成

### 2. 集群管理器适配
**多环境支持：**
- YARN/K8s：完整的资源配置支持
- Standalone：自适应核心分配
- 本地集群：特殊配置处理

**配置回退：**
- 自定义Profile未指定的资源使用默认值
- 确保所有必要的资源都有配置
- 支持渐进式的资源配置

### 3. 资源调度优化
**限制性资源识别：**
- 自动识别最紧缺的资源类型
- 基于资源比例计算最大并行度
- 优化集群资源利用率

**小数资源支持：**
- 创新的资源共享机制
- 支持资源时分复用
- 提高昂贵资源的使用效率

### 4. 错误处理和验证
**防御性编程：**
- 全面的参数验证
- 资源配置完整性检查
- 资源浪费检测和警告

**详细错误信息：**
- 明确的异常消息
- 包含具体的资源名称和配置值
- 便于问题诊断和修复

## 使用场景示例

### 基本资源配置
```scala
// 创建自定义资源配置文件
val profile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests()
    .cores(4)
    .memory("8g")
    .resource("gpu", 2, "/scripts/gpu-discovery.sh"))
  .require(new TaskResourceRequests()
    .cpus(1)
    .resource("gpu", 0.5))  // 2个任务共享1个GPU
  .build
```

### 动态分配场景
```scala
// 只指定任务资源，Executor动态分配
val taskProfile = new TaskResourceProfile(
  Map("cpus" -> new TaskResourceRequest("cpus", 2))
)
```

### 集群管理器集成
```scala
// 获取集群管理器所需的资源配置
val resources = ResourceProfile.getResourcesForClusterManager(
  profile.id, profile.executorResources, 0.1, sparkConf, false, Map.empty
)
```

## 性能优化考虑

### 延迟计算策略
**计算开销优化：**
- 复杂属性按需计算
- 避免不必要的重复计算
- 计算结果缓存复用

**内存效率：**
- 使用Option避免空值存储
- 懒加载减少初始化开销
- 对象复用减少GC压力

### 算法复杂度
**计算效率：**
- 资源计算为O(n)复杂度
- 使用哈希映射快速查找
- 避免嵌套循环和重复计算

## 扩展性设计

### 自定义资源支持
**灵活扩展：**
- 通过资源名称动态支持新资源类型
- 不限制特定的资源类别
- 支持任意的资源地址格式

### 插件化架构
**资源发现：**与ResourceDiscoveryPlugin集成
**集群适配：**通过资源映射支持不同管理器
**配置驱动：**基于Spark配置自动适配

这个类体现了Spark资源管理系统的高度复杂性和成熟度，通过精心的设计实现了灵活性、性能和易用性的平衡。