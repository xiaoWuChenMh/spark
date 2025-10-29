# ResourceProfileManager 类分析

## 类的概述和定义

`ResourceProfileManager` 是 Spark 资源管理系统的核心管理器类，负责统一管理所有的 `ResourceProfile` 实例。它通过 ID 映射的方式存储和检索 ResourceProfile，为整个 Spark 应用提供资源配置的集中管理服务。

**类定义签名：**
```scala
@Evolving
private[spark] class ResourceProfileManager(
    sparkConf: SparkConf,
    listenerBus: LiveListenerBus) extends Logging
```

**设计目的：**
- **集中管理**：统一管理所有 ResourceProfile 实例
- **ID 映射**：通过 ID 快速查找 ResourceProfile，节省存储空间
- **线程安全**：支持多线程环境下的并发访问
- **集群适配**：处理不同集群管理器的兼容性问题
- **事件通知**：通过事件总线通知 ResourceProfile 的变更

## 构造函数参数说明

### 1. sparkConf: SparkConf
- **作用**：Spark 配置对象，提供运行时配置信息
- **用途**：
  - 获取集群管理器类型
  - 检查动态分配配置
  - 创建默认 ResourceProfile
  - 验证资源配置的兼容性

### 2. listenerBus: LiveListenerBus
- **作用**：Spark 事件总线，用于发布资源相关事件
- **事件类型**：`SparkListenerResourceProfileAdded`
- **通知机制**：当添加新 ResourceProfile 时通知监听器

## 核心属性分析

### 1. 资源配置文件存储
```scala
private val resourceProfileIdToResourceProfile = new HashMap[Int, ResourceProfile]()
```

**数据结构设计：**
- **存储类型**：可变 HashMap，键为 Profile ID，值为 ResourceProfile
- **ID 映射**：通过 ID 快速查找，避免存储重复的 Profile 对象
- **内存优化**：假设 Profile 数量较少，不会造成显著内存开销

### 2. 读写锁机制
```scala
private val (readLock, writeLock) = {
  val lock = new ReentrantReadWriteLock()
  (lock.readLock(), lock.writeLock())
}
```

**并发控制策略：**
- **读写分离**：读操作共享，写操作互斥
- **性能优化**：支持多个线程同时读取
- **线程安全**：确保并发访问的数据一致性

### 3. 默认资源配置文件
```scala
private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
addResourceProfile(defaultProfile)
```

**默认 Profile 管理：**
- **自动创建**：启动时自动创建基于应用配置的默认 Profile
- **自动注册**：将默认 Profile 添加到管理器中
- **单例保证**：通过 ResourceProfile 伴生对象确保单例

### 4. 集群环境检测属性
```scala
private val dynamicEnabled = Utils.isDynamicAllocationEnabled(sparkConf)
private val master = sparkConf.getOption("spark.master")
private val isYarn = master.isDefined && master.get.equals("yarn")
private val isK8s = master.isDefined && master.get.startsWith("k8s://")
private val isStandaloneOrLocalCluster = master.isDefined && (
    master.get.startsWith("spark://") || master.get.startsWith("local-cluster")
)
```

**环境检测：**
- **动态分配**：检查是否启用动态资源分配
- **集群类型**：识别 YARN、Kubernetes、Standalone 等集群管理器
- **测试环境**：区分测试环境和生产环境

## 主要方法分类和说明

### 1. 资源支持性验证方法

#### isSupported(rp: ResourceProfile): Boolean
```scala
private[spark] def isSupported(rp: ResourceProfile): Boolean
```

**验证逻辑：**
- **TaskResourceProfile 检查**：动态分配禁用时仅支持 Standalone
- **非默认 Profile 检查**：仅支持 YARN、K8s 和动态分配启用的 Standalone
- **异常处理**：不支持的配置抛出 `SparkException`
- **警告机制**：Standalone 集群缺少 Executor 核心配置时发出警告

**支持矩阵：**
| Profile 类型 | YARN | K8s | Standalone (动态分配) | Standalone (静态分配) |
|-------------|------|-----|---------------------|---------------------|
| 默认 Profile | ✅ | ✅ | ✅ | ✅ |
| 自定义 Profile | ✅ | ✅ | ✅ | ❌ |
| TaskResourceProfile | ❌ | ❌ | ❌ | ✅ |

### 2. 任务调度验证方法

#### canBeScheduled(taskRpId: Int, executorRpId: Int): Boolean
```scala
private[spark] def canBeScheduled(taskRpId: Int, executorRpId: Int): Boolean
```

**调度规则：**
1. **Profile ID 匹配**：任务和 Executor 的 Profile ID 相同
2. **静态分配特殊规则**：动态分配禁用时，TaskResourceProfile 可调度到默认 Profile 的 Executor
3. **动态分配规则**：Profile ID 必须精确匹配

**验证逻辑：**
```scala
taskRpId == executorRpId || (!dynamicEnabled && taskRp.isInstanceOf[TaskResourceProfile])
```

### 3. ResourceProfile 管理方法

#### addResourceProfile(rp: ResourceProfile): Unit
```scala
def addResourceProfile(rp: ResourceProfile): Unit
```

**添加流程：**
1. **支持性验证**：调用 `isSupported` 检查配置兼容性
2. **写锁保护**：获取写锁确保线程安全
3. **唯一性检查**：检查是否已存在相同 ID 的 Profile
4. **事件通知**：新 Profile 添加成功后发布事件
5. **预计算优化**：强制计算限制性资源，避免运行时开销

**线程安全实现：**
```scala
writeLock.lock()
try {
  if (!resourceProfileIdToResourceProfile.contains(rp.id)) {
    val prev = resourceProfileIdToResourceProfile.put(rp.id, rp)
    if (prev.isEmpty) putNewProfile = true
  }
} finally {
  writeLock.unlock()
}
```

#### resourceProfileFromId(rpId: Int): ResourceProfile
```scala
def resourceProfileFromId(rpId: Int): ResourceProfile
```

**查找逻辑：**
- **读锁保护**：获取读锁支持并发读取
- **ID 查找**：通过 ID 快速查找 ResourceProfile
- **异常处理**：未找到 Profile 时抛出 `SparkException`
- **默认回退**：注释说明可回退到默认 Profile（但实际实现中抛出异常）

#### getEquivalentProfile(rp: ResourceProfile): Option[ResourceProfile]
```scala
def getEquivalentProfile(rp: ResourceProfile): Option[ResourceProfile]
```

**等效性查找：**
- **资源比较**：使用 `resourcesEqual` 方法比较资源配置
- **性能优化**：避免创建重复的等效 Profile
- **返回选项**：返回 `Option[ResourceProfile]` 处理查找结果

## 设计特点总结

### 1. 线程安全设计
**读写锁策略：**
- **读操作**：共享锁，支持高并发读取
- **写操作**：排他锁，确保数据一致性
- **锁粒度**：方法级别锁，平衡性能和安全性

**锁使用模式：**
```scala
readLock.lock()
try {
  // 读操作
} finally {
  readLock.unlock()
}
```

### 2. 集群管理器适配
**多环境支持：**
- **YARN/K8s**：完整支持自定义 ResourceProfile
- **Standalone**：动态分配启用时支持自定义 Profile
- **静态分配**：仅支持 TaskResourceProfile
- **本地集群**：特殊处理 local-cluster 模式

**配置验证：**
- **前置检查**：在添加 Profile 前验证兼容性
- **详细错误**：提供明确的错误信息和修复建议
- **警告机制**：对潜在问题发出警告而非直接失败

### 3. 性能优化策略
**ID 映射优化：**
- **空间节省**：通过 ID 引用而非完整对象
- **快速查找**：HashMap 提供 O(1) 查找性能
- **内存控制**：假设 Profile 数量有限，不会造成内存压力

**预计算优化：**
```scala
// 强制计算限制性资源，避免运行时开销
rp.limitingResource(sparkConf)
```

### 4. 事件驱动架构
**事件通知：**
- **监听器模式**：通过 LiveListenerBus 发布事件
- **实时通知**：新 Profile 添加时立即通知相关组件
- **扩展性**：支持多个监听器订阅资源变更事件

**事件类型：**
```scala
listenerBus.post(SparkListenerResourceProfileAdded(rp))
```

## 使用场景分析

### 1. 应用启动阶段
**默认 Profile 初始化：**
```scala
// 创建管理器时自动初始化默认 Profile
val manager = new ResourceProfileManager(sparkConf, listenerBus)
// 默认 Profile 已自动添加并注册
```

### 2. 阶段资源配置
**自定义 Profile 添加：**
```scala
// 为特定阶段创建自定义资源配置
val customProfile = new ResourceProfileBuilder()
  .require(executorRequests)
  .require(taskRequests)
  .build()

// 添加并验证 Profile
manager.addResourceProfile(customProfile)
```

### 3. 任务调度阶段
**调度验证：**
```scala
// 检查任务是否可以调度到指定 Executor
val canSchedule = manager.canBeScheduled(taskProfileId, executorProfileId)
if (canSchedule) {
  // 执行任务调度
}
```

### 4. 资源查询阶段
**Profile 查找：**
```scala
// 通过 ID 快速查找 Profile
val profile = manager.resourceProfileFromId(profileId)

// 查找等效 Profile 避免重复创建
val equivalent = manager.getEquivalentProfile(newProfile)
val finalProfile = equivalent.getOrElse {
  manager.addResourceProfile(newProfile)
  newProfile
}
```

## 错误处理和验证

### 1. 配置兼容性验证
**验证场景：**
- **集群类型不匹配**：非 YARN/K8s 集群使用自定义 Profile
- **动态分配禁用**：静态分配下使用非 TaskResourceProfile
- **配置缺失**：Standalone 集群缺少 Executor 核心配置

**错误信息示例：**
```scala
throw new SparkException("ResourceProfiles are only supported on YARN and Kubernetes " +
  "and Standalone with dynamic allocation enabled.")
```

### 2. 运行时状态检查
**断言验证：**
```scala
assert(resourceProfileIdToResourceProfile.contains(taskRpId) &&
  resourceProfileIdToResourceProfile.contains(executorRpId),
  "Tasks and executors must have valid resource profile id")
```

**异常处理：**
```scala
resourceProfileIdToResourceProfile.getOrElse(rpId,
  throw new SparkException(s"ResourceProfileId $rpId not found!")
)
```

## 扩展性设计

### 1. 新集群管理器支持
**扩展模式：**
- 添加新的集群类型检测逻辑
- 更新 `isSupported` 方法的支持矩阵
- 保持向后兼容性

### 2. 新 Profile 类型支持
**类型扩展：**
- 通过继承 ResourceProfile 创建新类型
- 在 `canBeScheduled` 中添加新的调度规则
- 保持现有功能的完整性

### 3. 事件系统扩展
**新事件类型：**
- 定义新的 SparkListener 事件
- 在相应操作中发布新事件
- 支持更细粒度的资源变更通知

## 性能考虑

### 1. 并发性能
**读写锁优势：**
- **读多写少**：Profile 查找频繁，添加操作较少
- **高并发读取**：多个任务可同时查询 Profile 信息
- **写操作安全**：添加操作确保数据一致性

### 2. 内存效率
**存储优化：**
- **ID 映射**：减少重复 Profile 对象的存储
- **对象复用**：通过等效查找避免重复创建
- **缓存策略**：预计算限制性资源减少运行时开销

### 3. 计算效率
**算法优化：**
- **哈希查找**：HashMap 提供高效查找
- **延迟计算**：复杂计算按需执行
- **批量操作**：支持批量 Profile 管理

## 实际应用示例

### 1. 多阶段资源优化
```scala
// 为不同阶段创建不同的资源配置
val stage1Profile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests().cores(2).memory("4g"))
  .require(new TaskResourceRequests().cpus(1))
  .build()

val stage2Profile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests().cores(4).memory("8g"))
  .require(new TaskResourceRequests().cpus(2))
  .build()

// 注册到管理器
manager.addResourceProfile(stage1Profile)
manager.addResourceProfile(stage2Profile)
```

### 2. GPU 资源管理
```scala
// GPU 密集型任务配置
val gpuProfile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests()
    .cores(8)
    .memory("16g")
    .resource("gpu", 2, "/scripts/gpu-discovery.sh"))
  .require(new TaskResourceRequests()
    .cpus(1)
    .resource("gpu", 0.5))  // 2个任务共享1个GPU
  .build()

// 验证并添加 GPU Profile
if (manager.isSupported(gpuProfile)) {
  manager.addResourceProfile(gpuProfile)
}
```

### 3. 动态分配场景
```scala
// 动态分配启用时的 Profile 管理
if (Utils.isDynamicAllocationEnabled(sparkConf)) {
  // 支持完整的自定义 Profile
  val customProfile = // ... 创建自定义配置
  manager.addResourceProfile(customProfile)
} else {
  // 仅支持 TaskResourceProfile
  val taskProfile = new TaskResourceProfile(taskResources)
  manager.addResourceProfile(taskProfile)
}
```

ResourceProfileManager 通过精心的设计实现了资源管理的集中化、线程安全和集群适配，为 Spark 应用的资源调度提供了可靠的基础设施支持。