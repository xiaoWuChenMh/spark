# ExecutorResourceRequests 类分析

## 类的概述和定义

`ExecutorResourceRequests` 类是 Spark 资源管理系统中用于管理多个 Executor 资源请求的容器类。它提供了一套便捷的 API 来构建复杂的资源需求配置，与 `ResourceProfile` 配合使用，在阶段级别编程式指定 RDD 所需的资源。

**类定义签名：**
```scala
class ExecutorResourceRequests() extends Serializable
```

**主要用途：**
- 管理多个 Executor 资源请求的集合
- 提供流畅的 API 接口用于构建资源配置
- 支持标准资源和自定义资源的统一管理
- 与 Java API 兼容

## 构造函数参数说明

### 无参构造函数
- **设计意图**：采用构建器模式，通过方法链式调用逐步构建资源请求
- **初始化**：内部创建空的 `ConcurrentHashMap` 用于存储资源请求

## 核心属性分析

### 1. _executorResources: ConcurrentHashMap[String, ExecutorResourceRequest]
- **类型**：线程安全的哈希映射
- **键**：资源名称（String）
- **值**：ExecutorResourceRequest 对象
- **线程安全**：使用 ConcurrentHashMap 确保并发安全
- **存储策略**：每个资源名称对应一个资源请求对象

### 2. 资源名称常量（从 ResourceProfile 导入）
- `MEMORY`：堆内存资源
- `OFFHEAP_MEM`：堆外内存资源  
- `OVERHEAD_MEM`：内存开销资源
- `PYSPARK_MEM`：PySpark 内存资源
- `CORES`：CPU 核心资源

## 主要方法分类和说明

### 1. 资源请求访问方法

#### requests: Map[String, ExecutorResourceRequest]
```scala
def requests: Map[String, ExecutorResourceRequest] = _executorResources.asScala.toMap
```
- **功能**：返回所有资源请求的不可变映射
- **转换**：将 Java ConcurrentHashMap 转换为 Scala 不可变 Map
- **线程安全**：返回的是快照，确保数据一致性

#### requestsJMap: JMap[String, ExecutorResourceRequest]
```scala
def requestsJMap: JMap[String, ExecutorResourceRequest] = requests.asJava
```
- **功能**：Java 专用的资源请求访问方法
- **兼容性**：为 Java 用户提供原生 Java Map 接口
- **转换**：将 Scala Map 转换回 Java Map

### 2. 标准资源配置方法

#### memory(amount: String): this.type
```scala
def memory(amount: String): this.type = {
  val amountMiB = JavaUtils.byteStringAsMb(amount)
  val req = new ExecutorResourceRequest(MEMORY, amountMiB)
  _executorResources.put(MEMORY, req)
  this
}
```
- **功能**：配置堆内存资源
- **参数处理**：使用 `JavaUtils.byteStringAsMb` 将字符串格式转换为 MiB
- **支持格式**："512m", "2g" 等 JVM 内存字符串格式
- **返回类型**：返回 this，支持方法链式调用

#### offHeapMemory(amount: String): this.type
- **功能**：配置堆外内存资源
- **生效条件**：仅在 `MEMORY_OFFHEAP_ENABLED` 为 true 时生效
- **用途**：用于存储序列化数据等场景

#### memoryOverhead(amount: String): this.type
- **功能**：配置内存开销资源
- **用途**：用于 YARN 等集群管理器的内存开销计算

#### pysparkMemory(amount: String): this.type
- **功能**：配置 PySpark 内存资源
- **特定用途**：为 Python 进程分配独立的内存空间

#### cores(amount: Int): this.type
```scala
def cores(amount: Int): this.type = {
  val req = new ExecutorResourceRequest(CORES, amount)
  _executorResources.put(CORES, req)
  this
}
```
- **功能**：配置每个 Executor 的 CPU 核心数
- **参数类型**：直接使用整数，无需单位转换
- **重要性**：控制 Executor 的并行度

### 3. 自定义资源配置方法

#### resource(resourceName: String, amount: Long, discoveryScript: String = "", vendor: String = ""): this.type
```scala
def resource(
    resourceName: String,
    amount: Long,
    discoveryScript: String = "",
    vendor: String = ""): this.type = {
  val req = new ExecutorResourceRequest(resourceName, amount, discoveryScript, vendor)
  _executorResources.put(resourceName, req)
  this
}
```

**参数说明：**
- `resourceName`：自定义资源名称（如 "gpu", "fpga"）
- `amount`：资源数量
- `discoveryScript`：资源发现脚本（集群管理器不支持地址发现时必需）
- `vendor`：供应商标识（Kubernetes 等特定环境需要）

**设计特点：**
- 支持任意类型的自定义资源
- 参数默认值处理：空字符串表示未设置
- 与标准资源配置保持一致的 API 风格

### 4. toString 方法
```scala
override def toString: String = {
  s"Executor resource requests: ${_executorResources}"
}
```
- **功能**：提供调试信息
- **输出格式**：显示所有资源请求的详细信息

## 设计特点总结

### 1. 构建器模式设计
- **方法链式调用**：所有配置方法返回 `this.type`，支持流畅的 API 调用
- **示例用法**：
```scala
val requests = new ExecutorResourceRequests()
  .memory("2g")
  .cores(4)
  .resource("gpu", 2, "/path/to/discovery.sh")
```

### 2. 线程安全保证
- **存储结构**：使用 `ConcurrentHashMap` 确保并发访问安全
- **访问方法**：返回不可变映射，避免外部修改内部状态

### 3. 多语言支持
- **Scala API**：原生的 Scala 集合接口
- **Java API**：专门的 `requestsJMap` 方法提供 Java 兼容性

### 4. 资源类型扩展性
- **标准资源**：预定义的内存、CPU 等常用资源
- **自定义资源**：通过 `resource` 方法支持任意资源类型
- **命名规范**：与 Spark 配置项命名保持一致

### 5. 单位转换处理
- **内存单位**：自动处理 "512m", "2g" 等格式到 MiB 的转换
- **统一标准**：内部统一使用 MiB 作为内存单位

## 配置参数说明

### 与全局配置的对应关系
| 方法名 | 对应配置项 | 说明 |
|--------|------------|------|
| memory() | spark.executor.memory | 堆内存配置 |
| offHeapMemory() | spark.executor.memoryOverhead | 堆外内存配置 |
| memoryOverhead() | spark.memory.offHeap.enabled | 内存开销配置 |
| pysparkMemory() | spark.executor.pyspark.memory | PySpark 内存配置 |
| cores() | spark.executor.cores | CPU 核心数配置 |
| resource() | spark.executor.resource.{name}.* | 自定义资源配置 |

### 资源发现机制集成
- **集群适配**：支持不同集群管理器的资源发现需求
- **脚本执行**：通过 discoveryScript 在 Executor 启动时动态发现资源
- **供应商特定**：vendor 参数用于 Kubernetes 等环境的供应商标识

## 使用场景示例

### 标准资源配置
```scala
val standardRequests = new ExecutorResourceRequests()
  .memory("4g")        // 4GB 堆内存
  .cores(2)            // 2个CPU核心
  .memoryOverhead("1g") // 1GB 内存开销
```

### GPU 资源分配（YARN 环境）
```scala
val gpuRequests = new ExecutorResourceRequests()
  .memory("8g")
  .cores(4)
  .resource("gpu", 2, "/opt/spark/gpu-discovery.sh")
```

### 混合资源类型配置
```scala
val complexRequests = new ExecutorResourceRequests()
  .memory("16g")
  .offHeapMemory("4g") 
  .pysparkMemory("2g")
  .cores(8)
  .resource("gpu", 4, "/scripts/gpu.sh", "nvidia")
  .resource("fpga", 1, "/scripts/fpga.sh")
```

## 补充分析

### 错误处理策略
- **参数验证**：依赖 `JavaUtils.byteStringAsMb` 进行内存格式验证
- **资源冲突**：相同资源名称的请求会被覆盖（后设置的生效）
- **集群兼容性**：不支持的资源类型可能被忽略或报错

### 性能考虑
- **内存效率**：使用轻量级的 ConcurrentHashMap
- **构建效率**：方法链式调用减少中间对象创建
- **访问效率**：哈希映射提供 O(1) 的访问性能

### 版本演进标记
- `@Since("3.1.0")`：从 Spark 3.1.0 版本开始提供
- `@Evolving`：API 仍在演进中，未来可能有改进