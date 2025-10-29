# ResourceProfileBuilder 类分析

## 类的概述和定义

`ResourceProfileBuilder` 是 Spark 资源管理系统中的构建器类，采用构建器模式（Builder Pattern）来创建 `ResourceProfile` 对象。它提供了流畅的 API 接口，允许用户逐步配置 Executor 和 Task 的资源需求，最终构建不可变的 ResourceProfile 实例。

**类定义签名：**
```scala
@Evolving
@Since("3.1.0")
class ResourceProfileBuilder()
```

**设计模式：**构建器模式（Builder Pattern）

**主要用途：**
- 提供类型安全的方式构建 ResourceProfile
- 支持链式方法调用，提高代码可读性
- 分离资源配置的构建过程和使用过程
- 确保最终构建的 ResourceProfile 是不可变对象

## 核心属性分析

### 内部存储结构
```scala
private val _taskResources = new ConcurrentHashMap[String, TaskResourceRequest]()
private val _executorResources = new ConcurrentHashMap[String, ExecutorResourceRequest]()
```

**数据结构设计：**
- **存储类型**：使用 `ConcurrentHashMap` 确保线程安全
- **键类型**：资源名称（String）
- **值类型**：对应的资源请求对象
- **线程安全**：支持多线程环境下的并发访问

### 公共访问属性
```scala
def taskResources: Map[String, TaskResourceRequest] = _taskResources.asScala.toMap
def executorResources: Map[String, ExecutorResourceRequest] = _executorResources.asScala.toMap
```

**访问控制：**
- **只读视图**：返回不可变的 Scala Map
- **数据转换**：将 Java ConcurrentHashMap 转换为 Scala Map
- **快照特性**：返回的是当前状态的快照，后续修改不影响已返回的视图

## 主要方法分类和说明

### 1. 资源请求配置方法

#### require(requests: ExecutorResourceRequests): this.type
```scala
def require(requests: ExecutorResourceRequests): this.type = {
  _executorResources.putAll(requests.requests.asJava)
  this
}
```

**功能说明：**
- **参数类型**：接受 `ExecutorResourceRequests` 对象
- **操作**：将 Executor 资源请求添加到构建器中
- **返回类型**：`this.type` 支持链式调用
- **实现细节**：使用 `putAll` 批量添加资源请求

#### require(requests: TaskResourceRequests): this.type
```scala
def require(requests: TaskResourceRequests): this.type = {
  _taskResources.putAll(requests.requests.asJava)
  this
}
```

**功能说明：**
- **参数类型**：接受 `TaskResourceRequests` 对象
- **操作**：将 Task 资源请求添加到构建器中
- **方法重载**：与 Executor 版本形成重载关系
- **一致性**：保持相同的 API 设计风格

### 2. 资源清空方法

#### clearExecutorResourceRequests(): this.type
```scala
def clearExecutorResourceRequests(): this.type = {
  _executorResources.clear()
  this
}
```

**使用场景：**
- **重置配置**：清除所有已配置的 Executor 资源请求
- **错误恢复**：配置错误时重新开始
- **动态调整**：支持配置的动态修改

#### clearTaskResourceRequests(): this.type
```scala
def clearTaskResourceRequests(): this.type = {
  _taskResources.clear()
  this
}
```

**对称设计：**
- 与 Executor 清空方法对称
- 支持独立的资源类型管理
- 保持链式调用能力

### 3. Java API 兼容方法

#### taskResourcesJMap: JMap[String, TaskResourceRequest]
```scala
def taskResourcesJMap: JMap[String, TaskResourceRequest] = _taskResources.asScala.asJava
```

**Java 兼容性：**
- **返回类型**：Java Map 接口
- **转换过程**：Scala Map → Java Map
- **使用场景**：Java 用户直接使用原生 Java 集合

#### executorResourcesJMap: JMap[String, ExecutorResourceRequest]
```scala
def executorResourcesJMap: JMap[String, ExecutorResourceRequest] = {
  _executorResources.asScala.asJava
}
```

**设计一致性：**
- 与 Task 资源保持相同的 API 风格
- 提供对称的 Java 接口
- 支持 Java 用户的便捷访问

### 4. 构建方法

#### build(): ResourceProfile
```scala
def build(): ResourceProfile = {
  if (_executorResources.isEmpty) {
    new TaskResourceProfile(taskResources)
  } else {
    new ResourceProfile(executorResources, taskResources)
  }
}
```

**智能构建逻辑：**
- **条件判断**：检查 Executor 资源是否为空
- **TaskResourceProfile**：当只有 Task 资源时，创建轻量级 Profile
- **ResourceProfile**：当有 Executor 资源时，创建完整 Profile
- **不可变性**：构建完成后返回不可变对象

### 5. 调试信息方法

#### toString(): String
```scala
override def toString(): String = {
  "Profile executor resources: " +
    s"${_executorResources.asScala.map(pair => s"${pair._1}=${pair._2.toString()}")}, " +
    s"task resources: ${_taskResources.asScala.map(pair => s"${pair._1}=${pair._2.toString()}")}"
}
```

**调试信息格式：**
- **结构清晰**：分别显示 Executor 和 Task 资源
- **详细信息**：包含资源名称和具体请求内容
- **可读性强**：便于日志记录和调试

## 设计特点总结

### 1. 构建器模式应用
**模式优势：**
- **分离关注点**：将复杂对象的构建与表示分离
- **逐步构建**：支持分步骤配置资源需求
- **不可变性**：最终产品是不可变的 ResourceProfile

**API 设计：**
```scala
val profile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests().cores(4).memory("8g"))
  .require(new TaskResourceRequests().cpus(2))
  .build()
```

### 2. 线程安全设计
**并发控制：**
- **ConcurrentHashMap**：内部使用线程安全的集合
- **无状态方法**：大多数方法不依赖外部状态
- **快照返回**：公共访问方法返回数据快照

**多线程场景：**
- 支持多线程环境下的配置构建
- 避免竞态条件和数据不一致
- 确保构建过程的可靠性

### 3. 多语言支持
**Scala 原生支持：**
- 返回 Scala 不可变 Map
- 支持函数式编程风格
- 与 Scala 集合生态系统集成

**Java 兼容性：**
- 提供 Java Map 接口
- 支持 Java 用户的直接使用
- 保持跨语言的一致性

### 4. 智能构建策略
**条件化构建：**
- **自动检测**：根据配置内容选择适当的 Profile 类型
- **性能优化**：TaskResourceProfile 更轻量级
- **语义清晰**：明确区分不同的使用场景

**使用场景区分：**
- **完整配置**：Executor + Task → ResourceProfile
- **任务配置**：只有 Task → TaskResourceProfile
- **动态分配**：支持不同的资源分配策略

## 使用模式分析

### 1. 基本使用模式
```scala
// 标准构建模式
val builder = new ResourceProfileBuilder()
val profile = builder
  .require(executorRequests)  // 配置Executor资源
  .require(taskRequests)      // 配置Task资源
  .build()                    // 构建最终对象
```

### 2. 链式调用模式
```scala
// 流畅的链式调用
val profile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests().cores(4).memory("8g"))
  .require(new TaskResourceRequests().cpus(2))
  .build()
```

### 3. 动态配置模式
```scala
// 支持动态修改配置
val builder = new ResourceProfileBuilder()
builder.require(initialExecutorRequests)

// 根据条件调整配置
if (needMoreResources) {
  builder.clearExecutorResourceRequests()
    .require(enhancedExecutorRequests)
}

val finalProfile = builder.build()
```

### 4. 错误恢复模式
```scala
// 配置错误时的恢复
val builder = new ResourceProfileBuilder()
try {
  builder.require(validatedRequests)
} catch {
  case e: ValidationException =>
    builder.clearExecutorResourceRequests()
    builder.clearTaskResourceRequests()
    // 重新配置
}
```

## 与相关类的协作关系

### 与 ResourceProfile 的关系
**构建目标：**
- ResourceProfileBuilder 是构建器
- ResourceProfile 是构建的产品
- 构建完成后，Builder 可以丢弃或重用

**设计约束：**
- ResourceProfile 是不可变的
- Builder 负责处理可变状态
- 构建过程封装复杂性

### 与 ExecutorResourceRequests/TaskResourceRequests 的关系
**输入提供者：**
- 这两个类提供具体的资源配置
- Builder 负责整合多个配置源
- 支持批量添加资源请求

**协作模式：**
```scala
val executorReqs = new ExecutorResourceRequests().cores(4).memory("8g")
val taskReqs = new TaskResourceRequests().cpus(2)

val profile = new ResourceProfileBuilder()
  .require(executorReqs)
  .require(taskReqs)
  .build()
```

### 与 TaskResourceProfile 的关系
**智能选择：**
- Builder 自动检测配置内容
- 纯任务配置 → TaskResourceProfile
- 完整配置 → ResourceProfile
- 优化资源使用和性能

## 错误处理和验证

### 1. 输入验证
**参数检查：**
- 依赖 ExecutorResourceRequests/TaskResourceRequests 进行参数验证
- Builder 专注于构建逻辑，验证委托给专业类
- 分离验证责任，提高代码可维护性

### 2. 构建时验证
**完整性检查：**
- build() 方法执行最终验证
- 确保资源配置的合理性
- 返回有效的 ResourceProfile 实例

### 3. 异常处理策略
**防御性编程：**
- 使用 ConcurrentHashMap 避免并发问题
- 方法返回 this 支持链式调用的错误恢复
- 清空方法支持配置重置

## 性能优化考虑

### 1. 内存效率
**集合选择：**
- ConcurrentHashMap 平衡线程安全和性能
- 按需转换，避免不必要的对象创建
- 快照返回减少内存占用

### 2. 构建效率
**批量操作：**
- putAll() 方法批量添加资源
- 减少多次单个添加的开销
- 优化构建过程性能

### 3. 对象复用
**构建器重用：**
- 清空方法支持构建器重用
- 减少新对象创建开销
- 适合批量构建场景

## 扩展性设计

### 1. API 扩展
**方法设计：**
- 清晰的方法签名便于扩展
- 重载机制支持新参数类型
- 返回类型设计支持链式调用

### 2. 资源类型扩展
**泛化支持：**
- 不限制特定的资源类型
- 通过资源名称动态支持新资源
- 与资源发现机制无缝集成

### 3. 构建策略扩展
**条件构建：**
- build() 方法可扩展新的构建逻辑
- 支持不同的 Profile 类型创建
- 适应未来的架构演进

## 实际应用示例

### 1. GPU 资源配置
```scala
val profile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests()
    .cores(8)
    .memory("16g")
    .resource("gpu", 2, "/opt/scripts/gpu-discovery.sh"))
  .require(new TaskResourceRequests()
    .cpus(1)
    .resource("gpu", 0.5))  // 2个任务共享1个GPU
  .build()
```

### 2. 内存优化配置
```scala
val profile = new ResourceProfileBuilder()
  .require(new ExecutorResourceRequests()
    .cores(4)
    .memory("4g")
    .offHeapMemory("1g")
    .memoryOverhead("512m"))
  .require(new TaskResourceRequests().cpus(1))
  .build()
```

### 3. 纯任务资源配置（动态分配）
```scala
val taskProfile = new ResourceProfileBuilder()
  .require(new TaskResourceRequests()
    .cpus(2)
    .resource("fpga", 1))
  .build()  // 自动创建 TaskResourceProfile
```

ResourceProfileBuilder 通过精心的设计实现了资源配置的灵活性、安全性和易用性，是 Spark 资源管理系统中的重要组成部分。