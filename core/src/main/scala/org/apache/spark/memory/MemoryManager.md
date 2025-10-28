# MemoryManager 源码分析

## 类的概述和定义

`MemoryManager` 是 Spark 内存管理系统的核心抽象类，负责强制执行内存和存储内存之间的共享策略。每个 JVM 实例中只有一个 MemoryManager。

**主要职责：**
- 管理执行内存和存储内存的分配与共享
- 提供统一的内存管理接口
- 处理 Tungsten 内存管理相关功能
- 协调四个内存池的协同工作

**内存类型定义：**
- **执行内存**: 用于 Shuffle、Join、排序和聚合计算的内存
- **存储内存**: 用于缓存和在集群间传播内部数据的内存

**类定义：**
```scala
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging
```

## 构造函数参数说明

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `conf` | `SparkConf` | Spark 配置对象，包含内存相关配置参数 |
| `numCores` | `Int` | CPU 核心数量，用于计算默认页面大小 |
| `onHeapStorageMemory` | `Long` | 初始堆上存储内存大小（字节） |
| `onHeapExecutionMemory` | `Long` | 初始堆上执行内存大小（字节） |

## 核心属性分析

### 四个内存池

#### 1. onHeapStorageMemoryPool
- **类型**: `StorageMemoryPool`
- **内存模式**: `MemoryMode.ON_HEAP`
- **同步**: `@GuardedBy("this")` 注解保护
- **作用**: 管理堆上存储内存

#### 2. offHeapStorageMemoryPool
- **类型**: `StorageMemoryPool`
- **内存模式**: `MemoryMode.OFF_HEAP`
- **同步**: `@GuardedBy("this")` 注解保护
- **作用**: 管理堆外存储内存

#### 3. onHeapExecutionMemoryPool
- **类型**: `ExecutionMemoryPool`
- **内存模式**: `MemoryMode.ON_HEAP`
- **同步**: `@GuardedBy("this")` 注解保护
- **作用**: 管理堆上执行内存

#### 4. offHeapExecutionMemoryPool
- **类型**: `ExecutionMemoryPool`
- **内存模式**: `MemoryMode.OFF_HEAP`
- **同步**: `@GuardedBy("this")` 注解保护
- **作用**: 管理堆外执行内存

### 内存配置参数

#### maxOffHeapMemory
- **来源**: `conf.get(MEMORY_OFFHEAP_SIZE)`
- **作用**: 最大可用堆外内存大小

#### offHeapStorageMemory
- **计算方式**: `maxOffHeapMemory * conf.get(MEMORY_STORAGE_FRACTION)`
- **默认比例**: 0.5（可通过 `spark.memory.storageFraction` 配置）
- **作用**: 堆外存储内存大小

### Tungsten 内存相关

#### tungstenMemoryMode
- **确定逻辑**: 根据 `spark.memory.offHeap.enabled` 配置决定
- **作用**: 确定 Tungsten 内存使用堆内还是堆外模式

#### pageSizeBytes
- **默认计算**: 基于可用内存和核心数动态计算
- **范围**: 1MB 到 64MB
- **G1GC 优化**: 考虑 `LONG_ARRAY_OFFSET` 减少内存浪费

#### tungstenMemoryAllocator
- **分配器选择**: 根据内存模式选择 `HEAP` 或 `UNSAFE` 分配器

## 主要方法分类和说明

### 抽象方法（需要子类实现）

#### acquireStorageMemory
- **功能**: 获取存储内存用于缓存块
- **参数**: `blockId`, `numBytes`, `memoryMode`
- **返回值**: 是否成功获取所有请求的内存

#### acquireUnrollMemory
- **功能**: 获取内存用于展开块
- **作用**: 允许子类区分存储内存和展开内存的行为

#### acquireExecutionMemory
- **功能**: 为当前任务获取执行内存
- **特点**: 可能阻塞以确保任务公平性
- **公平策略**: 确保每个任务至少获得 `1/(2N)` 的内存份额

### 具体实现方法

#### 内存释放方法

##### releaseExecutionMemory
- **功能**: 释放任务的执行内存
- **实现**: 根据内存模式调用对应内存池的释放方法

##### releaseAllExecutionMemoryForTask
- **功能**: 释放任务的所有执行内存
- **使用场景**: 任务结束时调用

##### releaseStorageMemory
- **功能**: 释放存储内存
- **实现**: 根据内存模式调用对应存储池的释放方法

##### releaseAllStorageMemory
- **功能**: 释放所有存储内存

##### releaseUnrollMemory
- **功能**: 释放展开内存
- **实现**: 委托给 `releaseStorageMemory`

#### 内存使用查询方法

##### executionMemoryUsed
- **功能**: 返回当前使用的执行内存总量

##### storageMemoryUsed
- **功能**: 返回当前使用的存储内存总量

##### 各种细粒度查询方法
- `onHeapExecutionMemoryUsed`, `offHeapExecutionMemoryUsed`
- `onHeapStorageMemoryUsed`, `offHeapStorageMemoryUsed`
- `getExecutionMemoryUsageForTask`

#### 配置方法

##### setMemoryStore
- **功能**: 设置用于驱逐缓存块的 MemoryStore
- **调用时机**: 构造后设置，由于初始化顺序约束

#### 最大可用内存查询

##### maxOnHeapStorageMemory
- **功能**: 返回堆上存储内存的最大可用量
- **特点**: 随时间变化，取决于具体实现

##### maxOffHeapStorageMemory
- **功能**: 返回堆外存储内存的最大可用量
- **特点**: 随时间变化，取决于具体实现

## 设计特点总结

### 1. 抽象设计模式
- **模板方法**: 定义基本框架，具体实现由子类完成
- **接口统一**: 提供一致的内存管理接口
- **扩展性**: 支持不同的内存管理策略实现

### 2. 内存池架构
- **四池分离**: 堆内/堆外、执行/存储四个独立内存池
- **职责分离**: 不同内存类型有专门的管理池
- **协同工作**: 各池通过 MemoryManager 协调

### 3. 线程安全设计
- **同步机制**: 所有公共方法使用 `synchronized` 保护
- `@GuardedBy` 注解: 明确标识受保护的字段
- **锁对象**: 使用 `this` 作为同步锁

### 4. 配置驱动
- **灵活配置**: 通过 SparkConf 支持多种配置选项
- **动态计算**: 页面大小等参数根据运行时环境计算
- **验证机制**: 对配置参数进行有效性检查

### 5. 内存模式支持
- **双模式**: 完整支持堆内和堆外内存
- **统一接口**: 对使用者透明，统一的内存操作接口
- **模式检测**: 自动检测和配置合适的内存模式

## 配置参数说明

### 核心配置参数

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.memory.offHeap.enabled` | `false` | 是否启用堆外内存 |
| `spark.memory.offHeap.size` | `0` | 堆外内存大小（字节） |
| `spark.memory.storageFraction` | `0.5` | 存储内存占总内存的比例 |
| `spark.buffer.pageSize` | 自动计算 | Tungsten 页面大小 |

### 内存分配策略
- **存储内存比例**: 默认 50% 用于存储，50% 用于执行
- **堆外内存**: 需要显式启用并配置大小
- **页面大小**: 自动优化，考虑 G1GC 特性

## 补充分析

### 内存管理模型演进
- **Spark 1.5 及之前**: 对展开内存的空间释放有限制
- **当前版本**: 统一的动态内存管理模型

### Tungsten 项目集成
- **内存优化**: 专门为 Tungsten 优化内存管理
- **页面分配**: 智能的页面大小计算
- **GC 友好**: 考虑 G1GC 特性减少内存碎片

### 错误处理机制
- **参数验证**: 对构造函数参数进行有效性检查
- **配置检查**: 启用堆外内存时验证相关配置
- **平台兼容性**: 检查 Unsafe 支持情况

### 性能优化特性
- **懒加载**: 默认页面大小使用懒加载计算
- **内存对齐**: 考虑内存对齐减少访问开销
- **核心感知**: 根据 CPU 核心数优化内存分配

该类作为 Spark 内存管理系统的基石，通过精心的抽象设计和丰富的功能支持，为上层应用提供了高效、安全的内存管理能力。