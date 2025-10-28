# UnifiedMemoryManager 源码分析

## 类的概述和定义

`UnifiedMemoryManager` 是 Spark 内存管理系统的默认实现，采用统一内存管理模型，允许执行内存和存储内存之间相互借用。该类继承自 `MemoryManager`，是 Spark 1.6 之后引入的重要改进。

**核心设计理念：**
- **软边界**: 执行内存和存储内存之间没有严格的固定边界
- **双向借用**: 存储可以借用执行内存，执行也可以借用存储内存
- **动态调整**: 内存分配根据实际使用情况动态调整
- **公平性**: 确保任务间的内存分配公平性

**内存分配模型：**
- **总共享内存**: `(总堆空间 - 300MB) * spark.memory.fraction`（默认0.6）
- **存储区域**: 共享内存的 `spark.memory.storageFraction`（默认0.5）
- **默认分配**: 存储区域占堆空间的 0.6 * 0.5 = 0.3（30%）

**类定义：**
```scala
private[spark] class UnifiedMemoryManager(
    conf: SparkConf,
    val maxHeapMemory: Long,
    onHeapStorageRegionSize: Long,
    numCores: Int)
  extends MemoryManager(
    conf,
    numCores,
    onHeapStorageRegionSize,
    maxHeapMemory - onHeapStorageRegionSize)
```

## 构造函数参数说明

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `conf` | `SparkConf` | Spark 配置对象 |
| `maxHeapMemory` | `Long` | 最大堆内存大小（字节） |
| `onHeapStorageRegionSize` | `Long` | 堆上存储区域初始大小（字节） |
| `numCores` | `Int` | CPU 核心数量 |

**父类构造函数参数传递：**
- `onHeapStorageMemory`: 直接使用 `onHeapStorageRegionSize`
- `onHeapExecutionMemory`: 计算为 `maxHeapMemory - onHeapStorageRegionSize`

## 核心属性分析

### 1. 不变式验证（Invariants）

#### assertInvariants 方法
**功能**: 验证内存池大小的一致性
**验证规则：**
- 堆上执行池大小 + 堆上存储池大小 = 最大堆内存
- 堆外执行池大小 + 堆外存储池大小 = 最大堆外内存

**设计意义：**
- **状态一致性**: 确保内存分配的正确性
- **调试辅助**: 在关键操作前后验证状态
- **错误预防**: 及早发现内存管理错误

### 2. 最大可用存储内存

#### maxOnHeapStorageMemory
**计算公式**: `maxHeapMemory - onHeapExecutionMemoryPool.memoryUsed`
**动态特性**: 随着执行内存使用量变化
**设计理念**: 存储内存可以扩展到整个堆空间减去执行内存使用量

#### maxOffHeapStorageMemory
**计算公式**: `maxOffHeapMemory - offHeapExecutionMemoryPool.memoryUsed`
**堆外支持**: 同样支持堆外内存的动态扩展

## 主要方法分类和说明

### 执行内存获取方法

#### acquireExecutionMemory
**功能**: 为任务获取执行内存，支持内存借用机制

**方法流程：**

1. **参数验证和初始化**
   - 验证不变式
   - 验证请求字节数非负
   - 根据内存模式选择对应的内存池

2. **内存池扩展回调（maybeGrowExecutionPool）**
   ```scala
   def maybeGrowExecutionPool(extraMemoryNeeded: Long): Unit
   ```
   **扩展策略：**
   - 计算可从存储回收的内存：`math.max(storagePool.memoryFree, storagePool.poolSize - storageRegionSize)`
   - 如果可回收内存 > 0，执行回收操作
   - 通过 `freeSpaceToShrinkPool` 释放存储空间
   - 调整池大小：存储池减小，执行池增加

3. **最大执行池大小计算（computeMaxExecutionPoolSize）**
   ```scala
   def computeMaxExecutionPoolSize(): Long
   ```
   **计算公式**: `maxMemory - math.min(storagePool.memoryUsed, storageRegionSize)`
   **设计考虑（SPARK-12155）：**
   - 考虑可通过驱逐存储释放的潜在内存
   - 限制在 `maxMemory` 以内确保公平性
   - 防止任务占用超过公平份额的内存

4. **委托执行**
   - 调用 `executionPool.acquireMemory` 执行实际的内存分配
   - 传入扩展回调和最大池大小计算函数

### 存储内存获取方法

#### acquireStorageMemory
**功能**: 获取存储内存用于缓存块

**方法流程：**

1. **参数验证和初始化**
   - 验证不变式
   - 验证请求字节数非负
   - 根据内存模式选择对应的内存池

2. **快速失败检查**
   - 如果请求大小超过最大可用存储内存，直接返回失败
   - 记录日志说明原因

3. **内存借用机制**
   - 如果存储池空闲内存不足
   - 从执行池借用空闲内存：`Math.min(executionPool.memoryFree, numBytes - storagePool.memoryFree)`
   - 调整池大小：执行池减小，存储池增加

4. **委托执行**
   - 调用 `storagePool.acquireMemory` 执行实际的内存分配

#### acquireUnrollMemory
**功能**: 获取展开内存
**实现**: 直接委托给 `acquireStorageMemory`
**设计**: 在统一内存模型中，展开内存和存储内存使用相同的管理策略

## 伴生对象和工厂方法

### UnifiedMemoryManager 伴生对象

#### RESERVED_SYSTEM_MEMORY_BYTES
- **值**: 300 * 1024 * 1024（300MB）
- **作用**: 为系统预留固定大小的非存储、非执行内存
- **类比**: 功能类似 `spark.memory.fraction`，但保证小堆时的系统内存

#### apply 工厂方法
**功能**: 创建 UnifiedMemoryManager 实例
**参数**: `conf: SparkConf`, `numCores: Int`
**创建流程：**
1. 调用 `getMaxMemory` 计算最大内存
2. 计算存储区域大小：`maxMemory * conf.get(MEMORY_STORAGE_FRACTION)`
3. 创建新的 UnifiedMemoryManager 实例

#### getMaxMemory 方法
**功能**: 计算执行和存储共享的总内存量

**计算流程：**

1. **系统内存获取**
   - 从配置获取测试内存或实际系统内存
   - 计算预留内存（测试环境为0，生产环境为300MB）

2. **最小系统内存验证**
   - 最小要求：`reservedMemory * 1.5`
   - 验证系统内存是否满足最小要求
   - 验证执行器内存是否满足最小要求

3. **可用内存计算**
   - `usableMemory = systemMemory - reservedMemory`
   - 最终内存：`usableMemory * memoryFraction`

## 设计特点总结

### 1. 统一内存管理模型
- **软边界设计**: 打破固定边界，提高内存利用率
- **双向借用**: 存储和执行可以相互借用空闲内存
- **动态调整**: 根据实际需求动态调整内存分配

### 2. 内存借用策略
- **存储借用执行**: 存储可以借用执行的空闲内存
- **执行借用存储**: 执行可以通过驱逐缓存借用存储内存
- **非对称性**: 执行内存不会被存储主动驱逐

### 3. 公平性保障
- **任务公平**: 通过 ExecutionMemoryPool 确保任务间公平
- **池大小限制**: 计算最大池大小防止过度占用
- **SPARK-12155 修复**: 考虑可回收内存的公平分配

### 4. 错误处理和验证
- **不变式验证**: 关键操作前后验证状态一致性
- **参数验证**: 所有输入参数进行有效性检查
- **快速失败**: 明显不可能满足的请求直接失败

### 5. 配置驱动
- **灵活配置**: 支持多种内存相关配置参数
- **自动计算**: 根据系统环境自动计算合适的内存大小
- **测试支持**: 支持测试环境的特殊配置

## 配置参数说明

### 核心配置参数

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.memory.fraction` | `0.6` | 用于执行和存储的内存比例 |
| `spark.memory.storageFraction` | `0.5` | 存储内存占共享内存的比例 |
| `spark.memory.offHeap.enabled` | `false` | 是否启用堆外内存 |
| `spark.memory.offHeap.size` | `0` | 堆外内存大小 |

### 测试相关配置
| 配置项 | 说明 |
|--------|------|
| `spark.testing.memory` | 测试环境下的模拟内存大小 |
| `spark.testing.reservedMemory` | 测试环境下的预留内存 |

### 内存计算示例
**1GB JVM 示例：**
- 系统内存：1024MB
- 预留内存：300MB
- 可用内存：1024 - 300 = 724MB
- 共享内存：724 * 0.6 ≈ 434MB
- 存储区域：434 * 0.5 = 217MB

## 补充分析

### 在Spark演进中的意义

#### 从静态分配到动态共享
- **Spark 1.5及之前**: 静态划分执行和存储内存
- **Spark 1.6引入**: 统一内存管理，大幅提升内存利用率
- **当前版本**: 成为默认内存管理器

#### 解决的关键问题
- **内存碎片化**: 通过动态共享减少内存浪费
- **任务阻塞**: 减少因内存不足导致的任务阻塞
- **缓存效率**: 提高缓存命中率和系统性能

### 内存管理策略深度分析

#### 存储借用执行的限制
- **借用条件**: 只能借用执行的空闲内存
- **不会驱逐**: 存储不会主动驱逐执行内存
- **影响**: 执行占用过多内存时，新缓存可能立即被驱逐

#### 执行借用存储的机制
- **驱逐策略**: 通过驱逐缓存块来回收内存
- **按需回收**: 只在需要时执行驱逐操作
- **精确计算**: 准确计算需要回收的内存量

### 性能优化特性

#### 内存操作优化
- **懒计算**: 最大池大小动态计算
- **最小开销**: 内存借用操作开销很小
- **同步优化**: 使用细粒度同步块

#### 缓存管理优化
- **智能驱逐**: 只在必要时执行块驱逐
- **空间预计算**: 准确预估需要释放的空间
- **最小影响**: 尽量减少对现有缓存的影响

### 健壮性设计

#### 错误恢复机制
- **状态验证**: 通过不变式验证确保状态正确
- **异常处理**: 完善的异常处理和日志记录
- **资源清理**: 确保内存资源的正确释放

#### 边界情况处理
- **小内存系统**: 通过预留内存保证系统运行
- **大内存请求**: 快速失败避免无谓等待
- **并发访问**: 完善的同步机制防止竞态条件

`UnifiedMemoryManager` 作为 Spark 内存管理的核心组件，通过先进的统一内存模型和精妙的借用机制，极大地提升了内存利用率和系统性能，是 Spark 高性能计算的重要保障。