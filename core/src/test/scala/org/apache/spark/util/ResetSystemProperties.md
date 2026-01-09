# ResetSystemProperties.scala

## 类的概述和定义
`ResetSystemProperties` 是 Spark Core 测试框架中的一个辅助特质（Trait），继承自 ScalaTest 的 `BeforeAndAfterEach`。
该特质的主要作用是提供测试环境的隔离性，具体针对 Java 系统属性（System Properties）。它会在每个测试用例执行前保存当前的系统属性快照，并在测试执行后自动恢复，从而防止某个测试对系统属性的修改（如设置 `spark.master` 或其他 JVM 参数）污染后续的测试用例。

## 构造函数参数说明
该类是一个 Trait，没有构造函数。它要求混入该特质的类必须是 `Suite` 的子类。

## 核心属性分析
- **oldProperties**: `java.util.Properties` 类型。
  - 用于暂存测试开始前的系统属性副本。
  - 在 `beforeEach` 中赋值，在 `afterEach` 中使用并置空。

## 主要方法分类和说明

### 1. 生命周期管理
- **beforeEach()**: 
  - **执行时机**: 在每个测试方法运行之前。
  - **功能**: 
    1. 调用 `Utils.cloneProperties(System.getProperties)` 创建当前系统属性的深拷贝，并保存到 `oldProperties`。这里特别注释说明了为什么不能使用 `new Properties(defaults)`，因为那样只是设置默认值而不是真正的拷贝，会导致 Scala 的 Properties 包装器无法正确识别。
    2. 调用 `super.beforeEach()` 继续执行后续 Trait 的初始化逻辑。
- **afterEach()**: 
  - **执行时机**: 在每个测试方法运行之后。
  - **功能**: 
    1. 首先调用 `super.afterEach()` 执行其他 Trait 的清理逻辑。
    2. 在 `finally` 块中调用 `System.setProperties(oldProperties)`，将系统属性恢复到测试前的状态。这确保了即使测试失败或抛出异常，环境也能被正确还原。

## 设计特点总结
1.  **Trait 堆叠模式 (Stackable Trait Pattern)**: 
    - 该特质的设计利用了 Scala Trait 的线性化（Linearization）规则。
    - 文档明确建议将 `ResetSystemProperties` 作为混入链中的最后一个 Trait（例如 `class MySuite extends SparkFunSuite with Foo with ResetSystemProperties`）。
    - 这样可以确保它的 `beforeEach` 最先执行（最早保存快照），而它的 `afterEach` 最后执行（最晚恢复快照），从而包裹住其他 Trait 的逻辑，提供最大范围的保护。
2.  **防御性编程**: 通过深拷贝属性对象，避免了引用共享带来的潜在问题。
3.  **自动化清理**: 开发者只需混入该 Trait，无需在每个测试中手动编写 try-finally 块来重置属性，减少了样板代码和出错概率。

## 配置参数说明
该特质不涉及外部配置参数。
