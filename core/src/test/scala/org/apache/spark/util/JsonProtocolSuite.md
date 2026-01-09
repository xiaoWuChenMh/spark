# JsonProtocolSuite.scala

## 类的概述和定义
`JsonProtocolSuite` 是 Spark Core 中用于测试 `JsonProtocol` 工具类的测试套件。它继承自 `SparkFunSuite`。
该类的主要职责是验证 Spark 事件总线（LiveListenerBus）中使用的各种事件对象（`SparkListenerEvent`）与其 JSON 表示形式之间的序列化和反序列化逻辑的正确性。它确保了 Spark 能够正确地将运行时事件转换为 JSON 格式（用于日志记录、History Server 等），并能从 JSON 格式准确还原为对象。此外，它还重点测试了不同 Spark 版本之间的 JSON 格式向后兼容性。

## 构造函数参数说明
该类是一个测试套件，使用默认的无参构造函数。

## 核心属性分析
该类本身主要包含测试逻辑，核心数据和辅助工具定义在伴生对象 `JsonProtocolSuite` 中：

- **mapper**: `ObjectMapper` 实例，用于处理 JSON 数据的解析和生成。
- **预定义的时间戳常量**: 如 `jobSubmissionTime`, `jobCompletionTime` 等，用于构建具有确定性时间戳的测试对象。
- **预定义的 JSON 字符串常量**: 如 `stageSubmittedJsonString`, `taskStartJsonString` 等，这些长字符串包含了预期的 JSON 输出格式，用于与实际序列化结果进行比对。
- **properties**: 一个 `Properties` 对象，包含一些测试用的键值对。

## 主要方法分类和说明

### 1. 基础序列化/反序列化测试
这些测试用例验证标准 Spark 事件的转换逻辑。

- **test("SparkListenerEvent")**: 
  - 这是最核心的测试方法。它创建了几乎所有类型的 `SparkListenerEvent` 实例（如 `SparkListenerStageSubmitted`, `SparkListenerTaskStart`, `SparkListenerJobStart` 等）。
  - 对每个事件调用 `testEvent` 方法，验证对象转 JSON 后是否与预期的 JSON 字符串匹配，以及 JSON 转回对象后是否与原对象相等。

- **test("Dependent Classes")**: 
  - 测试构成事件的各个组件类的序列化，如 `RDDInfo`, `StageInfo`, `TaskInfo`, `TaskMetrics`, `BlockManagerId` 等。
  - 同时也测试了 `StorageLevel`, `JobResult`, `TaskEndReason`, `BlockId` 等枚举或密封类的处理。

### 2. 向后兼容性测试
这些测试用例确保新版本的 Spark 能够读取旧版本生成的 Event Log。

- **test("ExceptionFailure backward compatibility...")**: 验证 `ExceptionFailure` 在不同版本（如堆栈跟踪格式变化、累加器更新字段变化）下的兼容性。
- **test("StageInfo backward compatibility...")**: 验证 `StageInfo` 在添加了新字段（如 `details`, `accumulables`, `resourceProfileId`, `Parent IDs`）后，旧格式数据的解析情况。
- **test("TaskMetrics backward compatibility")**: 验证 `TaskMetrics` 在字段增减（如 CPU 时间、内存指标）后的兼容性。
- **test("BlockManager events backward compatibility")**: 处理旧版本中缺失时间戳字段的情况。
- **test("SparkListenerApplicationStart backwards compatibility")**: 处理旧版本缺失 `appId`, `appAttemptId`, `driverLogs` 等字段的情况。
- **test("SPARK-30936: ...")**: 验证 JSON 解析器对未知字段的忽略能力（向前兼容性）以及对缺失字段的默认值处理。

### 3. 辅助验证方法 (伴生对象中)
- **testEvent(event: SparkListenerEvent, jsonString: String)**: 
  - 将给定的事件对象序列化为 JSON 字符串，并与预期的 `jsonString` 进行比对。
  - 将生成的 JSON 字符串反序列化为新对象，并验证新旧对象是否相等。
- **assertEquals(...)**: 
  - 由于许多 Spark 内部对象没有实现 `equals` 方法，或者包含数组等无法直接比较的字段，该套件实现了一系列重载的 `assertEquals` 方法，用于深度比较 `StageInfo`, `TaskInfo`, `TaskMetrics`, `Exception` 等对象。
- **assertJsonStringEquals(expected: String, actual: String, metadata: String)**: 
  - 解析两个 JSON 字符串为 `JsonNode` 树，然后进行结构化比较，忽略格式差异（如空格、换行）。

### 4. 测试数据构建方法 (伴生对象中)
- **makeStageInfo(...)**: 构造包含 RDD 信息、累加器等的复杂 `StageInfo` 对象。
- **makeTaskInfo(...)**: 构造包含累加器更新的 `TaskInfo` 对象。
- **makeTaskMetrics(...)**: 构造包含 Shuffle 读写指标、输入输出指标的 `TaskMetrics` 对象。
- **makeExecutorMetricsUpdate(...)**: 构造执行器指标更新事件。

## 设计特点总结
1.  **全面的回归测试**: 通过硬编码大量的预期 JSON 字符串，确保任何对序列化逻辑的修改都能被立即检测到，防止意外破坏日志格式。
2.  **自定义相等性断言**: 针对 Spark 内部复杂对象（包含数组、Map、嵌套对象）实现了详细的字段级比较逻辑，弥补了默认 `equals` 方法的不足。
3.  **关注演进与兼容**: 测试用例显式地覆盖了从 Spark 1.x 到 3.x 各个版本引入的字段变化，体现了对长期维护和兼容性的重视。
4.  **伴生对象分离数据与逻辑**: 将大量的静态测试数据（JSON 字符串）和辅助构建方法移至伴生对象，使得测试类本身专注于测试流程的定义，结构清晰。

## 配置参数说明
该测试套件不涉及外部配置参数，主要依赖代码中定义的常量和 `JsonProtocol` 的默认行为。
