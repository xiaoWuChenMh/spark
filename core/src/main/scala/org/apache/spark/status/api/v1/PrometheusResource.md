# PrometheusResource 类分析文档

## 类的概述和定义

`PrometheusResource` 是 Spark REST API v1 版本中的一个特殊资源类，专门用于将 Spark 执行器指标导出为 Prometheus 监控系统兼容的格式。该类继承自 `ApiRequestContext`，提供了标准的 Prometheus 指标暴露接口。

**功能定位**:
- 将 Spark 执行器性能指标转换为 Prometheus 格式
- 提供 `/executors/prometheus` 端点用于指标采集
- 支持 Prometheus 监控系统的自动发现和采集
- 实验性功能，标注为 `@Experimental`

**重要说明**:
- 基于 `ExecutorSummary` 数据，与 `ExecutorSource` 不同
- 遵循 Prometheus 指标命名规范和格式要求

## 构造函数参数说明

该类没有显式定义的构造函数，继承自 `ApiRequestContext` 基类。通过继承获得对 Spark UI 根对象的访问权限。

## 核心属性分析

### 注解属性
- `@Experimental`: 标记为实验性功能，API 可能发生变化
- `@Path("/executors")`: 定义资源的基础路径
- `@GET @Path("prometheus")`: 定义 Prometheus 指标端点
- `@Produces(Array(MediaType.TEXT_PLAIN))`: 指定响应内容类型为纯文本格式

### 静态导入常量
- `SPARK_VERSION_SHORT`: Spark 版本信息
- `SPARK_REVISION`: Spark 修订版本信息

## 主要方法分类和说明

### 1. executors 方法 - 主指标导出方法

**方法签名**:
```scala
def executors(): String
```

**功能说明**:
将当前应用程序的所有执行器指标转换为 Prometheus 文本格式返回。

**执行流程**:
1. 构建字符串构建器用于组装指标文本
2. 添加 Spark 版本信息指标
3. 遍历所有活跃执行器
4. 为每个执行器生成完整的指标集合
5. 返回格式化的 Prometheus 指标文本

**指标生成逻辑**:
- **基础指标**: RDD块数、内存使用、磁盘使用、核心数等
- **任务指标**: 活跃任务、失败任务、完成任务、总任务数
- **时间指标**: 总持续时间、GC时间（转换为秒）
- **I/O指标**: 输入字节、Shuffle读写字节
- **内存指标**: 最大内存、堆内存使用情况
- **峰值内存指标**: 16种不同类型的内存使用峰值
- **GC统计**: Minor/Major GC次数和时间

### 2. getServletHandler 静态方法 - Servlet配置

**方法签名**:
```scala
def getServletHandler(uiRoot: UIRoot): ServletContextHandler
```

**功能说明**:
创建并配置 Prometheus 指标服务的 Servlet 处理器。

**配置步骤**:
1. 创建无会话的 Servlet 上下文处理器
2. 设置上下文路径为 `/metrics`
3. 配置 Jersey Servlet 容器
4. 设置提供者包路径
5. 绑定 UI 根对象到 Servlet 上下文
6. 添加 Servlet 映射

## 设计特点总结

### 1. Prometheus 兼容性设计
- **指标命名规范**: 使用 `metrics_executor_` 前缀，符合 Prometheus 命名约定
- **标签系统**: 使用标准的 Prometheus 标签格式 `{key="value"}`
- **单位规范**: 正确使用 `_bytes`, `_seconds`, `_total` 等后缀
- **数据类型**: 支持计数器（`_total`）和测量值类型

### 2. 性能指标完整性
- **全面覆盖**: 包含执行器的所有关键性能指标
- **内存细分**: 详细的内存使用情况统计
- **任务统计**: 完整的任务执行生命周期监控
- **I/O监控**: 输入输出和Shuffle操作统计

### 3. 架构设计特点
- **分离关注点**: 指标生成与Servlet配置分离
- **可扩展性**: 易于添加新的指标类型
- **标准化**: 遵循行业标准的监控格式

### 4. 安全性考虑
- **只读接口**: 仅提供指标查询，不修改系统状态
- **权限控制**: 通过Servlet上下文进行访问控制

## 配置参数说明

### Prometheus 指标格式
- **指标前缀**: `metrics_executor_`
- **基础标签**: 
  - `application_id`: 应用程序ID
  - `application_name`: 应用程序名称
  - `executor_id`: 执行器ID

### Servlet 配置
- **上下文路径**: `/metrics`
- **提供者包**: `org.apache.spark.status.api.v1`
- **会话管理**: 无会话模式（`NO_SESSIONS`）

### 响应格式
- **内容类型**: `text/plain`
- **编码**: UTF-8 文本格式
- **结构**: 标准的 Prometheus 指标文本格式

## 使用场景和最佳实践

### 典型使用场景
1. **监控系统集成**: 与 Prometheus + Grafana 监控栈集成
2. **性能分析**: 实时监控 Spark 应用程序的执行器性能
3. **容量规划**: 基于历史指标进行资源规划
4. **故障诊断**: 通过指标异常发现系统问题

### Prometheus 配置示例
```yaml
scrape_configs:
  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:4040']
    metrics_path: '/executors/prometheus'
    scrape_interval: 15s
```

### 最佳实践建议
1. **采集频率**: 建议 15-30秒的采集间隔
2. **指标保留**: 根据业务需求设置合适的保留时间
3. **告警规则**: 基于关键指标设置智能告警
4. **仪表板**: 创建专门的 Spark 监控仪表板

### 注意事项
- 该功能为实验性，API 可能发生变化
- 指标基于执行器摘要，可能与实时数据有轻微延迟
- 需要确保 Prometheus 能够访问 Spark Web UI 端口
- 在大规模集群中注意指标采集的性能影响