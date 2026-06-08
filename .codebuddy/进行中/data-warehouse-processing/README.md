# 数仓数据加工技能 (Data Warehouse Processing Skill)

## 概述

数仓数据加工技能是一个专门用于数仓数据处理的综合技能，采用分层架构（DWD、DIM、DWS、ADS）和工作流决策树，能够根据用户请求智能选择合适的工作流执行数据加工任务。

**核心特性：**
- 🏗️ **分层架构支持**：完整的DWD、DIM、DWS、ADS四层数据处理
- 🤖 **智能工作流选择**：基于决策树自动选择合适的工作流
- 📊 **表结构管理**：统一的表结构信息存储和查询
- 🐍 **Python加工逻辑**：丰富的Python数据处理模式库
- 🛠️ **SQL模板库**：各层标准SQL模板和最佳实践
- 🔄 **工作流调度**：支持任务依赖和并行执行
- 📈 **数据质量监控**：内置数据质量检查和告警

## 技能结构

```
.data-warehouse-processing/
├── SKILL.md                    # 技能主文档
├── README.md                   # 本文件
├── config/                     # 配置文件目录
│   └── workflow_config.yaml    # 工作流配置文件模板
├── scripts/                    # Python脚本目录
│   ├── data_cleaning.py        # DWD层数据清洗工具
│   ├── dimension_processing.py # DIM层维度处理工具
│   ├── aggregation_calculations.py # DWS层聚合计算工具
│   ├── wide_table_builder.py   # ADS层宽表构建工具
│   └── workflow_scheduler.py   # 工作流调度器
├── references/                 # 参考文档目录
│   ├── table_schema_guide.md   # 表结构设计指南
│   ├── python_patterns.md      # Python加工模式库
│   └── workflow_examples.md    # 工作流示例详解
└── assets/                     # 资源文件目录
    └── sql_templates.sql       # SQL模板库
```

## 快速开始

### 1. 技能激活

```bash
# 在CodeBuddy中激活技能
激活 data-warehouse-processing 技能
```

### 2. 基础使用

**示例1：DWS层表添加字段**

用户请求：
> "给dws的表A增加一个字段，字段的解析逻辑是从用户行为表中统计最近7天的访问次数"

技能响应：
1. 识别为DWS层、指标数据、添加字段操作
2. 选择`dws_metric_add_field`工作流
3. 提供Python加工逻辑和SQL模板
4. 生成完整的字段添加方案

**示例2：DWD层数据处理**

用户请求：
> "处理DWD层交易数据，需要去重和异常值检测"

技能响应：
1. 识别为DWD层、交易数据、数据处理操作
2. 选择`dwd_data_cleaning`工作流
3. 执行数据去重、空值处理、异常值检测步骤
4. 输出处理报告和数据质量指标

## 核心功能

### 1. 工作流决策树

技能内置智能工作流决策树，根据三个维度选择合适的工作流：

**数据分层 (Layer)：**
- DWD：明细数据层 - 原始数据清洗和标准化
- DIM：维度数据层 - 维度表创建和维护（支持SCD Type 1/2）
- DWS：汇总数据层 - 指标聚合和计算
- ADS：应用数据层 - 宽表构建和报表准备

**数据分类 (Category)：**
- Transaction：交易数据
- Master：主数据
- Reference：参考数据
- Dimension：维度数据
- Metric：指标数据
- Aggregation：聚合数据
- Wide Table：宽表数据

**操作类型 (Operation)：**
- Add Field：添加字段
- Modify Field：修改字段
- Delete Field：删除字段
- Create Table：创建表
- Process Data：处理数据
- Quality Check：质量检查

### 2. 表结构管理

技能维护表结构信息库，包含：
- 表名、所属层、数据分类
- 字段列表（名称、类型、注释、约束）
- 分区策略和存储位置
- 创建时间和更新时间

**API示例：**
```python
# 获取表结构
schema = scheduler.get_table_schema("dws_sales_daily")
print(f"表名: {schema.table_name}")
print(f"所属层: {schema.layer.value}")
print(f"字段数: {len(schema.columns)}")

# 添加表结构
new_schema = TableSchema(
    table_name="dwd_order_detail",
    layer=DataLayer.DWD,
    category=DataCategory.TRANSACTION,
    columns=[
        {"name": "order_id", "type": "string", "comment": "订单ID"},
        {"name": "order_amount", "type": "decimal(10,2)", "comment": "订单金额"}
    ],
    partitions=["data_date"],
    location="/data/warehouse/dwd/order_detail",
    comment="订单明细表"
)
scheduler.add_table_schema(new_schema)
```

### 3. Python加工逻辑库

技能提供丰富的Python数据处理模式：

**DWD层数据清洗：**
- 数据去重（基于业务主键）
- 空值处理（填充、删除、插值）
- 数据类型转换
- 异常值检测（IQR、Z-Score）
- 数据标准化和规范化

**DIM层维度处理：**
- SCD Type 1处理（直接覆盖）
- SCD Type 2处理（历史版本管理）
- 维度关联更新
- 缓慢变化维度合并

**DWS层聚合计算：**
- 多维度聚合（SUM、AVG、COUNT等）
- 时间窗口聚合（滚动窗口、滑动窗口）
- 增量聚合计算
- 排名和百分位计算

**ADS层宽表构建：**
- 多表关联（内连接、左连接、全连接）
- 字段映射和转换
- 复合指标计算
- 标签体系构建

### 4. SQL模板库

技能提供完整的SQL模板库，覆盖各层常见操作：

**DWD层模板：**
- 增量表创建
- 增量数据插入
- 数据质量检查SQL

**DIM层模板：**
- SCD Type 2表创建
- 维度数据处理
- 维度关联更新

**DWS层模板：**
- 日汇总表创建
- 周期上卷聚合
- 衍生指标计算

**ADS层模板：**
- 宽表创建
- 复合指标计算
- 排名和趋势分析

**使用示例：**
```sql
-- 使用DWS层日汇总模板
-- 替换参数：{subject_name} -> sales, {date_key} -> 20240311
INSERT OVERWRITE TABLE dws_sales_daily PARTITION (date_key=20240311)
SELECT 
    20240311 AS date_key,
    '2024-03-11' AS date_str,
    -- 维度字段
    user_id,
    product_category,
    -- 基础指标
    SUM(order_amount) AS order_amount_sum,
    AVG(order_amount) AS order_amount_avg,
    COUNT(DISTINCT order_id) AS order_count,
    -- 技术字段
    CURRENT_TIMESTAMP() AS create_time
FROM 
    dwd_order_detail
WHERE 
    data_date = '2024-03-11'
    AND is_valid = 1
GROUP BY 
    user_id, product_category;
```

## 工作流调度器

### 基本使用

```python
# 初始化调度器
scheduler = WorkflowScheduler("config/workflow_config.yaml")

# 根据用户请求选择工作流
user_request = "给dws的表A增加一个字段，字段的解析逻辑是xx"
workflow_key = scheduler.select_workflow(user_request)

if workflow_key:
    print(f"选择的工作流: {workflow_key}")
    
    # 执行工作流
    parameters = {
        "table_name": "dws_sales_daily",
        "field_name": "recent_7day_visits",
        "calculation_logic": "统计最近7天的访问次数"
    }
    
    success = scheduler.execute_workflow(workflow_key, parameters)
    
    if success:
        print("工作流执行成功")
    else:
        print("工作流执行失败")
else:
    print("无法选择合适的工作流")
```

### 命令行使用

```bash
# 选择工作流（不执行）
python scripts/workflow_scheduler.py --request "给dws的表A增加一个字段"

# 选择并执行工作流
python scripts/workflow_scheduler.py --request "处理DWD层交易数据" --execute

# 导出配置
python scripts/workflow_scheduler.py --request "测试请求" --export "workflow_config.json"
```

## 配置说明

### 配置文件结构

主要配置文件：`config/workflow_config.yaml`

**关键配置项：**

1. **数据库配置**：Hive/Spark连接参数
2. **HDFS配置**：数据存储位置和保留策略
3. **调度器配置**：并发控制、重试策略、超时设置
4. **工作流配置**：预定义工作流和调度时间
5. **表结构配置**：命名规范和默认结构
6. **数据质量配置**：检查规则和告警设置
7. **性能优化配置**：查询优化和分区策略
8. **监控配置**：指标收集和系统集成
9. **安全配置**：认证授权和加密设置
10. **备份配置**：备份策略和恢复目标

### 环境配置

支持多环境配置（开发、测试、生产）：

```yaml
environment:
  profiles:
    development:
      database:
        host: "dev-hive.example.com"
    testing:
      database:
        host: "test-hive.example.com"
    production:
      database:
        host: "prod-hive.example.com"
```

### 变量替换

支持动态变量替换：

```yaml
data_date: "{{ now().strftime('%Y-%m-%d') }}"
host: "{{ os.environ.get('HIVE_HOST', 'localhost') }}"
```

## 使用场景

### 场景1：字段添加需求

**用户请求：** "给dws_sales_daily表增加一个客单价字段，计算逻辑是订单总金额/订单数"

**技能处理：**
1. 识别为DWS层、指标数据、添加字段操作
2. 选择`dws_metric_add_field`工作流
3. 提供Python加工逻辑：
   ```python
   def calculate_avg_order_value(total_amount, order_count):
       if order_count > 0:
           return total_amount / order_count
       else:
           return 0
   ```
4. 提供SQL模板：
   ```sql
   ALTER TABLE dws_sales_daily ADD COLUMNS (avg_order_value DECIMAL(10,2) COMMENT '客单价');
   
   UPDATE dws_sales_daily 
   SET avg_order_value = CASE 
       WHEN order_count > 0 THEN order_amount_sum / order_count 
       ELSE 0 
   END;
   ```
5. 输出完整的实施方案

### 场景2：维度表处理

**用户请求：** "创建用户维度表，支持SCD Type 2，包含用户基本信息变化历史"

**技能处理：**
1. 识别为DIM层、维度数据、创建表操作
2. 选择`dim_create_table_scd2`工作流
3. 提供表结构设计：
   - 代理键（user_sk）
   - 业务键（user_id）
   - 维度属性（user_name, email, phone等）
   - 时间有效性字段（valid_from, valid_to, is_current）
4. 提供SCD Type 2处理逻辑
5. 输出完整的维度表设计方案

### 场景3：数据质量检查

**用户请求：** "检查DWD层订单数据的完整性，重点检查金额字段和订单状态"

**技能处理：**
1. 识别为DWD层、交易数据、质量检查操作
2. 选择`dwd_data_cleaning`工作流中的质量检查步骤
3. 提供数据质量检查SQL：
   ```sql
   SELECT 
       'dwd_order_detail' AS table_name,
       COUNT(*) AS total_records,
       SUM(CASE WHEN order_amount IS NULL THEN 1 ELSE 0 END) AS null_amount_count,
       SUM(CASE WHEN order_amount < 0 THEN 1 ELSE 0 END) AS negative_amount_count,
       SUM(CASE WHEN order_status NOT IN ('pending', 'paid', 'shipped', 'completed', 'cancelled') THEN 1 ELSE 0 END) AS invalid_status_count
   FROM dwd_order_detail
   WHERE data_date = '2024-03-11';
   ```
4. 输出数据质量报告和问题建议

## 高级功能

### 1. 自定义工作流

可以创建自定义工作流来满足特定需求：

```python
# 创建自定义工作流
custom_steps = [
    WorkflowStep(
        step_id="custom_001",
        step_name="自定义数据转换",
        description="特定的业务数据转换逻辑",
        layer=DataLayer.DWD,
        operation_type=OperationType.PROCESS_DATA,
        processing_logic=custom_logic,
        dependencies=[],
        parameters={"conversion_rule": "business_specific"},
        expected_duration=30
    )
]

scheduler.create_custom_workflow("custom_workflow", custom_steps)
```

### 2. 工作流依赖管理

支持复杂的工作流依赖关系：

```python
# 定义有依赖关系的工作流步骤
dependent_steps = [
    WorkflowStep(
        step_id="step1",
        step_name="数据准备",
        description="数据准备步骤",
        layer=DataLayer.DWD,
        operation_type=OperationType.PROCESS_DATA,
        dependencies=[],  # 无依赖
        expected_duration=20
    ),
    WorkflowStep(
        step_id="step2",
        step_name="数据处理",
        description="数据处理步骤",
        layer=DataLayer.DWD,
        operation_type=OperationType.PROCESS_DATA,
        dependencies=["step1"],  # 依赖step1
        expected_duration=30
    ),
    WorkflowStep(
        step_id="step3",
        step_name="数据验证",
        description="数据验证步骤",
        layer=DataLayer.DWD,
        operation_type=OperationType.QUALITY_CHECK,
        dependencies=["step2"],  # 依赖step2
        expected_duration=15
    )
]
```

### 3. 配置导出和导入

```python
# 导出当前配置
scheduler.export_workflow_config("backup/config_20240311.json")

# 从文件导入配置（需要实现import方法）
# scheduler.import_workflow_config("saved_config.json")
```

## 最佳实践

### 1. 表命名规范
- DWD层：`dwd_{业务主题}_{粒度}_detail`，如`dwd_order_daily_detail`
- DIM层：`dim_{维度名称}`，如`dim_user`、`dim_product`
- DWS层：`dws_{业务主题}_{粒度}_{周期}`，如`dws_sales_daily`、`dws_sales_monthly`
- ADS层：`ads_{应用名称}_{报表类型}`，如`ads_sales_dashboard`、`ads_user_profile`

### 2. 字段命名规范
- ID字段：`{实体}_id`，如`user_id`、`order_id`
- 名称字段：`{实体}_name`，如`user_name`、`product_name`
- 时间字段：`{事件}_time`，如`create_time`、`update_time`
- 金额字段：`{业务}_amount`，如`order_amount`、`payment_amount`
- 数量字段：`{业务}_count`，如`order_count`、`user_count`

### 3. 数据质量检查点
- 完整性：关键字段非空率 > 95%
- 一致性：与源系统数据差异 < 2%
- 准确性：业务规则验证通过率 > 99%
- 及时性：数据延迟 < 24小时

### 4. 性能优化建议
- 合理分区：按时间分区，保留最近90天热数据
- 列式存储：使用Parquet格式，Snappy压缩
- 统计信息：定期收集表和列的统计信息
- 索引优化：对高频查询字段创建索引

## 故障排除

### 常见问题

**Q1：无法识别用户请求的工作流**
- 确保请求中包含明确的数据层关键词（DWD、DIM、DWS、ADS）
- 明确指定操作类型（增加、修改、创建、处理等）
- 可以提供更具体的业务场景描述

**Q2：工作流执行失败**
- 检查数据库连接配置
- 验证表结构是否存在
- 查看详细日志文件`data_warehouse_workflow.log`

**Q3：SQL模板参数替换错误**
- 确保所有模板参数`{param_name}`都被正确替换
- 检查参数值的类型和格式
- 参考`assets/sql_templates.sql`中的参数说明

### 日志查看

```bash
# 查看工作流执行日志
tail -f data_warehouse_workflow.log

# 查看错误日志
tail -f data_warehouse_error.log

# 查看审计日志
tail -f data_warehouse_audit.log
```

## 扩展开发

### 添加新的加工逻辑

1. 在`scripts/`目录下创建新的Python处理脚本
2. 在`workflow_scheduler.py`中注册新的ProcessingLogic
3. 更新决策树以包含新的工作流路径

### 添加新的SQL模板

1. 在`assets/sql_templates.sql`中添加新的SQL模板
2. 在对应的工作流步骤中引用模板
3. 更新文档说明新的模板使用方法

### 集成外部系统

技能可以集成以下外部系统：
- 调度系统：Airflow、DolphinScheduler、Azkaban
- 监控系统：Prometheus、Grafana、ELK
- 元数据管理：Atlas、DataHub、Amundsen
- 数据质量：Great Expectations、Deequ、Soda

## 版本历史

- **v1.0.0** (2024-03-11): 初始版本发布
  - 支持四层数据架构
  - 实现工作流决策树
  - 提供基础Python加工逻辑
  - 包含完整SQL模板库

## 贡献指南

欢迎贡献代码、文档、用例或提出改进建议。

1. Fork本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建Pull Request

## 许可证

本技能遵循MIT许可证。详见LICENSE文件。

## 联系方式

- 问题反馈：提交GitHub Issue
- 功能建议：提交GitHub Discussion
- 紧急支持：联系系统管理员

---

**温馨提示：** 使用本技能前，请确保已了解基本的数据仓库概念和分层架构原理。对于复杂的业务场景，建议先在小规模数据上测试验证。