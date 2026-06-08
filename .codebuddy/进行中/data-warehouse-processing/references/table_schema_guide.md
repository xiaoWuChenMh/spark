# 表结构设计指南

## 概述

本指南提供数仓各层表结构设计的最佳实践，包括字段命名规范、数据类型选择、主键设计、索引策略等。

## 分层表结构特征

### DWD层（明细数据层）

**核心特征**：
- 保留最细粒度的业务数据
- 包含完整的业务过程信息
- 数据量最大，更新频率最高

**设计原则**：
1. **字段完整性**：包含业务过程所有相关信息
2. **数据原始性**：尽量保留原始数据格式
3. **时间标记**：必须有数据产生时间、采集时间、处理时间
4. **业务键**：包含业务主键，用于数据溯源

**典型字段**：
- 业务主键（如order_id, user_id）
- 时间字段（create_time, update_time）
- 状态字段（status, state）
- 业务属性字段
- 数据质量标记（is_valid, data_source）

**示例表结构**：
```sql
CREATE TABLE dwd_order_detail (
    order_id STRING COMMENT '订单ID',
    user_id STRING COMMENT '用户ID',
    product_id STRING COMMENT '产品ID',
    order_amount DECIMAL(18,2) COMMENT '订单金额',
    order_quantity INT COMMENT '订单数量',
    order_time TIMESTAMP COMMENT '订单时间',
    order_status STRING COMMENT '订单状态',
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    data_date DATE COMMENT '数据日期',
    is_valid INT COMMENT '数据是否有效(1/0)'
) COMMENT '订单明细表';
```

### DIM层（维度数据层）

**核心特征**：
- 描述性数据，业务实体属性
- 数据相对稳定，变化缓慢
- 支持缓慢变化维度（SCD）

**设计原则**：
1. **代理键**：使用代理键作为主键
2. **属性完整**：包含所有相关属性
3. **历史跟踪**：支持属性变化历史
4. **层次结构**：支持多级层次关系

**典型字段**：
- 代理键（dim_key, sk）
- 业务自然键（business_key）
- 属性字段（name, type, level等）
- 时间有效性字段（valid_from, valid_to）
- 当前标记（is_current）

**示例表结构**：
```sql
CREATE TABLE dim_user (
    user_sk BIGINT COMMENT '用户代理键',
    user_id STRING COMMENT '用户业务键',
    user_name STRING COMMENT '用户姓名',
    user_type STRING COMMENT '用户类型',
    register_date DATE COMMENT '注册日期',
    user_level STRING COMMENT '用户等级',
    valid_from DATE COMMENT '有效开始日期',
    valid_to DATE COMMENT '有效结束日期',
    is_current INT COMMENT '是否当前版本(1/0)',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT '用户维度表';
```

### DWS层（汇总数据层）

**核心特征**：
- 按主题域轻度汇总
- 包含预计算指标
- 支持多维度分析

**设计原则**：
1. **主题域划分**：按业务主题组织数据
2. **预计算指标**：包含常用聚合指标
3. **时间粒度**：明确时间维度（日/周/月）
4. **维度组合**：支持常用维度组合

**典型字段**：
- 维度组合键（dim_combo_key）
- 时间维度（date_key, week_key, month_key）
- 业务维度（product_id, user_id, region_id）
- 聚合指标（total_amount, order_count, avg_amount）
- 衍生指标（growth_rate, percentage）

**示例表结构**：
```sql
CREATE TABLE dws_user_daily (
    date_key INT COMMENT '日期键',
    user_id STRING COMMENT '用户ID',
    order_count BIGINT COMMENT '订单数量',
    total_amount DECIMAL(18,2) COMMENT '总金额',
    avg_amount DECIMAL(18,2) COMMENT '平均金额',
    max_amount DECIMAL(18,2) COMMENT '最大金额',
    min_amount DECIMAL(18,2) COMMENT '最小金额',
    distinct_product_count INT COMMENT '商品种类数',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT '用户日汇总表';
```

### ADS层（应用数据层）

**核心特征**：
- 面向应用的宽表
- 高度汇总，多表整合
- 直接支持报表和业务分析

**设计原则**：
1. **宽表设计**：减少关联，提高查询性能
2. **业务指标**：包含完整的业务指标体系
3. **应用导向**：按应用场景设计表结构
4. **数据冗余**：允许适当冗余以提高性能

**典型字段**：
- 业务主体键（subject_key）
- 多维度属性（dimension_attributes）
- 复合指标（composite_metrics）
- 排名指标（ranking_metrics）
- 趋势指标（trend_metrics）

**示例表结构**：
```sql
CREATE TABLE ads_user_profile (
    user_id STRING COMMENT '用户ID',
    user_name STRING COMMENT '用户姓名',
    user_type STRING COMMENT '用户类型',
    register_date DATE COMMENT '注册日期',
    total_order_count BIGINT COMMENT '累计订单数',
    total_order_amount DECIMAL(18,2) COMMENT '累计订单金额',
    avg_order_amount DECIMAL(18,2) COMMENT '平均订单金额',
    last_order_date DATE COMMENT '最近订单日期',
    last_order_amount DECIMAL(18,2) COMMENT '最近订单金额',
    favorite_category STRING COMMENT '偏好品类',
    purchase_frequency DECIMAL(10,2) COMMENT '购买频率',
    user_value_score DECIMAL(10,2) COMMENT '用户价值评分',
    create_time TIMESTAMP COMMENT '创建时间'
) COMMENT '用户画像宽表';
```

## 字段命名规范

### 通用规则
1. **小写字母**：全部使用小写字母
2. **下划线分隔**：使用下划线连接单词
3. **避免缩写**：尽量使用完整单词
4. **语义明确**：字段名应反映数据含义

### 前缀规则
- `dim_`：维度表前缀
- `fact_`：事实表前缀
- `dwd_`：明细数据层前缀
- `dws_`：汇总数据层前缀
- `ads_`：应用数据层前缀
- `tmp_`：临时表前缀

### 后缀规则
- `_id`：标识字段
- `_key`：键字段
- `_name`：名称字段
- `_type`：类型字段
- `_date`：日期字段
- `_time`：时间字段
- `_amount`：金额字段
- `_count`：计数字段
- `_rate`：比率字段

## 数据类型选择

### 数值类型
- **整型**：INT, BIGINT（用于计数、ID）
- **小数**：DECIMAL(18,2)（用于金额、百分比）
- **浮点**：FLOAT, DOUBLE（用于科学计算）

### 字符类型
- **短文本**：STRING, VARCHAR(255)（用于名称、代码）
- **长文本**：TEXT（用于描述、备注）
- **固定长度**：CHAR(10)（用于固定格式代码）

### 时间类型
- **日期**：DATE（用于日期）
- **时间戳**：TIMESTAMP（用于精确时间）
- **时间间隔**：INTERVAL（用于时长）

### 布尔类型
- **布尔值**：BOOLEAN, TINYINT(1)（用于标记位）

## 主键与索引设计

### 主键策略
1. **代理键**：维度表使用自增代理键
2. **自然键**：事实表使用业务自然键组合
3. **复合主键**：多字段组合主键

### 索引策略
1. **查询频率**：高频查询字段建立索引
2. **关联字段**：关联使用的字段建立索引
3. **排序字段**：排序使用的字段建立索引
4. **分区字段**：分区字段通常不需要索引

### 分区策略
1. **时间分区**：按日期分区（最常用）
2. **范围分区**：按数值范围分区
3. **列表分区**：按离散值分区
4. **哈希分区**：均匀分布数据

## 数据质量约束

### 非空约束
- 主键字段必须非空
- 关键业务字段建议非空
- 时间字段必须非空

### 唯一约束
- 主键必须唯一
- 业务键建议唯一
- 组合唯一约束用于防止重复

### 检查约束
- 数值范围检查
- 代码值域检查
- 格式正则检查

### 外键约束
- 维度-事实关联建议外键约束
- 跨层关联建议外键约束
- 同一层内部关联可选外键约束

## 表注释规范

### 表级别注释
- 说明表的业务含义
- 说明表所属分层
- 说明表的主要用途

### 字段级别注释
- 说明字段的业务含义
- 说明字段的数据来源
- 说明字段的计算规则（如果是计算字段）
- 说明字段的取值范围

### 示例
```sql
-- 表注释
COMMENT '订单明细事实表，存储所有订单的明细数据，属于DWD层'

-- 字段注释
COMMENT '订单金额，单位为元，精确到分，来源于订单系统的amount字段'
```

## 变更管理

### 字段增加
1. 评估对下游的影响
2. 确定默认值策略
3. 更新相关文档
4. 通知相关团队

### 字段修改
1. 评估数据类型兼容性
2. 制定数据迁移方案
3. 保证向下兼容
4. 分阶段实施

### 字段删除
1. 确认不再使用
2. 评估对历史数据的影响
3. 保留历史版本备份
4. 更新所有相关代码