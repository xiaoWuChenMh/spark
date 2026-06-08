-- 数仓数据加工SQL模板库
-- 包含各层表创建、数据加工、查询优化等常用SQL模板

-- ============================================================================
-- DWD层（明细数据层）SQL模板
-- ============================================================================

-- 模板1：DWD层增量表创建
CREATE TABLE IF NOT EXISTS dwd_{table_name}_detail (
    -- 业务主键
    {business_key}_id STRING COMMENT '{业务主键ID}',
    
    -- 业务属性字段
    {attribute_columns},
    
    -- 时间字段
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    event_time TIMESTAMP COMMENT '业务发生时间',
    
    -- 数据质量标记
    data_date DATE COMMENT '数据日期',
    is_valid INT COMMENT '数据是否有效(1/0)',
    data_source STRING COMMENT '数据来源',
    process_time TIMESTAMP COMMENT '处理时间'
)
COMMENT '{表中文名}明细表，DWD层'
PARTITIONED BY (data_date STRING)
STORED AS PARQUET
LOCATION '/data/warehouse/dwd/{table_name}_detail';

-- 模板2：DWD层增量数据插入
INSERT OVERWRITE TABLE dwd_{table_name}_detail PARTITION (data_date='{target_date}')
SELECT 
    {business_key}_id,
    {attribute_columns},
    create_time,
    update_time,
    event_time,
    '{target_date}' AS data_date,
    1 AS is_valid,
    '{source_system}' AS data_source,
    CURRENT_TIMESTAMP() AS process_time
FROM 
    {source_table}
WHERE 
    -- 增量条件
    update_time >= DATE_SUB('{target_date}', {incremental_days})
    AND update_time < DATE_ADD('{target_date}', 1)
    -- 数据质量过滤条件
    AND {quality_filters};

-- ============================================================================
-- DIM层（维度数据层）SQL模板
-- ============================================================================

-- 模板3：DIM层SCD Type 2表创建
CREATE TABLE IF NOT EXISTS dim_{dimension_name} (
    -- 代理键
    {dimension_name}_sk BIGINT COMMENT '{维度名}代理键',
    
    -- 业务自然键
    {dimension_name}_id STRING COMMENT '{维度名}业务ID',
    
    -- 维度属性
    {attribute_columns},
    
    -- 时间有效性
    valid_from DATE COMMENT '有效开始日期',
    valid_to DATE COMMENT '有效结束日期',
    is_current INT COMMENT '是否当前版本(1/0)',
    
    -- 技术字段
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT '{维度中文名}维度表，支持SCD Type 2'
STORED AS PARQUET
LOCATION '/data/warehouse/dim/{dimension_name}';

-- 模板4：DIM层SCD Type 2数据处理
WITH 
-- 当前维度数据
current_dim AS (
    SELECT * 
    FROM dim_{dimension_name} 
    WHERE is_current = 1
),
-- 新的维度数据
new_dim_data AS (
    SELECT 
        {dimension_name}_id,
        {attribute_columns},
        CURRENT_DATE() AS change_date
    FROM 
        {source_table}
    WHERE 
        data_date = '{target_date}'
),
-- 识别变化
changes AS (
    SELECT 
        n.{dimension_name}_id,
        {attribute_comparisons},
        CASE 
            WHEN c.{dimension_name}_id IS NULL THEN '新增'
            WHEN {change_conditions} THEN '更新'
            ELSE '无变化'
        END AS change_type
    FROM 
        new_dim_data n
        LEFT JOIN current_dim c ON n.{dimension_name}_id = c.{dimension_name}_id
)
-- 生成新的维度数据
INSERT OVERWRITE TABLE dim_{dimension_name}
SELECT 
    -- 原有记录（关闭）
    c.{dimension_name}_sk,
    c.{dimension_name}_id,
    c.{attribute_columns},
    c.valid_from,
    CASE 
        WHEN ch.change_type = '更新' THEN DATE_SUB('{target_date}', 1)
        ELSE c.valid_to
    END AS valid_to,
    CASE 
        WHEN ch.change_type = '更新' THEN 0
        ELSE c.is_current
    END AS is_current,
    c.create_time,
    CURRENT_TIMESTAMP() AS update_time
FROM 
    current_dim c
    LEFT JOIN changes ch ON c.{dimension_name}_id = ch.{dimension_name}_id
WHERE 
    ch.change_type IS NULL OR ch.change_type = '更新'

UNION ALL

SELECT 
    -- 新增/更新记录
    COALESCE(c.{dimension_name}_sk, ROW_NUMBER() OVER (ORDER BY n.{dimension_name}_id) + (SELECT COALESCE(MAX({dimension_name}_sk), 0) FROM dim_{dimension_name})) AS {dimension_name}_sk,
    n.{dimension_name}_id,
    n.{attribute_columns},
    CASE 
        WHEN ch.change_type = '新增' THEN '{target_date}'
        WHEN ch.change_type = '更新' THEN '{target_date}'
        ELSE c.valid_from
    END AS valid_from,
    DATE '9999-12-31' AS valid_to,
    1 AS is_current,
    CASE 
        WHEN ch.change_type = '新增' THEN CURRENT_TIMESTAMP()
        ELSE c.create_time
    END AS create_time,
    CURRENT_TIMESTAMP() AS update_time
FROM 
    new_dim_data n
    LEFT JOIN current_dim c ON n.{dimension_name}_id = c.{dimension_name}_id
    LEFT JOIN changes ch ON n.{dimension_name}_id = ch.{dimension_name}_id
WHERE 
    ch.change_type IN ('新增', '更新');

-- ============================================================================
-- DWS层（汇总数据层）SQL模板
-- ============================================================================

-- 模板5：DWS层日汇总表创建
CREATE TABLE IF NOT EXISTS dws_{subject_name}_daily (
    -- 时间维度
    date_key INT COMMENT '日期键，格式YYYYMMDD',
    date_str STRING COMMENT '日期字符串，格式YYYY-MM-DD',
    
    -- 业务维度
    {dimension_columns},
    
    -- 汇总指标
    {measure_columns},
    
    -- 衍生指标
    {derived_columns},
    
    -- 技术字段
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT '{主题中文名}日汇总表，DWS层'
PARTITIONED BY (date_key INT)
STORED AS PARQUET
LOCATION '/data/warehouse/dws/{subject_name}_daily';

-- 模板6：DWS层日汇总数据加工
INSERT OVERWRITE TABLE dws_{subject_name}_daily PARTITION (date_key={date_key})
SELECT 
    {date_key} AS date_key,
    '{date_str}' AS date_str,
    
    -- 维度字段
    {dimension_expressions},
    
    -- 基础指标
    SUM({measure1}) AS {measure1}_sum,
    AVG({measure1}) AS {measure1}_avg,
    COUNT(DISTINCT {measure1_id}) AS {measure1}_count,
    MAX({measure1}) AS {measure1}_max,
    MIN({measure1}) AS {measure1}_min,
    
    -- 更多指标...
    {additional_measures},
    
    -- 衍生指标
    CASE 
        WHEN SUM({measure2}) > 0 THEN SUM({measure1}) / SUM({measure2})
        ELSE 0 
    END AS {derived_ratio},
    
    {additional_derived},
    
    -- 技术字段
    CURRENT_TIMESTAMP() AS create_time,
    CURRENT_TIMESTAMP() AS update_time
FROM 
    dwd_{source_table}_detail
WHERE 
    data_date = '{date_str}'
    AND is_valid = 1
GROUP BY 
    {dimension_grouping};

-- 模板7：DWS层周/月/年汇总
CREATE TABLE IF NOT EXISTS dws_{subject_name}_{period} (
    -- 时间维度
    {period}_key STRING COMMENT '{周期}键',
    {period}_label STRING COMMENT '{周期}标签',
    
    -- 业务维度
    {dimension_columns},
    
    -- 周期汇总指标（从日汇总上卷）
    {period_measures},
    
    -- 周期特有指标
    {period_specific_measures},
    
    -- 技术字段
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT '{主题中文名}{周期}汇总表，DWS层'
PARTITIONED BY ({period}_key STRING)
STORED AS PARQUET
LOCATION '/data/warehouse/dws/{subject_name}_{period}';

-- 模板8：DWS层周期上卷
INSERT OVERWRITE TABLE dws_{subject_name}_{period} PARTITION ({period}_key='{period_value}')
SELECT 
    '{period_value}' AS {period}_key,
    '{period_label}' AS {period}_label,
    
    -- 维度字段
    {dimension_columns},
    
    -- 从日汇总上卷
    SUM({measure}_sum) AS {measure}_sum,
    AVG({measure}_avg) AS {measure}_avg,
    SUM({measure}_count) AS {measure}_count,
    MAX({measure}_max) AS {measure}_max,
    MIN({measure}_min) AS {measure}_min,
    
    -- 周期特有指标
    COUNT(DISTINCT date_key) AS active_days,
    CASE 
        WHEN COUNT(DISTINCT date_key) > 0 
        THEN SUM({measure}_sum) / COUNT(DISTINCT date_key)
        ELSE 0
    END AS daily_avg_{measure},
    
    {additional_period_metrics},
    
    -- 技术字段
    CURRENT_TIMESTAMP() AS create_time,
    CURRENT_TIMESTAMP() AS update_time
FROM 
    dws_{subject_name}_daily
WHERE 
    -- 时间范围条件
    date_key >= {start_date_key}
    AND date_key <= {end_date_key}
GROUP BY 
    {dimension_grouping};

-- ============================================================================
-- ADS层（应用数据层）SQL模板
-- ============================================================================

-- 模板9：ADS层宽表创建
CREATE TABLE IF NOT EXISTS ads_{application_name} (
    -- 主体标识
    {subject}_id STRING COMMENT '{主体}ID',
    {subject}_name STRING COMMENT '{主体}名称',
    
    -- 多维度属性
    {dimension_attributes},
    
    -- 复合指标
    {composite_metrics},
    
    -- 排名指标
    {ranking_columns},
    
    -- 趋势指标
    {trend_metrics},
    
    -- 标签字段
    {tag_columns},
    
    -- 技术字段
    create_time TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP COMMENT '更新时间',
    data_date DATE COMMENT '数据日期'
)
COMMENT '{应用中文名}宽表，ADS层'
PARTITIONED BY (data_date DATE)
STORED AS PARQUET
LOCATION '/data/warehouse/ads/{application_name}';

-- 模板10：ADS层宽表数据加工
WITH 
-- 基础数据
base_data AS (
    SELECT 
        {subject}_id,
        {subject}_name,
        {base_attributes},
        {base_metrics}
    FROM 
        {source_table1}
    WHERE 
        data_date = '{target_date}'
),
-- 关联维度数据
dim_joined AS (
    SELECT 
        b.*,
        d1.{dimension_attribute1},
        d1.{dimension_attribute2},
        d2.{dimension_attribute3}
    FROM 
        base_data b
        LEFT JOIN dim_{dimension1} d1 ON b.{dimension_key1} = d1.{dimension_key1} AND d1.is_current = 1
        LEFT JOIN dim_{dimension2} d2 ON b.{dimension_key2} = d2.{dimension_key2} AND d2.is_current = 1
),
-- 关联汇总数据
agg_joined AS (
    SELECT 
        d.*,
        a1.{aggregate_metric1},
        a1.{aggregate_metric2},
        a2.{aggregate_metric3}
    FROM 
        dim_joined d
        LEFT JOIN dws_{aggregate1} a1 ON d.{aggregate_key1} = a1.{aggregate_key1} AND a1.date_key = {date_key}
        LEFT JOIN dws_{aggregate2} a2 ON d.{aggregate_key2} = a2.{aggregate_key2} AND a2.{period}_key = '{period_value}'
),
-- 指标计算
metrics_calculated AS (
    SELECT 
        *,
        -- 复合指标
        CASE 
            WHEN {condition1} THEN {calculation1}
            WHEN {condition2} THEN {calculation2}
            ELSE {default_calculation}
        END AS {composite_metric1},
        
        -- 排名指标
        ROW_NUMBER() OVER (PARTITION BY {partition_column} ORDER BY {order_metric} DESC) AS {ranking_metric},
        
        -- 趋势指标
        {trend_calculation} AS {trend_metric},
        
        -- 标签
        CASE 
            WHEN {tag_condition1} THEN '{tag_value1}'
            WHEN {tag_condition2} THEN '{tag_value2}'
            ELSE '{default_tag}'
        END AS {tag_column}
    FROM 
        agg_joined
)
-- 插入宽表
INSERT OVERWRITE TABLE ads_{application_name} PARTITION (data_date='{target_date}')
SELECT 
    {subject}_id,
    {subject}_name,
    {dimension_attributes},
    {composite_metrics},
    {ranking_columns},
    {trend_metrics},
    {tag_columns},
    CURRENT_TIMESTAMP() AS create_time,
    CURRENT_TIMESTAMP() AS update_time,
    '{target_date}' AS data_date
FROM 
    metrics_calculated;

-- ============================================================================
-- 数据质量SQL模板
-- ============================================================================

-- 模板11：数据完整性检查
SELECT 
    '{table_name}' AS table_name,
    '{check_date}' AS check_date,
    COUNT(*) AS total_records,
    -- 字段空值检查
    SUM(CASE WHEN {field1} IS NULL THEN 1 ELSE 0 END) AS {field1}_null_count,
    SUM(CASE WHEN {field2} IS NULL THEN 1 ELSE 0 END) AS {field2}_null_count,
    -- 字段值域检查
    SUM(CASE WHEN {field3} NOT IN ({valid_values}) THEN 1 ELSE 0 END) AS {field3}_invalid_count,
    SUM(CASE WHEN {field4} < 0 THEN 1 ELSE 0 END) AS {field4}_negative_count,
    -- 业务规则检查
    SUM(CASE WHEN NOT ({business_rule1}) THEN 1 ELSE 0 END) AS rule1_violation_count,
    SUM(CASE WHEN NOT ({business_rule2}) THEN 1 ELSE 0 END) AS rule2_violation_count,
    -- 完整性率
    (COUNT(*) - SUM(CASE WHEN {field1} IS NULL THEN 1 ELSE 0 END)) * 1.0 / COUNT(*) AS {field1}_completeness_rate
FROM 
    {table_name}
WHERE 
    data_date = '{check_date}';

-- 模板12：数据一致性检查
SELECT 
    '{check_date}' AS check_date,
    '一致性检查' AS check_type,
    -- 与历史数据对比
    COUNT(*) AS current_count,
    (SELECT COUNT(*) FROM {table_name}_history WHERE data_date = DATE_SUB('{check_date}', 1)) AS history_count,
    COUNT(*) - (SELECT COUNT(*) FROM {table_name}_history WHERE data_date = DATE_SUB('{check_date}', 1)) AS count_diff,
    -- 关键指标对比
    SUM({key_metric}) AS current_total,
    (SELECT SUM({key_metric}) FROM {table_name}_history WHERE data_date = DATE_SUB('{check_date}', 1)) AS history_total,
    SUM({key_metric}) - (SELECT SUM({key_metric}) FROM {table_name}_history WHERE data_date = DATE_SUB('{check_date}', 1)) AS metric_diff,
    -- 波动率
    CASE 
        WHEN (SELECT SUM({key_metric}) FROM {table_name}_history WHERE data_date = DATE_SUB('{check_date}', 1)) > 0
        THEN (SUM({key_metric}) - (SELECT SUM({key_metric}) FROM {table_name}_history WHERE data_date = DATE_SUB('{check_date}', 1))) * 1.0 / 
             (SELECT SUM({key_metric}) FROM {table_name}_history WHERE data_date = DATE_SUB('{check_date}', 1))
        ELSE NULL
    END AS metric_change_rate
FROM 
    {table_name}
WHERE 
    data_date = '{check_date}';

-- ============================================================================
-- 性能优化SQL模板
-- ============================================================================

-- 模板13：分区优化
-- 查看分区信息
SHOW PARTITIONS {table_name};

-- 添加分区
ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (data_date='{new_date}');

-- 删除旧分区
ALTER TABLE {table_name} DROP IF EXISTS PARTITION (data_date < '{retention_date}');

-- 模板14：索引优化
-- 创建索引
CREATE INDEX {index_name} ON TABLE {table_name} ({index_columns})
AS 'COMPACT'
WITH DEFERRED REBUILD;

-- 重建索引
ALTER INDEX {index_name} ON {table_name} REBUILD;

-- 模板15：统计信息收集
-- 收集表统计信息
ANALYZE TABLE {table_name} COMPUTE STATISTICS;

-- 收集列统计信息
ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR COLUMNS {column_list};

-- 收集分区统计信息
ANALYZE TABLE {table_name} PARTITION (data_date='{partition_date}') COMPUTE STATISTICS;

-- ============================================================================
-- 数据血缘SQL模板
-- ============================================================================

-- 模板16：数据血缘查询
WITH RECURSIVE 
-- 获取表依赖关系
table_deps AS (
    SELECT 
        dependent_table,
        source_table,
        dependency_type
    FROM 
        table_dependencies
    WHERE 
        dependent_table = '{target_table}'
    
    UNION ALL
    
    SELECT 
        td.dependent_table,
        td.source_table,
        td.dependency_type
    FROM 
        table_dependencies td
        INNER JOIN table_deps d ON td.dependent_table = d.source_table
)
SELECT 
    dependent_table,
    source_table,
    dependency_type,
    LEVEL
FROM 
    table_deps
ORDER BY 
    LEVEL, dependent_table;

-- 模板17：影响分析
SELECT 
    dependent_table,
    table_type,
    business_owner,
    last_update_time
FROM 
    table_metadata
WHERE 
    table_name IN (
        SELECT DISTINCT dependent_table
        FROM table_dependencies
        WHERE source_table = '{source_table}'
    );

-- ============================================================================
-- 参数说明和使用示例
-- ============================================================================

-- 使用示例1：创建DWD层订单明细表
-- 替换参数：
--   {table_name} -> order
--   {business_key} -> order
--   {attribute_columns} -> 实际字段定义
--   {表中文名} -> 订单

-- 使用示例2：DWS层销售日汇总
-- 替换参数：
--   {subject_name} -> sales
--   {date_key} -> 20240311
--   {date_str} -> '2024-03-11'
--   {dimension_expressions} -> 实际维度表达式
--   {measure1} -> order_amount
--   {measure1_id} -> order_id
--   {dimension_grouping} -> 实际分组字段

-- 使用示例3：ADS层用户画像宽表
-- 替换参数：
--   {application_name} -> user_profile
--   {subject} -> user
--   {target_date} -> '2024-03-11'
--   {date_key} -> 20240311
--   {period_value} -> 实际周期值

-- 注意事项：
-- 1. 所有模板中的参数需要根据实际业务需求替换
-- 2. 表名、字段名需符合命名规范
-- 3. 分区策略根据数据量和使用频率选择
-- 4. 性能优化根据实际查询模式调整
-- 5. 数据质量检查需定期执行并记录结果