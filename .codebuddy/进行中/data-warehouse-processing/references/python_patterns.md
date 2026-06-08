# Python加工模式库

## 概述

本库提供数仓数据加工的常用Python模式，包括数据处理、转换、聚合、质量检查等场景的代码模板和最佳实践。

## 基础数据处理模式

### 数据读取模式

**CSV文件读取**：
```python
def read_csv_file(file_path, **kwargs):
    """
    读取CSV文件的通用模式
    
    参数:
        file_path: CSV文件路径
        **kwargs: pandas.read_csv的其他参数
        
    返回:
        DataFrame
    """
    import pandas as pd
    
    default_params = {
        'encoding': 'utf-8',
        'sep': ',',
        'quotechar': '"',
        'escapechar': '\\\\',
        'low_memory': False
    }
    
    # 合并默认参数和用户参数
    params = {**default_params, **kwargs}
    
    try:
        df = pd.read_csv(file_path, **params)
        print(f"成功读取文件: {file_path}, 记录数: {len(df)}")
        return df
    except Exception as e:
        print(f"读取文件失败: {file_path}, 错误: {e}")
        return pd.DataFrame()
```

**数据库读取**：
```python
def read_from_database(query, connection_string):
    """
    从数据库读取数据的通用模式
    """
    import pandas as pd
    import sqlalchemy
    
    try:
        engine = sqlalchemy.create_engine(connection_string)
        df = pd.read_sql(query, engine)
        print(f"成功读取数据，记录数: {len(df)}")
        return df
    except Exception as e:
        print(f"数据库读取失败: {e}")
        return pd.DataFrame()
    finally:
        if 'engine' in locals():
            engine.dispose()
```

### 数据清洗模式

**空值处理模式**：
```python
def handle_missing_values(df, strategy='default'):
    """
    处理缺失值的通用模式
    
    策略选项:
        'default': 数值用0，字符用空字符串，日期用None
        'mean': 数值用均值填充
        'median': 数值用中位数填充
        'mode': 用众数填充
        'ffill': 前向填充
        'bfill': 后向填充
        'drop': 删除包含空值的行
    """
    df_clean = df.copy()
    
    for col in df_clean.columns:
        null_count = df_clean[col].isnull().sum()
        if null_count > 0:
            print(f"字段 {col} 有 {null_count} 个空值")
            
            if strategy == 'drop':
                df_clean = df_clean.dropna(subset=[col])
                print(f"已删除包含字段 {col} 空值的行")
                
            elif strategy == 'mean' and df_clean[col].dtype in ['int64', 'float64']:
                fill_value = df_clean[col].mean()
                df_clean[col] = df_clean[col].fillna(fill_value)
                print(f"字段 {col} 用均值 {fill_value} 填充")
                
            elif strategy == 'median' and df_clean[col].dtype in ['int64', 'float64']:
                fill_value = df_clean[col].median()
                df_clean[col] = df_clean[col].fillna(fill_value)
                print(f"字段 {col} 用中位数 {fill_value} 填充")
                
            elif strategy == 'mode':
                fill_value = df_clean[col].mode()[0] if not df_clean[col].mode().empty else None
                if fill_value is not None:
                    df_clean[col] = df_clean[col].fillna(fill_value)
                    print(f"字段 {col} 用众数 {fill_value} 填充")
                    
            elif strategy == 'ffill':
                df_clean[col] = df_clean[col].ffill()
                print(f"字段 {col} 前向填充")
                
            elif strategy == 'bfill':
                df_clean[col] = df_clean[col].bfill()
                print(f"字段 {col} 后向填充")
                
            else:  # default策略
                if df_clean[col].dtype in ['int64', 'float64']:
                    df_clean[col] = df_clean[col].fillna(0)
                    print(f"字段 {col} 用0填充")
                elif df_clean[col].dtype == 'object':
                    df_clean[col] = df_clean[col].fillna('')
                    print(f"字段 {col} 用空字符串填充")
                else:
                    # 其他类型保持None
                    pass
    
    return df_clean
```

**异常值检测模式**：
```python
def detect_outliers(df, column, method='iqr', threshold=1.5):
    """
    检测异常值的通用模式
    
    方法选项:
        'iqr': IQR方法（默认）
        'zscore': Z-score方法
        'percentile': 百分位方法
    """
    if column not in df.columns:
        return [], {}
    
    data = df[column].dropna()
    
    if method == 'iqr':
        # IQR方法
        q1 = data.quantile(0.25)
        q3 = data.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr
        
        outliers = data[(data < lower_bound) | (data > upper_bound)]
        
        stats = {
            'method': 'iqr',
            'q1': q1,
            'q3': q3,
            'iqr': iqr,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'outlier_count': len(outliers),
            'outlier_rate': len(outliers) / len(data) if len(data) > 0 else 0
        }
        
    elif method == 'zscore':
        # Z-score方法
        mean = data.mean()
        std = data.std()
        
        if std == 0:
            outliers = pd.Series([], dtype=data.dtype)
        else:
            z_scores = (data - mean) / std
            outliers = data[abs(z_scores) > threshold]
        
        stats = {
            'method': 'zscore',
            'mean': mean,
            'std': std,
            'threshold': threshold,
            'outlier_count': len(outliers),
            'outlier_rate': len(outliers) / len(data) if len(data) > 0 else 0
        }
        
    elif method == 'percentile':
        # 百分位方法
        lower_percentile = threshold
        upper_percentile = 100 - threshold
        
        lower_bound = data.quantile(lower_percentile / 100)
        upper_bound = data.quantile(upper_percentile / 100)
        
        outliers = data[(data < lower_bound) | (data > upper_bound)]
        
        stats = {
            'method': 'percentile',
            'lower_percentile': lower_percentile,
            'upper_percentile': upper_percentile,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'outlier_count': len(outliers),
            'outlier_rate': len(outliers) / len(data) if len(data) > 0 else 0
        }
        
    else:
        outliers = pd.Series([], dtype=data.dtype)
        stats = {'method': 'unknown', 'error': '不支持的方法'}
    
    return outliers.index.tolist(), stats
```

### 数据类型转换模式

**智能类型转换**：
```python
def smart_type_conversion(df, column_conversions=None):
    """
    智能数据类型转换模式
    
    参数:
        df: 输入DataFrame
        column_conversions: 字段转换配置字典
            {
                '字段名': '目标类型',
                'date_field': 'datetime',
                'numeric_field': 'float'
            }
    """
    df_converted = df.copy()
    
    # 如果没有指定转换配置，尝试自动推断
    if column_conversions is None:
        column_conversions = {}
        for col in df_converted.columns:
            # 尝试推断日期字段
            if any(keyword in col.lower() for keyword in ['date', 'time', 'dt', 'timestamp']):
                column_conversions[col] = 'datetime'
            # 尝试推断数值字段
            elif any(keyword in col.lower() for keyword in ['amount', 'price', 'total', 'count', 'num', 'id']):
                column_conversions[col] = 'numeric'
    
    for col, target_type in column_conversions.items():
        if col not in df_converted.columns:
            continue
            
        if target_type == 'datetime':
            try:
                df_converted[col] = pd.to_datetime(df_converted[col], errors='coerce')
                print(f"字段 {col} 转换为datetime类型")
            except Exception as e:
                print(f"字段 {col} 转换为datetime失败: {e}")
                
        elif target_type == 'numeric':
            try:
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce')
                print(f"字段 {col} 转换为numeric类型")
            except Exception as e:
                print(f"字段 {col} 转换为numeric失败: {e}")
                
        elif target_type == 'string':
            try:
                df_converted[col] = df_converted[col].astype(str)
                print(f"字段 {col} 转换为string类型")
            except Exception as e:
                print(f"字段 {col} 转换为string失败: {e}")
                
        elif target_type == 'int':
            try:
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').astype('Int64')
                print(f"字段 {col} 转换为int类型")
            except Exception as e:
                print(f"字段 {col} 转换为int失败: {e}")
    
    return df_converted
```

## 分层加工模式

### DWD层加工模式

**增量数据合并模式**：
```python
def merge_incremental_data(old_df, new_df, key_columns, timestamp_column='update_time'):
    """
    DWD层增量数据合并模式
    
    参数:
        old_df: 历史数据
        new_df: 新增数据
        key_columns: 主键字段列表
        timestamp_column: 时间戳字段
        
    返回:
        合并后的完整数据
    """
    if old_df.empty:
        return new_df.copy()
    
    if new_df.empty:
        return old_df.copy()
    
    # 构建复合键
    old_df['_merge_key'] = old_df[key_columns].astype(str).agg('_'.join, axis=1)
    new_df['_merge_key'] = new_df[key_columns].astype(str).agg('_'.join, axis=1)
    
    # 找出需要更新的记录
    existing_keys = set(old_df['_merge_key'])
    new_keys = set(new_df['_merge_key'])
    
    # 更新的记录（键存在且时间戳更新）
    update_keys = []
    for key in existing_keys.intersection(new_keys):
        old_time = old_df.loc[old_df['_merge_key'] == key, timestamp_column].iloc[0]
        new_time = new_df.loc[new_df['_merge_key'] == key, timestamp_column].iloc[0]
        
        if pd.isna(old_time) or (not pd.isna(new_time) and new_time > old_time):
            update_keys.append(key)
    
    # 删除旧记录中的更新记录
    old_df_filtered = old_df[~old_df['_merge_key'].isin(update_keys)].copy()
    
    # 获取新增记录
    new_records = new_df[new_df['_merge_key'].isin(new_keys - existing_keys)].copy()
    
    # 获取更新记录
    updated_records = new_df[new_df['_merge_key'].isin(update_keys)].copy()
    
    # 合并所有记录
    merged_df = pd.concat([old_df_filtered, new_records, updated_records], ignore_index=True)
    
    # 删除临时键
    merged_df = merged_df.drop(columns=['_merge_key'])
    
    print(f"增量合并完成:")
    print(f"  保留记录: {len(old_df_filtered)}")
    print(f"  新增记录: {len(new_records)}")
    print(f"  更新记录: {len(updated_records)}")
    print(f"  总计记录: {len(merged_df)}")
    
    return merged_df
```

### DIM层加工模式

**缓慢变化维度处理模式（SCD Type 2）**：
```python
def process_scd_type2(current_dim, changes_df, natural_key_column, attribute_columns):
    """
    DIM层缓慢变化维度处理模式（SCD Type 2）
    
    参数:
        current_dim: 当前维度表
        changes_df: 属性变化数据
        natural_key_column: 自然键字段
        attribute_columns: 属性字段列表
        
    返回:
        更新后的维度表
    """
    import datetime
    
    result_dim = current_dim.copy() if not current_dim.empty else pd.DataFrame()
    
    # 为current_dim添加必要字段（如果不存在）
    if not result_dim.empty:
        if 'valid_from' not in result_dim.columns:
            result_dim['valid_from'] = datetime.date(1900, 1, 1)
        if 'valid_to' not in result_dim.columns:
            result_dim['valid_to'] = datetime.date(9999, 12, 31)
        if 'is_current' not in result_dim.columns:
            result_dim['is_current'] = 1
    
    # 处理每个变化记录
    for idx, change_row in changes_df.iterrows():
        natural_key = change_row[natural_key_column]
        
        # 查找当前维度中该自然键的记录
        existing_records = result_dim[result_dim[natural_key_column] == natural_key]
        
        if existing_records.empty:
            # 新增记录
            new_record = change_row.to_dict()
            new_record['valid_from'] = datetime.date.today()
            new_record['valid_to'] = datetime.date(9999, 12, 31)
            new_record['is_current'] = 1
            
            # 添加到结果
            result_dim = pd.concat([result_dim, pd.DataFrame([new_record])], ignore_index=True)
            print(f"新增维度记录: {natural_key}")
            
        else:
            # 检查属性是否有变化
            latest_record = existing_records[existing_records['is_current'] == 1].iloc[-1]
            
            attribute_changed = False
            for attr in attribute_columns:
                if attr in change_row and attr in latest_record:
                    old_value = latest_record[attr]
                    new_value = change_row[attr]
                    
                    if pd.isna(old_value) and not pd.isna(new_value):
                        attribute_changed = True
                        break
                    elif not pd.isna(old_value) and pd.isna(new_value):
                        attribute_changed = True
                        break
                    elif not pd.isna(old_value) and not pd.isna(new_value) and old_value != new_value:
                        attribute_changed = True
                        break
            
            if attribute_changed:
                # 关闭旧记录的当前标记
                result_dim.loc[
                    (result_dim[natural_key_column] == natural_key) & 
                    (result_dim['is_current'] == 1),
                    ['valid_to', 'is_current']
                ] = [datetime.date.today() - datetime.timedelta(days=1), 0]
                
                # 添加新记录
                new_record = change_row.to_dict()
                new_record['valid_from'] = datetime.date.today()
                new_record['valid_to'] = datetime.date(9999, 12, 31)
                new_record['is_current'] = 1
                
                result_dim = pd.concat([result_dim, pd.DataFrame([new_record])], ignore_index=True)
                print(f"更新维度记录: {natural_key}")
            else:
                print(f"维度记录无变化: {natural_key}")
    
    return result_dim
```

### DWS层加工模式

**主题域汇总模式**：
```python
def aggregate_by_subject(df, subject_config):
    """
    DWS层主题域汇总模式
    
    参数:
        df: 输入数据
        subject_config: 主题配置字典
            {
                'subject_name': '主题名称',
                'group_columns': ['分组字段1', '分组字段2'],
                'measure_columns': ['度量字段1', '度量字段2'],
                'aggregations': ['sum', 'mean', 'count', 'max', 'min'],
                'time_granularity': 'day'  # day/week/month/quarter/year
            }
        
    返回:
        主题汇总结果
    """
    result_df = df.copy()
    
    # 处理时间粒度
    time_column = subject_config.get('time_column')
    time_granularity = subject_config.get('time_granularity', 'day')
    
    if time_column and time_column in result_df.columns:
        result_df[time_column] = pd.to_datetime(result_df[time_column])
        
        if time_granularity == 'day':
            result_df['time_key'] = result_df[time_column].dt.strftime('%Y%m%d')
        elif time_granularity == 'week':
            result_df['time_key'] = result_df[time_column].dt.strftime('%Y%W')
        elif time_granularity == 'month':
            result_df['time_key'] = result_df[time_column].dt.strftime('%Y%m')
        elif time_granularity == 'quarter':
            result_df['time_key'] = result_df[time_column].dt.year.astype(str) + 'Q' + result_df[time_column].dt.quarter.astype(str)
        elif time_granularity == 'year':
            result_df['time_key'] = result_df[time_column].dt.strftime('%Y')
        
        # 将time_key添加到分组字段
        group_columns = subject_config.get('group_columns', []) + ['time_key']
    else:
        group_columns = subject_config.get('group_columns', [])
    
    # 执行聚合
    measure_columns = subject_config.get('measure_columns', [])
    aggregations = subject_config.get('aggregations', ['sum', 'mean', 'count'])
    
    # 构建聚合字典
    agg_dict = {}
    for measure in measure_columns:
        if measure in result_df.columns:
            for agg_func in aggregations:
                col_name = f"{measure}_{agg_func}"
                agg_dict[col_name] = (measure, agg_func)
    
    if agg_dict:
        grouped = result_df.groupby(group_columns)
        aggregated = grouped.agg(agg_dict)
        
        # 重命名列
        aggregated.columns = [f"{measure}_{agg_func}" for measure in measure_columns for agg_func in aggregations]
        
        result = aggregated.reset_index()
        print(f"主题 '{subject_config['subject_name']}' 汇总完成，记录数: {len(result)}")
        return result
    else:
        print(f"警告: 没有找到有效的度量字段进行聚合")
        return pd.DataFrame()
```

### ADS层加工模式

**宽表构建模式**：
```python
def build_wide_table(source_tables, join_logic, business_rules):
    """
    ADS层宽表构建模式
    
    参数:
        source_tables: 源表字典 {表名: DataFrame}
        join_logic: 关联逻辑列表
            [
                {
                    'left': '表1',
                    'right': '表2',
                    'on': ['关联字段'],
                    'how': 'left'
                }
            ]
        business_rules: 业务规则字典
            {
                '指标计算': {
                    '指标1': lambda df: df['字段1'] + df['字段2'],
                    '指标2': lambda df: df['字段3'] / df['字段4']
                },
                '过滤条件': "字段5 > 0",
                '排序规则': ['字段1', '-字段2']
            }
        
    返回:
        宽表DataFrame
    """
    # 从第一个表开始
    if not source_tables:
        return pd.DataFrame()
    
    wide_df = source_tables[list(source_tables.keys())[0]].copy()
    
    # 执行关联
    for join_step in join_logic:
        left_table = join_step.get('left')
        right_table = join_step.get('right')
        join_on = join_step.get('on', [])
        how = join_step.get('how', 'left')
        
        if left_table not in source_tables or right_table not in source_tables:
            print(f"警告: 表 '{left_table}' 或 '{right_table}' 不存在")
            continue
        
        # 执行关联
        wide_df = pd.merge(
            wide_df,
            source_tables[right_table],
            left_on=join_on,
            right_on=join_on,
            how=how
        )
        
        print(f"关联完成: {left_table} ← {right_table} on {join_on}")
    
    # 应用业务规则
    if '过滤条件' in business_rules:
        filter_expr = business_rules['过滤条件']
        try:
            # 简单的表达式求值
            wide_df = wide_df.query(filter_expr)
            print(f"应用过滤条件: {filter_expr}")
        except Exception as e:
            print(f"过滤条件执行失败: {e}")
    
    if '指标计算' in business_rules:
        metric_calculations = business_rules['指标计算']
        for metric_name, calculation_func in metric_calculations.items():
            try:
                wide_df[metric_name] = calculation_func(wide_df)
                print(f"计算指标: {metric_name}")
            except Exception as e:
                print(f"指标计算失败 {metric_name}: {e}")
    
    if '排序规则' in business_rules:
        sort_columns = business_rules['排序规则']
        ascending_flags = []
        sort_cols_clean = []
        
        for col in sort_columns:
            if col.startswith('-'):
                sort_cols_clean.append(col[1:])
                ascending_flags.append(False)
            else:
                sort_cols_clean.append(col)
                ascending_flags.append(True)
        
        # 只排序存在的字段
        existing_cols = [col for col in sort_cols_clean if col in wide_df.columns]
        if existing_cols:
            wide_df = wide_df.sort_values(by=existing_cols, ascending=ascending_flags[:len(existing_cols)])
            print(f"应用排序规则: {existing_cols}")
    
    print(f"宽表构建完成，总记录数: {len(wide_df)}, 总字段数: {len(wide_df.columns)}")
    return wide_df
```

## 数据质量检查模式

**完整性检查模式**：
```python
def check_data_completeness(df, required_columns=None, completeness_threshold=0.95):
    """
    数据完整性检查模式
    
    参数:
        df: 输入数据
        required_columns: 必需字段列表
        completeness_threshold: 完整性阈值
        
    返回:
        完整性检查报告
    """
    report = {
        '总体统计': {},
        '字段完整性': {},
        '问题字段': [],
        '通过检查': False
    }
    
    # 总体统计
    report['总体统计']['总记录数'] = len(df)
    report['总体统计']['总字段数'] = len(df.columns)
    
    # 如果没有指定必需字段，检查所有字段
    if required_columns is None:
        required_columns = df.columns.tolist()
    
    # 检查每个字段的完整性
    all_passed = True
    for col in required_columns:
        if col not in df.columns:
            report['字段完整性'][col] = {
                '缺失': True,
                '完整率': 0.0,
                '空值数': '字段不存在'
            }
            report['问题字段'].append({
                '字段': col,
                '问题': '字段不存在',
                '建议': '检查字段名或数据源'
            })
            all_passed = False
        else:
            null_count = df[col].isnull().sum()
            total_count = len(df)
            completeness_rate = 1 - (null_count / total_count) if total_count > 0 else 0
            
            report['字段完整性'][col] = {
                '缺失': False,
                '完整率': completeness_rate,
                '空值数': null_count,
                '总记录数': total_count
            }
            
            if completeness_rate < completeness_threshold:
                report['问题字段'].append({
                    '字段': col,
                    '问题': f'完整率过低 ({completeness_rate:.2%})',
                    '建议': '检查数据源或增加空值处理'
                })
                all_passed = False
    
    report['通过检查'] = all_passed
    return report
```

**一致性检查模式**：
```python
def check_data_consistency(df, consistency_rules):
    """
    数据一致性检查模式
    
    参数:
        df: 输入数据
        consistency_rules: 一致性规则列表
            [
                {
                    'name': '规则1',
                    'condition': "字段1 > 字段2",
                    'description': '字段1应大于字段2'
                }
            ]
        
    返回:
        一致性检查报告
    """
    report = {
        '检查规则': [],
        '违规记录': {},
        '总结': {
            '总规则数': len(consistency_rules),
            '通过规则数': 0,
            '违规规则数': 0
        }
    }
    
    for rule in consistency_rules:
        rule_name = rule.get('name', '未命名规则')
        condition = rule.get('condition')
        
        if not condition:
            continue
            
        try:
            # 执行条件检查
            violations = df.query(f"not ({condition})")
            violation_count = len(violations)
            
            rule_result = {
                '规则名称': rule_name,
                '检查条件': condition,
                '描述': rule.get('description', ''),
                '违规记录数': violation_count,
                '通过': violation_count == 0
            }
            
            report['检查规则'].append(rule_result)
            
            if violation_count > 0:
                report['违规记录'][rule_name] = violations.head(10).to_dict('records')
                report['总结']['违规规则数'] += 1
            else:
                report['总结']['通过规则数'] += 1
                
        except Exception as e:
            rule_result = {
                '规则名称': rule_name,
                '检查条件': condition,
                '描述': rule.get('description', ''),
                '错误': str(e),
                '通过': False
            }
            report['检查规则'].append(rule_result)
            report['总结']['违规规则数'] += 1
    
    return report
```

## 性能优化模式

**内存优化模式**：
```python
def optimize_memory_usage(df):
    """
    内存使用优化模式
    
    参数:
        df: 输入DataFrame
        
    返回:
        优化后的DataFrame和优化报告
    """
    import numpy as np
    
    report = {
        '优化前内存(MB)': df.memory_usage(deep=True).sum() / 1024 / 1024,
        '优化措施': []
    }
    
    optimized_df = df.copy()
    
    # 检查数值类型优化
    for col in optimized_df.select_dtypes(include=[np.number]).columns:
        col_min = optimized_df[col].min()
        col_max = optimized_df[col].max()
        
        # 检查是否可以转换为更小的整数类型
        if optimized_df[col].dtype == 'int64':
            if col_min >= 0:
                if col_max < 255:
                    optimized_df[col] = optimized_df[col].astype(np.uint8)
                    report['优化措施'].append(f"{col}: int64 -> uint8")
                elif col_max < 65535:
                    optimized_df[col] = optimized_df[col].astype(np.uint16)
                    report['优化措施'].append(f"{col}: int64 -> uint16")
                elif col_max < 4294967295:
                    optimized_df[col] = optimized_df[col].astype(np.uint32)
                    report['优化措施'].append(f"{col}: int64 -> uint32")
            else:
                if col_min > -128 and col_max < 127:
                    optimized_df[col] = optimized_df[col].astype(np.int8)
                    report['优化措施'].append(f"{col}: int64 -> int8")
                elif col_min > -32768 and col_max < 32767:
                    optimized_df[col] = optimized_df[col].astype(np.int16)
                    report['优化措施'].append(f"{col}: int64 -> int16")
                elif col_min > -2147483648 and col_max < 2147483647:
                    optimized_df[col] = optimized_df[col].astype(np.int32)
                    report['优化措施'].append(f"{col}: int64 -> int32")
        
        # 检查是否可以转换为float32
        elif optimized_df[col].dtype == 'float64':
            # 检查精度需求
            precision_needed = optimized_df[col].apply(lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0).max()
            if precision_needed <= 6:  # float32提供约6-7位小数精度
                optimized_df[col] = optimized_df[col].astype(np.float32)
                report['优化措施'].append(f"{col}: float64 -> float32")
    
    # 检查字符串类型优化
    for col in optimized_df.select_dtypes(include=['object']).columns:
        unique_count = optimized_df[col].nunique()
        total_count = len(optimized_df[col])
        
        # 如果唯一值较少，转换为category类型
        if unique_count / total_count < 0.5:  # 唯一值占比小于50%
            optimized_df[col] = optimized_df[col].astype('category')
            report['优化措施'].append(f"{col}: object -> category (唯一值: {unique_count}/{total_count})")
    
    report['优化后内存(MB)'] = optimized_df.memory_usage(deep=True).sum() / 1024 / 1024
    report['内存节省比例'] = 1 - (report['优化后内存(MB)'] / report['优化前内存(MB)'])
    
    return optimized_df, report
```

## 错误处理模式

**数据加工异常处理模式**：
```python
def safe_data_processing(processing_func, df, error_handling='log', fallback_value=None):
    """
    安全数据加工异常处理模式
    
    参数:
        processing_func: 加工函数
        df: 输入数据
        error_handling: 错误处理策略 ('log', 'skip', 'fill', 'raise')
        fallback_value: 出错时的回退值
        
    返回:
        加工结果或错误信息
    """
    import traceback
    
    try:
        result = processing_func(df)
        return {
            'success': True,
            'result': result,
            'error': None
        }
        
    except Exception as e:
        error_info = {
            'success': False,
            'result': None,
            'error': {
                'type': type(e).__name__,
                'message': str(e),
                'traceback': traceback.format_exc()
            }
        }
        
        if error_handling == 'log':
            print(f"数据处理错误: {type(e).__name__}: {e}")
            print(f"详细追踪: {traceback.format_exc()}")
            return error_info
            
        elif error_handling == 'skip':
            print(f"数据处理错误，跳过处理: {e}")
            return {'success': True, 'result': df, 'error': None}  # 返回原始数据
            
        elif error_handling == 'fill' and fallback_value is not None:
            print(f"数据处理错误，使用回退值: {e}")
            if isinstance(fallback_value, pd.DataFrame):
                return {'success': True, 'result': fallback_value, 'error': None}
            else:
                # 创建一个包含回退值的DataFrame
                fallback_df = pd.DataFrame([fallback_value] * len(df))
                return {'success': True, 'result': fallback_df, 'error': None}
            
        elif error_handling == 'raise':
            raise
        
        else:
            return error_info
```

## 日志记录模式

**结构化日志记录模式**：
```python
def log_data_processing(operation, stage, df, additional_info=None):
    """
    结构化数据加工日志记录模式
    
    参数:
        operation: 操作名称
        stage: 处理阶段
        df: 当前数据DataFrame
        additional_info: 附加信息字典
        
    返回:
        日志记录字典
    """
    import datetime
    
    log_record = {
        'timestamp': datetime.datetime.now().isoformat(),
        'operation': operation,
        'stage': stage,
        'data_stats': {
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024 if not df.empty else 0
        }
    }
    
    if additional_info:
        log_record.update(additional_info)
    
    # 输出到控制台
    print(f"[{log_record['timestamp']}] {operation} - {stage}")
    print(f"  记录数: {log_record['data_stats']['row_count']}")
    print(f"  字段数: {log_record['data_stats']['column_count']}")
    
    # 可以添加文件或数据库日志记录
    # with open('data_processing.log', 'a') as f:
    #     f.write(json.dumps(log_record) + '\n')
    
    return log_record
```