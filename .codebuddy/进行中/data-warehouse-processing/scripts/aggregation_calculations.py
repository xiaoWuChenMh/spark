#!/usr/bin/env python3
"""
聚合计算工具 - 用于DWS层汇总数据加工

包含分组聚合、窗口函数、分层计算和性能优化功能。
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def calculate_basic_aggregations(df, group_columns, agg_columns, agg_functions):
    """
    计算基本聚合指标
    
    参数:
        df: 输入DataFrame
        group_columns: 分组字段列表
        agg_columns: 聚合字段列表
        agg_functions: 聚合函数列表或字典
            
    返回:
        聚合结果DataFrame
    """
    if isinstance(agg_functions, list):
        # 为每个聚合字段应用相同的函数列表
        agg_dict = {}
        for col in agg_columns:
            for func in agg_functions:
                agg_dict[f"{col}_{func}"] = (col, func)
    else:
        # 使用指定的聚合字典
        agg_dict = agg_functions
    
    # 执行分组聚合
    grouped = df.groupby(group_columns)
    result = grouped.agg(agg_dict)
    
    # 重命名列（如果使用元组格式）
    if isinstance(agg_functions, list):
        result.columns = [f"{col}_{func}" for col in agg_columns for func in agg_functions]
    
    return result.reset_index()


def calculate_window_aggregations(df, partition_columns, order_column, window_columns, window_functions):
    """
    计算窗口聚合指标
    
    参数:
        df: 输入DataFrame
        partition_columns: 分区字段列表
        order_column: 排序字段
        window_columns: 窗口计算字段列表
        window_functions: 窗口函数字典 {字段名: 函数列表}
        
    返回:
        包含窗口计算结果的DataFrame
    """
    result_df = df.copy()
    
    # 确保排序字段存在
    if order_column not in result_df.columns:
        print(f"错误: 排序字段 '{order_column}' 不存在")
        return result_df
    
    # 排序数据
    result_df = result_df.sort_values(by=[order_column])
    
    # 计算窗口函数
    for col, functions in window_functions.items():
        if col not in result_df.columns:
            continue
            
        for func_name in functions:
            new_col_name = f"{col}_{func_name}_window"
            
            if partition_columns:
                # 按分区计算
                result_df[new_col_name] = result_df.groupby(partition_columns)[col].transform(
                    lambda x: x.rolling(window=len(x), min_periods=1).agg(func_name)
                )
            else:
                # 全局计算
                result_df[new_col_name] = result_df[col].rolling(window=len(result_df), min_periods=1).agg(func_name)
    
    return result_df


def calculate_time_based_aggregations(df, time_column, value_columns, time_windows):
    """
    计算基于时间窗口的聚合
    
    参数:
        df: 输入DataFrame
        time_column: 时间字段
        value_columns: 值字段列表
        time_windows: 时间窗口列表，例如 ['7D', '30D', '90D']
        
    返回:
        时间窗口聚合结果
    """
    # 确保时间字段是datetime类型
    df = df.copy()
    df[time_column] = pd.to_datetime(df[time_column])
    
    # 按时间排序
    df = df.sort_values(by=time_column)
    
    # 创建结果容器
    results = {}
    
    for window in time_windows:
        window_df = df.copy()
        
        # 解析时间窗口
        window_days = int(window[:-1])
        
        # 计算每个时间点的窗口聚合
        for idx, row in window_df.iterrows():
            current_time = row[time_column]
            window_start = current_time - timedelta(days=window_days)
            
            # 获取窗口内数据
            window_data = df[
                (df[time_column] >= window_start) & 
                (df[time_column] <= current_time)
            ]
            
            # 计算聚合指标
            for col in value_columns:
                if col in window_data.columns:
                    col_sum = f"{col}_{window}_sum"
                    col_avg = f"{col}_{window}_avg"
                    col_count = f"{col}_{window}_count"
                    
                    if col_sum not in results:
                        results[col_sum] = []
                    if col_avg not in results:
                        results[col_avg] = []
                    if col_count not in results:
                        results[col_count] = []
                    
                    results[col_sum].append(window_data[col].sum() if not window_data.empty else 0)
                    results[col_avg].append(window_data[col].mean() if not window_data.empty else 0)
                    results[col_count].append(len(window_data))
        
        # 添加结果到DataFrame
        for col_name, values in results.items():
            window_df[col_name] = values
            
        results[window] = window_df
    
    return results


def calculate_hierarchical_aggregations(df, hierarchy_levels, measure_columns):
    """
    计算分层聚合（从细粒度到粗粒度）
    
    参数:
        df: 输入DataFrame
        hierarchy_levels: 层次级别列表，从细到粗
        measure_columns: 度量字段列表
        
    返回:
        分层聚合结果字典
    """
    hierarchical_results = {}
    
    # 计算每个层次的聚合
    for i, level in enumerate(hierarchy_levels):
        # 获取当前层次及所有更细的层次
        current_levels = hierarchy_levels[:i+1]
        
        # 分组聚合
        agg_dict = {}
        for col in measure_columns:
            agg_dict[f"{col}_sum"] = (col, 'sum')
            agg_dict[f"{col}_avg"] = (col, 'mean')
            agg_dict[f"{col}_count"] = (col, 'count')
        
        grouped = df.groupby(current_levels)
        result = grouped.agg(agg_dict)
        
        # 重命名列
        result.columns = [f"{col}_{func}" for col in measure_columns for func in ['sum', 'avg', 'count']]
        
        hierarchical_results[level] = result.reset_index()
    
    return hierarchical_results


def optimize_aggregation_performance(df, group_columns, agg_columns):
    """
    优化聚合计算性能
    
    参数:
        df: 输入DataFrame
        group_columns: 分组字段列表
        agg_columns: 聚合字段列表
        
    返回:
        性能优化建议字典
    """
    performance_advice = {
        '数据量分析': {},
        '分组分析': {},
        '优化建议': []
    }
    
    # 数据量分析
    total_records = len(df)
    performance_advice['数据量分析']['总记录数'] = total_records
    
    for col in group_columns:
        if col in df.columns:
            unique_values = df[col].nunique()
            performance_advice['数据量分析'][f"{col}唯一值数"] = unique_values
    
    # 分组分析
    if group_columns:
        sample_grouped = df.groupby(group_columns).size()
        avg_group_size = sample_grouped.mean() if len(sample_grouped) > 0 else 0
        max_group_size = sample_grouped.max() if len(sample_grouped) > 0 else 0
        
        performance_advice['分组分析']['平均组大小'] = avg_group_size
        performance_advice['分组分析']['最大组大小'] = max_group_size
        performance_advice['分组分析']['总组数'] = len(sample_grouped)
    
    # 优化建议
    if total_records > 1000000:
        performance_advice['优化建议'].append("数据量超过100万，建议使用增量计算或采样计算")
    
    if len(group_columns) > 5:
        performance_advice['优化建议'].append("分组字段过多，考虑减少分组维度或使用预聚合")
    
    if avg_group_size > 10000:
        performance_advice['优化建议'].append("平均组大小较大，考虑使用并行计算")
    
    # 数据类型优化建议
    for col in agg_columns:
        if col in df.columns:
            dtype = df[col].dtype
            if dtype == 'float64':
                performance_advice['优化建议'].append(f"字段 '{col}' 为float64，可考虑转换为float32以节省内存")
    
    return performance_advice


def calculate_percentile_aggregations(df, group_columns, value_column, percentiles=[25, 50, 75, 90, 95]):
    """
    计算百分位数聚合
    
    参数:
        df: 输入DataFrame
        group_columns: 分组字段列表
        value_column: 值字段
        percentiles: 百分位数列表
        
    返回:
        百分位数聚合结果
    """
    if value_column not in df.columns:
        print(f"错误: 值字段 '{value_column}' 不存在")
        return pd.DataFrame()
    
    # 计算每个分组的百分位数
    percentile_results = []
    
    if group_columns:
        grouped = df.groupby(group_columns)
        for name, group in grouped:
            if isinstance(name, tuple):
                group_dict = dict(zip(group_columns, name))
            else:
                group_dict = {group_columns[0]: name}
            
            group_dict[f"{value_column}_count"] = len(group)
            
            for p in percentiles:
                percentile_value = group[value_column].quantile(p/100)
                group_dict[f"{value_column}_p{p}"] = percentile_value
            
            percentile_results.append(group_dict)
    else:
        # 全局计算
        group_dict = {}
        group_dict[f"{value_column}_count"] = len(df)
        
        for p in percentiles:
            percentile_value = df[value_column].quantile(p/100)
            group_dict[f"{value_column}_p{p}"] = percentile_value
        
        percentile_results.append(group_dict)
    
    return pd.DataFrame(percentile_results)


if __name__ == "__main__":
    """示例用法"""
    # 创建示例销售数据
    sales_data = {
        'date': pd.date_range('2023-01-01', periods=100, freq='D'),
        'product_category': np.random.choice(['电子', '服装', '食品', '家居'], 100),
        'product_id': np.random.randint(1, 20, 100),
        'sales_amount': np.random.uniform(10, 1000, 100).round(2),
        'quantity': np.random.randint(1, 10, 100)
    }
    sales_df = pd.DataFrame(sales_data)
    
    print("原始销售数据（前5行）:")
    print(sales_df.head())
    print()
    
    # 基本聚合
    basic_agg = calculate_basic_aggregations(
        sales_df,
        group_columns=['product_category', 'date'],
        agg_columns=['sales_amount', 'quantity'],
        agg_functions=['sum', 'mean', 'count']
    )
    print("基本聚合结果（前5行）:")
    print(basic_agg.head())
    print()
    
    # 窗口聚合
    window_agg = calculate_window_aggregations(
        sales_df,
        partition_columns=['product_category'],
        order_column='date',
        window_columns=['sales_amount'],
        window_functions={'sales_amount': ['mean', 'sum']}
    )
    print("窗口聚合结果（前5行）:")
    print(window_agg[['date', 'product_category', 'sales_amount', 'sales_amount_mean_window', 'sales_amount_sum_window']].head())
    print()
    
    # 百分位数聚合
    percentile_agg = calculate_percentile_aggregations(
        sales_df,
        group_columns=['product_category'],
        value_column='sales_amount',
        percentiles=[25, 50, 75]
    )
    print("百分位数聚合:")
    print(percentile_agg)
    print()
    
    # 性能优化建议
    performance_advice = optimize_aggregation_performance(
        sales_df,
        group_columns=['product_category', 'product_id'],
        agg_columns=['sales_amount', 'quantity']
    )
    print("性能优化建议:")
    for category, info in performance_advice.items():
        print(f"{category}: {info}")