#!/usr/bin/env python3
"""
宽表构建工具 - 用于ADS层应用表加工

包含多表关联、指标衍生、业务逻辑封装和宽表优化功能。
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def join_multiple_tables(tables, join_configs):
    """
    关联多个表构建宽表
    
    参数:
        tables: 表名->DataFrame字典
        join_configs: 关联配置列表，每个元素为:
            {
                'left_table': 左表名,
                'right_table': 右表名,
                'on': 关联键或键列表,
                'how': 关联方式 ('left', 'inner', 'outer'等),
                'suffixes': 后缀元组（可选）
            }
        
    返回:
        关联后的宽表DataFrame
    """
    if not tables:
        return pd.DataFrame()
    
    # 从第一个表开始
    result_df = tables[list(tables.keys())[0]].copy()
    
    # 按顺序执行关联
    for config in join_configs:
        left_table = config.get('left_table')
        right_table = config.get('right_table')
        join_on = config.get('on')
        how = config.get('how', 'left')
        suffixes = config.get('suffixes', ('_x', '_y'))
        
        if left_table not in tables or right_table not in tables:
            print(f"警告: 表 '{left_table}' 或 '{right_table}' 不存在")
            continue
        
        # 执行关联
        result_df = pd.merge(
            result_df,
            tables[right_table],
            left_on=join_on,
            right_on=join_on,
            how=how,
            suffixes=suffixes
        )
    
    return result_df


def derive_business_metrics(df, metric_definitions):
    """
    衍生业务指标
    
    参数:
        df: 输入DataFrame
        metric_definitions: 指标定义字典
            格式: {
                '指标名': {
                    'calculation': lambda函数或表达式字符串,
                    'source_fields': 依赖字段列表,
                    'description': 指标描述
                }
            }
        
    返回:
        包含衍生指标的DataFrame
    """
    result_df = df.copy()
    
    for metric_name, definition in metric_definitions.items():
        calculation = definition.get('calculation')
        source_fields = definition.get('source_fields', [])
        
        # 验证依赖字段是否存在
        missing_fields = [field for field in source_fields if field not in result_df.columns]
        if missing_fields:
            print(f"警告: 指标 '{metric_name}' 依赖的字段不存在: {missing_fields}")
            continue
        
        try:
            if callable(calculation):
                # 如果是可调用函数
                result_df[metric_name] = calculation(result_df)
            elif isinstance(calculation, str):
                # 如果是表达式字符串
                # 简单实现：支持基本的算术运算
                expr = calculation
                for field in source_fields:
                    expr = expr.replace(field, f"result_df['{field}']")
                result_df[metric_name] = eval(expr)
            else:
                print(f"警告: 指标 '{metric_name}' 的计算定义无效")
        except Exception as e:
            print(f"计算指标 '{metric_name}' 时出错: {e}")
    
    return result_df


def encapsulate_business_logic(df, logic_modules):
    """
    封装业务逻辑模块
    
    参数:
        df: 输入DataFrame
        logic_modules: 逻辑模块字典
            格式: {
                '模块名': {
                    'function': 处理函数,
                    'params': 参数字典,
                    'output_prefix': 输出字段前缀
                }
            }
        
    返回:
        经过业务逻辑处理的DataFrame
    """
    result_df = df.copy()
    
    for module_name, module_config in logic_modules.items():
        function = module_config.get('function')
        params = module_config.get('params', {})
        output_prefix = module_config.get('output_prefix', module_name)
        
        if not callable(function):
            print(f"警告: 模块 '{module_name}' 的函数不可调用")
            continue
        
        try:
            # 执行业务逻辑函数
            module_result = function(result_df, **params)
            
            # 处理返回结果
            if isinstance(module_result, pd.DataFrame):
                # 如果是DataFrame，合并列
                for col in module_result.columns:
                    if col not in result_df.columns:
                        result_df[f"{output_prefix}_{col}"] = module_result[col]
            elif isinstance(module_result, pd.Series):
                # 如果是Series，作为新列添加
                result_df[f"{output_prefix}_result"] = module_result
            elif isinstance(module_result, dict):
                # 如果是字典，每个键值对作为新列
                for key, value in module_result.items():
                    result_df[f"{output_prefix}_{key}"] = value
        except Exception as e:
            print(f"执行业务逻辑模块 '{module_name}' 时出错: {e}")
    
    return result_df


def optimize_wide_table(df, optimization_rules):
    """
    优化宽表结构
    
    参数:
        df: 宽表DataFrame
        optimization_rules: 优化规则字典
            格式: {
                'drop_columns': 要删除的列列表,
                'rename_columns': 重命名映射字典,
                'fill_na': 空值填充规则字典,
                'cast_types': 类型转换字典
            }
        
    返回:
        优化后的宽表
    """
    result_df = df.copy()
    
    # 删除指定列
    drop_columns = optimization_rules.get('drop_columns', [])
    for col in drop_columns:
        if col in result_df.columns:
            result_df = result_df.drop(columns=[col])
    
    # 重命名列
    rename_columns = optimization_rules.get('rename_columns', {})
    result_df = result_df.rename(columns=rename_columns)
    
    # 填充空值
    fill_na = optimization_rules.get('fill_na', {})
    for col, fill_value in fill_na.items():
        if col in result_df.columns:
            result_df[col] = result_df[col].fillna(fill_value)
    
    # 类型转换
    cast_types = optimization_rules.get('cast_types', {})
    for col, target_type in cast_types.items():
        if col in result_df.columns:
            try:
                if target_type == 'int':
                    result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('Int64')
                elif target_type == 'float':
                    result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('float64')
                elif target_type == 'str':
                    result_df[col] = result_df[col].astype('str')
                elif target_type == 'datetime':
                    result_df[col] = pd.to_datetime(result_df[col], errors='coerce')
            except Exception as e:
                print(f"转换字段 '{col}' 到类型 '{target_type}' 时出错: {e}")
    
    return result_df


def create_summary_statistics(df, summary_config):
    """
    创建宽表摘要统计
    
    参数:
        df: 宽表DataFrame
        summary_config: 摘要配置字典
            格式: {
                'numeric_columns': 数值型字段列表,
                'categorical_columns': 分类型字段列表,
                'date_columns': 日期型字段列表
            }
        
    返回:
        摘要统计字典
    """
    summary = {
        '基本信息': {},
        '数值字段统计': {},
        '分类字段统计': {},
        '日期字段统计': {},
        '数据质量': {}
    }
    
    # 基本信息
    summary['基本信息']['总记录数'] = len(df)
    summary['基本信息']['总字段数'] = len(df.columns)
    
    # 数值字段统计
    numeric_cols = summary_config.get('numeric_columns', [])
    for col in numeric_cols:
        if col in df.columns:
            col_stats = {
                '非空数': df[col].count(),
                '空值数': df[col].isnull().sum(),
                '平均值': df[col].mean(),
                '标准差': df[col].std(),
                '最小值': df[col].min(),
                '最大值': df[col].max(),
                '中位数': df[col].median()
            }
            summary['数值字段统计'][col] = col_stats
    
    # 分类字段统计
    categorical_cols = summary_config.get('categorical_columns', [])
    for col in categorical_cols:
        if col in df.columns:
            col_stats = {
                '非空数': df[col].count(),
                '空值数': df[col].isnull().sum(),
                '唯一值数': df[col].nunique(),
                '前5个值': df[col].value_counts().head(5).to_dict()
            }
            summary['分类字段统计'][col] = col_stats
    
    # 日期字段统计
    date_cols = summary_config.get('date_columns', [])
    for col in date_cols:
        if col in df.columns:
            col_stats = {
                '非空数': df[col].count(),
                '空值数': df[col].isnull().sum(),
                '最早日期': df[col].min(),
                '最晚日期': df[col].max()
            }
            summary['日期字段统计'][col] = col_stats
    
    # 数据质量
    total_cells = len(df) * len(df.columns)
    non_null_cells = df.count().sum()
    null_cells = df.isnull().sum().sum()
    
    summary['数据质量']['总单元格数'] = total_cells
    summary['数据质量']['非空单元格数'] = non_null_cells
    summary['数据质量']['空单元格数'] = null_cells
    summary['数据质量']['空值率'] = null_cells / total_cells if total_cells > 0 else 0
    
    return summary


def export_wide_table(df, export_config):
    """
    导出宽表
    
    参数:
        df: 宽表DataFrame
        export_config: 导出配置字典
            格式: {
                'format': 导出格式 ('csv', 'parquet', 'excel'),
                'path': 导出路径,
                'compression': 压缩方式（可选）,
                'partition_by': 分区字段（可选）
            }
        
    返回:
        导出结果信息
    """
    export_format = export_config.get('format', 'csv')
    export_path = export_config.get('path', 'wide_table_export')
    
    result_info = {
        'format': export_format,
        'path': export_path,
        'records': len(df),
        'columns': len(df.columns),
        'success': False,
        'error': None
    }
    
    try:
        if export_format == 'csv':
            compression = export_config.get('compression')
            df.to_csv(export_path, index=False, compression=compression)
            
        elif export_format == 'parquet':
            df.to_parquet(export_path, index=False, compression=export_config.get('compression', 'snappy'))
            
        elif export_format == 'excel':
            df.to_excel(export_path, index=False)
            
        else:
            result_info['error'] = f"不支持的导出格式: {export_format}"
            return result_info
        
        result_info['success'] = True
        
    except Exception as e:
        result_info['error'] = str(e)
    
    return result_info


if __name__ == "__main__":
    """示例用法"""
    # 创建示例数据
    user_data = {
        'user_id': [1, 2, 3, 4, 5],
        'user_name': ['张三', '李四', '王五', '赵六', '孙七'],
        'register_date': ['2023-01-01', '2023-02-01', '2023-03-01', '2023-01-15', '2023-02-20'],
        'user_level': ['VIP', '普通', 'VIP', '普通', 'VIP']
    }
    
    order_data = {
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'user_id': [1, 2, 3, 1, 4],
        'order_date': ['2023-01-10', '2023-02-05', '2023-03-10', '2023-01-20', '2023-02-25'],
        'order_amount': [500.0, 300.0, 800.0, 200.0, 450.0]
    }
    
    product_data = {
        'order_id': [1001, 1002, 1003, 1004, 1005],
        'product_name': ['手机', '衣服', '电脑', '书籍', '食品'],
        'category': ['电子', '服装', '电子', '文化', '食品']
    }
    
    user_df = pd.DataFrame(user_data)
    order_df = pd.DataFrame(order_data)
    product_df = pd.DataFrame(product_data)
    
    tables = {
        'users': user_df,
        'orders': order_df,
        'products': product_df
    }
    
    # 关联配置
    join_configs = [
        {
            'left_table': 'users',
            'right_table': 'orders',
            'on': 'user_id',
            'how': 'left'
        },
        {
            'left_table': 'users',
            'right_table': 'products',
            'on': 'order_id',
            'how': 'left'
        }
    ]
    
    # 构建宽表
    wide_df = join_multiple_tables(tables, join_configs)
    print("关联后的宽表（前5行）:")
    print(wide_df.head())
    print()
    
    # 衍生业务指标
    metric_defs = {
        'order_count': {
            'calculation': lambda df: df.groupby('user_id')['order_id'].transform('count'),
            'source_fields': ['user_id', 'order_id'],
            'description': '用户订单数'
        },
        'total_amount': {
            'calculation': lambda df: df.groupby('user_id')['order_amount'].transform('sum'),
            'source_fields': ['user_id', 'order_amount'],
            'description': '用户总金额'
        }
    }
    
    enriched_df = derive_business_metrics(wide_df, metric_defs)
    print("衍生指标后（前5行）:")
    print(enriched_df[['user_id', 'user_name', 'order_count', 'total_amount']].head())
    print()
    
    # 创建摘要统计
    summary_config = {
        'numeric_columns': ['order_amount', 'order_count', 'total_amount'],
        'categorical_columns': ['user_level', 'category'],
        'date_columns': ['register_date', 'order_date']
    }
    
    summary = create_summary_statistics(enriched_df, summary_config)
    print("宽表摘要统计:")
    for category, stats in summary.items():
        print(f"\n{category}:")
        for key, value in stats.items():
            print(f"  {key}: {value}")