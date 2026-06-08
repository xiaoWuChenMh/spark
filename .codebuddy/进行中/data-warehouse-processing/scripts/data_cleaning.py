#!/usr/bin/env python3
"""
数据清洗工具函数 - 用于DWD层明细数据清洗

包含通用数据清洗函数，适用于事实表的字段验证、格式转换和异常处理。
"""

def validate_data_types(df, schema_dict):
    """
    验证数据框字段数据类型是否符合预期
    
    参数:
        df: pandas/pyspark DataFrame
        schema_dict: 字典，字段名->期望的数据类型
        
    返回:
        validation_results: 字典，包含验证结果
    """
    validation_results = {
        'passed': [],
        'failed': [],
        'warnings': []
    }
    
    for column, expected_type in schema_dict.items():
        if column not in df.columns:
            validation_results['failed'].append(f"字段 '{column}' 不存在")
            continue
            
        actual_type = str(df[column].dtype)
        
        # 简化类型匹配逻辑
        if expected_type.lower() in actual_type.lower():
            validation_results['passed'].append(f"字段 '{column}': {actual_type} 符合预期 {expected_type}")
        else:
            validation_results['failed'].append(f"字段 '{column}': {actual_type} 不符合预期 {expected_type}")
    
    return validation_results


def handle_missing_values(df, strategy='fill', fill_value=None):
    """
    处理缺失值
    
    参数:
        df: pandas/pyspark DataFrame
        strategy: 处理策略 ('fill', 'drop', 'ignore')
        fill_value: 填充值（当strategy='fill'时使用）
        
    返回:
        处理后的DataFrame
    """
    if strategy == 'fill':
        if fill_value is not None:
            return df.fillna(fill_value)
        else:
            # 数值型字段填充0，字符型字段填充空字符串
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(0)
                else:
                    df[col] = df[col].fillna('')
            return df
    elif strategy == 'drop':
        return df.dropna()
    else:  # 'ignore'
        return df


def format_date_columns(df, date_columns, target_format='%Y-%m-%d'):
    """
    格式化日期字段
    
    参数:
        df: pandas DataFrame
        date_columns: 日期字段列表
        target_format: 目标日期格式
        
    返回:
        格式化后的DataFrame
    """
    import pandas as pd
    
    for col in date_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col]).dt.strftime(target_format)
            except Exception as e:
                print(f"格式化字段 '{col}' 时出错: {e}")
                
    return df


def validate_value_ranges(df, validation_rules):
    """
    验证字段取值范围
    
    参数:
        df: pandas DataFrame
        validation_rules: 字典，字段名->验证规则
            规则格式: {'min': 最小值, 'max': 最大值, 'allowed_values': 允许值列表}
        
    返回:
        validation_report: 验证报告
    """
    validation_report = {
        'total_records': len(df),
        'valid_records': 0,
        'invalid_fields': {}
    }
    
    valid_mask = pd.Series([True] * len(df))
    
    for col, rules in validation_rules.items():
        if col not in df.columns:
            continue
            
        col_invalid = pd.Series([False] * len(df))
        
        if 'min' in rules:
            col_invalid = col_invalid | (df[col] < rules['min'])
        if 'max' in rules:
            col_invalid = col_invalid | (df[col] > rules['max'])
        if 'allowed_values' in rules:
            col_invalid = col_invalid | (~df[col].isin(rules['allowed_values']))
            
        if col_invalid.any():
            validation_report['invalid_fields'][col] = int(col_invalid.sum())
            valid_mask = valid_mask & ~col_invalid
    
    validation_report['valid_records'] = int(valid_mask.sum())
    
    return validation_report


def standardize_categorical_values(df, mapping_dicts):
    """
    标准化分类字段值
    
    参数:
        df: pandas DataFrame
        mapping_dicts: 字典，字段名->值映射字典
        
    返回:
        标准化后的DataFrame
    """
    for col, mapping_dict in mapping_dicts.items():
        if col in df.columns:
            df[col] = df[col].map(mapping_dict).fillna(df[col])
            
    return df


if __name__ == "__main__":
    """示例用法"""
    import pandas as pd
    
    # 创建示例数据
    data = {
        'user_id': [1, 2, 3, 4, 5],
        'age': [25, 30, None, 45, 20],
        'gender': ['M', 'F', 'M', 'X', 'F'],
        'score': [85, 92, 78, 105, 65]
    }
    df = pd.DataFrame(data)
    
    print("原始数据:")
    print(df)
    print()
    
    # 验证数据类型
    schema = {'user_id': 'int64', 'age': 'float64', 'gender': 'object', 'score': 'int64'}
    validation = validate_data_types(df, schema)
    print("数据类型验证:")
    print(validation)
    print()
    
    # 处理缺失值
    df_clean = handle_missing_values(df, strategy='fill', fill_value=0)
    print("缺失值处理后:")
    print(df_clean)
    print()
    
    # 验证取值范围
    rules = {
        'age': {'min': 18, 'max': 60},
        'score': {'min': 0, 'max': 100},
        'gender': {'allowed_values': ['M', 'F']}
    }
    range_report = validate_value_ranges(df_clean, rules)
    print("取值范围验证:")
    print(range_report)