#!/usr/bin/env python3
"""
维度表处理工具 - 用于DIM层维度数据加工

包含维度属性扩展、代码映射、缓慢变化维度处理和属性标准化功能。
"""

import pandas as pd
from datetime import datetime


def create_slowly_changing_dimension(current_df, history_df, key_columns, attribute_columns):
    """
    创建缓慢变化维度（SCD）
    
    参数:
        current_df: 当前维度数据
        history_df: 历史维度数据（可为空）
        key_columns: 维度键字段列表
        attribute_columns: 属性字段列表
        
    返回:
        scd_df: 包含历史版本的缓慢变化维度
    """
    # 为当前数据添加时间标记
    current_df = current_df.copy()
    current_df['valid_from'] = datetime.now().strftime('%Y-%m-%d')
    current_df['valid_to'] = '9999-12-31'
    current_df['is_current'] = True
    
    # 如果没有历史数据，直接返回当前数据
    if history_df is None or len(history_df) == 0:
        return current_df
    
    # 合并历史数据
    scd_df = pd.concat([history_df, current_df], ignore_index=True)
    
    # 更新历史记录的有效期
    for idx, row in current_df.iterrows():
        # 找到相同键值的历史记录
        mask = True
        for key in key_columns:
            if key in history_df.columns:
                mask = mask & (history_df[key] == row[key])
        
        matching_history = history_df[mask]
        
        for _, hist_row in matching_history.iterrows():
            # 如果属性有变化，关闭历史记录
            attribute_changed = False
            for attr in attribute_columns:
                if attr in hist_row and attr in row and hist_row[attr] != row[attr]:
                    attribute_changed = True
                    break
            
            if attribute_changed:
                hist_idx = scd_df.index[
                    (scd_df['valid_to'] == '9999-12-31') & 
                    scd_df[key_columns].eq(hist_row[key_columns]).all(axis=1)
                ]
                if len(hist_idx) > 0:
                    scd_df.at[hist_idx[0], 'valid_to'] = datetime.now().strftime('%Y-%m-%d')
                    scd_df.at[hist_idx[0], 'is_current'] = False
    
    return scd_df


def map_code_values(df, code_mappings):
    """
    代码值映射转换
    
    参数:
        df: 包含代码字段的DataFrame
        code_mappings: 字典，字段名->代码映射字典
        
    返回:
        映射后的DataFrame
    """
    result_df = df.copy()
    
    for field, mapping in code_mappings.items():
        if field in result_df.columns:
            # 创建映射列
            mapped_col_name = f"{field}_name"
            result_df[mapped_col_name] = result_df[field].map(mapping)
            
            # 对于无法映射的值，保留原值
            result_df[mapped_col_name] = result_df[mapped_col_name].fillna(result_df[field])
    
    return result_df


def standardize_dimension_attributes(df, standardization_rules):
    """
    标准化维度属性
    
    参数:
        df: 维度DataFrame
        standardization_rules: 字典，字段名->标准化规则
            规则格式: {
                'type': 'categorical'|'numeric'|'date',
                'format': 格式化字符串（日期类型）,
                'categories': 分类列表（分类类型）,
                'rounding': 小数位数（数值类型）
            }
        
    返回:
        标准化后的DataFrame
    """
    result_df = df.copy()
    
    for field, rules in standardization_rules.items():
        if field not in result_df.columns:
            continue
            
        field_type = rules.get('type', 'categorical')
        
        if field_type == 'categorical':
            # 分类属性标准化
            if 'categories' in rules:
                # 将不在分类列表中的值标记为'其他'
                result_df[field] = result_df[field].apply(
                    lambda x: x if x in rules['categories'] else '其他'
                )
            
            # 统一大小写
            result_df[field] = result_df[field].str.upper()
            
        elif field_type == 'numeric':
            # 数值属性标准化
            rounding = rules.get('rounding', 2)
            result_df[field] = pd.to_numeric(result_df[field], errors='coerce')
            result_df[field] = result_df[field].round(rounding)
            
        elif field_type == 'date':
            # 日期属性标准化
            date_format = rules.get('format', '%Y-%m-%d')
            try:
                result_df[field] = pd.to_datetime(result_df[field])
                result_df[field] = result_df[field].dt.strftime(date_format)
            except Exception as e:
                print(f"标准化日期字段 '{field}' 时出错: {e}")
    
    return result_df


def create_hierarchical_dimension(df, hierarchy_config):
    """
    创建层次维度
    
    参数:
        df: 维度数据
        hierarchy_config: 层次配置字典
            格式: {
                'levels': [层级字段列表],
                'parent_child': 是否为父子层次（布尔值）
            }
        
    返回:
        hierarchy_df: 层次维度DataFrame
    """
    levels = hierarchy_config.get('levels', [])
    parent_child = hierarchy_config.get('parent_child', False)
    
    if parent_child:
        # 父子层次处理
        result_df = df.copy()
        
        # 添加层级字段
        for i, level in enumerate(levels, 1):
            level_field = f"level_{i}"
            if level in result_df.columns:
                result_df[level_field] = result_df[level]
            
        # 添加父子关系字段
        if len(levels) >= 2:
            result_df['parent_id'] = None
            # 简化处理：假设第一个字段是ID，第二个字段是父ID
            if len(result_df.columns) >= 2:
                id_field = result_df.columns[0]
                parent_field = result_df.columns[1]
                result_df['parent_id'] = result_df[parent_field]
        
        return result_df
    else:
        # 级别层次处理
        hierarchy_data = []
        
        for idx, row in df.iterrows():
            for i, level in enumerate(levels, 1):
                if level in row:
                    hierarchy_data.append({
                        'dimension_key': row.get('dimension_key', idx),
                        'level': i,
                        'level_name': level,
                        'level_value': row[level]
                    })
        
        return pd.DataFrame(hierarchy_data)


def enrich_dimension_with_attributes(dim_df, attr_df, join_keys, attribute_columns):
    """
    使用属性表丰富维度
    
    参数:
        dim_df: 维度主表
        attr_df: 属性表
        join_keys: 关联键字段列表
        attribute_columns: 需要添加的属性字段列表
        
    返回:
        丰富后的维度DataFrame
    """
    # 验证关联键
    for key in join_keys:
        if key not in dim_df.columns or key not in attr_df.columns:
            print(f"警告: 关联键 '{key}' 在维度表或属性表中不存在")
            return dim_df
    
    # 执行关联
    enriched_df = pd.merge(
        dim_df,
        attr_df[join_keys + attribute_columns],
        on=join_keys,
        how='left'
    )
    
    return enriched_df


if __name__ == "__main__":
    """示例用法"""
    # 创建示例维度数据
    dim_data = {
        'customer_id': [1, 2, 3, 4, 5],
        'customer_code': ['C001', 'C002', 'C003', 'C004', 'C005'],
        'customer_name': ['张三', '李四', '王五', '赵六', '孙七'],
        'customer_type': ['VIP', '普通', 'VIP', '普通', '普通'],
        'join_date': ['2023-01-15', '2023-02-20', '2023-03-10', '2023-01-25', '2023-04-05'],
        'credit_score': [85.5, 72.3, 90.0, 68.7, 75.2]
    }
    dim_df = pd.DataFrame(dim_data)
    
    print("原始维度数据:")
    print(dim_df)
    print()
    
    # 代码映射
    type_mapping = {
        'VIP': '重要客户',
        '普通': '普通客户'
    }
    mapped_df = map_code_values(dim_df, {'customer_type': type_mapping})
    print("代码映射后:")
    print(mapped_df[['customer_id', 'customer_type', 'customer_type_name']])
    print()
    
    # 属性标准化
    std_rules = {
        'customer_type': {'type': 'categorical', 'categories': ['VIP', '普通']},
        'credit_score': {'type': 'numeric', 'rounding': 1},
        'join_date': {'type': 'date', 'format': '%Y/%m/%d'}
    }
    standardized_df = standardize_dimension_attributes(dim_df, std_rules)
    print("属性标准化后:")
    print(standardized_df[['customer_type', 'credit_score', 'join_date']])
    print()
    
    # 创建层次维度
    hierarchy_config = {
        'levels': ['customer_type', 'join_date'],
        'parent_child': False
    }
    hierarchy_df = create_hierarchical_dimension(dim_df, hierarchy_config)
    print("层次维度:")
    print(hierarchy_df.head())