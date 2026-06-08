#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数仓数据加工工作流调度器

基于分层架构（DWD、DIM、DWS、ADS）和工作流决策树，智能调度和执行数据加工任务。
支持根据用户请求自动选择合适的工作流，包含表结构信息和Python加工逻辑。
"""

import os
import sys
import json
import yaml
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, asdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_warehouse_workflow.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataLayer(Enum):
    """数据分层枚举"""
    DWD = "dwd"      # 明细数据层
    DIM = "dim"      # 维度数据层
    DWS = "dws"      # 汇总数据层
    ADS = "ads"      # 应用数据层


class OperationType(Enum):
    """操作类型枚举"""
    ADD_FIELD = "add_field"           # 添加字段
    MODIFY_FIELD = "modify_field"     # 修改字段
    DELETE_FIELD = "delete_field"     # 删除字段
    CREATE_TABLE = "create_table"     # 创建表
    DROP_TABLE = "drop_table"         # 删除表
    PROCESS_DATA = "process_data"     # 处理数据
    QUALITY_CHECK = "quality_check"   # 质量检查
    SCHEDULE_TASK = "schedule_task"   # 调度任务


class DataCategory(Enum):
    """数据分类类型枚举"""
    TRANSACTION = "transaction"      # 交易数据
    MASTER = "master"                # 主数据
    REFERENCE = "reference"          # 参考数据
    DIMENSION = "dimension"          # 维度数据
    METRIC = "metric"                # 指标数据
    AGGREGATION = "aggregation"      # 聚合数据
    WIDE_TABLE = "wide_table"        # 宽表数据


@dataclass
class TableSchema:
    """表结构信息"""
    table_name: str
    layer: DataLayer
    category: DataCategory
    columns: List[Dict[str, Any]]
    partitions: List[str]
    location: str
    comment: str
    create_time: datetime = None
    update_time: datetime = None
    
    def __post_init__(self):
        if self.create_time is None:
            self.create_time = datetime.now()
        if self.update_time is None:
            self.update_time = datetime.now()


@dataclass
class ProcessingLogic:
    """加工逻辑信息"""
    logic_id: str
    logic_name: str
    description: str
    layer: DataLayer
    operation_type: OperationType
    python_code: str
    sql_template: str
    dependencies: List[str]
    parameters: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "logic_id": self.logic_id,
            "logic_name": self.logic_name,
            "description": self.description,
            "layer": self.layer.value,
            "operation_type": self.operation_type.value,
            "python_code": self.python_code,
            "sql_template": self.sql_template,
            "dependencies": self.dependencies,
            "parameters": self.parameters
        }


@dataclass
class WorkflowStep:
    """工作流步骤"""
    step_id: str
    step_name: str
    description: str
    layer: DataLayer
    operation_type: OperationType
    processing_logic: Optional[ProcessingLogic]
    dependencies: List[str]
    parameters: Dict[str, Any]
    expected_duration: int  # 预期执行时间（分钟）
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "step_id": self.step_id,
            "step_name": self.step_name,
            "description": self.description,
            "layer": self.layer.value,
            "operation_type": self.operation_type.value,
            "processing_logic": self.processing_logic.to_dict() if self.processing_logic else None,
            "dependencies": self.dependencies,
            "parameters": self.parameters,
            "expected_duration": self.expected_duration
        }


class WorkflowScheduler:
    """数仓工作流调度器"""
    
    def __init__(self, config_path: str = None):
        """初始化调度器"""
        self.config_path = config_path or "config/workflow_config.yaml"
        self.config = self._load_config()
        
        # 工作流决策树
        self.workflow_decision_tree = self._build_decision_tree()
        
        # 表结构缓存
        self.table_schemas: Dict[str, TableSchema] = {}
        
        # 加工逻辑缓存
        self.processing_logics: Dict[str, ProcessingLogic] = {}
        
        # 工作流缓存
        self.workflows: Dict[str, List[WorkflowStep]] = {}
        
        # 初始化默认工作流
        self._init_default_workflows()
        
        logger.info(f"工作流调度器初始化完成，配置路径: {self.config_path}")
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    if self.config_path.endswith('.yaml') or self.config_path.endswith('.yml'):
                        return yaml.safe_load(f)
                    else:
                        return json.load(f)
            else:
                logger.warning(f"配置文件 {self.config_path} 不存在，使用默认配置")
                return self._get_default_config()
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "database": {
                "type": "hive",
                "host": "localhost",
                "port": 10000,
                "username": "hive",
                "database": "default"
            },
            "hdfs": {
                "namenode": "hdfs://localhost:9000",
                "warehouse_dir": "/data/warehouse"
            },
            "scheduler": {
                "max_parallel_tasks": 5,
                "retry_count": 3,
                "retry_delay": 60,
                "timeout": 3600
            },
            "logging": {
                "level": "INFO",
                "file": "data_warehouse_workflow.log"
            }
        }
    
    def _build_decision_tree(self) -> Dict[str, Any]:
        """构建工作流决策树"""
        return {
            "dwd": {
                "transaction": {
                    "add_field": "dwd_transaction_add_field",
                    "modify_field": "dwd_transaction_modify_field",
                    "create_table": "dwd_transaction_create_table",
                    "process_data": "dwd_data_cleaning"
                },
                "master": {
                    "add_field": "dwd_master_add_field",
                    "modify_field": "dwd_master_modify_field",
                    "create_table": "dwd_master_create_table",
                    "process_data": "dwd_master_processing"
                }
            },
            "dim": {
                "dimension": {
                    "add_field": "dim_add_field",
                    "modify_field": "dim_modify_field",
                    "create_table": "dim_create_table_scd2",
                    "process_data": "dim_scd_processing"
                },
                "reference": {
                    "add_field": "dim_reference_add_field",
                    "create_table": "dim_reference_create_table",
                    "process_data": "dim_reference_processing"
                }
            },
            "dws": {
                "metric": {
                    "add_field": "dws_metric_add_field",
                    "modify_field": "dws_metric_modify_field",
                    "create_table": "dws_metric_create_table",
                    "process_data": "dws_aggregation_processing"
                },
                "aggregation": {
                    "add_field": "dws_aggregation_add_field",
                    "modify_field": "dws_aggregation_modify_field",
                    "create_table": "dws_aggregation_create_table",
                    "process_data": "dws_aggregation_processing"
                }
            },
            "ads": {
                "wide_table": {
                    "add_field": "ads_wide_table_add_field",
                    "modify_field": "ads_wide_table_modify_field",
                    "create_table": "ads_wide_table_create",
                    "process_data": "ads_wide_table_processing"
                },
                "metric": {
                    "add_field": "ads_metric_add_field",
                    "create_table": "ads_metric_create_table",
                    "process_data": "ads_metric_processing"
                }
            }
        }
    
    def _init_default_workflows(self):
        """初始化默认工作流"""
        # 加载加工逻辑
        self._load_processing_logics()
        
        # 定义默认工作流
        default_workflows = {
            "dwd_data_cleaning": [
                WorkflowStep(
                    step_id="dwd_clean_001",
                    step_name="数据去重",
                    description="DWD层数据去重处理",
                    layer=DataLayer.DWD,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("dwd_deduplication"),
                    dependencies=[],
                    parameters={"deduplication_keys": ["id", "create_time"]},
                    expected_duration=30
                ),
                WorkflowStep(
                    step_id="dwd_clean_002",
                    step_name="空值处理",
                    description="DWD层空值填充和清洗",
                    layer=DataLayer.DWD,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("dwd_null_handling"),
                    dependencies=["dwd_clean_001"],
                    parameters={"null_strategy": "fill_with_default"},
                    expected_duration=20
                ),
                WorkflowStep(
                    step_id="dwd_clean_003",
                    step_name="异常值检测",
                    description="DWD层异常值检测和处理",
                    layer=DataLayer.DWD,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("dwd_outlier_detection"),
                    dependencies=["dwd_clean_002"],
                    parameters={"outlier_method": "iqr", "threshold": 3},
                    expected_duration=25
                )
            ],
            "dim_scd_processing": [
                WorkflowStep(
                    step_id="dim_scd_001",
                    step_name="SCD Type 2处理",
                    description="维度表SCD Type 2处理",
                    layer=DataLayer.DIM,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("dim_scd2_processing"),
                    dependencies=[],
                    parameters={"scd_type": 2, "history_retention": 365},
                    expected_duration=45
                ),
                WorkflowStep(
                    step_id="dim_scd_002",
                    step_name="维度关联更新",
                    description="更新关联的维度数据",
                    layer=DataLayer.DIM,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("dim_association_update"),
                    dependencies=["dim_scd_001"],
                    parameters={"update_strategy": "incremental"},
                    expected_duration=30
                )
            ],
            "dws_aggregation_processing": [
                WorkflowStep(
                    step_id="dws_agg_001",
                    step_name="日粒度聚合",
                    description="DWS层日粒度数据聚合",
                    layer=DataLayer.DWS,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("dws_daily_aggregation"),
                    dependencies=[],
                    parameters={"granularity": "daily", "aggregation_functions": ["sum", "avg", "count"]},
                    expected_duration=60
                ),
                WorkflowStep(
                    step_id="dws_agg_002",
                    step_name="周期上卷",
                    description="DWS层周期上卷聚合",
                    layer=DataLayer.DWS,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("dws_period_rollup"),
                    dependencies=["dws_agg_001"],
                    parameters={"periods": ["weekly", "monthly", "yearly"]},
                    expected_duration=90
                )
            ],
            "ads_wide_table_processing": [
                WorkflowStep(
                    step_id="ads_wide_001",
                    step_name="宽表构建",
                    description="ADS层宽表构建",
                    layer=DataLayer.ADS,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("ads_wide_table_build"),
                    dependencies=[],
                    parameters={"table_type": "wide_table", "join_tables": 5},
                    expected_duration=120
                ),
                WorkflowStep(
                    step_id="ads_wide_002",
                    step_name="指标计算",
                    description="ADS层复合指标计算",
                    layer=DataLayer.ADS,
                    operation_type=OperationType.PROCESS_DATA,
                    processing_logic=self.processing_logics.get("ads_metric_calculation"),
                    dependencies=["ads_wide_001"],
                    parameters={"metric_types": ["composite", "ranking", "trend"]},
                    expected_duration=60
                )
            ]
        }
        
        self.workflows.update(default_workflows)
        logger.info(f"初始化了 {len(default_workflows)} 个默认工作流")
    
    def _load_processing_logics(self):
        """加载加工逻辑"""
        # 这里可以连接到知识库或从文件中加载
        # 目前使用内置逻辑
        
        # DWD层逻辑
        self.processing_logics["dwd_deduplication"] = ProcessingLogic(
            logic_id="dwd_deduplication",
            logic_name="DWD层数据去重",
            description="DWD层基于业务主键的数据去重逻辑",
            layer=DataLayer.DWD,
            operation_type=OperationType.PROCESS_DATA,
            python_code="""
def deduplicate_data(df, deduplication_keys):
    """DWD层数据去重函数"""
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy(deduplication_keys).orderBy(df['create_time'].desc())
    df_with_rank = df.withColumn('row_num', row_number().over(window_spec))
    
    # 保留最新的一条记录
    deduplicated_df = df_with_rank.filter(df_with_rank.row_num == 1).drop('row_num')
    
    logger.info(f"去重完成，原始记录数: {{df.count()}}, 去重后记录数: {{deduplicated_df.count()}}")
    return deduplicated_df""",
            sql_template="SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {deduplication_keys} ORDER BY create_time DESC) AS row_num FROM {table_name}) t WHERE row_num = 1",
            dependencies=[],
            parameters={"deduplication_keys": ["id", "create_time"]}
        )
        
        # 更多加工逻辑可以在这里添加...
        
        logger.info(f"加载了 {len(self.processing_logics)} 个加工逻辑")
    
    def select_workflow(self, user_request: str) -> Optional[str]:
        """
        根据用户请求选择工作流
        
        示例请求：
        - "给dws的表A增加一个字段，字段的解析逻辑是xx"
        - "处理DWD层交易数据"
        - "创建DIM层用户维度表"
        """
        # 解析用户请求
        request_lower = user_request.lower()
        
        # 判断数据层
        layer = None
        for data_layer in DataLayer:
            if data_layer.value in request_lower:
                layer = data_layer
                break
        
        if not layer:
            # 如果没有明确指定层，根据关键词推断
            if any(keyword in request_lower for keyword in ["明细", "交易", "原始"]):
                layer = DataLayer.DWD
            elif any(keyword in request_lower for keyword in ["维度", "属性", "主数据"]):
                layer = DataLayer.DIM
            elif any(keyword in request_lower for keyword in ["汇总", "聚合", "指标"]):
                layer = DataLayer.DWS
            elif any(keyword in request_lower for keyword in ["应用", "宽表", "报表"]):
                layer = DataLayer.ADS
            else:
                logger.warning(f"无法从请求中推断数据层: {user_request}")
                return None
        
        # 判断操作类型
        operation = None
        if "增加" in request_lower or "添加" in request_lower or "add" in request_lower:
            operation = OperationType.ADD_FIELD
        elif "修改" in request_lower or "更新" in request_lower or "modify" in request_lower:
            operation = OperationType.MODIFY_FIELD
        elif "删除" in request_lower or "删除" in request_lower or "delete" in request_lower:
            operation = OperationType.DELETE_FIELD
        elif "创建" in request_lower or "新建" in request_lower or "create" in request_lower:
            operation = OperationType.CREATE_TABLE
        elif "处理" in request_lower or "加工" in request_lower or "process" in request_lower:
            operation = OperationType.PROCESS_DATA
        elif "质量" in request_lower or "检查" in request_lower:
            operation = OperationType.QUALITY_CHECK
        
        # 判断数据分类
        category = None
        if "交易" in request_lower or "transaction" in request_lower:
            category = DataCategory.TRANSACTION
        elif "维度" in request_lower or "dimension" in request_lower:
            category = DataCategory.DIMENSION
        elif "指标" in request_lower or "metric" in request_lower:
            category = DataCategory.METRIC
        elif "聚合" in request_lower or "aggregation" in request_lower:
            category = DataCategory.AGGREGATION
        elif "宽表" in request_lower or "wide_table" in request_lower:
            category = DataCategory.WIDE_TABLE
        
        # 根据决策树选择工作流
        workflow_key = self._get_workflow_from_decision_tree(layer, category, operation)
        
        if workflow_key:
            logger.info(f"为请求 '{user_request}' 选择了工作流: {workflow_key} "
                       f"(层: {layer.value}, 分类: {category.value if category else '未指定'}, 操作: {operation.value if operation else '未指定'})")
            return workflow_key
        else:
            logger.warning(f"没有找到匹配的工作流 for 请求: {user_request}")
            return None
    
    def _get_workflow_from_decision_tree(self, layer: DataLayer, category: DataCategory, operation: OperationType) -> Optional[str]:
        """从决策树中获取工作流"""
        try:
            layer_tree = self.workflow_decision_tree.get(layer.value)
            if not layer_tree:
                return None
            
            # 如果有分类，优先使用分类
            if category:
                category_tree = layer_tree.get(category.value)
                if category_tree and operation:
                    return category_tree.get(operation.value)
            
            # 如果没有分类或分类中没找到，尝试在层中查找
            if operation:
                # 遍历该层下的所有分类
                for cat_key, cat_tree in layer_tree.items():
                    if operation.value in cat_tree:
                        return cat_tree[operation.value]
            
            return None
        except Exception as e:
            logger.error(f"从决策树获取工作流失败: {e}")
            return None
    
    def execute_workflow(self, workflow_key: str, parameters: Dict[str, Any] = None) -> bool:
        """执行工作流"""
        if workflow_key not in self.workflows:
            logger.error(f"工作流不存在: {workflow_key}")
            return False
        
        workflow_steps = self.workflows[workflow_key]
        parameters = parameters or {}
        
        logger.info(f"开始执行工作流: {workflow_key}, 共 {len(workflow_steps)} 个步骤")
        
        # 执行每个步骤
        executed_steps = []
        for step in workflow_steps:
            try:
                logger.info(f"执行步骤: {step.step_name} ({step.step_id})")
                
                # 检查依赖
                for dep in step.dependencies:
                    if dep not in executed_steps:
                        logger.error(f"依赖步骤 {dep} 未执行，跳过步骤 {step.step_id}")
                        return False
                
                # 合并参数
                step_params = {**step.parameters, **parameters}
                
                # 执行步骤逻辑
                success = self._execute_step(step, step_params)
                
                if success:
                    executed_steps.append(step.step_id)
                    logger.info(f"步骤 {step.step_id} 执行成功")
                else:
                    logger.error(f"步骤 {step.step_id} 执行失败")
                    return False
                
            except Exception as e:
                logger.error(f"执行步骤 {step.step_id} 时发生异常: {e}")
                return False
        
        logger.info(f"工作流 {workflow_key} 执行完成，成功执行了 {len(executed_steps)} 个步骤")
        return True
    
    def _execute_step(self, step: WorkflowStep, parameters: Dict[str, Any]) -> bool:
        """执行单个步骤"""
        # 这里可以实现具体的执行逻辑
        # 例如：执行Python代码、SQL语句、调用外部系统等
        
        logger.info(f"执行步骤逻辑: {step.description}")
        logger.info(f"步骤参数: {parameters}")
        
        if step.processing_logic:
            logger.info(f"使用加工逻辑: {step.processing_logic.logic_name}")
            # 这里可以执行processing_logic中的python_code或sql_template
            
        # 模拟执行成功
        return True
    
    def add_table_schema(self, schema: TableSchema):
        """添加表结构信息"""
        self.table_schemas[schema.table_name] = schema
        logger.info(f"添加表结构: {schema.table_name} ({schema.layer.value}层)")
    
    def get_table_schema(self, table_name: str) -> Optional[TableSchema]:
        """获取表结构信息"""
        return self.table_schemas.get(table_name)
    
    def add_processing_logic(self, logic: ProcessingLogic):
        """添加工资逻辑"""
        self.processing_logics[logic.logic_id] = logic
        logger.info(f"添加工资逻辑: {logic.logic_name} ({logic.layer.value}层)")
    
    def create_custom_workflow(self, workflow_name: str, steps: List[WorkflowStep]):
        """创建自定义工作流"""
        self.workflows[workflow_name] = steps
        logger.info(f"创建自定义工作流: {workflow_name}, 包含 {len(steps)} 个步骤")
    
    def export_workflow_config(self, output_path: str):
        """导出工作流配置"""
        config = {
            "workflows": {},
            "table_schemas": {},
            "processing_logics": {}
        }
        
        # 导出工作流
        for workflow_name, steps in self.workflows.items():
            config["workflows"][workflow_name] = [step.to_dict() for step in steps]
        
        # 导出表结构
        for table_name, schema in self.table_schemas.items():
            config["table_schemas"][table_name] = asdict(schema)
        
        # 导出加工逻辑
        for logic_id, logic in self.processing_logics.items():
            config["processing_logics"][logic_id] = logic.to_dict()
        
        # 保存到文件
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"工作流配置已导出到: {output_path}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="数仓数据加工工作流调度器")
    parser.add_argument("--config", type=str, help="配置文件路径")
    parser.add_argument("--request", type=str, required=True, help="用户请求，例如：'给dws的表A增加一个字段'")
    parser.add_argument("--execute", action="store_true", help="是否执行选择的工作流")
    parser.add_argument("--export", type=str, help="导出配置到指定路径")
    
    args = parser.parse_args()
    
    # 创建调度器
    scheduler = WorkflowScheduler(args.config)
    
    # 选择工作流
    workflow_key = scheduler.select_workflow(args.request)
    
    if not workflow_key:
        print(f"错误: 无法为请求 '{args.request}' 选择合适的工作流")
        return 1
    
    print(f"为请求 '{args.request}' 选择的工作流: {workflow_key}")
    
    # 如果需要执行
    if args.execute:
        print(f"开始执行工作流: {workflow_key}")
        success = scheduler.execute_workflow(workflow_key)
        if success:
            print(f"工作流执行成功")
        else:
            print(f"工作流执行失败")
            return 1
    
    # 如果需要导出配置
    if args.export:
        scheduler.export_workflow_config(args.export)
        print(f"配置已导出到: {args.export}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())