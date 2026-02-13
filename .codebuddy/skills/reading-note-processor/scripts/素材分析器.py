#!/usr/bin/env python3
"""
读书笔记素材分析器
用于分析用户输入的阅读素材，包括重点划线、灵感和逻辑重构等内容
支持16种逻辑骨架框架的素材识别和分类
新增读物类型判断功能，支持核心类型识别、二级分类、混合类型判断和置信度计算
"""

import re
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum


class MaterialType(Enum):
    """素材类型枚举"""
    HIGHLIGHT = "重点划线"
    INSPIRATION = "灵感"
    LOGIC_RECONSTRUCTION = "逻辑重构"
    QUESTION = "疑问"
    CONNECTION = "关联思考"
    FRAMEWORK_CLUE = "框架线索"


class FrameworkType(Enum):
    """逻辑骨架框架类型枚举"""
    TAXONOMY = "树状分类"
    CAUSAL = "因果解释"
    PROCESS = "流程序列"
    ARGUMENT = "论点案例"
    DAOFA_SHU_QI = "道-法-术-器"
    WWWH = "5W1H深度分析"
    WWH = "2W1H快速理解"
    FEEDBACK_LOOP = "增强调节回路"
    COMPARATIVE = "比较分析"
    TIMELINE = "时间线"
    SPATIAL = "空间结构"
    CONCEPTUAL = "概念体系"
    PROBLEM_SOLUTION = "问题解决方案"
    GROWTH_JOURNEY = "成长历程"
    PHENOMENON_PRINCIPLE = "现象原理应用"
    THEME_TECHNIQUE = "主题手法意义"


class CoreType(Enum):
    """核心类型枚举"""
    PRACTICAL = "实用型"
    THEORETICAL = "理论型"
    IMAGINATIVE = "想象文学"


class SecondaryType(Enum):
    """二级分类枚举"""
    # 想象文学
    NOVEL = "小说"
    POETRY = "诗歌"
    DRAMA = "戏剧"
    
    # 理论型
    HISTORY = "历史类"
    SCIENCE = "科学类"
    PHILOSOPHY = "哲学类"
    MATH = "数学类"
    SOCIAL_SCIENCE = "社会科学类"
    THEORY_COMPREHENSIVE = "理论综合类"
    
    # 实用型
    BUSINESS = "商业类"
    TECHNOLOGY = "技术类"
    HEALTH = "健康类"
    EDUCATION = "教育类"
    PRACTICAL_COMPREHENSIVE = "实用综合类"


class StructureType(Enum):
    """结构类型枚举"""
    STRONG = "强结构"
    WEAK = "弱结构"


@dataclass
class Material:
    """素材数据类"""
    type: MaterialType
    content: str
    source_page: str = ""
    tags: List[str] = None
    priority: int = 1  # 1-5，优先级越高越重要
    framework_hints: List[FrameworkType] = None  # 框架线索
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.framework_hints is None:
            self.framework_hints = []


@dataclass
class BookTypeAnalysis:
    """读物类型分析结果"""
    core_type: CoreType
    secondary_types: List[SecondaryType]
    structure_type: StructureType
    hybrid_flag: bool
    confidence: float
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "core_type": self.core_type.value,
            "secondary_types": [st.value for st in self.secondary_types],
            "structure_type": self.structure_type.value,
            "hybrid_flag": self.hybrid_flag,
            "confidence": round(self.confidence, 2)
        }


class MaterialAnalyzer:
    """素材分析器主类"""
    
    def __init__(self):
        self.patterns = {
            MaterialType.HIGHLIGHT: [
                r'重点划线[:：]\s*(.+)',
                r'划线[:：]\s*(.+)',
                r'重点[:：]\s*(.+)'
            ],
            MaterialType.INSPIRATION: [
                r'灵感[:：]\s*(.+)',
                r'启发[:：]\s*(.+)',
                r'想到[:：]\s*(.+)'
            ],
            MaterialType.LOGIC_RECONSTRUCTION: [
                r'逻辑重构[:：]\s*(.+)',
                r'重构[:：]\s*(.+)',
                r'重新组织[:：]\s*(.+)'
            ],
            MaterialType.QUESTION: [
                r'疑问[:：]\s*(.+)',
                r'问题[:：]\s*(.+)',
                r'为什么[:：]\s*(.+)'
            ],
            MaterialType.CONNECTION: [
                r'关联[:：]\s*(.+)',
                r'联系[:：]\s*(.+)',
                r'联想到[:：]\s*(.+)'
            ],
            MaterialType.FRAMEWORK_CLUE: [
                r'框架[:：]\s*(.+)',
                r'结构[:：]\s*(.+)',
                r'组织方式[:：]\s*(.+)'
            ]
        }
        
        # 框架关键词映射
        self.framework_keywords = {
            FrameworkType.TAXONOMY: [
                '分类', '层级', '类别', '体系', '分类法', '树状', '分支', '子类'
            ],
            FrameworkType.CAUSAL: [
                '原因', '结果', '影响', '因果关系', '导致', '引发', '因为所以'
            ],
            FrameworkType.PROCESS: [
                '步骤', '流程', '顺序', '阶段', '环节', '操作', '方法'
            ],
            FrameworkType.ARGUMENT: [
                '论点', '论据', '论证', '案例', '证据', '说服', '辩论'
            ],
            FrameworkType.DAOFA_SHU_QI: [
                '道', '法', '术', '器', '规律', '方法论', '技巧', '工具'
            ],
            FrameworkType.WWWH: [
                '是什么', '为什么', '怎么做', '何时', '何地', '谁', '5W1H'
            ],
            FrameworkType.WWH: [
                '是什么', '为什么', '怎么做', '2W1H', '快速理解'
            ],
            FrameworkType.FEEDBACK_LOOP: [
                '反馈', '回路', '增强', '调节', '平衡', '动力', '阻力'
            ],
            FrameworkType.COMPARATIVE: [
                '比较', '对比', '优劣', '差异', '选择', '方案', '选项'
            ],
            FrameworkType.TIMELINE: [
                '时间', '历史', '发展', '历程', '节点', '阶段', '演变'
            ],
            FrameworkType.SPATIAL: [
                '空间', '结构', '布局', '位置', '区域', '分布', '地理'
            ]
        }
        
        # 核心类型关键词
        self.core_type_keywords = {
            CoreType.PRACTICAL: [
                '如何', '指南', '手册', '技巧', '方法', '实操', '实战',
                '步骤', '提升', '改变', '训练', '养成', '操作', '应用'
            ],
            CoreType.THEORETICAL: [
                '原理', '本质', '导论', '概论', '哲学', '研究', '理论',
                '分析', '思考', '批判', '探讨', '论证', '历史', '科学', '数学'
            ],
            CoreType.IMAGINATIVE: [
                '小说', '诗集', '戏剧', '故事', '传', '记', '演义',
                '童话', '寓言', '散文', '诗歌', '剧本'
            ]
        }
        
        # 二级分类关键词
        self.secondary_type_keywords = {
            # 想象文学
            SecondaryType.NOVEL: ['小说', '故事', '情节', '人物', '叙事'],
            SecondaryType.POETRY: ['诗', '词', '歌', '赋', '韵', '押韵'],
            SecondaryType.DRAMA: ['戏剧', '幕', '场', '台词', '对白', '舞台'],
            
            # 理论型
            SecondaryType.HISTORY: ['世纪', '年代', '公元前', '王朝', '历史', '古代', '近代', '现代', '事件', '时期'],
            SecondaryType.SCIENCE: ['实验', '数据', '观测', '验证', '定律', '公式', '定理', '证明', '物理', '化学', '生物', '自然'],
            SecondaryType.PHILOSOPHY: ['存在', '意识', '道德', '逻辑', '真理', '形而上学', '认识', '价值', '伦理', '美学'],
            SecondaryType.MATH: ['证明', '公理', '推导', '算式', '集合', '函数', '方程', '几何', '代数', '统计'],
            SecondaryType.SOCIAL_SCIENCE: ['社会', '经济', '政治', '文化', '心理', '人类', '组织', '制度', '行为'],
            
            # 实用型
            SecondaryType.BUSINESS: ['管理', '领导', '商业', '市场', '营销', '战略', '创业', '投资', '财务'],
            SecondaryType.TECHNOLOGY: ['编程', '代码', '开发', '设计', '制作', '技术', '软件', '硬件', '工程'],
            SecondaryType.HEALTH: ['健身', '饮食', '健康', '养生', '心理', '家庭', '关系', '沟通', '时间管理'],
            SecondaryType.EDUCATION: ['学习', '教育', '教学', '记忆', '阅读', '写作', '考试', '训练']
        }
    
    def analyze_text(self, text: str) -> List[Material]:
        """
        分析文本中的素材
        
        Args:
            text: 用户输入的文本内容
            
        Returns:
            分析得到的素材列表
        """
        materials = []
        
        # 按行分割文本
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # 尝试匹配各种素材类型
            material = self._parse_line(line)
            if material:
                materials.append(material)
        
        return materials
    
    def _parse_line(self, line: str) -> Material:
        """解析单行文本"""
        for material_type, patterns in self.patterns.items():
            for pattern in patterns:
                match = re.match(pattern, line, re.IGNORECASE)
                if match:
                    content = match.group(1).strip()
                    framework_hints = self._detect_framework_hints(content)
                    return Material(
                        type=material_type,
                        content=content,
                        tags=self._extract_tags(content),
                        priority=self._assess_priority(content, material_type),
                        framework_hints=framework_hints
                    )
        
        # 如果没有匹配到特定模式，默认为重点划线
        framework_hints = self._detect_framework_hints(line)
        return Material(
            type=MaterialType.HIGHLIGHT,
            content=line,
            tags=self._extract_tags(line),
            priority=self._assess_priority(line, MaterialType.HIGHLIGHT),
            framework_hints=framework_hints
        )
    
    def _detect_framework_hints(self, content: str) -> List[FrameworkType]:
        """检测内容中的框架线索"""
        hints = []
        
        for framework_type, keywords in self.framework_keywords.items():
            # 检查是否包含该框架的关键词
            if any(keyword in content for keyword in keywords):
                hints.append(framework_type)
        
        return hints
    
    def _extract_tags(self, content: str) -> List[str]:
        """从内容中提取标签"""
        tags = []
        
        # 提取关键词作为标签
        keywords = [
            '认知', '决策', '学习', '记忆', '思考', '问题', '解决',
            '方法', '策略', '技巧', '原理', '理论', '实践',
            '效率', '效果', '质量', '优化', '改进', '创新',
            '分类', '因果', '流程', '论证', '比较', '时间', '空间'
        ]
        
        for keyword in keywords:
            if keyword in content:
                tags.append(keyword)
        
        return tags
    
    def _assess_priority(self, content: str, material_type: MaterialType) -> int:
        """评估素材优先级"""
        priority = 1
        
        # 根据类型设置基础优先级
        type_priority = {
            MaterialType.LOGIC_RECONSTRUCTION: 5,
            MaterialType.INSPIRATION: 4,
            MaterialType.FRAMEWORK_CLUE: 4,
            MaterialType.CONNECTION: 3,
            MaterialType.HIGHLIGHT: 2,
            MaterialType.QUESTION: 1
        }
        
        priority = type_priority.get(material_type, 1)
        
        # 根据内容长度和复杂度调整优先级
        if len(content) > 50:  # 较长内容通常更重要
            priority = min(priority + 1, 5)
        
        # 包含特定关键词提升优先级
        important_keywords = ['重要', '关键', '核心', '本质', '原理', '框架', '结构']
        if any(keyword in content for keyword in important_keywords):
            priority = min(priority + 1, 5)
        
        return priority
    
    def categorize_materials(self, materials: List[Material]) -> Dict[str, List[Material]]:
        """
        按类型分类素材
        
        Args:
            materials: 素材列表
            
        Returns:
            按类型分类的素材字典
        """
        categorized = {}
        
        for material_type in MaterialType:
            categorized[material_type.value] = [
                material for material in materials 
                if material.type == material_type
            ]
        
        return categorized
    
    def analyze_framework_hints(self, materials: List[Material]) -> Dict[FrameworkType, int]:
        """
        分析素材中的框架线索，推荐最适合的框架
        
        Args:
            materials: 素材列表
            
        Returns:
            框架类型及其得分
        """
        framework_scores = {}
        
        # 初始化所有框架的分数
        for framework_type in FrameworkType:
            framework_scores[framework_type] = 0
        
        # 根据素材内容计算框架得分
        for material in materials:
            # 直接框架线索加分更多
            if material.type == MaterialType.FRAMEWORK_CLUE:
                for hint in material.framework_hints:
                    framework_scores[hint] += 3
            
            # 其他类型素材的框架线索加分较少
            for hint in material.framework_hints:
                framework_scores[hint] += 1
            
            # 根据内容关键词加分
            for framework_type, keywords in self.framework_keywords.items():
                keyword_count = sum(1 for keyword in keywords if keyword in material.content)
                framework_scores[framework_type] += keyword_count * 0.5
        
        return framework_scores
    
    def recommend_framework(self, materials: List[Material]) -> Tuple[FrameworkType, Dict[FrameworkType, int]]:
        """
        推荐最适合的框架
        
        Args:
            materials: 素材列表
            
        Returns:
            (推荐框架, 所有框架得分)
        """
        framework_scores = self.analyze_framework_hints(materials)
        
        if not framework_scores:
            return FrameworkType.WWH, framework_scores  # 默认使用2W1H
        
        # 找出得分最高的框架
        recommended_framework = max(framework_scores.items(), key=lambda x: x[1])[0]
        
        return recommended_framework, framework_scores
    
    def generate_summary(self, materials: List[Material]) -> Dict:
        """
        生成素材分析摘要
        
        Args:
            materials: 素材列表
            
        Returns:
            分析摘要字典
        """
        summary = {
            'total_count': len(materials),
            'type_distribution': {},
            'high_priority_count': 0,
            'avg_priority': 0,
            'common_tags': [],
            'framework_recommendation': None,
            'framework_scores': {}
        }
        
        if not materials:
            return summary
        
        # 类型分布
        for material_type in MaterialType:
            count = len([m for m in materials if m.type == material_type])
            summary['type_distribution'][material_type.value] = count
        
        # 高优先级计数
        summary['high_priority_count'] = len([m for m in materials if m.priority >= 4])
        
        # 平均优先级
        summary['avg_priority'] = sum(m.priority for m in materials) / len(materials)
        
        # 常见标签
        all_tags = [tag for m in materials for tag in m.tags]
        tag_counts = {}
        for tag in all_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        summary['common_tags'] = sorted(
            tag_counts.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:5]  # 取前5个常见标签
        
        # 框架推荐
        recommended_framework, framework_scores = self.recommend_framework(materials)
        summary['framework_recommendation'] = recommended_framework.value
        summary['framework_scores'] = {
            framework.value: score 
            for framework, score in framework_scores.items()
            if score > 0  # 只显示有得分的框架
        }
        
        return summary
    
    def analyze_book_type(self, title: str, preface: str = "", chapter_samples: List[str] = None) -> BookTypeAnalysis:
        """
        分析读物类型
        
        Args:
            title: 书名
            preface: 前言/简介内容
            chapter_samples: 章节抽样内容列表
            
        Returns:
            读物类型分析结果
        """
        if chapter_samples is None:
            chapter_samples = []
        
        # 阶段1：核心类型识别
        core_type = self._identify_core_type(title, preface, chapter_samples)
        
        # 阶段2：细化分类
        secondary_types = self._identify_secondary_types(title, preface, chapter_samples, core_type)
        
        # 阶段3：混合类型判断
        hybrid_flag = self._detect_hybrid_type(title, preface, chapter_samples, core_type, secondary_types)
        
        # 结构分析
        structure_type = self._analyze_structure_type(chapter_samples)
        
        # 置信度计算
        confidence = self._calculate_confidence(title, preface, chapter_samples, core_type, secondary_types)
        
        return BookTypeAnalysis(
            core_type=core_type,
            secondary_types=secondary_types,
            structure_type=structure_type,
            hybrid_flag=hybrid_flag,
            confidence=confidence
        )
    
    def _identify_core_type(self, title: str, preface: str, chapter_samples: List[str]) -> CoreType:
        """识别核心类型"""
        # 步骤1：检查是否为想象文学
        if self._is_imaginative_literature(title, preface, chapter_samples):
            return CoreType.IMAGINATIVE
        
        # 步骤2：检查是否为实用型
        if self._is_practical_type(title, preface):
            return CoreType.PRACTICAL
        
        # 步骤3：默认为理论型
        return CoreType.THEORETICAL
    
    def _is_imaginative_literature(self, title: str, preface: str, chapter_samples: List[str]) -> bool:
        """判断是否为想象文学"""
        # 标题包含想象文学触发词
        title_contains_keywords = any(keyword in title for keyword in self.core_type_keywords[CoreType.IMAGINATIVE])
        
        # 前言包含想象文学特征
        preface_contains_features = any(keyword in preface for keyword in ['故事', '情节', '人物', '虚构', '情感', '想象', '创作', '叙事'])
        
        # 章节抽样为叙事性内容
        narrative_content = any(self._is_narrative_content(sample) for sample in chapter_samples)
        
        return title_contains_keywords and (preface_contains_features or narrative_content)
    
    def _is_practical_type(self, title: str, preface: str) -> bool:
        """判断是否为实用型"""
        # 计算标题关键词频率
        practical_title_count = sum(1 for keyword in self.core_type_keywords[CoreType.PRACTICAL] if keyword in title)
        theoretical_title_count = sum(1 for keyword in self.core_type_keywords[CoreType.THEORETICAL] if keyword in title)
        
        # 计算前言特征词频
        practical_preface_count = sum(1 for keyword in ['教会读者', '步骤', '方法', '应用', '效果', '实践', '操作', '建议', '应该'] if keyword in preface)
        theoretical_preface_count = sum(1 for keyword in ['探讨', '研究', '分析', '论证', '解释', '证明', '阐述', '理解'] if keyword in preface)
        
        return (practical_title_count > theoretical_title_count) or (practical_preface_count > theoretical_preface_count)
    
    def _identify_secondary_types(self, title: str, preface: str, chapter_samples: List[str], core_type: CoreType) -> List[SecondaryType]:
        """识别二级分类"""
        all_text = title + " " + preface + " " + " ".join(chapter_samples)
        
        if core_type == CoreType.IMAGINATIVE:
            return self._identify_imaginative_types(all_text)
        elif core_type == CoreType.THEORETICAL:
            return self._identify_theoretical_types(all_text)
        else:  # PRACTICAL
            return self._identify_practical_types(all_text)
    
    def _identify_imaginative_types(self, text: str) -> List[SecondaryType]:
        """识别想象文学二级分类"""
        # 检查体裁标识
        if any(keyword in text for keyword in ['诗', '词', '歌', '赋', '韵']):
            return [SecondaryType.POETRY]
        elif any(keyword in text for keyword in ['第X幕', '场', '台词', '对白', '舞台']):
            return [SecondaryType.DRAMA]
        else:
            return [SecondaryType.NOVEL]
    
    def _identify_theoretical_types(self, text: str) -> List[SecondaryType]:
        """识别理论型二级分类"""
        type_scores = {}
        
        # 计算各领域关键词频率
        for secondary_type, keywords in self.secondary_type_keywords.items():
            if secondary_type in [SecondaryType.NOVEL, SecondaryType.POETRY, SecondaryType.DRAMA]:
                continue  # 跳过想象文学类型
            
            score = sum(1 for keyword in keywords if keyword in text)
            type_scores[secondary_type] = score
        
        # 选择得分最高的前2个类型
        top_types = sorted(type_scores.items(), key=lambda x: x[1], reverse=True)[:2]
        
        # 如果最高频率 < 总关键词的40%，标记为理论综合类
        if top_types and top_types[0][1] < sum(type_scores.values()) * 0.4:
            return [SecondaryType.THEORY_COMPREHENSIVE]
        
        return [st for st, score in top_types if score > 0]
    
    def _identify_practical_types(self, text: str) -> List[SecondaryType]:
        """识别实用型二级分类"""
        type_scores = {}
        
        # 计算各领域关键词频率
        for secondary_type, keywords in self.secondary_type_keywords.items():
            if secondary_type in [SecondaryType.BUSINESS, SecondaryType.TECHNOLOGY, SecondaryType.HEALTH, SecondaryType.EDUCATION]:
                score = sum(1 for keyword in keywords if keyword in text)
                type_scores[secondary_type] = score
        
        # 选择得分最高的前2个类型
        top_types = sorted(type_scores.items(), key=lambda x: x[1], reverse=True)[:2]
        
        # 如果没有明显领域倾向，标记为实用综合类
        if not top_types or top_types[0][1] == 0:
            return [SecondaryType.PRACTICAL_COMPREHENSIVE]
        
        return [st for st, score in top_types if score > 0]
    
    def _detect_hybrid_type(self, title: str, preface: str, chapter_samples: List[str], core_type: CoreType, secondary_types: List[SecondaryType]) -> bool:
        """检测混合类型"""
        all_text = title + " " + preface + " " + " ".join(chapter_samples)
        
        # 多领域混合检测
        if len(secondary_types) > 1:
            type_scores = {}
            for st in secondary_types:
                keywords = self.secondary_type_keywords.get(st, [])
                type_scores[st] = sum(1 for keyword in keywords if keyword in all_text)
            
            if len(type_scores) >= 2:
                scores = list(type_scores.values())
                max_score = max(scores)
                second_max = sorted(scores, reverse=True)[1] if len(scores) > 1 else 0
                
                if max_score < 2 * second_max:
                    return True
        
        # 理论-实用混合检测
        practical_score = sum(1 for keyword in self.core_type_keywords[CoreType.PRACTICAL] if keyword in all_text)
        theoretical_score = sum(1 for keyword in self.core_type_keywords[CoreType.THEORETICAL] if keyword in all_text)
        
        total_score = practical_score + theoretical_score
        if total_score > 0:
            practical_ratio = practical_score / total_score
            theoretical_ratio = theoretical_score / total_score
            
            if practical_ratio > 0.3 and theoretical_ratio > 0.3 and abs(practical_ratio - theoretical_ratio) < 0.2:
                return True
        
        return False
    
    def _analyze_structure_type(self, chapter_samples: List[str]) -> StructureType:
        """分析结构类型"""
        if not chapter_samples:
            return StructureType.WEAK
        
        # 简化的结构强度分析
        # 在实际应用中，这里应该分析章节间的逻辑关系
        structure_strength = 0.5  # 默认中等强度
        
        return StructureType.STRONG if structure_strength > 0.6 else StructureType.WEAK
    
    def _calculate_confidence(self, title: str, preface: str, chapter_samples: List[str], core_type: CoreType, secondary_types: List[SecondaryType]) -> float:
        """计算置信度"""
        base_confidence = 0.7
        
        # 加分项
        if self._has_clear_title_features(title, core_type):
            base_confidence += 0.1
        
        if self._has_clear_preface_features(preface, core_type):
            base_confidence += 0.15
        
        if len(chapter_samples) > 0:
            base_confidence += 0.05
        
        if len(chapter_samples) >= 3:
            base_confidence += 0.1
        
        # 减分项
        if not preface and not chapter_samples:
            base_confidence -= 0.2
        
        if self._has_conflicting_features(title, preface, core_type):
            base_confidence -= 0.15
        
        if len(secondary_types) == 0:
            base_confidence -= 0.1
        
        # 限制在0.5-0.95之间
        return max(0.5, min(0.95, base_confidence))
    
    def _has_clear_title_features(self, title: str, core_type: CoreType) -> bool:
        """判断标题特征是否明显"""
        keywords = self.core_type_keywords[core_type]
        return any(keyword in title for keyword in keywords)
    
    def _has_clear_preface_features(self, preface: str, core_type: CoreType) -> bool:
        """判断前言特征是否明显"""
        if core_type == CoreType.PRACTICAL:
            features = ['教会读者', '步骤', '方法', '应用', '效果', '实践', '操作', '建议', '应该']
        elif core_type == CoreType.THEORETICAL:
            features = ['探讨', '研究', '分析', '论证', '解释', '证明', '阐述', '理解']
        else:  # IMAGINATIVE
            features = ['故事', '情节', '人物', '虚构', '情感', '想象', '创作', '叙事']
        
        return any(feature in preface for feature in features)
    
    def _has_conflicting_features(self, title: str, preface: str, core_type: CoreType) -> bool:
        """判断是否存在特征冲突"""
        # 简化的冲突检测
        # 在实际应用中，这里应该进行更复杂的冲突分析
        return False
    
    def _is_narrative_content(self, text: str) -> bool:
        """判断是否为叙事性内容"""
        narrative_keywords = ['说', '道', '想', '感觉', '看到', '听到', '经历', '发生']
        return any(keyword in text for keyword in narrative_keywords)


def main():
    """主函数 - 测试用"""
    # 测试素材分析
    test_text = """
    重点划线：认知偏差会影响决策质量
    灵感：这和我的投资决策失误有关
    逻辑重构：需要建立检查清单来避免偏差
    疑问：为什么人们容易忽视自己的认知偏差？
    关联：这与之前学习的确认偏误有联系
    框架：这本书采用了因果解释的框架结构
    """
    
    analyzer = MaterialAnalyzer()
    materials = analyzer.analyze_text(test_text)
    
    print("=== 素材分析结果 ===")
    for i, material in enumerate(materials, 1):
        print(f"{i}. [{material.type.value}] {material.content}")
        print(f"   标签: {material.tags}, 优先级: {material.priority}")
        if material.framework_hints:
            print(f"   框架线索: {[hint.value for hint in material.framework_hints]}")
    
    categorized = analyzer.categorize_materials(materials)
    print("\n=== 分类结果 ===")
    for category, items in categorized.items():
        print(f"{category}: {len(items)}个")
    
    # 框架推荐
    recommended_framework, framework_scores = analyzer.recommend_framework(materials)
    print(f"\n=== 框架推荐 ===")
    print(f"推荐框架: {recommended_framework.value}")
    print("框架得分:")
    for framework, score in sorted(framework_scores.items(), key=lambda x: x[1], reverse=True):
        if score > 0:
            print(f"  {framework.value}: {score}")
    
    summary = analyzer.generate_summary(materials)
    print("\n=== 分析摘要 ===")
    print(f"总素材数: {summary['total_count']}")
    print(f"高优先级素材: {summary['high_priority_count']}")
    print(f"平均优先级: {summary['avg_priority']:.2f}")
    print(f"常见标签: {summary['common_tags']}")
    print(f"推荐框架: {summary['framework_recommendation']}")
    
    # 测试读物类型分析
    print("\n=== 读物类型分析测试 ===")
    book_analysis = analyzer.analyze_book_type(
        title="思考，快与慢",
        preface="本书探讨人类思维的两种系统，分析认知偏差对决策的影响",
        chapter_samples=["系统1的快速思考特征", "系统2的慢速思考过程", "认知偏差的分类和影响"]
    )
    
    print("读物类型分析结果:")
    print(book_analysis.to_dict())


if __name__ == "__main__":
    main()