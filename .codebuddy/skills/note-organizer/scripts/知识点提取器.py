#!/usr/bin/env python3
"""
知识点提取器 - 读书笔记知识点自动提取工具

功能：
1. 从读书笔记中自动识别和提取知识点
2. 按照高内聚低耦合原则进行知识点分类
3. 生成结构化的知识点输出
4. 支持多种笔记格式的输入

使用方法：
    python 知识点提取器.py --input 笔记文件.txt --output 知识点.json
    python 知识点提取器.py --text "笔记内容文本" --format json
"""

import re
import json
import argparse
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class KnowledgePoint:
    """知识点数据结构"""
    title: str
    content: str
    category: str
    keywords: List[str]
    level: str  # micro, meso, macro
    related_points: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "title": self.title,
            "content": self.content,
            "category": self.category,
            "keywords": self.keywords,
            "level": self.level,
            "related_points": self.related_points
        }


class KnowledgeExtractor:
    """知识点提取器核心类"""
    
    def __init__(self):
        # 知识点识别模式
        self.patterns = {
            'concept': [
                r'(?:定义|概念|什么是)([^。！？]+)',
                r'(?:核心概念|关键概念)([^。！？]+)',
                r'(?:术语|名词)([^。！？]+)的定义'
            ],
            'principle': [
                r'(?:原理|定理|定律)([^。！？]+)',
                r'(?:基本|核心)原理([^。！？]+)',
                r'(?:工作|运行)原理([^。！？]+)'
            ],
            'method': [
                r'(?:方法|步骤|流程)([^。！？]+)',
                r'(?:如何|怎样)([^。！？]+)',
                r'(?:操作|使用)方法([^。！？]+)'
            ],
            'case': [
                r'(?:案例|实例|例子)([^。！？]+)',
                r'(?:应用|实践)案例([^。！？]+)',
                r'(?:典型|代表性)案例([^。！？]+)'
            ]
        }
        
        # 关键词库
        self.keywords = {
            '技术类': ['编程', '代码', '算法', '框架', '系统', '开发', '测试'],
            '理论类': ['理论', '原理', '定律', '公式', '模型', '假设'],
            '实践类': ['操作', '步骤', '方法', '技巧', '经验', '实践'],
            '思维类': ['思维', '思考', '分析', '决策', '逻辑', '推理']
        }
    
    def extract_knowledge_points(self, text: str) -> List[KnowledgePoint]:
        """从文本中提取知识点"""
        points = []
        
        # 按段落分割文本
        paragraphs = self._split_paragraphs(text)
        
        for para in paragraphs:
            if self._is_knowledge_paragraph(para):
                point = self._extract_single_point(para)
                if point:
                    points.append(point)
        
        return points
    
    def _split_paragraphs(self, text: str) -> List[str]:
        """将文本分割为段落"""
        # 按换行符分割，过滤空段落
        paragraphs = [p.strip() for p in text.split('\n') if p.strip()]
        return paragraphs
    
    def _is_knowledge_paragraph(self, paragraph: str) -> bool:
        """判断段落是否包含知识点"""
        # 检查段落长度和内容特征
        if len(paragraph) < 20 or len(paragraph) > 500:
            return False
        
        # 检查是否包含知识性内容的关键词
        knowledge_keywords = ['定义', '原理', '方法', '步骤', '案例', '概念', '理论']
        return any(keyword in paragraph for keyword in knowledge_keywords)
    
    def _extract_single_point(self, paragraph: str) -> KnowledgePoint:
        """从单个段落中提取知识点"""
        # 提取标题
        title = self._extract_title(paragraph)
        
        # 确定分类
        category = self._classify_category(paragraph)
        
        # 确定知识粒度
        level = self._determine_level(paragraph)
        
        # 提取关键词
        keywords = self._extract_keywords(paragraph)
        
        return KnowledgePoint(
            title=title,
            content=paragraph,
            category=category,
            keywords=keywords,
            level=level,
            related_points=[]
        )
    
    def _extract_title(self, paragraph: str) -> str:
        """从段落中提取标题"""
        # 尝试提取第一句话作为标题
        sentences = re.split(r'[。！？]', paragraph)
        if sentences:
            title = sentences[0].strip()
            # 限制标题长度
            if len(title) > 50:
                title = title[:47] + '...'
            return title
        return "未命名知识点"
    
    def _classify_category(self, paragraph: str) -> str:
        """分类知识点类型"""
        for category, patterns in self.patterns.items():
            for pattern in patterns:
                if re.search(pattern, paragraph):
                    return category
        
        # 根据关键词分类
        for cat_name, keywords in self.keywords.items():
            if any(keyword in paragraph for keyword in keywords):
                return cat_name
        
        return "其他"
    
    def _determine_level(self, paragraph: str) -> str:
        """确定知识粒度级别"""
        length = len(paragraph)
        
        if length < 100:
            return "micro"  # 微观知识点
        elif length < 300:
            return "meso"   # 中观知识点
        else:
            return "macro"  # 宏观知识点
    
    def _extract_keywords(self, paragraph: str) -> List[str]:
        """提取关键词"""
        keywords = []
        
        # 提取名词短语
        noun_phrases = re.findall(r'[\u4e00-\u9fff]{2,6}(?:的|之|和|与)[\u4e00-\u9fff]{2,6}', paragraph)
        keywords.extend(noun_phrases)
        
        # 提取技术术语
        tech_terms = re.findall(r'[A-Za-z]+[A-Za-z0-9]*(?:\.[A-Za-z]+[A-Za-z0-9]*)*', paragraph)
        keywords.extend(tech_terms)
        
        # 去重并限制数量
        keywords = list(set(keywords))[:10]
        return keywords
    
    def organize_pyramid_structure(self, points: List[KnowledgePoint]) -> Dict[str, Any]:
        """组织金字塔结构"""
        # 按分类分组
        categorized = {}
        for point in points:
            if point.category not in categorized:
                categorized[point.category] = []
            categorized[point.category].append(point)
        
        # 构建金字塔结构
        pyramid = {
            "core_theme": self._extract_core_theme(points),
            "main_arguments": categorized,
            "structure_type": "pyramid"
        }
        
        return pyramid
    
    def _extract_core_theme(self, points: List[KnowledgePoint]) -> str:
        """提取核心主题"""
        if not points:
            return "未定义主题"
        
        # 分析关键词频率
        keyword_freq = {}
        for point in points:
            for keyword in point.keywords:
                keyword_freq[keyword] = keyword_freq.get(keyword, 0) + 1
        
        # 返回最频繁的关键词作为主题
        if keyword_freq:
            return max(keyword_freq.items(), key=lambda x: x[1])[0]
        
        return points[0].category + "相关知识"


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='知识点提取器')
    parser.add_argument('--input', '-i', help='输入文件路径')
    parser.add_argument('--text', '-t', help='直接输入文本内容')
    parser.add_argument('--output', '-o', help='输出文件路径')
    parser.add_argument('--format', '-f', choices=['json', 'text'], default='json', 
                       help='输出格式')
    
    args = parser.parse_args()
    
    # 读取输入内容
    if args.input:
        with open(args.input, 'r', encoding='utf-8') as f:
            text = f.read()
    elif args.text:
        text = args.text
    else:
        print("请提供输入文件或文本内容")
        return
    
    # 提取知识点
    extractor = KnowledgeExtractor()
    points = extractor.extract_knowledge_points(text)
    
    # 组织金字塔结构
    pyramid = extractor.organize_pyramid_structure(points)
    
    # 输出结果
    if args.format == 'json':
        output_data = {
            "knowledge_points": [point.to_dict() for point in points],
            "pyramid_structure": pyramid
        }
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            print(f"结果已保存到: {args.output}")
        else:
            print(json.dumps(output_data, ensure_ascii=False, indent=2))
    else:
        # 文本格式输出
        output = f"提取到 {len(points)} 个知识点\n\n"
        
        for i, point in enumerate(points, 1):
            output += f"{i}. {point.title}\n"
            output += f"   分类: {point.category}\n"
            output += f"   粒度: {point.level}\n"
            output += f"   关键词: {', '.join(point.keywords)}\n"
            output += f"   内容: {point.content[:100]}...\n\n"
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output)
            print(f"结果已保存到: {args.output}")
        else:
            print(output)


if __name__ == "__main__":
    main()