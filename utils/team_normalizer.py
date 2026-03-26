"""
球队名称标准化工具
实现西班牙语 → 英语标准化
"""

import re
from typing import Optional, Dict, List
import unicodedata


# 西班牙语特殊字符映射到英语
SPANISH_TO_ENGLISH_CHARS = {
    'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
    'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
    'ñ': 'n', 'Ñ': 'N',
    'ü': 'u', 'Ü': 'U',
    'ç': 'c', 'Ç': 'C'
}

# 需要移除的常见冗余词
REMOVE_WORDS = [
    'FC', 'CF', 'SC', 'AC', 'RC', 'AS', 'SS', 'US', 'UC',
    'Real', 'Club', 'Deportivo', 'Atletico', 'Athletic',
    'De', 'La', 'Los', 'Las', 'El', 'CF'
]


def remove_accents(text: str) -> str:
    """
    移除重音符号
    例如: Atlético → Atletico
    """
    return ''.join(
        SPANISH_TO_ENGLISH_CHARS.get(char, char)
        for char in text
    )


def normalize_text(text: str) -> str:
    """
    标准化文本:
    1. 转小写
    2. 移除重音符号
    3. 移除多余空格
    4. 移除特殊字符
    """
    if not text:
        return ''
    
    # 转小写
    text = text.lower()
    
    # 移除重音符号
    text = remove_accents(text)
    
    # 移除标点符号和特殊字符，保留字母数字和空格
    text = re.sub(r'[^\w\s]', '', text)
    
    # 移除多余空格
    text = ' '.join(text.split())
    
    return text.strip()


def remove_redundant_words(text: str) -> str:
    """
    移除冗余词
    例如: "Real Madrid CF" → "Madrid"
    """
    words = text.split()
    filtered_words = [
        word for word in words 
        if word.lower() not in [w.lower() for w in REMOVE_WORDS]
    ]
    return ' '.join(filtered_words)


def normalize_team_name(
    team_name: str,
    remove_redundant: bool = True
) -> str:
    """
    标准化球队名称
    
    处理流程:
    1. 转小写
    2. 移除西班牙语重音符号
    3. 移除冗余词 (可选)
    4. 清理特殊字符和多余空格
    
    Args:
        team_name: 原始球队名称
        remove_redundant: 是否移除冗余词
    
    Returns:
        标准化后的球队名称
    """
    if not team_name:
        return ''
    
    # 基础标准化
    normalized = normalize_text(team_name)
    
    # 移除冗余词
    if remove_redundant:
        normalized = remove_redundant_words(normalized)
    
    return normalized


def normalize_team_names_pair(
    home_team: str,
    away_team: str,
    remove_redundant: bool = True
) -> tuple:
    """
    同时标准化主客队名称
    
    Returns:
        (home_normalized, away_normalized)
    """
    return (
        normalize_team_name(home_team, remove_redundant),
        normalize_team_name(away_team, remove_redundant)
    )


class TeamNameNormalizer:
    """
    球队名称标准化器
    支持自定义规则和映射
    """
    
    def __init__(self):
        self.custom_mappings: Dict[str, str] = {}
        self.remove_words: List[str] = REMOVE_WORDS.copy()
    
    def add_custom_mapping(self, alias: str, normalized: str):
        """添加自定义映射"""
        self.custom_mappings[normalize_text(alias)] = normalize_text(normalized)
    
    def add_remove_word(self, word: str):
        """添加要移除的词"""
        self.remove_words.append(word)
    
    def normalize(self, team_name: str) -> str:
        """
        标准化球队名称
        先检查自定义映射，再应用标准流程
        """
        if not team_name:
            return ''
        
        # 检查自定义映射
        normalized_key = normalize_text(team_name)
        if normalized_key in self.custom_mappings:
            return self.custom_mappings[normalized_key]
        
        # 应用标准流程
        result = normalize_text(team_name)
        
        # 移除自定义冗余词
        words = result.split()
        filtered_words = [
            word for word in words 
            if word not in [w.lower() for w in self.remove_words]
        ]
        
        return ' '.join(filtered_words)
    
    def normalize_pair(self, home: str, away: str) -> tuple:
        """同时标准化主客队"""
        return (self.normalize(home), self.normalize(away))


# 全局标准化器实例
default_normalizer = TeamNameNormalizer()


# 便捷函数
def normalize(team_name: str) -> str:
    """使用默认标准化器标准化球队名称"""
    return default_normalizer.normalize(team_name)


def are_teams_equal(team1: str, team2: str) -> bool:
    """
    比较两个球队名称是否相等（标准化后）
    """
    return normalize(team1) == normalize(team2)
