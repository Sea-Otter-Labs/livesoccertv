import re
from datetime import datetime
from typing import Optional


def parse_livesoccertv_date(
    date_text: str,
    time_text: str,
    timezone_hint: str = 'CET'
) -> Optional[int]:
    """
    解析 LiveSoccerTV 的日期文本为 UTC 时间戳
    
    Args:
        date_text: 日期文本（如 "25 Mar" 或 "Tuesday, 25 Mar 2025"）
        time_text: 时间文本（如 "20:00"）
        timezone_hint: 时区提示（默认CET）
    
    Returns:
        UTC时间戳（秒）或None
    """
    if not date_text:
        return None
    
    try:
        # 清理日期文本
        date_text = date_text.strip()
        
        # 提取日期部分
        # 可能的格式:
        # - "25 Mar"
        # - "Tuesday, 25 Mar 2025"
        # - "25 March 2025"
        
        current_year = datetime.now().year
        
        # 尝试提取日、月、年
        day = None
        month = None
        year = current_year
        
        # 匹配 "25 Mar" 或 "25 March"
        match = re.search(r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)', date_text, re.IGNORECASE)
        if match:
            day = int(match.group(1))
            month_str = match.group(2).lower()
            
            # 转换月份
            months = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12
            }
            month = months.get(month_str)
        
        # 匹配年份
        year_match = re.search(r'20\d{2}', date_text)
        if year_match:
            year = int(year_match.group())
        
        if not day or not month:
            return None
        
        # 构建 datetime
        dt = datetime(year, month, day)
        
        # 添加时间
        if time_text:
            time_match = re.search(r'(\d{1,2}):(\d{2})', time_text)
            if time_match:
                hour = int(time_match.group(1))
                minute = int(time_match.group(2))
                dt = dt.replace(hour=hour, minute=minute)
        
        # 转换为 UTC
        from datetime import timezone, timedelta
        
        timezone_offsets = {
            'CET': 1, 'CEST': 2,
            'GMT': 0, 'BST': 1,
            'EST': -5, 'EDT': -4,
            'PST': -8, 'PDT': -7,
        }
        
        offset_hours = timezone_offsets.get(timezone_hint.upper(), 0)
        local_tz = timezone(timedelta(hours=offset_hours))
        dt = dt.replace(tzinfo=local_tz)
        dt_utc = dt.astimezone(timezone.utc)
        
        return int(dt_utc.timestamp())
    
    except Exception as e:
        return None


def normalize_team_name(team_name: str) -> str:
    """
    标准化球队名称
    
    注意：此函数已委托给主工程的标准化实现，确保全系统使用同一套规则。
    导入路径处理：尝试从主工程导入，失败则使用本地兼容实现（仅用于独立测试）
    """
    if not team_name:
        return ''
    
    # 尝试从主工程导入标准化函数
    try:
        import sys
        import os
        # 添加项目根目录到路径（如果还没添加）
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        from utils.team_normalizer import normalize_team_name as main_normalizer
        return main_normalizer(team_name)
    except ImportError:
        # 如果无法导入主工程（例如在独立测试环境），使用兼容实现
        # 这个实现应该与主工程保持一致
        name = team_name.lower()
        
        # 移除西班牙语重音符号
        char_map = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'ñ': 'n', 'ü': 'u', 'ç': 'c',
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U',
            'Ñ': 'N', 'Ü': 'U', 'Ç': 'C'
        }
        
        for char, replacement in char_map.items():
            name = name.replace(char, replacement)
        
        # 移除标点符号
        import re
        name = re.sub(r'[^\w\s]', '', name)
        
        # 移除多余空格
        name = ' '.join(name.split())
        
        # 移除冗余词（与主工程保持一致）
        remove_words = [
            'fc', 'cf', 'sc', 'ac', 'rc', 'as', 'ss', 'us', 'uc',
            'real', 'club', 'deportivo', 'atletico', 'athletic',
            'de', 'la', 'los', 'las', 'el'
        ]
        words = name.split()
        filtered_words = [w for w in words if w not in remove_words]
        
        # 如果过滤后为空，回退到过滤前的结果
        if filtered_words:
            name = ' '.join(filtered_words)
        
        return name.strip()


def utc_now_timestamp() -> int:
    """获取当前 UTC 时间戳"""
    from datetime import timezone
    return int(datetime.now(timezone.utc).timestamp())
