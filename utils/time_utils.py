"""
时间处理工具
处理UTC时间戳转换和时区问题
"""

from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
import time


def utc_now_timestamp() -> int:
    """获取当前UTC时间戳（秒）"""
    return int(datetime.now(timezone.utc).timestamp())


def datetime_to_utc_timestamp(dt: datetime) -> int:
    """
    将datetime转换为UTC时间戳（秒）
    
    Args:
        dt: datetime对象（可以带时区信息或不带）
    
    Returns:
        UTC时间戳（秒）
    """
    if dt.tzinfo is None:
        # 如果没有时区信息，假设是本地时间，转换为UTC
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        # 转换到UTC
        dt = dt.astimezone(timezone.utc)
    
    return int(dt.timestamp())


def utc_timestamp_to_datetime(timestamp: int) -> datetime:
    """
    将UTC时间戳转换为datetime对象
    
    Args:
        timestamp: UTC时间戳（秒）
    
    Returns:
        带UTC时区的datetime对象
    """
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def parse_date_string(
    date_str: str,
    format_str: str = '%Y-%m-%d %H:%M:%S',
    from_tz: Optional[str] = None
) -> Optional[datetime]:
    """
    解析日期字符串为datetime对象
    
    Args:
        date_str: 日期字符串
        format_str: 日期格式
        from_tz: 来源时区（如果字符串不包含时区信息）
    
    Returns:
        datetime对象，带UTC时区
    """
    try:
        dt = datetime.strptime(date_str, format_str)
        
        if from_tz:
            # 转换到UTC
            from_zone = timezone(timedelta(hours=get_timezone_offset(from_tz)))
            dt = dt.replace(tzinfo=from_zone)
            dt = dt.astimezone(timezone.utc)
        else:
            # 假设已经是UTC
            dt = dt.replace(tzinfo=timezone.utc)
        
        return dt
    except ValueError:
        return None


def get_timezone_offset(tz_name: str) -> int:
    """
    获取时区偏移量（小时）
    
    支持常见时区缩写
    """
    timezone_offsets = {
        'UTC': 0,
        'GMT': 0,
        'CET': 1,   # 中欧时间
        'CEST': 2,  # 中欧夏令时
        'EET': 2,   # 东欧时间
        'EEST': 3,  # 东欧夏令时
        'EST': -5,  # 美国东部标准时间
        'EDT': -4,  # 美国东部夏令时
        'PST': -8,  # 美国太平洋标准时间
        'PDT': -7,  # 美国太平洋夏令时
        'JST': 9,   # 日本标准时间
        'CST': 8,   # 中国标准时间
        'IST': 5.5, # 印度标准时间
    }
    
    return int(timezone_offsets.get(tz_name.upper(), 0))


def get_date_range_timestamps(
    days_back: int = 7,
    days_forward: int = 7,
    base_timestamp: Optional[int] = None
) -> Tuple[int, int]:
    """
    获取日期范围的时间戳
    
    Args:
        days_back: 往回推的天数
        days_forward: 往后推的天数
        base_timestamp: 基准时间戳（默认当前时间）
    
    Returns:
        (start_timestamp, end_timestamp)
    """
    if base_timestamp is None:
        base_timestamp = utc_now_timestamp()
    
    start = base_timestamp - (days_back * 24 * 60 * 60)
    end = base_timestamp + (days_forward * 24 * 60 * 60)
    
    return start, end


def is_within_time_window(
    timestamp: int,
    start_timestamp: int,
    end_timestamp: int
) -> bool:
    """检查时间戳是否在指定窗口内"""
    return start_timestamp <= timestamp <= end_timestamp


def format_timestamp(
    timestamp: int,
    format_str: str = '%Y-%m-%d %H:%M:%S'
) -> str:
    """
    将时间戳格式化为字符串
    
    Args:
        timestamp: UTC时间戳（秒）
        format_str: 输出格式
    
    Returns:
        格式化后的时间字符串
    """
    dt = utc_timestamp_to_datetime(timestamp)
    return dt.strftime(format_str)


def parse_livesoccertv_date(
    date_text: str,
    time_text: Optional[str] = None,
    timezone_hint: str = 'CET'
) -> Optional[int]:
    """
    解析LiveSoccerTV的日期文本为UTC时间戳
    
    LiveSoccerTV通常显示本地时间，需要根据联赛所在时区转换
    
    Args:
        date_text: 日期文本（如 "25 Mar" 或 "2025-03-25"）
        time_text: 时间文本（如 "20:00"）
        timezone_hint: 时区提示（默认CET）
    
    Returns:
        UTC时间戳（秒）或None
    """
    try:
        # 尝试多种日期格式
        current_year = datetime.now().year
        
        # 格式1: "25 Mar" 或 "Mar 25"
        for fmt in ['%d %b %Y', '%b %d %Y']:
            try:
                dt = datetime.strptime(f"{date_text} {current_year}", fmt)
                break
            except ValueError:
                continue
        else:
            # 格式2: "2025-03-25"
            try:
                dt = datetime.strptime(date_text, '%Y-%m-%d')
            except ValueError:
                return None
        
        # 添加时间
        if time_text:
            try:
                hour, minute = map(int, time_text.split(':'))
                dt = dt.replace(hour=hour, minute=minute, second=0)
            except (ValueError, AttributeError):
                dt = dt.replace(hour=0, minute=0, second=0)
        else:
            dt = dt.replace(hour=0, minute=0, second=0)
        
        # 转换为UTC
        offset = get_timezone_offset(timezone_hint)
        from_zone = timezone(timedelta(hours=offset))
        dt = dt.replace(tzinfo=from_zone)
        dt_utc = dt.astimezone(timezone.utc)
        
        return int(dt_utc.timestamp())
    
    except Exception:
        return None


class TimeMatcher:
    """
    时间匹配器
    用于API和网页数据的时间对齐
    """
    
    def __init__(self, tolerance_hours: float = 4.0):
        """
        Args:
            tolerance_hours: 时间匹配容差（小时）
        """
        self.tolerance_seconds = tolerance_hours * 3600
    
    def is_match(
        self,
        timestamp1: int,
        timestamp2: int
    ) -> bool:
        """
        检查两个时间戳是否匹配（在容差范围内）
        """
        diff = abs(timestamp1 - timestamp2)
        return diff <= self.tolerance_seconds
    
    def find_best_match(
        self,
        target_timestamp: int,
        candidates: list,
        timestamp_key: str = 'match_timestamp_utc'
    ) -> Optional[dict]:
        """
        在候选列表中找到最佳时间匹配
        
        Args:
            target_timestamp: 目标时间戳
            candidates: 候选列表
            timestamp_key: 时间戳字段名
        
        Returns:
            最佳匹配项或None
        """
        best_match = None
        best_diff = float('inf')
        
        for candidate in candidates:
            candidate_ts = candidate.get(timestamp_key)
            if candidate_ts is None:
                continue
            
            diff = abs(target_timestamp - candidate_ts)
            if diff <= self.tolerance_seconds and diff < best_diff:
                best_diff = diff
                best_match = candidate
        
        return best_match
