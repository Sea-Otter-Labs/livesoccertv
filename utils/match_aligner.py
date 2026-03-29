from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from utils.team_normalizer import normalize_team_name
from utils.time_utils import TimeMatcher


class MatchResult(Enum):
    """对齐结果枚举"""
    MATCHED = 'matched'
    UNMATCHED = 'unmatched'
    AMBIGUOUS = 'ambiguous'
    MISSING_CHANNELS = 'missing_channels'


@dataclass
class MatchCandidate:
    """匹配候选"""
    web_crawl_id: int
    home_team_normalized: str
    away_team_normalized: str
    match_timestamp_utc: int
    confidence: float = 0.0


@dataclass
class MatchAlignment:
    """对齐结果"""
    fixture_id: int
    web_crawl_raw_id: Optional[int]
    result: MatchResult
    confidence: float
    channels: Optional[List[Dict]]
    reason: Optional[str] = None


class MatchAligner:
    """
    比赛对齐器
    将API-Football比赛数据与LiveSoccerTV网页数据进行对齐
    """
    
    def __init__(
        self,
        time_tolerance_hours: float = 4.0,
        min_confidence: float = 0.8
    ):
        """
        Args:
            time_tolerance_hours: 时间匹配容差（小时）
            min_confidence: 最小匹配置信度
        """
        self.time_matcher = TimeMatcher(time_tolerance_hours)
        self.min_confidence = min_confidence
    
    def align_single(
        self,
        api_fixture: Dict[str, Any],
        web_candidates: List[Dict[str, Any]]
    ) -> MatchAlignment:
        """
        对齐单个API比赛与多个网页候选
        
        Args:
            api_fixture: API比赛数据
            web_candidates: 网页抓取候选列表
        
        Returns:
            对齐结果
        """
        # 标准化API数据
        api_home = normalize_team_name(api_fixture.get('home_team_name', ''))
        api_away = normalize_team_name(api_fixture.get('away_team_name', ''))
        api_time = api_fixture.get('match_timestamp_utc')
        fixture_id = api_fixture.get('fixture_id')
        
        if not all([api_home, api_away, api_time]):
            return MatchAlignment(
                fixture_id=fixture_id,
                web_crawl_raw_id=None,
                result=MatchResult.UNMATCHED,
                confidence=0.0,
                channels=None,
                reason='API数据不完整'
            )
        
        # 过滤时间匹配的候选
        time_matches = []
        for candidate in web_candidates:
            web_time = candidate.get('match_timestamp_utc')
            if web_time and self.time_matcher.is_match(api_time, web_time):
                time_matches.append(candidate)
        
        if not time_matches:
            return MatchAlignment(
                fixture_id=fixture_id,
                web_crawl_raw_id=None,
                result=MatchResult.UNMATCHED,
                confidence=0.0,
                channels=None,
                reason='无时间匹配候选'
            )
        
        # 在时间内匹配的候选中查找球队匹配
        team_matches = []
        for candidate in time_matches:
            web_home = normalize_team_name(candidate.get('home_team_name_raw', ''))
            web_away = normalize_team_name(candidate.get('away_team_name_raw', ''))
            
            # 检查主客队是否匹配（考虑主客场可能互换的情况）
            match_type = self._check_team_match(
                api_home, api_away,
                web_home, web_away
            )
            
            if match_type:
                confidence = self._calculate_confidence(
                    api_home, api_away,
                    web_home, web_away,
                    match_type
                )
                
                team_matches.append({
                    'candidate': candidate,
                    'confidence': confidence,
                    'match_type': match_type
                })
        
        if not team_matches:
            return MatchAlignment(
                fixture_id=fixture_id,
                web_crawl_raw_id=None,
                result=MatchResult.UNMATCHED,
                confidence=0.0,
                channels=None,
                reason='无球队匹配'
            )
        
        # 按置信度排序
        team_matches.sort(key=lambda x: x['confidence'], reverse=True)
        
        # 检查是否有歧义（多个高置信度匹配）
        if len(team_matches) > 1:
            top_confidence = team_matches[0]['confidence']
            second_confidence = team_matches[1]['confidence']
            
            # 如果前两个匹配都很接近，认为是歧义
            if second_confidence >= top_confidence * 0.9 and second_confidence >= self.min_confidence:
                return MatchAlignment(
                    fixture_id=fixture_id,
                    web_crawl_raw_id=None,
                    result=MatchResult.AMBIGUOUS,
                    confidence=top_confidence,
                    channels=None,
                    reason=f'存在多个高置信度匹配: {top_confidence:.2f} vs {second_confidence:.2f}'
                )
        
        # 返回最佳匹配
        best_match = team_matches[0]
        
        if best_match['confidence'] < self.min_confidence:
            return MatchAlignment(
                fixture_id=fixture_id,
                web_crawl_raw_id=None,
                result=MatchResult.UNMATCHED,
                confidence=best_match['confidence'],
                channels=None,
                reason=f'置信度低于阈值: {best_match["confidence"]:.2f}'
            )
        
        # 检查是否有频道信息
        channels = best_match['candidate'].get('channel_list')
        has_channels = channels and len(channels) > 0
        
        result = MatchResult.MATCHED if has_channels else MatchResult.MISSING_CHANNELS
        
        return MatchAlignment(
            fixture_id=fixture_id,
            web_crawl_raw_id=best_match['candidate'].get('id'),
            result=result,
            confidence=best_match['confidence'],
            channels=channels if has_channels else None,
            reason=None if has_channels else '缺少频道信息'
        )
    
    def _check_team_match(
        self,
        api_home: str,
        api_away: str,
        web_home: str,
        web_away: str
    ) -> Optional[str]:
        """
        检查球队匹配类型
        
        Returns:
            'normal' - 正常匹配（主对主，客对客）
            'swapped' - 互换匹配（主对客，客对主，可能是数据错误）
            None - 不匹配
        """
        # 正常匹配
        if api_home == web_home and api_away == web_away:
            return 'normal'
        
        # 互换匹配（可能是网页数据错误）
        if api_home == web_away and api_away == web_home:
            return 'swapped'
        
        return None
    
    def _calculate_confidence(
        self,
        api_home: str,
        api_away: str,
        web_home: str,
        web_away: str,
        match_type: str
    ) -> float:
        """
        计算匹配置信度
        
        Returns:
            0.0 - 1.0 之间的置信度
        """
        base_confidence = 1.0
        
        # 互换匹配降低置信度
        if match_type == 'swapped':
            base_confidence *= 0.7
        
        # 检查完全匹配
        home_exact = api_home == web_home
        away_exact = api_away == web_away
        
        if home_exact and away_exact:
            return base_confidence
        
        # 部分匹配（使用相似度计算）
        from difflib import SequenceMatcher
        
        home_sim = SequenceMatcher(None, api_home, web_home).ratio()
        away_sim = SequenceMatcher(None, api_away, web_away).ratio()
        
        avg_sim = (home_sim + away_sim) / 2
        
        return base_confidence * avg_sim
    
    def align_batch(
        self,
        api_fixtures: List[Dict[str, Any]],
        web_crawls: List[Dict[str, Any]]
    ) -> Tuple[List[MatchAlignment], List[Dict[str, Any]]]:
        """
        批量对齐
        
        Args:
            api_fixtures: API比赛列表
            web_crawls: 网页抓取列表
        
        Returns:
            (对齐结果列表, 未使用的网页记录列表)
        """
        alignments = []
        used_web_ids = set()
        
        for fixture in api_fixtures:
            alignment = self.align_single(fixture, web_crawls)
            alignments.append(alignment)
            
            if alignment.web_crawl_raw_id:
                used_web_ids.add(alignment.web_crawl_raw_id)
        
        # 找出未使用的网页记录
        unused_web = [
            web for web in web_crawls
            if web.get('id') not in used_web_ids
        ]
        
        return alignments, unused_web


def align_matches(
    api_fixtures: List[Dict[str, Any]],
    web_crawls: List[Dict[str, Any]],
    time_tolerance_hours: float = 4.0
) -> List[MatchAlignment]:
    """
    对齐比赛的便捷函数
    
    Args:
        api_fixtures: API比赛列表
        web_crawls: 网页抓取列表
        time_tolerance_hours: 时间容差（小时）
    
    Returns:
        对齐结果列表
    """
    aligner = MatchAligner(time_tolerance_hours)
    alignments, _ = aligner.align_batch(api_fixtures, web_crawls)
    return alignments
