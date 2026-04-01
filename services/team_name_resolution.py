import logging
from typing import Optional, Dict, Any, List
from difflib import SequenceMatcher
from sqlalchemy.ext.asyncio import AsyncSession

from repo.team_name_mapping_repo import TeamNameMappingRepository
from services.api_football_client import ApiFootballClient
from utils.team_normalizer import normalize_team_name

logger = logging.getLogger(__name__)


class TeamNameResolutionService:
    """
    球队名称解析服务
    
    将网页原始队名映射到 API-Football 的 team_id
    优先查本地缓存，未命中则调用 API 获取该赛季所有球队进行本地匹配
    """
    
    def __init__(
        self,
        api_client: ApiFootballClient,
        mapping_repo: TeamNameMappingRepository
    ):
        self.api_client = api_client
        self.mapping_repo = mapping_repo
        # 单次任务内的内存缓存
        self._memory_cache: Dict[str, Optional[int]] = {}
        # 赛季球队列表缓存（避免重复获取同赛季所有球队）
        self._league_teams_cache: Dict[str, List[Dict]] = {}
    
    def _get_league_cache_key(self, league_id: int, season: int) -> str:
        """生成联赛赛季缓存 key"""
        return f"{league_id}:{season}"
    
    def _normalize_for_comparison(self, name: str) -> str:
        """标准化名称用于比较"""
        return normalize_team_name(name).lower().strip()
    
    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """计算两个名称的相似度"""
        return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()
    
    async def _get_league_teams(
        self,
        league_id: int,
        season: int
    ) -> List[Dict]:
        """
        获取指定联赛赛季的所有球队
        带缓存机制，避免重复请求
        """
        cache_key = self._get_league_cache_key(league_id, season)
        
        if cache_key in self._league_teams_cache:
            logger.debug(f"Using cached teams for league {league_id}, season {season}")
            return self._league_teams_cache[cache_key]
        
        try:
            logger.info(f"Fetching all teams for league {league_id}, season {season}")
            teams = await self.api_client.get_teams(
                league_id=league_id,
                season=season
            )
            
            if not teams:
                logger.warning(f"No teams found for league {league_id}, season {season}")
                return []
            
            # 缓存结果
            self._league_teams_cache[cache_key] = teams
            logger.info(f"Cached {len(teams)} teams for league {league_id}, season {season}")
            return teams
            
        except Exception as e:
            logger.error(f"Failed to fetch teams for league {league_id}, season {season}: {e}")
            return []
    
    def _find_best_match(
        self,
        raw_team_name: str,
        teams: List[Dict]
    ) -> Optional[Dict]:
        """
        在球队列表中找到最佳匹配
        
        匹配策略：
        1. 精确匹配（不区分大小写）
        2. 标准化名称匹配
        3. 包含关系匹配
        4. 相似度最高匹配
        """
        if not teams:
            return None
        
        normalized_input = self._normalize_for_comparison(raw_team_name)
        
        # 候选列表：(team_data, score)
        candidates = []
        
        for team_data in teams:
            team = team_data.get('team', {})
            team_name = team.get('name', '')
            
            if not team_name:
                continue
            
            normalized_team = self._normalize_for_comparison(team_name)
            
            # 1. 精确匹配（不区分大小写）- 最高分
            if raw_team_name.lower() == team_name.lower():
                candidates.append((team_data, 1.0))
                continue
            
            # 2. 标准化名称完全匹配
            if normalized_input == normalized_team:
                candidates.append((team_data, 0.95))
                continue
            
            # 3. 互相包含检查
            if normalized_input in normalized_team or normalized_team in normalized_input:
                # 计算包含匹配的分数（越长越精确分数越高）
                min_len = min(len(normalized_input), len(normalized_team))
                max_len = max(len(normalized_input), len(normalized_team))
                score = 0.8 + (0.1 * min_len / max_len)
                candidates.append((team_data, score))
                continue
            
            # 4. 相似度匹配
            similarity = self._calculate_similarity(normalized_input, normalized_team)
            if similarity >= 0.6:  # 阈值过滤
                candidates.append((team_data, similarity))
        
        if not candidates:
            return None
        
        # 按分数排序，取最高分
        candidates.sort(key=lambda x: x[1], reverse=True)
        best_match, best_score = candidates[0]
        
        logger.debug(
            f"Best match for '{raw_team_name}': "
            f"'{best_match['team']['name']}' (score={best_score:.2f})"
        )
        
        return best_match
    
    async def resolve_team_id(
        self,
        session: AsyncSession,
        league_id: int,
        season: int,
        raw_team_name: str
    ) -> Optional[int]:
        """
        解析单个球队名称
        
        Args:
            session: 数据库会话
            league_id: API-Football 联赛 ID
            season: 赛季年份
            raw_team_name: 网页原始队名
            
        Returns:
            API-Football team_id，解析失败返回 None
        """
        if not raw_team_name or not raw_team_name.strip():
            logger.warning("Empty raw_team_name provided")
            return None
        
        cache_key = f"{league_id}:{season}:{raw_team_name}"
        
        # 步骤 A：查内存缓存
        if cache_key in self._memory_cache:
            cached_id = self._memory_cache[cache_key]
            logger.debug(f"Memory cache hit for '{raw_team_name}': {cached_id}")
            return cached_id
        
        # 步骤 B：查本地数据库缓存
        existing_id = await self.mapping_repo.find_api_team_id(raw_team_name)
        if existing_id is not None:
            logger.debug(f"Database cache hit for '{raw_team_name}': {existing_id}")
            self._memory_cache[cache_key] = existing_id
            return existing_id
        
        # 步骤 C：获取该赛季所有球队
        try:
            teams = await self._get_league_teams(league_id, season)
            
            if not teams:
                logger.warning(
                    f"No teams available for league {league_id}, season {season}"
                )
                self._memory_cache[cache_key] = None
                return None
            
            # 步骤 D：本地名称匹配
            best_match = self._find_best_match(raw_team_name, teams)
            
            if not best_match:
                logger.warning(
                    f"No matching team found for '{raw_team_name}' "
                    f"in league {league_id}, season {season}"
                )
                self._memory_cache[cache_key] = None
                return None
            
            # 步骤 E：提取匹配结果
            team = best_match.get('team', {})
            api_team_id = team.get('id')
            normalized_name = team.get('name')
            
            if not api_team_id or not normalized_name:
                logger.error(
                    f"Invalid team data for '{raw_team_name}'"
                )
                self._memory_cache[cache_key] = None
                return None
            
            logger.info(
                f"Resolved '{raw_team_name}' -> '{normalized_name}' "
                f"(team_id={api_team_id})"
            )
            
            # 步骤 F：写回数据库缓存
            try:
                await self.mapping_repo.add_mapping(
                    normalized_name=normalized_name,
                    alias_name=raw_team_name,
                    api_team_id=api_team_id,
                    alias_type='api_lookup',
                    source='api_teams_lookup'
                )
                logger.debug(
                    f"Saved mapping: '{raw_team_name}' -> "
                    f"'{normalized_name}' (id={api_team_id})"
                )
            except Exception as e:
                # 写回失败不影响返回结果，只记录日志
                logger.error(f"Failed to save mapping for '{raw_team_name}': {e}")
            
            # 更新内存缓存
            self._memory_cache[cache_key] = api_team_id
            return api_team_id
            
        except Exception as e:
            logger.error(f"Error resolving team '{raw_team_name}': {e}")
            self._memory_cache[cache_key] = None
            return None
    
    async def resolve_team(
        self,
        session: AsyncSession,
        league_id: int,
        season: int,
        raw_team_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        解析球队并返回完整信息
        
        Args:
            session: 数据库会话
            league_id: API-Football 联赛 ID
            season: 赛季年份
            raw_team_name: 网页原始队名
            
        Returns:
            {
                'team_id': int,
                'normalized_name': str,
                'alias_name': str
            }
            解析失败返回 None
        """
        team_id = await self.resolve_team_id(
            session, league_id, season, raw_team_name
        )
        
        if team_id is None:
            return None
        
        # 从数据库获取标准化名称
        normalized_name = await self.mapping_repo.find_normalized_name(raw_team_name)
        
        return {
            'team_id': team_id,
            'normalized_name': normalized_name or raw_team_name,
            'alias_name': raw_team_name
        }
    
    async def resolve_teams_for_web_match(
        self,
        session: AsyncSession,
        league_id: int,
        season: int,
        home_team_raw: str,
        away_team_raw: str
    ) -> Dict[str, Optional[int]]:
        """
        为一场比赛解析主客队
        
        Args:
            session: 数据库会话
            league_id: API-Football 联赛 ID
            season: 赛季年份
            home_team_raw: 主队原始名称
            away_team_raw: 客队原始名称
            
        Returns:
            {
                'home_team_id': Optional[int],
                'away_team_id': Optional[int]
            }
        """
        home_team_id = await self.resolve_team_id(
            session, league_id, season, home_team_raw
        )
        away_team_id = await self.resolve_team_id(
            session, league_id, season, away_team_raw
        )
        
        return {
            'home_team_id': home_team_id,
            'away_team_id': away_team_id
        }
