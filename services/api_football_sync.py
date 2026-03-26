"""
API-Football 数据同步服务
将 API 数据同步到本地数据库
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from services.api_football_client import ApiFootballClient
from repo import ApiFixtureRepository, LeagueConfigRepository
from utils import normalize_team_name
from config.database import AsyncSessionLocal

logger = logging.getLogger(__name__)


class ApiFootballSyncService:
    """
    API-Football 数据同步服务
    负责将 API 数据转换并同步到本地数据库
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client: Optional[ApiFootballClient] = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.client = ApiFootballClient(self.api_key)
        await self.client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    def _transform_fixture(self, api_fixture: Dict[str, Any]) -> Dict[str, Any]:
        """
        将 API 数据转换为数据库模型格式
        
        Args:
            api_fixture: API 返回的 fixture 数据
        
        Returns:
            转换后的数据字典
        """
        fixture = api_fixture.get('fixture', {})
        league = api_fixture.get('league', {})
        teams = api_fixture.get('teams', {})
        home_team = teams.get('home', {})
        away_team = teams.get('away', {})
        goals = api_fixture.get('goals', {})
        
        # 解析比赛时间
        match_date_str = fixture.get('date', '')
        match_timestamp_utc = None
        match_date = None
        
        if match_date_str:
            try:
                # API 返回 ISO 格式时间
                from datetime import timezone
                dt = datetime.fromisoformat(match_date_str.replace('Z', '+00:00'))
                match_timestamp_utc = int(dt.timestamp())
                match_date = dt.date()
            except Exception as e:
                logger.warning(f"Failed to parse date: {match_date_str}, error: {e}")
        
        # 获取比分
        home_score = goals.get('home')
        away_score = goals.get('away')
        
        # 处理可能为 None 的比分
        if home_score is not None:
            try:
                home_score = int(home_score)
            except (ValueError, TypeError):
                home_score = None
        
        if away_score is not None:
            try:
                away_score = int(away_score)
            except (ValueError, TypeError):
                away_score = None
        
        return {
            'fixture_id': fixture.get('id'),
            'league_id': league.get('id'),
            'season': league.get('season'),
            'match_timestamp_utc': match_timestamp_utc,
            'match_date': match_date,
            'home_team_id': home_team.get('id'),
            'home_team_name': home_team.get('name', ''),
            'home_team_name_normalized': normalize_team_name(home_team.get('name', '')),
            'away_team_id': away_team.get('id'),
            'away_team_name': away_team.get('name', ''),
            'away_team_name_normalized': normalize_team_name(away_team.get('name', '')),
            'status': fixture.get('status', {}).get('short', ''),
            'round': league.get('round', ''),
            'home_score': home_score,
            'away_score': away_score,
            'venue': fixture.get('venue', {}).get('name', ''),
            'synced_at': datetime.utcnow(),
        }
    
    async def sync_league_fixtures(
        self,
        league_id: int,
        season: int,
        session: AsyncSession,
        days_back: int = 7,
        days_forward: int = 7
    ) -> Tuple[int, int]:
        """
        同步指定联赛的比赛数据
        
        Args:
            league_id: 联赛 ID
            season: 赛季
            session: 数据库会话
            days_back: 往回推天数
            days_forward: 往后推天数
        
        Returns:
            (新增数量, 更新数量)
        """
        logger.info(f"Syncing fixtures for league {league_id}, season {season}")
        
        # 获取 API 数据
        api_fixtures = await self.client.get_fixtures_by_date_range(
            league_id=league_id,
            season=season,
            days_back=days_back,
            days_forward=days_forward
        )
        
        if not api_fixtures:
            logger.warning(f"No fixtures retrieved for league {league_id}, season {season}")
            return 0, 0
        
        # 转换数据
        transformed_fixtures = [
            self._transform_fixture(f) for f in api_fixtures
        ]
        
        # 过滤无效数据
        valid_fixtures = [
            f for f in transformed_fixtures
            if f['fixture_id'] and f['match_timestamp_utc']
        ]
        
        logger.info(f"Transformed {len(valid_fixtures)} valid fixtures")
        
        # 批量插入或更新
        repo = ApiFixtureRepository(session)
        inserted, updated = await repo.upsert_many(valid_fixtures)
        
        await session.commit()
        
        logger.info(f"Sync completed: {inserted} inserted, {updated} updated")
        
        return inserted, updated
    
    async def sync_all_season_fixtures(
        self,
        league_id: int,
        season: int,
        session: AsyncSession
    ) -> Tuple[int, int]:
        """
        同步整个赛季的所有比赛
        
        Args:
            league_id: 联赛 ID
            season: 赛季
            session: 数据库会话
        
        Returns:
            (新增数量, 更新数量)
        """
        logger.info(f"Syncing all fixtures for league {league_id}, season {season}")
        
        # 获取 API 数据
        api_fixtures = await self.client.get_all_season_fixtures(
            league_id=league_id,
            season=season
        )
        
        if not api_fixtures:
            logger.warning(f"No fixtures retrieved")
            return 0, 0
        
        # 转换数据
        transformed_fixtures = [
            self._transform_fixture(f) for f in api_fixtures
        ]
        
        # 过滤无效数据
        valid_fixtures = [
            f for f in transformed_fixtures
            if f['fixture_id'] and f['match_timestamp_utc']
        ]
        
        logger.info(f"Transformed {len(valid_fixtures)} valid fixtures")
        
        # 批量插入或更新
        repo = ApiFixtureRepository(session)
        inserted, updated = await repo.upsert_many(valid_fixtures)
        
        await session.commit()
        
        logger.info(f"Full sync completed: {inserted} inserted, {updated} updated")
        
        return inserted, updated
    
    async def sync_all_enabled_leagues(
        self,
        session: AsyncSession,
        full_sync: bool = False,
        days_back: int = 7,
        days_forward: int = 7
    ) -> Dict[str, Any]:
        """
        同步所有启用的联赛
        
        Args:
            session: 数据库会话
            full_sync: 是否全量同步（整个赛季）
            days_back: 增量同步的往回推天数
            days_forward: 增量同步的往后推天数
        
        Returns:
            同步统计信息
        """
        # 获取启用的联赛配置
        league_repo = LeagueConfigRepository(session)
        enabled_leagues = await league_repo.get_enabled_configs()
        
        if not enabled_leagues:
            logger.warning("No enabled leagues found")
            return {'total_leagues': 0, 'results': []}
        
        logger.info(f"Syncing {len(enabled_leagues)} enabled leagues")
        
        results = []
        total_inserted = 0
        total_updated = 0
        
        for league_config in enabled_leagues:
            try:
                if full_sync:
                    inserted, updated = await self.sync_all_season_fixtures(
                        league_id=league_config.api_league_id,
                        season=league_config.api_season,
                        session=session
                    )
                else:
                    # 使用配置的时间窗口，如果没有则使用默认值
                    history_days = league_config.history_days or days_back
                    future_days = league_config.future_days or days_forward
                    
                    inserted, updated = await self.sync_league_fixtures(
                        league_id=league_config.api_league_id,
                        season=league_config.api_season,
                        session=session,
                        days_back=history_days,
                        days_forward=future_days
                    )
                
                results.append({
                    'league_id': league_config.api_league_id,
                    'league_name': league_config.league_name,
                    'season': league_config.api_season,
                    'inserted': inserted,
                    'updated': updated,
                    'status': 'success'
                })
                
                total_inserted += inserted
                total_updated += updated
                
            except Exception as e:
                logger.error(f"Failed to sync league {league_config.league_name}: {e}")
                results.append({
                    'league_id': league_config.api_league_id,
                    'league_name': league_config.league_name,
                    'season': league_config.api_season,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return {
            'total_leagues': len(enabled_leagues),
            'total_inserted': total_inserted,
            'total_updated': total_updated,
            'results': results
        }
    
    async def sync_fixture_by_id(
        self,
        fixture_id: int,
        session: AsyncSession
    ) -> Optional[Dict[str, Any]]:
        """
        同步指定比赛数据
        
        Args:
            fixture_id: 比赛 ID
            session: 数据库会话
        
        Returns:
            同步后的数据或 None
        """
        logger.info(f"Syncing fixture {fixture_id}")
        
        # 获取 API 数据
        api_fixtures = await self.client.get_fixtures(fixture_id=fixture_id)
        
        if not api_fixtures:
            logger.warning(f"Fixture {fixture_id} not found")
            return None
        
        # 转换并保存
        transformed = self._transform_fixture(api_fixtures[0])
        
        repo = ApiFixtureRepository(session)
        fixture = await repo.upsert(transformed)
        
        await session.commit()
        
        logger.info(f"Fixture {fixture_id} synced successfully")
        
        return fixture.to_dict() if fixture else None


# 便捷函数
async def sync_league(
    api_key: str,
    league_id: int,
    season: int,
    days_back: int = 7,
    days_forward: int = 7
) -> Tuple[int, int]:
    """
    便捷函数：同步单个联赛
    
    Args:
        api_key: API 密钥
        league_id: 联赛 ID
        season: 赛季
        days_back: 往回推天数
        days_forward: 往后推天数
    
    Returns:
        (新增数量, 更新数量)
    """
    async with AsyncSessionLocal() as session:
        async with ApiFootballSyncService(api_key) as service:
            return await service.sync_league_fixtures(
                league_id=league_id,
                season=season,
                session=session,
                days_back=days_back,
                days_forward=days_forward
            )


async def sync_all_leagues(
    api_key: str,
    full_sync: bool = False
) -> Dict[str, Any]:
    """
    便捷函数：同步所有启用的联赛
    
    Args:
        api_key: API 密钥
        full_sync: 是否全量同步
    
    Returns:
        同步统计信息
    """
    async with AsyncSessionLocal() as session:
        async with ApiFootballSyncService(api_key) as service:
            return await service.sync_all_enabled_leagues(
                session=session,
                full_sync=full_sync
            )
