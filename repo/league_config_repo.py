"""
联赛配置 Repository
"""

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from repo.base_repo import BaseRepository
from models.league_config import LeagueConfig


class LeagueConfigRepository(BaseRepository[LeagueConfig]):
    """联赛配置数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, LeagueConfig)
    
    async def get_by_api_league_and_season(
        self, 
        api_league_id: int, 
        api_season: int
    ) -> Optional[LeagueConfig]:
        """根据API联赛ID和赛季获取配置"""
        result = await self.session.execute(
            select(LeagueConfig)
            .where(
                and_(
                    LeagueConfig.api_league_id == api_league_id,
                    LeagueConfig.api_season == api_season
                )
            )
        )
        return result.scalar_one_or_none()
    
    async def get_enabled_configs(self) -> List[LeagueConfig]:
        """获取所有启用的联赛配置"""
        result = await self.session.execute(
            select(LeagueConfig)
            .where(LeagueConfig.enabled == True)
            .order_by(LeagueConfig.league_name)
        )
        return result.scalars().all()
    
    async def get_by_country(self, country: str) -> List[LeagueConfig]:
        """根据国家获取联赛配置"""
        result = await self.session.execute(
            select(LeagueConfig)
            .where(LeagueConfig.country == country)
        )
        return result.scalars().all()
    
    async def exists_by_api_league(
        self, 
        api_league_id: int, 
        api_season: int
    ) -> bool:
        """检查联赛配置是否存在"""
        result = await self.session.execute(
            select(LeagueConfig)
            .where(
                and_(
                    LeagueConfig.api_league_id == api_league_id,
                    LeagueConfig.api_season == api_season
                )
            )
        )
        return result.scalar_one_or_none() is not None
