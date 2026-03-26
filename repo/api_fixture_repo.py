"""
API 比赛数据 Repository
"""

from typing import List, Optional, Tuple
from datetime import date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func
from repo.base_repo import BaseRepository
from models.api_fixture import ApiFixture


class ApiFixtureRepository(BaseRepository[ApiFixture]):
    """API比赛数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, ApiFixture)
    
    async def get_by_fixture_id(self, fixture_id: int) -> Optional[ApiFixture]:
        """根据fixture_id获取比赛"""
        result = await self.session.execute(
            select(ApiFixture)
            .where(ApiFixture.fixture_id == fixture_id)
        )
        return result.scalar_one_or_none()
    
    async def get_by_league_and_season(
        self, 
        league_id: int, 
        season: int,
        skip: int = 0,
        limit: int = 100
    ) -> List[ApiFixture]:
        """获取指定联赛和赛季的比赛"""
        result = await self.session.execute(
            select(ApiFixture)
            .where(
                and_(
                    ApiFixture.league_id == league_id,
                    ApiFixture.season == season
                )
            )
            .order_by(ApiFixture.match_timestamp_utc)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()
    
    async def get_by_date_range(
        self,
        start_timestamp: int,
        end_timestamp: int,
        league_id: Optional[int] = None
    ) -> List[ApiFixture]:
        """获取时间范围内的比赛"""
        query = select(ApiFixture).where(
            and_(
                ApiFixture.match_timestamp_utc >= start_timestamp,
                ApiFixture.match_timestamp_utc <= end_timestamp
            )
        )
        
        if league_id:
            query = query.where(ApiFixture.league_id == league_id)
        
        result = await self.session.execute(
            query.order_by(ApiFixture.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def get_by_date(
        self, 
        match_date: date,
        league_id: Optional[int] = None
    ) -> List[ApiFixture]:
        """获取指定日期的比赛"""
        query = select(ApiFixture).where(ApiFixture.match_date == match_date)
        
        if league_id:
            query = query.where(ApiFixture.league_id == league_id)
        
        result = await self.session.execute(
            query.order_by(ApiFixture.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def get_by_team(
        self,
        team_id: int,
        season: Optional[int] = None
    ) -> List[ApiFixture]:
        """获取指定球队的比赛"""
        query = select(ApiFixture).where(
            or_(
                ApiFixture.home_team_id == team_id,
                ApiFixture.away_team_id == team_id
            )
        )
        
        if season:
            query = query.where(ApiFixture.season == season)
        
        result = await self.session.execute(
            query.order_by(ApiFixture.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def upsert(self, data: dict) -> ApiFixture:
        """插入或更新比赛数据"""
        existing = await self.get_by_fixture_id(data['fixture_id'])
        
        if existing:
            # 更新现有记录
            return await self.update(existing.id, data)
        else:
            # 创建新记录
            return await self.create(data)
    
    async def upsert_many(self, data_list: List[dict]) -> Tuple[int, int]:
        """批量插入或更新比赛数据"""
        inserted = 0
        updated = 0
        
        for data in data_list:
            existing = await self.get_by_fixture_id(data['fixture_id'])
            if existing:
                await self.update(existing.id, data)
                updated += 1
            else:
                await self.create(data)
                inserted += 1
        
        return inserted, updated
    
    async def get_matches_needing_broadcast(
        self,
        league_id: int,
        season: int,
        start_timestamp: int,
        end_timestamp: int
    ) -> List[ApiFixture]:
        """获取需要补充转播信息的比赛"""
        from models.match_broadcast import MatchBroadcast
        
        result = await self.session.execute(
            select(ApiFixture)
            .outerjoin(
                MatchBroadcast,
                ApiFixture.fixture_id == MatchBroadcast.fixture_id
            )
            .where(
                and_(
                    ApiFixture.league_id == league_id,
                    ApiFixture.season == season,
                    ApiFixture.match_timestamp_utc >= start_timestamp,
                    ApiFixture.match_timestamp_utc <= end_timestamp,
                    or_(
                        MatchBroadcast.id == None,
                        MatchBroadcast.broadcast_match_status != 'matched'
                    )
                )
            )
            .order_by(ApiFixture.match_timestamp_utc)
        )
        return result.scalars().all()
