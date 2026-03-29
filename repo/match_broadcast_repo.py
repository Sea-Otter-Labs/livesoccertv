"""
比赛转播整合结果 Repository
"""

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, desc
from repo.base_repo import BaseRepository
from models.match_broadcast import MatchBroadcast, BroadcastMatchStatus


class MatchBroadcastRepository(BaseRepository[MatchBroadcast]):
    """比赛转播整合结果数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, MatchBroadcast)
    
    async def get_by_fixture_id(self, fixture_id: int) -> Optional[MatchBroadcast]:
        """根据fixture_id获取比赛转播信息"""
        result = await self.session.execute(
            select(MatchBroadcast)
            .where(MatchBroadcast.fixture_id == fixture_id)
        )
        return result.scalar_one_or_none()

    async def query_matches(
        self,
        league_id: Optional[int] = None,
        season: Optional[int] = None,
        date_from: Optional[int] = None,
        date_to: Optional[int] = None,
        team_id: Optional[int] = None,
        status: Optional[str] = None,
        has_channels: Optional[bool] = None,
        broadcast_status: Optional[BroadcastMatchStatus] = None,
    ) -> List[MatchBroadcast]:
        """按组合条件查询比赛转播信息"""
        conditions = []

        if league_id is not None:
            conditions.append(MatchBroadcast.league_id == league_id)

        if season is not None:
            conditions.append(MatchBroadcast.season == season)

        if date_from is not None:
            conditions.append(MatchBroadcast.match_timestamp_utc >= date_from)

        if date_to is not None:
            conditions.append(MatchBroadcast.match_timestamp_utc <= date_to)

        if team_id is not None:
            conditions.append(
                or_(
                    MatchBroadcast.home_team_id == team_id,
                    MatchBroadcast.away_team_id == team_id,
                )
            )

        if status:
            conditions.append(MatchBroadcast.match_status == status)

        if has_channels is True:
            conditions.append(MatchBroadcast.channels.isnot(None))
        elif has_channels is False:
            conditions.append(MatchBroadcast.channels.is_(None))

        if broadcast_status is not None:
            conditions.append(MatchBroadcast.broadcast_match_status == broadcast_status)

        query = select(MatchBroadcast)
        if conditions:
            query = query.where(and_(*conditions))

        result = await self.session.execute(
            query.order_by(MatchBroadcast.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def get_by_league_and_season(
        self,
        league_id: int,
        season: int,
        status: Optional[BroadcastMatchStatus] = None
    ) -> List[MatchBroadcast]:
        """获取指定联赛和赛季的比赛转播信息"""
        return await self.query_matches(
            league_id=league_id,
            season=season,
            broadcast_status=status,
        )
    
    async def get_by_status(
        self,
        status: BroadcastMatchStatus,
        league_id: Optional[int] = None
    ) -> List[MatchBroadcast]:
        """根据对齐状态获取比赛"""
        return await self.query_matches(
            league_id=league_id,
            broadcast_status=status,
        )
    
    async def get_by_time_range(
        self,
        start_timestamp: int,
        end_timestamp: int,
        league_id: Optional[int] = None,
        has_channels: Optional[bool] = None
    ) -> List[MatchBroadcast]:
        """获取时间范围内的比赛"""
        return await self.query_matches(
            league_id=league_id,
            date_from=start_timestamp,
            date_to=end_timestamp,
            has_channels=has_channels,
        )
    
    async def get_mismatches(
        self,
        league_id: Optional[int] = None,
        season: Optional[int] = None
    ) -> List[MatchBroadcast]:
        """获取异常记录（未对齐/歧义/缺频道）"""
        query = select(MatchBroadcast).where(
            or_(
                MatchBroadcast.broadcast_match_status == BroadcastMatchStatus.UNMATCHED,
                MatchBroadcast.broadcast_match_status == BroadcastMatchStatus.MISSING_CHANNELS,
                MatchBroadcast.broadcast_match_status == BroadcastMatchStatus.AMBIGUOUS
            )
        )
        
        if league_id:
            query = query.where(MatchBroadcast.league_id == league_id)
        
        if season:
            query = query.where(MatchBroadcast.season == season)
        
        result = await self.session.execute(
            query.order_by(desc(MatchBroadcast.match_timestamp_utc))
        )
        return result.scalars().all()
    
    async def get_upcoming_matches_without_channels(
        self,
        current_timestamp: int,
        hours_threshold: int = 2
    ) -> List[MatchBroadcast]:
        """获取即将开赛但缺少频道的比赛"""
        from sqlalchemy import text
        
        # 计算阈值时间戳
        threshold_seconds = hours_threshold * 3600
        
        result = await self.session.execute(
            select(MatchBroadcast)
            .where(
                and_(
                    MatchBroadcast.match_timestamp_utc > current_timestamp,
                    MatchBroadcast.match_timestamp_utc <= current_timestamp + threshold_seconds,
                    or_(
                        MatchBroadcast.channels.is_(None),
                        MatchBroadcast.broadcast_match_status == BroadcastMatchStatus.MISSING_CHANNELS
                    )
                )
            )
            .order_by(MatchBroadcast.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def upsert(self, data: dict) -> MatchBroadcast:
        """插入或更新比赛转播信息"""
        existing = await self.get_by_fixture_id(data['fixture_id'])
        
        if existing:
            return await self.update(existing.id, data)
        else:
            return await self.create(data)
    
    async def update_channels(
        self,
        fixture_id: int,
        channels: list,
        web_crawl_raw_id: Optional[int] = None
    ) -> Optional[MatchBroadcast]:
        """更新比赛频道信息"""
        update_data = {
            'channels': channels,
            'broadcast_match_status': BroadcastMatchStatus.MATCHED
        }
        
        if web_crawl_raw_id:
            update_data['web_crawl_raw_id'] = web_crawl_raw_id
        
        existing = await self.get_by_fixture_id(fixture_id)
        if existing:
            return await self.update(existing.id, update_data)
        return None
