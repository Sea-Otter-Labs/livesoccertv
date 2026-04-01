from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, desc, ColumnElement, func
from sqlalchemy.dialects.mysql import insert
from repo.base_repo import BaseRepository
from models.match_broadcast import MatchBroadcast


def build_time_range_filters(
    start_timestamp: Optional[int] = None,
    end_timestamp: Optional[int] = None
) -> list[ColumnElement[bool]]:
    """
    构建时间范围过滤条件（开区间逻辑）

    - 仅传入 start_timestamp：查询 >= start_timestamp 的记录（右开区间）
    - 仅传入 end_timestamp：查询 <= end_timestamp 的记录（左开区间）
    - 两者都传：查询两者之间的记录（闭区间）
    - 都不传：返回空列表（不过滤）
    """
    filters = []

    if start_timestamp is not None:
        filters.append(MatchBroadcast.match_timestamp_utc >= start_timestamp)

    if end_timestamp is not None:
        filters.append(MatchBroadcast.match_timestamp_utc <= end_timestamp)

    return filters


class MatchBroadcastRepository(BaseRepository[MatchBroadcast]):
    """比赛转播整合结果数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, MatchBroadcast)
    
    async def get_by_fixture_id(self, fixture_id: int) -> Optional[MatchBroadcast]:
        """根据fixture_id获取比赛转播信息"""
        result = await self.session.execute(
            select(MatchBroadcast)
            .where(MatchBroadcast.fixture_id == fixture_id,
                   MatchBroadcast.broadcast_match_status == "matched")
        )
        return result.scalar_one_or_none()
    

    async def get_by_league_and_season(
        self,
        league_id: int,
        season: int,
        status: Optional[str] = None
    ) -> List[MatchBroadcast]:
        """获取指定联赛和赛季的比赛转播信息"""
        query = select(MatchBroadcast).where(
            and_(
                MatchBroadcast.league_id == league_id,
                MatchBroadcast.season == season
            )
        )
        
        if status:
            query = query.where(MatchBroadcast.broadcast_match_status == status)
        
        result = await self.session.execute(
            query.order_by(MatchBroadcast.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def get_by_status(
        self,
        status: str,
        league_id: Optional[int] = None
    ) -> List[MatchBroadcast]:
        """根据对齐状态获取比赛"""
        query = select(MatchBroadcast).where(
            MatchBroadcast.broadcast_match_status == status
        )
        
        if league_id:
            query = query.where(MatchBroadcast.league_id == league_id)
        
        result = await self.session.execute(
            query.order_by(MatchBroadcast.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def get_by_time_range(
        self,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None,
        league_id: Optional[int] = None,
        season: Optional[int] = None,
        has_channels: Optional[bool] = None,
        broadcast_status: Optional[str] = 'matched',
        channel_country: Optional[str] = 'Spain'
    ) -> List[MatchBroadcast]:
        """
        获取时间范围内的比赛

        时间范围使用开区间逻辑：
        - 仅传入 start_timestamp：查询 >= start_timestamp 的记录（右开区间）
        - 仅传入 end_timestamp：查询 <= end_timestamp 的记录（左开区间）
        - 两者都传：查询两者之间的记录（闭区间）
        - 都不传：查询所有记录（忽略时间过滤）
        """
        query = select(MatchBroadcast)

        # 应用时间范围过滤（开区间逻辑）
        time_filters = build_time_range_filters(start_timestamp, end_timestamp)
        for time_filter in time_filters:
            query = query.where(time_filter)

        if league_id:
            query = query.where(MatchBroadcast.league_id == league_id)

        if season:
            query = query.where(MatchBroadcast.season == season)

        if broadcast_status:
            query = query.where(MatchBroadcast.broadcast_match_status == broadcast_status)

        if has_channels is not None:
            if has_channels:
                query = query.where(MatchBroadcast.channels.isnot(None))
            else:
                query = query.where(MatchBroadcast.channels.is_(None))
        result = await self.session.execute(
            query.order_by(MatchBroadcast.match_timestamp_utc)
        )
        return result.scalars().all()
    
    
    async def get_mismatches(
        self,
        league_id: Optional[int] = None,
        season: Optional[int] = None
    ) -> List[MatchBroadcast]:
        """获取异常记录（未对齐/歧义/缺频道）"""
        query = select(MatchBroadcast).where(
            or_(
                MatchBroadcast.broadcast_match_status == 'unmatched',
                MatchBroadcast.broadcast_match_status == 'missing_channels',
                MatchBroadcast.broadcast_match_status == 'ambiguous'
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
                        MatchBroadcast.broadcast_match_status == 'missing_channels'
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
    
    async def upsert_by_fixture_id(self, data: dict) -> MatchBroadcast:
        """
        使用 MySQL INSERT ... ON DUPLICATE KEY UPDATE 原子性插入或更新
        解决并发场景下的重复键冲突问题
        """
        # 准备数据，确保包含所有必需字段
        insert_data = {
            'fixture_id': data['fixture_id'],
            'league_id': data.get('league_id', 0),
            'season': data.get('season', 0),
            'match_timestamp_utc': data.get('match_timestamp_utc', 0),
            'match_date': data.get('match_date'),
            'home_team_id': data.get('home_team_id', 0),
            'home_team_name': data.get('home_team_name', ''),
            'away_team_id': data.get('away_team_id', 0),
            'away_team_name': data.get('away_team_name', ''),
            'match_status': data.get('match_status'),
            'home_score': data.get('home_score'),
            'away_score': data.get('away_score'),
            'broadcast_match_status': data.get('broadcast_match_status', 'unmatched'),
            'matched_confidence': data.get('matched_confidence', 0.0),
            'web_crawl_raw_id': data.get('web_crawl_raw_id'),
            'channels': data.get('channels'),
            'last_verified_at': data.get('last_verified_at'),
        }
        
        # 构建 INSERT ... ON DUPLICATE KEY UPDATE 语句
        stmt = insert(MatchBroadcast).values(**insert_data)
        
        # 定义更新时覆盖的字段（除了主键和fixture_id）
        update_fields = {
            'league_id': stmt.inserted.league_id,
            'season': stmt.inserted.season,
            'match_timestamp_utc': stmt.inserted.match_timestamp_utc,
            'match_date': stmt.inserted.match_date,
            'home_team_id': stmt.inserted.home_team_id,
            'home_team_name': stmt.inserted.home_team_name,
            'away_team_id': stmt.inserted.away_team_id,
            'away_team_name': stmt.inserted.away_team_name,
            'match_status': stmt.inserted.match_status,
            'home_score': stmt.inserted.home_score,
            'away_score': stmt.inserted.away_score,
            'broadcast_match_status': stmt.inserted.broadcast_match_status,
            'matched_confidence': stmt.inserted.matched_confidence,
            'web_crawl_raw_id': stmt.inserted.web_crawl_raw_id,
            'channels': stmt.inserted.channels,
            'last_verified_at': stmt.inserted.last_verified_at,
            'updated_at': func.now(),  # 更新时间戳
        }
        
        # 使用 MySQL 的 ON DUPLICATE KEY UPDATE
        upsert_stmt = stmt.on_duplicate_key_update(**update_fields)
        
        # 执行语句
        await self.session.execute(upsert_stmt)
        await self.session.flush()
        
        # 返回更新/插入后的记录
        result = await self.session.execute(
            select(MatchBroadcast)
            .where(MatchBroadcast.fixture_id == data['fixture_id'])
        )
        return result.scalar_one()
    
    async def update_channels(
        self,
        fixture_id: int,
        channels: list,
        web_crawl_raw_id: Optional[int] = None
    ) -> Optional[MatchBroadcast]:
        """更新比赛频道信息"""
        update_data = {
            'channels': channels,
            'broadcast_match_status': 'matched'
        }
        
        if web_crawl_raw_id:
            update_data['web_crawl_raw_id'] = web_crawl_raw_id
        
        existing = await self.get_by_fixture_id(fixture_id)
        if existing:
            return await self.update(existing.id, update_data)
        return None
