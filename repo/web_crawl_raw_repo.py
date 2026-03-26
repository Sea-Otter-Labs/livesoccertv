"""
网页抓取原始数据 Repository
"""

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc
from repo.base_repo import BaseRepository
from models.web_crawl_raw import WebCrawlRaw


class WebCrawlRawRepository(BaseRepository[WebCrawlRaw]):
    """网页抓取原始数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, WebCrawlRaw)
    
    async def get_by_batch_id(self, batch_id: str) -> List[WebCrawlRaw]:
        """根据批次ID获取抓取记录"""
        result = await self.session.execute(
            select(WebCrawlRaw)
            .where(WebCrawlRaw.crawl_batch_id == batch_id)
            .order_by(WebCrawlRaw.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def get_by_league_config(
        self, 
        league_config_id: int,
        batch_id: Optional[str] = None
    ) -> List[WebCrawlRaw]:
        """根据联赛配置ID获取抓取记录"""
        query = select(WebCrawlRaw).where(
            WebCrawlRaw.league_config_id == league_config_id
        )
        
        if batch_id:
            query = query.where(WebCrawlRaw.crawl_batch_id == batch_id)
        
        result = await self.session.execute(
            query.order_by(desc(WebCrawlRaw.crawled_at))
        )
        return result.scalars().all()
    
    async def get_by_time_range(
        self,
        start_timestamp: int,
        end_timestamp: int,
        league_config_id: Optional[int] = None
    ) -> List[WebCrawlRaw]:
        """获取时间范围内的抓取记录"""
        query = select(WebCrawlRaw).where(
            and_(
                WebCrawlRaw.match_timestamp_utc >= start_timestamp,
                WebCrawlRaw.match_timestamp_utc <= end_timestamp
            )
        )
        
        if league_config_id:
            query = query.where(WebCrawlRaw.league_config_id == league_config_id)
        
        result = await self.session.execute(
            query.order_by(WebCrawlRaw.match_timestamp_utc)
        )
        return result.scalars().all()
    
    async def get_by_pagination_cursor(
        self,
        league_config_id: int,
        cursor: str
    ) -> List[WebCrawlRaw]:
        """根据分页游标获取记录（用于去重检查）"""
        result = await self.session.execute(
            select(WebCrawlRaw)
            .where(
                and_(
                    WebCrawlRaw.league_config_id == league_config_id,
                    WebCrawlRaw.pagination_cursor == cursor
                )
            )
        )
        return result.scalars().all()
    
    async def exists_by_cursor(
        self,
        league_config_id: int,
        cursor: str
    ) -> bool:
        """检查分页游标是否已存在"""
        result = await self.session.execute(
            select(WebCrawlRaw)
            .where(
                and_(
                    WebCrawlRaw.league_config_id == league_config_id,
                    WebCrawlRaw.pagination_cursor == cursor
                )
            )
        )
        return result.scalar_one_or_none() is not None
    
    async def find_potential_matches(
        self,
        league_config_id: int,
        home_team_normalized: str,
        away_team_normalized: str,
        time_tolerance_seconds: int = 14400  # 默认4小时容差
    ) -> List[WebCrawlRaw]:
        """
        查找潜在匹配的比赛
        根据标准化后的球队名称和时间窗口
        """
        result = await self.session.execute(
            select(WebCrawlRaw)
            .where(
                and_(
                    WebCrawlRaw.league_config_id == league_config_id,
                    WebCrawlRaw.home_team_name_normalized == home_team_normalized,
                    WebCrawlRaw.away_team_name_normalized == away_team_normalized
                )
            )
        )
        return result.scalars().all()
