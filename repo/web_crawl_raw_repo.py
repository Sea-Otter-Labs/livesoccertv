from datetime import date
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc
from sqlalchemy.dialects.mysql import insert as mysql_insert
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
    
    async def upsert_match(self, data: Dict[str, Any]) -> WebCrawlRaw:
        """
        Upsert 比赛数据
        基于唯一键 (league_config_id, home_team_name_normalized, away_team_name_normalized, match_date)
        存在则更新，不存在则插入
        
        Args:
            data: 比赛数据字典
            
        Returns:
            插入或更新后的记录
        """
        stmt = mysql_insert(WebCrawlRaw).values(**data)
        
        stmt = stmt.on_duplicate_key_update(
            crawl_batch_id=stmt.inserted.crawl_batch_id,
            match_timestamp_utc=stmt.inserted.match_timestamp_utc,
            channel_list=stmt.inserted.channel_list,
            pagination_cursor=stmt.inserted.pagination_cursor,
            source_match_text=stmt.inserted.source_match_text,
            page_url=stmt.inserted.page_url,
            crawled_at=stmt.inserted.crawled_at,
            match_date_text=stmt.inserted.match_date_text,
            home_team_name_raw=stmt.inserted.home_team_name_raw,
            away_team_name_raw=stmt.inserted.away_team_name_raw,
        )
        
        await self.session.execute(stmt)
        await self.session.flush()
        
        return await self._get_by_unique_key(
            data['league_config_id'],
            data['home_team_name_normalized'],
            data['away_team_name_normalized'],
            data['match_timestamp_utc']
        )
    
    async def _get_by_unique_key(
        self,
        league_config_id: int,
        home_team_normalized: str,
        away_team_normalized: str,
        match_timestamp_utc: int
    ) -> Optional[WebCrawlRaw]:
        """根据唯一键获取记录，处理重复数据"""
        result = await self.session.execute(
            select(WebCrawlRaw).where(
                and_(
                    WebCrawlRaw.league_config_id == league_config_id,
                    WebCrawlRaw.home_team_name_normalized == home_team_normalized,
                    WebCrawlRaw.away_team_name_normalized == away_team_normalized,
                    WebCrawlRaw.match_timestamp_utc == match_timestamp_utc
                )
            ).limit(1)  # 使用 limit(1) 处理可能的重复数据
        )
        records = result.scalars().all()
        
        if records:
            if len(records) > 1:
                # 记录重复数据警告
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(
                    f"[DATA_QUALITY] 发现 {len(records)} 条重复记录: "
                    f"league_id={league_config_id}, "
                    f"match={home_team_normalized} vs {away_team_normalized}, "
                    f"timestamp={match_timestamp_utc}, "
                    f"ids={[r.id for r in records[:5]]}"
                )
            return records[0]
        
        return None
    
    async def get_unique_team_names_raw(
        self,
        league_config_id: Optional[int] = None,
        limit: int = 100
    ) -> Dict[str, List[str]]:
        """
        获取所有唯一的原始球队名称
        
        Args:
            league_config_id: 联赛配置ID，为None则获取所有联赛
            limit: 最多返回多少条记录用于提取队名
            
        Returns:
            Dict with 'home_teams' and 'away_teams' keys containing unique raw team names
        """
        from sqlalchemy import distinct
        
        query = select(WebCrawlRaw)
        if league_config_id:
            query = query.where(WebCrawlRaw.league_config_id == league_config_id)
        query = query.limit(limit)
        
        result = await self.session.execute(query)
        records = result.scalars().all()
        
        home_teams = list(set([r.home_team_name_raw for r in records if r.home_team_name_raw]))
        away_teams = list(set([r.away_team_name_raw for r in records if r.away_team_name_raw]))
        
        return {
            'home_teams': home_teams,
            'away_teams': away_teams,
            'all_teams': list(set(home_teams + away_teams))
        }
    
    async def get_team_name_pairs(
        self,
        league_config_id: Optional[int] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        获取成对的球队名称（主客队）
        
        Args:
            league_config_id: 联赛配置ID
            limit: 返回记录数限制
            
        Returns:
            包含比赛信息的字典列表
        """
        query = select(WebCrawlRaw)
        if league_config_id:
            query = query.where(WebCrawlRaw.league_config_id == league_config_id)
        query = query.order_by(desc(WebCrawlRaw.crawled_at)).limit(limit)
        
        result = await self.session.execute(query)
        records = result.scalars().all()
        
        return [
            {
                'id': r.id,
                'league_config_id': r.league_config_id,
                'home_team_raw': r.home_team_name_raw,
                'away_team_raw': r.away_team_name_raw,
                'home_team_normalized': r.home_team_name_normalized,
                'away_team_normalized': r.away_team_name_normalized,
                'match_date_text': r.match_date_text,
            }
            for r in records
        ]
