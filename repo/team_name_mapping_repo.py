from typing import List, Optional, Set
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_
from repo.base_repo import BaseRepository
from models.team_name_mapping import TeamNameMapping


class TeamNameMappingRepository(BaseRepository[TeamNameMapping]):
    """球队名称映射数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, TeamNameMapping)
    
    async def get_by_alias(self, alias_name: str) -> List[TeamNameMapping]:
        """根据别名获取映射"""
        result = await self.session.execute(
            select(TeamNameMapping)
            .where(
                and_(
                    TeamNameMapping.alias_name == alias_name,
                    TeamNameMapping.is_active == True
                )
            )
        )
        return result.scalars().all()
    
    async def get_by_normalized_name(
        self, 
        normalized_name: str
    ) -> List[TeamNameMapping]:
        """根据标准化名称获取所有别名"""
        result = await self.session.execute(
            select(TeamNameMapping)
            .where(
                and_(
                    TeamNameMapping.normalized_name == normalized_name,
                    TeamNameMapping.is_active == True
                )
            )
        )
        return result.scalars().all()
    
    async def get_by_api_team_id(
        self, 
        api_team_id: int
    ) -> List[TeamNameMapping]:
        """根据API球队ID获取所有映射"""
        result = await self.session.execute(
            select(TeamNameMapping)
            .where(
                and_(
                    TeamNameMapping.api_team_id == api_team_id,
                    TeamNameMapping.is_active == True
                )
            )
        )
        return result.scalars().all()
    
    async def find_normalized_name(
        self,
        alias_name: str
    ) -> Optional[str]:
        """
        查找标准化名称
        返回第一个匹配的标准化名称，如果没有找到则返回None
        """
        result = await self.session.execute(
            select(TeamNameMapping.normalized_name)
            .where(
                and_(
                    TeamNameMapping.alias_name == alias_name,
                    TeamNameMapping.is_active == True
                )
            )
            .limit(1)
        )
        return result.scalar_one_or_none()
    
    async def find_api_team_id(
        self,
        alias_name: str
    ) -> Optional[int]:
        """查找API球队ID"""
        result = await self.session.execute(
            select(TeamNameMapping.api_team_id)
            .where(
                and_(
                    TeamNameMapping.alias_name == alias_name,
                    TeamNameMapping.is_active == True
                )
            )
            .limit(1)
        )
        return result.scalar_one_or_none()
    
    async def add_mapping(
        self,
        normalized_name: str,
        alias_name: str,
        api_team_id: Optional[int] = None,
        alias_type: str = 'common',
        source: Optional[str] = None
    ) -> TeamNameMapping:
        """
        添加新的名称映射
        
        如果同 alias_name + api_team_id 已存在，则返回现有记录
        否则创建新记录
        """
        # 检查是否已存在（复用现有记录）
        result = await self.session.execute(
            select(TeamNameMapping)
            .where(
                and_(
                    TeamNameMapping.alias_name == alias_name,
                    TeamNameMapping.api_team_id == api_team_id
                )
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            return existing
        
        # 创建新记录
        data = {
            'normalized_name': normalized_name,
            'alias_name': alias_name,
            'api_team_id': api_team_id,
            'alias_type': alias_type,
            'source': source
        }
        
        return await self.create(data)
    
    async def add_mappings_batch(
        self,
        mappings: List[dict]
    ) -> List[TeamNameMapping]:
        """批量添加名称映射"""
        return await self.create_many(mappings)
    
    async def get_all_aliases_for_team(
        self,
        normalized_name: str
    ) -> Set[str]:
        """获取指定标准化名称的所有别名（包含标准化名称本身）"""
        mappings = await self.get_by_normalized_name(normalized_name)
        aliases = {m.alias_name for m in mappings}
        aliases.add(normalized_name)
        return aliases
    
    async def search_aliases(
        self,
        search_term: str,
        limit: int = 20
    ) -> List[TeamNameMapping]:
        """搜索别名"""
        result = await self.session.execute(
            select(TeamNameMapping)
            .where(
                and_(
                    TeamNameMapping.alias_name.contains(search_term),
                    TeamNameMapping.is_active == True
                )
            )
            .limit(limit)
        )
        return result.scalars().all()
