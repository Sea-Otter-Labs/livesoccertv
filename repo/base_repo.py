"""
基础 Repository 类
提供通用的异步 CRUD 操作
"""

from typing import TypeVar, Generic, List, Optional, Dict, Any, Type
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, func
from sqlalchemy.orm import joinedload
from models.base import BaseModel

T = TypeVar('T', bound=BaseModel)


class BaseRepository(Generic[T]):
    """基础仓库类 - 封装通用CRUD操作"""
    
    def __init__(self, session: AsyncSession, model_class: Type[T]):
        self.session = session
        self.model_class = model_class
    
    async def get_by_id(self, id: int) -> Optional[T]:
        """根据ID获取记录"""
        result = await self.session.execute(
            select(self.model_class).where(self.model_class.id == id)
        )
        return result.scalar_one_or_none()
    
    async def get_all(self, skip: int = 0, limit: int = 100) -> List[T]:
        """获取所有记录（支持分页）"""
        result = await self.session.execute(
            select(self.model_class).offset(skip).limit(limit)
        )
        return result.scalars().all()
    
    async def create(self, data: Dict[str, Any]) -> T:
        """创建新记录"""
        instance = self.model_class.from_dict(data)
        self.session.add(instance)
        await self.session.flush()
        return instance
    
    async def create_many(self, data_list: List[Dict[str, Any]]) -> List[T]:
        """批量创建记录"""
        instances = [self.model_class.from_dict(data) for data in data_list]
        self.session.add_all(instances)
        await self.session.flush()
        return instances
    
    async def update(self, id: int, data: Dict[str, Any]) -> Optional[T]:
        """更新记录"""
        # 过滤掉None值和id字段
        update_data = {k: v for k, v in data.items() if v is not None and k != 'id'}
        
        if update_data:
            await self.session.execute(
                update(self.model_class)
                .where(self.model_class.id == id)
                .values(**update_data)
            )
            await self.session.flush()
        
        return await self.get_by_id(id)
    
    async def delete(self, id: int) -> bool:
        """删除记录"""
        result = await self.session.execute(
            delete(self.model_class).where(self.model_class.id == id)
        )
        await self.session.flush()
        return result.rowcount > 0
    
    async def count(self) -> int:
        """获取记录总数"""
        result = await self.session.execute(
            select(func.count()).select_from(self.model_class)
        )
        return result.scalar()
    
    async def exists(self, id: int) -> bool:
        """检查记录是否存在"""
        result = await self.session.execute(
            select(func.count())
            .select_from(self.model_class)
            .where(self.model_class.id == id)
        )
        return result.scalar() > 0
