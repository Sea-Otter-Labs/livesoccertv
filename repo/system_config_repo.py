"""
系统配置 Repository
"""

from typing import List, Optional, Any
import json
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from repo.base_repo import BaseRepository
from models.system_config import SystemConfig


class SystemConfigRepository(BaseRepository[SystemConfig]):
    """系统配置数据访问层"""
    
    def __init__(self, session: AsyncSession):
        super().__init__(session, SystemConfig)
    
    async def get_by_key(self, key: str) -> Optional[SystemConfig]:
        """根据配置键获取配置"""
        result = await self.session.execute(
            select(SystemConfig).where(SystemConfig.config_key == key)
        )
        return result.scalar_one_or_none()
    
    async def get_value(
        self, 
        key: str, 
        default: Any = None
    ) -> Any:
        """
        获取配置值
        根据配置类型自动转换值
        """
        config = await self.get_by_key(key)
        
        if not config:
            return default
        
        value = config.config_value
        
        if config.config_type == 'integer':
            return int(value) if value else default
        elif config.config_type == 'float':
            return float(value) if value else default
        elif config.config_type == 'boolean':
            return value.lower() in ('true', '1', 'yes') if value else default
        elif config.config_type == 'json':
            return json.loads(value) if value else default
        else:
            return value if value else default
    
    async def get_string(self, key: str, default: str = '') -> str:
        """获取字符串配置"""
        return await self.get_value(key, default)
    
    async def get_int(self, key: str, default: int = 0) -> int:
        """获取整数配置"""
        return await self.get_value(key, default)
    
    async def get_float(self, key: str, default: float = 0.0) -> float:
        """获取浮点数配置"""
        return await self.get_value(key, default)
    
    async def get_bool(self, key: str, default: bool = False) -> bool:
        """获取布尔配置"""
        return await self.get_value(key, default)
    
    async def get_json(self, key: str, default: dict = None) -> dict:
        """获取JSON配置"""
        return await self.get_value(key, default or {})
    
    async def set_value(
        self, 
        key: str, 
        value: Any, 
        config_type: str = 'string',
        description: str = None
    ) -> SystemConfig:
        """
        设置配置值
        """
        # 根据类型转换值
        if config_type == 'json':
            str_value = json.dumps(value)
        elif config_type == 'boolean':
            str_value = 'true' if value else 'false'
        else:
            str_value = str(value) if value is not None else ''
        
        existing = await self.get_by_key(key)
        
        if existing:
            update_data = {
                'config_value': str_value,
                'config_type': config_type
            }
            if description:
                update_data['description'] = description
            return await self.update(existing.id, update_data)
        else:
            return await self.create({
                'config_key': key,
                'config_value': str_value,
                'config_type': config_type,
                'description': description or ''
            })
    
    async def set_values_batch(
        self, 
        configs: List[dict]
    ) -> List[SystemConfig]:
        """批量设置配置"""
        results = []
        for config in configs:
            result = await self.set_value(
                config['key'],
                config['value'],
                config.get('type', 'string'),
                config.get('description')
            )
            results.append(result)
        return results
    
    async def get_editable_configs(self) -> List[SystemConfig]:
        """获取可编辑的配置"""
        result = await self.session.execute(
            select(SystemConfig)
            .where(SystemConfig.is_editable == True)
            .order_by(SystemConfig.config_key)
        )
        return result.scalars().all()
    
    async def get_all_as_dict(self) -> dict:
        """获取所有配置为字典"""
        configs = await self.get_all()
        result = {}
        for config in configs:
            result[config.config_key] = await self.get_value(config.config_key)
        return result
