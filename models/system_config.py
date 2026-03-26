from sqlalchemy import Column, BigInteger, String, Text, Boolean, UniqueConstraint, DateTime
from models.base import BaseModel
import enum


class ConfigType(enum.Enum):
    """配置类型枚举"""
    STRING = 'string'
    INTEGER = 'integer'
    FLOAT = 'float'
    BOOLEAN = 'boolean'
    JSON = 'json'


class SystemConfig(BaseModel):
    """系统配置表"""
    
    __tablename__ = 'system_configs'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    config_key = Column(String(100), nullable=False, unique=True, comment='配置键')
    config_value = Column(Text, comment='配置值')
    config_type = Column(String(50), default='string', comment='配置类型')
    description = Column(Text, comment='配置说明')
    is_editable = Column(Boolean, default=True, comment='是否可编辑')
    
    __table_args__ = (
        UniqueConstraint('config_key', name='uk_config_key'),
        {'comment': '系统配置表'}
    )
    
    def __repr__(self):
        return f"<SystemConfig(key={self.config_key}, type={self.config_type})>"
