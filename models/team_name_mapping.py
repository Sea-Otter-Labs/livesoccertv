from sqlalchemy import Column, BigInteger, Integer, String, Boolean, Index, Enum, UniqueConstraint, DateTime
from models.base import BaseModel
import enum


class AliasType(enum.Enum):
    """别名类型枚举"""
    LANGUAGE = 'language'
    ABBREVIATION = 'abbreviation'
    HISTORIC = 'historic'
    COMMON = 'common'


class TeamNameMapping(BaseModel):
    """球队名称映射表 - 支持多语言别名匹配"""
    
    __tablename__ = 'team_name_mappings'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    api_team_id = Column(Integer, comment='API-Football 球队ID')
    normalized_name = Column(String(100), nullable=False, comment='标准化名称')
    alias_name = Column(String(100), nullable=False, comment='别名/变体名称')
    alias_type = Column(Enum(AliasType), default=AliasType.COMMON, comment='别名类型')
    source = Column(String(50), comment='来源')
    is_active = Column(Boolean, default=True, comment='是否启用')
    
    __table_args__ = (
        UniqueConstraint('alias_name', 'api_team_id', name='uk_alias_team'),
        Index('idx_normalized', 'normalized_name'),
        Index('idx_api_team', 'api_team_id'),
        Index('idx_alias_type', 'alias_type'),
        Index('idx_is_active', 'is_active'),
        {'comment': '球队名称映射表，支持多语言别名匹配'}
    )
    
    def __repr__(self):
        return f"<TeamNameMapping(alias={self.alias_name} -> {self.normalized_name})>"
