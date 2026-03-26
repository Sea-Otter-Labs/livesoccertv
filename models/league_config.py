from sqlalchemy import Column, BigInteger, Integer, String, Boolean, UniqueConstraint, Index, DateTime
from sqlalchemy.orm import relationship
from models.base import BaseModel


class LeagueConfig(BaseModel):
    """联赛配置表 - 存储需要抓取的联赛列表"""
    
    __tablename__ = 'league_configs'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    api_league_id = Column(Integer, nullable=False, comment='API-Football 联赛ID')
    api_season = Column(Integer, nullable=False, comment='赛季年份')
    league_name = Column(String(100), nullable=False, comment='联赛名称')
    livesoccertv_url = Column(String(500), nullable=False, comment='LiveSoccerTV 联赛详情页URL')
    country = Column(String(100), comment='国家/地区')
    enabled = Column(Boolean, default=True, comment='是否启用')
    history_days = Column(Integer, default=7, comment='历史抓取天数')
    future_days = Column(Integer, default=7, comment='未来抓取天数')
    
    # 关系
    web_crawl_raws = relationship("WebCrawlRaw", back_populates="league_config", cascade="all, delete-orphan")
    crawl_task_statuses = relationship("CrawlTaskStatus", back_populates="league_config", cascade="all, delete-orphan")
    
    # 约束
    __table_args__ = (
        UniqueConstraint('api_league_id', 'api_season', name='uk_league_season'),
        Index('idx_enabled', 'enabled'),
        Index('idx_country', 'country'),
        {'comment': '联赛配置表，存储需要抓取的联赛列表'}
    )
    
    def __repr__(self):
        return f"<LeagueConfig(id={self.id}, name={self.league_name}, season={self.api_season})>"
