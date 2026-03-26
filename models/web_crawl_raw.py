"""
网页抓取原始数据模型
契约: 网页补充比赛契约
"""

from sqlalchemy import Column, BigInteger, Integer, String, JSON, Text, ForeignKey, Index
from sqlalchemy.orm import relationship
from models.base import BaseModel


class WebCrawlRaw(BaseModel):
    """LiveSoccerTV 网页抓取原始数据表"""
    
    __tablename__ = 'web_crawl_raw'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    crawl_batch_id = Column(String(50), nullable=False, comment='抓取批次ID')
    source_site = Column(String(50), default='livesoccertv', comment='来源站点')
    league_config_id = Column(BigInteger, ForeignKey('league_configs.id', ondelete='CASCADE'), nullable=False, comment='关联联赛配置ID')
    league_name = Column(String(100), nullable=False, comment='联赛名称(网页原始)')
    match_date_text = Column(String(100), comment='比赛日期原始文本')
    match_timestamp_utc = Column(Integer, comment='比赛时间 UTC 时间戳(秒)')
    home_team_name_raw = Column(String(100), nullable=False, comment='主队名称原始文本')
    home_team_name_normalized = Column(String(100), comment='主队名称(标准化后)')
    away_team_name_raw = Column(String(100), nullable=False, comment='客队名称原始文本')
    away_team_name_normalized = Column(String(100), comment='客队名称(标准化后)')
    channel_list = Column(JSON, comment='频道列表 JSON 格式')
    pagination_cursor = Column(String(200), comment='分页游标/页码标识')
    source_match_text = Column(Text, comment='内部解析文本(用于匹配参考)')
    page_url = Column(String(500), comment='抓取来源URL')
    crawled_at = Column(DateTime, comment='抓取时间')  # type: ignore
    
    # 关系
    league_config = relationship("LeagueConfig", back_populates="web_crawl_raws")
    match_broadcasts = relationship("MatchBroadcast", back_populates="web_crawl_raw")
    alert_logs = relationship("AlertLog", back_populates="web_crawl_raw")
    
    __table_args__ = (
        Index('idx_crawl_batch', 'crawl_batch_id'),
        Index('idx_league_config', 'league_config_id'),
        Index('idx_match_time', 'match_timestamp_utc'),
        Index('idx_crawled_at', 'crawled_at'),
        Index('idx_source_site', 'source_site'),
        {'comment': 'LiveSoccerTV 网页抓取原始数据表'}
    )
    
    def __repr__(self):
        return f"<WebCrawlRaw(id={self.id}, batch={self.crawl_batch_id}, {self.home_team_name_raw} vs {self.away_team_name_raw})>"
