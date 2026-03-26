from sqlalchemy import Column, BigInteger, Integer, String, Date, JSON, Numeric, ForeignKey, Index, Enum, DateTime
from sqlalchemy.orm import relationship
from models.base import BaseModel
import enum


class BroadcastMatchStatus(enum.Enum):
    """转播匹配状态枚举"""
    MATCHED = 'matched'
    UNMATCHED = 'unmatched'
    MISSING_CHANNELS = 'missing_channels'
    AMBIGUOUS = 'ambiguous'


class MatchBroadcast(BaseModel):
    """比赛与转播整合结果表 - API主数据 + LiveSoccerTV补充"""
    
    __tablename__ = 'match_broadcasts'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fixture_id = Column(Integer, ForeignKey('api_fixtures.fixture_id'), nullable=False, unique=True, comment='API fixture_id 业务主键')
    league_id = Column(Integer, nullable=False, comment='联赛ID')
    season = Column(Integer, nullable=False, comment='赛季')
    match_timestamp_utc = Column(Integer, nullable=False, comment='比赛时间 UTC 时间戳(秒)')
    match_date = Column(Date, nullable=False, comment='比赛日期')
    home_team_id = Column(Integer, nullable=False, comment='主队ID')
    home_team_name = Column(String(100), nullable=False, comment='主队名称')
    away_team_id = Column(Integer, nullable=False, comment='客队ID')
    away_team_name = Column(String(100), nullable=False, comment='客队名称')
    match_status = Column(String(50), comment='比赛状态')
    home_score = Column(Integer, comment='主队比分')
    away_score = Column(Integer, comment='客队比分')
    broadcast_match_status = Column(
        Enum(BroadcastMatchStatus),
        default=BroadcastMatchStatus.UNMATCHED,
        comment='转播匹配状态'
    )
    matched_confidence = Column(Numeric(3, 2), comment='匹配置信度 (0.00-1.00)')
    web_crawl_raw_id = Column(BigInteger, ForeignKey('web_crawl_raw.id', ondelete='SET NULL'), comment='关联的网页抓取记录ID')
    channels = Column(JSON, comment='频道列表 JSON [{name, country, type, is_streaming}]')
    last_verified_at = Column(DateTime, comment='最后验证时间')  # type: ignore
    
    # 关系
    api_fixture = relationship("ApiFixture", back_populates="match_broadcast")
    web_crawl_raw = relationship("WebCrawlRaw", back_populates="match_broadcasts")
    
    __table_args__ = (
        Index('idx_league_season', 'league_id', 'season'),
        Index('idx_match_time', 'match_timestamp_utc'),
        Index('idx_match_date', 'match_date'),
        Index('idx_broadcast_status', 'broadcast_match_status'),
        Index('idx_home_team', 'home_team_id'),
        Index('idx_away_team', 'away_team_id'),
        Index('idx_verified_at', 'last_verified_at'),
        {'comment': '比赛与转播整合结果表，API主数据 + LiveSoccerTV补充'}
    )
    
    def __repr__(self):
        return f"<MatchBroadcast(fixture_id={self.fixture_id}, {self.home_team_name} vs {self.away_team_name}, status={self.broadcast_match_status.value})>"
