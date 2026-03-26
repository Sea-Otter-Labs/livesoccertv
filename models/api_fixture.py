from sqlalchemy import Column, BigInteger, Integer, String, Date, Index, DateTime
from sqlalchemy.orm import relationship
from models.base import BaseModel


class ApiFixture(BaseModel):
    """API-Football 主比赛数据表 - 权威数据源"""
    
    __tablename__ = 'api_fixtures'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    fixture_id = Column(Integer, nullable=False, unique=True, comment='API-Football fixture.id 业务主键')
    league_id = Column(Integer, nullable=False, comment='联赛ID')
    season = Column(Integer, nullable=False, comment='赛季')
    match_timestamp_utc = Column(Integer, nullable=False, comment='比赛时间 UTC 时间戳(秒)')
    match_date = Column(Date, nullable=False, comment='比赛日期')
    home_team_id = Column(Integer, nullable=False, comment='主队ID')
    home_team_name = Column(String(100), nullable=False, comment='主队名称')
    home_team_name_normalized = Column(String(100), comment='主队名称(标准化后)')
    away_team_id = Column(Integer, nullable=False, comment='客队ID')
    away_team_name = Column(String(100), nullable=False, comment='客队名称')
    away_team_name_normalized = Column(String(100), comment='客队名称(标准化后)')
    status = Column(String(50), comment='比赛状态')
    round = Column(String(100), comment='轮次')
    home_score = Column(Integer, comment='主队比分')
    away_score = Column(Integer, comment='客队比分')
    venue = Column(String(200), comment='比赛场地')
    synced_at = Column(DateTime, comment='同步时间')  # type: ignore
    
    # 关系
    match_broadcast = relationship("MatchBroadcast", back_populates="api_fixture", uselist=False)
    
    __table_args__ = (
        Index('idx_league_season', 'league_id', 'season'),
        Index('idx_match_time', 'match_timestamp_utc'),
        Index('idx_match_date', 'match_date'),
        Index('idx_status', 'status'),
        Index('idx_home_team', 'home_team_id'),
        Index('idx_away_team', 'away_team_id'),
        {'comment': 'API-Football 主比赛数据表，权威数据源'}
    )
    
    def __repr__(self):
        return f"<ApiFixture(fixture_id={self.fixture_id}, {self.home_team_name} vs {self.away_team_name})>"
