"""
告警记录模型
契约: 告警契约
"""

from sqlalchemy import Column, BigInteger, Integer, String, Text, Boolean, ForeignKey, Index, Enum
from sqlalchemy.orm import relationship
from models.base import BaseModel
import enum


class AlertType(enum.Enum):
    """告警类型枚举"""
    UNMATCHED_WEB_TO_API = 'unmatched_web_to_api'
    UNMATCHED_API_TO_WEB = 'unmatched_api_to_web'
    MISSING_CHANNELS = 'missing_channels'
    AMBIGUOUS_MATCH = 'ambiguous_match'
    CAPTCHA_BLOCKED = 'captcha_blocked'
    PARSE_ERROR = 'parse_error'
    SYSTEM_ERROR = 'system_error'


class Severity(enum.Enum):
    """严重等级枚举"""
    CRITICAL = 'critical'
    HIGH = 'high'
    MEDIUM = 'medium'
    LOW = 'low'


class AlertLog(BaseModel):
    """告警记录表 - 用于追踪对齐失败、抓取异常等"""
    
    __tablename__ = 'alert_logs'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    alert_type = Column(Enum(AlertType), nullable=False, comment='告警类型')
    severity = Column(Enum(Severity), default=Severity.MEDIUM, comment='严重等级')
    league_id = Column(Integer, comment='联赛ID')
    league_name = Column(String(100), comment='联赛名称')
    season = Column(Integer, comment='赛季')
    fixture_id = Column(Integer, comment='API fixture ID (如有)')
    web_crawl_raw_id = Column(BigInteger, ForeignKey('web_crawl_raw.id', ondelete='SET NULL'), comment='网页记录ID (如有)')
    match_timestamp_utc = Column(Integer, comment='比赛时间 UTC 时间戳')
    home_team_name = Column(String(100), comment='主队名')
    away_team_name = Column(String(100), comment='客队名')
    exception_summary = Column(Text, comment='异常原因摘要')
    suggested_action = Column(Text, comment='建议处理动作')
    is_resolved = Column(Boolean, default=False, comment='是否已处理')
    resolved_at = Column(DateTime, comment='处理时间')  # type: ignore
    resolved_by = Column(String(100), comment='处理人')
    resolution_notes = Column(Text, comment='处理备注')
    notified_at = Column(DateTime, comment='Lark通知发送时间')  # type: ignore
    notification_response = Column(Text, comment='通知响应')
    
    # 关系
    web_crawl_raw = relationship("WebCrawlRaw", back_populates="alert_logs")
    
    __table_args__ = (
        Index('idx_alert_type', 'alert_type'),
        Index('idx_severity', 'severity'),
        Index('idx_is_resolved', 'is_resolved'),
        Index('idx_league', 'league_id'),
        Index('idx_fixture', 'fixture_id'),
        Index('idx_created_at', 'created_at'),
        Index('idx_match_time', 'match_timestamp_utc'),
        {'comment': '告警记录表，用于追踪对齐失败、抓取异常等'}
    )
    
    def __repr__(self):
        return f"<AlertLog(id={self.id}, type={self.alert_type.value}, severity={self.severity.value})>"
