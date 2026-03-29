from sqlalchemy import Column, BigInteger, Integer, String, Date, ForeignKey, Index, Enum, Text, UniqueConstraint, DateTime
from sqlalchemy.orm import relationship
from models.base import BaseModel
import enum


class TaskPhase(enum.Enum):
    """任务阶段枚举"""
    INIT = 'init'
    API_SYNC = 'api_sync'
    WEB_CRAWL = 'web_crawl'
    MATCHING = 'matching'
    COMPLETED = 'completed'
    FAILED = 'failed'
    PAUSED_CAPTCHA = 'paused_captcha'


class TaskStatus(enum.Enum):
    """任务状态枚举"""
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'
    PAUSED = 'paused'
    ENVIRONMENT_BLOCKED = 'environment_blocked'


class PaginationDirection(enum.Enum):
    """翻页方向枚举"""
    LEFT = 'left'
    RIGHT = 'right'
    NONE = 'none'


class CrawlTaskStatus(BaseModel):
    """抓取任务状态表 - 支持断点恢复和流程追踪"""
    
    __tablename__ = 'crawl_task_status'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    crawl_batch_id = Column(String(50), nullable=False, comment='抓取批次ID')
    league_config_id = Column(BigInteger, ForeignKey('league_configs.id', ondelete='CASCADE'), nullable=False, comment='联赛配置ID')
    task_phase = Column(Enum(TaskPhase), default=TaskPhase.INIT, comment='任务阶段')
    status = Column(Enum(TaskStatus), default=TaskStatus.PENDING, comment='任务状态')
    current_pagination_cursor = Column(String(200), comment='当前分页游标')
    pagination_direction = Column(Enum(PaginationDirection), default=PaginationDirection.NONE, comment='当前翻页方向')
    window_start_date = Column(Date, comment='时间窗口开始日期')
    window_end_date = Column(Date, comment='时间窗口结束日期')
    matches_crawled = Column(Integer, default=0, comment='已抓取比赛数')
    matches_matched = Column(Integer, default=0, comment='已对齐比赛数')
    started_at = Column(DateTime, comment='开始时间')  # type: ignore
    completed_at = Column(DateTime, comment='完成时间')  # type: ignore
    error_message = Column(Text, comment='错误信息')
    # captcha_detected_at = Column(DateTime, comment='检测到验证码时间')  # type: ignore
    # captcha_resolved_at = Column(DateTime, comment='验证码解决时间')  # type: ignore
    
    # 关系
    league_config = relationship("LeagueConfig", back_populates="crawl_task_statuses")
    
    __table_args__ = (
        UniqueConstraint('crawl_batch_id', 'league_config_id', name='uk_batch_league'),
        Index('idx_league_config', 'league_config_id'),
        Index('idx_task_phase', 'task_phase'),
        Index('idx_status', 'status'),
        Index('idx_started_at', 'started_at'),
        {'comment': '抓取任务状态表，支持断点恢复和流程追踪'}
    )
    
    def __repr__(self):
        return f"<CrawlTaskStatus(batch={self.crawl_batch_id}, phase={self.task_phase.value}, status={self.status.value})>"
