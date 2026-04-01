from typing import Optional, List, Any, Dict
from pydantic import BaseModel, Field
from datetime import date, datetime


# ==================== 通用响应 ====================

class HealthResponse(BaseModel):
    service: str = "Football Broadcasts API"
    version: str = "2.0.0"
    status: str = "running"


class ErrorResponse(BaseModel):
    error: str


# ==================== 比赛详情 ====================

class MatchDetailResponse(BaseModel):
    fixture_id: int
    channels: Any = None
    channel_names: List[str] = []


# ==================== 比赛列表 ====================

class MatchListItem(BaseModel):
    fixture_id: int
    channels: Any = None


class MatchListFilters(BaseModel):
    league_id: Optional[int] = None
    season: Optional[int] = None
    start_timestamp: Optional[int] = None
    end_timestamp: Optional[int] = None
    team_id: Optional[int] = None
    status: Optional[str] = None
    has_channels: Optional[bool] = None
    broadcast_status: Optional[str] = None
    channel_country: Optional[str] = None
    default_league_id: int = 140


class MatchListRequest(BaseModel):
    """比赛列表查询请求"""
    league_id: Optional[int] = Field(default=140, description="联赛ID，默认 140")
    season: Optional[int] = Field(default=None, description="赛季")
    start_timestamp: Optional[int] = Field(default=None, description="开始日期 UTC timestamp")
    end_timestamp: Optional[int] = Field(default=None, description="结束日期 UTC timestamp")
    team_id: Optional[int] = Field(default=None, description="球队ID")
    status: Optional[str] = Field(default=None, description="比赛状态")
    has_channels: Optional[bool] = Field(default=None, description="是否有频道信息 true/false")
    broadcast_status: Optional[str] = Field(default="matched", description="转播状态 matched/unmatched/missing_channels/ambiguous")
    channel_country: Optional[str] = Field(default="Spain", description="频道国家，传空字符串返回所有国家")
    limit: Optional[int] = Field(default=None, description="返回数量限制；不传时返回全部命中结果")
    offset: int = Field(default=0, description="偏移量")


class MatchListResponse(BaseModel):
    total: int
    offset: int
    limit: Optional[int] = None
    # filters: Optional[MatchListFilters] = None
    matches: List[MatchListItem]


# ==================== 联赛 ====================

class LeagueItem(BaseModel):
    id: int
    api_league_id: int
    season: int
    name: str
    country: Optional[str] = None
    enabled: bool


class LeagueListResponse(BaseModel):
    leagues: List[LeagueItem]


# ==================== 告警 ====================

class AlertItem(BaseModel):
    id: int
    alert_type: Optional[str] = None
    severity: Optional[str] = None
    league_id: Optional[int] = None
    league_name: Optional[str] = None
    fixture_id: Optional[int] = None
    match_timestamp_utc: Optional[int] = None
    home_team_name: Optional[str] = None
    away_team_name: Optional[str] = None
    exception_summary: Optional[str] = None
    suggested_action: Optional[str] = None
    is_resolved: bool = False
    created_at: Optional[str] = None


class AlertListResponse(BaseModel):
    total: int
    offset: int
    limit: int
    alerts: List[AlertItem]


# ==================== 异常比赛 ====================

class MismatchItem(BaseModel):
    fixture_id: int
    league_id: int
    season: int
    match_timestamp_utc: Optional[int] = None
    match_date: Optional[str] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    broadcast_match_status: Optional[str] = None
    matched_confidence: Optional[float] = None


class MismatchListResponse(BaseModel):
    total: int
    offset: int
    limit: int
    mismatches: List[MismatchItem]


# ==================== 缓存 ====================

class CacheStatusResponse(BaseModel):
    cached: bool
    ttl_seconds: Optional[int] = None
    data: Optional[Dict[str, Any]] = None


class CacheDeleteResponse(BaseModel):
    deleted: bool
    message: str


class CacheClearResponse(BaseModel):
    cleared: bool
    deleted_count: int
