import os
import sys
import logging
import json
import hashlib
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import JSONResponse
import redis
from datetime import datetime, timedelta
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware

from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.match_channels import (
    DEFAULT_MATCH_LIST_LEAGUE_ID,
    build_list_matches_filters,
    serialize_match_list_item,
)
from api.schemas import (
    HealthResponse,
    ErrorResponse,
    MatchDetailResponse,
    MatchListResponse,
    MatchListFilters,
    MatchListRequest,
    LeagueListResponse,
    AlertListResponse,
    MismatchListResponse,
    CacheStatusResponse,
    CacheDeleteResponse,
)

from config.database import AsyncSessionLocal, init_db
from repo import MatchBroadcastRepository, AlertLogRepository, LeagueConfigRepository

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis 缓存配置
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=int(os.getenv('REDIS_DB', 0)),
    password=os.getenv('REDIS_PASSWORD', None),
    decode_responses=True
)

# 缓存过期时间（秒）- 1天
CACHE_TTL = 86400


def get_cache_key(fixture_id: int) -> str:
    """生成比赛详情缓存键"""
    return f"match:detail:{fixture_id}"


def get_list_cache_key(filters: dict) -> str:
    """
    生成比赛列表缓存键（分页参数不参与）
    
    Args:
        filters: 筛选参数字典
    
    Returns:
        缓存键字符串
    """
    # 排除分页参数
    cache_params = {
        k: v for k, v in filters.items() 
        if v is not None and k not in ('limit', 'offset')
    }
    
    # 统一序列化（None 已排除，布尔值转小写字符串保持一致性）
    param_str = json.dumps(cache_params, sort_keys=True, separators=(',', ':'))
    hash_val = hashlib.md5(param_str.encode()).hexdigest()[:16]
    
    return f"match:list:{hash_val}"


# ==================== 生命周期管理 ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时初始化数据库
    logger.info("Initializing database...")
    await init_db()
    logger.info("Database initialized.")
    yield
    # 关闭时清理（可选）
    logger.info("Shutting down...")


# 创建 FastAPI 应用
app = FastAPI(
    title="Football Broadcasts API",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== 路由 ====================

@app.get("/", response_model=HealthResponse)
async def index():
    """首页"""
    return HealthResponse()


@app.get("/health", tags=["Health"])
async def health_check():
    """
    详细健康检查端点
    
    用于 Docker 健康检查和监控系统
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {}
    }
    
    # 检查数据库连接
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        health_status["services"]["database"] = "healthy"
    except Exception as e:
        health_status["services"]["database"] = f"unhealthy: {str(e)}"
        health_status["status"] = "unhealthy"
    
    # 检查 Redis 连接
    try:
        redis_client.ping()
        health_status["services"]["redis"] = "healthy"
    except Exception as e:
        health_status["services"]["redis"] = f"unhealthy: {str(e)}"
        health_status["status"] = "unhealthy"
    
    if health_status["status"] == "unhealthy":
        raise HTTPException(status_code=503, detail=health_status)
    
    return health_status


@app.post("/api/matches/{fixture_id}", response_model=MatchDetailResponse)
async def get_match_by_fixture_id(fixture_id: int):
    """
    获取指定比赛的详情和转播信息（带 Redis 缓存）
    """
    cache_key = get_cache_key(fixture_id)

    # 1. 先尝试从 Redis 缓存获取
    try:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            logger.info(f"Cache hit for match {fixture_id}")
            return MatchDetailResponse(**json.loads(cached_data))
    except Exception as e:
        logger.warning(f"Redis read error: {e}")

    # 2. 缓存未命中，从数据库查询
    async with AsyncSessionLocal() as session:
        repo = MatchBroadcastRepository(session)
        match = await repo.get_by_fixture_id(fixture_id)

        if not match:
            raise HTTPException(status_code=404, detail="Match not found")

        # 提取频道名称列表
        channels_data = match.channels or {}
        channel_names = []
        for country, channels in channels_data.items():
            if isinstance(channels, list):
                channel_names.extend(channels)
        
        result = {
            'fixture_id': match.fixture_id,
            'channels': match.channels,
            'channel_names': channel_names
        }

    # 3. 写入 Redis 缓存（1小时过期）
    try:
        redis_client.setex(
            cache_key,
            3600,
            json.dumps(result, ensure_ascii=False)
        )
        logger.info(f"Cache set for match {fixture_id}, TTL={CACHE_TTL}s")
    except Exception as e:
        logger.warning(f"Redis write error: {e}")

    return MatchDetailResponse(**result)


async def parse_list_matches_request(request: Request) -> MatchListRequest:
    """
    解析比赛列表请求，同时支持 JSON 和 form-data 格式
    """
    content_type = request.headers.get("content-type", "")
    
    if "application/json" in content_type:
        data = await request.json()
    elif "multipart/form-data" in content_type or "application/x-www-form-urlencoded" in content_type:
        form_data = await request.form()
        data = {}
        for key, value in form_data.items():
            if value == "":
                data[key] = None
            elif key in ["league_id", "season", "team_id", "limit", "offset"]:
                data[key] = int(value) if value else None
            elif key == "has_channels":
                data[key] = value.lower() == "true" if value else None
            else:
                data[key] = value
    else:
        # 默认为 JSON
        data = await request.json()
    
    return MatchListRequest(**data)


@app.post("/api/matches", response_model=MatchListResponse)
async def list_matches(request: Request):
    """
    获取比赛列表（支持基于筛选条件的缓存）
    
    支持 JSON 请求体和 form-data 两种格式传递参数
    缓存键基于筛选条件（不含分页参数），完整结果缓存后按分页切片返回
    """
    req = await parse_list_matches_request(request)

    start_timestamp = None
    end_timestamp = None
    if req.expire_at:
        try:
            dt = datetime.fromisoformat(req.expire_at)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"expire_at 格式无效，期望 ISO 8601 格式（如 2027-04-01T00:00:00.000+08:00），实际传入: {req.expire_at}"
            )
        start_timestamp = int(dt.timestamp())
        end_timestamp = int((dt + timedelta(days=1)).timestamp())
        logger.info(f"开始：{start_timestamp} --- 结束：{end_timestamp}")

    params = {
        'league_id': req.league_id,
        'season': req.season,
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp,
        'team_id': req.team_id,
        'status': req.status,
        'has_channels': req.has_channels,
        'broadcast_status': req.broadcast_status,
        'channel_country': req.channel_country if req.channel_country else None,
        'limit': req.limit,
        'offset': req.offset,
    }
    
    filters = build_list_matches_filters(params)
    
    # 生成缓存键（分页参数不参与）
    cache_key = get_list_cache_key(filters)
    
    # 1. 尝试从 Redis 缓存获取完整结果
    try:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            logger.info(f"List cache hit: {cache_key}")
            # 解析缓存的完整结果
            all_results = json.loads(cached_data)
            total = len(all_results)
            
            # 在缓存结果上进行分页切片
            if req.limit is not None:
                paginated_results = all_results[req.offset:req.offset + req.limit]
            elif req.offset:
                paginated_results = all_results[req.offset:]
            else:
                paginated_results = all_results
            
            return MatchListResponse(
                total=total,
                offset=req.offset,
                limit=req.limit,
                matches=paginated_results
            )
    except Exception as e:
        logger.warning(f"Redis list cache read error: {e}")
    
    # 2. 缓存未命中，从数据库查询
    logger.info(f"List cache miss: {cache_key}, querying database")
    
    async with AsyncSessionLocal() as session:
        repo = MatchBroadcastRepository(session)
        
        matches = await repo.get_by_time_range(
            start_timestamp=filters['start_timestamp'],
            end_timestamp=filters['end_timestamp'],
            league_id=filters['league_id'],
            season=filters['season'],
            has_channels=filters['has_channels'],
            broadcast_status=filters['broadcast_status'],
            channel_country=filters['channel_country'],
        )

        # 转换所有数据（先序列化，再分页）
        total = len(matches)
        all_results = [serialize_match_list_item(match, channel_country=filters['channel_country']) for match in matches]
        
        # 3. 写入 Redis 缓存（完整结果，1天过期）
        try:
            redis_client.setex(
                cache_key,
                CACHE_TTL,
                json.dumps(all_results, ensure_ascii=False)
            )
            logger.info(f"List cache set: {cache_key}, TTL={CACHE_TTL}s, total={total}")
        except Exception as e:
            logger.warning(f"Redis list cache write error: {e}")
        
        # 4. 分页切片
        if req.limit is not None:
            paginated_results = all_results[req.offset:req.offset + req.limit]
        elif req.offset:
            paginated_results = all_results[req.offset:]
        else:
            paginated_results = all_results

        return MatchListResponse(
            total=total,
            offset=req.offset,
            limit=req.limit,
            matches=paginated_results
        )


@app.get("/api/leagues", response_model=LeagueListResponse)
async def list_leagues():
    """
    获取联赛列表
    """
    async with AsyncSessionLocal() as session:
        repo = LeagueConfigRepository(session)
        leagues = await repo.get_enabled_configs()
        
        return LeagueListResponse(
            leagues=[
                {
                    'id': l.id,
                    'api_league_id': l.api_league_id,
                    'season': l.api_season,
                    'name': l.league_name,
                    'country': l.country,
                    'enabled': l.enabled
                }
                for l in leagues
            ]
        )


@app.get("/api/mismatches", response_model=MismatchListResponse)
async def list_mismatches(
    league_id: Optional[int] = Query(None, description="联赛ID"),
    season: Optional[int] = Query(None, description="赛季"),
    limit: int = Query(default=100, description="返回数量限制"),
    offset: int = Query(default=0, description="偏移量"),
):
    """
    获取未对齐/异常的比赛列表
    """
    async with AsyncSessionLocal() as session:
        repo = MatchBroadcastRepository(session)
        
        mismatches = await repo.get_mismatches(
            league_id=league_id,
            season=season
        )
        
        # 分页
        total = len(mismatches)
        mismatches = mismatches[offset:offset + limit]
        
        # 转换数据
        results = []
        for match in mismatches:
            results.append({
                'fixture_id': match.fixture_id,
                'league_id': match.league_id,
                'season': match.season,
                'match_timestamp_utc': match.match_timestamp_utc,
                'match_date': match.match_date.isoformat() if match.match_date else None,
                'home_team': match.home_team_name,
                'away_team': match.away_team_name,
                'broadcast_match_status': match.broadcast_match_status.value if hasattr(match.broadcast_match_status, 'value') else match.broadcast_match_status,
                'matched_confidence': float(match.matched_confidence) if match.matched_confidence else None
            })
        
        return MismatchListResponse(
            total=total,
            offset=offset,
            limit=limit,
            mismatches=results
        )


@app.get("/api/alerts", response_model=AlertListResponse)
async def list_alerts(
    unresolved_only: bool = Query(default=True, description="只显示未解决的 true/false"),
    severity: Optional[str] = Query(None, description="严重等级 critical/high/medium/low"),
    league_id: Optional[int] = Query(None, description="联赛ID"),
    limit: int = Query(default=100, description="返回数量限制"),
    offset: int = Query(default=0, description="偏移量"),
):
    """
    获取告警列表
    """
    async with AsyncSessionLocal() as session:
        repo = AlertLogRepository(session)
        
        if unresolved_only:
            alerts = await repo.get_unresolved(severity=severity)
        elif league_id:
            alerts = await repo.get_by_league(league_id, is_resolved=not unresolved_only)
        else:
            alerts = await repo.get_all(limit=limit)
        
        # 分页
        total = len(alerts)
        alerts = alerts[offset:offset + limit]
        
        # 转换数据
        results = []
        for alert in alerts:
            results.append({
                'id': alert.id,
                'alert_type': alert.alert_type.value if hasattr(alert.alert_type, 'value') else alert.alert_type,
                'severity': alert.severity.value if hasattr(alert.severity, 'value') else alert.severity,
                'league_id': alert.league_id,
                'league_name': alert.league_name,
                'fixture_id': alert.fixture_id,
                'match_timestamp_utc': alert.match_timestamp_utc,
                'home_team_name': alert.home_team_name,
                'away_team_name': alert.away_team_name,
                'exception_summary': alert.exception_summary,
                'suggested_action': alert.suggested_action,
                'is_resolved': alert.is_resolved,
                'created_at': alert.created_at.isoformat() if alert.created_at else None
            })
        
        return AlertListResponse(
            total=total,
            offset=offset,
            limit=limit,
            alerts=results
        )


# ==================== 缓存管理接口 ====================

@app.get("/api/cache/match/{fixture_id}", response_model=CacheStatusResponse)
async def get_match_cache(fixture_id: int):
    """
    手动查询比赛详情缓存
    """
    cache_key = get_cache_key(fixture_id)

    try:
        # 检查缓存是否存在
        ttl = redis_client.ttl(cache_key)
        if ttl == -2:  # key 不存在
            return CacheStatusResponse(cached=False)

        # 获取缓存数据
        cached_data = redis_client.get(cache_key)
        if cached_data:
            return CacheStatusResponse(
                cached=True,
                ttl_seconds=ttl if ttl > 0 else None,
                data=json.loads(cached_data)
            )
        else:
            return CacheStatusResponse(cached=False)

    except Exception as e:
        logger.error(f"Error checking cache for match {fixture_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/cache/match/{fixture_id}", response_model=CacheDeleteResponse)
async def delete_match_cache(fixture_id: int):
    """
    手动清除指定比赛的详情缓存
    """
    cache_key = get_cache_key(fixture_id)

    try:
        # 删除缓存
        deleted = redis_client.delete(cache_key)
        if deleted > 0:
            logger.info(f"Cache manually cleared for match {fixture_id}")
            return CacheDeleteResponse(
                deleted=True,
                message=f'Cache cleared for match {fixture_id}'
            )
        else:
            return CacheDeleteResponse(
                deleted=False,
                message=f'Cache key not found for match {fixture_id}'
            )

    except Exception as e:
        logger.error(f"Error clearing cache for match {fixture_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/cache/list", response_model=CacheDeleteResponse)
async def delete_list_cache(
    league_id: int = Query(default=DEFAULT_MATCH_LIST_LEAGUE_ID, description="联赛ID，默认 140"),
    season: Optional[int] = Query(None, description="赛季"),
    start_timestamp: Optional[int] = Query(None, description="开始日期 UTC timestamp"),
    end_timestamp: Optional[int] = Query(None, description="结束日期 UTC timestamp"),
    team_id: Optional[int] = Query(None, description="球队ID"),
    status: Optional[str] = Query(None, description="比赛状态"),
    has_channels: Optional[bool] = Query(None, description="是否有频道信息 true/false"),
    broadcast_status: str = Query(default="matched", description="转播状态 matched/unmatched/missing_channels/ambiguous"),
    channel_country: str = Query(default="Spain", description="频道国家，传空字符串返回所有国家"),
):
    """
    手动清除指定筛选条件的比赛列表缓存
    
    根据传入的筛选参数生成缓存键并删除对应缓存
    """
    # 构建筛选参数（排除分页参数）
    filters = {
        'league_id': league_id,
        'season': season,
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp,
        'team_id': team_id,
        'status': status,
        'has_channels': has_channels,
        'broadcast_status': broadcast_status,
        'channel_country': channel_country if channel_country else None,
    }
    
    cache_key = get_list_cache_key(filters)
    
    try:
        deleted = redis_client.delete(cache_key)
        if deleted > 0:
            logger.info(f"List cache manually cleared: {cache_key}")
            return CacheDeleteResponse(
                deleted=True,
                message=f'List cache cleared for filters: {filters}'
            )
        else:
            return CacheDeleteResponse(
                deleted=False,
                message=f'List cache key not found: {cache_key}'
            )
    except Exception as e:
        logger.error(f"Error clearing list cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def clear_all_match_cache() -> int:
    """
    清除所有比赛缓存（包括详情缓存和列表缓存）
    
    Returns:
        删除的 key 数量
    """
    deleted_count = 0
    
    # 清除比赛详情缓存
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match='match:detail:*', count=100)
        if keys:
            deleted_count += redis_client.delete(*keys)
        if cursor == 0:
            break
    
    # 清除比赛列表缓存
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match='match:list:*', count=100)
        if keys:
            deleted_count += redis_client.delete(*keys)
        if cursor == 0:
            break

    logger.info(f"All match cache cleared, deleted {deleted_count} keys")
    return deleted_count


# ==================== 异常处理 ====================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """HTTP 异常处理"""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """通用异常处理"""
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"}
    )


# ==================== 启动 ====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.app:app",
        host="127.0.0.1",
        port=int(os.getenv('API_PORT', 30000)),
        reload=os.getenv('API_RELOAD', 'False').lower() == 'true'
    )
