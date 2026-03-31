import os
import logging
import json
from datetime import datetime
from typing import Optional

from flask import Flask, jsonify, request
from asgiref.sync import async_to_sync
import redis

from api.match_listing import (
    DEFAULT_MATCH_LIST_LEAGUE_ID,
    build_list_matches_filters,
    serialize_match_list_item,
)
from config.database import AsyncSessionLocal, init_db, close_db
from repo import MatchBroadcastRepository, AlertLogRepository, LeagueConfigRepository

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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


def get_db_session():
    """获取数据库会话"""
    return AsyncSessionLocal()


@app.route('/')
def index():
    """首页"""
    return jsonify({
        'service': 'Football Broadcasts API',
        'version': '1.0.0',
        'status': 'running'
    })


@app.route('/api/matches/<int:fixture_id>')
def get_match_by_fixture_id(fixture_id: int):
    """
    获取指定比赛的详情和转播信息（带 Redis 缓存）
    """
    cache_key = get_cache_key(fixture_id)

    # 1. 先尝试从 Redis 缓存获取
    try:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            logger.info(f"Cache hit for match {fixture_id}")
            return jsonify(json.loads(cached_data))
    except Exception as e:
        logger.warning(f"Redis read error: {e}")

    # 2. 缓存未命中，从数据库查询
    async def _fetch():
        async with AsyncSessionLocal() as session:
            repo = MatchBroadcastRepository(session)
            match = await repo.get_by_fixture_id(fixture_id)

            if not match:
                return None

            return {
                'fixture_id': match.fixture_id,
                'league_id': match.league_id,
                'season': match.season,
                'match_timestamp_utc': match.match_timestamp_utc,
                'match_date': match.match_date.isoformat() if match.match_date else None,
                'home_team_id': match.home_team_id,
                'home_team': match.home_team_name,
                'away_team_id': match.away_team_id,
                'away_team': match.away_team_name,
                'status': match.match_status,
                'broadcast_match_status': match.broadcast_match_status.value if match.broadcast_match_status else None,
                'matched_confidence': float(match.matched_confidence) if match.matched_confidence else None,
                'channels': match.channels,
                'last_verified_at': match.last_verified_at.isoformat() if match.last_verified_at else None
            }

    try:
        result = async_to_sync(_fetch)()

        if not result:
            return jsonify({'error': 'Match not found'}), 404

        # 3. 写入 Redis 缓存（1天过期）
        try:
            redis_client.setex(
                cache_key,
                CACHE_TTL,
                json.dumps(result, ensure_ascii=False)
            )
            logger.info(f"Cache set for match {fixture_id}, TTL={CACHE_TTL}s")
        except Exception as e:
            logger.warning(f"Redis write error: {e}")

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error fetching match {fixture_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/matches')
def list_matches():
    """
    获取比赛列表
    
    Query Parameters:
        league_id: 联赛ID，默认 140 (可选)
        season: 赛季 (可选)
        date_from: 开始日期 UTC timestamp (可选)
        date_to: 结束日期 UTC timestamp (可选)
        team_id: 球队ID (可选)
        status: 比赛状态 (可选)
        has_channels: 是否有频道信息 true/false (可选)
        broadcast_status: 转播状态 matched/unmatched/missing_channels/ambiguous (可选)
        limit: 返回数量限制；不传时返回全部命中结果 (可选)
        offset: 偏移量，默认 0 (可选)
    
    Response:
        {
            "total": 150,
            "offset": 0,
            "limit": null,
            "matches": [...]
        }
    """
    async def _fetch():
        async with AsyncSessionLocal() as session:
            repo = MatchBroadcastRepository(session)
            filters = build_list_matches_filters(request.args)

            broadcast_status = filters['broadcast_status']
            status_enum = None
            if broadcast_status:
                from models import BroadcastMatchStatus
                status_enum = BroadcastMatchStatus(broadcast_status)

            matches = await repo.query_matches(
                league_id=filters['league_id'],
                season=filters['season'],
                date_from=filters['date_from'],
                date_to=filters['date_to'],
                team_id=filters['team_id'],
                status=filters['status'],
                has_channels=filters['has_channels'],
                broadcast_status=status_enum,
            )
            
            # 分页
            total = len(matches)
            offset = filters['offset']
            limit = filters['limit']
            if limit is not None:
                matches = matches[offset:offset + limit]
            elif offset:
                matches = matches[offset:]
            
            # 转换数据
            results = [serialize_match_list_item(match) for match in matches]
            
            return {
                'total': total,
                'offset': offset,
                'limit': limit,
                'filters': {
                    'league_id': filters['league_id'],
                    'season': filters['season'],
                    'date_from': filters['date_from'],
                    'date_to': filters['date_to'],
                    'team_id': filters['team_id'],
                    'status': filters['status'],
                    'has_channels': filters['has_channels'],
                    'broadcast_status': broadcast_status,
                    'default_league_id': DEFAULT_MATCH_LIST_LEAGUE_ID,
                },
                'matches': results
            }
    
    try:
        result = async_to_sync(_fetch)()
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error listing matches: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/leagues')
def list_leagues():
    """
    获取联赛列表
    
    Response:
        {
            "leagues": [
                {
                    "id": 1,
                    "api_league_id": 140,
                    "season": 2025,
                    "name": "La Liga",
                    "country": "Spain",
                    "enabled": true
                }
            ]
        }
    """
    async def _fetch():
        async with AsyncSessionLocal() as session:
            repo = LeagueConfigRepository(session)
            leagues = await repo.get_enabled_configs()
            
            return {
                'leagues': [
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
            }
    
    try:
        result = async_to_sync(_fetch)()
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error listing leagues: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/mismatches')
def list_mismatches():
    """
    获取未对齐/异常的比赛列表
    
    Query Parameters:
        league_id: 联赛ID (可选)
        season: 赛季 (可选)
        limit: 返回数量限制，默认 100 (可选)
        offset: 偏移量，默认 0 (可选)
    
    Response:
        {
            "total": 10,
            "offset": 0,
            "limit": 20,
            "mismatches": [...]
        }
    """
    async def _fetch():
        async with AsyncSessionLocal() as session:
            repo = MatchBroadcastRepository(session)
            
            league_id = request.args.get('league_id', type=int)
            season = request.args.get('season', type=int)
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            
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
                    'broadcast_match_status': match.broadcast_match_status.value if match.broadcast_match_status else None,
                    'matched_confidence': float(match.matched_confidence) if match.matched_confidence else None
                })
            
            return {
                'total': total,
                'offset': offset,
                'limit': limit,
                'mismatches': results
            }
    
    try:
        result = async_to_sync(_fetch)()
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error listing mismatches: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts')
def list_alerts():
    """
    获取告警列表
    
    Query Parameters:
        unresolved_only: 只显示未解决的 true/false，默认 true (可选)
        severity: 严重等级 critical/high/medium/low (可选)
        league_id: 联赛ID (可选)
        limit: 返回数量限制，默认 100 (可选)
        offset: 偏移量，默认 0 (可选)
    
    Response:
        {
            "total": 5,
            "alerts": [...]
        }
    """
    async def _fetch():
        async with AsyncSessionLocal() as session:
            repo = AlertLogRepository(session)
            
            unresolved_only = request.args.get('unresolved_only', 'true').lower() == 'true'
            severity = request.args.get('severity')
            league_id = request.args.get('league_id', type=int)
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            
            if unresolved_only:
                from models import Severity as SeverityEnum
                severity_enum = SeverityEnum(severity) if severity else None
                alerts = await repo.get_unresolved(severity=severity_enum)
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
                    'alert_type': alert.alert_type.value if alert.alert_type else None,
                    'severity': alert.severity.value if alert.severity else None,
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
            
            return {
                'total': total,
                'offset': offset,
                'limit': limit,
                'alerts': results
            }
    
    try:
        result = async_to_sync(_fetch)()
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error listing alerts: {e}")
        return jsonify({'error': str(e)}), 500


@app.errorhandler(404)
def not_found(error):
    """404 错误处理"""
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    """500 错误处理"""
    return jsonify({'error': 'Internal server error'}), 500


# ==================== 缓存手动管理接口 ====================

@app.route('/api/cache/match/<int:fixture_id>', methods=['GET'])
def get_match_cache(fixture_id: int):
    """
    手动查询比赛详情缓存

    GET /api/cache/match/12345

    Response:
        {
            "cached": true,
            "ttl_seconds": 85000,
            "data": { ... }
        }
        或
        {
            "cached": false
        }
    """
    cache_key = get_cache_key(fixture_id)

    try:
        # 检查缓存是否存在
        ttl = redis_client.ttl(cache_key)
        if ttl == -2:  # key 不存在
            return jsonify({'cached': False}), 200

        # 获取缓存数据
        cached_data = redis_client.get(cache_key)
        if cached_data:
            return jsonify({
                'cached': True,
                'ttl_seconds': ttl if ttl > 0 else None,
                'data': json.loads(cached_data)
            }), 200
        else:
            return jsonify({'cached': False}), 200

    except Exception as e:
        logger.error(f"Error checking cache for match {fixture_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/cache/match/<int:fixture_id>', methods=['DELETE'])
def delete_match_cache(fixture_id: int):
    """
    手动清除指定比赛的缓存

    DELETE /api/cache/match/12345

    Response:
        {
            "deleted": true,
            "message": "Cache cleared for match 12345"
        }
        或
        {
            "deleted": false,
            "message": "Cache key not found for match 12345"
        }
    """
    cache_key = get_cache_key(fixture_id)

    try:
        # 删除缓存
        deleted = redis_client.delete(cache_key)
        if deleted > 0:
            logger.info(f"Cache manually cleared for match {fixture_id}")
            return jsonify({
                'deleted': True,
                'message': f'Cache cleared for match {fixture_id}'
            }), 200
        else:
            return jsonify({
                'deleted': False,
                'message': f'Cache key not found for match {fixture_id}'
            }), 200

    except Exception as e:
        logger.error(f"Error clearing cache for match {fixture_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/cache/clear', methods=['POST'])
def clear_all_match_cache():
    """
    批量清除所有比赛详情缓存

    POST /api/cache/clear

    Response:
        {
            "cleared": true,
            "deleted_count": 15
        }
    """
    try:
        # 查找所有比赛详情缓存
        pattern = 'match:detail:*'
        cursor = 0
        deleted_count = 0

        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                deleted_count += redis_client.delete(*keys)
            if cursor == 0:
                break

        logger.info(f"All match cache manually cleared, deleted {deleted_count} keys")
        return jsonify({
            'cleared': True,
            'deleted_count': deleted_count
        }), 200

    except Exception as e:
        logger.error(f"Error clearing all match cache: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # 初始化数据库
    async_to_sync(init_db)()
    
    # 运行 Flask 应用
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('API_PORT', 5000)),
        debug=os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    )
