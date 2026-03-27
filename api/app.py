import os
import logging
from datetime import datetime
from typing import Optional

from flask import Flask, jsonify, request
from asgiref.sync import async_to_sync

from config.database import AsyncSessionLocal, init_db, close_db
from repo import MatchBroadcastRepository, AlertLogRepository, LeagueConfigRepository

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


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
    获取指定比赛的详情和转播信息
    
    Path Parameters:
        fixture_id: API-Football fixture ID
    
    Response:
        {
            "fixture_id": 12345,
            "league_id": 140,
            "season": 2025,
            "match_timestamp_utc": 1704067200,
            "home_team": "Barcelona",
            "away_team": "Atletico Madrid",
            "status": "NS",
            "score": "0 - 0",
            "broadcast_match_status": "matched",
            "channels": [
                {
                    "name": "ESPN",
                    "country": "USA",
                    "type": "TV",
                    "is_streaming": false
                }
            ],
            "last_verified_at": "2024-01-01T12:00:00"
        }
    """
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
        
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error fetching match {fixture_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/matches')
def list_matches():
    """
    获取比赛列表
    
    Query Parameters:
        league_id: 联赛ID (可选)
        season: 赛季 (可选)
        date_from: 开始日期 UTC timestamp (可选)
        date_to: 结束日期 UTC timestamp (可选)
        team_id: 球队ID (可选)
        status: 比赛状态 (可选)
        has_channels: 是否有频道信息 true/false (可选)
        broadcast_status: 转播状态 matched/unmatched/missing_channels/ambiguous (可选)
        limit: 返回数量限制，默认 100 (可选)
        offset: 偏移量，默认 0 (可选)
    
    Response:
        {
            "total": 150,
            "offset": 0,
            "limit": 20,
            "matches": [...]
        }
    """
    async def _fetch():
        async with AsyncSessionLocal() as session:
            repo = MatchBroadcastRepository(session)
            
            # 解析查询参数
            league_id = request.args.get('league_id', type=int)
            season = request.args.get('season', type=int)
            date_from = request.args.get('date_from', type=int)
            date_to = request.args.get('date_to', type=int)
            team_id = request.args.get('team_id', type=int)
            status = request.args.get('status')
            has_channels = request.args.get('has_channels')
            broadcast_status = request.args.get('broadcast_status')
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            
            # 转换 has_channels
            if has_channels is not None:
                has_channels = has_channels.lower() == 'true'
            
            # 查询数据
            if date_from and date_to:
                matches = await repo.get_by_time_range(
                    start_timestamp=date_from,
                    end_timestamp=date_to,
                    league_id=league_id,
                    has_channels=has_channels
                )
            elif broadcast_status:
                from models import BroadcastMatchStatus
                status_enum = BroadcastMatchStatus(broadcast_status)
                matches = await repo.get_by_status(
                    status=status_enum,
                    league_id=league_id
                )
            else:
                matches = await repo.get_by_league_and_season(
                    league_id=league_id or 0,
                    season=season or 0
                )
            
            # 分页
            total = len(matches)
            matches = matches[offset:offset + limit]
            
            # 转换数据
            results = []
            for match in matches:
                # 提取频道名称并拼接
                channel_names = ''
                if match.channels:
                    names = [ch.get('name') for ch in match.channels if ch.get('name')]
                    channel_names = ','.join(names)

                results.append({
                    'fixture_id': match.fixture_id,
                    'league_id': match.league_id,
                    'season': match.season,
                    'match_timestamp_utc': match.match_timestamp_utc,
                    'match_date': match.match_date.isoformat() if match.match_date else None,
                    'home_team': match.home_team_name,
                    'away_team': match.away_team_name,
                    'status': match.match_status,
                    'broadcast_match_status': match.broadcast_match_status.value if match.broadcast_match_status else None,
                    'channels_count': len(match.channels) if match.channels else 0,
                    'channel_names': channel_names
                })
            
            return {
                'total': total,
                'offset': offset,
                'limit': limit,
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


if __name__ == '__main__':
    # 初始化数据库
    async_to_sync(init_db)()
    
    # 运行 Flask 应用
    app.run(
        host='0.0.0.0',
        port=int(os.getenv('API_PORT', 5000)),
        debug=os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    )
