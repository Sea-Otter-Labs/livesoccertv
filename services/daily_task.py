"""
每日任务协调器
整合 API 同步、网页抓取、比赛对齐的完整流程
"""

import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from config.database import AsyncSessionLocal, init_db, close_db
from services import ApiFootballSyncService
from repo import (
    LeagueConfigRepository,
    ApiFixtureRepository,
    WebCrawlRawRepository,
    MatchBroadcastRepository,
    AlertLogRepository,
)
from models import BroadcastMatchStatus, AlertType, Severity

logger = logging.getLogger(__name__)


class DailyTaskOrchestrator:
    """
    每日任务协调器
    按 spec 规范执行三段式任务：API 同步 -> 网页抓取 -> 数据对齐
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.sync_service: Optional[ApiFootballSyncService] = None
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.sync_service = ApiFootballSyncService(self.api_key)
        await self.sync_service.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.sync_service:
            await self.sync_service.__aexit__(exc_type, exc_val, exc_tb)
    
    async def execute_daily_task(
        self,
        session: AsyncSession,
        league_config_id: Optional[int] = None,
        skip_api_sync: bool = False,
        skip_web_crawl: bool = False,
        skip_alignment: bool = False
    ) -> Dict[str, Any]:
        """
        执行每日任务
        
        Args:
            session: 数据库会话
            league_config_id: 指定联赛配置ID，为None则处理所有启用的联赛
            skip_api_sync: 是否跳过 API 同步
            skip_web_crawl: 是否跳过网页抓取
            skip_alignment: 是否跳过比赛对齐
        
        Returns:
            任务执行结果统计
        """
        results = {
            'task_start_time': datetime.utcnow().isoformat(),
            'api_sync': {},
            'web_crawl': {},
            'alignment': {},
            'errors': []
        }
        
        # 阶段 1: API 同步
        if not skip_api_sync:
            logger.info("=" * 60)
            logger.info("Phase 1: API-Football Data Sync")
            logger.info("=" * 60)
            try:
                api_results = await self._phase_api_sync(session, league_config_id)
                results['api_sync'] = api_results
            except Exception as e:
                logger.error(f"API sync phase failed: {e}")
                results['errors'].append(f"API sync: {str(e)}")
        
        # 阶段 2: 网页抓取
        if not skip_web_crawl:
            logger.info("=" * 60)
            logger.info("Phase 2: LiveSoccerTV Web Crawl")
            logger.info("=" * 60)
            try:
                crawl_results = await self._phase_web_crawl(session, league_config_id)
                results['web_crawl'] = crawl_results
            except Exception as e:
                logger.error(f"Web crawl phase failed: {e}")
                results['errors'].append(f"Web crawl: {str(e)}")
        
        # 阶段 3: 比赛对齐
        if not skip_alignment:
            logger.info("=" * 60)
            logger.info("Phase 3: Match Alignment")
            logger.info("=" * 60)
            try:
                alignment_results = await self._phase_alignment(session, league_config_id)
                results['alignment'] = alignment_results
            except Exception as e:
                logger.error(f"Alignment phase failed: {e}")
                results['errors'].append(f"Alignment: {str(e)}")
        
        results['task_end_time'] = datetime.utcnow().isoformat()
        
        logger.info("=" * 60)
        logger.info("Daily Task Completed")
        logger.info("=" * 60)
        
        return results
    
    async def _phase_api_sync(
        self,
        session: AsyncSession,
        league_config_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        阶段 1: API 数据同步
        """
        results = await self.sync_service.sync_all_enabled_leagues(
            session=session,
            full_sync=False  # 增量同步
        )
        
        logger.info(f"API Sync Results:")
        logger.info(f"  Total leagues: {results['total_leagues']}")
        logger.info(f"  Total inserted: {results['total_inserted']}")
        logger.info(f"  Total updated: {results['total_updated']}")
        
        return results
    
    async def _phase_web_crawl(
        self,
        session: AsyncSession,
        league_config_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        阶段 2: 网页抓取
        启动 Scrapy 爬虫抓取 LiveSoccerTV 数据
        """
        from crawler.launcher import CrawlerLauncher
        
        launcher = CrawlerLauncher()
        
        if league_config_id:
            await launcher.run_single(league_config_id)
        else:
            await launcher.run_all()
        
        return {'status': 'completed'}
    
    async def _phase_alignment(
        self,
        session: AsyncSession,
        league_config_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        阶段 3: 比赛对齐
        将 API 数据和网页抓取数据进行对齐
        """
        # 获取联赛配置
        league_repo = LeagueConfigRepository(session)
        
        if league_config_id:
            leagues = [await league_repo.get_by_id(league_config_id)]
        else:
            leagues = await league_repo.get_enabled_configs()
        
        total_aligned = 0
        total_unmatched = 0
        total_ambiguous = 0
        
        for league in leagues:
            if not league:
                continue
            
            try:
                # 获取该联赛需要处理的 API 比赛
                api_repo = ApiFixtureRepository(session)
                api_fixtures = await api_repo.get_by_league_and_season(
                    league_id=league.api_league_id,
                    season=league.api_season
                )
                
                if not api_fixtures:
                    logger.info(f"No API fixtures found for {league.league_name}")
                    continue
                
                # 获取该联赛的网页抓取数据
                web_repo = WebCrawlRawRepository(session)
                # 获取最近批次的抓取数据
                web_data = await web_repo.get_by_league_config(league.id)
                
                logger.info(
                    f"Aligning {league.league_name}: "
                    f"{len(api_fixtures)} API fixtures, "
                    f"{len(web_data)} web crawls"
                )
                
                # 执行对齐
                aligned_count, unmatched_count, ambiguous_count = await self._align_league(
                    session=session,
                    league=league,
                    api_fixtures=api_fixtures,
                    web_data=web_data
                )
                
                total_aligned += aligned_count
                total_unmatched += unmatched_count
                total_ambiguous += ambiguous_count
                
            except Exception as e:
                logger.error(f"Alignment failed for {league.league_name}: {e}")
        
        return {
            'total_aligned': total_aligned,
            'total_unmatched': total_unmatched,
            'total_ambiguous': total_ambiguous
        }
    
    async def _align_league(
        self,
        session: AsyncSession,
        league: Any,
        api_fixtures: List[Any],
        web_data: List[Any]
    ) -> tuple:
        """
        对齐单个联赛的数据
        
        Returns:
            (对齐成功数, 未匹配数, 歧义数)
        """
        broadcast_repo = MatchBroadcastRepository(session)
        alert_repo = AlertLogRepository(session)
        
        aligned = 0
        unmatched = 0
        ambiguous = 0
        
        # 转换数据格式
        api_list = [
            {
                'fixture_id': f.fixture_id,
                'league_id': f.league_id,
                'season': f.season,
                'match_timestamp_utc': f.match_timestamp_utc,
                'home_team_name': f.home_team_name,
                'away_team_name': f.away_team_name,
                'status': f.status
            }
            for f in api_fixtures
        ]
        
        web_list = [
            {
                'id': w.id,
                'league_config_id': w.league_config_id,
                'match_timestamp_utc': w.match_timestamp_utc,
                'home_team_name_raw': w.home_team_name_raw,
                'home_team_name_normalized': w.home_team_name_normalized,
                'away_team_name_raw': w.away_team_name_raw,
                'away_team_name_normalized': w.away_team_name_normalized,
                'channel_list': w.channel_list
            }
            for w in web_data
        ]
        
        # 使用对齐器进行对齐
        from utils import align_matches
        alignments = align_matches(
            api_fixtures=api_list,
            web_crawls=web_list,
            time_tolerance_hours=4.0
        )
        
        for alignment in alignments:
            # 检查是否已存在
            existing = await broadcast_repo.get_by_fixture_id(alignment.fixture_id)
            
            if alignment.result.value == 'matched':
                # 对齐成功，保存到 match_broadcasts 表
                data = {
                    'fixture_id': alignment.fixture_id,
                    'league_id': league.api_league_id,
                    'season': league.api_season,
                    'match_timestamp_utc': alignment.match_timestamp_utc,
                    'match_date': datetime.fromtimestamp(alignment.match_timestamp_utc).date(),
                    'home_team_id': api_fixtures[0].home_team_id,  # 从原始数据获取
                    'home_team_name': alignment.home_team_name,
                    'away_team_id': api_fixtures[0].away_team_id,
                    'away_team_name': alignment.away_team_name,
                    'broadcast_match_status': BroadcastMatchStatus.MATCHED,
                    'matched_confidence': alignment.confidence,
                    'web_crawl_raw_id': alignment.web_crawl_raw_id,
                    'channels': alignment.channels,
                    'last_verified_at': datetime.utcnow()
                }
                
                if existing:
                    await broadcast_repo.update(existing.id, data)
                else:
                    await broadcast_repo.create(data)
                
                aligned += 1
                
            elif alignment.result.value == 'unmatched':
                # 未匹配，创建未匹配记录
                data = {
                    'fixture_id': alignment.fixture_id,
                    'league_id': league.api_league_id,
                    'season': league.api_season,
                    'match_timestamp_utc': alignment.match_timestamp_utc,
                    'match_date': datetime.fromtimestamp(alignment.match_timestamp_utc).date() if alignment.match_timestamp_utc else None,
                    'home_team_id': 0,
                    'home_team_name': alignment.home_team_name,
                    'away_team_id': 0,
                    'away_team_name': alignment.away_team_name,
                    'broadcast_match_status': BroadcastMatchStatus.UNMATCHED,
                    'matched_confidence': 0.0,
                    'channels': None,
                    'last_verified_at': datetime.utcnow()
                }
                
                if existing:
                    await broadcast_repo.update(existing.id, data)
                else:
                    await broadcast_repo.create(data)
                
                # 创建告警
                await alert_repo.create({
                    'alert_type': AlertType.UNMATCHED_API_TO_WEB,
                    'severity': Severity.MEDIUM,
                    'league_id': league.api_league_id,
                    'league_name': league.league_name,
                    'season': league.api_season,
                    'fixture_id': alignment.fixture_id,
                    'match_timestamp_utc': alignment.match_timestamp_utc,
                    'home_team_name': alignment.home_team_name,
                    'away_team_name': alignment.away_team_name,
                    'exception_summary': alignment.reason or 'API 比赛无法在网页数据中找到匹配',
                    'suggested_action': '检查 LiveSoccerTV 页面是否有该比赛',
                    'is_resolved': False
                })
                
                unmatched += 1
                
            elif alignment.result.value == 'ambiguous':
                # 歧义匹配，创建告警
                data = {
                    'fixture_id': alignment.fixture_id,
                    'league_id': league.api_league_id,
                    'season': league.api_season,
                    'match_timestamp_utc': alignment.match_timestamp_utc,
                    'match_date': datetime.fromtimestamp(alignment.match_timestamp_utc).date() if alignment.match_timestamp_utc else None,
                    'home_team_id': 0,
                    'home_team_name': alignment.home_team_name,
                    'away_team_id': 0,
                    'away_team_name': alignment.away_team_name,
                    'broadcast_match_status': BroadcastMatchStatus.AMBIGUOUS,
                    'matched_confidence': alignment.confidence,
                    'channels': None,
                    'last_verified_at': datetime.utcnow()
                }
                
                if existing:
                    await broadcast_repo.update(existing.id, data)
                else:
                    await broadcast_repo.create(data)
                
                # 创建告警
                await alert_repo.create({
                    'alert_type': AlertType.AMBIGUOUS_MATCH,
                    'severity': Severity.HIGH,
                    'league_id': league.api_league_id,
                    'league_name': league.league_name,
                    'season': league.api_season,
                    'fixture_id': alignment.fixture_id,
                    'match_timestamp_utc': alignment.match_timestamp_utc,
                    'home_team_name': alignment.home_team_name,
                    'away_team_name': alignment.away_team_name,
                    'exception_summary': alignment.reason or '存在多个匹配的候选',
                    'suggested_action': '需要人工确认正确匹配',
                    'is_resolved': False
                })
                
                ambiguous += 1
        
        await session.commit()
        
        logger.info(
            f"Alignment completed for {league.league_name}: "
            f"{aligned} aligned, {unmatched} unmatched, {ambiguous} ambiguous"
        )
        
        return aligned, unmatched, ambiguous


async def run_daily_task(api_key: str, league_config_id: Optional[int] = None):
    """
    便捷函数：运行每日任务
    
    Args:
        api_key: API-Football API 密钥
        league_config_id: 指定联赛配置ID，为None则处理所有启用的联赛
    """
    logger.info("Starting daily task...")
    
    # 初始化数据库
    await init_db()
    
    try:
        async with AsyncSessionLocal() as session:
            async with DailyTaskOrchestrator(api_key) as orchestrator:
                results = await orchestrator.execute_daily_task(
                    session=session,
                    league_config_id=league_config_id
                )
                
                logger.info("Daily task results:")
                logger.info(f"  API Sync: {results['api_sync']}")
                logger.info(f"  Web Crawl: {results['web_crawl']}")
                logger.info(f"  Alignment: {results['alignment']}")
                
                if results['errors']:
                    logger.error(f"  Errors: {results['errors']}")
                
                return results
    
    except Exception as e:
        logger.error(f"Daily task failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        await close_db()
