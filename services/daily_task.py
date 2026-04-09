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
from config.settings import LARK_WEBHOOK_URL, LARK_SECRET, ALERT_ENABLED
from services.lark_notifier import AlertNotifier
from services.team_name_resolution import TeamNameResolutionService
from repo.team_name_mapping_repo import TeamNameMappingRepository
from utils.match_aligner import MatchAligner, MATCHED, UNMATCHED, AMBIGUOUS, MISSING_CHANNELS

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
        skip_web_crawl: bool = True,  # 默认跳过网页抓取，改为手动运行
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
        else:
            logger.info("=" * 60)
            logger.info("Phase 2: LiveSoccerTV Web Crawl (SKIPPED)")
            logger.info("Note: Please run crawler manually if needed:")
            logger.info("  python livesoccertv_crawler/run_crawler_cli.py")
            logger.info("=" * 60)
            results['web_crawl'] = {'status': 'skipped', 'message': 'Manual run required'}
        
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
            full_sync=True  # 增量同步
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
        启动 Scrapy 爬虫抓取 LiveSoccerTV 数据
        注意：此方法默认不执行，网页抓取已改为手动运行
        如需自动运行，请设置 skip_web_crawl=False
        """
        logger.warning("Web crawl is now manual-only. Please run separately:")
        logger.warning("  python livesoccertv_crawler/run_crawler_cli.py")
        return {'status': 'manual', 'message': 'Please run crawler manually'}  
    
    # 旧的自动爬虫代码已移除，如需恢复请查看 git history
    
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
        total_web_unmatched = 0
        total_missing_channels = 0
        
        for league in leagues:
            if not league:
                continue
            
            try:
                # 获取该联赛需要处理的 API 比赛（全量查询）
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
                
                # 执行对齐（双向核对）
                aligned_count, unmatched_count, ambiguous_count, web_unmatched_count, missing_channels_count = await self._align_league(
                    session=session,
                    league=league,
                    api_fixtures=api_fixtures,
                    web_data=web_data
                )
                
                total_aligned += aligned_count
                total_unmatched += unmatched_count
                total_ambiguous += ambiguous_count
                total_web_unmatched += web_unmatched_count
                total_missing_channels += missing_channels_count
                
            except Exception as e:
                logger.error(f"Alignment failed for {league.league_name}: {e}")
                # 回滚当前事务，避免污染后续联赛处理
                await session.rollback()
        
        return {
            'total_aligned': total_aligned,
            'total_unmatched': total_unmatched,
            'total_ambiguous': total_ambiguous,
            'total_web_unmatched': total_web_unmatched,
            'total_missing_channels': total_missing_channels
        }
    
    async def _align_league(
        self,
        session: AsyncSession,
        league: Any,
        api_fixtures: List[Any],
        web_data: List[Any]
    ) -> tuple:
        """
        对齐单个联赛的数据，执行双向核对
        
        对API -> web: 每条API比赛都会产生一个对齐结果
        对web -> API: 未被使用的web记录会输出error log
        
        Returns:
            (对齐成功数, 未匹配数, 歧义数, web未匹配数)
        """
        broadcast_repo = MatchBroadcastRepository(session)
        alert_repo = AlertLogRepository(session)
        
        # 初始化飞书通知器
        # lark_notifier = None
        # if ALERT_ENABLED and LARK_WEBHOOK_URL:
        #     lark_notifier = AlertNotifier(webhook_url=LARK_WEBHOOK_URL, secret=LARK_SECRET or None)
        
        aligned = 0
        unmatched = 0
        ambiguous = 0
        missing_channels_count = 0
        
        # 转换数据格式
        api_list = [
            {
                'fixture_id': f.fixture_id,
                'league_id': f.league_id,
                'season': f.season,
                'match_timestamp_utc': f.match_timestamp_utc,
                'home_team_name': f.home_team_name,
                'home_team_id': f.home_team_id,
                'away_team_name': f.away_team_name,
                'away_team_id': f.away_team_id,
                'status': f.status
            }
            for f in api_fixtures
        ]
        
        # 创建 fixture_id 到 ApiFixture 对象的映射，用于后续查找 team_id
        fixture_map = {f.fixture_id: f for f in api_fixtures}
        
        # 创建web数据id到原始对象的映射
        web_map = {w.id: w for w in web_data}
        
        # 使用 async with 上下文管理器初始化 API 客户端
        from services.api_football_client import ApiFootballClient
        async with ApiFootballClient(self.api_key) as api_client:
            mapping_repo = TeamNameMappingRepository(session)
            team_resolution_service = TeamNameResolutionService(api_client, mapping_repo)
            
            # 解析网页数据的球队名称
            logger.info(f"Resolving team names for {len(web_data)} web records...")
            web_list = []
            for w in web_data:
                resolved = await team_resolution_service.resolve_teams_for_web_match(
                    session,
                    league_id=league.api_league_id,
                    season=league.api_season,
                    home_team_raw=w.home_team_name_raw,
                    away_team_raw=w.away_team_name_raw
                )
                
                if resolved['home_team_id'] is None:
                    logger.warning(
                        f"Team resolution failed for home team - "
                        f"league: {league.league_name}, web_id: {w.id}, "
                        f"raw_name: '{w.home_team_name_raw}'"
                    )
                if resolved['away_team_id'] is None:
                    logger.warning(
                        f"Team resolution failed for away team - "
                        f"league: {league.league_name}, web_id: {w.id}, "
                        f"raw_name: '{w.away_team_name_raw}'"
                    )
                
                web_list.append({
                    'id': w.id,
                    'league_config_id': w.league_config_id,
                    'match_timestamp_utc': w.match_timestamp_utc,
                    'home_team_name_raw': w.home_team_name_raw,
                    'home_team_name_normalized': w.home_team_name_normalized,
                    'away_team_name_raw': w.away_team_name_raw,
                    'away_team_name_normalized': w.away_team_name_normalized,
                    'channel_list': w.channel_list,
                    'resolved_home_team_id': resolved['home_team_id'],
                    'resolved_away_team_id': resolved['away_team_id']
                })
            
            logger.info(f"Team name resolution completed for {league.league_name}")
            
            # 使用对齐器进行对齐（双向核对）
            aligner = MatchAligner(time_tolerance_hours=4.0)
            alignments, unused_web = aligner.align_batch(api_list, web_list)
            
            # 处理API -> web的匹配结果
            for alignment in alignments:
                # 检查是否已存在
                existing = await broadcast_repo.get_by_fixture_id(alignment.fixture_id)
                
                # 通过映射获取原始的 ApiFixture 对象以获取基础字段
                original_fixture = fixture_map.get(alignment.fixture_id)
                if original_fixture is None:
                    logger.warning(
                        f"Fixture {alignment.fixture_id} not found in fixture_map, "
                        f"using alignment fallback data"
                    )
                
                if alignment.result == MATCHED:
                    data = {
                        'fixture_id': alignment.fixture_id,
                        'league_id': league.api_league_id,
                        'season': league.api_season,
                        'match_timestamp_utc': original_fixture.match_timestamp_utc if original_fixture else alignment.match_timestamp_utc,
                        'match_date': datetime.fromtimestamp(original_fixture.match_timestamp_utc).date() if original_fixture and original_fixture.match_timestamp_utc else (datetime.fromtimestamp(alignment.match_timestamp_utc).date() if alignment.match_timestamp_utc else None),
                        'home_team_id': original_fixture.home_team_id if original_fixture else 0,
                        'home_team_name': original_fixture.home_team_name if original_fixture else alignment.home_team_name,
                        'away_team_id': original_fixture.away_team_id if original_fixture else 0,
                        'away_team_name': original_fixture.away_team_name if original_fixture else alignment.away_team_name,
                        'broadcast_match_status': 'matched',
                        'matched_confidence': alignment.confidence,
                        'web_crawl_raw_id': alignment.web_crawl_raw_id,
                        'channels': alignment.channels,
                        'last_verified_at': datetime.utcnow()
                    }
                    
                    try:
                        await broadcast_repo.upsert_by_fixture_id(data)
                        aligned += 1
                    except Exception as e:
                        logger.error(
                            f"Failed to upsert matched fixture {alignment.fixture_id}: {e}"
                        )
                    
                elif alignment.result == MISSING_CHANNELS:
                    data = {
                        'fixture_id': alignment.fixture_id,
                        'league_id': league.api_league_id,
                        'season': league.api_season,
                        'match_timestamp_utc': original_fixture.match_timestamp_utc if original_fixture else alignment.match_timestamp_utc,
                        'match_date': datetime.fromtimestamp(original_fixture.match_timestamp_utc).date() if original_fixture and original_fixture.match_timestamp_utc else (datetime.fromtimestamp(alignment.match_timestamp_utc).date() if alignment.match_timestamp_utc else None),
                        'home_team_id': original_fixture.home_team_id if original_fixture else 0,
                        'home_team_name': original_fixture.home_team_name if original_fixture else alignment.home_team_name,
                        'away_team_id': original_fixture.away_team_id if original_fixture else 0,
                        'away_team_name': original_fixture.away_team_name if original_fixture else alignment.away_team_name,
                        'broadcast_match_status': 'missing_channels',
                        'matched_confidence': alignment.confidence,
                        'web_crawl_raw_id': alignment.web_crawl_raw_id,
                        'channels': None,
                        'last_verified_at': datetime.utcnow()
                    }
                    
                    try:
                        await broadcast_repo.upsert_by_fixture_id(data)
                        missing_channels_count += 1
                        logger.warning(
                            f"Match {alignment.fixture_id} ({original_fixture.home_team_name if original_fixture else alignment.home_team_name} "
                            f"vs {original_fixture.away_team_name if original_fixture else alignment.away_team_name}): "
                            f"aligned but missing channels, reason: {alignment.reason}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to upsert missing_channels fixture {alignment.fixture_id}: {e}"
                        )
                    
                elif alignment.result == UNMATCHED:
                    # 未匹配，创建未匹配记录
                    data = {
                        'fixture_id': alignment.fixture_id,
                        'league_id': league.api_league_id,
                        'season': league.api_season,
                        'match_timestamp_utc': original_fixture.match_timestamp_utc if original_fixture else alignment.match_timestamp_utc,
                        'match_date': datetime.fromtimestamp(original_fixture.match_timestamp_utc).date() if original_fixture and original_fixture.match_timestamp_utc else (datetime.fromtimestamp(alignment.match_timestamp_utc).date() if alignment.match_timestamp_utc else None),
                        'home_team_id': 0,
                        'home_team_name': original_fixture.home_team_name if original_fixture else alignment.home_team_name,
                        'away_team_id': 0,
                        'away_team_name': original_fixture.away_team_name if original_fixture else alignment.away_team_name,
                        'broadcast_match_status': 'unmatched',
                        'matched_confidence': 0.0,
                        'web_crawl_raw_id': alignment.web_crawl_raw_id,
                        'channels': None,
                        'last_verified_at': datetime.utcnow()
                    }

                    logger.warning(
                        f"Unmatched fixture {alignment.fixture_id} "
                        f"({original_fixture.home_team_name if original_fixture else alignment.home_team_name} "
                        f"vs {original_fixture.away_team_name if original_fixture else alignment.away_team_name}), "
                        f"reason: {alignment.reason}"
                    )

                    try:
                        await broadcast_repo.upsert_by_fixture_id(data)
                    except Exception as e:
                        logger.error(
                            f"Failed to upsert unmatched fixture {alignment.fixture_id}: {e}"
                        )

                    # 创建告警
                    alert_data = {
                        'alert_type': 'unmatched_api_to_web',
                        'severity': 'medium',
                        'league_id': league.api_league_id,
                        'league_name': league.league_name,
                        'season': league.api_season,
                        'fixture_id': alignment.fixture_id,
                        'match_timestamp_utc': original_fixture.match_timestamp_utc if original_fixture else alignment.match_timestamp_utc,
                        'home_team_name': original_fixture.home_team_name if original_fixture else alignment.home_team_name,
                        'away_team_name': original_fixture.away_team_name if original_fixture else alignment.away_team_name,
                        'exception_summary': alignment.reason or 'API 比赛无法在网页数据中找到匹配',
                        'suggested_action': '检查 LiveSoccerTV 页面是否有该比赛',
                        'is_resolved': False
                    }
                    alert_log = await alert_repo.create(alert_data)
                    
                    # # 发送飞书通知
                    # if lark_notifier and alert_log:
                    #     try:
                    #         notified = await lark_notifier.notify_alignment_failure(
                    #             alert_log=alert_log,
                    #             error_log=alignment.reason
                    #         )
                    #         if notified:
                    #             await alert_repo.mark_as_notified(
                    #                 alert_log.id,
                    #                 notification_response="飞书通知已发送"
                    #             )
                    #     except Exception as e:
                    #         logger.error(f"Failed to send Lark notification: {e}")
                    
                    unmatched += 1
                    
                elif alignment.result == AMBIGUOUS:
                    # 歧义匹配，创建告警
                    data = {
                        'fixture_id': alignment.fixture_id,
                        'league_id': league.api_league_id,
                        'season': league.api_season,
                        'match_timestamp_utc': original_fixture.match_timestamp_utc if original_fixture else alignment.match_timestamp_utc,
                        'match_date': datetime.fromtimestamp(original_fixture.match_timestamp_utc).date() if original_fixture and original_fixture.match_timestamp_utc else (datetime.fromtimestamp(alignment.match_timestamp_utc).date() if alignment.match_timestamp_utc else None),
                        'home_team_id': 0,
                        'home_team_name': original_fixture.home_team_name if original_fixture else alignment.home_team_name,
                        'away_team_id': 0,
                        'away_team_name': original_fixture.away_team_name if original_fixture else alignment.away_team_name,
                        'broadcast_match_status': 'ambiguous',
                        'matched_confidence': alignment.confidence,
                        'channels': None,
                        'last_verified_at': datetime.utcnow()
                    }

                    logger.warning(
                        f"Ambiguous fixture {alignment.fixture_id} "
                        f"({original_fixture.home_team_name if original_fixture else alignment.home_team_name} "
                        f"vs {original_fixture.away_team_name if original_fixture else alignment.away_team_name}), "
                        f"confidence: {alignment.confidence:.2f}, reason: {alignment.reason}"
                    )

                    try:
                        await broadcast_repo.upsert_by_fixture_id(data)
                    except Exception as e:
                        logger.error(
                            f"Failed to upsert ambiguous fixture {alignment.fixture_id}: {e}"
                        )

                    # 创建告警
                    alert_data = {
                        'alert_type': 'ambiguous_match',
                        'severity': 'high',
                        'league_id': league.api_league_id,
                        'league_name': league.league_name,
                        'season': league.api_season,
                        'fixture_id': alignment.fixture_id,
                        'match_timestamp_utc': original_fixture.match_timestamp_utc if original_fixture else alignment.match_timestamp_utc,
                        'home_team_name': original_fixture.home_team_name if original_fixture else alignment.home_team_name,
                        'away_team_name': original_fixture.away_team_name if original_fixture else alignment.away_team_name,
                        'exception_summary': alignment.reason or '存在多个匹配的候选',
                        'suggested_action': '需要人工确认正确匹配',
                        'is_resolved': False
                    }
                    alert_log = await alert_repo.create(alert_data)
                    
                    # 发送飞书通知
                    # if lark_notifier and alert_log:
                    #     try:
                    #         notified = await lark_notifier.notify_alignment_failure(
                    #             alert_log=alert_log,
                    #             error_log=alignment.reason
                    #         )
                    #         if notified:
                    #             await alert_repo.mark_as_notified(
                    #                 alert_log.id,
                    #                 notification_response="飞书通知已发送"
                    #             )
                    #     except Exception as e:
                    #         logger.error(f"Failed to send Lark notification: {e}")
                    
                    ambiguous += 1
            
            # 处理web -> API的未匹配：输出error log
            web_unmatched = len(unused_web)
            for web_record in unused_web:
                web_id = web_record.get('id')
                
                logger.error(
                    f"Web crawl record without API fixture match - "
                    f"league: {league.league_name}, "
                    f"web_id: {web_id}, "
                    f"timestamp: {web_record.get('match_timestamp_utc')}, "
                    f"home: {web_record.get('home_team_name_raw')}, "
                    f"away: {web_record.get('away_team_name_raw')}"
                )
            
            try:
                await session.commit()
            except Exception as e:
                logger.error(
                    f"Failed to commit alignment for {league.league_name}: {e}"
                )
                await session.rollback()
                raise
            
            logger.info(
                f"Alignment completed for {league.league_name}: "
                f"{aligned} aligned, {unmatched} unmatched, {ambiguous} ambiguous, "
                f"{missing_channels_count} missing_channels, {web_unmatched} web_unmatched"
            )
            
            return aligned, unmatched, ambiguous, web_unmatched, missing_channels_count


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
