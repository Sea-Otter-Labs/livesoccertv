import logging
from datetime import datetime
from typing import Optional
import sys
import os
import traceback
import asyncio
import random

from crawler.items import CaptchaDetectedItem, CrawlTaskItem, LiveSoccerTVMatchItem

workspace_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from config.database import AsyncSessionLocal
from repo.alert_log_repo import AlertLogRepository
from repo.crawl_task_status_repo import CrawlTaskStatusRepository
from repo.web_crawl_raw_repo import WebCrawlRawRepository

# 引入降级告警
from crawler.pipelines.fallback_alerts import send_db_connection_alert, send_db_recovery_alert

logger = logging.getLogger(__name__)


class MatchDataPipeline:
    """
    比赛数据处理管道
    将抓取的数据存储到数据库
    
    改进点:
    1. 区分死锁错误和连接错误，分别处理
    2. 连接错误使用指数退避重试
    3. 增加显式 rollback 避免会话污染
    4. 数据库不可用时启用降级告警（飞书/本地日志）
    5. 连接恢复后发送恢复通知
    """

    @classmethod
    def from_crawler(cls, crawler):
        logger.info("[PIPELINE] Creating pipeline from crawler...")
        pipeline = cls()
        pipeline.crawler = crawler
        logger.info("[PIPELINE] Pipeline created successfully")
        return pipeline

    def __init__(self):
        self.batch_stats = {}
        
        # 死锁重试配置
        self.max_deadlock_retries = 3
        self.deadlock_retry_delay = 1  # 秒
        
        # 连接错误重试配置（指数退避）
        self.max_connection_retries = 5
        self.connection_base_delay = 1.0  # 基础延迟（秒）
        self.connection_max_delay = 30.0  # 最大延迟（秒）
        self.connection_jitter = 0.1  # 抖动比例
        
        # 连接失败追踪（用于恢复告警）
        self._db_failure_start_time: Optional[datetime] = None
        self._db_failure_count = 0
        self._last_alert_league = None
        self._last_alert_host = None
        
        logger.info(
            f"[PIPELINE] MatchDataPipeline initialized "
            f"(deadlock_retries={self.max_deadlock_retries}, "
            f"connection_retries={self.max_connection_retries})"
        )

    def open_spider(self, spider):
        """Spider 启动时初始化"""
        logger.info(f"[PIPELINE] Pipeline opened for spider: {spider.name}")
        logger.info(f"[PIPELINE] Spider batch_id: {getattr(spider, 'crawl_batch_id', 'N/A')}")
        logger.info(f"[PIPELINE] Spider league: {getattr(spider, 'league_name', 'N/A')}")
        self.batch_stats[spider.name] = {
            'matches_saved': 0,
            'errors': 0,
            'connection_failures': 0,
            'deadlock_retries': 0
        }
        self._db_failure_start_time = None
        self._db_failure_count = 0
        logger.info("[PIPELINE] Stats initialized")

    def close_spider(self, spider):
        """Spider 关闭时输出统计"""
        stats = self.batch_stats.get(spider.name, {})
        logger.info(f"[PIPELINE] Pipeline closing for spider: {spider.name}")
        logger.info(
            f"[PIPELINE] Final stats - Matches saved: {stats.get('matches_saved', 0)}, "
            f"Errors: {stats.get('errors', 0)}, "
            f"Connection failures: {stats.get('connection_failures', 0)}, "
            f"Deadlock retries: {stats.get('deadlock_retries', 0)}"
        )
        logger.info("[PIPELINE] Pipeline closed")

    async def process_item(self, item, spider):
        """处理 Item"""
        item_type = type(item).__name__

        if isinstance(item, LiveSoccerTVMatchItem):
            logger.debug(f"[PIPELINE] Processing LiveSoccerTVMatchItem: {item.get('home_team_name_raw')} vs {item.get('away_team_name_raw')}")
            return await self._process_match_item(item, spider)
        elif isinstance(item, CrawlTaskItem):
            logger.debug(f"[PIPELINE] Processing CrawlTaskItem: phase={item.get('task_phase')}, status={item.get('status')}")
            return await self._process_task_item(item, spider)
        elif isinstance(item, CaptchaDetectedItem):
            logger.debug(f"[PIPELINE] Processing CaptchaDetectedItem")
            return await self._process_captcha_item(item, spider)

        logger.warning(f"[PIPELINE] Unknown item type: {item_type}, returning without processing")
        return item

    async def _process_match_item(self, item, spider):
        """处理比赛数据 Item"""
        home_team = item.get('home_team_name_raw', 'N/A')
        away_team = item.get('away_team_name_raw', 'N/A')
        league_id = item.get('league_config_id', 'N/A')

        try:
            await self._save_match_with_retry(item)
            
            # 检查是否需要发送恢复告警
            if self._db_failure_start_time is not None:
                await self._check_and_send_recovery_alert(league_id)
            
            self.batch_stats[spider.name]['matches_saved'] += 1
            logger.info(f"[MATCH] Saved OK: {home_team} vs {away_team} (league_id={league_id})")

        except Exception as e:
            self.batch_stats[spider.name]['errors'] += 1
            logger.error(f"[MATCH] FAILED: {home_team} vs {away_team} (league_id={league_id})")
            logger.error(f"[MATCH] Error: {e}")
            logger.error(f"[MATCH] Traceback: {traceback.format_exc()}")
            
            # 如果是连接错误，发送降级告警
            if self._is_retryable_db_error(e):
                self.batch_stats[spider.name]['connection_failures'] += 1
                await self._send_connection_failure_alert(e, league_id, home_team, away_team)

        return item

    def _is_deadlock_error(self, e):
        """检查是否是数据库死锁错误"""
        error_str = str(e)
        # MySQL 死锁错误码
        return '1213' in error_str or 'Deadlock found' in error_str or 'Lock wait timeout' in error_str

    def _is_connection_error(self, e):
        """检查是否是数据库连接错误"""
        error_str = str(e).lower()
        connection_indicators = [
            "can't connect to mysql server",
            "2003",  # 连接错误码
            "2006",  # 服务器断开
            "2013",  # 查询过程中丢失连接
            "timeouterror",
            "10060",  # Windows 连接超时
            "10061",  # 连接被拒绝
            "connection refused",
            "network is unreachable",
            "no route to host",
            "broken pipe",
            "connection reset by peer",
            "operationalerror"
        ]
        return any(indicator in error_str for indicator in connection_indicators)

    def _is_retryable_db_error(self, e):
        """检查是否是可重试的数据库错误（死锁或连接错误）"""
        return self._is_deadlock_error(e) or self._is_connection_error(e)

    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        计算指数退避延迟
        delay = min(base * 2^attempt + jitter, max_delay)
        """
        exponential_delay = self.connection_base_delay * (2 ** (attempt - 1))
        jitter = random.uniform(0, exponential_delay * self.connection_jitter)
        delay = min(exponential_delay + jitter, self.connection_max_delay)
        return delay

    async def _check_and_send_recovery_alert(self, league_id: str):
        """检查并发送数据库恢复告警"""
        if self._db_failure_start_time is None:
            return
        
        downtime = (datetime.now() - self._db_failure_start_time).total_seconds()
        
        # 只有当故障持续超过5秒才发送恢复告警（避免短时抖动）
        if downtime > 5:
            try:
                await send_db_recovery_alert(
                    league_id=str(league_id),
                    host=self._last_alert_host or 'unknown',
                    downtime_seconds=downtime
                )
                logger.info(f"[DB_RECOVERY] Sent recovery alert after {downtime:.1f}s downtime")
            except Exception as e:
                logger.warning(f"[DB_RECOVERY] Failed to send recovery alert: {e}")
        
        # 重置故障追踪
        self._db_failure_start_time = None
        self._db_failure_count = 0

    async def _send_connection_failure_alert(
        self, 
        error: Exception, 
        league_id: str, 
        home_team: str, 
        away_team: str
    ):
        """发送连接失败降级告警"""
        error_msg = str(error)
        host = 'unknown'
        
        # 尝试从错误信息中提取主机地址
        if 'pplivedatabase' in error_msg:
            host = 'pplivedatabase.cn4csgi60ope.eu-west-3.rds.amazonaws.com'
        
        # 记录首次故障时间
        if self._db_failure_start_time is None:
            self._db_failure_start_time = datetime.now()
        
        self._db_failure_count += 1
        self._last_alert_league = str(league_id)
        self._last_alert_host = host
        
        try:
            await send_db_connection_alert(
                league_id=str(league_id),
                host=host,
                error_msg=error_msg[:500],  # 限制长度
                retry_count=self.max_connection_retries,
                match_info=f"{home_team} vs {away_team}"
            )
        except Exception as e:
            # 降级告警也失败，至少确保本地日志已记录
            logger.error(f"[FALLBACK_ALERT_FAILED] Could not send fallback alert: {e}")

    async def _save_match_with_retry(self, item: LiveSoccerTVMatchItem):
        """
        保存比赛数据，带智能重试机制
        
        区分处理:
        - 死锁错误: 固定延迟重试
        - 连接错误: 指数退避重试
        """
        home_team = item.get('home_team_name_raw', 'N/A')
        away_team = item.get('away_team_name_raw', 'N/A')
        league_id = item.get('league_config_id', 'N/A')
        batch_id = item.get('crawl_batch_id', 'N/A')

        max_retries = max(self.max_deadlock_retries, self.max_connection_retries)
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"[DB] Saving match (attempt {attempt}/{max_retries}): "
                    f"{home_team} vs {away_team}"
                )
                await self._save_match(item)
                
                # 如果有连接错误重试成功，记录统计
                if attempt > 1 and self._db_failure_start_time:
                    logger.info(f"[DB] Connection recovered after {attempt} attempts")
                
                return  # 成功，直接返回

            except Exception as e:
                is_deadlock = self._is_deadlock_error(e)
                is_connection = self._is_connection_error(e)
                
                if is_deadlock and attempt < self.max_deadlock_retries:
                    # 死锁错误: 固定延迟重试
                    self.batch_stats.get(self.crawler.spider.name, {}).get('deadlock_retries', 0)
                    logger.warning(
                        f"[DB] Deadlock detected (attempt {attempt}/{self.max_deadlock_retries}), "
                        f"retrying in {self.deadlock_retry_delay}s... "
                        f"[league_id={league_id}, batch={batch_id}, match={home_team} vs {away_team}]"
                    )
                    await asyncio.sleep(self.deadlock_retry_delay)
                    continue
                    
                elif is_connection and attempt < self.max_connection_retries:
                    # 连接错误: 指数退避重试
                    delay = self._calculate_backoff_delay(attempt)
                    logger.warning(
                        f"[DB] Connection error (attempt {attempt}/{self.max_connection_retries}), "
                        f"retrying in {delay:.1f}s... "
                        f"[league_id={league_id}, batch={batch_id}, match={home_team} vs {away_team}] "
                        f"Error: {str(e)[:200]}"
                    )
                    await asyncio.sleep(delay)
                    continue
                    
                else:
                    # 非可重试错误或已达到最大重试次数
                    logger.error(
                        f"[DB] Final attempt failed after {attempt} retries. "
                        f"Error type: {'deadlock' if is_deadlock else 'connection' if is_connection else 'other'}"
                    )
                    raise

    async def _save_match(self, item: LiveSoccerTVMatchItem):
        """
        异步保存/更新比赛数据到数据库 (Upsert)
        
        增加显式错误处理和 rollback，避免会话污染
        """
        session = None
        try:
            async with AsyncSessionLocal() as session:
                try:
                    repo = WebCrawlRawRepository(session)

                    # 计算 match_date
                    match_timestamp = item.get('match_timestamp_utc')
                    match_date = None
                    if match_timestamp:
                        match_date = datetime.utcfromtimestamp(match_timestamp).date()

                    data = {
                        'crawl_batch_id': item.get('crawl_batch_id'),
                        'source_site': item.get('source_site', 'livesoccertv'),
                        'league_config_id': item.get('league_config_id'),
                        'league_name': item.get('league_name'),
                        'match_date_text': item.get('match_date_text'),
                        'match_timestamp_utc': match_timestamp,
                        'match_date': match_date,
                        'home_team_name_raw': item.get('home_team_name_raw'),
                        'home_team_name_normalized': item.get('home_team_name_normalized'),
                        'away_team_name_raw': item.get('away_team_name_raw'),
                        'away_team_name_normalized': item.get('away_team_name_normalized'),
                        'channel_list': item.get('channel_list'),
                        'pagination_cursor': item.get('pagination_cursor'),
                        'source_match_text': item.get('source_match_text'),
                        'page_url': item.get('page_url'),
                        'crawled_at': item.get('crawled_at', datetime.utcnow())
                    }

                    # 使用 upsert：存在则更新，不存在则插入
                    await repo.upsert_match(data)
                    await session.commit()
                    
                except Exception as e:
                    # 发生错误时显式回滚
                    if session:
                        try:
                            await session.rollback()
                            logger.debug(f"[DB] Session rolled back due to: {type(e).__name__}")
                        except Exception as rollback_error:
                            logger.warning(f"[DB] Rollback failed: {rollback_error}")
                    raise
                    
        except Exception as e:
            # 重新抛出，让上层处理
            logger.debug(f"[DB] Save failed: {type(e).__name__}: {str(e)[:200]}")
            raise

    async def _process_task_item(self, item, spider):
        """处理任务状态 Item"""
        batch_id = item.get('crawl_batch_id', 'N/A')
        league_id = item.get('league_config_id', 'N/A')
        phase = item.get('task_phase', 'N/A')

        try:
            await self._update_task_status(item)
            logger.debug(f"[TASK] Updated OK: phase={phase}, batch={batch_id}")
        except Exception as e:
            logger.error(f"[TASK] FAILED: phase={phase}, batch={batch_id}, league={league_id}")
            logger.error(f"[TASK] Error: {e}")
            logger.error(f"[TASK] Traceback: {traceback.format_exc()}")

        return item

    async def _update_task_status(self, item: CrawlTaskItem):
        """异步更新任务状态（带错误处理和 rollback）"""
        async with AsyncSessionLocal() as session:
            try:
                repo = CrawlTaskStatusRepository(session)

                # 查找现有任务
                existing = await repo.get_by_batch_and_league(
                    item.get('crawl_batch_id'),
                    item.get('league_config_id')
                )

                if existing:
                    update_data = {
                        'task_phase': item.get('task_phase'),
                        'status': item.get('status'),
                        'current_pagination_cursor': item.get('current_pagination_cursor'),
                        'pagination_direction': item.get('pagination_direction'),
                        'matches_crawled': item.get('matches_crawled'),
                    }

                    if item.get('error_message'):
                        update_data['error_message'] = item.get('error_message')

                    await repo.update(existing.id, update_data)
                else:
                    # 创建新任务
                    await repo.create({
                        'crawl_batch_id': item.get('crawl_batch_id'),
                        'league_config_id': item.get('league_config_id'),
                        'task_phase': item.get('task_phase'),
                        'status': item.get('status'),
                        'current_pagination_cursor': item.get('current_pagination_cursor'),
                        'pagination_direction': item.get('pagination_direction'),
                        'matches_crawled': item.get('matches_crawled'),
                        'error_message': item.get('error_message'),
                    })

                await session.commit()
                
            except Exception as e:
                await session.rollback()
                raise

    async def _process_captcha_item(self, item, spider):
        """处理验证码检测 Item"""
        league_id = item.get('league_config_id', 'N/A')

        try:
            await self._save_captcha_alert(item)
            logger.info(f"[CAPTCHA] Saved alert OK: league_id={league_id}")
        except Exception as e:
            logger.error(f"[CAPTCHA] FAILED: league_id={league_id}")
            logger.error(f"[CAPTCHA] Error: {e}")
            logger.error(f"[CAPTCHA] Traceback: {traceback.format_exc()}")

        return item

    async def _save_captcha_alert(self, item: CaptchaDetectedItem):
        """异步保存验证码告警（带错误处理和 rollback）"""
        async with AsyncSessionLocal() as session:
            try:
                repo = AlertLogRepository(session)

                await repo.create({
                    'alert_type': 'captcha_blocked',
                    'severity': 'high',
                    'league_config_id': item.get('league_config_id'),
                    'exception_summary': f"检测到验证码: {item.get('captcha_type')}",
                    'suggested_action': '请人工处理验证码后继续',
                    'is_resolved': False,
                })

                await session.commit()
                
            except Exception as e:
                await session.rollback()
                raise
