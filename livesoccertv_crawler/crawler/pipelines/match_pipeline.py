import logging
import asyncio
from datetime import datetime
import sys
import os

workspace_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from config.database import AsyncSessionLocal
from crawler.items import LiveSoccerTVMatchItem, CrawlTaskItem, CaptchaDetectedItem
from models import AlertType, Severity
from repo.alert_log_repo import AlertLogRepository
from repo.crawl_task_status_repo import CrawlTaskStatusRepository
from repo.web_crawl_raw_repo import WebCrawlRawRepository

logger = logging.getLogger(__name__)


class MatchDataPipeline:
    """
    比赛数据处理管道
    将抓取的数据存储到数据库
    """
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline
    
    def __init__(self):
        self.batch_stats = {}
    
    def open_spider(self):
        """Spider 启动时初始化"""
        spider = self.crawler.spider
        logger.info(f"Pipeline opened for spider: {spider.name}")
        self.batch_stats[spider.name] = {
            'matches_saved': 0,
            'errors': 0
        }
    
    def close_spider(self):
        """Spider 关闭时输出统计"""
        spider = self.crawler.spider
        stats = self.batch_stats.get(spider.name, {})
        logger.info(
            f"Pipeline closed for spider: {spider.name}. "
            f"Matches saved: {stats.get('matches_saved', 0)}, "
            f"Errors: {stats.get('errors', 0)}"
        )
    
    async def process_item(self, item):
        """处理 Item"""
        if isinstance(item, LiveSoccerTVMatchItem):
            return await self._process_match_item(item)
        elif isinstance(item, CrawlTaskItem):
            return await self._process_task_item(item)
        elif isinstance(item, CaptchaDetectedItem):
            return await self._process_captcha_item(item)
        
        return item
    
    async def _process_match_item(self, item):
        """处理比赛数据 Item"""
        spider = self.crawler.spider
        try:
            await self._save_match(item)
            
            self.batch_stats[spider.name]['matches_saved'] += 1
            logger.debug(f"Saved match: {item.get('home_team_name_raw')} vs {item.get('away_team_name_raw')}")
            
        except Exception as e:
            self.batch_stats[spider.name]['errors'] += 1
            logger.error(f"Error saving match: {e}")
        
        return item
    
    async def _save_match(self, item: LiveSoccerTVMatchItem):
        """异步保存比赛数据到数据库"""
        async with AsyncSessionLocal() as session:
            repo = WebCrawlRawRepository(session)
            
            data = {
                'crawl_batch_id': item.get('crawl_batch_id'),
                'source_site': item.get('source_site', 'livesoccertv'),
                'league_config_id': item.get('league_config_id'),
                'league_name': item.get('league_name'),
                'match_date_text': item.get('match_date_text'),
                'match_timestamp_utc': item.get('match_timestamp_utc'),
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
            
            await repo.create(data)
            await session.commit()
    
    async def _process_task_item(self, item):
        """处理任务状态 Item"""
        try:
            await self._update_task_status(item)
            logger.debug(f"Updated task status: {item.get('status')}")
        except Exception as e:
            logger.error(f"Error updating task status: {e}")
        
        return item
    
    async def _update_task_status(self, item: CrawlTaskItem):
        """异步更新任务状态"""
        async with AsyncSessionLocal() as session:
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
    
    async def _process_captcha_item(self, item):
        """处理验证码检测 Item"""
        try:
            await self._save_captcha_alert(item)
            logger.warning(f"Captcha alert saved for league: {item.get('league_config_id')}")
        except Exception as e:
            logger.error(f"Error saving captcha alert: {e}")
        
        return item
    
    async def _save_captcha_alert(self, item: CaptchaDetectedItem):
        """异步保存验证码告警"""
        async with AsyncSessionLocal() as session:
            repo = AlertLogRepository(session)
            
            await repo.create({
                'alert_type': AlertType.CAPTCHA_BLOCKED,
                'severity': Severity.HIGH,
                'league_config_id': item.get('league_config_id'),
                'exception_summary': f"检测到验证码: {item.get('captcha_type')}",
                'suggested_action': '请人工处理验证码后继续',
                'is_resolved': False,
            })
            
            await session.commit()
