import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional

workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from config.database import AsyncSessionLocal, init_db, close_db
from repo import LeagueConfigRepository, CrawlTaskStatusRepository, SystemConfigRepository
from crawler.settings import BOT_NAME
from crawler.spiders import LiveSoccerTVSpider

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CrawlerLauncher:
    """
    爬虫启动器
    管理爬虫配置和启动流程
    """
    
    def __init__(self):
        self.crawler_settings = None
        self._init_settings()
    
    def _init_settings(self):
        """初始化爬虫设置"""
        self.crawler_settings = Settings()
        self.crawler_settings.setmodule('crawler.settings')
    
    async def get_enabled_leagues(self) -> List[dict]:
        """
        从数据库获取启用的联赛配置
        """
        async with AsyncSessionLocal() as session:
            repo = LeagueConfigRepository(session)
            configs = await repo.get_enabled_configs()
            
            return [
                {
                    'id': c.id,
                    'api_league_id': c.api_league_id,
                    'api_season': c.api_season,
                    'league_name': c.league_name,
                    'livesoccertv_url': c.livesoccertv_url,
                    'country': c.country,
                    'history_days': c.history_days,
                    'future_days': c.future_days,
                }
                for c in configs
            ]
    
    async def create_crawl_batch(self, league_config_id: int) -> str:
        """
        创建抓取批次ID
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"batch_{league_config_id}_{timestamp}"
    
    def run_spider(self, league_config: dict, batch_id: str):
        """
        运行单个爬虫
        """
        logger.info(f"Starting spider for {league_config['league_name']} (batch: {batch_id})")
        
        process = CrawlerProcess(settings=self.crawler_settings)
        
        process.crawl(
            LiveSoccerTVSpider,
            league_config_id=league_config['id'],
            league_name=league_config['league_name'],
            start_url=league_config['livesoccertv_url'],
            crawl_batch_id=batch_id,
            history_days=league_config.get('history_days', 7),
            future_days=league_config.get('future_days', 7),
            country=league_config.get('country', ''),
        )
        
        # 启动爬虫（阻塞）
        process.start()
        
        logger.info(f"Spider completed for {league_config['league_name']}")
    
    async def run_single(self, league_config_id: Optional[int] = None):
        """
        运行单个联赛爬虫
        """
        leagues = await self.get_enabled_leagues()
        
        if not leagues:
            logger.warning("No enabled leagues found")
            return
        
        # 如果指定了联赛ID，只运行该联赛
        if league_config_id:
            league = next((l for l in leagues if l['id'] == league_config_id), None)
            if not league:
                logger.error(f"League config {league_config_id} not found")
                return
            leagues = [league]
        
        # 运行每个联赛的爬虫
        for league in leagues:
            batch_id = await self.create_crawl_batch(league['id'])
            self.run_spider(league, batch_id)
    
    async def run_all(self):
        """
        运行所有启用的联赛爬虫
        """
        await self.run_single()


async def main():
    """
    主函数
    """
    # 初始化数据库
    logger.info("Initializing database...")
    await init_db()
    
    try:
        launcher = CrawlerLauncher()
        await launcher.run_all()
    finally:
        await close_db()
        logger.info("Database connection closed")


def run_crawler(league_config_id: Optional[int] = None):
    """
    便捷函数：运行爬虫
    
    Args:
        league_config_id: 指定联赛配置ID，为None则运行所有启用的联赛
    """
    async def _run():
        await init_db()
        
        try:
            launcher = CrawlerLauncher()
            if league_config_id:
                await launcher.run_single(league_config_id)
            else:
                await launcher.run_all()
        finally:
            await close_db()
    
    asyncio.run(_run())


if __name__ == '__main__':
    # 运行所有爬虫
    run_crawler()
    
    # 或者运行指定联赛：
    # run_crawler(league_config_id=1)
