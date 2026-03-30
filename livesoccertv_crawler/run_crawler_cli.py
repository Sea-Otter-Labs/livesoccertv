import subprocess
import os
import sys
import asyncio
import logging
import argparse
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.database import init_db, close_db
from livesoccertv_crawler.launcher import CrawlerLauncher

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_crawler_async(league_config_id: int = None):
    """异步运行爬虫（在独立进程中使用）"""
    await init_db()
    
    try:
        launcher = CrawlerLauncher()
        
        if league_config_id:
            logger.info(f"Starting crawler for league_config_id: {league_config_id}")
            await launcher.run_single(league_config_id)
        else:
            logger.info("Starting crawler for all enabled leagues")
            await launcher.run_all()
        
        logger.info("Crawler completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Crawler failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
        
    finally:
        await close_db()


def main():
    """主函数 - 同步入口"""
    parser = argparse.ArgumentParser(description='LiveSoccerTV Web Crawler')
    parser.add_argument(
        '--league-config-id',
        type=int,
        default=None,
        help='Specific league config ID to crawl (optional)'
    )
    
    args = parser.parse_args()
    
    exit_code = asyncio.run(run_crawler_async(args.league_config_id))
    sys.exit(exit_code)

def test():
    return subprocess.run([
    "scrapy", "crawl", "livesoccertv",
    "-a", "league_config_id=1",
    "-a", "league_name=La Liga",
    "-a", "start_url=https://www.livesoccertv.com/competitions/spain/primera-division/",
    "-a", "crawl_batch_id=batch_1_xxx",
    "-a", "history_days=300",
    "-a", "future_days=120",
    "-a", "country=Spain",
], cwd="livesoccertv_crawler")


if __name__ == '__main__':
    # main()
    test()
    
