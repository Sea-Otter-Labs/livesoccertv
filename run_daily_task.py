import os
import sys
import asyncio
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
load_dotenv()

from services import run_daily_task

log_file = f'logs/daily_task_{datetime.now().strftime("%Y%m%d")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


async def main():
    """
    主函数
    """
    api_key = os.getenv('API_FOOTBALL_KEY', '')
    
    if not api_key:
        logger.error("=" * 60)
        logger.error("Error: API_FOOTBALL_KEY environment variable not set")
        logger.error("=" * 60)
        logger.error("\nPlease set the API key:")
        logger.error("  Windows: set API_FOOTBALL_KEY=your_api_key")
        logger.error("  Linux/Mac: export API_FOOTBALL_KEY=your_api_key")
        logger.error("\nOr create a .env file with:")
        logger.error("  API_FOOTBALL_KEY=your_api_key")
        sys.exit(1)
    
    logger.info("=" * 60)
    logger.info("Starting Daily Task")
    logger.info("=" * 60)
    
    try:
        # 运行每日任务
        results = await run_daily_task(api_key)
        
        logger.info("=" * 60)
        logger.info("Daily Task Summary")
        logger.info("=" * 60)
        
        # API 同步结果
        api_sync = results.get('api_sync', {})
        logger.info(f"\n1. API Sync:")
        logger.info(f"   - Leagues processed: {api_sync.get('total_leagues', 0)}")
        logger.info(f"   - Fixtures inserted: {api_sync.get('total_inserted', 0)}")
        logger.info(f"   - Fixtures updated: {api_sync.get('total_updated', 0)}")
        
        # 网页抓取结果
        web_crawl = results.get('web_crawl', {})
        logger.info(f"\n2. Web Crawl:")
        logger.info(f"   - Status: {web_crawl.get('status', 'unknown')}")
        if web_crawl.get('status') == 'skipped':
            logger.info(f"   - Note: Web crawl is now manual. Run separately if needed:")
            logger.info(f"     python livesoccertv_crawler/run_crawler_cli.py")
        
        # 对齐结果
        alignment = results.get('alignment', {})
        logger.info(f"\n3. Match Alignment:")
        logger.info(f"   - Aligned: {alignment.get('total_aligned', 0)}")
        logger.info(f"   - Unmatched: {alignment.get('total_unmatched', 0)}")
        logger.info(f"   - Ambiguous: {alignment.get('total_ambiguous', 0)}")
        logger.info(f"   - Missing Channels: {alignment.get('total_missing_channels', 0)}")
        logger.info(f"   - Web Unmatched: {alignment.get('total_web_unmatched', 0)}")
        
        # 错误
        errors = results.get('errors', [])
        if errors:
            logger.error(f"\n4. Errors ({len(errors)}):")
            for error in errors:
                logger.error(f"   - {error}")
        else:
            logger.info(f"\n4. Errors: None")
        
        logger.info("\n" + "=" * 60)
        logger.info("Daily Task Completed Successfully")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"Daily Task Failed: {e}")
        logger.error("=" * 60)
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
