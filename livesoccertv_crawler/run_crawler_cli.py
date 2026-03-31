import subprocess
import os
import sys
import asyncio
import logging
import argparse
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.database import init_db, close_db
from repo.league_config_repo import LeagueConfigRepository
from sqlalchemy.ext.asyncio import AsyncSession

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_enabled_leagues(db_session: AsyncSession, league_config_id: int = None) -> list:
    """从数据库获取启用的联赛配置"""
    repo = LeagueConfigRepository(db_session)
    
    if league_config_id:
        # 查询所有启用的联赛，然后过滤出指定ID
        all_enabled = await repo.get_enabled_configs()
        league = next((l for l in all_enabled if l.id == league_config_id), None)
        if not league:
            logger.error(f"League config {league_config_id} not found or not enabled")
            return []
        return [league]
    else:
        return await repo.get_enabled_configs()


def build_scrapy_command(league_config) -> list:
    """构造 scrapy crawl 命令参数"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    batch_id = f"batch_{league_config.id}_{timestamp}"
    
    return [
        "scrapy", "crawl", "livesoccertv",
        "-a", f"league_config_id={league_config.id}",
        "-a", f"league_name={league_config.league_name}",
        "-a", f"start_url={league_config.livesoccertv_url}",
        "-a", f"crawl_batch_id={batch_id}",
        "-a", f"history_days={league_config.history_days}",
        "-a", f"future_days={league_config.future_days}",
        "-a", f"country={league_config.country or ''}",
    ]


def run_scrapy_command(command: list, cwd: str) -> int:
    """执行 scrapy 命令，返回退出码"""
    try:
        result = subprocess.run(command, cwd=cwd)
        return result.returncode
    except FileNotFoundError:
        logger.error("scrapy command not found. Please ensure Scrapy is installed.")
        return 1
    except Exception as e:
        logger.error(f"Failed to run scrapy command: {e}")
        return 1


async def run_crawler_async(league_config_id: int = None):
    """异步运行爬虫 - 使用 subprocess 启动 scrapy"""
    await init_db()
    
    try:
        from config.database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            # 查询目标联赛
            leagues = await get_enabled_leagues(session, league_config_id)
            
            if not leagues:
                logger.error("No enabled leagues found to crawl")
                return 1
            
            logger.info(f"Starting crawler for {len(leagues)} league(s)")
            
            # 确定工作目录
            current_dir = Path(__file__).parent
            cwd = str(current_dir)
            
            all_success = True
            
            for league in leagues:
                logger.info(f"Crawling league: {league.league_name} (ID: {league.id})")
                
                command = build_scrapy_command(league)
                exit_code = run_scrapy_command(command, cwd)
                
                if exit_code != 0:
                    logger.error(f"League {league.league_name} failed with exit code {exit_code}")
                    all_success = False
                else:
                    logger.info(f"League {league.league_name} completed successfully")
            
            if all_success:
                logger.info("All leagues crawled successfully")
                return 0
            else:
                logger.error("Some leagues failed to crawl")
                return 1
                
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
    """测试函数 - 使用预定义的联赛配置"""
    from config.database import AsyncSessionLocal
    
    async def _test():
        await init_db()
        try:
            async with AsyncSessionLocal() as session:
                leagues = await get_enabled_leagues(session, league_config_id=1)
                
                if not leagues:
                    logger.error("Test league config 1 not found")
                    return 1
                
                league = leagues[0]
                command = build_scrapy_command(league)
                
                logger.info(f"Running test crawl for: {league.league_name}")
                logger.info(f"Command: {' '.join(command)}")
                
                current_dir = Path(__file__).parent
                exit_code = run_scrapy_command(command, str(current_dir))
                
                return exit_code
                
        finally:
            await close_db()
    
    return asyncio.run(_test())


if __name__ == '__main__':
    main()
