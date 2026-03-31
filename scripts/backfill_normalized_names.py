import argparse
import asyncio
import logging
from datetime import datetime
from typing import List, Tuple

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config.database import AsyncSessionLocal, init_db, close_db
from models.api_fixture import ApiFixture
from models.web_crawl_raw import WebCrawlRaw
from utils.team_normalizer import normalize_team_name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def backfill_api_fixtures(
    session: AsyncSession,
    dry_run: bool = False,
    batch_size: int = 1000
) -> Tuple[int, int]:
    """
    回填 api_fixtures 表的标准化字段
    
    Returns:
        (处理的记录数, 实际更新的记录数)
    """
    processed = 0
    updated = 0
    
    logger.info("开始处理 api_fixtures 表...")
    
    # 获取所有记录
    result = await session.execute(select(ApiFixture))
    fixtures = result.scalars().all()
    
    logger.info(f"找到 {len(fixtures)} 条 api_fixtures 记录")
    
    for fixture in fixtures:
        processed += 1
        
        # 计算新的标准化值
        new_home_normalized = normalize_team_name(fixture.home_team_name)
        new_away_normalized = normalize_team_name(fixture.away_team_name)
        
        # 检查是否有变化
        home_changed = fixture.home_team_name_normalized != new_home_normalized
        away_changed = fixture.away_team_name_normalized != new_away_normalized
        
        if home_changed or away_changed:
            if dry_run:
                logger.info(
                    f"[DRY RUN] 将更新 fixture_id={fixture.fixture_id}: "
                    f"home '{fixture.home_team_name}' -> '{new_home_normalized}' "
                    f"(was '{fixture.home_team_name_normalized}'), "
                    f"away '{fixture.away_team_name}' -> '{new_away_normalized}' "
                    f"(was '{fixture.away_team_name_normalized}')"
                )
            else:
                fixture.home_team_name_normalized = new_home_normalized
                fixture.away_team_name_normalized = new_away_normalized
                updated += 1
                
                # 批量提交
                if updated % batch_size == 0:
                    await session.commit()
                    logger.info(f"已提交 {updated} 条更新...")
    
    if not dry_run and updated > 0:
        await session.commit()
    
    logger.info(f"api_fixtures 处理完成: {processed} 条记录, {updated} 条更新")
    return processed, updated


async def backfill_web_crawl_raw(
    session: AsyncSession,
    dry_run: bool = False,
    batch_size: int = 1000
) -> Tuple[int, int]:
    """
    回填 web_crawl_raw 表的标准化字段
    
    Returns:
        (处理的记录数, 实际更新的记录数)
    """
    processed = 0
    updated = 0
    
    logger.info("开始处理 web_crawl_raw 表...")
    
    # 获取所有记录
    result = await session.execute(select(WebCrawlRaw))
    crawls = result.scalars().all()
    
    logger.info(f"找到 {len(crawls)} 条 web_crawl_raw 记录")
    
    for crawl in crawls:
        processed += 1
        
        # 计算新的标准化值
        new_home_normalized = normalize_team_name(crawl.home_team_name_raw)
        new_away_normalized = normalize_team_name(crawl.away_team_name_raw)
        
        # 检查是否有变化
        home_changed = crawl.home_team_name_normalized != new_home_normalized
        away_changed = crawl.away_team_name_normalized != new_away_normalized
        
        if home_changed or away_changed:
            if dry_run:
                logger.info(
                    f"[DRY RUN] 将更新 id={crawl.id}: "
                    f"home '{crawl.home_team_name_raw}' -> '{new_home_normalized}' "
                    f"(was '{crawl.home_team_name_normalized}'), "
                    f"away '{crawl.away_team_name_raw}' -> '{new_away_normalized}' "
                    f"(was '{crawl.away_team_name_normalized}')"
                )
            else:
                crawl.home_team_name_normalized = new_home_normalized
                crawl.away_team_name_normalized = new_away_normalized
                updated += 1
                
                # 批量提交
                if updated % batch_size == 0:
                    await session.commit()
                    logger.info(f"已提交 {updated} 条更新...")
    
    if not dry_run and updated > 0:
        await session.commit()
    
    logger.info(f"web_crawl_raw 处理完成: {processed} 条记录, {updated} 条更新")
    return processed, updated


async def main():
    parser = argparse.ArgumentParser(
        description='回填标准化球队名称字段'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='只打印将要更新的记录，不实际更新数据库'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='每批处理的记录数，默认1000'
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        logger.info("=== DRY RUN 模式：不会实际更新数据库 ===")
    
    # 初始化数据库
    await init_db()
    
    try:
        async with AsyncSessionLocal() as session:
            # 处理 api_fixtures
            api_processed, api_updated = await backfill_api_fixtures(
                session, 
                dry_run=args.dry_run,
                batch_size=args.batch_size
            )
            
            # 处理 web_crawl_raw
            web_processed, web_updated = await backfill_web_crawl_raw(
                session,
                dry_run=args.dry_run,
                batch_size=args.batch_size
            )
            
            # 汇总报告
            logger.info("\n" + "="*60)
            logger.info("回填完成报告")
            logger.info("="*60)
            logger.info(f"api_fixtures: 处理 {api_processed} 条, 更新 {api_updated} 条")
            logger.info(f"web_crawl_raw: 处理 {web_processed} 条, 更新 {web_updated} 条")
            logger.info(f"总计: 处理 {api_processed + web_processed} 条, 更新 {api_updated + web_updated} 条")
            
            if args.dry_run:
                logger.info("\n这是 DRY RUN，没有实际更新数据库")
                logger.info("如需实际更新，请去掉 --dry-run 参数重新运行")
    
    except Exception as e:
        logger.error(f"回填过程出错: {e}")
        raise
    
    finally:
        await close_db()


if __name__ == '__main__':
    asyncio.run(main())
