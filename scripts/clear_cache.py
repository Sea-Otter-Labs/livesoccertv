import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from api.app import clear_all_match_cache

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    logger.info("开始清理所有比赛缓存...")
    deleted = clear_all_match_cache()
    logger.info(f"缓存清理完成，共删除 {deleted} 个 key")


if __name__ == '__main__':
    main()
