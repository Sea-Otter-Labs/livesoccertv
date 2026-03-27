"""
Config Package
"""

from config.database import (
    engine,
    AsyncSessionLocal,
    Base,
    get_db_session,
    init_db,
    close_db,
    DATABASE_URL
)
from config.settings import (
    LARK_WEBHOOK_URL,
    LARK_SECRET,
    ALERT_ENABLED,
    ALERT_SEVERITY_THRESHOLD,
    API_FOOTBALL_KEY,
    CRAWL_CONCURRENCY,
    CRAWL_DELAY,
    ALIGN_TIME_TOLERANCE_HOURS,
    ALIGN_MIN_CONFIDENCE,
)

__all__ = [
    'engine',
    'AsyncSessionLocal',
    'Base',
    'get_db_session',
    'init_db',
    'close_db',
    'DATABASE_URL',
    'LARK_WEBHOOK_URL',
    'LARK_SECRET',
    'ALERT_ENABLED',
    'ALERT_SEVERITY_THRESHOLD',
    'API_FOOTBALL_KEY',
    'CRAWL_CONCURRENCY',
    'CRAWL_DELAY',
    'ALIGN_TIME_TOLERANCE_HOURS',
    'ALIGN_MIN_CONFIDENCE',
]
