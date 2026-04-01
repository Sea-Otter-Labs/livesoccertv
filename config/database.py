from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import event
import os
import logging
from typing import AsyncGenerator

# 配置日志
logger = logging.getLogger(__name__)

# 数据库配置 - 请根据实际情况修改
DB_HOST = os.getenv('DB_HOST', 'pplivedatabase.cn4csgi60ope.eu-west-3.rds.amazonaws.com')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'livesoccertv_list')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'pplive123$')

# 构建数据库URL (使用 aiomysql 驱动)
DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 创建异步引擎
engine = create_async_engine(
    DATABASE_URL,
    echo=False,  # 设置为 True 可查看SQL语句
    pool_size=30,
    max_overflow=60,
    pool_timeout=60,  # 连接池超时时间（秒）
    pool_pre_ping=True,
    pool_recycle=3600
)


# 连接池事件监听 - 用于监控连接状态
@event.listens_for(engine.sync_engine, "checkout")
def on_checkout(dbapi_conn, connection_record, connection_proxy):
    """连接从池中被取出时"""
    pool = engine.pool
    logger.debug(f"[POOL] Connection checked out. Pool size: {pool.size()}, Checked out: {pool.checkedout()}")


@event.listens_for(engine.sync_engine, "checkin")
def on_checkin(dbapi_conn, connection_record):
    """连接归还到池中时"""
    pool = engine.pool
    logger.debug(f"[POOL] Connection checked in. Pool size: {pool.size()}, Checked out: {pool.checkedout()}")


@event.listens_for(engine.sync_engine, "connect")
def on_connect(dbapi_conn, connection_record):
    """新连接建立时"""
    logger.debug("[POOL] New connection created")


@event.listens_for(engine.sync_engine, "close")
def on_close(dbapi_conn, connection_record):
    """连接关闭时"""
    logger.debug("[POOL] Connection closed")

# 异步会话工厂
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

# 声明基类
Base = declarative_base()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话的依赖函数"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():
    """初始化数据库（创建所有表）"""
    async with engine.begin() as conn:
        # 注意：生产环境不建议使用 delete 和 create_all
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    """关闭数据库连接"""
    await engine.dispose()


def get_pool_status():
    """
    获取连接池状态
    用于监控连接池使用情况
    
    Returns:
        dict: 包含 pool_size, checkedout, available 等信息
    """
    pool = engine.pool
    return {
        'pool_size': pool.size(),
        'checkedout': pool.checkedout(),
        'available': pool.size() - pool.checkedout(),
        'overflow': max(0, pool.checkedout() - pool.size()),
        'max_size': pool.size() + pool._max_overflow
    }


def log_pool_status():
    """记录连接池状态到日志"""
    status = get_pool_status()
    logger.info(
        f"[POOL_STATUS] Size: {status['pool_size']}, "
        f"Checked out: {status['checkedout']}, "
        f"Available: {status['available']}, "
        f"Overflow: {status['overflow']}"
    )
