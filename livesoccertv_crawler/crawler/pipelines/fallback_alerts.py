import logging
import os
import sys
from datetime import datetime
from typing import Optional
import asyncio

workspace_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

try:
    from config.settings import LARK_WEBHOOK_URL, LARK_SECRET
    from services.lark_notifier import AlertNotifier
    LARK_AVAILABLE = True
except ImportError:
    LARK_AVAILABLE = False
    LARK_WEBHOOK_URL = os.getenv('LARK_WEBHOOK_URL', '')
    LARK_SECRET = os.getenv('LARK_SECRET', '')

logger = logging.getLogger(__name__)


class FallbackAlertManager:
    """
    降级告警管理器
    - 支持飞书 webhook 告警（不依赖数据库）
    - 内存级别的告警去重（按 league + host + 15分钟窗口）
    - 本地日志兜底
    """
    
    def __init__(self):
        self.lark_notifier: Optional[AlertNotifier] = None
        self._alert_cache: set = set()  # 告警去重缓存
        self._cache_ttl_seconds = 900  # 15分钟
        self._enabled = bool(LARK_WEBHOOK_URL) and LARK_AVAILABLE
        
        if self._enabled:
            try:
                self.lark_notifier = AlertNotifier(
                    webhook_url=LARK_WEBHOOK_URL, 
                    secret=LARK_SECRET or None
                )
                logger.info("[FALLBACK_ALERT] Lark notifier initialized")
            except Exception as e:
                logger.warning(f"[FALLBACK_ALERT] Failed to init Lark notifier: {e}")
                self._enabled = False
    
    def _get_cache_key(self, alert_type: str, league_id: str, host: str) -> str:
        """生成告警缓存键（按15分钟窗口）"""
        from datetime import datetime
        time_bucket = datetime.now().strftime('%Y%m%d%H%M')[:11] + '0'  # 15分钟窗口: 00/15/30/45
        return f"{alert_type}:{league_id}:{host}:{time_bucket}"
    
    def _should_send_alert(self, alert_type: str, league_id: str, host: str) -> bool:
        """检查是否应该发送告警（去重）"""
        cache_key = self._get_cache_key(alert_type, league_id, host)
        if cache_key in self._alert_cache:
            return False
        
        # 清理过期缓存（简化处理：超过1000条时清空）
        if len(self._alert_cache) > 1000:
            self._alert_cache.clear()
        
        self._alert_cache.add(cache_key)
        return True
    
    async def send_db_connection_alert(
        self, 
        league_id: str,
        host: str,
        error_msg: str,
        retry_count: int,
        match_info: str = ""
    ):
        """
        发送数据库连接失败告警
        """
        if not self._should_send_alert('db_connection_failed', league_id, host):
            logger.debug("[FALLBACK_ALERT] Alert suppressed (dedup)")
            return
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 1. 首先记录到本地日志（保底）
        logger.error(
            f"[DB_CONNECTION_ALERT] 数据库连接失败 | "
            f"联赛: {league_id} | 主机: {host} | "
            f"重试次数: {retry_count} | "
            f"比赛: {match_info} | "
            f"错误: {error_msg}"
        )
        
        # 2. 尝试发送飞书告警
        if self._enabled and self.lark_notifier:
            try:
                alert_data = {
                    'alert_type': 'db_connection_failed',
                    'severity': 'critical',
                    'league_name': f"联赛ID: {league_id}",
                    'home_team': host,
                    'away_team': '数据库连接失败',
                    'match_time': timestamp,
                    'error_message': error_msg,
                    'suggested_action': f'已重试{retry_count}次仍失败，请检查RDS状态和网络安全组配置',
                }
                
                # 使用飞书的富文本告警
                await self.lark_notifier.send_alert_card(
                    alert_type=alert_data['alert_type'],
                    severity=alert_data['severity'],
                    league_name=alert_data['league_name'],
                    home_team=alert_data['home_team'],
                    away_team=alert_data['away_team'],
                    match_time=alert_data['match_time'],
                    error_message=alert_data['error_message'],
                    suggested_action=alert_data['suggested_action']
                )
                
                logger.info(f"[FALLBACK_ALERT] Lark alert sent successfully")
                
            except Exception as e:
                logger.error(f"[FALLBACK_ALERT] Failed to send Lark alert: {e}")
                # 飞书也失败了，确保本地日志已记录
    
    async def send_db_recovery_alert(
        self,
        league_id: str,
        host: str,
        downtime_seconds: float
    ):
        """
        发送数据库恢复告警
        """
        if not self._should_send_alert('db_recovery', league_id, host):
            return
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(
            f"[DB_RECOVERY_ALERT] 数据库连接恢复 | "
            f"联赛: {league_id} | 主机: {host} | "
            f"中断时长: {downtime_seconds:.1f}s"
        )
        
        if self._enabled and self.lark_notifier:
            try:
                await self.lark_notifier.send_rich_text(
                    title="🟢 数据库连接恢复",
                    content=[
                        f"**时间**: {timestamp}",
                        f"**联赛**: {league_id}",
                        f"**主机**: {host}",
                        f"**中断时长**: {downtime_seconds:.1f}秒",
                        "**状态**: 已恢复"
                    ],
                    at_all=False
                )
            except Exception as e:
                logger.warning(f"[FALLBACK_ALERT] Failed to send recovery alert: {e}")


# 全局实例
_fallback_alert_manager: Optional[FallbackAlertManager] = None


def get_fallback_alert_manager() -> FallbackAlertManager:
    """获取全局降级告警管理器实例"""
    global _fallback_alert_manager
    if _fallback_alert_manager is None:
        _fallback_alert_manager = FallbackAlertManager()
    return _fallback_alert_manager


# 便捷函数
async def send_db_connection_alert(
    league_id: str,
    host: str,
    error_msg: str,
    retry_count: int,
    match_info: str = ""
):
    """发送数据库连接失败告警"""
    manager = get_fallback_alert_manager()
    await manager.send_db_connection_alert(
        league_id=league_id,
        host=host,
        error_msg=error_msg,
        retry_count=retry_count,
        match_info=match_info
    )


async def send_db_recovery_alert(
    league_id: str,
    host: str,
    downtime_seconds: float
):
    """发送数据库恢复告警"""
    manager = get_fallback_alert_manager()
    await manager.send_db_recovery_alert(
        league_id=league_id,
        host=host,
        downtime_seconds=downtime_seconds
    )
