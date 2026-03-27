import logging
import json
from typing import Optional, Dict, Any, List
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)


class LarkNotifier:
    """
    飞书机器人通知器
    支持发送文本、富文本、卡片等多种消息格式
    """
    
    def __init__(self, webhook_url: str, secret: Optional[str] = None):
        """
        Args:
            webhook_url: 飞书机器人Webhook地址
            secret: 机器人密钥（如果设置了签名验证）
        """
        self.webhook_url = webhook_url
        self.secret = secret
    
    def _generate_sign(self, timestamp: str) -> str:
        """
        生成飞书签名（如果需要）
        """
        import hashlib
        import hmac
        import base64
        
        if not self.secret:
            return ""
        
        string_to_sign = f"{timestamp}\n{self.secret}"
        hmac_code = hmac.new(
            string_to_sign.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        return sign
    
    async def send_text(self, text: str) -> Dict[str, Any]:
        """
        发送纯文本消息
        
        Args:
            text: 消息内容
        
        Returns:
            飞书API响应
        """
        payload = {
            "msg_type": "text",
            "content": {
                "text": text
            }
        }
        return await self._send(payload)
    
    async def send_rich_text(
        self,
        title: str,
        content: List[List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        发送富文本消息
        
        Args:
            title: 标题
            content: 富文本内容，格式为 [[{"tag": "text", "text": "内容"}]]
        
        Returns:
            飞书API响应
        """
        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": content
                    }
                }
            }
        }
        return await self._send(payload)
    
    async def send_alert_card(
        self,
        alert_type: str,
        severity: str,
        league_name: str,
        home_team: str,
        away_team: str,
        match_time: str,
        error_message: str,
        error_log: Optional[str] = None,
        suggested_action: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        发送告警卡片消息（推荐）
        
        Args:
            alert_type: 告警类型
            severity: 严重等级
            league_name: 联赛名称
            home_team: 主队名
            away_team: 客队名
            match_time: 比赛时间
            error_message: 错误信息
            error_log: 错误日志（可选）
            suggested_action: 建议操作（可选）
        
        Returns:
            飞书API响应
        """
        # 根据严重等级设置颜色
        severity_colors = {
            'critical': 'red',
            'high': 'orange',
            'medium': 'yellow',
            'low': 'blue'
        }
        color = severity_colors.get(severity.lower(), 'grey')
        
        # 构建卡片元素
        elements = [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**告警类型:** {alert_type}\n**严重等级:** {severity.upper()}\n**联赛:** {league_name}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**比赛:** {home_team} vs {away_team}\n**时间:** {match_time}"
                }
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**错误信息:**\n{error_message}"
                }
            }
        ]
        
        # 添加错误日志（如果提供）
        if error_log:
            # 截断过长的日志
            truncated_log = error_log[:1000] + "..." if len(error_log) > 1000 else error_log
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**错误日志:**\n```\n{truncated_log}\n```"
                }
            })
        
        # 添加建议操作（如果提供）
        if suggested_action:
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**建议操作:** {suggested_action}"
                }
            })
        
        # 添加时间戳
        elements.append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": f"发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            ]
        })
        
        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {
                    "wide_screen_mode": True
                },
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": "数据对齐失败告警"
                    },
                    "template": color
                },
                "elements": elements
            }
        }
        
        return await self._send(payload)
    
    async def _send(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        发送消息到飞书
        
        Args:
            payload: 消息负载
        
        Returns:
            飞书API响应
        """
        timestamp = str(int(datetime.now().timestamp()))
        
        # 如果需要签名验证
        if self.secret:
            sign = self._generate_sign(timestamp)
            payload["timestamp"] = timestamp
            payload["sign"] = sign
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('code') == 0:
                            logger.info(f"Lark notification sent successfully: {result}")
                            return {
                                'success': True,
                                'response': result
                            }
                        else:
                            logger.error(f"Lark API error: {result}")
                            return {
                                'success': False,
                                'error': result.get('msg', 'Unknown error'),
                                'response': result
                            }
                    else:
                        error_text = await response.text()
                        logger.error(f"Lark HTTP error: {response.status} - {error_text}")
                        return {
                            'success': False,
                            'error': f"HTTP {response.status}: {error_text}"
                        }
        except Exception as e:
            logger.error(f"Failed to send Lark notification: {e}")
            return {
                'success': False,
                'error': str(e)
            }


class AlertNotifier:
    """
    告警通知器
    集成数据库告警记录和飞书通知
    """
    
    def __init__(
        self,
        webhook_url: Optional[str] = None,
        secret: Optional[str] = None
    ):
        """
        Args:
            webhook_url: 飞书Webhook地址（可从配置读取）
            secret: 机器人密钥
        """
        self.lark_notifier = None
        if webhook_url:
            self.lark_notifier = LarkNotifier(webhook_url, secret)
    
    async def notify_alignment_failure(
        self,
        alert_log: Any,
        error_log: Optional[str] = None
    ) -> bool:
        """
        发送数据对齐失败通知
        
        Args:
            alert_log: AlertLog模型实例
            error_log: 错误日志内容（可选）
        
        Returns:
            是否发送成功
        """
        if not self.lark_notifier:
            logger.warning("Lark notifier not configured, skipping notification")
            return False
        
        # 格式化比赛时间
        match_time_str = "未知"
        if alert_log.match_timestamp_utc:
            from datetime import datetime
            match_time_str = datetime.fromtimestamp(
                alert_log.match_timestamp_utc
            ).strftime('%Y-%m-%d %H:%M:%S')
        
        # 发送卡片消息
        result = await self.lark_notifier.send_alert_card(
            alert_type=alert_log.alert_type.value if hasattr(alert_log.alert_type, 'value') else str(alert_log.alert_type),
            severity=alert_log.severity.value if hasattr(alert_log.severity, 'value') else str(alert_log.severity),
            league_name=alert_log.league_name or "未知联赛",
            home_team=alert_log.home_team_name or "未知",
            away_team=alert_log.away_team_name or "未知",
            match_time=match_time_str,
            error_message=alert_log.exception_summary or "无错误信息",
            error_log=error_log,
            suggested_action=alert_log.suggested_action
        )
        
        return result.get('success', False)
    
    async def notify_simple_error(
        self,
        title: str,
        error_message: str,
        error_log: Optional[str] = None
    ) -> bool:
        """
        发送简单错误通知
        
        Args:
            title: 标题
            error_message: 错误信息
            error_log: 错误日志（可选）
        
        Returns:
            是否发送成功
        """
        if not self.lark_notifier:
            logger.warning("Lark notifier not configured, skipping notification")
            return False
        
        # 构建富文本内容
        content = [[
            {
                "tag": "text",
                "text": f"错误信息: {error_message}\n\n"
            }
        ]]
        
        if error_log:
            truncated_log = error_log[:500] + "..." if len(error_log) > 500 else error_log
            content[0].append({
                "tag": "text",
                "text": f"错误日志:\n{truncated_log}"
            })
        
        result = await self.lark_notifier.send_rich_text(
            title=title,
            content=content
        )
        
        return result.get('success', False)


# 便捷函数
async def send_alignment_alert(
    webhook_url: str,
    alert_type: str,
    severity: str,
    league_name: str,
    home_team: str,
    away_team: str,
    match_time: str,
    error_message: str,
    error_log: Optional[str] = None,
    secret: Optional[str] = None
) -> bool:
    """
    便捷函数：发送对齐失败告警
    
    Args:
        webhook_url: 飞书Webhook地址
        alert_type: 告警类型
        severity: 严重等级
        league_name: 联赛名称
        home_team: 主队名
        away_team: 客队名
        match_time: 比赛时间
        error_message: 错误信息
        error_log: 错误日志（可选）
        secret: 机器人密钥（可选）
    
    Returns:
        是否发送成功
    """
    notifier = LarkNotifier(webhook_url, secret)
    result = await notifier.send_alert_card(
        alert_type=alert_type,
        severity=severity,
        league_name=league_name,
        home_team=home_team,
        away_team=away_team,
        match_time=match_time,
        error_message=error_message,
        error_log=error_log
    )
    return result.get('success', False)
