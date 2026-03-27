"""
应用配置
集中管理所有环境变量和配置项
"""

import os

# 飞书机器人配置
LARK_WEBHOOK_URL = os.getenv('LARK_WEBHOOK_URL', '')
LARK_SECRET = os.getenv('LARK_SECRET', '')

# 数据对齐告警配置
ALERT_ENABLED = os.getenv('ALERT_ENABLED', 'true').lower() == 'true'
ALERT_SEVERITY_THRESHOLD = os.getenv('ALERT_SEVERITY_THRESHOLD', 'medium')  # low, medium, high, critical

# API-Football 配置
API_FOOTBALL_KEY = os.getenv('API_FOOTBALL_KEY', '')

# 爬虫配置
CRAWL_CONCURRENCY = int(os.getenv('CRAWL_CONCURRENCY', '3'))
CRAWL_DELAY = float(os.getenv('CRAWL_DELAY', '2.0'))

# 对齐配置
ALIGN_TIME_TOLERANCE_HOURS = float(os.getenv('ALIGN_TIME_TOLERANCE_HOURS', '4.0'))
ALIGN_MIN_CONFIDENCE = float(os.getenv('ALIGN_MIN_CONFIDENCE', '0.8'))
