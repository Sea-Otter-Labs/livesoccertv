import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


BOT_NAME = 'livesoccertv_crawler'

SPIDER_MODULES = ['livesoccertv_crawler.crawler.spiders']
NEWSPIDER_MODULE = 'livesoccertv_crawler.crawler.spiders'

# 遵守 robots.txt 规则
ROBOTSTXT_OBEY = False

# 配置管道
ITEM_PIPELINES = {
    'livesoccertv_crawler.crawler.pipelines.match_pipeline.MatchDataPipeline': 300,
}

# 配置中间件
DOWNLOADER_MIDDLEWARES = {
    'livesoccertv_crawler.crawler.middlewares.drission_middleware.DrissionPageMiddleware': 600,
}

# 下载延迟（秒）
DOWNLOAD_DELAY = 2

# 随机下载延迟范围
RANDOMIZE_DOWNLOAD_DELAY = True

# 并发请求数
CONCURRENT_REQUESTS = 1

# 每个域名的并发请求数
CONCURRENT_REQUESTS_PER_DOMAIN = 1

# 超时设置
DOWNLOAD_TIMEOUT = 60

# 重试设置
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# 默认请求头
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# DrissionPage 配置
DRISSION_PAGE_CONFIG = {
    'headless': False,  # 开发时设为 False 以便观察，生产可设为 True
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'window_size': (1920, 1080),
    'timeout': 30,
}

# 验证码检测配置
CAPTCHA_CONFIG = {
    'enabled': True,
    'check_selectors': [
        # Cloudflare
        '#challenge-running',
        '.cf-browser-verification',
        'input[name="cf-turnstile-response"]',
        # reCAPTCHA
        '.g-recaptcha',
        'iframe[src*="recaptcha"]',
        # hCaptcha
        '.h-captcha',
        'iframe[src*="hcaptcha"]',
        # 通用
        '.captcha',
        '#captcha',
        'form[action*="captcha"]',
        'title:contains("CAPTCHA")',
        'title:contains("Security Check")',
        'title:contains("Just a moment")',
    ],
    'max_wait_time': 1800,  # 最大等待时间（秒），默认30分钟
    'check_interval': 2,    # 检查间隔（秒）
}

# 爬虫默认配置
SPIDER_CONFIG = {
    # 时间窗口（天）
    'history_days': 7,
    'future_days': 7,
    
    # 翻页配置
    'pagination': {
        'left_button_selector': 'div.pagination a[title*="Previous"], div.pagination a[title*="previous"], div.pagination a.prev, div.pagination a:contains("‹")',
        'right_button_selector': 'div.pagination a[title*="Next"], div.pagination a[title*="next"], div.pagination a.next, div.pagination a:contains("›")',
        'stop_conditions': {
            'max_pages': 100,  # 最大翻页数
            'duplicate_check': True,  # 启用重复检测
        }
    },
    
    # 选择器配置
    'selectors': {
        'schedule_table': 'table.schedules.blueborder',
        'date_row': 'tr.drow',
        'match_row': 'tr.matchrow',
        'channels_cell': 'td#channels',
        'match_time': 'td.time',
        'home_team': 'td.hometeam a',
        'away_team': 'td.awayteam a',
    }
}

# 日志配置
LOG_LEVEL = 'INFO'
LOG_FILE = 'logs/crawler.log'
LOG_STDOUT = True  # 同时输出到控制台

# 自定义日志格式
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# 自动限速
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

# 内存监控
MEMDEBUG_ENABLED = False
MEMUSAGE_ENABLED = False

# 扩展
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
}
