import time
import os
from datetime import datetime
from scrapy import signals
from scrapy.exceptions import IgnoreRequest
import logging

logger = logging.getLogger(__name__)


class CaptchaDetectionMiddleware:
    """
    验证码检测中间件
    检测页面是否出现验证码，并暂停等待人工处理
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.config = settings.get('CAPTCHA_CONFIG', {})
        self.enabled = self.config.get('enabled', True)
        self.check_selectors = self.config.get('check_selectors', [])
        self.max_wait_time = self.config.get('max_wait_time', 1800)  # 30分钟
        self.check_interval = self.config.get('check_interval', 2)
        self.screenshot_dir = 'logs/screenshots'
        
        # 确保截图目录存在
        os.makedirs(self.screenshot_dir, exist_ok=True)
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        middleware.crawler = crawler
        return middleware
    
    def process_response(self, request, response):
        """
        处理响应，检测验证码
        """
        if not self.enabled:
            return response
        
        # 只处理 DrissionPage 的请求
        if not request.meta.get('use_drission', False):
            return response
        
        page = response.meta.get('page')
        if not page:
            return response
        
        # 检测验证码
        captcha_info = self._detect_captcha(page)
        
        if captcha_info:
            logger.warning(f"Captcha detected: {captcha_info['type']}")
            
            # 保存截图
            screenshot_path = self._save_screenshot(page)
            
            # 更新任务状态为暂停
            spider = self.crawler.spider
            if hasattr(spider, 'pause_for_captcha'):
                spider.pause_for_captcha(
                    league_config_id=request.meta.get('league_config_id'),
                    page_url=request.url,
                    captcha_type=captcha_info['type'],
                    screenshot_path=screenshot_path
                )
            
            # 等待人工处理
            if self._wait_for_captcha_resolution(page):
                logger.info("Captcha resolved, continuing...")
                # 刷新页面内容
                html = page.html
                response = response.replace(body=html.encode('utf-8'))
                response.meta['page'] = page
            else:
                logger.error("Captcha resolution timeout or failed")
                raise IgnoreRequest("Captcha not resolved")
        
        return response
    
    def _detect_captcha(self, page) -> dict:
        """
        检测页面是否存在验证码
        
        Returns:
            dict: {'type': 'captcha_type'} 或 None
        """
        # 检查标题
        title = page.title.lower()
        if any(keyword in title for keyword in ['captcha', 'security check', 'just a moment', 'attention required']):
            return {'type': 'unknown', 'indicator': 'title'}
        
        # 检查选择器
        captcha_indicators = [
            ('.cf-browser-verification', 'cloudflare'),
            ('#challenge-running', 'cloudflare'),
            ('input[name="cf-turnstile-response"]', 'cloudflare_turnstile'),
            ('.g-recaptcha', 'recaptcha'),
            ('iframe[src*="recaptcha"]', 'recaptcha'),
            ('.h-captcha', 'hcaptcha'),
            ('iframe[src*="hcaptcha"]', 'hcaptcha'),
            ('#captcha', 'generic'),
            ('.captcha', 'generic'),
        ]
        
        for selector, captcha_type in captcha_indicators:
            try:
                if page.ele(selector, timeout=0.5):
                    return {'type': captcha_type, 'indicator': selector}
            except:
                continue
        
        return None
    
    def _save_screenshot(self, page) -> str:
        """保存验证码截图"""
        spider = self.crawler.spider
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"captcha_{spider.name}_{timestamp}.png"
        filepath = os.path.join(self.screenshot_dir, filename)
        
        try:
            page.get_screenshot(filepath)
            logger.info(f"Captcha screenshot saved: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save screenshot: {e}")
            return ""
    
    def _wait_for_captcha_resolution(self, page) -> bool:
        """
        等待验证码被解决
        
        Returns:
            bool: True 如果验证码已解决，False 如果超时
        """
        spider = self.crawler.spider
        logger.info(f"Waiting for captcha resolution (max {self.max_wait_time}s)...")
        
        start_time = time.time()
        check_count = 0
        
        while time.time() - start_time < self.max_wait_time:
            time.sleep(self.check_interval)
            check_count += 1
            
            # 检查验证码是否还存在
            captcha_info = self._detect_captcha(page)
            
            if not captcha_info:
                logger.info(f"Captcha resolved after {check_count} checks")
                return True
            
            # 每30秒打印一次日志
            if check_count % 15 == 0:
                elapsed = int(time.time() - start_time)
                logger.info(f"Still waiting for captcha resolution... ({elapsed}s elapsed)")
        
        logger.warning(f"Captcha resolution timeout after {self.max_wait_time}s")
        return False
