import time
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
from DrissionPage import ChromiumPage, ChromiumOptions
import logging

from utils.proxy_manager import get_proxy_manager

logger = logging.getLogger(__name__)

# 页面挑战/校验页面检测关键词
CHALLENGE_KEYWORDS = ['请稍候', 'just a moment', 'security check', 'attention required']


class DrissionPageMiddleware:
    """
    DrissionPage 中间件
    使用真实浏览器处理动态页面
    """
    
    def __init__(self, settings):
        self.settings = settings
        self.page = None
        self.config = settings.get('DRISSION_PAGE_CONFIG', {})
        
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        middleware.crawler = crawler
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware
    
    def spider_opened(self, spider):
        """Spider 启动时初始化浏览器"""
        logger.info(f"Initializing DrissionPage browser for spider: {spider.name}")
        
        co = ChromiumOptions()
        
        if self.config.get('headless', False):
            co.headless(True)
            import platform
            system = platform.system()
            if system == "Linux":
                co.set_argument('--no-sandbox')
                co.set_argument('--disable-dev-shm-usage')
                co.set_browser_path('/opt/chrome-linux64/chrome')
                co.set_argument('--disable-setuid-sandbox')
                co.set_argument('--headless=new')
                co.set_argument('--disable-gpu')
                co.set_argument('--remote-debugging-address=0.0.0.0')
                co.set_local_port(9444)
        
        # 检测并配置代理（运行时，不依赖 API）
        proxy_manager = get_proxy_manager()
        use_proxy = False
        
        if proxy_manager.is_enabled:
            logger.info("Testing proxy connectivity...")
            success, result = proxy_manager.test_proxy_connectivity()
            
            if success:
                logger.info(f"Proxy test passed, IP: {result}")
                use_proxy = True
            else:
                logger.warning(f"Proxy test failed: {result}")
                logger.warning("Falling back to no proxy mode")
        
        if use_proxy:
            proxy_config = proxy_manager.get_chromium_proxy_config()
            if proxy_config:
                logger.info("Configuring browser with proxy")
                co.set_proxy(proxy_config['server'])
        
        try:
            self.page = ChromiumPage(addr_or_opts=co)
            spider.page = self.page
            logger.info("DrissionPage browser initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise
    
    def spider_closed(self, spider):
        """Spider 关闭时关闭浏览器"""
        if self.page:
            try:
                self.page.quit()
                logger.info("DrissionPage browser closed")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")
    
    def _is_challenge_page(self, page) -> bool:
        """检测页面是否为挑战/校验页面"""
        title = (page.title or '').lower()
        return any(kw in title for kw in CHALLENGE_KEYWORDS)
    
    def _wait_for_challenge(self, page, url) -> None:
        """
        检测到挑战页面时等待30秒后刷新
        只等待1次，不管结果继续执行
        """
        if not self._is_challenge_page(page):
            return
        
        logger.warning(f"[CHALLENGE] Detected challenge page, waiting 30s... URL={url}")
        time.sleep(30)
        
        # 刷新页面
        page.get(url)
        timeout = self.config.get('timeout', 30)
        page.wait.doc_loaded(timeout=timeout)
        
        if self._is_challenge_page(page):
            logger.warning(f"[CHALLENGE] Still present after 30s, continuing anyway. URL={url}")
        else:
            logger.info(f"[CHALLENGE] Cleared after waiting. URL={url}")
    
    def process_request(self, request):
        """
        处理请求
        使用 DrissionPage 加载页面并返回响应
        """
        # 只处理标记为使用 DrissionPage 的请求
        if not request.meta.get('use_drission', False):
            return None
        
        if not self.page:
            logger.error("Browser not initialized")
            raise IgnoreRequest("Browser not initialized")
        
        url = request.url
        logger.debug(f"Loading page with DrissionPage: {url}")
        
        try:
            # 加载页面
            self.page.get(url)
            
            # 等待页面加载完成
            timeout = self.config.get('timeout', 30)
            self.page.wait.doc_loaded(timeout=timeout)
            
            # 检测挑战页面并等待
            self._wait_for_challenge(self.page, url)
            
            # 获取页面内容
            html = self.page.html
            
            # 构建 Scrapy Response
            response = HtmlResponse(
                url=url,
                body=html.encode('utf-8'),
                encoding='utf-8',
                request=request
            )
            
            # 传递 page 对象给 spider，以便进行后续操作（如点击翻页）
            response.meta['page'] = self.page
            
            logger.debug(f"Page loaded successfully: {url}")
            return response
            
        except Exception as e:
            logger.error(f"Error loading page {url}: {e}")
            raise IgnoreRequest(f"Failed to load page: {e}")
