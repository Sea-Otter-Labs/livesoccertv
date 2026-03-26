from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
from DrissionPage import ChromiumPage, ChromiumOptions
import logging

logger = logging.getLogger(__name__)


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
        
        # 配置浏览器选项
        co = ChromiumOptions()
        
        # 无头模式
        if self.config.get('headless', False):
            co.headless(True)
        
        # 窗口大小
        window_size = self.config.get('window_size', (1920, 1080))
        co.set_argument(f'--window-size={window_size[0]},{window_size[1]}')
        
        # 禁用自动化检测
        co.set_user_agent(self.config.get('user_agent'))
        
        # 初始化页面
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
