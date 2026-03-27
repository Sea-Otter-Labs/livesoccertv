import re
import hashlib
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import scrapy
from scrapy.http import Request

from livesoccertv_crawler.crawler.items import LiveSoccerTVMatchItem, CrawlTaskItem, CaptchaDetectedItem
from livesoccertv_crawler.crawler.utils.helpers import normalize_team_name, parse_livesoccertv_date, utc_now_timestamp
from models.crawl_task_status import TaskPhase, TaskStatus


class LiveSoccerTVSpider(scrapy.Spider):
    """
    LiveSoccerTV 爬虫
    抓取联赛页面的比赛信息和转播频道
    """
    
    name = 'livesoccertv'
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # 从参数获取配置
        self.league_config_id = kwargs.get('league_config_id')
        self.league_name = kwargs.get('league_name')
        self.start_url = kwargs.get('start_url')
        self.crawl_batch_id = kwargs.get('crawl_batch_id')
        self.history_days = int(kwargs.get('history_days', 7))
        self.future_days = int(kwargs.get('future_days', 7))
        self.country = kwargs.get('country', '')
        
        # 内部状态
        self.page = None  # DrissionPage 实例
        self.matches_crawled = 0
        self.current_direction = None
        self.visited_cursors = set()  # 已访问的分页游标
        self.captcha_detected = False
        
        # 翻页按钮缓存（用于调试和验证）
        self._last_pagination_button_info = None
    
    async def start(self):

        if not self.start_url:
            self.logger.error("No start_url provided")
            return
        
        self.logger.info(f"Starting crawl for {self.league_name}: {self.start_url}")
        self.logger.info(f"Time window: -{self.history_days}d to +{self.future_days}d")
        
        # 使用 DrissionPage 加载页面
        yield Request(
            url=self.start_url,
            callback=self.parse_league_page,
            meta={'use_drission': True, 'league_config_id': self.league_config_id}
        )
    
    def start_requests(self):

        if not self.start_url:
            self.logger.error("No start_url provided")
            return
        
        self.logger.info(f"Starting crawl for {self.league_name}: {self.start_url}")
        self.logger.info(f"Time window: -{self.history_days}d to +{self.future_days}d")
        
        # 使用 DrissionPage 加载页面
        yield Request(
            url=self.start_url,
            callback=self.parse_league_page,
            meta={'use_drission': True, 'league_config_id': self.league_config_id}
        )
    
    def parse_league_page(self, response):
        """
        解析联赛页面
        """
        self.page = response.meta.get('page')
        
        if not self.page:
            self.logger.error("No page object in response")
            return
        
        self.logger.info(f"Parsing league page: {response.url}")
        
        # 更新任务状态为爬取中
        yield self._create_task_item(
            task_phase=TaskPhase.WEB_CRAWL,
            status=TaskStatus.RUNNING,
            matches_crawled=self.matches_crawled
        )
        
        # 1. 抓取当前页
        matches = self._parse_current_page(response.url)
        for match_data in matches:
            yield self._create_match_item(match_data)
            self.matches_crawled += 1
        
        self.logger.info(f"Current page: {len(matches)} matches extracted")
        
        # 2. 向左翻页抓历史
        left_matches = self._crawl_pagination('left')
        for match_data in left_matches:
            yield self._create_match_item(match_data)
            self.matches_crawled += 1
        
        # 3. 回到起始页
        self.page.get(self.start_url)
        self.page.wait.doc_loaded(timeout=20)
        self.visited_cursors.clear()
        
        # 4. 向右翻页抓未来
        right_matches = self._crawl_pagination('right')
        for match_data in right_matches:
            yield self._create_match_item(match_data)
            self.matches_crawled += 1
        
        # 完成任务
        yield self._create_task_item(
            task_phase=TaskPhase.COMPLETED,
            status=TaskStatus.SUCCESS,
            matches_crawled=self.matches_crawled
        )
        
        self.logger.info(f"Crawl completed. Total matches: {self.matches_crawled}")
    
    def _parse_current_page(self, page_url):
        """
        解析当前页的比赛数据
        """
        matches = []
        
        try:
            # 等待表格加载
            if not self._wait_until_page_ready(timeout=30):
                self.logger.warning(f"Schedule table not found on {page_url}")
                return matches
            
            # 获取页面内容哈希用于去重
            page_hash = self._get_page_hash()
            if page_hash in self.visited_cursors:
                self.logger.info(f"Page already visited: {page_hash}")
                return matches
            
            self.visited_cursors.add(page_hash)
            
            # 解析比赛数据
            current_cursor = self._get_pagination_cursor()
            matches = self._extract_matches(page_url, current_cursor)
            
        except Exception as e:
            self.logger.error(f"Error parsing current page: {e}")
        
        return matches

    def _wait_until_page_ready(self, timeout=20):
        """等待业务页面恢复到可解析状态。"""
        end_time = time.time() + timeout
        logged_captcha_wait = False

        while time.time() < end_time:
            try:
                self.page.wait.doc_loaded(timeout=3)
            except Exception:
                pass

            try:
                if self.page.ele('css:#_live table.schedules.blueborder', timeout=3):
                    self.captcha_detected = False
                    return True
            except Exception:
                pass

            if self._is_captcha_page():
                if not logged_captcha_wait:
                    self.logger.info("Captcha still active, waiting for manual verification to complete...")
                    logged_captcha_wait = True
                time.sleep(1)
                continue

            time.sleep(1)

        return False

    def _is_captcha_page(self):
        """判断当前页面是否仍处于验证码或安全校验状态。"""
        try:
            title = (self.page.title or '').lower()
        except Exception:
            title = ''

        title_indicators = ['captcha', 'security check', 'just a moment', 'attention required', '请稍侯']
        if any(keyword in title for keyword in title_indicators):
            return True

        try:
            if self.page.ele('css:#_live table.schedules.blueborder', timeout=3):
                return False
        except Exception:
            pass

        strong_selectors = [
            'css:.cf-browser-verification',
            'css:#challenge-running',
            'css:#captcha',
            'css:.captcha',
        ]

        for selector in strong_selectors:
            try:
                if self.page.ele(selector, timeout=0.5):
                    return True
            except Exception:
                continue

        weak_selectors = [
            'css:input[name="cf-turnstile-response"]',
            'css:.g-recaptcha',
            'css:iframe[src*="recaptcha"]',
            'css:.h-captcha',
            'css:iframe[src*="hcaptcha"]',
        ]

        for selector in weak_selectors:
            try:
                if self.page.ele(selector, timeout=0.5):
                    return True
            except Exception:
                continue

        return False
    
    def _find_pagination_button(self, direction):
        """
        查找翻页按钮，通过读取 onclick 验证方向
        
        Args:
            direction: 'left' 或 'right'
            
        Returns:
            tuple: (button_element, button_info_dict) 或 (None, None)
        """
        expected_action = 'previous' if direction == 'left' else 'next'
        
        # 先尝试精确匹配：class 包含 pagination-left/right 且 onclick 包含对应 action
        try:
            class_suffix = 'pagination-left' if direction == 'left' else 'pagination-right'
            button = self.page.ele(f'css:div.pagination.clickable.{class_suffix}', timeout=2)
            if button:
                onclick = button.attr('onclick') or ''
                if f"'{expected_action}'" in onclick or f'"{expected_action}"' in onclick:
                    info = {
                        'selector': f'div.pagination.clickable.{class_suffix}',
                        'text': button.text,
                        'class': button.attr('class'),
                        'onclick': onclick[:200] + '...' if len(onclick) > 200 else onclick
                    }
                    self.logger.info(f"Found {direction} button by class: {info}")
                    return button, info
        except Exception as e:
            self.logger.debug(f"Class-based search failed: {e}")
        
        # 备选：查找所有 clickable pagination，逐个检查 onclick
        try:
            buttons = self.page.eles('css:div.pagination.clickable')
            self.logger.debug(f"Found {len(buttons)} clickable pagination buttons")
            
            for idx, btn in enumerate(buttons):
                onclick = btn.attr('onclick') or ''
                btn_class = btn.attr('class') or ''
                btn_text = btn.text
                
                # 检查 onclick 中是否包含预期的 action
                if f"'{expected_action}'" in onclick or f'"{expected_action}"' in onclick:
                    info = {
                        'selector': f'div.pagination.clickable (index {idx})',
                        'text': btn_text,
                        'class': btn_class,
                        'onclick': onclick[:200] + '...' if len(onclick) > 200 else onclick
                    }
                    self.logger.info(f"Found {direction} button by onclick scan: {info}")
                    return btn, info
                
                # 记录所有按钮的信息用于调试
                self.logger.debug(f"Button {idx}: text='{btn_text}', class='{btn_class}', onclick='{onclick[:100]}...'")
        except Exception as e:
            self.logger.debug(f"Scan search failed: {e}")
        
        # 最后尝试：查找 onclick 属性包含对应 action 的任何元素
        try:
            button = self.page.ele(f'css:[onclick*="{expected_action}"]', timeout=2)
            if button:
                onclick = button.attr('onclick') or ''
                info = {
                    'selector': f'[onclick*="{expected_action}"]',
                    'text': button.text,
                    'class': button.attr('class'),
                    'onclick': onclick[:200] + '...' if len(onclick) > 200 else onclick
                }
                self.logger.info(f"Found {direction} button by attribute: {info}")
                return button, info
        except Exception as e:
            self.logger.debug(f"Attribute search failed: {e}")
        
        self.logger.warning(f"No {direction} pagination button found")
        return None, None
    
    def _execute_pagination(self, button, button_info, direction, page_count):
        """
        执行翻页操作，优先使用 onclick JS
        
        Args:
            button: 按钮元素
            button_info: 按钮信息字典
            direction: 翻页方向
            page_count: 当前页码
            
        Returns:
            bool: 是否成功
        """
        try:
            onclick_js = button.attr('onclick')
            
            if onclick_js and 'paginate' in onclick_js:
                self.logger.info(f"Executing onclick JS for {direction} pagination (page {page_count})")
                self.logger.debug(f"JS: {onclick_js[:150]}...")
                
                # 记录翻页前的状态
                pre_hash = self._get_page_hash()
                pre_url = self.page.url
                
                # 执行 JS
                self.page.run_js(onclick_js)
                
                # 等待内容变化（而不是整页加载）
                max_wait = 10
                for i in range(max_wait):
                    time.sleep(0.5)
                    current_hash = self._get_page_hash()
                    if current_hash != pre_hash:
                        self.logger.debug(f"Content changed after {i+1} attempts")
                        return True
                
                self.logger.warning(f"Content did not change after {direction} pagination")
                return False
            else:
                # 回退到点击方式
                self.logger.info(f"Clicking {direction} button (page {page_count})")
                button.click()
                self.page.wait.doc_loaded(timeout=10)
                return True
                
        except Exception as e:
            self.logger.error(f"Error executing pagination: {e}")
            return False
    
    def _crawl_pagination(self, direction):
        """
        翻页抓取
        
        Args:
            direction: 'left' 或 'right'
        
        Returns:
            list: 抓取的比赛数据列表
        """
        self.current_direction = direction
        all_matches = []
        max_pages = 100
        page_count = 0
        
        self.logger.info(f"Starting {direction} pagination...")
        
        while page_count < max_pages:
            page_count += 1
            
            # 查找翻页按钮（新逻辑）
            button, button_info = self._find_pagination_button(direction)
            
            if not button:
                self.logger.info(f"No {direction} pagination button found at page {page_count}")
                break
            
            # 检查按钮是否可点击
            btn_class = button.attr('class') or ''
            if 'disabled' in btn_class:
                self.logger.info(f"{direction.capitalize()} button disabled")
                break
            
            # 执行翻页
            self._last_pagination_button_info = button_info
            success = self._execute_pagination(button, button_info, direction, page_count)
            
            if not success:
                self.logger.error(f"Pagination execution failed for {direction}")
                break
            
            # 等待页面准备就绪
            if not self._wait_until_page_ready(timeout=30):
                self.logger.warning(f"Page not ready after {direction} pagination click")
                break
            
            # 检查页面是否变化（去重）
            page_hash = self._get_page_hash()
            if page_hash in self.visited_cursors:
                self.logger.info(f"Duplicate page detected, stopping {direction} pagination")
                break
            
            self.visited_cursors.add(page_hash)
            
            # 解析新页面
            current_url = self.page.url
            current_cursor = self._get_pagination_cursor()
            
            matches = self._extract_matches(current_url, current_cursor)
            
            for match_data in matches:
                # 检查是否超出时间窗口
                if self._is_outside_window(match_data.get('match_timestamp_utc'), direction):
                    self.logger.info(f"Reached window boundary in {direction} direction")
                    return all_matches
                
                all_matches.append(match_data)
            
            self.logger.info(
                f"Page {page_count} ({direction}): extracted {len(matches)} matches, "
                f"total: {len(all_matches)}"
            )
        
        self.logger.info(f"Completed {direction} pagination, pages: {page_count}, matches: {len(all_matches)}")
        return all_matches
    
    def _extract_matches(self, page_url, pagination_cursor):
        """
        提取页面中的比赛数据
        """
        matches = []
        current_date = None
        
        try:
            # 获取表格所有行
            self.logger.info(f"=== Extracting matches from: {page_url} ===")
            self.logger.info(f"Current page URL: {self.page.url}")
            self.logger.info(f"Page title: {self.page.title}")


            # 检查 #_live 容器是否存在
            try:
                live_container = self.page.ele('css:#_live', timeout=2)
                self.logger.info(f"#_live container found: {live_container is not None}")
                if live_container:
                    self.logger.info(f"#_live container HTML snippet: {live_container.html[:500]}...")
            except Exception as e:
                self.logger.error(f"Failed to find #_live container: {e}")

            table = self.page.ele('css:#_live table.schedules.blueborder', timeout=3)
            if not table:
                return matches
            
            rows = table.eles('css:tbody tr')
            self.logger.info(f"{rows}")

            for row in rows:
                try:
                    row_class = row.attr('class') or ''
                    
                    # 检查是否是日期行
                    if 'drow' in row_class:
                        current_date = self._parse_date_row(row)
                        continue
                    
                    # 检查是否是比赛行
                    if 'matchrow' in row_class:
                        match_data = self._parse_match_row(row, current_date, page_url, pagination_cursor)
                        if match_data:
                            matches.append(match_data)
                
                except Exception as e:
                    self.logger.debug(f"Error parsing row: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"Error extracting matches: {e}")
        
        return matches
    
    def _parse_date_row(self, row):
        """解析日期行"""
        try:
            date_text = row.text.strip()
            date_text = re.sub(r'\s+', ' ', date_text)
            return date_text
        except:
            return None
    
    def _parse_match_row(self, row, current_date, page_url, pagination_cursor):
        """解析比赛行"""
        try:
            # 提取时间
            time_elem = row.ele('css:.timecell .ts', timeout=0.5)
            time_text = time_elem.text.strip() if time_elem else ''

            # 提取比赛文本并拆分主客队
            match_elem = row.ele('css:td#match a', timeout=0.5)
            match_text = match_elem.text.strip() if match_elem else ''
            home_team, away_team = self._split_match_teams(match_text)
            
            if not home_team or not away_team:
                return None
            
            # 提取频道
            channels = self._extract_channels(row)
            
            # 解析时间戳
            timezone_hint = self._get_timezone_hint()
            timestamp = parse_livesoccertv_date(
                current_date or '',
                time_text,
                timezone_hint
            )
            
            # 构建数据
            match_data = {
                'match_date_text': current_date,
                'match_time_text': time_text,
                'match_timestamp_utc': timestamp,
                'home_team_name_raw': home_team,
                'home_team_name_normalized': normalize_team_name(home_team),
                'away_team_name_raw': away_team,
                'away_team_name_normalized': normalize_team_name(away_team),
                'channel_list': channels,
                'pagination_cursor': pagination_cursor,
                'source_match_text': match_text or f"{home_team} vs {away_team}",
                'page_url': page_url,
            }
            
            return match_data
        
        except Exception as e:
            self.logger.debug(f"Error parsing match row: {e}")
            return None

    def _split_match_teams(self, match_text):
        """从比赛文案中拆分主客队"""
        text = re.sub(r'\s+', ' ', (match_text or '').strip())
        if not text:
            return '', ''

        vs_match = re.match(r'^(.*?)\s+vs\s+(.*)$', text, flags=re.IGNORECASE)
        if vs_match:
            return vs_match.group(1).strip(), vs_match.group(2).strip()

        score_match = re.match(r'^(.*?)\s+\d+\s*-\s*\d+\s+(.*)$', text)
        if score_match:
            return score_match.group(1).strip(), score_match.group(2).strip()

        return '', ''
    
    def _extract_channels(self, row):
        """提取频道信息"""
        channels = []
        
        try:
            channels_cell = row.ele('css:td#channels', timeout=0.5)
            if not channels_cell:
                return channels
            
            channel_links = channels_cell.eles('a')
            
            for link in channel_links:
                try:
                    channel_name = link.text.strip()
                    if not channel_name:
                        continue
                    
                    channel_url = link.attr('href') or ''
                    country = self._infer_country(channel_url, channel_name)
                    is_streaming = self._is_streaming_channel(channel_name, channel_url)
                    
                    channels.append({
                        'name': channel_name,
                        'country': country,
                        'type': 'Streaming' if is_streaming else 'TV',
                        'is_streaming': is_streaming,
                        'url': channel_url if channel_url.startswith('http') else None
                    })
                except:
                    continue
        
        except Exception as e:
            self.logger.debug(f"Error extracting channels: {e}")
        
        return channels
    
    def _infer_country(self, url, name):
        """推断国家"""
        country_patterns = {
            'uk': 'UK', 'usa': 'USA', 'espn': 'USA',
            'espana': 'Spain', 'spain': 'Spain',
            'deutschland': 'Germany', 'germany': 'Germany',
            'italia': 'Italy', 'italy': 'Italy',
            'france': 'France', 'china': 'China',
        }
        
        text = f"{url} {name}".lower()
        
        for pattern, country in country_patterns.items():
            if pattern in text:
                return country
        
        return 'Unknown'
    
    def _is_streaming_channel(self, name, url):
        """判断是否是流媒体"""
        streaming_keywords = [
            'stream', 'app', 'online', 'web', 'digital',
            'espn+', 'dazn', 'paramount+', 'peacock',
            'youtube', 'prime video', 'netflix'
        ]
        
        text = f"{name} {url}".lower()
        return any(keyword in text for keyword in streaming_keywords)
    
    def _get_page_hash(self):
        """获取页面哈希"""
        try:
            table = self.page.ele('css:#_live table.schedules.blueborder', timeout=3)
            if table:
                content = table.text
                return hashlib.md5(content.encode()).hexdigest()[:16]
        except:
            pass
        
        return hashlib.md5(self.page.url.encode()).hexdigest()[:16]
    
    def _get_pagination_cursor(self):
        """获取分页游标"""
        try:
            url = self.page.url
            if 'page=' in url:
                match = re.search(r'page=(\d+)', url)
                if match:
                    return match.group(1)
            
            return self._get_page_hash()
        except:
            return datetime.now().strftime('%Y%m%d%H%M%S')
    
    def _get_timezone_hint(self):
        """获取时区提示"""
        timezone_map = {
            'Spain': 'CET', 'England': 'GMT', 'Germany': 'CET',
            'Italy': 'CET', 'France': 'CET',
        }
        return timezone_map.get(self.country, 'CET')
    
    def _is_outside_window(self, timestamp, direction):
        """检查是否超出时间窗口"""
        if not timestamp:
            return False
        
        current_time = utc_now_timestamp()
        
        if direction == 'left':
            boundary = current_time - (self.history_days * 24 * 3600)
            return timestamp < boundary
        else:
            boundary = current_time + (self.future_days * 24 * 3600)
            return timestamp > boundary
    
    def _create_match_item(self, match_data):
        """创建比赛 Item"""
        item = LiveSoccerTVMatchItem()
        
        item['crawl_batch_id'] = self.crawl_batch_id
        item['source_site'] = 'livesoccertv'
        item['league_config_id'] = int(self.league_config_id) if self.league_config_id else None
        item['league_name'] = self.league_name
        item['match_date_text'] = match_data.get('match_date_text')
        item['match_timestamp_utc'] = match_data.get('match_timestamp_utc')
        item['match_time_text'] = match_data.get('match_time_text')
        item['home_team_name_raw'] = match_data.get('home_team_name_raw')
        item['home_team_name_normalized'] = match_data.get('home_team_name_normalized')
        item['away_team_name_raw'] = match_data.get('away_team_name_raw')
        item['away_team_name_normalized'] = match_data.get('away_team_name_normalized')
        item['channel_list'] = match_data.get('channel_list', [])
        item['pagination_cursor'] = match_data.get('pagination_cursor')
        item['source_match_text'] = match_data.get('source_match_text')
        item['page_url'] = match_data.get('page_url')
        item['crawled_at'] = datetime.utcnow()
        
        return item
    
    def _create_task_item(self, task_phase=None, status=None, **kwargs):
        """创建任务 Item"""
        item = CrawlTaskItem()
        
        item['crawl_batch_id'] = self.crawl_batch_id
        item['league_config_id'] = int(self.league_config_id) if self.league_config_id else None
        item['task_phase'] = task_phase
        item['status'] = status
        item['current_pagination_cursor'] = kwargs.get('current_pagination_cursor')
        item['pagination_direction'] = kwargs.get('pagination_direction', self.current_direction)
        item['matches_crawled'] = kwargs.get('matches_crawled', self.matches_crawled)
        item['error_message'] = kwargs.get('error_message')
        
        return item
    
    def pause_for_captcha(self, league_config_id, page_url, captcha_type, screenshot_path):
        """暂停处理验证码"""
        self.captcha_detected = True
        self.logger.warning(f"Captcha detected ({captcha_type})")
        self.logger.info(f"Screenshot: {screenshot_path}")
        
        return CaptchaDetectedItem(
            league_config_id=league_config_id,
            page_url=page_url,
            detected_at=datetime.utcnow(),
            captcha_type=captcha_type,
            screenshot_path=screenshot_path
        )
