import scrapy
from typing import Optional, List


class LiveSoccerTVMatchItem(scrapy.Item):
    """LiveSoccerTV 比赛数据 Item"""
    
    # 元数据
    crawl_batch_id = scrapy.Field()      # 抓取批次ID
    source_site = scrapy.Field()         # 来源站点
    league_config_id = scrapy.Field()    # 联赛配置ID
    
    # 联赛信息
    league_name = scrapy.Field()         # 联赛名称
    
    # 比赛信息
    match_date_text = scrapy.Field()     # 比赛日期原始文本
    match_timestamp_utc = scrapy.Field() # 比赛时间 UTC 时间戳
    match_time_text = scrapy.Field()     # 比赛时间原始文本
    
    # 球队信息
    home_team_name_raw = scrapy.Field()  # 主队名称原始文本
    home_team_name_normalized = scrapy.Field()  # 主队名称标准化后
    away_team_name_raw = scrapy.Field()  # 客队名称原始文本
    away_team_name_normalized = scrapy.Field()  # 客队名称标准化后
    
    # 频道信息
    channel_list = scrapy.Field()        # 频道列表
    
    # 抓取元数据
    pagination_cursor = scrapy.Field()   # 分页游标
    source_match_text = scrapy.Field()   # 内部解析文本
    page_url = scrapy.Field()            # 抓取来源URL
    crawled_at = scrapy.Field()          # 抓取时间


class CrawlTaskItem(scrapy.Item):
    """爬虫任务状态 Item"""
    
    crawl_batch_id = scrapy.Field()
    league_config_id = scrapy.Field()
    task_phase = scrapy.Field()
    status = scrapy.Field()
    current_pagination_cursor = scrapy.Field()
    pagination_direction = scrapy.Field()
    matches_crawled = scrapy.Field()
    error_message = scrapy.Field()


class CaptchaDetectedItem(scrapy.Item):
    """验证码检测 Item"""
    
    league_config_id = scrapy.Field()
    page_url = scrapy.Field()
    detected_at = scrapy.Field()
    captcha_type = scrapy.Field()
    screenshot_path = scrapy.Field()
