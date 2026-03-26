"""
API-Football 客户端
用于获取比赛数据
"""

import logging
import aiohttp
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ApiFootballClient:
    """
    API-Football 客户端
    官方文档: https://www.api-football.com/documentation-v3
    """
    
    BASE_URL = "https://v3.football.api-sports.io"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': "v3.football.api-sports.io"
        }
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.session:
            await self.session.close()
    
    async def _make_request(
        self,
        endpoint: str,
        params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        发起 API 请求
        
        Args:
            endpoint: API 端点路径
            params: 查询参数
        
        Returns:
            API 响应数据
        """
        if not self.session:
            raise RuntimeError("Client not initialized. Use async with context.")
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                else:
                    error_text = await response.text()
                    logger.error(f"API request failed: {response.status} - {error_text}")
                    return {'errors': [{'message': f'HTTP {response.status}'}]}
        
        except Exception as e:
            logger.error(f"API request error: {e}")
            return {'errors': [{'message': str(e)}]}
    
    async def get_fixtures(
        self,
        league_id: Optional[int] = None,
        season: Optional[int] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        fixture_id: Optional[int] = None,
        team_id: Optional[int] = None,
        live: Optional[bool] = None,
        timezone: str = 'UTC'
    ) -> List[Dict[str, Any]]:
        """
        获取比赛数据 (fixtures)
        
        Args:
            league_id: 联赛 ID
            season: 赛季年份
            from_date: 开始日期 (YYYY-MM-DD)
            to_date: 结束日期 (YYYY-MM-DD)
            fixture_id: 特定比赛 ID
            team_id: 球队 ID
            live: 是否只获取进行中比赛
            timezone: 时区
        
        Returns:
            比赛数据列表
        """
        params = {'timezone': timezone}
        
        if league_id:
            params['league'] = league_id
        if season:
            params['season'] = season
        if from_date:
            params['from'] = from_date
        if to_date:
            params['to'] = to_date
        if fixture_id:
            params['id'] = fixture_id
        if team_id:
            params['team'] = team_id
        if live:
            params['live'] = 'all'
        
        data = await self._make_request('fixtures', params)
        
        if 'response' in data:
            logger.info(f"Retrieved {len(data['response'])} fixtures")
            return data['response']
        
        if 'errors' in data:
            logger.error(f"API errors: {data['errors']}")
        
        return []
    
    async def get_leagues(
        self,
        league_id: Optional[int] = None,
        country: Optional[str] = None,
        season: Optional[int] = None,
        team_id: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        获取联赛信息
        
        Args:
            league_id: 联赛 ID
            country: 国家名称
            season: 赛季
            team_id: 球队 ID
        
        Returns:
            联赛数据列表
        """
        params = {}
        
        if league_id:
            params['id'] = league_id
        if country:
            params['country'] = country
        if season:
            params['season'] = season
        if team_id:
            params['team'] = team_id
        
        data = await self._make_request('leagues', params)
        
        return data.get('response', [])
    
    async def get_teams(
        self,
        team_id: Optional[int] = None,
        league_id: Optional[int] = None,
        season: Optional[int] = None,
        country: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取球队信息
        
        Args:
            team_id: 球队 ID
            league_id: 联赛 ID
            season: 赛季
            country: 国家
        
        Returns:
            球队数据列表
        """
        params = {}
        
        if team_id:
            params['id'] = team_id
        if league_id:
            params['league'] = league_id
        if season:
            params['season'] = season
        if country:
            params['country'] = country
        
        data = await self._make_request('teams', params)
        
        return data.get('response', [])
    
    async def get_fixtures_by_date_range(
        self,
        league_id: int,
        season: int,
        days_back: int = 7,
        days_forward: int = 7
    ) -> List[Dict[str, Any]]:
        """
        获取指定时间窗口内的比赛
        
        Args:
            league_id: 联赛 ID
            season: 赛季
            days_back: 往回推的天数
            days_forward: 往后推的天数
        
        Returns:
            比赛数据列表
        """
        today = datetime.now()
        
        from_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
        to_date = (today + timedelta(days=days_forward)).strftime('%Y-%m-%d')
        
        logger.info(
            f"Fetching fixtures for league {league_id}, season {season}, "
            f"from {from_date} to {to_date}"
        )
        
        return await self.get_fixtures(
            league_id=league_id,
            season=season,
            from_date=from_date,
            to_date=to_date
        )
    
    async def get_all_season_fixtures(
        self,
        league_id: int,
        season: int
    ) -> List[Dict[str, Any]]:
        """
        获取整个赛季的所有比赛
        
        Args:
            league_id: 联赛 ID
            season: 赛季
        
        Returns:
            比赛数据列表
        """
        logger.info(f"Fetching all fixtures for league {league_id}, season {season}")
        
        return await self.get_fixtures(
            league_id=league_id,
            season=season
        )


# 便捷函数
async def fetch_league_fixtures(
    api_key: str,
    league_id: int,
    season: int,
    days_back: int = 7,
    days_forward: int = 7
) -> List[Dict[str, Any]]:
    """
    便捷函数：获取联赛比赛数据
    
    Args:
        api_key: API 密钥
        league_id: 联赛 ID
        season: 赛季
        days_back: 往回推天数
        days_forward: 往后推天数
    
    Returns:
        比赛数据列表
    """
    async with ApiFootballClient(api_key) as client:
        return await client.get_fixtures_by_date_range(
            league_id=league_id,
            season=season,
            days_back=days_back,
            days_forward=days_forward
        )
