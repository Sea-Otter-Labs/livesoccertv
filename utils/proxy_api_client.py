import os
import logging
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class Proxy911APIClient:
    """
    911proxy API 客户端
    
    功能：
    - 代理账户管理（创建、删除、启用、禁用）
    - 流量查询
    - 代理 IP 获取
    - 订单管理
    """
    
    BASE_URL = "https://api.911proxy.com"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('PROXY_API_KEY')
        if not self.api_key:
            logger.warning("911proxy API key not configured, API features will be disabled")
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        发起 API 请求
        
        Args:
            method: HTTP 方法（GET, POST）
            endpoint: API 端点
            params: 查询参数
            data: POST 数据
        
        Returns:
            API 响应数据
        """
        if not self.api_key:
            raise ValueError("API key is required but not configured")
        
        url = f"{self.BASE_URL}{endpoint}"
        
        if params is None:
            params = {}
        params['app_key'] = self.api_key
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, params=params, timeout=30)
            elif method.upper() == 'POST':
                response = requests.post(url, params=params, json=data, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            result = response.json()
            
            if result.get('code') != 200:
                logger.error(f"API request failed: {result.get('msg')}")
                raise Exception(f"API error: {result.get('msg')}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request error: {e}")
            raise
    
    # ==================== 代理账户管理 ====================
    
    def list_proxy_accounts(self) -> List[Dict[str, Any]]:
        """
        获取代理账户列表
        
        Returns:
            代理账户列表
        """
        result = self._make_request('GET', '/api-gate-way/whitelist-account/list')
        return result.get('data', {}).get('list', [])
    
    def create_proxy_account(
        self,
        accounts: str,
        product_type: int = 9,
        remark: str = ""
    ) -> bool:
        """
        创建代理账户
        
        Args:
            accounts: 账户密码，格式 "username:password"，多个用逗号分隔
            product_type: 套餐类型（9: 动态住宅流量包, 11: 动态住宅IP, 14: 静态数据中心IP, 25: 静态住宅IP）
            remark: 备注说明
        
        Returns:
            是否创建成功
        """
        data = {
            'accounts': accounts,
            'product_type': product_type,
            'remark': remark,
        }
        
        result = self._make_request('POST', '/api-gate-way/whitelist-account/add', data=data)
        logger.info(f"Proxy account created: {accounts}")
        return result.get('code') == 200
    
    def delete_proxy_account(self, accounts: str) -> bool:
        """
        删除代理账户
        
        Args:
            accounts: 账户名，多个用逗号分隔
        
        Returns:
            是否删除成功
        """
        data = {'accounts': accounts}
        result = self._make_request('POST', '/api-gate-way/whitelist-account/delete', data=data)
        logger.info(f"Proxy account deleted: {accounts}")
        return result.get('code') == 200
    
    def enable_proxy_account(self, accounts: str) -> bool:
        """
        启用代理账户
        
        Args:
            accounts: 账户名，多个用逗号分隔
        
        Returns:
            是否启用成功
        """
        data = {'accounts': accounts}
        result = self._make_request('POST', '/api-gate-way/whitelist-account/enable', data=data)
        logger.info(f"Proxy account enabled: {accounts}")
        return result.get('code') == 200
    
    def disable_proxy_account(self, accounts: str) -> bool:
        """
        禁用代理账户
        
        Args:
            accounts: 账户名，多个用逗号分隔
        
        Returns:
            是否禁用成功
        """
        data = {'accounts': accounts}
        result = self._make_request('POST', '/api-gate-way/whitelist-account/disable', data=data)
        logger.info(f"Proxy account disabled: {accounts}")
        return result.get('code') == 200
    
    def change_proxy_account_password(
        self,
        account: str,
        new_password: str
    ) -> bool:
        """
        修改代理账户密码
        
        Args:
            account: 账户名
            new_password: 新密码
        
        Returns:
            是否修改成功
        """
        data = {
            'account': account,
            'password': new_password,
        }
        result = self._make_request('POST', '/api-gate-way/whitelist-account/change-password', data=data)
        logger.info(f"Proxy account password changed: {account}")
        return result.get('code') == 200
    
    def set_proxy_account_traffic_limit(
        self,
        account: str,
        limit_gb: int
    ) -> bool:
        """
        设置代理账户流量限制
        
        Args:
            account: 账户名
            limit_gb: 流量限制（GB），0 表示不限制
        
        Returns:
            是否设置成功
        """
        data = {
            'account': account,
            'limit': limit_gb,
        }
        result = self._make_request('POST', '/api-gate-way/whitelist-account/change-limit', data=data)
        logger.info(f"Proxy account traffic limit set: {account} -> {limit_gb}GB")
        return result.get('code') == 200
    
    # ==================== 流量查询 ====================
    
    def get_daily_traffic(
        self,
        username: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        product_type: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        获取每日流量统计
        
        Args:
            username: 子账户名（可选，默认查询所有）
            start_time: 开始时间（YYYY-MM-DD HH:MM:SS）
            end_time: 结束时间（YYYY-MM-DD HH:MM:SS）
            product_type: 套餐类型
        
        Returns:
            每日流量统计列表
        """
        params = {}
        if username:
            params['username'] = username
        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time
        if product_type:
            params['product_type'] = product_type
        
        result = self._make_request('GET', '/api-gate-way/user-usage-flow/total', params=params)
        return result.get('data', {}).get('list', [])
    
    # ==================== 代理 IP 获取 ====================
    
    def get_proxy_ips(
        self,
        country_code: Optional[str] = None,
        state: Optional[str] = None,
        city: Optional[str] = None,
        num: int = 10,
        life: int = 30,
        format: str = "json"
    ) -> List[str]:
        """
        获取代理 IP 列表
        
        Args:
            country_code: 国家代码（如 US, GB, JP）
            state: 州/省
            city: 城市
            num: 获取数量
            life: IP 保留时长（分钟）
            format: 返回格式（json 或 text）
        
        Returns:
            代理 IP 列表
        """
        params = {
            'num': num,
            'life': life,
            'format': format,
        }
        
        if country_code:
            params['cc'] = country_code
        if state:
            params['state'] = state
        if city:
            params['city'] = city
        
        result = self._make_request('GET', '/api-gate-way/ip/v3', params=params)
        
        data = result.get('data', {})
        if 'list' in data and data['list']:
            return data['list'][0] if isinstance(data['list'][0], list) else data['list']
        return []
    
    def get_available_countries(self) -> List[Dict[str, Any]]:
        """
        获取可用的国家/地区列表
        
        Returns:
            国家/地区列表
        """
        result = self._make_request('GET', '/api-gate-way/ip/dynamic-citys')
        return result.get('data', {}).get('list', [])
    
    def get_available_states(self, country_code: str) -> List[str]:
        """
        获取指定国家的州/省列表
        
        Args:
            country_code: 国家代码
        
        Returns:
            州/省列表
        """
        params = {'country_code': country_code}
        result = self._make_request('GET', '/api-gate-way/ip/dynamic-states/search', params=params)
        return result.get('data', {}).get('list', [])
    
    def get_available_cities(self, country_code: str, state: str) -> List[str]:
        """
        获取指定州的城市列表
        
        Args:
            country_code: 国家代码
            state: 州/省代码
        
        Returns:
            城市列表
        """
        params = {
            'country_code': country_code,
            'state': state,
        }
        result = self._make_request('GET', '/api-gate-way/ip/dynamic-citys/search', params=params)
        return result.get('data', {}).get('list', [])
    
    # ==================== 套餐查询 ====================
    
    def get_product_list(
        self,
        product_type: Optional[int] = None,
        page: int = 1,
        size: int = 20
    ) -> Dict[str, Any]:
        """
        获取已购买套餐列表
        
        Args:
            product_type: 套餐类型
            page: 页码
            size: 每页数量
        
        Returns:
            套餐列表和分页信息
        """
        params = {
            'page': page,
            'size': size,
        }
        if product_type:
            params['product_type'] = product_type
        
        result = self._make_request('GET', '/api-gate-way/user-product/list', params=params)
        return result.get('data', {})
    
    def get_product_summary(self, product_type: Optional[int] = None) -> Dict[str, Any]:
        """
        获取套餐统计摘要
        
        Args:
            product_type: 套餐类型（9: 动态流量包, 12: 长期IDC流量包）
        
        Returns:
            套餐统计信息
        """
        params = {}
        if product_type:
            params['product_type'] = product_type
        
        result = self._make_request('GET', '/api-gate-way/user-product/summary', params=params)
        return result.get('data', {})


_911_api_client_instance: Optional[Proxy911APIClient] = None


def get_911_api_client() -> Proxy911APIClient:
    """
    获取 911proxy API 客户端单例实例
    
    Returns:
        Proxy911APIClient 实例
    """
    global _911_api_client_instance
    if _911_api_client_instance is None:
        _911_api_client_instance = Proxy911APIClient()
    return _911_api_client_instance
