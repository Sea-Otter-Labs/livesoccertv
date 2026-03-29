import os
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass
from urllib.parse import quote

logger = logging.getLogger(__name__)


@dataclass
class ProxyConfig:
    """代理配置数据类"""
    enabled: bool
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    
    @property
    def proxy_url(self) -> Optional[str]:
        """生成代理 URL（带认证）"""
        if not self.enabled or not self.host:
            return None
        
        if self.username and self.password:
            encoded_username = quote(self.username, safe='')
            encoded_password = quote(self.password, safe='')
            return f"http://{encoded_username}:{encoded_password}@{self.host}:{self.port}"
        else:
            return f"http://{self.host}:{self.port}"
    
    @property
    def proxy_address(self) -> Optional[str]:
        """获取代理地址（不含认证信息）"""
        if not self.enabled or not self.host:
            return None
        return f"{self.host}:{self.port}"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'enabled': self.enabled,
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'password': self.password,
            'proxy_url': self.proxy_url,
            'proxy_address': self.proxy_address,
        }


class ProxyManager:
    """
    代理管理器
    负责加载、验证和管理代理配置
    """
    
    def __init__(self):
        self._config: Optional[ProxyConfig] = None
        self._load_config()
    
    def _load_config(self) -> None:
        """从环境变量加载代理配置"""
        try:
            enabled = os.getenv('PROXY_ENABLED', 'false').lower() == 'true'
            host = os.getenv('PROXY_HOST', '').strip()
            port_str = os.getenv('PROXY_PORT', '8080').strip()
            username = os.getenv('PROXY_USERNAME', '').strip() or None
            password = os.getenv('PROXY_PASSWORD', '').strip() or None
            
            try:
                port = int(port_str)
                if not (1 <= port <= 65535):
                    logger.warning(f"Invalid proxy port: {port}, using default 8080")
                    port = 8080
            except ValueError:
                logger.warning(f"Invalid proxy port format: {port_str}, using default 8080")
                port = 8080
            
            self._config = ProxyConfig(
                enabled=enabled,
                host=host,
                port=port,
                username=username,
                password=password,
            )
            
            if enabled:
                logger.info(f"Proxy configuration loaded: {host}:{port}")
                if username:
                    logger.info("Proxy authentication configured")
            else:
                logger.info("Proxy is disabled")
                
        except Exception as e:
            logger.error(f"Failed to load proxy configuration: {e}")
            self._config = ProxyConfig(enabled=False, host='', port=8080)
    
    @property
    def config(self) -> ProxyConfig:
        """获取代理配置"""
        if self._config is None:
            self._load_config()
        return self._config
    
    @property
    def is_enabled(self) -> bool:
        """检查代理是否启用"""
        return self.config.enabled and bool(self.config.host)
    
    def get_chromium_proxy_config(self) -> Dict[str, Any]:
        """
        获取适用于 Chromium 的代理配置
        
        Returns:
            Chromium 代理配置字典
        """
        if not self.is_enabled:
            return {}
        
        config = self.config
        
        proxy_config = {
            'server': f"{config.host}:{config.port}",
        }
        
        if config.username and config.password:
            proxy_config['username'] = config.username
            proxy_config['password'] = config.password
        
        logger.debug(f"Chromium proxy config: server={config.host}:{config.port}")
        return proxy_config
    
    def get_requests_proxy(self) -> Dict[str, str]:
        """
        获取适用于 requests 库的代理配置
        
        Returns:
            requests 代理字典 {'http': ..., 'https': ...}
        """
        if not self.is_enabled:
            return {}
        
        proxy_url = self.config.proxy_url
        if proxy_url:
            return {
                'http': proxy_url,
                'https': proxy_url,
            }
        return {}
    
    def get_aiohttp_proxy(self) -> Optional[str]:
        """
        获取适用于 aiohttp 的代理 URL
        
        Returns:
            代理 URL 字符串，如果未启用则返回 None
        """
        if not self.is_enabled:
            return None
        return self.config.proxy_url
    
    def validate_config(self) -> bool:
        """
        验证代理配置是否有效
        
        Returns:
            配置是否有效
        """
        if not self.is_enabled:
            logger.info("Proxy is not enabled, validation skipped")
            return True
        
        config = self.config
        
        if not config.host:
            logger.error("Proxy host is required but not configured")
            return False
        
        if not config.port:
            logger.error("Proxy port is required but not configured")
            return False
        
        if (config.username and not config.password) or (config.password and not config.username):
            logger.error("Both username and password must be provided for authentication")
            return False
        
        logger.info("Proxy configuration is valid")
        return True
    
    def reload_config(self) -> None:
        """重新加载配置（从环境变量）"""
        logger.info("Reloading proxy configuration...")
        self._load_config()
        self.validate_config()
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取代理状态信息（用于调试和监控）
        
        Returns:
            代理状态字典（隐藏敏感信息）
        """
        config = self.config
        
        status = {
            'enabled': config.enabled,
            'host': config.host if config.enabled else None,
            'port': config.port if config.enabled else None,
            'has_auth': bool(config.username and config.password),
            'is_valid': self.validate_config(),
        }
        
        if config.username:
            status['username_masked'] = f"{config.username[:2]}***" if len(config.username) > 2 else "***"
        
        return status


_proxy_manager_instance: Optional[ProxyManager] = None


def get_proxy_manager() -> ProxyManager:
    """
    获取代理管理器单例实例
    
    Returns:
        ProxyManager 实例
    """
    global _proxy_manager_instance
    if _proxy_manager_instance is None:
        _proxy_manager_instance = ProxyManager()
    return _proxy_manager_instance


def is_proxy_enabled() -> bool:
    """检查代理是否启用"""
    return get_proxy_manager().is_enabled


def get_proxy_url() -> Optional[str]:
    """获取代理 URL"""
    return get_proxy_manager().config.proxy_url


def get_proxy_for_chromium() -> Dict[str, Any]:
    """获取 Chromium 代理配置"""
    return get_proxy_manager().get_chromium_proxy_config()


def get_proxy_for_requests() -> Dict[str, str]:
    """获取 requests 代理配置"""
    return get_proxy_manager().get_requests_proxy()


def get_proxy_for_aiohttp() -> Optional[str]:
    """获取 aiohttp 代理 URL"""
    return get_proxy_manager().get_aiohttp_proxy()
