import os
import sys
import asyncio

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv
load_dotenv()

from utils.proxy_manager import (
    get_proxy_manager,
    is_proxy_enabled,
    get_proxy_for_chromium,
    get_proxy_for_requests,
    get_proxy_for_aiohttp
)


def example_requests_usage():
    """示例：使用 requests 库通过代理访问网站"""
    import requests
    
    print("\n=== 示例 1: 使用 requests 库 ===")
    
    if not is_proxy_enabled():
        print("代理未启用，跳过此示例")
        return
    
    proxy_config = get_proxy_for_requests()
    print(f"代理配置: {proxy_config}")
    
    try:
        response = requests.get(
            'https://httpbin.org/ip',
            proxies=proxy_config,
            timeout=10
        )
        print(f"响应状态码: {response.status_code}")
        print(f"当前 IP: {response.json()}")
    except Exception as e:
        print(f"请求失败: {e}")


async def example_aiohttp_usage():
    """示例：使用 aiohttp 库通过代理访问网站"""
    import aiohttp
    
    print("\n=== 示例 2: 使用 aiohttp 库 ===")
    
    if not is_proxy_enabled():
        print("代理未启用，跳过此示例")
        return
    
    proxy_url = get_proxy_for_aiohttp()
    print(f"代理 URL: {proxy_url}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                'https://httpbin.org/ip',
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                data = await response.json()
                print(f"响应状态码: {response.status}")
                print(f"当前 IP: {data}")
    except Exception as e:
        print(f"请求失败: {e}")


def example_drissionpage_usage():
    """示例：使用 DrissionPage 通过代理访问网站"""
    from DrissionPage import ChromiumPage, ChromiumOptions
    
    print("\n=== 示例 3: 使用 DrissionPage ===")
    
    if not is_proxy_enabled():
        print("代理未启用，跳过此示例")
        return
    
    chromium_config = get_proxy_for_chromium()
    print(f"Chromium 代理配置: {chromium_config}")
    
    try:
        co = ChromiumOptions()
        co.set_proxy(chromium_config['server'])
        
        page = ChromiumPage(addr_or_opts=co)
        page.get('https://httpbin.org/ip')
        
        print(f"页面标题: {page.title}")
        print(f"页面内容: {page.html[:200]}...")
        
        page.quit()
    except Exception as e:
        print(f"请求失败: {e}")


def example_api_client_usage():
    """示例：使用 911proxy API 客户端"""
    from utils.proxy_api_client import get_911_api_client
    
    print("\n=== 示例 4: 使用 911proxy API ===")
    
    api_client = get_911_api_client()
    
    if not api_client.api_key:
        print("API key 未配置，跳过此示例")
        return
    
    try:
        print("1. 获取代理账户列表...")
        accounts = api_client.list_proxy_accounts()
        print(f"找到 {len(accounts)} 个代理账户")
        
        for account in accounts[:3]:
            print(f"  - {account['username']}: {account['usage_flow']}KB / {account['limit_flow']}KB")
        
        print("\n2. 获取套餐摘要...")
        summary = api_client.get_product_summary(product_type=9)
        print(f"总流量: {summary.get('total', 0) / 1024 / 1024:.2f} GB")
        print(f"已使用: {summary.get('used', 0) / 1024 / 1024:.2f} GB")
        print(f"剩余流量: {summary.get('effective', 0) / 1024 / 1024:.2f} GB")
        
        print("\n3. 获取可用国家列表...")
        countries = api_client.get_available_countries()
        print(f"支持 {len(countries)} 个国家/地区")
        
        if countries:
            first_country = countries[0]
            print(f"  示例: {first_country['name_en']} ({first_country['country_code']})")
        
        print("\n4. 获取代理 IP 列表...")
        ips = api_client.get_proxy_ips(
            country_code='US',
            num=5,
            life=30
        )
        print(f"获取到 {len(ips)} 个代理 IP:")
        for ip in ips[:3]:
            print(f"  - {ip}")
        
    except Exception as e:
        print(f"API 调用失败: {e}")


def example_create_proxy_account():
    """示例：创建代理账户"""
    from utils.proxy_api_client import get_911_api_client
    
    print("\n=== 示例 5: 创建代理账户 ===")
    
    api_client = get_911_api_client()
    
    if not api_client.api_key:
        print("API key 未配置，跳过此示例")
        return
    
    print("注意：此示例仅演示 API 用法，不会实际创建账户")
    print("实际使用时，请取消注释相关代码")
    
    """
    try:
        success = api_client.create_proxy_account(
            accounts="testuser:testpass123",
            product_type=9,
            remark="测试账户"
        )
        
        if success:
            print("✓ 代理账户创建成功")
        else:
            print("✗ 代理账户创建失败")
    except Exception as e:
        print(f"创建失败: {e}")
    """


def main():
    """主函数"""
    print("=" * 60)
    print("911proxy 代理使用示例")
    print("=" * 60)
    
    proxy_manager = get_proxy_manager()
    status = proxy_manager.get_status()
    
    print(f"\n代理状态:")
    print(f"  启用: {status['enabled']}")
    print(f"  主机: {status['host']}")
    print(f"  端口: {status['port']}")
    print(f"  认证: {'已配置' if status['has_auth'] else '未配置'}")
    print(f"  有效: {status['is_valid']}")
    
    if not status['enabled']:
        print("\n⚠️  代理未启用，请先配置 .env 文件")
        print("示例配置:")
        print("  PROXY_ENABLED=true")
        print("  PROXY_HOST=proxy.911proxy.com")
        print("  PROXY_PORT=8080")
        print("  PROXY_USERNAME=your_username")
        print("  PROXY_PASSWORD=your_password")
        return
    
    example_requests_usage()
    
    print("\n运行 aiohttp 示例...")
    asyncio.run(example_aiohttp_usage())
    
    example_drissionpage_usage()
    
    example_api_client_usage()
    
    example_create_proxy_account()
    
    print("\n" + "=" * 60)
    print("示例运行完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
