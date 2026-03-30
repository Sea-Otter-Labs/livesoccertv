import os
import sys
import asyncio
import logging

project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv
load_dotenv()

from utils.proxy_api_client import get_911_api_client
from DrissionPage import ChromiumPage, ChromiumOptions

logger = logging.getLogger(__name__)


async def rotate_ip_example():
    """
    IP 轮换示例（API 功能演示）
    
    注意：此示例展示如何使用 911proxy API 动态获取 IP。
    这需要有效的 API Key。如果 API Key 过期，此示例无法运行。
    
    但运行时代理（爬虫）不依赖此功能，可以直接使用固定的
    PROXY_USERNAME/PROXY_PASSWORD 配置。
    """
    api_client = get_911_api_client()
    
    if not api_client.is_available:
        print("\n" + "="*60)
        print("⚠️  未配置 911proxy API Key")
        print("="*60)
        print("\n此示例需要 API Key 才能动态获取代理 IP。")
        print("\n请在 .env 文件中设置：")
        print("  PROXY_API_KEY=your_app_key_here")
        print("\n注意：")
        print("  - 运行时代理不依赖此功能")
        print("  - 只要配置了 PROXY_HOST/PORT/USERNAME/PASSWORD")
        print("  - 爬虫就能正常使用代理，无需 API Key")
        print("="*60 + "\n")
        return
    
    print("=== IP 轮换示例（API 功能）===\n")
    
    try:
        # 获取代理 IP
        print("1. 获取代理 IP...")
        ips = api_client.get_proxy_ips(
            country_code="US",
            num=3,
            life=30
        )
        
        if not ips:
            print("未获取到代理 IP")
            return
        
        print(f"获取到 {len(ips)} 个代理 IP:")
        for i, ip in enumerate(ips, 1):
            print(f"  {i}. {ip}")
        
        # 使用第一个 IP
        if ips:
            print(f"\n2. 使用代理 IP: {ips[0]}")
            
            # 配置浏览器使用代理
            co = ChromiumOptions()
            co.set_proxy(ips[0])
            co.headless(True)
            
            # 创建浏览器实例
            page = ChromiumPage(addr_or_opts=co)
            
            # 访问测试网站
            print("3. 访问测试网站...")
            page.get('https://httpbin.org/ip')
            
            # 获取当前 IP
            print("\n当前 IP 信息:")
            print(page.html[:200])
            
            # 等待一段时间
            print("\n4. 等待 5 秒后轮换 IP...")
            await asyncio.sleep(5)
            
            # 关闭浏览器
            page.quit()
            
            print("\n✓ IP 轮换示例完成")
        
    except Exception as e:
        logger.error(f"IP 轮换失败: {e}")
        print(f"\n错误: {e}")


def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("911proxy IP 轮换示例")
    print("=" * 60)
    
    asyncio.run(rotate_ip_example())
    
    print("\n" + "=" * 60)
    print("提示:")
    print("  - 此示例展示 API 功能（动态获取 IP）")
    print("  - 运行时代理无需此功能，使用固定配置即可")
    print("  - life 参数控制 IP 保留时长（分钟）")
    print("\n运行时代理配置（不依赖 API）:")
    print("  PROXY_ENABLED=true")
    print("  PROXY_HOST=eu.911proxy.net")
    print("  PROXY_PORT=2600")
    print("  PROXY_USERNAME=your_username")
    print("  PROXY_PASSWORD=your_password")
    print("=" * 60)


if __name__ == "__main__":
    main()
