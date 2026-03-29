"""
911proxy IP 轮换示例
演示如何动态切换代理 IP
"""

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
    """IP 轮换示例"""
    api_client = get_911_api_client()
    
    if not api_client.api_key:
        print("错误: 未配置 PROXY_API_KEY")
        print("请在 .env 文件中设置 PROXY_API_KEY")
        return
    
    print("=== IP 轮换示例 ===\n")
    
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
    print("  - IP 轮换适用于需要频繁切换 IP 的场景")
    print("  - 每次调用 get_proxy_ips() 会获取新的代理 IP")
    print("  - life 参数控制 IP 保留时长（分钟）")
    print("=" * 60)


if __name__ == "__main__":
    main()
