import os
import sys
import argparse

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dotenv import load_dotenv
load_dotenv()

from utils.proxy_api_client import get_911_api_client


def list_accounts():
    """列出所有代理账户"""
    print("\n=== 代理账户列表 ===")
    
    api_client = get_911_api_client()
    
    try:
        accounts = api_client.list_proxy_accounts()
        
        if not accounts:
            print("暂无代理账户")
            return
        
        print(f"共 {len(accounts)} 个账户:\n")
        print(f"{'用户名':<20} {'状态':<8} {'已用流量':<15} {'流量限制':<15} {'备注'}")
        print("-" * 80)
        
        for account in accounts:
            status_text = "启用" if account['status'] == 1 else "禁用"
            used_mb = account['usage_flow'] / 1024
            limit_gb = account['limit_flow'] / 1024 / 1024 if account['limit_flow'] > 0 else "无限制"
            
            print(f"{account['username']:<20} {status_text:<8} {used_mb:<15.2f}MB {str(limit_gb):<15} {account['remark']}")
        
    except Exception as e:
        print(f"获取账户列表失败: {e}")


def create_account(username: str, password: str, product_type: int = 9, remark: str = ""):
    """创建代理账户"""
    print(f"\n=== 创建代理账户 ===")
    print(f"用户名: {username}")
    print(f"密码: {password}")
    print(f"套餐类型: {product_type}")
    print(f"备注: {remark}")
    
    api_client = get_911_api_client()
    
    try:
        accounts_str = f"{username}:{password}"
        success = api_client.create_proxy_account(
            accounts=accounts_str,
            product_type=product_type,
            remark=remark
        )
        
        if success:
            print("✓ 代理账户创建成功")
        else:
            print("✗ 代理账户创建失败")
        
    except Exception as e:
        print(f"创建账户失败: {e}")


def delete_account(username: str):
    """删除代理账户"""
    print(f"\n=== 删除代理账户 ===")
    print(f"用户名: {username}")
    
    confirm = input("确认删除？(y/N): ")
    if confirm.lower() != 'y':
        print("已取消")
        return
    
    api_client = get_911_api_client()
    
    try:
        success = api_client.delete_proxy_account(accounts=username)
        
        if success:
            print("✓ 代理账户已删除")
        else:
            print("✗ 代理账户删除失败")
        
    except Exception as e:
        print(f"删除账户失败: {e}")


def enable_account(username: str):
    """启用代理账户"""
    print(f"\n=== 启用代理账户 ===")
    print(f"用户名: {username}")
    
    api_client = get_911_api_client()
    
    try:
        success = api_client.enable_proxy_account(accounts=username)
        
        if success:
            print("✓ 代理账户已启用")
        else:
            print("✗ 代理账户启用失败")
        
    except Exception as e:
        print(f"启用账户失败: {e}")


def disable_account(username: str):
    """禁用代理账户"""
    print(f"\n=== 禁用代理账户 ===")
    print(f"用户名: {username}")
    
    api_client = get_911_api_client()
    
    try:
        success = api_client.disable_proxy_account(accounts=username)
        
        if success:
            print("✓ 代理账户已禁用")
        else:
            print("✗ 代理账户禁用失败")
        
    except Exception as e:
        print(f"禁用账户失败: {e}")


def set_traffic_limit(username: str, limit_gb: int):
    """设置流量限制"""
    print(f"\n=== 设置流量限制 ===")
    print(f"用户名: {username}")
    print(f"流量限制: {limit_gb} GB")
    
    api_client = get_911_api_client()
    
    try:
        success = api_client.set_proxy_account_traffic_limit(
            account=username,
            limit_gb=limit_gb
        )
        
        if success:
            print("✓ 流量限制已设置")
        else:
            print("✗ 流量限制设置失败")
        
    except Exception as e:
        print(f"设置流量限制失败: {e}")


def get_traffic_stats(username: str = None):
    """获取流量统计"""
    print(f"\n=== 流量统计 ===")
    if username:
        print(f"用户名: {username}")
    else:
        print("所有账户")
    
    api_client = get_911_api_client()
    
    try:
        traffic_data = api_client.get_daily_traffic(username=username)
        
        if not traffic_data:
            print("暂无流量数据")
            return
        
        print(f"\n{'日期':<15} {'流量 (MB)':<15}")
        print("-" * 30)
        
        for data in traffic_data:
            date = data['day']
            flow_mb = data['flow'] / 1024
            print(f"{date:<15} {flow_mb:<15.2f}")
        
    except Exception as e:
        print(f"获取流量统计失败: {e}")


def get_proxy_ips(country: str = None, num: int = 10):
    """获取代理 IP 列表"""
    print(f"\n=== 获取代理 IP ===")
    print(f"国家: {country or '任意'}")
    print(f"数量: {num}")
    
    api_client = get_911_api_client()
    
    try:
        ips = api_client.get_proxy_ips(
            country_code=country,
            num=num,
            life=30
        )
        
        if not ips:
            print("未获取到代理 IP")
            return
        
        print(f"\n获取到 {len(ips)} 个代理 IP:")
        for i, ip in enumerate(ips, 1):
            print(f"{i}. {ip}")
        
    except Exception as e:
        print(f"获取代理 IP 失败: {e}")


def main():
    parser = argparse.ArgumentParser(description='911proxy 代理账户管理工具')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    list_parser = subparsers.add_parser('list', help='列出所有代理账户')
    
    create_parser = subparsers.add_parser('create', help='创建代理账户')
    create_parser.add_argument('username', help='用户名')
    create_parser.add_argument('password', help='密码')
    create_parser.add_argument('--type', type=int, default=9, help='套餐类型（默认9: 动态住宅代理）')
    create_parser.add_argument('--remark', default='', help='备注')
    
    delete_parser = subparsers.add_parser('delete', help='删除代理账户')
    delete_parser.add_argument('username', help='用户名')
    
    enable_parser = subparsers.add_parser('enable', help='启用代理账户')
    enable_parser.add_argument('username', help='用户名')
    
    disable_parser = subparsers.add_parser('disable', help='禁用代理账户')
    disable_parser.add_argument('username', help='用户名')
    
    limit_parser = subparsers.add_parser('limit', help='设置流量限制')
    limit_parser.add_argument('username', help='用户名')
    limit_parser.add_argument('gb', type=int, help='流量限制（GB），0表示不限制')
    
    traffic_parser = subparsers.add_parser('traffic', help='查看流量统计')
    traffic_parser.add_argument('--username', help='用户名（可选）')
    
    ips_parser = subparsers.add_parser('ips', help='获取代理 IP 列表')
    ips_parser.add_argument('--country', help='国家代码（如 US）')
    ips_parser.add_argument('--num', type=int, default=10, help='获取数量')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    api_client = get_911_api_client()
    if not api_client.api_key:
        print("错误: 未配置 911proxy API Key")
        print("请在 .env 文件中设置 PROXY_API_KEY")
        return
    
    if args.command == 'list':
        list_accounts()
    elif args.command == 'create':
        create_account(args.username, args.password, args.type, args.remark)
    elif args.command == 'delete':
        delete_account(args.username)
    elif args.command == 'enable':
        enable_account(args.username)
    elif args.command == 'disable':
        disable_account(args.username)
    elif args.command == 'limit':
        set_traffic_limit(args.username, args.gb)
    elif args.command == 'traffic':
        get_traffic_stats(args.username)
    elif args.command == 'ips':
        get_proxy_ips(args.country, args.num)


if __name__ == "__main__":
    main()
