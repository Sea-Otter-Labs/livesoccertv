import os
import sys
from pathlib import Path

_loaded = False


def load_env_file(env_file: str = None):
    """
    加载 .env 文件到环境变量
    
    Args:
        env_file: .env 文件路径，默认为项目根目录下的 .env
    """
    global _loaded
    
    if _loaded:
        return True
    
    try:
        from dotenv import load_dotenv
        
        # 确定环境变量文件路径
        if env_file:
            env_path = Path(env_file)
        else:
            # 默认使用项目根目录的 .env 文件
            project_root = Path(__file__).parent.parent
            env_path = project_root / '.env'
        
        if env_path.exists():
            load_dotenv(env_path)
            print(f"✓ 已加载环境变量文件: {env_path}")
            _loaded = True
            return True
        else:
            print(f"⚠️  未找到环境变量文件: {env_path}")
            print("提示：请复制 .env.example 为 .env 并配置参数")
            return False
        
    except ImportError:
        print("⚠️  python-dotenv 未安装，无法加载 .env 文件")
        print("提示：运行 'pip install python-dotenv' 安装")
        return False


def ensure_env_loaded():
    """
    确保环境变量已加载
    可在需要的地方调用
    """
    if not _loaded:
        return load_env_file()
    return True


def get_env_status():
    """
    获取环境变量加载状态
    
    Returns:
        dict: 包含加载状态和相关信息的字典
    """
    project_root = Path(__file__).parent.parent
    env_path = project_root / '.env'
    
    return {
        'loaded': _loaded,
        'env_file_exists': env_path.exists(),
        'env_file_path': str(env_path),
        'project_root': str(project_root),
    }


if __name__ == "__main__":
    # 测试加载
    print("测试环境变量加载...")
    print("-" * 60)
    
    status = get_env_status()
    print(f"项目根目录: {status['project_root']}")
    print(f"环境变量文件: {status['env_file_path']}")
    print(f"文件存在: {status['env_file_exists']}")
    
    print("\n加载环境变量...")
    success = load_env_file()
    
    if success:
        print("\n检查关键环境变量:")
        env_vars = [
            'PROXY_ENABLED',
            'PROXY_HOST',
            'PROXY_PORT',
            'PROXY_USERNAME',
            'PROXY_API_KEY',
            'API_FOOTBALL_KEY',
            'DB_HOST',
        ]
        
        for var in env_vars:
            value = os.getenv(var)
            if value:
                # 隐藏敏感信息
                if 'PASSWORD' in var or 'KEY' in var or 'SECRET' in var:
                    display_value = value[:8] + '***' if len(value) > 8 else '***'
                else:
                    display_value = value
                print(f"  {var}: {display_value}")
                # 添加说明
                if var == 'PROXY_API_KEY':
                    print(f"       (仅用于管理功能，运行时代理不依赖此项)")
            else:
                print(f"  {var}: 未设置")
                if var == 'PROXY_API_KEY':
                    print(f"       (可选：仅管理功能需要，运行时代理不需要)")
    
    print("\n" + "=" * 60)
    print("环境变量加载测试完成")
