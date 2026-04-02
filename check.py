import time
from DrissionPage import ChromiumPage, ChromiumOptions

def test_local_browser():
    print("1. 正在配置浏览器选项...")
    co = ChromiumOptions()
    
    # 1. 设置浏览器路径 (请确保这里是正确的绝对路径)
    # 如果不知道路径，请在终端运行: which google-chrome
    co.set_browser_path('/usr/bin/google-chrome') 
    
    # 2. 【关键】不要设置任何 address 或 IP，让它自动分配本地端口
    
    # 3. 【关键】Linux 必须参数
    co.set_argument('--headless=new') 
    co.set_argument('--no-sandbox')
    co.set_argument('--disable-dev-shm-usage')
    co.set_argument('--disable-gpu')

    print("2. 正在尝试启动浏览器 (这可能需要 5-10 秒)...")
    
    try:
        # 初始化页面对象
        page = ChromiumPage(addr_or_opts=co)
        
        print("3. ✅ 浏览器启动成功！")
        
        # 访问一个简单的网页测试
        print("4. 正在访问百度测试...")
        page.get('https://www.baidu.com')
        
        print(f"5. ✅ 网页加载成功，标题: {page.title}")
        
        # 稍等一下看看输出
        time.sleep(2)
        
        # 关闭浏览器
        page.quit()
        print("6. ✅ 测试完成，浏览器已关闭。")
        
    except Exception as e:
        print(f"❌ 测试失败！错误信息: {e}")
        print("请检查：")
        print("1. 路径是否正确？")
        print("2. 是否安装了浏览器依赖库？")

if __name__ == '__main__':
    test_local_browser()