from DrissionPage import ChromiumPage, ChromiumOptions
import os
import shutil

def test_manual_chrome():
    # 每次运行前清理
    user_data_path = '/tmp/chrome_test_profile'
    if os.path.exists(user_data_path):
        shutil.rmtree(user_data_path)

    print("1. 正在配置选项...")
    co =ChromiumOptions()
    
    # 指定路径
    co.set_browser_path('/opt/chrome-linux64/chrome')
    
    # --- root 用户必备参数 ---
    co.set_argument('--no-sandbox')               # 必须：root 运行的核心
    co.set_argument('--disable-setuid-sandbox')    禁用沙盒
    co.set_argument('--headless=new')             # 必须：无界面模式
    
    # --- 稳定性参数 ---
    co.set_argument('--disable-dev-shm-usage')    # 必须：防止内存溢出
    co.set_argument('--disable-gpu')
    co.set_argument('--remote-debugging-address=0.0.0.0') # 允许绑定地址
    
    # --- 隔离环境 ---
    co.set_local_port(9444)                       # 使用不常用的端口避免冲突
    co.set_user_data_path(user_data_path)         # 使用独立的用户数据目录
    
    print(f"2. 正在尝试启动浏览器 (端口: 9444, 用户目录: {user_data_path})...")
    
    try:
        # 启动
        page = ChromiumPage(addr_or_opts=co)
        print("3. ✅ 浏览器连接成功！")
        
        print("4. 正在测试访问...")
        page.get('https://www.baidu.com')
        print(f"5. ✅ 页面标题: {page.title}")
        
        page.quit()
        print("6. ✅ 测试完成")
    except Exception as e:
        print(f"❌ 启动失败！错误详情:\n{e}")

if __name__ == '__main__':
    test_manual_chrome()