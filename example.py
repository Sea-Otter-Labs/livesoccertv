from DrissionPage import ChromiumPage

# 实例化一个页面对象
page = ChromiumPage()

url_page = 1
url = f"https://www.livesoccertv.com/competitions/spain/primera-division/"
# 访问网址
page.get(url)

# 等待人机验证出现并自动处理（DrissionPage 通常能直接无感通过）
# 如果页面有明显的验证框，可以尝试等待几秒
page.wait(10)

# # 如果需要保存
with open('livesoccertv-detail.html', 'w', encoding='utf-8') as f:
    f.write(page.html)

