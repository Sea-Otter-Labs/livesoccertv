# API-Football + LiveSoccerTV 数据整合项目

## 项目结构

```
.
├── config/                     # 配置模块
│   └── database.py            # 数据库连接配置
├── models/                     # 数据模型 (SQLAlchemy)
│   ├── base.py
│   ├── league_config.py
│   ├── api_fixture.py
│   ├── web_crawl_raw.py
│   ├── match_broadcast.py
│   ├── alert_log.py
│   ├── team_name_mapping.py
│   ├── crawl_task_status.py
│   └── system_config.py
├── repo/                       # 数据访问层 (Repository)
│   ├── base_repo.py
│   ├── league_config_repo.py
│   ├── api_fixture_repo.py
│   ├── web_crawl_raw_repo.py
│   ├── match_broadcast_repo.py
│   ├── alert_log_repo.py
│   ├── team_name_mapping_repo.py
│   ├── crawl_task_status_repo.py
│   └── system_config_repo.py
├── services/                   # 业务服务层
│   ├── api_football_client.py    # API-Football 客户端
│   ├── api_football_sync.py      # API 数据同步服务
│   └── daily_task.py             # 每日任务协调器
├── crawler/                    # Scrapy 爬虫项目
│   ├── crawler/
│   │   ├── settings.py
│   │   ├── items.py
│   │   ├── spiders/
│   │   │   └── livesoccertv_spider.py
│   │   ├── middlewares/
│   │   │   ├── drission_middleware.py
│   │   │   └── captcha_middleware.py
│   │   ├── pipelines/
│   │   │   └── match_pipeline.py
│   │   └── utils/
│   │       └── helpers.py
│   └── launcher.py
├── utils/                      # 工具函数
│   ├── team_normalizer.py     # 球队名称标准化
│   ├── time_utils.py          # 时间处理
│   └── match_aligner.py       # 比赛对齐逻辑
├── schema.sql                  # MySQL 数据库结构
├── requirements.txt            # Python 依赖
└── run_daily_task.py          # 每日任务运行脚本
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `.env.example` 到 `.env` 并修改配置：

```bash
cp .env.example .env
```

编辑 `.env` 文件：

```env
API_FOOTBALL_KEY=your_api_key_here
DB_HOST=localhost
DB_PORT=3306
DB_NAME=football_broadcasts
DB_USER=root
DB_PASSWORD=your_password
```

### 3. 创建数据库

```bash
mysql -u root -p < schema.sql
```

### 4. 运行每日任务

```bash
# 设置环境变量并运行（Windows）
set API_FOOTBALL_KEY=your_api_key
python run_daily_task.py

# 或（Linux/Mac）
export API_FOOTBALL_KEY=your_api_key
python run_daily_task.py
```

### 5. 启动查询 API

```bash
python api/app.py
```

## 项目入口

- `python run_daily_task.py`：唯一的主任务入口，按顺序执行 API 同步、网页抓取、比赛对齐
- `python api/app.py`：唯一的查询 API 入口
- `services/`、`repo/`、`models/`、`livesoccertv_crawler/`：内部模块，不作为直接运行入口

## 功能模块

### 1. API-Football 数据同步

- 自动同步多个联赛的比赛数据
- 支持增量同步（默认过去7天+未来7天）
- 支持全量同步（整个赛季）
- 球队名称自动标准化

### 2. LiveSoccerTV 网页抓取

- 使用 DrissionPage 控制真实浏览器
- 动态翻页抓取（历史+未来）
- 验证码检测与人工处理
- 频道信息提取

### 3. 比赛对齐

- 基于时间容差和球队名称匹配
- 支持球队名称别名映射
- 匹配置信度计算
- 未匹配/歧义告警生成

### 4. 数据存储

- MySQL 数据库存储
- SQLAlchemy 2.0 + asyncio 异步操作
- 分层架构（Model + Repository）

## API-Football 密钥获取

1. 访问 [API-Football](https://www.api-football.com/)
2. 注册账号并获取 API 密钥
3. 免费版有每日请求限制，注意控制频率

## 注意事项

1. 首次运行会创建数据库表（如果尚不存在）
2. 爬虫使用真实浏览器，首次运行可能需要安装 Chromium
3. 遇到验证码时会自动暂停等待人工处理
4. 建议在定时任务中运行（如 cron）
5. **强烈建议配置代理以提升爬取稳定性**（详见 [911proxy 代理配置](#911proxy-代理配置)）

## 911proxy 代理配置

### 为什么需要代理？

- ✅ 避免频繁请求导致 IP 被封禁
- ✅ 模拟真实用户行为，提升爬取成功率
- ✅ 支持 IP 自动轮换，降低检测风险
- ✅ 地理位置定位，访问地区限制内容

### 快速配置

1. **获取 911proxy 账户**
   - 访问 [911proxy 官网](https://www.911proxy.com/) 注册账户
   - 购买动态住宅代理套餐（推荐 product_type=9）
   - 获取 API Key（用于账户管理，可选）

2. **创建代理账户**
   
   方式 1：通过控制台
   ```bash
   # 登录 911proxy 控制台
   # 导航到"代理账户管理" -> "添加账户"
   # 输入用户名和密码，选择套餐类型
   ```
   
   方式 2：使用命令行工具
   ```bash
   python manage_proxy_accounts.py create your_username your_password --type 9 --remark "爬虫专用"
   ```

3. **配置环境变量**
   
   编辑 `.env` 文件：
   ```env
   # 911proxy 代理配置
   PROXY_ENABLED=true
   PROXY_HOST=proxy.911proxy.com
   PROXY_PORT=8080
   PROXY_USERNAME=your_username
   PROXY_PASSWORD=your_password
   
   # 911proxy API Key（可选，用于账户管理）
   PROXY_API_KEY=your_api_key_here
   ```

4. **测试代理配置**
   ```bash
   python tests/test_proxy_manager.py
   ```

5. **运行爬虫**
   ```bash
   # 代理会自动启用（通过 PROXY_ENABLED 控制）
   python run_daily_task.py
   ```

### 代理管理工具

查看所有代理账户：
```bash
python manage_proxy_accounts.py list
```

创建新账户：
```bash
python manage_proxy_accounts.py create username password --type 9
```

查看流量统计：
```bash
python manage_proxy_accounts.py traffic --username username
```

获取代理 IP 列表：
```bash
python manage_proxy_accounts.py ips --country US --num 10
```

### 高级用法

- **动态 IP 轮换**：911proxy 自动处理 IP 轮换，无需手动管理
- **地理位置定位**：支持按国家/州/城市筛选代理 IP
- **流量监控**：实时查看流量使用情况，避免超额
- **账户管理**：通过 API 或命令行工具管理多个代理账户

### 常见问题

**Q: 代理连接失败怎么办？**
- 检查 `.env` 配置是否正确
- 确认代理账户状态是否为"启用"
- 查看流量是否已用完
- 尝试使用 911proxy 提供的其他代理网关地址

**Q: 如何临时禁用代理？**
```bash
# 在 .env 文件中设置
PROXY_ENABLED=false
```

**Q: 如何查看代理是否生效？**
```bash
python tests/example_proxy_usage.py
```

**Q: 支持哪些代理类型？**
- 动态住宅代理（product_type=9, 11）：IP 自动轮换，适合爬虫
- 静态住宅代理（product_type=25）：固定 IP，适合需要稳定 IP 的场景
- 数据中心代理（product_type=14）：速度快但容易被识别

### 相关文件

- `utils/proxy_manager.py` - 代理配置管理器
- `utils/proxy_api_client.py` - 911proxy API 客户端
- `manage_proxy_accounts.py` - 代理账户管理脚本
- `tests/test_proxy_manager.py` - 代理功能测试
- `tests/example_proxy_usage.py` - 使用示例
- `config/settings.py` - 代理配置项
- `.env.example` - 环境变量示例

## 开发调试

```bash
# 正式入口：运行完整任务
python run_daily_task.py

# 正式入口：启动 API
python api/app.py
```

## License

MIT
