深圳民政数据采集系统

本项目是一个高效、稳定的数据采集系统，专门用于从深圳民政局网站爬取各类公共服务数据。系统采用模块化设计，支持多线程并发，能够自动化完成数据爬取、解析和存储全流程。

项目特点

• 🚀 高效爬取：基于Selenium的多线程爬取，支持智能滚动加载

• 🧩 模块化设计：清晰分离爬取、解析、存储逻辑

• 📊 数据完整性：支持数据去重和增量更新

• ⚙️ 灵活配置：通过配置文件管理所有爬取参数

• 📝 详细日志：完整记录操作日志和错误信息

• 🔒 安全可靠：完善的错误处理和重试机制

系统架构

graph TD
    A[主程序运行模块] --> B[配置管理]
    A --> C[线程池管理]
    C --> D[爬取模块]
    C --> E[解析存储模块]
    D --> F[目标网站]
    E --> G[数据库]
    
    subgraph 核心模块
    B --> H[爬虫配置]
    B --> I[数据库配置]
    C --> J[爬取线程池]
    C --> K[解析线程池]
    end


安装指南

前置要求

• Python 3.8+

• Chrome浏览器

• ChromeDriver

• MySQL 8.0+

安装步骤

1. 克隆仓库
   git clone https://github.com/zjc00129/shenzhen_mzj_spider.git
   cd shenzhen_mzj_spider
   

2. 安装依赖
   pip install -r requirements.txt
   

3. 配置数据库
   • 创建数据库：
     CREATE DATABASE spider CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
     
   • 修改 config/database_config.py 中的连接信息

4. 配置爬虫
   • 修改 config/spider_config.py 中的ChromeDriver路径

   • 根据需要调整其他爬取参数

使用说明

运行爬虫

python run_spider.py


查看日志

日志文件保存在 logs/ 目录：
• spider_system.log：系统运行日志

• spider_errors.log：错误日志

输出结果

数据存储在MySQL数据库中，表结构根据 database/table_schemas.py 自动创建

配置说明

数据库配置 (config/database_config.py)

# 数据库连接配置
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'spider',
    'port': 3306
}


爬虫配置 (config/spider_config.py)

# 主要配置项
spider_config = {
    'chromedriver_path': '/path/to/chromedriver',
    'targets': {
        'yljg': { /* 养老机构配置 */ },
        # 其他目标配置...
    },
    'browser_options': {
        'headless': True,
        # 其他浏览器选项...
    },
    'retry_config': {
        'max_retries': 3,
        # 重试策略...
    }
}


数据表结构

系统自动创建以下数据表：
• 婚姻登记机关 (marriage_registration_agencies)

• 养老机构 (elderly_care_institutions)

• 救助站 (rescue_stations)

• 等17类公共服务机构数据表

完整表结构定义见 database/table_schemas.py

贡献指南

欢迎提交Issue和Pull Request：
1. Fork 本项目
2. 创建您的特性分支 (git checkout -b feature/your-feature)
3. 提交您的更改 (git commit -am 'Add some feature')
4. 推送至分支 (git push origin feature/your-feature)
5. 创建Pull Request

许可证

本项目采用 LICENSE

免责声明

本项目仅用于技术研究和学习目的，使用者应遵守相关网站的服务条款和法律法规，禁止用于任何非法用途。开发者不承担任何因使用本项目而产生的法律责任。

深圳民政数据采集系统 © 2025