"""
爬虫配置模块
负责爬虫相关的所有配置参数管理
"""

import random
import os


class SpiderConfig:
    """爬虫配置类"""

    def __init__(self):
        # ================================
        # 浏览器配置 - 请根据实际情况修改
        # ================================

        # Chrome驱动路径 - 请修改为你的chromedriver实际路径
        self.chromedriver_path = 'D:/install/chromedriver-win64/chromedriver.exe'

        # 浏览器选项配置
        self.browser_options = {
            'headless': True,  # 是否无头模式运行，True为后台运行，False为显示浏览器窗口
            'disable_gpu': True,  # 禁用GPU加速，建议保持True
            'disable_images': True,  # 禁用图片加载以提高速度，可根据需要调整
            'window_size': '1920,1080',  # 浏览器窗口大小
            'disable_extensions': True,  # 禁用扩展
            'no_sandbox': True,  # 禁用沙盒模式
            'disable_dev_shm_usage': True,  # 解决资源限制问题
        }

        # 用户代理池 - 模拟不同浏览器访问，可根据需要增减
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]

        # ================================
        # 爬取行为配置 - 可根据网站情况调整
        # ================================

        # 页面加载等待时间配置
        self.timeout_config = {
            'page_load_timeout': 30,  # 页面加载超时时间(秒)
            'implicit_wait': 10,  # 隐式等待时间(秒)
            'explicit_wait': 20,  # 显式等待时间(秒)
        }

        # 滚动加载配置 - 针对所有页面都需要滚动获取的特点优化
        self.scroll_config = {
            'scroll_pause_time': 3,  # 每次滚动后等待时间(秒)，数据量不大可以稍微快一点
            'max_scroll_attempts': 5,  # 最大滚动尝试次数，增加以确保获取完整数据
            'scroll_step': 800,  # 每次滚动的像素距离，适当增大
            'check_no_more_data_times': 3,  # 检查没有更多数据的次数
        }

        # 请求间隔配置 - 模拟人为访问节奏
        self.request_delays = {
            'min_delay': 2,  # 最小延迟时间(秒)
            'max_delay': 5,  # 最大延迟时间(秒)
            'between_pages': (3, 8),  # 页面间访问延迟范围(秒)
        }

        # 重试配置
        self.retry_config = {
            'max_retries': 3,  # 最大重试次数
            'retry_delay': 5,  # 重试间隔(秒)
            'backoff_factor': 2,  # 退避因子，每次重试延迟翻倍
        }

        # ================================
        # 多线程配置
        # ================================

        self.thread_config = {
            'crawler_pool_size': 3,  # 爬取线程池大小
            'parser_pool_size': 3,  # 解析线程池大小
            'queue_max_size': 200,  # 队列最大大小，根据最大1000条数据调整
        }

        # ================================
        # 目标网站配置 - 你的具体爬取目标
        # ================================

        # 基础URL模板
        self.base_url_template = "https://mzj.sz.gov.cn/cn/isz/{point}/index.html"

        # 要爬取的所有point配置 - 从你提供的数据中提取
        self.crawl_targets = {
            # point名称: {分类ID, 描述, 表名}
            'hydjjg': {
                'categoryId': 23436,
                'description': '婚姻登记机关名单',
                'table_name': 'marriage_registration_agencies'
            },
            'gnjmsydjjg': {
                'categoryId': 23435,
                'description': '国内居民收养登记机关名单',
                'table_name': 'domestic_adoption_agencies'
            },
            'hqgatjmsydjjg': {
                'categoryId': 23434,
                'description': '华侨、港澳台居民收养登记机关名单',
                'table_name': 'overseas_adoption_agencies'
            },
            'jzz': {
                'categoryId': 23433,
                'description': '救助站名单',
                'table_name': 'rescue_stations'
            },
            'wcnrshbhsd': {
                'categoryId': 23432,
                'description': '未成年人社会保护试点名单',
                'table_name': 'minor_protection_pilots'
            },
            'cscs': {
                'categoryId': 23431,
                'description': '慈善超市名单',
                'table_name': 'charity_supermarkets'
            },
            'sqlnrrjzlzx': {
                'categoryId': 23430,
                'description': '社区老年人日间照料中心名单',
                'table_name': 'community_elderly_care_centers'
            },
            'zzft': {
                'categoryId': 103605,
                'description': '深圳市长者饭堂名册',
                'table_name': 'elderly_dining_halls'
            },
            'jzjsd': {
                'categoryId': 23427,
                'description': '捐助接收点名单',
                'table_name': 'donation_receiving_points'
            },
            'bzfwdw': {
                'categoryId': 23426,
                'description': '殡葬服务单位名单',
                'table_name': 'funeral_service_units'
            },
            'jyxgm': {
                'categoryId': 23425,
                'description': '经营性公墓名单',
                'table_name': 'commercial_cemeteries'
            },
            'gyxgm': {
                'categoryId': 23424,
                'description': '公益性公墓名单',
                'table_name': 'public_welfare_cemeteries'
            },
            'yljg': {
                'categoryId': 23422,
                'description': '养老机构名单',
                'table_name': 'elderly_care_institutions'
            },
            'etfljg': {
                'categoryId': 23421,
                'description': '儿童福利服务机构名单',
                'table_name': 'children_welfare_institutions'
            },
            'xzqhgkb': {
                'categoryId': 23419,
                'description': '行政区划概况表',
                'table_name': 'administrative_divisions'
            },
            'sggw': {
                'categoryId': 23416,
                'description': '社工岗位名单',
                'table_name': 'social_worker_positions'
            },
            'jsshjzkwfw': {
                'categoryId': 23414,
                'description': '接收社会捐赠款物范围',
                'table_name': 'donation_receiving_scope'
            },
        }

        # ================================
        # 页面元素选择器配置 - 可能需要根据实际页面调整
        # ================================

        self.selectors = {
            'content_container': '.content',  # 主要内容容器
            'data_item': '.dataItem',  # 数据项容器
            'title': 'h4.title',  # 标题选择器
            'load_more_button': '.load-more-button',  # 加载更多按钮（如果有）
        }

        # ================================
        # 日志配置
        # ================================

        self.log_config = {
            'log_level': 'INFO',  # 日志级别: DEBUG, INFO, WARNING, ERROR
            'log_file': 'spider.log',  # 日志文件名
            'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'max_log_size': 10 * 1024 * 1024,  # 最大日志文件大小 (10MB)
            'backup_count': 5,  # 保留的日志文件数量
        }

        # ================================
        # 数据处理配置
        # ================================

        self.data_config = {
            'batch_size': 100,  # 批量处理数据大小，根据数据量1-1000条调整
            'enable_data_validation': True,  # 是否启用数据验证
            'skip_duplicates': True,  # 是否跳过重复数据
            'empty_value_replacement': '',  # 空值替换内容
            'max_items_per_page': 1000,  # 每页最大数据条数预估
        }

    def get_random_user_agent(self):
        """随机获取用户代理"""
        return random.choice(self.user_agents)

    def get_random_delay(self, delay_type='default'):
        """
        获取随机延迟时间

        Args:
            delay_type: 延迟类型 ('default', 'between_pages')
        """
        if delay_type == 'between_pages':
            min_delay, max_delay = self.request_delays['between_pages']
        else:
            min_delay = self.request_delays['min_delay']
            max_delay = self.request_delays['max_delay']

        return random.uniform(min_delay, max_delay)

    def get_url(self, point):
        """
        根据point获取完整URL

        Args:
            point: 目标点名称

        Returns:
            完整的URL地址
        """
        return self.base_url_template.format(point=point)

    def get_target_info(self, point):
        """
        获取目标点的详细信息

        Args:
            point: 目标点名称

        Returns:
            目标点的配置信息字典
        """
        return self.crawl_targets.get(point, {})

    def get_all_targets(self):
        """获取所有爬取目标"""
        return list(self.crawl_targets.keys())

    def validate_chromedriver_path(self):
        """验证chromedriver路径是否有效"""
        return os.path.exists(self.chromedriver_path)

    def get_chrome_options_list(self):
        """获取Chrome选项列表"""
        options = []

        if self.browser_options['headless']:
            options.append('--headless')

        if self.browser_options['disable_gpu']:
            options.append('--disable-gpu')

        if self.browser_options['disable_images']:
            options.append('--blink-settings=imagesEnabled=false')

        if self.browser_options['window_size']:
            options.append(f"--window-size={self.browser_options['window_size']}")

        if self.browser_options['disable_extensions']:
            options.append('--disable-extensions')

        if self.browser_options['no_sandbox']:
            options.append('--no-sandbox')

        if self.browser_options['disable_dev_shm_usage']:
            options.append('--disable-dev-shm-usage')

        # 添加用户代理
        options.append(f"--user-agent={self.get_random_user_agent()}")

        return options


# 全局爬虫配置实例
# 你可以直接导入使用: from config.spider_config import spider_config
spider_config = SpiderConfig()

if __name__ == "__main__":
    # 测试爬虫配置
    print("测试爬虫配置...")

    # 测试chromedriver路径
    if spider_config.validate_chromedriver_path():
        print("✅ ChromeDriver路径验证通过")
    else:
        print(f"❌ ChromeDriver路径无效: {spider_config.chromedriver_path}")
        print("请在spider_config.py中修改chromedriver_path为正确路径")

    # 显示所有爬取目标
    print(f"\n📋 共有 {len(spider_config.get_all_targets())} 个爬取目标:")
    for i, point in enumerate(spider_config.get_all_targets(), 1):
        info = spider_config.get_target_info(point)
        print(f"{i:2d}. {point} - {info.get('description', 'N/A')}")

    # 测试随机功能
    print(f"\n🔀 随机用户代理示例: {spider_config.get_random_user_agent()}")
    print(f"🕐 随机延迟示例: {spider_config.get_random_delay():.2f}秒")

    # 显示URL示例
    test_point = 'yljg'
    print(f"\n🌐 URL示例 ({test_point}): {spider_config.get_url(test_point)}")