"""
Web爬取模块 - 基于Selenium的通用爬取框架
根据spider_config配置适应不同point类型的页面
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from config.spider_config import spider_config
import logging
import time
import random
from config.database_config import db_config
from database.db_manager import db_manager
from utils.thread_pool_manager import thread_pool_manager
from typing import Dict, List, Any, Optional

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NetworkException(Exception):
    """自定义网络异常基类"""
    pass


class TemporaryNetworkError(NetworkException):
    """临时性网络错误，适合重试"""
    pass


class PermanentNetworkError(NetworkException):
    """永久性网络错误，不适合重试"""
    pass


class DataNotFoundException(Exception):
    """数据未找到异常"""
    pass


class PageLoadException(Exception):
    """页面加载异常"""
    pass


class WebCrawler:
    """通用的Web爬取类"""

    def __init__(self, point: str):
        """
        初始化爬取器

        Args:
            point: 要爬取的point名称
        """
        self.point = point
        self.point_info = spider_config.get_target_info(point)
        self.table_name = self.point_info['table_name']
        self.url = spider_config.get_url(point)
        self.driver = None
        self.wait = None

        # 初始化浏览器驱动
        self._init_driver()

    def _init_driver(self):
        """初始化浏览器驱动"""
        try:
            # 创建浏览器选项
            chrome_options = Options()

            # 应用配置中的浏览器选项
            if spider_config.browser_options['headless']:
                chrome_options.add_argument("--headless")
            if spider_config.browser_options['disable_gpu']:
                chrome_options.add_argument("--disable-gpu")
            if spider_config.browser_options['disable_images']:
                chrome_options.add_argument("--blink-settings=imagesEnabled=false")
            if spider_config.browser_options['window_size']:
                chrome_options.add_argument(f"--window-size={spider_config.browser_options['window_size']}")
            if spider_config.browser_options['disable_extensions']:
                chrome_options.add_argument('--disable-extensions')
            if spider_config.browser_options['no_sandbox']:
                chrome_options.add_argument('--no-sandbox')
            if spider_config.browser_options['disable_dev_shm_usage']:
                chrome_options.add_argument('--disable-dev-shm-usage')

            # 随机用户代理
            chrome_options.add_argument(f"user-agent={spider_config.get_random_user_agent()}")

            # 创建服务
            service = Service(executable_path=spider_config.chromedriver_path)

            # 初始化驱动
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # 设置超时
            self.driver.set_page_load_timeout(spider_config.timeout_config['page_load_timeout'])
            self.driver.implicitly_wait(spider_config.timeout_config['implicit_wait'])

            # 初始化等待对象
            self.wait = WebDriverWait(
                self.driver,
                spider_config.timeout_config['explicit_wait']
            )

            logger.info(f"成功初始化浏览器驱动 for {self.point}")

        except Exception as e:
            logger.error(f"初始化浏览器驱动失败: {e}")
            raise RuntimeError(f"无法初始化浏览器驱动: {e}")

    def crawl(self) -> bool:
        """带智能重试机制的爬取方法"""
        max_retries = spider_config.retry_config['max_retries']
        retries = 1
        success = False

        while retries <= max_retries and not success:
            try:
                logger.info(f"开始爬取 {self.point} - {self.url}")
                # 1. 访问目标URL
                self._navigate_to_url()

                # 2. 等待页面加载
                self._wait_for_page_load()

                # 3. 滚动加载所有数据
                self._scroll_to_load_all_data()

                # 4. 提取数据
                data_items = self._extract_data_items()

                # 5. 保存数据
                if data_items:
                    thread_pool_manager.submit_parser_task(
                        self._save_data,
                        data_items
                    )
                    success = True
                    logger.info(f"成功爬取 {len(data_items)} 条数据并提交保存")
                    # 礼貌性延迟
                    self._random_delay('between_pages')
                else:
                    logger.warning(f"未找到数据项: {self.url}")
                    # 视为失败，触发重试
                    raise DataNotFoundException(f"未找到数据项: {self.url}")

            except NetworkException as e:
                logger.warning(f"网络错误 ({retries}/{max_retries}): {e}")
            except DataNotFoundException as e:
                logger.warning(f"数据错误 ({retries}/{max_retries}): {e}")
            except Exception as e:
                logger.error(f"未知错误 ({retries}/{max_retries}): {e}")

            # 处理重试逻辑
            if not success:
                retries += 1

                if retries > max_retries:
                    logger.error(f"爬取失败，达到最大重试次数: {self.url}")
                    return False

                # 计算退避时间
                backoff = self._calculate_backoff(retries)
                logger.info(f"等待 {backoff} 秒后重试...")
                time.sleep(backoff)

                # 重建连接
                self._reinitialize_driver()

        return success

    def _navigate_to_url(self):
        """带重试的URL导航"""
        max_retries = spider_config.retry_config['op_max_retries']
        retries = 1

        while retries <= max_retries:
            try:
                self.driver.get(self.url)
                logger.debug(f"已访问URL: {self.url}")
                return
            except (WebDriverException, TimeoutException) as e:
                retries += 1
                if "net::ERR_CONNECTION_TIMED_OUT" in str(e):
                    logger.warning(f"连接超时 ({retries}/{max_retries})")
                    if retries > max_retries:
                        raise NetworkException(f"导航失败，达到最大重试次数: {self.url}")
                else:
                    logger.warning(f"访问URL失败 ({retries}/{max_retries}): {e}")
                    if retries > max_retries:
                        raise
                time.sleep(spider_config.retry_config['op_retry_delay'])

    def _wait_for_page_load(self):
        """带重试的页面加载等待"""
        selector = spider_config.selectors.get('content_container', '.content')
        retries = 1
        max_retries = spider_config.retry_config['op_max_retries']

        while retries <= max_retries:
            try:
                # 使用带超时的等待
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                logger.debug("页面主要内容已加载")
                return
            except TimeoutException:
                retries += 1
                if retries > max_retries:
                    raise PageLoadException("页面加载超时")

                # 尝试刷新页面
                logger.warning(f"页面加载超时，尝试刷新... ({retries}/{max_retries})")
                self.driver.refresh()
                time.sleep(spider_config.retry_config['op_retry_delay'])  # 刷新后短暂等待

    def _scroll_to_load_all_data(self):
        """滚动加载所有数据"""
        scroll_config = spider_config.scroll_config
        max_attempts = scroll_config['max_scroll_attempts']
        check_count = scroll_config['check_no_more_data_times']

        last_height = self.driver.execute_script("return document.body.scrollHeight")
        no_change_count = 0
        scroll_count = 0

        logger.debug("开始滚动加载数据...")

        while scroll_count < max_attempts and no_change_count < check_count:
            # 滚动到底部
            scroll_step = scroll_config.get('scroll_step', 800)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_step});")

            # 随机延迟
            self._random_delay()

            # 获取新高度
            new_height = self.driver.execute_script("return document.body.scrollHeight")

            # 检查高度变化
            if new_height == last_height:
                no_change_count += 1
            else:
                no_change_count = 0
                last_height = new_height

            scroll_count += 1

        logger.debug(f"滚动完成, 总滚动次数: {scroll_count}")

    def _random_delay(self, delay_type='default'):
        """随机延迟"""
        delay = spider_config.get_random_delay(delay_type)
        time.sleep(delay)

    def _extract_data_items(self) -> List[Dict[str, Any]]:
        """提取数据项"""
        try:
            # 使用配置中的数据项选择器
            item_selector = spider_config.selectors.get('data_item', '.dataItem')
            items = self.driver.find_elements(By.CSS_SELECTOR, item_selector)

            if not items:
                logger.warning(f"未找到数据项: {self.url}")
                return []

            logger.info(f"找到 {len(items)} 个数据项")

            # 提取数据
            data_items = []
            for idx, item in enumerate(items):
                try:
                    # 获取数据项HTML用于解析
                    item_html = item.get_attribute('outerHTML')

                    # 提交给解析器解析
                    data = {
                        'point': self.point,
                        'table_name': self.table_name,
                        'item_html': item_html,
                        'url': self.url,
                        'item_index': idx
                    }
                    data_items.append(data)
                except Exception as e:
                    logger.error(f"提取数据项 {idx} 失败: {e}")

            return data_items

        except Exception as e:
            logger.error(f"提取数据项失败: {e}")
            return []

    def _calculate_backoff(self, retry_count: int) -> float:
        """计算指数退避时间"""
        base_delay = spider_config.retry_config['base_delay']
        max_delay = spider_config.retry_config['max_delay']
        backoff_factor = spider_config.retry_config['backoff_factor']

        # 指数退避公式: delay = min(base_delay * (backoff_factor ** retry_count), max_delay)
        delay = min(base_delay * (backoff_factor ** retry_count), max_delay)

        # 添加随机抖动避免所有重试同时发生
        jitter = random.uniform(0.5, 1.5)
        return delay * jitter

    def _reinitialize_driver(self):
        """重建浏览器驱动"""
        logger.info("重建浏览器驱动...")
        self.close()
        self._init_driver()

    def _save_data(self, data_items: List[Dict[str, Any]]):
        """保存数据到数据库"""
        try:
            # 这里应该调用data_parser模块进行解析
            # 但根据开发计划，解析将在单独的模块中完成
            # 此处仅为保存流程的占位符实现

            # 在实际应用中，这里会调用解析器将item_html解析为结构化数据
            # 然后使用db_manager保存到数据库

            # 示例: 直接保存原始HTML到数据库 (仅用于演示)
            for data in data_items:
                # 在实际应用中，这里应该是解析后的结构化数据
                structured_data = {
                    'point': data['point'],
                    'url': data['url'],
                    'item_index': data['item_index'],
                    'raw_html': data['item_html'][:500] + '...'  # 只保存部分
                }

                # 保存到数据库
                db_manager.save_data(
                    self.table_name,
                    structured_data
                )

            logger.info(f"成功保存 {len(data_items)} 条数据到数据库")

        except Exception as e:
            logger.error(f"保存数据失败: {e}")

    def close(self):
        """关闭浏览器驱动"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info(f"已关闭浏览器 for {self.point}")
            except Exception as e:
                logger.error(f"关闭浏览器时出错: {e}")


def crawl_point(point: str) -> bool:
    """
    爬取指定point的入口函数

    Args:
        point: 要爬取的point名称

    Returns:
        爬取是否成功
    """
    crawler = None
    try:
        crawler = WebCrawler(point)
        return crawler.crawl()
    except Exception as e:
        logger.error(f"爬取point {point} 失败: {e}")
        return False
    finally:
        if crawler:
            crawler.close()