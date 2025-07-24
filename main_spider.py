"""
主爬取方法模块
协调各个模块的工作，实现任务分发、进度监控和异常处理
"""

import time
import logging
from typing import List, Dict, Any
from config.spider_config import spider_config
from utils.thread_pool_manager import thread_pool_manager
from crawler.web_crawler import crawl_point
from database.db_manager import db_manager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MainSpider:
    """主爬取控制器"""

    def __init__(self):
        # 爬取状态
        self.status = {
            'start_time': None,
            'end_time': None,
            'completed': 0,
            'failed': 0,
            'total': 0
        }

        # 创建任务队列
        self.task_queue = []

        # 初始化线程池
        self._init_thread_pool()

        # 初始化任务
        self._init_tasks()

    def _init_thread_pool(self):
        """初始化线程池"""
        # 启动线程池管理器
        thread_pool_manager.start()
        logger.info("线程池管理器已启动")

    def _init_tasks(self):
        """初始化爬取任务"""
        # 获取所有爬取目标
        all_targets = spider_config.get_all_targets()
        self.task_queue = all_targets.copy()
        self.status['total'] = len(all_targets)
        logger.info(f"任务队列初始化完成，共 {self.status['total']} 个任务")

    def start_crawling(self):
        """开始爬取"""
        self.status['start_time'] = time.time()
        logger.info("=" * 80)
        logger.info(f"开始爬取任务，共 {self.status['total']} 个目标")
        logger.info("=" * 80)

        try:
            # 提交所有任务
            for point in self.task_queue:
                thread_pool_manager.submit_crawler_task(crawl_point, point)

            # 监控任务进度
            self._monitor_progress()

        except Exception as e:
            logger.error(f"爬取过程中发生错误: {e}")
        finally:
            # 确保资源释放
            self.stop_crawling()

    def _monitor_progress(self):
        """监控任务进度"""
        logger.info("开始监控任务进度...")

        while True:
            # 获取当前统计
            pool_stats = thread_pool_manager.get_stats()
            completed = pool_stats['parser_tasks_completed']
            failed = pool_stats['errors']

            # 更新状态
            self.status['completed'] = completed
            self.status['failed'] = failed

            # 计算进度
            progress = completed + failed
            remaining = self.status['total'] - progress

            # 打印进度
            logger.info(
                f"进度: {progress}/{self.status['total']} "
                f"({(progress / self.status['total']) * 100:.1f}%) | "
                f"成功: {completed} | 失败: {failed} | 剩余: {remaining}"
            )

            # 检查是否完成
            if progress >= self.status['total']:
                logger.info("所有任务已完成")
                break

            # 等待一段时间
            time.sleep(10)

    def stop_crawling(self, timeout=spider_config.thread_config.get('pool_stop_max_time', 300)):
        """停止爬取并清理资源"""
        logger.info("正在停止爬取任务...")

        try:
            # 先等待所有任务完成（最多5分钟）
            logger.info("等待所有任务完成...")
            if thread_pool_manager.wait_all_completed(timeout=timeout):
                logger.info("所有任务已完成")
            else:
                logger.warning(f"等待超时，仍有任务未完成，强制停止")

        except Exception as e:
            logger.error(f"等待任务完成时出错: {e}")

        finally:
            # 停止线程池
            thread_pool_manager.stop(wait=True)

            # 更新结束时间
            self.status['end_time'] = time.time()

            # 记录数据库统计
            db_stats = db_manager.get_stats()
            self.status['db_insert'] = db_stats['insert']
            self.status['db_update'] = db_stats['update']
            self.status['db_duplicate'] = db_stats['duplicate']
            self.status['db_error'] = db_stats['error']

            # 计算耗时
            duration = self.status['end_time'] - self.status['start_time']
            self.status['duration'] = duration

            logger.info("爬取任务已停止")

    def generate_report(self) -> Dict[str, Any]:
        """生成爬取报告"""
        if not self.status['end_time']:
            self.stop_crawling()

        report = {
            'total_targets': self.status['total'],
            'completed': self.status['completed'],
            'failed': self.status['failed'],
            'start_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.status['start_time'])),
            'end_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.status['end_time'])),
            'duration_seconds': self.status['duration'],
            'duration_human': time.strftime('%H:%M:%S', time.gmtime(self.status['duration'])),
            'database_stats': {
                'insert': self.status['db_insert'],
                'update': self.status['db_update'],
                'duplicate': self.status['db_duplicate'],
                'error': self.status['db_error']
            },
            'success_rate': self.status['completed'] / self.status['total'] * 100 if self.status['total'] > 0 else 0
        }

        return report


# 全局主爬取实例
main_spider = MainSpider()