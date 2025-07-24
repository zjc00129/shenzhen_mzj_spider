"""
线程池管理模块
负责创建和管理两个独立的线程池：爬取线程池和解析线程池
使用Queue进行线程间通信，提供任务提交和结果收集功能
"""

import concurrent.futures
from queue import Queue, Empty
import threading
import logging
import time
from typing import Callable, Any
from config.spider_config import spider_config

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ThreadPoolManager:
    """线程池管理类"""

    def __init__(self, crawler_pool_size=spider_config.thread_config.get('crawler_pool_size'),
                 parser_pool_size=spider_config.thread_config.get('parser_pool_size')):
        """
        初始化线程池管理器

        Args:
            crawler_pool_size: 爬取线程池大小
            parser_pool_size: 解析线程池大小
        """
        # 线程池配置
        self.crawler_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=crawler_pool_size,
            thread_name_prefix='crawler_'
        )
        self.parser_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=parser_pool_size,
            thread_name_prefix='parser_'
        )

        # 任务队列
        self.crawler_task_queue = Queue(maxsize=spider_config.thread_config.get('queue_max_size'))
        self.parser_task_queue = Queue(maxsize=spider_config.thread_config.get('queue_max_size'))

        # 结果队列
        # self.crawler_result_queue = Queue(maxsize=spider_config.thread_config.get('queue_max_size'))
        # self.parser_result_queue = Queue(maxsize=spider_config.thread_config.get('queue_max_size'))

        # 状态监控
        self.running = False
        self.stats = {
            'crawler_tasks_submitted': 0,
            'crawler_tasks_completed': 0,
            'parser_tasks_submitted': 0,
            'parser_tasks_completed': 0,
            'errors': 0
        }

        # 监控线程
        self.monitor_thread = None
        self.task_monitor_thread = None

        # 线程锁
        self.lock = threading.Lock()

    def start(self):
        """启动线程池管理器"""
        if self.running:
            logger.warning("线程池管理器已经在运行中")
            return

        self.running = True

        # 启动任务监控线程
        self.monitor_thread = threading.Thread(target=self._monitor_pools, name="pool_monitor")
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

        # 启动任务分发线程
        self.task_monitor_thread = threading.Thread(target=self._task_dispatcher, name="task_dispatcher")
        self.task_monitor_thread.daemon = True
        self.task_monitor_thread.start()

        logger.info("线程池管理器已启动")

    def stop(self, wait=True):
        """停止线程池管理器

        Args:
            wait: 是否等待所有任务完成
        """
        if not self.running:
            return

        self.running = False

        # 停止线程池
        self.crawler_pool.shutdown(wait=wait)
        self.parser_pool.shutdown(wait=wait)

        # 等待监控线程结束
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

        if self.task_monitor_thread and self.task_monitor_thread.is_alive():
            self.task_monitor_thread.join(timeout=5)

        logger.info("线程池管理器已停止")

    def submit_crawler_task(self, task_func: Callable, *args, **kwargs):
        """
        提交爬取任务

        Args:
            task_func: 要执行的任务函数
            *args: 任务函数的参数
            **kwargs: 任务函数的关键字参数
        """
        if not self.running:
            logger.error("线程池未运行，无法提交任务")
            return False

        self.crawler_task_queue.put((task_func, args, kwargs))
        with self.lock:
            self.stats['crawler_tasks_submitted'] += 1
        return True

    def submit_parser_task(self, task_func: Callable, *args, **kwargs):
        """
        提交解析任务

        Args:
            task_func: 要执行的任务函数
            *args: 任务函数的参数
            **kwargs: 任务函数的关键字参数
        """
        if not self.running:
            logger.error("线程池未运行，无法提交任务")
            return False

        self.parser_task_queue.put((task_func, args, kwargs))
        with self.lock:
            self.stats['parser_tasks_submitted'] += 1
        return True

    # def get_crawler_result(self, timeout: float = None) -> Any:
    #     """
    #     获取爬取任务结果
    #
    #     Args:
    #         timeout: 超时时间（秒）
    #
    #     Returns:
    #         任务结果，超时返回None
    #     """
    #     try:
    #         return self.crawler_result_queue.get(timeout=timeout)
    #     except Empty:
    #         return None

    # def get_parser_result(self, timeout: float = None) -> Any:
    #     """
    #     获取解析任务结果
    #
    #     Args:
    #         timeout: 超时时间（秒）
    #
    #     Returns:
    #         任务结果，超时返回None
    #     """
    #     try:
    #         return self.parser_result_queue.get(timeout=timeout)
    #     except Empty:
    #         return None

    def get_stats(self) -> dict:
        """获取当前统计信息"""
        with self.lock:
            return self.stats.copy()

    def _execute_crawler_task(self, task_func, *args, **kwargs):
        """执行爬取任务"""
        try:
            result = task_func(*args, **kwargs)
            # self.crawler_result_queue.put(result)
            return result
        except Exception as e:
            logger.error(f"爬取任务执行失败: {e}")
            with self.lock:
                self.stats['errors'] += 1
            return False
        finally:
            with self.lock:
                self.stats['crawler_tasks_completed'] += 1

    def _execute_parser_task(self, task_func, *args, **kwargs):
        """执行解析任务"""
        try:
            result = task_func(*args, **kwargs)
            # self.parser_result_queue.put(result)
            return result
        except Exception as e:
            logger.error(f"解析任务执行失败: {e}")
            with self.lock:
                self.stats['errors'] += 1
            return False
        finally:
            with self.lock:
                self.stats['parser_tasks_completed'] += 1

    def _task_dispatcher(self):
        """任务分发器，从队列中获取任务并提交到线程池"""
        logger.info("任务分发器已启动")
        while self.running:
            try:
                # 分发爬取任务
                if not self.crawler_task_queue.empty():
                    task_func, args, kwargs = self.crawler_task_queue.get(timeout=0.5)
                    self.crawler_pool.submit(
                        self._execute_crawler_task, task_func, *args, **kwargs
                    )

                # 分发解析任务
                if not self.parser_task_queue.empty():
                    task_func, args, kwargs = self.parser_task_queue.get(timeout=0.5)
                    self.parser_pool.submit(
                        self._execute_parser_task, task_func, *args, **kwargs
                    )

                # 短暂休眠避免CPU空转
                time.sleep(0.1)

            except Empty:
                # 队列为空时短暂休眠
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"任务分发异常: {e}")
                time.sleep(1)

        logger.info("任务分发器已停止")

    def _monitor_pools(self):
        """线程池状态监控"""
        logger.info("线程池监控器已启动")
        while self.running:
            # 监控爬取线程池
            crawler_active = self.crawler_pool._work_queue.qsize()
            crawler_threads = len(self.crawler_pool._threads)

            # 监控解析线程池
            parser_active = self.parser_pool._work_queue.qsize()
            parser_threads = len(self.parser_pool._threads)

            # 记录状态
            logger.debug(
                f"爬取线程池: 活动线程={crawler_threads}, 排队任务={crawler_active} | "
                f"解析线程池: 活动线程={parser_threads}, 排队任务={parser_active}"
            )

            # 每秒检查一次
            time.sleep(1)

        logger.info("线程池监控器已停止")

    def wait_all_completed(self, timeout: float = None) -> bool:
        """
        等待所有任务完成

        Args:
            timeout: 超时时间（秒）

        Returns:
            是否所有任务在超时前完成
        """
        start_time = time.time()
        while self.running:
            # 检查任务队列是否为空
            crawler_empty = self.crawler_task_queue.empty()
            parser_empty = self.parser_task_queue.empty()
            # 检查线程池状态
            crawler_idle = self.crawler_pool._work_queue.empty() and not self.crawler_pool._threads
            parser_idle = self.parser_pool._work_queue.empty() and not self.parser_pool._threads

            if crawler_empty and parser_empty and crawler_idle and parser_idle:
                return True

            # 检查超时
            if timeout and (time.time() - start_time) > timeout:
                return False

            time.sleep(0.5)

        return True


# 全局线程池管理实例
# 可以直接导入使用: from utils.thread_pool_manager import thread_pool_manager
thread_pool_manager = ThreadPoolManager()

if __name__ == "__main__":
    # 测试线程池管理模块

    # 测试任务函数
    data=1000

    def test_crawler_task(task_id):
        thread_pool_manager.submit_parser_task(test_parser_task, data)
        logger.info(f"爬取任务 {task_id} 开始执行")
        time.sleep(1)  # 模拟爬取耗时
        return f"爬取结果-{task_id}"


    def test_parser_task(data):
        logger.info(f"解析任务 {data} 开始执行")
        time.sleep(0.5)  # 模拟解析耗时
        return f"解析结果-{data}"


    # 启动线程池
    thread_pool_manager.start()

    # 提交测试任务
    for i in range(5):
        thread_pool_manager.submit_crawler_task(test_crawler_task, i)

    # 处理爬取结果并提交解析任务
    # for i in range(5):
    #     result = thread_pool_manager.get_crawler_result(timeout=5)
    #     if result:
    #         logger.info(f"收到爬取结果: {result}")
    #         thread_pool_manager.submit_parser_task(test_parser_task, result)

    # 获取解析结果
    # for i in range(5):
    #     result = thread_pool_manager.get_parser_result(timeout=5)
    #     if result:
    #         logger.info(f"收到解析结果: {result}")

    # 等待所有任务完成
    thread_pool_manager.wait_all_completed(timeout=10)

    # 打印统计信息
    stats = thread_pool_manager.get_stats()
    logger.info("任务统计:")
    for key, value in stats.items():
        logger.info(f"{key}: {value}")

    # 停止线程池
    thread_pool_manager.stop()