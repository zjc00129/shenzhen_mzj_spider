"""
主程序运行模块
程序入口点，负责配置加载、任务初始化和结果报告
"""

import logging
from main_spider import main_spider
from config import spider_config, database_config
from database.table_schemas import schema_manager
from utils.logger import setup_logger


# 初始化日志系统
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('spider_run.log'),
#         logging.StreamHandler()
#     ]
# )
setup_logger()
logger = logging.getLogger(__name__)


def initialize_system():
    """初始化系统"""

    logger.info("=" * 80)
    logger.info("深圳民政数据采集系统 - 初始化")
    logger.info("=" * 80)

    # 测试数据库连接
    if database_config.db_config.test_connection():
        logger.info("✅ 数据库连接测试通过")
    else:
        logger.error("❌ 数据库连接测试失败，请检查配置")
        return False

    # 确保表结构存在
    if schema_manager.create_all_tables():
        logger.info("✅ 数据库表结构初始化完成")
    else:
        logger.error("❌ 数据库表结构初始化失败")
        return False

    # 测试爬虫配置
    if spider_config.spider_config.validate_chromedriver_path():
        logger.info("✅ ChromeDriver路径验证通过")
    else:
        logger.error("❌ ChromeDriver路径无效，请检查配置")
        return False

    # 显示爬取目标
    targets = spider_config.spider_config.get_all_targets()
    logger.info(f"📋 共有 {len(targets)} 个爬取目标:")
    for i, point in enumerate(targets, 1):
        info = spider_config.spider_config.get_target_info(point)
        logger.info(f"    {i:2d}. {point} - {info.get('description', 'N/A')}")

    logger.info("系统初始化完成")
    return True


def run_spider():
    """主程序入口"""
    # 系统初始化
    if not initialize_system():
        logger.error("系统初始化失败，程序终止")
        return

    try:
        # 启动爬取
        main_spider.start_crawling()

        # 生成报告
        report = main_spider.generate_report()

        # 打印报告
        print("\n" + "=" * 80)
        print("爬取任务完成报告")
        print("=" * 80)
        print(f"开始时间: {report['start_time']}")
        print(f"结束时间: {report['end_time']}")
        print(f"总耗时: {report['duration_human']} ({report['duration_seconds']:.2f} 秒)")
        print(f"目标总数: {report['total_targets']}")
        print(f"成功完成: {report['completed']}")
        print(f"失败目标: {report['failed']}")
        print(f"成功率: {report['success_rate']:.2f}%")
        print("\n数据库统计:")
        print(f"  插入: {report['database_stats']['insert']}")
        print(f"  更新: {report['database_stats']['update']}")
        print(f"  重复: {report['database_stats']['duplicate']}")
        print(f"  错误: {report['database_stats']['error']}")
        print("=" * 80)

        # 记录报告
        logger.info("\n" + "=" * 80)
        logger.info("爬取任务完成报告")
        logger.info("=" * 80)
        logger.info(f"开始时间: {report['start_time']}")
        logger.info(f"结束时间: {report['end_time']}")
        logger.info(f"总耗时: {report['duration_human']} ({report['duration_seconds']:.2f} 秒)")
        logger.info(f"目标总数: {report['total_targets']}")
        logger.info(f"成功完成: {report['completed']}")
        logger.info(f"失败目标: {report['failed']}")
        logger.info(f"成功率: {report['success_rate']:.2f}%")
        logger.info("\n数据库统计:")
        logger.info(f"  插入: {report['database_stats']['insert']}")
        logger.info(f"  更新: {report['database_stats']['update']}")
        logger.info(f"  重复: {report['database_stats']['duplicate']}")
        logger.info(f"  错误: {report['database_stats']['error']}")
        logger.info("=" * 80)

    except Exception as e:
        logger.exception(f"程序运行异常: {e}")
    finally:
        logger.info("程序结束")


if __name__ == "__main__":
    run_spider()