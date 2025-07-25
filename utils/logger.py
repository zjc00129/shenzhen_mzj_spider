import logging
import os
from config.spider_config import spider_config
from logging.handlers import RotatingFileHandler

def setup_logger():
    """配置全局日志系统"""

    # 从配置获取日志设置
    log_config = spider_config.log_config

    # 创建日志目录
    log_dir = log_config.get('log_dir', 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 主日志文件路径
    main_log = os.path.join(log_dir, log_config.get('main_log', 'spider_system.log'))

    # 错误日志文件路径
    error_log = os.path.join(log_dir, log_config.get('error_log', 'spider_errors.log'))

    # 解析日志级别
    log_level = getattr(logging, log_config.get('log_level', 'INFO').upper(), logging.INFO)

    # 创建格式化器
    formatter = logging.Formatter(
        log_config.get('log_format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除所有现有处理器（关键修复）
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 创建主文件处理器（使用轮转）
    max_log_size = log_config.get('max_log_size', 10 * 1024 * 1024)
    backup_count = log_config.get('backup_count', 5)

    file_handler = RotatingFileHandler(
        main_log,
        maxBytes=max_log_size,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # 创建错误文件处理器（仅错误日志）
    error_handler = logging.FileHandler(error_log, encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    return root_logger