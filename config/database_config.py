"""
数据库配置模块
负责数据库连接配置、连接池管理和事务处理
"""

import pymysql
from dbutils.pooled_db import PooledDB
import logging
from contextlib import contextmanager


class DatabaseConfig:
    """数据库配置类"""

    def __init__(self):
        # ================================
        # 请根据你的实际数据库情况修改以下配置
        # ================================
        # 配置日志
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.db_config = {
            'host': 'localhost',  # 数据库主机地址，如果是远程数据库请修改为实际IP
            'user': 'root',  # 数据库用户名，请修改为你的用户名
            'password': 'QP285841730',  # 数据库密码，请修改为你的密码
            'database': 'spider',  # 数据库名称，请修改为你要使用的数据库名
            'charset': 'utf8mb4',  # 字符集，建议保持utf8mb4支持中文
            'port': 3306,  # 数据库端口，MySQL默认3306，如有修改请调整
            'autocommit': True,  # 自动提交，建议保持True
            'cursorclass': pymysql.cursors.DictCursor  # 游标类型，返回字典格式
        }

        # 连接池配置
        self.pool_config = {
            'maxconnections': 10,  # 最大连接数，可根据需要调整
            'mincached': 2,  # 最小缓存连接数
            'maxcached': 8,  # 最大缓存连接数
            'maxshared': 5,  # 最大共享连接数
            'blocking': True,  # 连接池满时是否阻塞等待
            'maxusage': 100,  # 单个连接最大使用次数
            'setsession': [],  # 连接前执行的SQL语句列表
            'ping': 4,  # ping MySQL服务器检查连接有效性
        }

        # 初始化连接池
        self.connection_pool = None
        self._init_pool()

    def _init_pool(self):
        """初始化数据库连接池"""
        try:
            # 合并配置
            pool_config = {**self.db_config, **self.pool_config}

            # 创建连接池
            self.connection_pool = PooledDB(
                creator=pymysql,
                **pool_config
            )

            # 测试连接
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    if result:
                        self.logger.info("数据库连接池初始化成功")
                    else:
                        raise Exception("数据库连接测试失败")

        except Exception as e:
            self.logger.error(f"数据库连接池初始化失败: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        获取数据库连接的上下文管理器
        使用方式:
        with db_config.get_connection() as conn:
            # 使用连接进行数据库操作
        """
        conn = None
        try:
            conn = self.connection_pool.connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"数据库操作异常: {e}")
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_cursor(self):
        """
        获取数据库游标的上下文管理器
        使用方式:
        with db_config.get_cursor() as cursor:
            cursor.execute("SELECT * FROM table")
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                self.logger.error(f"数据库操作异常: {e}")
                raise
            finally:
                cursor.close()

    def execute_sql(self, sql, params=None, fetch_one=False, fetch_all=False):
        """
        执行SQL语句的便捷方法

        Args:
            sql: SQL语句
            params: 参数
            fetch_one: 是否返回单条记录
            fetch_all: 是否返回所有记录

        Returns:
            根据参数返回相应结果
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(sql, params)

                if fetch_one:
                    return cursor.fetchone()
                elif fetch_all:
                    return cursor.fetchall()
                else:
                    return cursor.rowcount

        except Exception as e:
            self.logger.error(f"执行SQL失败: {sql}, 参数: {params}, 错误: {e}")
            raise

    def execute_many(self, sql, params_list):
        """
        批量执行SQL语句

        Args:
            sql: SQL语句
            params_list: 参数列表

        Returns:
            影响的行数
        """
        try:
            with self.get_cursor() as cursor:
                return cursor.executemany(sql, params_list)

        except Exception as e:
            self.logger.error(f"批量执行SQL失败: {sql}, 错误: {e}")
            raise

    def test_connection(self):
        """测试数据库连接"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()
                self.logger.info(f"数据库连接正常，版本: {version}")
                return True
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {e}")
            return False

    def create_database_if_not_exists(self, database_name=None):
        """
        创建数据库（如果不存在）

        Args:
            database_name: 数据库名称，如果不指定则使用配置中的数据库名
        """
        if not database_name:
            database_name = self.db_config['database']

        try:
            # 临时连接不指定数据库
            temp_config = self.db_config.copy()
            temp_config.pop('database')

            conn = pymysql.connect(**temp_config)
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{database_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                conn.commit()
                self.logger.info(f"数据库 {database_name} 创建成功或已存在")
            conn.close()

        except Exception as e:
            self.logger.error(f"创建数据库失败: {e}")
            raise


# 全局数据库配置实例
# 你可以直接导入使用: from config.database_config import db_config
db_config = DatabaseConfig()

if __name__ == "__main__":
    # 测试数据库配置
    print("测试数据库配置...")

    # 测试连接
    if db_config.test_connection():
        print("✅ 数据库连接测试通过")
    else:
        print("❌ 数据库连接测试失败")

    # 测试执行SQL
    try:
        result = db_config.execute_sql("SELECT NOW()", fetch_one=True)
        print(f"✅ SQL执行测试通过，当前时间: {result}")
    except Exception as e:
        print(f"❌ SQL执行测试失败: {e}")
