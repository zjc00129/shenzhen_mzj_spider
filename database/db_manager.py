"""
数据库管理模块
负责数据库操作的高级封装，与表结构模块集成，提供线程安全的数据访问
"""

from config.database_config import db_config
from database.table_schemas import schema_manager
import logging
from typing import List, Dict, Any, Optional
from queue import Queue
import threading

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """数据库管理主类"""

    def __init__(self):
        # 操作统计
        self.stats = {
            'insert': 0,
            'update': 0,
            'error': 0,
            'duplicate': 0
        }

        # 线程锁
        self.lock = threading.Lock()

        # 批量操作队列
        self.batch_queue = Queue(maxsize=1000)

        # 初始化表结构
        self._init_tables()

    def _init_tables(self):
        """初始化所有数据表"""
        with self.lock:
            if not schema_manager.create_all_tables():
                logger.error("表初始化失败")
                raise RuntimeError("数据库表初始化失败")
            logger.info("所有数据表已就绪")

    def save_data(self, table_name: str, data: Dict[str, Any],
                  key_field: str = 'name') -> bool:
        """
        保存单条数据到指定表
        自动处理插入或更新

        Args:
            table_name: 表名
            data: 数据字典
            key_field: 用于判断记录是否存在的字段

        Returns:
            是否保存成功
        """
        try:
            # 获取表字段
            table_fields = schema_manager.get_table_fields(table_name)
            if not table_fields:
                logger.error(f"表 {table_name} 不存在或没有定义字段")
                return False

            # 过滤无效字段
            filtered_data = {k: v for k, v in data.items() if k in table_fields}

            # 检查记录是否存在
            existing = self._check_existing(table_name, key_field, data.get(key_field))

            if existing:
                # 执行更新
                return self._update_data(table_name, filtered_data, key_field)
            else:
                # 执行插入
                return self._insert_data(table_name, filtered_data)

        except Exception as e:
            with self.lock:
                self.stats['error'] += 1
            logger.error(f"保存数据到表 {table_name} 失败: {e}")
            return False

    def _insert_data(self, table_name: str, data: Dict[str, Any]) -> bool:
        """
        插入单条数据

        Args:
            table_name: 表名
            data: 数据字典

        Returns:
            是否插入成功
        """
        try:
            sql = schema_manager.get_insert_sql(table_name)
            if not sql:
                return False

            fields = schema_manager.get_table_fields(table_name)
            values = [data.get(field) for field in fields]

            with db_config.get_cursor() as cursor:
                cursor.execute(sql, values)

            with self.lock:
                self.stats['insert'] += 1
            logger.debug(f"成功插入数据到表 {table_name}")
            return True

        except Exception as e:
            with self.lock:
                self.stats['error'] += 1
            logger.error(f"插入数据到表 {table_name} 失败: {e}")
            return False

    def _update_data(self, table_name: str, data: Dict[str, Any],
                     key_field: str) -> bool:
        """
        更新单条数据

        Args:
            table_name: 表名
            data: 数据字典
            key_field: 关键字段名

        Returns:
            是否更新成功
        """
        try:
            # 获取可更新字段
            update_fields = [f for f in data.keys() if f != key_field]
            if not update_fields:
                with self.lock:
                    self.stats['duplicate'] += 1
                logger.debug(f"表 {table_name} 数据无变化，跳过更新")
                return True

            sql = schema_manager.get_update_sql(table_name, update_fields, key_field)
            if not sql:
                return False

            values = [data.get(field) for field in update_fields]
            values.append(data.get(key_field))  # WHERE条件值

            with db_config.get_cursor() as cursor:
                cursor.execute(sql, values)

            with self.lock:
                self.stats['update'] += 1
            logger.debug(f"成功更新表 {table_name} 的数据")
            return True

        except Exception as e:
            with self.lock:
                self.stats['error'] += 1
            logger.error(f"更新表 {table_name} 数据失败: {e}")
            return False

    def _check_existing(self, table_name: str,
                        key_field: str, key_value: Any) -> bool:
        """
        检查记录是否存在

        Args:
            table_name: 表名
            key_field: 关键字段名
            key_value: 关键字段值

        Returns:
            记录是否存在
        """
        try:
            sql = f"SELECT 1 FROM `{table_name}` WHERE `{key_field}` = %s LIMIT 1"

            with db_config.get_cursor() as cursor:
                cursor.execute(sql, (key_value,))
                return bool(cursor.fetchone())

        except Exception as e:
            logger.error(f"检查记录存在性失败: {e}")
            return False

    def batch_save(self, table_name: str, data_list: List[Dict[str, Any]],
                   key_field: str = 'name') -> Dict[str, int]:
        """
        批量保存数据

        Args:
            table_name: 表名
            data_list: 数据字典列表
            key_field: 用于判断记录是否存在的字段

        Returns:
            操作统计字典
        """
        stats = {
            'total': len(data_list),
            'insert': 0,
            'update': 0,
            'error': 0,
            'duplicate': 0
        }

        for data in data_list:
            try:
                # 获取表字段
                table_fields = schema_manager.get_table_fields(table_name)
                if not table_fields:
                    stats['error'] += 1
                    continue

                # 过滤无效字段
                filtered_data = {k: v for k, v in data.items() if k in table_fields}

                # 检查记录是否存在
                existing = self._check_existing(table_name, key_field, data.get(key_field))

                if existing:
                    # 执行更新
                    update_fields = [f for f in filtered_data.keys() if f != key_field]
                    if update_fields:
                        sql = schema_manager.get_update_sql(table_name, update_fields, key_field)
                        values = [filtered_data.get(field) for field in update_fields]
                        values.append(filtered_data.get(key_field))

                        with db_config.get_cursor() as cursor:
                            cursor.execute(sql, values)
                        stats['update'] += 1
                    else:
                        stats['duplicate'] += 1
                else:
                    # 执行插入
                    sql = schema_manager.get_insert_sql(table_name)
                    values = [filtered_data.get(field) for field in table_fields]

                    with db_config.get_cursor() as cursor:
                        cursor.execute(sql, values)
                    stats['insert'] += 1

            except Exception as e:
                stats['error'] += 1
                logger.error(f"批量保存数据到表 {table_name} 失败: {e}")

        # 合并全局统计
        with self.lock:
            for k in ['insert', 'update', 'error', 'duplicate']:
                self.stats[k] += stats[k]

        return stats

    def get_stats(self) -> Dict[str, int]:
        """
        获取操作统计

        Returns:
            统计信息字典
        """
        with self.lock:
            return self.stats.copy()

    def clear_stats(self):
        """重置统计信息"""
        with self.lock:
            self.stats = {
                'insert': 0,
                'update': 0,
                'error': 0,
                'duplicate': 0
            }

    def table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在

        Args:
            table_name: 表名

        Returns:
            表是否存在
        """
        try:
            sql = "SHOW TABLES LIKE %s"
            with db_config.get_cursor() as cursor:
                cursor.execute(sql, (table_name,))
                return bool(cursor.fetchone())
        except Exception as e:
            logger.error(f"检查表存在性失败: {e}")
            return False

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        获取表结构信息

        Args:
            table_name: 表名

        Returns:
            表结构信息字典或None
        """
        if not self.table_exists(table_name):
            return None

        try:
            # 获取字段信息
            sql = f"DESCRIBE `{table_name}`"
            with db_config.get_cursor() as cursor:
                cursor.execute(sql)
                fields = cursor.fetchall()

            # 获取索引信息
            sql = f"SHOW INDEX FROM `{table_name}`"
            with db_config.get_cursor() as cursor:
                cursor.execute(sql)
                indexes = cursor.fetchall()

            return {
                'fields': fields,
                'indexes': indexes
            }
        except Exception as e:
            logger.error(f"获取表 {table_name} 信息失败: {e}")
            return None


# 全局数据库管理实例
# 可以直接导入使用: from database.db_manager import db_manager
db_manager = DatabaseManager()

if __name__ == "__main__":
    # 测试数据库管理模块
    print("测试数据库管理模块...")

    # 测试表存在性检查
    test_table = 'elderly_care_institutions'
    if db_manager.table_exists(test_table):
        print(f"✅ 表 {test_table} 存在")
    else:
        print(f"❌ 表 {test_table} 不存在")

    # 测试获取表信息
    table_info = db_manager.get_table_info(test_table)
    if table_info:
        print(f"✅ 成功获取表 {test_table} 的结构信息")
    else:
        print(f"❌ 获取表 {test_table} 信息失败")

    # 测试数据保存
    test_data = {
        'name': '测试养老院',
        'district': '福田区',
        'address': '测试地址',
        'office_phone': '12345678901'
    }

    if db_manager.save_data(test_table, test_data):
        print("✅ 数据保存测试通过")
    else:
        print("❌ 数据保存测试失败")

    # 显示统计信息
    print("\n操作统计:")
    print(db_manager.get_stats())