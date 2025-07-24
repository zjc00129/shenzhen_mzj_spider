"""
数据库管理模块 - 优化版
修复了重复数据检测逻辑，并优化了字段过滤
"""

from config.database_config import db_config
from database.table_schemas import schema_manager
import logging
from typing import List, Dict, Any, Optional
import threading

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """优化后的数据库管理类"""

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


        # 初始化表结构
        self._init_tables()

    def _init_tables(self):
        """初始化所有数据表"""
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
            # 获取表字段并过滤数据
            table_fields = schema_manager.get_table_fields(table_name)
            if not table_fields:
                self._update_stat('error')
                logger.error(f"表 {table_name} 不存在或没有定义字段")
                return False

            # 过滤无效字段
            filtered_data = {k: v for k, v in data.items() if k in table_fields}

            # 检查记录是否存在
            key_value = data.get(key_field)
            if not key_value:
                self._update_stat('error')
                logger.error(f"缺少关键字段 {key_field}，无法保存数据")
                return False

            existing_record = self._get_existing_record(table_name, key_field, key_value)

            if existing_record:
                return self._update_if_needed(table_name, filtered_data, key_field, existing_record)
            return self._insert_data(table_name, filtered_data)

        except Exception as e:
            self._update_stat('error')
            logger.error(f"保存数据到表 {table_name} 失败: {e}")
            return False

    def _get_existing_record(self, table_name: str, key_field: str, key_value: Any) -> Optional[Dict]:
        """
        获取已存在的记录（如果存在）

        Args:
            table_name: 表名
            key_field: 关键字段名
            key_value: 关键字段值

        Returns:
            记录字典或None
        """
        sql = f"SELECT * FROM `{table_name}` WHERE `{key_field}` = %s LIMIT 1"
        return db_config.execute_sql(sql, (key_value,), fetch_one=True)

    def _insert_data(self, table_name: str, data: Dict[str, Any]) -> bool:
        """优化后的插入数据方法"""
        sql = schema_manager.get_insert_sql(table_name)
        if not sql:
            self._update_stat('error')
            return False

        fields = schema_manager.get_table_fields(table_name)
        values = [data.get(field) for field in fields]

        if db_config.execute_sql(sql, values) > 0:
            self._update_stat('insert')
            logger.debug(f"成功插入数据到表 {table_name}")
            return True

        self._update_stat('error')
        return False

    def _update_if_needed(self, table_name: str, data: Dict[str, Any],
                         key_field: str, existing_record: Dict) -> bool:
        """优化后的更新数据方法，包含实际值比较"""
        # 确定哪些字段需要更新
        update_fields = []
        for field in data:
            if field == key_field:
                continue

            # 比较新值和旧值
            new_value = data.get(field)
            old_value = existing_record.get(field)

            # 检查值是否实际变化
            if str(new_value) != str(old_value) if old_value is not None else new_value is not None:
                update_fields.append(field)

        if not update_fields:
            self._update_stat('duplicate')
            logger.debug(f"表 {table_name} 数据无变化，跳过更新")
            return True

        sql = schema_manager.get_update_sql(table_name, update_fields, key_field)
        if not sql:
            self._update_stat('error')
            return False

        values = [data.get(field) for field in update_fields]
        values.append(data.get(key_field))

        if db_config.execute_sql(sql, values) > 0:
            self._update_stat('update')
            logger.debug(f"成功更新表 {table_name} 的 {len(update_fields)} 个字段")
            return True

        self._update_stat('error')
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
            是否执行成功
        """

        has_error = False

        for data in data_list:
            try:
                if not self.save_data(table_name, data, key_field):
                    logger.warning(f"批量保存部分数据失败: {data.get(key_field)}")
            except Exception as e:
                has_error = True
                logger.error(f"批量保存数据到表 {table_name} 失败: {e}")

        return not has_error

    def _update_stat(self, stat_key: str):
        """线程安全的统计更新"""
        with self.lock:
            self.stats[stat_key] += 1

    # 保留原有的统计和表管理方法
    def get_stats(self) -> Dict[str, int]:
        with self.lock:
            return self.stats.copy()

    def clear_stats(self):
        with self.lock:
            self.stats = {
                'insert': 0,
                'update': 0,
                'error': 0,
                'duplicate': 0
            }

    def table_exists(self, table_name: str) -> bool:
        sql = "SHOW TABLES LIKE %s"
        return bool(db_config.execute_sql(sql, (table_name,), fetch_one=True))

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        if not self.table_exists(table_name):
            return None

        try:
            return {
                'fields': db_config.execute_sql(f"DESCRIBE `{table_name}`", fetch_all=True),
                'indexes': db_config.execute_sql(f"SHOW INDEX FROM `{table_name}`", fetch_all=True)
            }
        except Exception as e:
            logger.error(f"获取表 {table_name} 信息失败: {e}")
            return None


# 全局数据库管理实例
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