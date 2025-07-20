"""
表结构设计模块
根据不同的数据类型设计对应的表结构，并提供动态表创建功能
"""

from config.database_config import db_config
import logging
from typing import Dict, List

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TableSchemaManager:
    """表结构管理类"""

    def __init__(self):
        # 所有表的字段定义
        self.table_schemas = self._init_table_schemas()

    def _init_table_schemas(self) -> Dict[str, Dict]:
        """初始化所有表的字段定义"""
        return {
            'marriage_registration_agencies': {
                'description': '婚姻登记机关名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'business_hours': 'VARCHAR(200) COMMENT "受理办证时间"',
                    'reservation_url': 'VARCHAR(200) COMMENT "预约网址"',
                    'service_phone': 'VARCHAR(50) COMMENT "服务电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'domestic_adoption_agencies': {
                'description': '国内居民收养登记机关名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'business_hours': 'VARCHAR(200) COMMENT "受理办证时间"',
                    'service_phone': 'VARCHAR(50) COMMENT "服务电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'overseas_adoption_agencies': {
                'description': '华侨、港澳台居民收养登记机关名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'business_hours': 'VARCHAR(200) COMMENT "受理办证时间"',
                    'service_phone': 'VARCHAR(50) COMMENT "服务电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'rescue_stations': {
                'description': '救助站名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'minor_protection_pilots': {
                'description': '未成年人社会保护试点名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'charity_supermarkets': {
                'description': '慈善超市名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'community_elderly_care_centers': {
                'description': '社区老年人日间照料中心名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'district': 'VARCHAR(50) COMMENT "所属区"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'nature': 'VARCHAR(50) COMMENT "性质"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                    'INDEX idx_district (district)',
                ]
            },
            'elderly_dining_halls': {
                'description': '深圳市长者饭堂名册',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'district': 'VARCHAR(50) COMMENT "所属区"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'contact_phone': 'VARCHAR(50) COMMENT "联系电话"',
                    'type': 'VARCHAR(50) COMMENT "类型"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                    'INDEX idx_district (district)',
                ]
            },
            'donation_receiving_points': {
                'description': '捐助接收点名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'funeral_service_units': {
                'description': '殡葬服务单位名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'commercial_cemeteries': {
                'description': '经营性公墓名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'district': 'VARCHAR(50) COMMENT "所属区"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                    'INDEX idx_district (district)',
                ]
            },
            'public_welfare_cemeteries': {
                'description': '公益性公墓名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'district': 'VARCHAR(50) COMMENT "所属区"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                    'INDEX idx_district (district)',
                ]
            },
            'elderly_care_institutions': {
                'description': '养老机构名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'district': 'VARCHAR(50) COMMENT "所属区"',
                    'nature': 'VARCHAR(50) COMMENT "性质"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'bed_count': 'INT COMMENT "床位"',
                    'nursing_bed_count': 'INT COMMENT "护理型床位"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                    'INDEX idx_district (district)',
                ]
            },
            'children_welfare_institutions': {
                'description': '儿童福利服务机构名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'address': 'VARCHAR(200) COMMENT "地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'administrative_divisions': {
                'description': '行政区划概况表',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "行政区划信息"',
                    'street_office_count': 'INT COMMENT "街道办数量"',
                    'street_office_names': 'VARCHAR(500) COMMENT "街道办名称"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            },
            'social_worker_positions': {
                'description': '社工岗位名单',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "机构名称"',
                    'district': 'VARCHAR(50) COMMENT "所属区"',
                    'office_address': 'VARCHAR(200) COMMENT "办公地址"',
                    'office_phone': 'VARCHAR(50) COMMENT "办公电话"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                    'INDEX idx_district (district)',
                ]
            },
            'donation_receiving_scope': {
                'description': '接收社会捐赠款物范围',
                'fields': {
                    'id': 'INT AUTO_INCREMENT PRIMARY KEY',
                    'name': 'VARCHAR(100) NOT NULL COMMENT "物品类别"',
                    'requirements': 'VARCHAR(500) COMMENT "要求"',
                    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                },
                'indexes': [
                    'INDEX idx_name (name)',
                ]
            }
        }

    def get_table_schema(self, table_name: str) -> Dict:
        """
        获取指定表的字段定义

        Args:
            table_name: 表名

        Returns:
            表的字段定义字典
        """
        return self.table_schemas.get(table_name, {})

    def create_table(self, table_name: str) -> bool:
        """
        创建数据表

        Args:
            table_name: 表名

        Returns:
            是否创建成功
        """
        schema = self.get_table_schema(table_name)
        if not schema:
            logger.error(f"无法创建表 {table_name}，未找到对应的表结构定义")
            return False

        try:
            # 构建创建表的SQL语句
            fields_sql = ",\n".join([f"{field_name} {field_def}"
                                     for field_name, field_def in schema['fields'].items()])

            if 'indexes' in schema and schema['indexes']:
                fields_sql += ",\n" + ",\n".join(schema['indexes'])

            create_sql = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                {fields_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
            COMMENT='{schema.get('description', '')}';
            """

            with db_config.get_cursor() as cursor:
                cursor.execute(create_sql)
                logger.info(f"成功创建表 {table_name}")
                return True

        except Exception as e:
            logger.error(f"创建表 {table_name} 失败: {e}")
            return False

    def create_all_tables(self) -> bool:
        """
        创建所有定义的表

        Returns:
            是否全部创建成功
        """
        success = True
        for table_name in self.table_schemas.keys():
            if not self.create_table(table_name):
                success = False
        return success

    def get_table_fields(self, table_name: str) -> List[str]:
        """
        获取表的字段列表

        Args:
            table_name: 表名

        Returns:
            字段名列表
        """
        schema = self.get_table_schema(table_name)
        if not schema:
            return []

        # 排除id和timestamp字段
        exclude_fields = {'id', 'created_at', 'updated_at'}
        return [field for field in schema['fields'].keys() if field not in exclude_fields]

    def get_insert_sql(self, table_name: str) -> str:
        """
        获取插入数据的SQL语句

        Args:
            table_name: 表名

        Returns:
            INSERT SQL语句
        """
        fields = self.get_table_fields(table_name)
        if not fields:
            return ""

        fields_str = ", ".join(fields)
        placeholders = ", ".join(["%s"] * len(fields))

        return f"INSERT INTO `{table_name}` ({fields_str}) VALUES ({placeholders})"

    def get_update_sql(self, table_name: str, update_fields: List[str], key_field: str = 'name') -> str:
        """
        获取更新数据的SQL语句

        Args:
            table_name: 表名
            update_fields: 要更新的字段列表
            key_field: 用于WHERE条件的字段，默认为name

        Returns:
            UPDATE SQL语句
        """
        if key_field not in self.get_table_fields(table_name):
            logger.error(f"更新表 {table_name} 失败: 关键字段 {key_field} 不存在")
            return ""

        set_clause = ", ".join([f"{field}=%s" for field in update_fields])
        return f"UPDATE `{table_name}` SET {set_clause} WHERE {key_field}=%s"


# 全局表结构管理实例
# 可以直接导入使用: from database.table_schemas import schema_manager
schema_manager = TableSchemaManager()

if __name__ == "__main__":
    # 测试表结构模块
    print("测试表结构模块...")

    # 确保数据库存在
    db_config.create_database_if_not_exists()

    # 测试创建所有表
    if schema_manager.create_all_tables():
        print("✅ 所有表创建成功")
    else:
        print("❌❌ 表创建过程中出现错误")

    # 测试获取表结构
    test_table = 'elderly_care_institutions'
    print(f"\n测试表 {test_table} 的结构:")
    print(schema_manager.get_table_schema(test_table))

    # 测试获取插入SQL
    print(f"\n测试表 {test_table} 的INSERT SQL:")
    print(schema_manager.get_insert_sql(test_table))

    # 测试获取更新SQL
    print(f"\n测试表 {test_table} 的UPDATE SQL:")
    print(schema_manager.get_update_sql(test_table, ['address', 'office_phone']))