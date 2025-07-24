"""
数据解析与保存模块 - 完整版
基于提供的解析规则实现所有point类型的解析
"""

import logging
from typing import Dict, List, Any
from database.db_manager import db_manager
from config.spider_config import spider_config
from bs4 import BeautifulSoup
import re
from database.table_schemas import schema_manager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    """高效数据解析与处理器"""

    # 所有point的解析规则
    PARSING_RULES = {
        'hydjjg': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("联系地址")) > p', 'method': 'text'},
            'business_hours': {'selector': 'li:has(label:contains("受理办证时间")) > p', 'method': 'text'},
            'reservation_url': {'selector': 'li:has(label:contains("预约网址")) > p', 'method': 'text'},
            'service_phone': {'selector': 'li:has(label:contains("服务电话")) > p', 'method': 'text'},
        },
        'gnjmsydjjg': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'business_hours': {'selector': 'li:has(label:contains("受理办证时间")) > p', 'method': 'text'},
            'service_phone': {'selector': 'li:has(label:contains("电话")) > p', 'method': 'text'},
        },
        'hqgatjmsydjjg': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'business_hours': {'selector': 'li:has(label:contains("受理办证时间")) > p', 'method': 'text'},
            'service_phone': {'selector': 'li:has(label:contains("电话")) > p', 'method': 'text'},
        },
        'jzz': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
        },
        'wcnrshbhsd': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("办公电话")) > p', 'method': 'text'},
        },
        'cscs': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("办公电话")) > p', 'method': 'text'},
        },
        'sqlnrrjzlzx': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'district': {'selector': 'li:has(label:contains("所属区")) > p', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'nature': {'selector': 'li:has(label:contains("性质")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("电话")) > p', 'method': 'text'},
        },
        'zzft': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'district': {'selector': 'li:has(label:contains("所属区")) > p', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("详细地址")) > p', 'method': 'text'},
            'contact_phone': {'selector': 'li:has(label:contains("联系电话")) > p', 'method': 'text'},
            'type': {'selector': 'li:has(label:contains("类型")) > p', 'method': 'text'},
        },
        'jzjsd': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("办公电话")) > p', 'method': 'text'},
        },
        'bzfwdw': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("办公电话")) > p', 'method': 'text'},
        },
        'jyxgm': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'district': {'selector': 'li:has(label:contains("所属区")) > p', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("办公电话")) > p', 'method': 'text'},
        },
        'gyxgm': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'district': {'selector': 'li:has(label:contains("所属区")) > p', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("办公电话")) > p', 'method': 'text'},
        },
        'yljg': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'district': {'selector': 'li:has(label:contains("辖区")) > p', 'method': 'text'},
            'nature': {'selector': 'li:has(label:contains("性质")) > p', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'bed_count': {'selector': 'li:has(label:contains("床位")) > p', 'method': 'text', 'processor': 'extract_number'},
            'nursing_bed_count': {'selector': 'li:has(label:contains("护理型床位")) > p', 'method': 'text', 'processor': 'extract_number'},
            'office_phone': {'selector': 'li:has(label:contains("联系电话")) > p', 'method': 'text'},
        },
        'etfljg': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'address': {'selector': 'li:has(label:contains("地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("电话")) > p', 'method': 'text'},
        },
        'xzqhgkb': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'street_office_count': {'selector': 'li:has(label:contains("街道办数量")) > p', 'method': 'text', 'processor': 'extract_number'},
            'street_office_names': {'selector': 'li:has(label:contains("街道办名称")) > p', 'method': 'text'},
        },
        'sggw': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'district': {'selector': 'li:has(label:contains("所属区")) > p', 'method': 'text'},
            'office_address': {'selector': 'li:has(label:contains("办公地址")) > p', 'method': 'text'},
            'office_phone': {'selector': 'li:has(label:contains("办公电话")) > p', 'method': 'text'},
        },
        'jsshjzkwfw': {
            'name': {'selector': 'h4.title', 'method': 'text'},
            'requirements': {'selector': 'li:has(label:contains("要求")) > p', 'method': 'text'},
        },
    }

    # 字段处理器
    FIELD_PROCESSORS = {
        'extract_number': lambda value: int(re.findall(r'\d+', value)[0]) if value and re.findall(r'\d+', value) else 0,
        'extract_phone': lambda value: re.sub(r'[^\d-]', '', value) if value else '',
        'extract_url': lambda value: value if value.startswith('http') else f"https://mzj.sz.gov.cn{value}" if value.startswith('/') else value,
        'extract_date': lambda value: value,  # 待实现具体日期处理
        # 也可直接编写函数对应，例如:'extract_number': self.process_number  ,self.process_number为函数名
    }

    def __init__(self):
        """初始化数据处理器"""
        # 创建解析规则缓存
        self.rules_cache = {}

    def process_items(self, data_items: List[Dict[str, Any]]):
        """
        处理一批数据项（来自同一个point）

        Args:
            data_items: 原始数据项列表
        """

        # 所有数据项来自同一个point
        point = data_items[0]['point']
        table_name = data_items[0]['table_name']

        # 获取该point的解析规则
        point_rules = self._get_point_rules(point, table_name)

        # 处理每个数据项
        for item in data_items:
            try:
                # 解析HTML数据
                structured_data = self.parse_item(item['item_html'], point_rules)

                # 添加元信息
                # structured_data['source_url'] = item['url']

                # 直接保存到数据库
                success = db_manager.save_data(table_name, structured_data)

                if success:
                    logger.debug(f"成功保存 {point} 数据项: {item['item_index']}")
                else:
                    logger.error(f"保存 {point} 数据项失败: {item['item_index']}")

            except Exception as e:
                logger.error(f"处理point {point} 数据项失败: {e}")

    def parse_item(self, html: str, point_rules: Dict) -> Dict[str, Any]:
        """
        解析单个数据项HTML

        Args:
            html: HTML字符串
            point_rules: point解析规则

        Returns:
            结构化数据字典
        """
        soup = BeautifulSoup(html, 'html.parser')
        data = {}

        # 应用解析规则
        for field, rule in point_rules.items():
            try:
                # 查找元素
                element = soup.select_one(rule['selector'])
                if not element:
                    data[field] = None
                    continue

                # 提取原始值
                method = rule.get('method', 'text')
                if method == 'text':
                    value = element.get_text(strip=True)
                elif method == 'html':
                    value = str(element)
                elif method == 'href':
                    value = element.get('href', '')
                else:
                    value = element.get(method, '')

                # 应用后处理，清洗数据
                processor_name = rule.get('processor')
                if processor_name and processor_name in self.FIELD_PROCESSORS:
                    value = self.FIELD_PROCESSORS[processor_name](value)

                data[field] = value
            except Exception as e:
                logger.warning(f"提取字段 {field} 失败: {e}")
                data[field] = None

        return data

    def _get_point_rules(self, point: str, table_name: str) -> Dict:
        """获取point的解析规则"""
        # 先从缓存获取
        if point in self.rules_cache:
            return self.rules_cache[point]

        # 获取表的字段
        table_fields = schema_manager.get_table_fields(table_name)
        if not table_fields:
            logger.error(f"找不到表结构: {table_name}")
            return {}

        # 获取该point的规则模板
        point_rules = self.PARSING_RULES.get(point, {})

        # 过滤规则 - 只保留表结构中存在的字段
        valid_rules = {}
        for field, rule in point_rules.items():
            if field in table_fields:
                valid_rules[field] = rule

        # 缓存规则
        self.rules_cache[point] = valid_rules

        return valid_rules

# 全局数据处理器实例
data_processor = DataProcessor()