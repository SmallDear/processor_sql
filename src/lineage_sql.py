import re
import json
from collections import defaultdict
from sqllineage.utils.constant import LineageLevel
from sqllineage.runner import LineageRunner
from sqllineage.utils.helpers import split


class OptimizedSQLLineageProcessor:
    def __init__(self):
        self.temp_tables = set()  # 存储临时表
        self.lineage_records = []  # 存储血缘关系记录

    def extract_temp_tables_from_script(self, sql_script):
        """
        从SQL脚本中提取临时表
        临时表定义：在脚本中既有CREATE TABLE又有DROP TABLE的表
        """
        self.temp_tables.clear()

        # 提取所有CREATE TABLE的表
        create_pattern = r'CREATE\s+(?:TEMPORARY\s+|TEMP\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s\(\;]+)'
        create_matches = re.findall(create_pattern, sql_script, re.IGNORECASE | re.MULTILINE)

        # 提取所有DROP TABLE的表
        drop_pattern = r'DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?([^\s\;\,]+)'
        drop_matches = re.findall(drop_pattern, sql_script, re.IGNORECASE | re.MULTILINE)

        # 清理表名（去掉引号、方括号等）
        def clean_table_name(table_name):
            cleaned = table_name.strip('`"[]').lower()
            # 如果有数据库前缀，只保留表名部分
            if '.' in cleaned:
                cleaned = cleaned.split('.')[-1]
            return cleaned

        created_tables = {clean_table_name(table) for table in create_matches}
        dropped_tables = {clean_table_name(table) for table in drop_matches}

        # 临时表 = 既被创建又被删除的表
        self.temp_tables = created_tables.intersection(dropped_tables)

        print(f"检测到的临时表: {self.temp_tables}")
        return self.temp_tables

    def split_sql_statements(self, sql_script):
        """
        使用sqllineage的split方法来正确拆分SQL语句
        """
        try:
            statements = split(sql_script)
            return [stmt.strip() for stmt in statements if stmt.strip()]
        except Exception as e:
            print(f"使用sqllineage拆分SQL失败，回退到简单拆分: {e}")
            statements = []
            parts = sql_script.split(';')
            for part in parts:
                part = part.strip()
                if part and not part.isspace():
                    statements.append(part)
            return statements

    def is_temp_table(self, table_identifier):
        """
        检查表是否为临时表
        """
        if not table_identifier or not self.temp_tables:
            return False

        # 提取表名（处理各种格式）
        table_name = str(table_identifier).lower()
        if '.' in table_name:
            table_name = table_name.split('.')[-1]

        return table_name in self.temp_tables

    def is_subquery_from_cytoscape(self, column_data):
        """
        基于cytoscape数据判断是否为子查询
        """
        try:
            if "parent_candidates" not in column_data:
                return False

            parent_candidates = column_data["parent_candidates"]
            if not isinstance(parent_candidates, list):
                return False

            # 检查所有parent candidates，如果有任何一个是SubQuery就认为是子查询
            for candidate in parent_candidates:
                if isinstance(candidate, dict) and candidate.get("type") == "SubQuery":
                    return True

            return False
        except Exception as e:
            print(f"判断子查询时出错: {e}")
            return False

    def extract_database_table_column(self, column_id):
        """
        从字段ID中提取数据库、表、字段信息
        允许数据库名为空
        """
        if not column_id:
            return None

        parts = str(column_id).split('.')

        if len(parts) >= 3:
            # database.table.column 格式
            return {
                'database': parts[0] if parts[0] != '<unknown>' else '',
                'schema': '',
                'table': parts[1],
                'column': parts[2]
            }
        elif len(parts) == 2:
            # table.column 格式（无数据库前缀）
            return {
                'database': '',
                'schema': '',
                'table': parts[0],
                'column': parts[1]
            }
        elif len(parts) == 1:
            # 只有字段名
            return {
                'database': '',
                'schema': '',
                'table': '',
                'column': parts[0]
            }
        else:
            return None

    def find_real_source_table(self, column_data, nodes_dict):
        """
        从parent_candidates中找到真实的源表（非SubQuery）
        """
        if "parent_candidates" not in column_data:
            return None

        parent_candidates = column_data["parent_candidates"]
        if not isinstance(parent_candidates, list):
            return None

        # 寻找类型为Table的候选表
        for candidate in parent_candidates:
            if isinstance(candidate, dict) and candidate.get("type") == "Table":
                table_name = candidate.get("name", "")
                if table_name and not self.is_temp_table(table_name):
                    return table_name

        return None

    def process_cytoscape_lineage(self, cytoscape_data):
        """
        处理cytoscape格式的血缘数据
        """
        self.lineage_records.clear()

        if not cytoscape_data:
            return

        # 构建节点字典
        nodes_dict = {}
        edges = []

        for item in cytoscape_data:
            data = item.get("data", {})
            item_id = data.get("id", "")

            if "source" in data and "target" in data:
                # 这是边
                edges.append(data)
            else:
                # 这是节点
                nodes_dict[item_id] = data

        # 处理每条边（血缘关系）
        for edge in edges:
            try:
                source_id = edge.get("source", "")
                target_id = edge.get("target", "")

                if not source_id or not target_id:
                    continue

                # 获取源和目标的节点信息
                source_data = nodes_dict.get(source_id, {})
                target_data = nodes_dict.get(target_id, {})

                # 跳过子查询
                if (self.is_subquery_from_cytoscape(source_data) or
                        self.is_subquery_from_cytoscape(target_data)):
                    continue

                # 解析源字段信息
                source_info = self.extract_database_table_column(source_id)
                if not source_info:
                    continue

                # 如果源字段没有明确的表信息，尝试从parent_candidates获取
                if not source_info['table']:
                    real_source_table = self.find_real_source_table(source_data, nodes_dict)
                    if real_source_table:
                        # 重新构造源信息
                        table_parts = real_source_table.split('.')
                        if len(table_parts) >= 2:
                            source_info['database'] = table_parts[0]
                            source_info['table'] = table_parts[1]
                        else:
                            source_info['table'] = table_parts[0]
                    else:
                        continue

                # 解析目标字段信息
                target_info = self.extract_database_table_column(target_id)
                if not target_info:
                    continue

                # 如果目标字段没有明确的表信息，尝试从parent_candidates获取
                if not target_info['table']:
                    real_target_table = self.find_real_source_table(target_data, nodes_dict)
                    if real_target_table:
                        table_parts = real_target_table.split('.')
                        if len(table_parts) >= 2:
                            target_info['database'] = table_parts[0]
                            target_info['table'] = table_parts[1]
                        else:
                            target_info['table'] = table_parts[0]
                    else:
                        continue

                # 跳过临时表
                if (self.is_temp_table(f"{source_info['database']}.{source_info['table']}") or
                        self.is_temp_table(f"{target_info['database']}.{target_info['table']}")):
                    continue

                # 添加血缘记录
                self.lineage_records.append({
                    'source_database': source_info['database'],
                    'source_schema': source_info['schema'],
                    'source_table': source_info['table'],
                    'source_column': source_info['column'],
                    'target_database': target_info['database'],
                    'target_schema': target_info['schema'],
                    'target_table': target_info['table'],
                    'target_column': target_info['column']
                })

            except Exception as e:
                print(f"处理边时出错: {e}")
                continue

    def process_single_sql(self, sql_statement, db_type='oracle'):
        """
        处理单条SQL语句，获取血缘关系
        """
        try:
            # 使用LineageRunner分析SQL
            runner = LineageRunner(sql_statement, dialect=db_type, silent_mode=True)

            # 获取cytoscape格式的字段级血缘数据
            try:
                cytoscape_data = runner.to_cytoscape(LineageLevel.COLUMN)

                if cytoscape_data and isinstance(cytoscape_data, list):
                    self.process_cytoscape_lineage(cytoscape_data)
                else:
                    print("未获取到字段级血缘数据")

            except Exception as e:
                print(f"获取字段级血缘失败: {e}")

        except Exception as e:
            print(f"处理SQL语句时出错: {e}")
            print(f"SQL: {sql_statement[:100]}...")

    def generate_oracle_insert_statements(self):
        """
        生成Oracle INSERT语句
        """
        if not self.lineage_records:
            return "-- 没有找到血缘关系数据"

        insert_statements = []
        insert_statements.append("-- SQL血缘关系数据插入语句")
        insert_statements.append(
            "-- 表结构: SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN")
        insert_statements.append("")

        for record in self.lineage_records:
            def format_value(value):
                if not value or value == '':
                    return 'NULL'
                else:
                    escaped_value = str(value).replace("'", "''")
                    return f"'{escaped_value}'"

            source_db = format_value(record['source_database'])
            source_schema = format_value(record['source_schema'])
            source_table = format_value(record['source_table'])
            source_column = format_value(record['source_column'])
            target_db = format_value(record['target_database'])
            target_schema = format_value(record['target_schema'])
            target_table = format_value(record['target_table'])
            target_column = format_value(record['target_column'])

            insert_sql = f"""INSERT INTO LINEAGE_TABLE (SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES ({source_db}, {source_schema}, {source_table}, {source_column}, {target_db}, {target_schema}, {target_table}, {target_column});"""

            insert_statements.append(insert_sql)

        insert_statements.append("")
        insert_statements.append("COMMIT;")

        return "\n".join(insert_statements)

    def process_sql_script(self, sql_script, db_type='oracle'):
        """
        处理完整的SQL脚本
        """
        print("=== 开始处理SQL脚本 ===")
        print(f"数据库类型: {db_type}")

        # 1. 提取临时表
        print("1. 提取临时表...")
        self.extract_temp_tables_from_script(sql_script)

        # 2. 使用sqllineage拆分SQL语句
        print("2. 使用sqllineage拆分SQL语句...")
        sql_statements = self.split_sql_statements(sql_script)
        print(f"共找到 {len(sql_statements)} 条SQL语句")

        # 3. 处理每条SQL
        print("3. 分析血缘关系...")
        self.lineage_records.clear()

        successful_count = 0
        failed_count = 0

        for i, sql in enumerate(sql_statements):
            try:
                print(f"处理第 {i + 1}/{len(sql_statements)} 条SQL...")
                initial_count = len(self.lineage_records)
                self.process_single_sql(sql, db_type)
                new_count = len(self.lineage_records) - initial_count
                print(f"  新增 {new_count} 条血缘关系")
                successful_count += 1
            except Exception as e:
                print(f"第 {i + 1} 条SQL处理失败: {e}")
                failed_count += 1

        print(f"处理完成: 成功 {successful_count} 条, 失败 {failed_count} 条")
        print(f"共提取到 {len(self.lineage_records)} 条血缘关系")

        # 4. 生成Oracle INSERT语句
        print("4. 生成Oracle INSERT语句...")
        oracle_statements = self.generate_oracle_insert_statements()

        return oracle_statements

    def print_lineage_summary(self):
        """
        打印血缘关系摘要
        """
        if not self.lineage_records:
            print("没有找到血缘关系")
            return

        print(f"\n=== 血缘关系摘要 (共{len(self.lineage_records)}条) ===")

        # 统计源表和目标表
        source_tables = set()
        target_tables = set()

        for record in self.lineage_records:
            source_full = f"{record['source_database']}.{record['source_table']}" if record['source_database'] else \
                record['source_table']
            target_full = f"{record['target_database']}.{record['target_table']}" if record['target_database'] else \
                record['target_table']
            source_tables.add(source_full)
            target_tables.add(target_full)

        print(f"涉及源表: {len(source_tables)} 个")
        for table in sorted(source_tables):
            print(f"  - {table}")

        print(f"\n涉及目标表: {len(target_tables)} 个")
        for table in sorted(target_tables):
            print(f"  - {table}")

        print(f"\n血缘关系:")
        for i, record in enumerate(self.lineage_records):
            source_full = f"{record['source_database']}.{record['source_table']}.{record['source_column']}" if record[
                'source_database'] else f"{record['source_table']}.{record['source_column']}"
            target_full = f"{record['target_database']}.{record['target_table']}.{record['target_column']}" if record[
                'target_database'] else f"{record['target_table']}.{record['target_column']}"
            print(f"  {i + 1}. {source_full} -> {target_full}")


# 完整的Demo示例
def main():
    # 创建处理器
    processor = OptimizedSQLLineageProcessor()

    # 复杂SQL脚本示例
    sql_script = """
  insert into query09
select case
           when (select count(*)
                 from store_sales
                 where ss_quantity between 1 and 20) > 74129
               then (select avg(ss_ext_discount_amt)
                     from store_sales
                     where ss_quantity between 1 and 20)
           else (select avg(ss_net_paid)
                 from store_sales
                 where ss_quantity between 1 and 20) end   bucket1,
       case
           when (select count(*)
                 from store_sales
                 where ss_quantity between 21 and 40) > 122840
               then (select avg(ss_ext_discount_amt)
                     from store_sales
                     where ss_quantity between 21 and 40)
           else (select avg(ss_net_paid)
                 from store_sales
                 where ss_quantity between 21 and 40) end  bucket2,
       case
           when (select count(*)
                 from store_sales
                 where ss_quantity between 41 and 60) > 56580
               then (select avg(ss_ext_discount_amt)
                     from store_sales
                     where ss_quantity between 41 and 60)
           else (select avg(ss_net_paid)
                 from store_sales
                 where ss_quantity between 41 and 60) end  bucket3,
       case
           when (select count(*)
                 from store_sales
                 where ss_quantity between 61 and 80) > 10097
               then (select avg(ss_ext_discount_amt)
                     from store_sales
                     where ss_quantity between 61 and 80)
           else (select avg(ss_net_paid)
                 from store_sales
                 where ss_quantity between 61 and 80) end  bucket4,
       case
           when (select count(*)
                 from store_sales
                 where ss_quantity between 81 and 100) > 165306
               then (select avg(ss_ext_discount_amt)
                     from store_sales
                     where ss_quantity between 81 and 100)
           else (select avg(ss_net_paid)
                 from store_sales
                 where ss_quantity between 81 and 100) end bucket5
from reason
where r_reason_sk = 1
;

   
    """

    try:

        print("开始处理SQL脚本...")

        # 处理SQL脚本
        oracle_inserts = processor.process_sql_script(sql_script, db_type='sparksql')

        # 打印摘要
        processor.print_lineage_summary()

        # 输出Oracle INSERT语句
        print("\n" + "=" * 60)
        print("Oracle INSERT语句:")
        print("=" * 60)
        print(oracle_inserts)

    except Exception as e:
        print(f"处理失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
