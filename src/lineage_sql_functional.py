import re
import json
import os
import glob
from collections import defaultdict
from sqllineage.utils.constant import LineageLevel
from sqllineage.runner import LineageRunner
from sqllineage.utils.helpers import split


def extract_temp_tables_from_script(sql_script):
    """
    从SQL脚本中提取临时表
    临时表定义：在脚本中既有CREATE TABLE又有DROP TABLE的表
    """
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
    temp_tables = created_tables.intersection(dropped_tables)

    print(f"检测到的临时表: {temp_tables}")
    return temp_tables


def split_sql_statements(sql_script):
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


def is_temp_table(table_identifier, temp_tables):
    """
    检查表是否为临时表
    """
    if not table_identifier or not temp_tables:
        return False

    # 提取表名（处理各种格式）
    table_name = str(table_identifier).lower()
    if '.' in table_name:
        table_name = table_name.split('.')[-1]

    return table_name in temp_tables


def is_subquery_from_cytoscape(column_data):
    """
    基于cytoscape数据判断是否为子查询
    修改：正确处理CTE(WITH子句)，不要过度过滤
    """
    try:
        if "parent_candidates" not in column_data:
            return False

        parent_candidates = column_data["parent_candidates"]
        if not isinstance(parent_candidates, list):
            return False

        # 对于只有一个SubQuery候选且名称不包含特殊标识符的，可能是CTE，应该保留
        if len(parent_candidates) == 1:
            candidate = parent_candidates[0]
            if isinstance(candidate, dict) and candidate.get("type") == "SubQuery":
                name = candidate.get("name", "")
                # 如果名称包含特殊字符(如随机数字)，可能是内嵌子查询，跳过
                # 如果是普通名称，可能是CTE，保留
                if "subquery_" in name and name.replace("subquery_", "").replace("-", "").isdigit():
                    return True  # 跳过内嵌子查询
                else:
                    return False  # 保留CTE
        
        # 检查是否所有parent candidates都是SubQuery，且没有实际表引用
        all_subquery = True
        has_table = False
        
        for candidate in parent_candidates:
            if isinstance(candidate, dict):
                if candidate.get("type") == "Table":
                    has_table = True
                    all_subquery = False
                elif candidate.get("type") != "SubQuery":
                    all_subquery = False
        
        # 如果有表引用，则不跳过
        if has_table:
            return False
            
        # 只有当所有候选都是SubQuery且没有表引用时，才认为是需要跳过的子查询
        return all_subquery

    except Exception as e:
        print(f"判断子查询时出错: {e}")
        return False


def extract_database_table_column(column_id):
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


def find_real_source_table(column_data, nodes_dict, temp_tables):
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
            if table_name and not is_temp_table(table_name, temp_tables):
                return table_name

    return None


def process_cytoscape_lineage(cytoscape_data, temp_tables, etl_system, etl_job, sql_path, sql_no):
    """
    处理cytoscape格式的血缘数据
    """
    lineage_records = []

    if not cytoscape_data:
        return lineage_records

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

    print(f"📊 处理 {len(edges)} 条血缘边...")

    # 处理每条边（血缘关系）
    skipped_edges = 0
    
    for edge in edges:
        try:
            source_id = edge.get("source", "")
            target_id = edge.get("target", "")

            if not source_id or not target_id:
                skipped_edges += 1
                continue

            # 获取源和目标的节点信息
            source_data = nodes_dict.get(source_id, {})
            target_data = nodes_dict.get(target_id, {})

            # 跳过子查询
            source_is_subquery = is_subquery_from_cytoscape(source_data)
            target_is_subquery = is_subquery_from_cytoscape(target_data)
            
            if source_is_subquery or target_is_subquery:
                skipped_edges += 1
                continue

            # 解析源字段信息
            source_info = extract_database_table_column(source_id)
            if not source_info:
                skipped_edges += 1
                continue

            # 如果源字段没有明确的表信息，尝试从parent_candidates获取
            if not source_info['table']:
                real_source_table = find_real_source_table(source_data, nodes_dict, temp_tables)
                if real_source_table:
                    # 重新构造源信息
                    table_parts = real_source_table.split('.')
                    if len(table_parts) >= 2:
                        source_info['database'] = table_parts[0]
                        source_info['table'] = table_parts[1]
                    else:
                        source_info['table'] = table_parts[0]
                else:
                    skipped_edges += 1
                    continue

            # 解析目标字段信息
            target_info = extract_database_table_column(target_id)
            if not target_info:
                skipped_edges += 1
                continue

            # 如果目标字段没有明确的表信息，尝试从parent_candidates获取
            if not target_info['table']:
                real_target_table = find_real_source_table(target_data, nodes_dict, temp_tables)
                if real_target_table:
                    table_parts = real_target_table.split('.')
                    if len(table_parts) >= 2:
                        target_info['database'] = table_parts[0]
                        target_info['table'] = table_parts[1]
                    else:
                        target_info['table'] = table_parts[0]
                else:
                    skipped_edges += 1
                    continue

            # 跳过临时表
            source_table_full = f"{source_info['database']}.{source_info['table']}"
            target_table_full = f"{target_info['database']}.{target_info['table']}"
            source_is_temp = is_temp_table(source_table_full, temp_tables)
            target_is_temp = is_temp_table(target_table_full, temp_tables)
            
            if source_is_temp or target_is_temp:
                skipped_edges += 1
                continue

            # 添加血缘记录（包含所有字段）
            record = {
                'etl_system': etl_system,
                'etl_job': etl_job,
                'sql_path': sql_path,
                'sql_no': sql_no,
                'source_database': source_info['database'],
                'source_schema': source_info['schema'],
                'source_table': source_info['table'],
                'source_column': source_info['column'],
                'target_database': target_info['database'],
                'target_schema': target_info['schema'],
                'target_table': target_info['table'],
                'target_column': target_info['column']
            }
            
            lineage_records.append(record)

        except Exception as e:
            print(f" 处理边时出错: {e}")
            skipped_edges += 1
            continue

    if skipped_edges > 0:
        print(f"⏭️  跳过 {skipped_edges} 条边（子查询/临时表/解析失败）")
        
    return lineage_records


# DDL和控制语句类型常量
class DDLStatementTypes:
    """不需要解析血缘关系的语句类型（类似Java静态类/枚举）"""
    
    # 不需要解析血缘关系的语句关键字
    SKIP_KEYWORDS = frozenset([
        'ALTER', 'DROP', 'USE', 'GRANT', 'REVOKE', 'SET', 'SHOW', 
        'DESCRIBE', 'DESC', 'EXPLAIN', 'ANALYZE', 'TRUNCATE', 
        'COMMENT', 'REFRESH', 'MSCK', 'CACHE', 'UNCACHE',
        'CREATE DATABASE', 'CREATE SCHEMA', 'CREATE USER', 'CREATE ROLE', 
        'CREATE INDEX', 'CREATE FUNCTION', 'CREATE PROCEDURE'
    ])


def is_ddl_or_control_statement(sql_statement):
    """
    检测SQL语句是否为不需要解析血缘关系的语句
    简化版：只跳过真正不需要解析的语句，有血缘关系的CREATE TABLE AS SELECT等应该正常解析
    """
    if not sql_statement or not sql_statement.strip():
        return False, None
    
    # 简单分词处理（假设输入已清理过注释）
    sql_upper = sql_statement.strip().upper()
    words = sql_upper.split()
    
    if not words:
        return False, None
    
    # 检查单关键词语句
    first_word = words[0]
    if first_word in DDLStatementTypes.SKIP_KEYWORDS:
        return True, first_word
    
    # 检查两关键词组合语句
    if len(words) >= 2:
        two_words = f"{words[0]} {words[1]}"
        if two_words in DDLStatementTypes.SKIP_KEYWORDS:
            return True, two_words
    
    # 特殊处理CREATE语句
    if first_word == 'CREATE' and len(words) >= 2:
        second_word = words[1]
        
        # CREATE TABLE/VIEW 语句细分
        if second_word in ('TABLE', 'VIEW') or (second_word in ('TEMPORARY', 'TEMP') and len(words) >= 3 and words[2] in ('TABLE', 'VIEW')):
            # 如果包含AS关键字，说明是CREATE TABLE AS SELECT，有血缘关系，需要解析
            if 'AS' in words and 'SELECT' in words:
                return False, None  # 不跳过，需要解析血缘关系
            else:
                # 纯CREATE TABLE定义语句，无血缘关系，跳过
                if second_word in ('TEMPORARY', 'TEMP'):
                    return True, f'CREATE {second_word} {words[2]}'
                else:
                    return True, f'CREATE {second_word}'
    
    # 其他语句（INSERT、SELECT等）都不跳过，正常解析血缘关系
    return False, None


def process_single_sql(sql_statement, temp_tables, etl_system, etl_job, sql_path, sql_no, db_type='oracle'):
    """
    处理单条SQL语句，获取血缘关系
    """
    lineage_records = []
    
    # 首先检查是否为DDL或控制语句
    is_ddl, stmt_type = is_ddl_or_control_statement(sql_statement)
    
    if is_ddl:
        print(f"⏭️  跳过{stmt_type}语句（无血缘关系解析意义）")
        return lineage_records
    
    try:
        # 使用LineageRunner分析SQL
        runner = LineageRunner(sql_statement, dialect=db_type, silent_mode=True)

        # 获取cytoscape格式的字段级血缘数据
        try:
            cytoscape_data = runner.to_cytoscape(LineageLevel.COLUMN)
            
            if cytoscape_data and isinstance(cytoscape_data, list):
                lineage_records = process_cytoscape_lineage(cytoscape_data, temp_tables, etl_system, etl_job, sql_path, sql_no)
                print(f"✅ 解析出 {len(lineage_records)} 条字段级血缘关系")
            else:
                print("❌ 未获取到字段级血缘数据")

        except Exception as e:
            print(f"❌ 获取字段级血缘失败: {e}")

    except Exception as e:
        print(f"❌ 创建LineageRunner时出错: {e}")

    return lineage_records


def generate_oracle_insert_statements(lineage_records):
    """
    生成Oracle INSERT语句（包含etl_system和etl_job字段）
    """
    if not lineage_records:
        return "-- 没有找到血缘关系数据"

    insert_statements = []
    insert_statements.append("-- SQL血缘关系数据插入语句")
    insert_statements.append(
        "-- 表结构: ETL_SYSTEM, ETL_JOB, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN")
    insert_statements.append("")

    for record in lineage_records:
        def format_value(value):
            if not value or value == '':
                return 'NULL'
            else:
                escaped_value = str(value).replace("'", "''")
                return f"'{escaped_value}'"

        etl_system = format_value(record['etl_system'])
        etl_job = format_value(record['etl_job'])
        source_db = format_value(record['source_database'])
        source_schema = format_value(record['source_schema'])
        source_table = format_value(record['source_table'])
        source_column = format_value(record['source_column'])
        target_db = format_value(record['target_database'])
        target_schema = format_value(record['target_schema'])
        target_table = format_value(record['target_table'])
        target_column = format_value(record['target_column'])

        insert_sql = f"""INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES ({etl_system}, {etl_job}, {source_db}, {source_schema}, {source_table}, {source_column}, {target_db}, {target_schema}, {target_table}, {target_column});"""

        insert_statements.append(insert_sql)

    insert_statements.append("")
    insert_statements.append("COMMIT;")

    return "\n".join(insert_statements)


def process_sql_script(sql_script, etl_system='', etl_job='', sql_path='', db_type='oracle'):
    """
    处理SQL脚本（支持单条SQL或完整脚本）
    """
    print("=== 开始处理SQL脚本 ===")

    # 1. 提取临时表
    temp_tables = extract_temp_tables_from_script(sql_script)

    # 2. 拆分SQL语句
    sql_statements = split_sql_statements(sql_script)
    print(f"共找到 {len(sql_statements)} 条SQL语句")

    # 3. 处理每条SQL
    all_lineage_records = []
    for i, sql in enumerate(sql_statements):
        sql_no = i + 1
        print(f"处理第 {sql_no}/{len(sql_statements)} 条SQL...")
        
        lineage_records = process_single_sql(sql, temp_tables, etl_system, etl_job, sql_path, sql_no, db_type)
        all_lineage_records.extend(lineage_records)
        
        print(f"  新增 {len(lineage_records)} 条血缘关系")

    print(f"共提取到 {len(all_lineage_records)} 条血缘关系")

    # 4. 生成Oracle INSERT语句
    oracle_statements = generate_oracle_insert_statements(all_lineage_records)

    return oracle_statements


def lineage_analysis(sql=None, file=None, db_type=None):
    """
    Args:
        sql: SQL脚本内容字符串
        file: SQL文件路径（单个文件或目录）
        db_type: 数据库类型，默认'oracle'
        
    Returns:
        str: Oracle INSERT语句
    """
    
    if sql is not None and file is not None:
        raise ValueError("sql和file参数不能同时提供，只能选择其中一个")
    
    if sql is None and file is None:
        raise ValueError("必须提供sql或file参数")

    if sql is not None:
        # 处理SQL字符串
        print("=== 处理SQL字符串 ===")
        return process_sql_script(sql, db_type=db_type)
        
    elif file is not None:
        # 处理文件路径
        print(f"=== 处理文件路径: {file} ===")
        
        if os.path.isfile(file):
            # 处理单个文件
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                etl_system = os.path.basename(os.path.dirname(file))
                etl_job = os.path.splitext(os.path.basename(file))[0]
                
                return process_sql_script(sql_content, etl_system, etl_job, file, db_type)
                
            except Exception as e:
                return f"-- 处理文件失败: {e}"
                
        elif os.path.isdir(file):
            # 处理目录
            sql_extensions = ['*.sql', '*.SQL', '*.hql', '*.HQL']
            all_results = []
            
            etl_system = os.path.basename(os.path.abspath(file))
            
            # 使用集合去重，避免Windows系统中大小写不敏感导致的重复文件
            all_files = set()
            for ext in sql_extensions:
                pattern = os.path.join(file, '**', ext)
                files = glob.glob(pattern, recursive=True)
                all_files.update(files)
            
            # 转换为列表并排序，确保处理顺序一致
            sql_files = sorted(list(all_files))
            file_count = len(sql_files)
            
            if file_count == 0:
                return "-- 未找到任何SQL文件"
            
            print(f"找到 {file_count} 个SQL文件")
            
            for i, sql_file in enumerate(sql_files):
                try:
                    print(f"\n处理文件 {i+1}/{file_count}: {sql_file}")
                    
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    etl_job = os.path.splitext(os.path.basename(sql_file))[0]
                    
                    result = process_sql_script(sql_content, etl_system, etl_job, sql_file, db_type)
                    all_results.append(result)
                    
                except Exception as e:
                    print(f"处理文件 {sql_file} 失败: {e}")
                    all_results.append(f"-- 文件: {sql_file}\n-- 处理失败: {e}")
            
            # 合并结果
            combined_result = []
            combined_result.append(f"-- 共处理 {file_count} 个文件")
            combined_result.append("")
            
            for result in all_results:
                combined_result.append(result)
                combined_result.append("")
            
            return "\n".join(combined_result)
        else:
            return f"-- 路径不存在: {file}"


if __name__ == "__main__":
    
    result = lineage_analysis( file='D:\\python3.8.3\\Lib\\site-packages\\sqllineage\\data\\tpcds\\query01.sql', 
                              db_type='ansi')
    print("结果:", result[:200] + "..." if len(result) > 200 else result)
