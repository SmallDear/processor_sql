import re
import json
import os
import glob
from collections import defaultdict
from sqllineage.utils.constant import LineageLevel
from sqllineage.runner import LineageRunner
from sqllineage.utils.helpers import split


# 表类型标记常量
class TableTypeMarkers:
    """表类型标记常量（便于程序处理）"""
    TEMP_TABLE_SUFFIX = "_TEMP_TBL"      # 临时表标记后缀
    SUBQUERY_TABLE_SUFFIX = "_SUBQRY_TBL"  # 子查询表标记后缀


def parse_etl_info_from_path(file_path, base_path):
    """
    从文件路径中解析ETL信息
    
    Args:
        file_path: 完整文件路径，如 D:\aaa\hql\F-DD_00001\aaa.hql
        base_path: 基础路径，如 D:\aaa\hql
        
    Returns:
        dict: 包含 etl_system, etl_job, appname 的字典
    """
    try:
        # 标准化路径（处理路径分隔符）
        file_path = os.path.normpath(file_path)
        base_path = os.path.normpath(base_path)
        
        # 获取相对路径
        relative_path = os.path.relpath(file_path, base_path)
        
        # 分割路径组件
        path_parts = relative_path.split(os.sep)
        
        if len(path_parts) >= 2:
            # etl_system = 目录名称
            etl_system = path_parts[0]
            
            # etl_job = 文件名（包含扩展名）
            etl_job = os.path.basename(file_path)
            
            # appname = etl_system 按 "_" 分割的前面部分
            if '_' in etl_system:
                appname = etl_system.split('_')[0]
            else:
                appname = etl_system
            
            return {
                'etl_system': etl_system,
                'etl_job': etl_job,
                'appname': appname
            }
        else:
            # 如果路径结构不符合预期，使用文件名作为默认值
            etl_job = os.path.basename(file_path)
            return {
                'etl_system': '',
                'etl_job': etl_job,
                'appname': ''
            }
            
    except Exception as e:
        print(f"解析路径失败: {e}")
        etl_job = os.path.basename(file_path) if file_path else ''
        return {
            'etl_system': '',
            'etl_job': etl_job,
            'appname': ''
        }


def extract_use_database(sql_statement):
    """
    从USE语句中提取数据库名称
    
    Args:
        sql_statement: SQL语句（已处理过注释）
        
    Returns:
        str: 数据库名称，如果不是USE语句则返回None
    """
    if not sql_statement or not sql_statement.strip():
        return None
    
    # 简单处理，直接检查USE语句
    sql_upper = sql_statement.strip().upper()
    words = sql_upper.split()
    
    if len(words) >= 2 and words[0] == 'USE':
        # 提取USE后面的数据库名，去掉可能的分号和引号
        db_name = words[1].rstrip(';').strip('`"[]')
        return db_name
    
    return None


def extract_temp_tables_from_script(sql_script):
    """
    从SQL脚本中提取临时表
    新定义：所有CREATE TABLE的表都算临时表
    """
    # 提取所有CREATE TABLE的表
    create_pattern = r'CREATE\s+(?:TEMPORARY\s+|TEMP\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s\(\;]+)'
    create_matches = re.findall(create_pattern, sql_script, re.IGNORECASE | re.MULTILINE)

    # 清理表名（去掉引号、方括号等）
    def clean_table_name(table_name):
        cleaned = table_name.strip('`"[]').lower()
        # 如果有数据库前缀，只保留表名部分
        if '.' in cleaned:
            cleaned = cleaned.split('.')[-1]
        return cleaned

    # 所有CREATE TABLE的表都算临时表
    temp_tables = {clean_table_name(table) for table in create_matches}

    print(f"检测到的临时表（所有CREATE TABLE）: {temp_tables}")
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


def add_table_type_marker(table_name, is_temp_table, is_subquery_table):
    """
    为表名添加类型标记
    
    Args:
        table_name: 原始表名
        is_temp_table: 是否为临时表
        is_subquery_table: 是否为子查询表
    
    Returns:
        str: 带标记的表名
    """
    if not table_name:
        return table_name
    
    # 如果是子查询表，优先标记为子查询（因为子查询表也可能是临时的）
    if is_subquery_table:
        return f"{table_name}{TableTypeMarkers.SUBQUERY_TABLE_SUFFIX}"
    elif is_temp_table:
        return f"{table_name}{TableTypeMarkers.TEMP_TABLE_SUFFIX}"
    else:
        return table_name


def extract_database_table_column(column_id, temp_tables, subquery_nodes, current_database=''):
    """
    从字段ID中提取数据库、表、字段信息，并为表名添加类型标记
    支持默认数据库补充功能，子查询使用虚拟数据库名
    
    Args:
        column_id: 字段标识符
        temp_tables: 临时表集合
        subquery_nodes: 子查询节点集合
        current_database: 当前默认数据库（来自USE语句）
    """
    if not column_id:
        return None

    parts = str(column_id).split('.')
    
    # 提取基本信息
    if len(parts) >= 3:
        # database.table.column 格式
        database = parts[0] if parts[0] not in ('<unknown>', '<default>') else ''
        table = parts[1]
        column = parts[2]
    elif len(parts) == 2:
        # table.column 格式（无数据库前缀）
        database = ''
        table = parts[0]
        column = parts[1]
    elif len(parts) == 1:
        # 只有字段名
        database = ''
        table = ''
        column = parts[0]
    else:
        return None
    
    # 判断表类型
    if table:
        is_temp = is_temp_table(table, temp_tables)
        is_subquery = table in subquery_nodes
        table_with_marker = add_table_type_marker(table, is_temp, is_subquery)
        
        # 数据库名处理逻辑
        if is_subquery:
            # 子查询表使用虚拟数据库名
            database = '<SUBQUERY_DB>'
            print(f"🔧 为子查询表 {table} 设置虚数据库: <SUBQUERY_DB>")
        elif (not database or database == '<default>') and current_database:
            # 物理表和临时表使用默认数据库补充
            database = current_database
            print(f"🔧 为表 {table} 补充默认数据库: {current_database}")
    else:
        table_with_marker = table
    
    return {
        'database': database,
        'table': table_with_marker,
        'column': column
    }


def trace_lineage_through_subqueries(cytoscape_data, temp_tables, current_database=''):
    """
    基于sqllineage的SubQuery类型信息，追踪跨子查询的血缘关系
    修改：不过滤临时表和子查询表，而是为它们添加标记
    支持默认数据库补充
    """
    # 构建节点和边的映射
    nodes_dict = {}
    edges = []
    subquery_nodes = set()
    
    for item in cytoscape_data:
        data = item.get("data", {})
        item_id = data.get("id", "")
        
        if "source" in data and "target" in data:
            edges.append(data)
        else:
            nodes_dict[item_id] = data
            # 识别SubQuery节点
            if data.get("type") == "SubQuery":
                subquery_nodes.add(item_id)
    
    # 构建图结构：出边和入边映射
    outgoing_edges = defaultdict(list)  # node_id -> [target_ids]
    incoming_edges = defaultdict(list)  # node_id -> [source_ids]
    
    for edge in edges:
        source_id = edge.get("source", "")
        target_id = edge.get("target", "")
        if source_id and target_id:
            outgoing_edges[source_id].append(target_id)
            incoming_edges[target_id].append(source_id)
    
    # 收集所有血缘路径（包括临时表和子查询表）
    lineage_paths = []
    
    def is_subquery_column(column_id):
        """判断字段是否属于SubQuery"""
        if not column_id or '.' not in column_id:
            return False
        table_part = column_id.split('.')[0]
        return table_part in subquery_nodes
    
    def is_temp_table_column(column_id):
        """判断字段是否属于临时表"""
        if not column_id or '.' not in column_id:
            return False
        table_part = column_id.split('.')[0]
        return is_temp_table(table_part, temp_tables)
    
    def is_real_table_column(column_id):
        """判断字段是否属于真实表（非SubQuery且非临时表）"""
        if not column_id:
            return False
        
        # 首先检查是否是SubQuery字段
        if is_subquery_column(column_id):
            return False
            
        # 检查是否是临时表字段
        if is_temp_table_column(column_id):
            return False
            
        # 检查该字段的parent_candidates
        column_data = nodes_dict.get(column_id, {})
        parent_candidates = column_data.get("parent_candidates", [])
        
        for candidate in parent_candidates:
            if isinstance(candidate, dict):
                candidate_type = candidate.get("type", "")
                candidate_name = candidate.get("name", "")
                
                # 如果parent是Table类型且不是临时表，则是真实表字段
                if (candidate_type == "Table" and 
                    not is_temp_table(candidate_name, temp_tables)):
                    return True
        
        # 如果没有parent_candidates，通过字段ID判断
        if '.' in column_id:
            table_part = column_id.split('.')[0]
            return not is_temp_table(table_part, temp_tables)
        
        return False
    
    def trace_through_temp_tables(start_column_id, visited=None):
        """追踪跨越临时表的血缘关系，返回最终的真实表字段"""
        if visited is None:
            visited = set()
        
        if start_column_id in visited:
            return []  # 避免循环
        
        visited.add(start_column_id)
        
        # 如果当前字段就是真实表字段，直接返回
        if is_real_table_column(start_column_id):
            return [start_column_id]
        
        # 如果是临时表字段或SubQuery字段，继续追踪其源字段
        real_sources = []
        for source_id in incoming_edges.get(start_column_id, []):
            deeper_sources = trace_through_temp_tables(source_id, visited.copy())
            real_sources.extend(deeper_sources)
        
        return real_sources
    
    def trace_to_real_source(subquery_column_id, visited=None):
        """从子查询字段追踪到真实源表字段"""
        if visited is None:
            visited = set()
        
        if subquery_column_id in visited:
            return []  # 避免循环
        
        visited.add(subquery_column_id)
        real_sources = []
        
        # 查找该子查询字段的所有源字段
        for source_id in incoming_edges.get(subquery_column_id, []):
            if is_subquery_column(source_id):
                # 如果源还是子查询字段，继续递归追踪
                deeper_sources = trace_to_real_source(source_id, visited.copy())
                real_sources.extend(deeper_sources)
            elif is_real_table_column(source_id):
                # 如果源是真实表字段，添加到结果
                real_sources.append(source_id)
            elif is_temp_table_column(source_id):
                # 如果源是临时表字段，追踪到真实表
                deeper_sources = trace_through_temp_tables(source_id, visited.copy())
                real_sources.extend(deeper_sources)
        
        return real_sources
    
    # 处理策略：收集所有血缘关系，包括涉及临时表和子查询表的
    
    if subquery_nodes:
        # 有SubQuery的情况：处理跨SubQuery的血缘关系
        processed_subquery_columns = set()
        
        for edge in edges:
            source_id = edge.get("source", "")
            target_id = edge.get("target", "")
            
            # 处理 SubQuery字段 → 最终表字段 的边
            if (is_subquery_column(source_id) and 
                not is_subquery_column(target_id) and  # 目标不是子查询字段
                source_id not in processed_subquery_columns):
                
                processed_subquery_columns.add(source_id)
                
                # 追踪该SubQuery字段的真实源表
                real_sources = trace_to_real_source(source_id)
                
                # 建立真实源表到最终目标表的血缘关系
                for real_source_id in real_sources:
                    lineage_paths.append({
                        'source': real_source_id,
                        'target': target_id
                    })
    
    # 处理所有直接的字段到字段的边（包括临时表和子查询表）
    for edge in edges:
        source_id = edge.get("source", "")
        target_id = edge.get("target", "")
        
        # 收集所有直接的血缘关系（不再过滤临时表和子查询表）
        if source_id and target_id and '.' in source_id and '.' in target_id:
            lineage_paths.append({
                'source': source_id,
                'target': target_id
            })
        
    return lineage_paths, subquery_nodes


def process_cytoscape_lineage(cytoscape_data, temp_tables, current_database, etl_system, etl_job, sql_path, sql_no):
    """
    处理cytoscape格式的血缘数据
    修改：不过滤临时表和子查询表，而是为它们添加标记
    支持默认数据库补充
    """
    lineage_records = []

    if not cytoscape_data:
        return lineage_records
    # 使用基于sqllineage SubQuery类型的追踪算法
    lineage_paths, subquery_nodes = trace_lineage_through_subqueries(cytoscape_data, temp_tables, current_database)
    
    for path in lineage_paths:
        source_id = path['source']
        target_id = path['target']
        
        # 解析源字段信息（会自动添加标记和默认数据库）
        source_info = extract_database_table_column(source_id, temp_tables, subquery_nodes, current_database)
        if not source_info or not source_info['table']:
            continue
        
        # 解析目标字段信息（会自动添加标记和默认数据库）
        target_info = extract_database_table_column(target_id, temp_tables, subquery_nodes, current_database)
        if not target_info or not target_info['table']:
            continue
        
        # 不再跳过临时表和子查询表，直接添加血缘记录（表名已带标记）
        record = {
            'etl_system': etl_system,
            'etl_job': etl_job,
            'sql_path': sql_path,
            'sql_no': sql_no,
            'source_database': source_info['database'],
            'source_table': source_info['table'],  # 已包含标记
            'source_column': source_info['column'],
            'target_database': target_info['database'],
            'target_table': target_info['table'],  # 已包含标记
            'target_column': target_info['column']
        }
        
        lineage_records.append(record)
    
    print(f"✅ 解析出 {len(lineage_records)} 条字段级血缘关系（包含标记的临时表和子查询表，默认数据库已补充）")
    return lineage_records


# DDL和控制语句类型常量
class DDLStatementTypes:
    """不需要解析血缘关系的语句类型（类似Java静态类/枚举）"""
    
    # 不需要解析血缘关系的语句关键字（注意：USE语句已移除，需要特殊处理）
    SKIP_KEYWORDS = frozenset([
        'ALTER', 'DROP', 'GRANT', 'REVOKE', 'SET', 'SHOW', 
        'DESCRIBE', 'DESC', 'EXPLAIN', 'ANALYZE', 'TRUNCATE', 
        'COMMENT', 'REFRESH', 'MSCK', 'CACHE', 'UNCACHE',
        'CREATE DATABASE', 'CREATE SCHEMA', 'CREATE USER', 'CREATE ROLE', 
        'CREATE INDEX', 'CREATE FUNCTION', 'CREATE PROCEDURE'
    ])


def is_ddl_or_control_statement(sql_statement):
    """
    检测SQL语句是否为不需要解析血缘关系的语句
    修改：USE语句不跳过，需要特殊处理来提取数据库名
    """
    if not sql_statement or not sql_statement.strip():
        return False, None
    
    # 简单分词处理（假设输入已清理过注释）
    sql_upper = sql_statement.strip().upper()
    words = sql_upper.split()
    
    if not words:
        return False, None
    
    # 特殊处理USE语句 - 不跳过，需要提取数据库名
    if words[0] == 'USE':
        return False, None  # 不跳过USE语句，让后续逻辑处理
    
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


def is_from_statement(sql_statement):
    """
    检测SQL语句是否以FROM开头（Hive特殊语法）
    
    Args:
        sql_statement: SQL语句
        
    Returns:
        bool: 是否为FROM开头的语句
    """
    if not sql_statement or not sql_statement.strip():
        return False
    
    # 去除前导空白并转换为大写
    sql_upper = sql_statement.strip().upper()
    words = sql_upper.split()
    
    if not words:
        return False
    
    # 检查第一个关键字是否为FROM
    return words[0] == 'FROM'


def process_single_sql(sql_statement, temp_tables, current_database, etl_system, etl_job, sql_path, sql_no, db_type='oracle'):
    """
    处理单条SQL语句，获取血缘关系
    修改：支持USE语句处理、默认数据库维护和FROM开头语句的non-validating处理
    
    Returns:
        tuple: (lineage_records, new_current_database)
    """
    lineage_records = []
    new_current_database = current_database
    
    # 首先检查是否为USE语句
    use_database = extract_use_database(sql_statement)
    if use_database:
        new_current_database = use_database
        return lineage_records, new_current_database
    
    # 检查是否为DDL或控制语句
    is_ddl, stmt_type = is_ddl_or_control_statement(sql_statement)
    
    if is_ddl:
        print(f"⏭️  跳过{stmt_type}语句（无血缘关系解析意义）")
        return lineage_records, new_current_database
    
    # 检查是否为FROM开头的语句，如果是则使用non-validating dialect
    actual_db_type = db_type
    if is_from_statement(sql_statement):
        actual_db_type = 'non-validating'
        print(f"🔧 检测到FROM开头语句，使用non-validating dialect解析")
    
    try:
        # 使用LineageRunner分析SQL，根据语句类型选择适当的dialect
        runner = LineageRunner(sql_statement, dialect=actual_db_type, silent_mode=True)

        # 获取cytoscape格式的字段级血缘数据
        try:
            cytoscape_data = runner.to_cytoscape(LineageLevel.COLUMN)
            
            if cytoscape_data and isinstance(cytoscape_data, list):
                lineage_records = process_cytoscape_lineage(cytoscape_data, temp_tables, current_database, etl_system, etl_job, sql_path, sql_no)
                print(f"✅ 解析出 {len(lineage_records)} 条字段级血缘关系")
            else:
                print("❌ 未获取到字段级血缘数据")

        except Exception as e:
            print(f"❌ 获取字段级血缘失败: {e}")

    except Exception as e:
        print(f"❌ 创建LineageRunner时出错: {e}")

    return lineage_records, new_current_database


def generate_oracle_insert_statements(lineage_records):
    """
    生成Oracle INSERT语句（包含etl_system、etl_job、sql_path、sql_no字段）
    
    Args:
        lineage_records: 血缘关系记录列表
        
    Returns:
        str: Oracle INSERT语句
    """
    if not lineage_records:
        return "-- 没有找到血缘关系数据"

    insert_statements = []
    insert_statements.append("-- SQL血缘关系数据插入语句（包含标记的临时表和子查询表，支持默认数据库）")
    insert_statements.append("-- 表结构: ETL_SYSTEM, ETL_JOB, SQL_PATH, SQL_NO, SOURCE_DATABASE, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_TABLE, TARGET_COLUMN")
    insert_statements.append(f"-- 临时表标记后缀: {TableTypeMarkers.TEMP_TABLE_SUFFIX}")
    insert_statements.append(f"-- 子查询表标记后缀: {TableTypeMarkers.SUBQUERY_TABLE_SUFFIX}")
    insert_statements.append("-- 支持USE语句自动补充默认数据库名")
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
        sql_path = format_value(record['sql_path'])
        sql_no = record['sql_no'] if record['sql_no'] is not None else 'NULL'
        source_db = format_value(record['source_database'])
        source_table = format_value(record['source_table'])
        source_column = format_value(record['source_column'])
        target_db = format_value(record['target_database'])
        target_table = format_value(record['target_table'])
        target_column = format_value(record['target_column'])

        insert_sql = f"""INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SQL_PATH, SQL_NO, SOURCE_DATABASE, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_TABLE, TARGET_COLUMN)
VALUES ({etl_system}, {etl_job}, {sql_path}, {sql_no}, {source_db}, {source_table}, {source_column}, {target_db}, {target_table}, {target_column});"""

        insert_statements.append(insert_sql)

    insert_statements.append("")
    insert_statements.append("COMMIT;")

    return "\n".join(insert_statements)


def process_sql_script(sql_script, etl_system='', etl_job='', sql_path='', db_type='oracle'):
    """
    处理SQL脚本（支持单条SQL或完整脚本）
    修改：支持USE语句处理和默认数据库维护
    """
    print("=== 开始处理SQL脚本（标记版本，支持默认数据库）===")

    # 1. 提取临时表
    temp_tables = extract_temp_tables_from_script(sql_script)

    # 2. 拆分SQL语句
    sql_statements = split_sql_statements(sql_script)
    print(f"共找到 {len(sql_statements)} 条SQL语句")

    # 3. 处理每条SQL，维护当前数据库状态
    all_lineage_records = []
    current_database = ''  # 默认数据库变量
    
    for i, sql in enumerate(sql_statements):
        sql_no = i + 1
        print(f"处理第 {sql_no}/{len(sql_statements)} 条SQL...")
        
        # 处理SQL语句，返回血缘记录和更新后的当前数据库
        lineage_records, current_database = process_single_sql(
            sql, temp_tables, current_database, etl_system, etl_job, sql_path, sql_no, db_type
        )
        all_lineage_records.extend(lineage_records)
        
        print(f"  新增 {len(lineage_records)} 条血缘关系")

    print(f"共提取到 {len(all_lineage_records)} 条血缘关系")

    # 4. 生成Oracle INSERT语句
    oracle_statements = generate_oracle_insert_statements(all_lineage_records)

    return oracle_statements


def lineage_analysis(sql=None, file=None, db_type='oracle'):
    """
    血缘关系分析主入口（标记版本，支持默认数据库）
    
    Args:
        sql: SQL脚本内容字符串
        file: SQL文件路径（单个文件或目录）
        db_type: 数据库类型，默认'oracle'
        
    Returns:
        str: Oracle INSERT语句（包含标记的临时表和子查询表，支持默认数据库）
    """
    
    if sql is not None and file is not None:
        raise ValueError("sql和file参数不能同时提供，只能选择其中一个")
    
    if sql is None and file is None:
        raise ValueError("必须提供sql或file参数")

    if sql is not None:
        # 处理SQL字符串
        print("=== 处理SQL字符串（标记版本，支持默认数据库）===")
        return process_sql_script(sql, db_type=db_type)
        
    elif file is not None:
        # 处理文件路径
        print(f"=== 处理文件路径（标记版本，支持默认数据库）: {file} ===")
        
        if os.path.isfile(file):
            # 处理单个文件
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                # 使用父目录作为基础路径，从路径中解析ETL信息
                base_path = os.path.dirname(file)
                etl_info = parse_etl_info_from_path(file, base_path)
                
                return process_sql_script(sql_content, etl_info['etl_system'], etl_info['etl_job'], file, db_type)
                
            except Exception as e:
                return f"-- 处理文件失败: {e}"
                
        elif os.path.isdir(file):
            # 处理目录 - 使用新的路径解析逻辑
            sql_extensions = ['*.sql', '*.SQL', '*.hql', '*.HQL']
            
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
            
            all_results = []
            
            for i, sql_file in enumerate(sql_files):
                try:
                    print(f"\n处理文件 {i+1}/{file_count}: {sql_file}")
                    
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    # 从完整路径中解析ETL信息
                    etl_info = parse_etl_info_from_path(sql_file, file)
                    print(f"  解析到的ETL信息: etl_system={etl_info['etl_system']}, etl_job={etl_info['etl_job']}, appname={etl_info['appname']}")
                    
                    result = process_sql_script(sql_content, etl_info['etl_system'], etl_info['etl_job'], sql_file, db_type)
                    all_results.append(result)
                    
                except Exception as e:
                    print(f"处理文件 {sql_file} 失败: {e}")
                    all_results.append(f"-- 文件: {sql_file}\n-- 处理失败: {e}")
            
            # 合并结果
            combined_result = []
            combined_result.append(f"-- 共处理 {file_count} 个文件（支持ETL路径解析和USE语句的默认数据库）")
            combined_result.append("")
            
            for result in all_results:
                combined_result.append(result)
                combined_result.append("")
            
            return "\n".join(combined_result)
        else:
            return f"-- 路径不存在: {file}"


if __name__ == "__main__":
    
    # 测试SQL示例（包含USE语句）
    test_sql = """
    USE mydb;
    
    CREATE TEMPORARY TABLE temp_sales AS (
        SELECT customer_id, SUM(amount) as total_amount
        FROM orders
        WHERE order_date >= '2023-01-01'
    );
    
    INSERT INTO customer_summary (customer_id, total_purchase, last_update)
    SELECT 
        t.customer_id,
        t.total_amount,
        SYSDATE
    FROM temp_sales t
    JOIN customers c ON t.customer_id = c.id
    WHERE c.status = 'ACTIVE';
    """
    
    result = lineage_analysis(sql=test_sql, db_type='oracle')
    print("结果:")
    print(result) 