#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL清理工具 - 完整Demo
删除所有注释和参数，特殊处理IN语句
"""

import re
import sys


def clean_sql(sql):
    """
    清理SQL中的注释和参数
    
    处理顺序：
    1. 单行内将''内的-- ,# 删除
    2. 单行内 函数里''内的参数删除
    3. 单行内 函数内不在 ''的参数删除
    4. 删除单行--开头的注释
    5. 删除单行内 /* */
    6. 删除多行 /* */
    
    Args:
        sql (str): 原始SQL
        
    Returns:
        str: 清理后的SQL
    """
    if not sql:
        return ""
    
    # 按行处理，避免跨行问题
    lines = sql.split('\n')
    processed_lines = []
    
    for line in lines:
        # 步骤1: 处理单行内''内的注释符号删除
        def clean_string_comments(match):
            full_string = match.group(0)
            inner_content = full_string[1:-1]  # 去掉引号
            # 删除字符串内的注释符号，但保留内容
            inner_content = re.sub(r'--', '', inner_content)
            inner_content = re.sub(r'#+', '', inner_content)
            return f"'{inner_content}'"
        
        line = re.sub(r"'([^'\\]|\\.)*'", clean_string_comments, line)
        
        # 步骤2: 处理单行内''内的参数删除  
        def clean_string_params(match):
            full_string = match.group(0)
            inner_content = full_string[1:-1]  # 去掉引号
            # 删除字符串内的参数
            inner_content = re.sub(r'\$\{[^}]*\}', '', inner_content)
            return f"'{inner_content}'"
        
        line = re.sub(r"'([^'\\]|\\.)*'", clean_string_params, line)
        
        # 步骤3: 特殊处理 IN ${param} → IN ()
        line = re.sub(r'\bIN\s+\$\{[^}]*\}', 'IN ()', line, flags=re.IGNORECASE)
        
        # 步骤4: 单行内函数内不在''的参数删除
        line = re.sub(r'\$\{[^}]*\}', '', line)
        
        # 步骤5: 删除单行--开头的注释（从--到行尾）
        line = re.sub(r'--.*$', '', line)
        
        # 步骤6: 删除单行内 /* */
        line = re.sub(r'/\*.*?\*/', '', line)
        
        processed_lines.append(line)
    
    # 重新组合
    sql = '\n'.join(processed_lines)
    
    # 步骤7: 删除多行 /* */
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    
    # 步骤8: 删除单行 # 注释（处理跨行的情况）
    sql = re.sub(r'#+.*$', '', sql, flags=re.MULTILINE)
    
    # 步骤9: 删除影响sqllineage解析的语句
    # 删除存储格式语句
    sql = re.sub(r'\bSTORED\s+AS\s+\w+', '', sql, flags=re.IGNORECASE)
    
    # 删除LOCATION语句
    sql = re.sub(r'\bLOCATION\s+[\'"][^\'\"]*[\'"]', '', sql, flags=re.IGNORECASE)
    
    # 删除PARTITIONED BY语句
    sql = re.sub(r'\bPARTITIONED\s+BY\s*\([^)]*\)', '', sql, flags=re.IGNORECASE)
    
    # 删除TBLPROPERTIES语句
    sql = re.sub(r'\bTBLPROPERTIES\s*\([^)]*\)', '', sql, flags=re.IGNORECASE)
    
    # 删除ROW FORMAT语句
    sql = re.sub(r'\bROW\s+FORMAT\s+[^;]*', '', sql, flags=re.IGNORECASE)
    
    # 步骤10: 清理空白和符号
    sql = re.sub(r',\s*,', ',', sql)      # 连续逗号
    sql = re.sub(r'\(\s*,', '(', sql)     # 括号后逗号
    sql = re.sub(r',\s*\)', ')', sql)     # 括号前逗号
    sql = re.sub(r' +', ' ', sql)         # 多空格
    sql = re.sub(r'\n\s*\n', '\n', sql)   # 空行
    
    # 步骤11: 整理格式
    lines = [line.strip() for line in sql.split('\n') if line.strip()]
    return '\n'.join(lines)




def main():
    sql = """

-- 这是测试SQL文件
############# 表头注释 #############


#############
爱看书的风口浪尖按时灯笼裤飞机啊六十九看
#############


*****************
介绍了雕刻技法决定了接口
爱上邓丽君了了 
******************

****************** 氨基酸的房间昂离开就 ******************

/* 
多行注释包含参数${comment_param}
用于测试清理功能
*/

create table if not exists db.sales_data (
    customer_id bigint,
    product_name string,
    status_desc string,
    amount_usd double,
    formatted_date string,
    region_type string
)
comment 'sales_data'
row format delimited fields terminated by '\t'
stored as orcfile;

WITH sales_data AS (
    SELECT 
        customer_id 'ttt#########------',
        product_name,
        '#######订单状态：${order_status}' as status_desc, -- 字符串内参数
        amount * ${exchange_rate} as amount_usd, -- 普通参数
        DATE_FORMAT(order_date, '${date_format}')  formatted_date,
        CASE 
            WHEN region = '${target_region}' THEN 'Target'
            WHEN region IN ${excluded_regions} THEN 'Excluded' -- IN 参数
            ELSE 'Other'
        END as region_type
    FROM db${source_schema}. table${orders_table} -- 表名参数
    WHERE order_date >= '${start_date}' -- 字符串内日期参数
        AND customer_id IN ${customer_list} -- IN 参数列表
        AND status NOT IN ${invalid_status_list}  ##
    ##### 中间注释
)
/* 
另一个多行注释
包含业务逻辑说明
*/
SELECT 
    sd.customer_id  '---11--1',
    sd.product_name as '#####lsdlkfj------------------11',
    /* 另一个表参数 */
    sd.status_desc,
    sd.amount_usd,
    c.customer_name,
    CONCAT('${prefix}', sd.product_name, ${suffix}) as full_product_name,
    func_custom('###${param1}', ${param2}, '固定文本${param3}') as computed_value
FROM sales_data sd
JOIN /* 另一个表参数 */  ${target_schema}.${customers_table} c /* 另一个表参数 */
    ON sd.customer_id = c.id
WHERE sd.region_type IN ${final_region_filter} /* 最终过滤 */
    AND c.status = '${customer_status}' -- 客户状态参数
ORDER BY ${sort_column} ${sort_direction} -- 排序参数 

    """

    print(clean_sql(sql))

if __name__ == "__main__":
    main() 