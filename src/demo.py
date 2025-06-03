#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL血缘关系分析器演示示例

本文件演示了如何使用SQL血缘关系分析工具的各种功能
"""

from lineage_sql_functional import lineage_analysis


# def demo_sql_string_analysis():
#     """演示SQL字符串分析功能"""
#     print("=== 演示1: SQL字符串分析 ===")
    
#     # 示例SQL脚本
#     sql_content = """
#     -- 数据清洗和转换示例
#     INSERT INTO dwh.dim_customer
#     SELECT 
#         c.customer_id,
#         c.customer_name,
#         c.email,
#         UPPER(c.status) as customer_status,
#         CASE 
#             WHEN c.registration_date >= '2023-01-01' THEN 'NEW'
#             ELSE 'EXISTING'
#         END as customer_type,
#         r.region_name,
#         r.region_code
#     FROM ods.customer_info c
#     LEFT JOIN ods.region_mapping r ON c.region_id = r.region_id
#     WHERE c.is_active = 1
#       AND c.customer_type NOT IN ('TEST', 'INVALID');
#     """
    
#     try:
#         result = lineage_analysis(sql=sql_content, db_type='oracle')
#         print("血缘关系分析结果:")
#         print(result)
#         print("\n" + "="*50 + "\n")
#     except Exception as e:
#         print(f"分析失败: {e}")


def demo_complex_sql_analysis():
    """演示复杂SQL分析功能（包含CTE和临时表）"""
    print("=== 演示2: 复杂SQL分析（CTE + 临时表） ===")
    
    complex_sql = """
    -- 创建临时表进行数据预处理
    CREATE TEMPORARY TABLE temp_sales_summary AS
    SELECT 
        customer_id,
        product_category,
        SUM(sales_amount) as total_sales,
        COUNT(*) as order_count
    FROM raw_sales_data
    WHERE order_date >= '2023-01-01'
    GROUP BY customer_id, product_category;
    
    -- 使用CTE进行复杂的数据处理
    WITH customer_metrics AS (
        SELECT 
            customer_id,
            SUM(total_sales) as annual_sales,
            SUM(order_count) as total_orders,
            COUNT(DISTINCT product_category) as category_count
        FROM temp_sales_summary
        GROUP BY customer_id
    ),
    customer_segments AS (
        SELECT 
            customer_id,
            annual_sales,
            total_orders,
            category_count,
            CASE 
                WHEN annual_sales >= 10000 THEN 'VIP'
                WHEN annual_sales >= 5000 THEN 'PREMIUM'
                ELSE 'REGULAR'
            END as customer_segment
        FROM customer_metrics
    )
    INSERT INTO mart.customer_analysis
    SELECT 
        cs.customer_id,
        c.customer_name,
        c.email,
        cs.annual_sales,
        cs.total_orders,
        cs.category_count,
        cs.customer_segment,
        CURRENT_DATE as analysis_date
    FROM customer_segments cs
    JOIN dim.customer_master c ON cs.customer_id = c.customer_id;
    
    -- 清理临时表
    DROP TABLE temp_sales_summary;
    """
    
    try:
        result = lineage_analysis(sql=complex_sql, db_type='sparksql')
        print("复杂SQL血缘关系分析结果:")
        print(result)
        print("\n" + "="*50 + "\n")
    except Exception as e:
        print(f"分析失败: {e}")


# def demo_subquery_alias_handling():
#     """演示子查询别名处理功能（重点修复）"""
#     print("=== 演示3: 子查询别名智能处理（重点功能） ===")
    
#     subquery_sql = """
#     INSERT INTO 
#     SELECT 
#         u.user_id,
#         u.user_name,
#         a.total_orders,
#         b.avg_amount
#     FROM user u
#     LEFT JOIN (
#         SELECT user_id, COUNT(*) as total_orders 
#         FROM orders 
#         GROUP BY user_id
#     ) as a ON u.user_id = a.user_id
#     LEFT JOIN (
#         SELECT user_id, AVG(amount) as avg_amount 
#         FROM payments 
#         GROUP BY user_id
#     ) as b ON u.user_id = b.user_id;
#     """
    
#     print("SQL语句包含子查询别名 'a' 和 'b'")
#     print("期望结果：提取真实血缘关系 (user, orders, payments -> final_result)")
#     print("期望结果：过滤子查询别名 (a, b 不应出现在血缘关系中)")
#     print()
    
#     try:
#         result = lineage_analysis(sql=subquery_sql, db_type='oracle')
#         print("子查询处理结果:")
#         print(result)
        
#         # 验证结果
#         if ('user' in result and 'orders' in result and 'payments' in result and 
#             'final_result' in result):
#             if "', 'a'," not in result and "', 'b'," not in result:
#                 print("\n✅ 成功！正确提取了真实血缘关系，过滤了子查询别名")
#             else:
#                 print("\n❌ 仍包含子查询别名")
#         else:
#             print("\n❌ 血缘关系提取不完整")
            
#         print("\n" + "="*50 + "\n")
#     except Exception as e:
#         print(f"分析失败: {e}")


# def demo_ddl_filtering():
#     """演示DDL语句过滤功能"""
#     print("=== 演示4: DDL语句智能过滤 ===")
    
#     mixed_sql = """
#     -- 这些语句会被自动跳过
#     USE data_warehouse;
    
#     CREATE INDEX idx_customer_email ON customer_info(email);
    
#     ALTER TABLE customer_info ADD COLUMN created_by VARCHAR(50);
    
#     -- 这个语句会被正常处理
#     INSERT INTO processed_data
#     SELECT 
#         customer_id,
#         customer_name,
#         processed_date
#     FROM raw_customer_data
#     WHERE status = 'ACTIVE';
    
#     -- 这些也会被跳过
#     GRANT SELECT ON processed_data TO analyst_role;
    
#     DROP INDEX idx_old_customer;
#     """
    
#     try:
#         result = lineage_analysis(sql=mixed_sql, db_type='mysql')
#         print("混合SQL脚本分析结果（DDL语句已被过滤）:")
#         print(result)
#         print("\n" + "="*50 + "\n")
#     except Exception as e:
#         print(f"分析失败: {e}")


# def demo_file_analysis():
#     """演示文件分析功能"""
#     print("=== 演示5: 文件路径分析说明 ===")
    
#     print("文件分析功能支持以下方式:")
#     print("1. 单个SQL文件分析:")
#     print("   result = lineage_analysis(file='path/to/script.sql')")
#     print()
#     print("2. 目录批量分析:")
#     print("   result = lineage_analysis(file='path/to/sql/directory')")
#     print()
#     print("3. 递归处理所有SQL文件:")
#     print("   - 自动查找.sql和.hql文件")
#     print("   - 提取ETL系统名（文件夹名）")
#     print("   - 提取ETL作业名（文件名）")
#     print("   - 记录完整文件路径")
#     print()
#     print("注意: 本演示环境中暂不包含实际文件，请根据实际情况调用")
#     print("\n" + "="*50 + "\n")


def main():

    
    # 运行各个演示
    # demo_sql_string_analysis()
    demo_complex_sql_analysis()
    # demo_subquery_alias_handling()  # 重点演示新功能
    # demo_ddl_filtering()
    # demo_file_analysis()



if __name__ == "__main__":
    main() 