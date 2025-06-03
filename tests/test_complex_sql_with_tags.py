import sys
import os
sys.path.append('src')
from lineage_sql_with_tags import lineage_analysis

def test_complex_sql():
    """测试复杂SQL案例：包含临时表、多层子查询、嵌套查询"""
    
    # 复杂SQL测试案例
    complex_sql = '''
-- 创建临时表1：销售汇总
CREATE TEMPORARY TABLE temp_sales_summary AS (
    SELECT 
        customer_id,
        product_category,
        SUM(sale_amount) as total_sales,
        COUNT(*) as order_count,
        AVG(sale_amount) as avg_order_value
    FROM sales_records sr
    JOIN product_catalog pc ON sr.product_id = pc.product_id
    WHERE sr.sale_date >= '2023-01-01'
    GROUP BY customer_id, product_category
);

-- 创建临时表2：客户等级
CREATE TABLE temp_customer_levels AS (
    SELECT 
        customer_id,
        CASE 
            WHEN total_purchase > 10000 THEN 'VIP'
            WHEN total_purchase > 5000 THEN 'Gold'
            WHEN total_purchase > 1000 THEN 'Silver'
            ELSE 'Bronze'
        END as customer_level,
        total_purchase
    FROM (
        SELECT 
            customer_id,
            SUM(order_amount) as total_purchase
        FROM customer_orders co
        JOIN order_details od ON co.order_id = od.order_id
        WHERE co.order_status = 'Completed'
        GROUP BY customer_id
    ) customer_totals
);

-- 复杂查询：多层嵌套子查询 + 临时表关联
INSERT INTO customer_analytics (
    customer_id, 
    customer_name, 
    customer_level, 
    primary_category, 
    total_sales, 
    avg_order_value,
    rank_in_level,
    category_penetration_score
)
SELECT 
    final_data.customer_id,
    final_data.customer_name,
    final_data.customer_level,
    final_data.primary_category,
    final_data.total_sales,
    final_data.avg_order_value,
    final_data.rank_in_level,
    final_data.category_penetration_score
FROM (
    -- 外层子查询：计算客户在其等级内的排名
    SELECT 
        ranked_customers.*,
        ROW_NUMBER() OVER (
            PARTITION BY ranked_customers.customer_level 
            ORDER BY ranked_customers.total_sales DESC
        ) as rank_in_level
    FROM (
        -- 中层子查询：计算客户的主要品类和渗透率
        SELECT 
            customer_summary.customer_id,
            customer_summary.customer_name,
            customer_summary.customer_level,
            customer_summary.primary_category,
            customer_summary.total_sales,
            customer_summary.avg_order_value,
            (customer_summary.category_count * 1.0 / total_categories.max_categories) as category_penetration_score
        FROM (
            -- 内层子查询1：客户基础数据汇总
            SELECT 
                c.customer_id,
                c.customer_name,
                tcl.customer_level,
                tss.primary_category,
                SUM(tss.total_sales) as total_sales,
                AVG(tss.avg_order_value) as avg_order_value,
                COUNT(DISTINCT tss.product_category) as category_count
            FROM customers c
            JOIN temp_customer_levels tcl ON c.customer_id = tcl.customer_id
            JOIN (
                -- 嵌套子查询：确定客户的主要购买品类
                SELECT 
                    customer_id,
                    product_category,
                    total_sales,
                    avg_order_value,
                    FIRST_VALUE(product_category) OVER (
                        PARTITION BY customer_id 
                        ORDER BY total_sales DESC
                    ) as primary_category
                FROM temp_sales_summary
            ) tss ON c.customer_id = tss.customer_id
            WHERE c.customer_status = 'Active'
                AND tcl.customer_level IN ('VIP', 'Gold', 'Silver')
            GROUP BY c.customer_id, c.customer_name, tcl.customer_level, tss.primary_category
        ) customer_summary
        CROSS JOIN (
            -- 内层子查询2：计算系统中的总品类数
            SELECT COUNT(DISTINCT category_name) as max_categories
            FROM product_categories
            WHERE category_status = 'Active'
        ) total_categories
        WHERE customer_summary.total_sales > (
            -- 嵌套子查询：动态阈值 - 该等级平均销售额的80%
            SELECT AVG(base_sales.total_sales) * 0.8
            FROM (
                SELECT 
                    tcl2.customer_id,
                    SUM(tss2.total_sales) as total_sales
                FROM temp_customer_levels tcl2
                JOIN temp_sales_summary tss2 ON tcl2.customer_id = tss2.customer_id
                WHERE tcl2.customer_level = customer_summary.customer_level
                GROUP BY tcl2.customer_id
            ) base_sales
        )
    ) ranked_customers
) final_data
WHERE final_data.rank_in_level <= 100  -- 只取每个等级前100名
ORDER BY final_data.customer_level, final_data.rank_in_level;

-- 另一个复杂查询：关联分析
UPDATE customer_segments cs
SET 
    segment_score = segment_calc.new_score,
    last_updated = SYSDATE
FROM (
    SELECT 
        customer_id,
        (behavior_score * 0.4 + purchase_score * 0.6) as new_score
    FROM (
        SELECT 
            ca.customer_id,
            ca.category_penetration_score * 100 as behavior_score,
            CASE 
                WHEN ca.customer_level = 'VIP' THEN 100
                WHEN ca.customer_level = 'Gold' THEN 80
                WHEN ca.customer_level = 'Silver' THEN 60
                ELSE 40
            END as purchase_score
        FROM customer_analytics ca
        WHERE ca.rank_in_level <= 50
    ) score_components
) segment_calc
WHERE cs.customer_id = segment_calc.customer_id;
'''

    print('=== 复杂SQL血缘分析测试（带标记版本）===')
    print('SQL特点：')
    print('1. 包含2个临时表')
    print('2. 多达5层嵌套子查询')
    print('3. 复杂JOIN关联')
    print('4. 窗口函数和聚合函数')
    print('5. 条件子查询')
    print()
    
    result = lineage_analysis(sql=complex_sql, db_type='oracle')
    print('=== 分析结果 ===')
    print(result)

if __name__ == "__main__":
    test_complex_sql() 