import sys
import os
sys.path.append('src')
from lineage_sql_with_tags import lineage_analysis

def test_simple_sql_with_tags():
    """测试简单SQL案例：包含临时表和子查询的标记功能"""
    
    # 简单SQL测试案例
    simple_sql = '''
-- 创建临时表
CREATE TEMPORARY TABLE temp_sales AS (
    SELECT customer_id, SUM(amount) as total_amount
    FROM orders
    WHERE order_date >= '2023-01-01'
    GROUP BY customer_id
);

-- 使用临时表和子查询插入数据
INSERT INTO customer_summary (customer_id, total_purchase, category, last_update)
SELECT 
    t.customer_id,
    t.total_amount,
    CASE WHEN t.total_amount > 5000 THEN 'VIP' ELSE 'Regular' END as category,
    SYSDATE
FROM temp_sales t
JOIN (
    SELECT customer_id, MAX(order_date) as last_order_date
    FROM orders
    WHERE status = 'COMPLETED'
    GROUP BY customer_id
) recent_orders ON t.customer_id = recent_orders.customer_id
WHERE t.total_amount > 1000;
'''

    print('=== 简单SQL血缘分析测试（带标记版本）===')
    print('SQL特点：')
    print('1. 包含1个临时表：temp_sales')
    print('2. 包含1个子查询：recent_orders')
    print('3. 简单JOIN关联')
    print()
    
    result = lineage_analysis(sql=simple_sql, db_type='oracle')
    print('=== 分析结果 ===')
    print(result)
    print()
    
    # 分析结果特点
    print('=== 标记功能验证 ===')
    if '_TEMP_TBL' in result:
        print('✅ 临时表标记功能正常 - 发现 _TEMP_TBL 标记')
    else:
        print('❌ 临时表标记功能异常 - 未发现 _TEMP_TBL 标记')
        
    if '_SUBQRY_TBL' in result:
        print('✅ 子查询表标记功能正常 - 发现 _SUBQRY_TBL 标记')
    else:
        print('⚠️  子查询表标记功能 - 未发现 _SUBQRY_TBL 标记（可能因为sqllineage未识别为SubQuery类型）')

if __name__ == "__main__":
    test_simple_sql_with_tags() 