-- =============================================================================
-- 血缘关系差异对比SQL - 跨表比较版
-- 专门用于比较两个不同表中的工具结果集差异
-- =============================================================================

-- 使用说明：
-- 1. 将下面的参数替换为实际值：
--    - TOOL_A_TABLE: 工具A数据所在的表名
--    - TOOL_B_TABLE: 工具B数据所在的表名
--    - TARGET_ETL_SYSTEM: 目标ETL系统名称（如果需要过滤）
--    - TARGET_ETL_JOB: 目标ETL作业名称（如果需要过滤）
-- 2. 如果不需要按ETL_SYSTEM或ETL_JOB过滤，可以删除相应的WHERE条件

-- =============================================================================
-- 第一步：基础数据准备和统计
-- =============================================================================

-- 1. 快速统计两个工具的数量差异
SELECT 
    '总体统计' as 类型,
    '工具A' as 工具,
    COUNT(*) as 血缘关系数,
    COUNT(DISTINCT SQL_PATH) as SQL文件数,
    COUNT(DISTINCT SOURCE_TABLE || '->' || TARGET_TABLE) as 表级血缘数
FROM TOOL_A_TABLE  -- 替换为工具A的实际表名
WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
  AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤

UNION ALL

SELECT 
    '总体统计' as 类型,
    '工具B' as 工具,
    COUNT(*) as 血缘关系数,
    COUNT(DISTINCT SQL_PATH) as SQL文件数,
    COUNT(DISTINCT SOURCE_TABLE || '->' || TARGET_TABLE) as 表级血缘数
FROM TOOL_B_TABLE  -- 替换为工具B的实际表名
WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
  AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤

ORDER BY 工具;

-- =============================================================================
-- 第二步：详细差异分析 - 字段级血缘关系
-- =============================================================================

-- 2. 找出字段级血缘关系的具体差异
WITH tool_a_lineages AS (
    SELECT 
        NVL(SOURCE_DATABASE, 'NULL') as SOURCE_DATABASE,
        NVL(SOURCE_TABLE, 'NULL') as SOURCE_TABLE,
        NVL(SOURCE_COLUMN, 'NULL') as SOURCE_COLUMN,
        NVL(TARGET_DATABASE, 'NULL') as TARGET_DATABASE,
        NVL(TARGET_TABLE, 'NULL') as TARGET_TABLE,
        NVL(TARGET_COLUMN, 'NULL') as TARGET_COLUMN,
        SQL_PATH,
        SQL_NO,
        -- 创建唯一标识符
        NVL(SOURCE_DATABASE, 'NULL') || '|' || 
        NVL(SOURCE_TABLE, 'NULL') || '|' || 
        NVL(SOURCE_COLUMN, 'NULL') || '|' ||
        NVL(TARGET_DATABASE, 'NULL') || '|' ||
        NVL(TARGET_TABLE, 'NULL') || '|' ||
        NVL(TARGET_COLUMN, 'NULL') as LINEAGE_KEY
    FROM TOOL_A_TABLE  -- 替换为工具A的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
      AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤
),
tool_b_lineages AS (
    SELECT 
        NVL(SOURCE_DATABASE, 'NULL') as SOURCE_DATABASE,
        NVL(SOURCE_TABLE, 'NULL') as SOURCE_TABLE,
        NVL(SOURCE_COLUMN, 'NULL') as SOURCE_COLUMN,
        NVL(TARGET_DATABASE, 'NULL') as TARGET_DATABASE,
        NVL(TARGET_TABLE, 'NULL') as TARGET_TABLE,
        NVL(TARGET_COLUMN, 'NULL') as TARGET_COLUMN,
        SQL_PATH,
        SQL_NO,
        -- 创建唯一标识符
        NVL(SOURCE_DATABASE, 'NULL') || '|' || 
        NVL(SOURCE_TABLE, 'NULL') || '|' || 
        NVL(SOURCE_COLUMN, 'NULL') || '|' ||
        NVL(TARGET_DATABASE, 'NULL') || '|' ||
        NVL(TARGET_TABLE, 'NULL') || '|' ||
        NVL(TARGET_COLUMN, 'NULL') as LINEAGE_KEY
    FROM TOOL_B_TABLE  -- 替换为工具B的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
      AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤
)
-- 显示所有差异：工具A独有 + 工具B独有
SELECT 
    '🔴 工具A独有' as 差异类型,
    a.SQL_PATH,
    a.SQL_NO,
    a.SOURCE_DATABASE,
    a.SOURCE_TABLE,
    a.SOURCE_COLUMN,
    ' --> ' as 箭头,
    a.TARGET_DATABASE,
    a.TARGET_TABLE,
    a.TARGET_COLUMN,
    a.LINEAGE_KEY as 血缘关系标识
FROM tool_a_lineages a
LEFT JOIN tool_b_lineages b ON a.LINEAGE_KEY = b.LINEAGE_KEY
WHERE b.LINEAGE_KEY IS NULL

UNION ALL

SELECT 
    '🔵 工具B独有' as 差异类型,
    b.SQL_PATH,
    b.SQL_NO,
    b.SOURCE_DATABASE,
    b.SOURCE_TABLE,
    b.SOURCE_COLUMN,
    ' --> ' as 箭头,
    b.TARGET_DATABASE,
    b.TARGET_TABLE,
    b.TARGET_COLUMN,
    b.LINEAGE_KEY as 血缘关系标识
FROM tool_b_lineages b
LEFT JOIN tool_a_lineages a ON b.LINEAGE_KEY = a.LINEAGE_KEY
WHERE a.LINEAGE_KEY IS NULL

ORDER BY 差异类型, SQL_PATH, SQL_NO;

-- =============================================================================
-- 第三步：表级血缘关系差异分析
-- =============================================================================

-- 3. 找出表级血缘关系的差异（如果字段级太细，可以看表级）
WITH tool_a_table_lineages AS (
    SELECT DISTINCT
        NVL(SOURCE_DATABASE, 'NULL') as SOURCE_DATABASE,
        NVL(SOURCE_TABLE, 'NULL') as SOURCE_TABLE,
        NVL(TARGET_DATABASE, 'NULL') as TARGET_DATABASE,
        NVL(TARGET_TABLE, 'NULL') as TARGET_TABLE,
        SQL_PATH,
        -- 创建表级唯一标识符
        NVL(SOURCE_DATABASE, 'NULL') || '|' || 
        NVL(SOURCE_TABLE, 'NULL') || '|' ||
        NVL(TARGET_DATABASE, 'NULL') || '|' ||
        NVL(TARGET_TABLE, 'NULL') as TABLE_LINEAGE_KEY
    FROM TOOL_A_TABLE  -- 替换为工具A的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
      AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤
),
tool_b_table_lineages AS (
    SELECT DISTINCT
        NVL(SOURCE_DATABASE, 'NULL') as SOURCE_DATABASE,
        NVL(SOURCE_TABLE, 'NULL') as SOURCE_TABLE,
        NVL(TARGET_DATABASE, 'NULL') as TARGET_DATABASE,
        NVL(TARGET_TABLE, 'NULL') as TARGET_TABLE,
        SQL_PATH,
        -- 创建表级唯一标识符
        NVL(SOURCE_DATABASE, 'NULL') || '|' || 
        NVL(SOURCE_TABLE, 'NULL') || '|' ||
        NVL(TARGET_DATABASE, 'NULL') || '|' ||
        NVL(TARGET_TABLE, 'NULL') as TABLE_LINEAGE_KEY
    FROM TOOL_B_TABLE  -- 替换为工具B的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
      AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤
)
-- 表级差异
SELECT 
    '🔴 工具A表级独有' as 差异类型,
    a.SQL_PATH,
    a.SOURCE_DATABASE,
    a.SOURCE_TABLE,
    ' --> ' as 箭头,
    a.TARGET_DATABASE,
    a.TARGET_TABLE,
    a.TABLE_LINEAGE_KEY as 表级血缘标识
FROM tool_a_table_lineages a
LEFT JOIN tool_b_table_lineages b ON a.TABLE_LINEAGE_KEY = b.TABLE_LINEAGE_KEY
WHERE b.TABLE_LINEAGE_KEY IS NULL

UNION ALL

SELECT 
    '🔵 工具B表级独有' as 差异类型,
    b.SQL_PATH,
    b.SOURCE_DATABASE,
    b.SOURCE_TABLE,
    ' --> ' as 箭头,
    b.TARGET_DATABASE,
    b.TARGET_TABLE,
    b.TABLE_LINEAGE_KEY as 表级血缘标识
FROM tool_b_table_lineages b
LEFT JOIN tool_a_table_lineages a ON b.TABLE_LINEAGE_KEY = a.TABLE_LINEAGE_KEY
WHERE a.TABLE_LINEAGE_KEY IS NULL

ORDER BY 差异类型, SQL_PATH;

-- =============================================================================
-- 第四步：按SQL文件维度的差异统计
-- =============================================================================

-- 4. 按SQL文件统计差异分布
WITH file_stats AS (
    SELECT 
        SQL_PATH,
        工具A数量,
        工具B数量
    FROM (
        SELECT 
            SQL_PATH,
            COUNT(*) as 工具A数量,
            0 as 工具B数量
        FROM TOOL_A_TABLE  -- 替换为工具A的实际表名
        WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
          AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤
        GROUP BY SQL_PATH
        
        UNION ALL
        
        SELECT 
            SQL_PATH,
            0 as 工具A数量,
            COUNT(*) as 工具B数量
        FROM TOOL_B_TABLE  -- 替换为工具B的实际表名
        WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'  -- 可选：如果需要按ETL系统过滤
          AND ETL_JOB = 'TARGET_ETL_JOB'        -- 可选：如果需要按ETL作业过滤
        GROUP BY SQL_PATH
    )
    GROUP BY SQL_PATH
    HAVING SUM(工具A数量) > 0 OR SUM(工具B数量) > 0
),
file_stats_final AS (
    SELECT 
        SQL_PATH,
        SUM(工具A数量) as 工具A数量,
        SUM(工具B数量) as 工具B数量
    FROM file_stats
    GROUP BY SQL_PATH
)
SELECT 
    SQL_PATH,
    工具A数量,
    工具B数量,
    ABS(工具A数量 - 工具B数量) as 数量差异,
    CASE 
        WHEN 工具A数量 > 工具B数量 THEN '🔴 工具A多' || (工具A数量 - 工具B数量) || '条'
        WHEN 工具B数量 > 工具A数量 THEN '🔵 工具B多' || (工具B数量 - 工具A数量) || '条'
        ELSE '✅ 数量相同'
    END as 差异说明
FROM file_stats_final
WHERE 工具A数量 > 0 OR 工具B数量 > 0  -- 至少有一个工具有数据
ORDER BY ABS(工具A数量 - 工具B数量) DESC, SQL_PATH;

-- =============================================================================
-- 第五步：找出相同表对但字段映射不同的情况
-- =============================================================================

-- 5. 相同表对的字段映射差异分析
WITH table_pairs AS (
    -- 找出两个工具都识别出的表对表血缘关系
    SELECT DISTINCT
        NVL(SOURCE_DATABASE, 'NULL') || '|' || NVL(SOURCE_TABLE, 'NULL') as SOURCE_TABLE_FULL,
        NVL(TARGET_DATABASE, 'NULL') || '|' || NVL(TARGET_TABLE, 'NULL') as TARGET_TABLE_FULL
    FROM TOOL_A_TABLE  -- 替换为工具A的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM' AND ETL_JOB = 'TARGET_ETL_JOB'
    
    INTERSECT
    
    SELECT DISTINCT
        NVL(SOURCE_DATABASE, 'NULL') || '|' || NVL(SOURCE_TABLE, 'NULL') as SOURCE_TABLE_FULL,
        NVL(TARGET_DATABASE, 'NULL') || '|' || NVL(TARGET_TABLE, 'NULL') as TARGET_TABLE_FULL
    FROM TOOL_B_TABLE  -- 替换为工具B的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM' AND ETL_JOB = 'TARGET_ETL_JOB'
),
column_mappings_a AS (
    SELECT 
        tp.SOURCE_TABLE_FULL,
        tp.TARGET_TABLE_FULL,
        'TOOL_A' as 工具,
        COUNT(*) as 字段映射数量,
        LISTAGG(l.SOURCE_COLUMN || '->' || l.TARGET_COLUMN, '; ') 
            WITHIN GROUP (ORDER BY l.SOURCE_COLUMN) as 字段映射详情
    FROM table_pairs tp
    JOIN TOOL_A_TABLE l ON (  -- 替换为工具A的实际表名
        NVL(l.SOURCE_DATABASE, 'NULL') || '|' || NVL(l.SOURCE_TABLE, 'NULL') = tp.SOURCE_TABLE_FULL
        AND NVL(l.TARGET_DATABASE, 'NULL') || '|' || NVL(l.TARGET_TABLE, 'NULL') = tp.TARGET_TABLE_FULL
        AND l.ETL_SYSTEM = 'TARGET_ETL_SYSTEM'
        AND l.ETL_JOB = 'TARGET_ETL_JOB'
    )
    GROUP BY tp.SOURCE_TABLE_FULL, tp.TARGET_TABLE_FULL
),
column_mappings_b AS (
    SELECT 
        tp.SOURCE_TABLE_FULL,
        tp.TARGET_TABLE_FULL,
        'TOOL_B' as 工具,
        COUNT(*) as 字段映射数量,
        LISTAGG(l.SOURCE_COLUMN || '->' || l.TARGET_COLUMN, '; ') 
            WITHIN GROUP (ORDER BY l.SOURCE_COLUMN) as 字段映射详情
    FROM table_pairs tp
    JOIN TOOL_B_TABLE l ON (  -- 替换为工具B的实际表名
        NVL(l.SOURCE_DATABASE, 'NULL') || '|' || NVL(l.SOURCE_TABLE, 'NULL') = tp.SOURCE_TABLE_FULL
        AND NVL(l.TARGET_DATABASE, 'NULL') || '|' || NVL(l.TARGET_TABLE, 'NULL') = tp.TARGET_TABLE_FULL
        AND l.ETL_SYSTEM = 'TARGET_ETL_SYSTEM'
        AND l.ETL_JOB = 'TARGET_ETL_JOB'
    )
    GROUP BY tp.SOURCE_TABLE_FULL, tp.TARGET_TABLE_FULL
)
SELECT 
    a.SOURCE_TABLE_FULL as 源表,
    a.TARGET_TABLE_FULL as 目标表,
    NVL(a.字段映射数量, 0) as 工具A字段数,
    NVL(b.字段映射数量, 0) as 工具B字段数,
    ABS(NVL(a.字段映射数量, 0) - NVL(b.字段映射数量, 0)) as 字段差异数,
    CASE 
        WHEN a.字段映射数量 IS NULL THEN '🔵 仅工具B识别'
        WHEN b.字段映射数量 IS NULL THEN '🔴 仅工具A识别'
        WHEN a.字段映射数量 = b.字段映射数量 THEN '✅ 字段数相同'
        WHEN a.字段映射数量 > b.字段映射数量 THEN '🔴 工具A多识别' || (a.字段映射数量 - b.字段映射数量) || '个字段'
        ELSE '🔵 工具B多识别' || (b.字段映射数量 - a.字段映射数量) || '个字段'
    END as 差异说明,
    '工具A: ' || NVL(a.字段映射详情, '无') as 工具A字段映射,
    '工具B: ' || NVL(b.字段映射详情, '无') as 工具B字段映射
FROM column_mappings_a a
FULL OUTER JOIN column_mappings_b b 
    ON a.SOURCE_TABLE_FULL = b.SOURCE_TABLE_FULL 
    AND a.TARGET_TABLE_FULL = b.TARGET_TABLE_FULL
ORDER BY ABS(NVL(a.字段映射数量, 0) - NVL(b.字段映射数量, 0)) DESC;

-- =============================================================================
-- 第六步：汇总报告 - 一目了然的差异概况
-- =============================================================================

-- 6. 最终汇总报告
WITH summary_stats_a AS (
    SELECT 
        'TOOL_A' as 工具标识,
        COUNT(*) as 总血缘数,
        COUNT(DISTINCT SQL_PATH) as SQL文件数,
        COUNT(DISTINCT NVL(SOURCE_TABLE, 'NULL') || '->' || NVL(TARGET_TABLE, 'NULL')) as 表级血缘数,
        COUNT(DISTINCT NVL(SOURCE_DATABASE, 'NULL')) as 涉及源库数,
        COUNT(DISTINCT NVL(TARGET_DATABASE, 'NULL')) as 涉及目标库数
    FROM TOOL_A_TABLE  -- 替换为工具A的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'
      AND ETL_JOB = 'TARGET_ETL_JOB'
),
summary_stats_b AS (
    SELECT 
        'TOOL_B' as 工具标识,
        COUNT(*) as 总血缘数,
        COUNT(DISTINCT SQL_PATH) as SQL文件数,
        COUNT(DISTINCT NVL(SOURCE_TABLE, 'NULL') || '->' || NVL(TARGET_TABLE, 'NULL')) as 表级血缘数,
        COUNT(DISTINCT NVL(SOURCE_DATABASE, 'NULL')) as 涉及源库数,
        COUNT(DISTINCT NVL(TARGET_DATABASE, 'NULL')) as 涉及目标库数
    FROM TOOL_B_TABLE  -- 替换为工具B的实际表名
    WHERE ETL_SYSTEM = 'TARGET_ETL_SYSTEM'
      AND ETL_JOB = 'TARGET_ETL_JOB'
),
difference_stats AS (
    SELECT 
        NVL(a.总血缘数, 0) as 工具A总数,
        NVL(b.总血缘数, 0) as 工具B总数,
        NVL(a.表级血缘数, 0) as 工具A表级数,
        NVL(b.表级血缘数, 0) as 工具B表级数
    FROM summary_stats_a a
    FULL OUTER JOIN summary_stats_b b ON 1=1
)
SELECT 
    '📊 汇总报告' as 报告项目,
    '工具A(TOOL_A_TABLE)' as 工具A,
    '工具B(TOOL_B_TABLE)' as 工具B,
    '差异' as 差异情况
FROM dual

UNION ALL

SELECT 
    '🔢 字段级血缘总数',
    TO_CHAR(工具A总数) || '条',
    TO_CHAR(工具B总数) || '条',
    CASE 
        WHEN 工具A总数 = 工具B总数 THEN '✅ 相同'
        WHEN 工具A总数 > 工具B总数 THEN '🔴 A多' || (工具A总数 - 工具B总数) || '条'
        ELSE '🔵 B多' || (工具B总数 - 工具A总数) || '条'
    END
FROM difference_stats

UNION ALL

SELECT 
    '🏠 表级血缘总数',
    TO_CHAR(工具A表级数) || '条',
    TO_CHAR(工具B表级数) || '条',
    CASE 
        WHEN 工具A表级数 = 工具B表级数 THEN '✅ 相同'
        WHEN 工具A表级数 > 工具B表级数 THEN '🔴 A多' || (工具A表级数 - 工具B表级数) || '条'
        ELSE '🔵 B多' || (工具B表级数 - 工具A表级数) || '条'
    END
FROM difference_stats;

-- =============================================================================
-- 使用指南：
-- 1. 将所有 'TOOL_A_TABLE' 替换为工具A数据所在的实际表名
-- 2. 将所有 'TOOL_B_TABLE' 替换为工具B数据所在的实际表名
-- 3. 将所有 'TARGET_ETL_SYSTEM' 替换为要比较的ETL系统名称（可选过滤条件）
-- 4. 将所有 'TARGET_ETL_JOB' 替换为要比较的ETL作业名称（可选过滤条件）
-- 5. 如果不需要按ETL_SYSTEM或ETL_JOB过滤，可以删除相应的WHERE条件
-- 6. 按顺序执行这些查询，从概况到详情逐步分析
-- 
-- 示例：
-- TOOL_A_TABLE → LINEAGE_TABLE_TOOL1
-- TOOL_B_TABLE → LINEAGE_TABLE_TOOL2
-- TARGET_ETL_SYSTEM → 'my_etl_system' (或删除此过滤条件)
-- TARGET_ETL_JOB → 'my_etl_job' (或删除此过滤条件)
-- =============================================================================
