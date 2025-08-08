/*
🎯 Neo4j血缘关系查询 - 保留实际表完整链路
================================================================

核心逻辑：
1. 找到所有路径中的实际表（tempFlag <> 1）
2. 保留实际表之间的顺序关系
3. 跳过所有临时表，但保持实际表的连接关系

示例：a(实际)-临时表-临时表-b(实际)-c(实际)-临时表-d(实际)
结果：a->b, b->c, c->d
*/

// ===================================================================
// 🎯 表维度查询
// ===================================================================

// 查询1：表维度溯源查询（找源头）
// 场景：查找指定表的源头表，保留实际表的完整血缘链路
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 查找所有到达目标表的路径
MATCH (tar:COLUMN_BDP_OLD {dbName: $dbName, tblName: $tblName})
WHERE tar.tempFlag <> 1
MATCH path = (src:COLUMN_BDP_OLD)-[:column_bdp_old_rel*]->(tar)
WHERE src.tempFlag <> 1 AND length(path) >= 1

// 提取路径中的所有实际表（按顺序）
WITH path, [n IN nodes(path) WHERE n.tempFlag <> 1] as realTables
WHERE size(realTables) >= 2

// 构建相邻实际表之间的关系
UNWIND range(0, size(realTables)-2) as i
WITH realTables[i] as src, realTables[i+1] as tar, path
MATCH path2 = (src)-[:column_bdp_old_rel*]->(tar)
WITH src, tar, relationships(path2)[0] as rel

RETURN DISTINCT 
    rel.etlSystem as etlSystem,
    rel.etlJob as etlJob,
    rel.sqlNo as sqlNo,
    rel.appName as appName,
    src.dbName as src_db,
    src.tblName as src_tbl,
    src.colName as src_col,
    tar.dbName as tar_db,
    tar.tblName as tar_tbl,
    tar.colName as tar_col

ORDER BY etlJob, sqlNo, src_db, src_tbl, src_col
LIMIT 1000;

// 查询2：表维度引用查询（找下游）
// 场景：查找指定表的下游表，保留实际表的完整血缘链路
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 查找所有从源表出发的路径
MATCH (src:COLUMN_BDP_OLD {dbName: $dbName, tblName: $tblName})
WHERE src.tempFlag <> 1
MATCH path = (src)-[:column_bdp_old_rel*]->(tar:COLUMN_BDP_OLD)
WHERE tar.tempFlag <> 1 AND length(path) >= 1

// 提取路径中的所有实际表（按顺序）
WITH path, [n IN nodes(path) WHERE n.tempFlag <> 1] as realTables
WHERE size(realTables) >= 2

// 构建相邻实际表之间的关系
UNWIND range(0, size(realTables)-2) as i
WITH realTables[i] as src, realTables[i+1] as tar, path
MATCH path2 = (src)-[:column_bdp_old_rel*]->(tar)
WITH src, tar, relationships(path2)[0] as rel

RETURN DISTINCT 
    rel.etlSystem as etlSystem,
    rel.etlJob as etlJob,
    rel.sqlNo as sqlNo,
    rel.appName as appName,
    src.dbName as src_db,
    src.tblName as src_tbl,
    src.colName as src_col,
    tar.dbName as tar_db,
    tar.tblName as tar_tbl,
    tar.colName as tar_col

ORDER BY etlJob, sqlNo, tar_db, tar_tbl, tar_col
LIMIT 1000;

// ===================================================================
// 🎯 字段维度查询
// ===================================================================

// 查询3：字段维度溯源查询（找源头）
// 场景：查找指定字段的源头字段，保留实际表的完整血缘链路
:param columnIds => ['column_id_1', 'column_id_2', 'column_id_3']
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 查找所有到达目标字段的路径
MATCH (tar:COLUMN_BDP_OLD)
WHERE tar.id IN $columnIds
  AND tar.dbName = $dbName
  AND tar.tblName = $tblName
  AND tar.tempFlag <> 1
MATCH path = (src:COLUMN_BDP_OLD)-[:column_bdp_old_rel*]->(tar)
WHERE src.tempFlag <> 1 AND length(path) >= 1

// 提取路径中的所有实际表节点（按顺序）
WITH path, [n IN nodes(path) WHERE n.tempFlag <> 1] as realTables
WHERE size(realTables) >= 2

// 构建相邻实际表之间的关系
UNWIND range(0, size(realTables)-2) as i
WITH realTables[i] as src, realTables[i+1] as tar, path
MATCH path2 = (src)-[:column_bdp_old_rel*]->(tar)
WITH src, tar, relationships(path2)[0] as rel

RETURN DISTINCT 
    rel.etlSystem as etlSystem,
    rel.etlJob as etlJob,
    rel.sqlNo as sqlNo,
    rel.appName as appName,
    src.dbName as src_db,
    src.tblName as src_tbl,
    src.colName as src_col,
    src.id as src_column_id,
    tar.dbName as tar_db,
    tar.tblName as tar_tbl,
    tar.colName as tar_col,
    tar.id as tar_column_id

ORDER BY tar_column_id, etlJob, sqlNo, src_db, src_tbl, src_col
LIMIT 1000;

// 查询4：字段维度引用查询（找下游）
// 场景：查找指定字段的下游字段，保留实际表的完整血缘链路
:param columnIds => ['column_id_1', 'column_id_2', 'column_id_3']
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 查找所有从源字段出发的路径
MATCH (src:COLUMN_BDP_OLD)
WHERE src.id IN $columnIds
  AND src.dbName = $dbName
  AND src.tblName = $tblName
  AND src.tempFlag <> 1
MATCH path = (src)-[:column_bdp_old_rel*]->(tar:COLUMN_BDP_OLD)
WHERE tar.tempFlag <> 1 AND length(path) >= 1

// 提取路径中的所有实际表节点（按顺序）
WITH path, [n IN nodes(path) WHERE n.tempFlag <> 1] as realTables
WHERE size(realTables) >= 2

// 构建相邻实际表之间的关系
UNWIND range(0, size(realTables)-2) as i
WITH realTables[i] as src, realTables[i+1] as tar, path
MATCH path2 = (src)-[:column_bdp_old_rel*]->(tar)
WITH src, tar, relationships(path2)[0] as rel

RETURN DISTINCT 
    rel.etlSystem as etlSystem,
    rel.etlJob as etlJob,
    rel.sqlNo as sqlNo,
    rel.appName as appName,
    src.dbName as src_db,
    src.tblName as src_tbl,
    src.colName as src_col,
    src.id as src_column_id,
    tar.dbName as tar_db,
    tar.tblName as tar_tbl,
    tar.colName as tar_col,
    tar.id as tar_column_id

ORDER BY src_column_id, etlJob, sqlNo, tar_db, tar_tbl, tar_col
LIMIT 1000;

// ===================================================================
// 🎯 Job维度查询
// ===================================================================

// 查询5：Job维度全量查询（不限制表和字段）
// 场景：查找指定ETL作业的所有血缘关系，保留实际表的完整血缘链路
:param etlSystem => 'your_etl_system'
:param etlJobs => ['job_1', 'job_2', 'job_3']
:param appName => 'your_app_name' // 可选参数

// 查找所有包含指定ETL作业的路径
MATCH path = (src:COLUMN_BDP_OLD)-[:column_bdp_old_rel*]->(tar:COLUMN_BDP_OLD)
WHERE src.tempFlag <> 1 AND tar.tempFlag <> 1 
  AND length(path) >= 1
  AND any(rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs AND ($appName IS NULL OR rel.appName = $appName))

// 提取路径中的所有实际表节点和相关ETL关系（按顺序）
WITH path, 
     [n IN nodes(path) WHERE n.tempFlag <> 1] as realTables,
     [rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs AND ($appName IS NULL OR rel.appName = $appName)][0] as etlRel
WHERE size(realTables) >= 2

// 构建相邻实际表之间的关系
UNWIND range(0, size(realTables)-2) as i
WITH realTables[i] as src, realTables[i+1] as tar, etlRel

RETURN DISTINCT 
    etlRel.etlSystem as etlSystem,
    etlRel.etlJob as etlJob,
    etlRel.sqlNo as sqlNo,
    etlRel.appName as appName,
    src.dbName as src_db,
    src.tblName as src_tbl,
    src.colName as src_col,
    tar.dbName as tar_db,
    tar.tblName as tar_tbl,
    tar.colName as tar_col

ORDER BY etlJob, sqlNo, src_db, src_tbl, src_col
LIMIT 1000;

// 查询6：ETL系统维度查询
// 场景：查找指定ETL系统的所有血缘关系，保留实际表的完整血缘链路
:param etlSystem => 'your_etl_system'

// 查找所有包含指定ETL系统的路径
MATCH path = (src:COLUMN_BDP_OLD)-[:column_bdp_old_rel*]->(tar:COLUMN_BDP_OLD)
WHERE src.tempFlag <> 1 AND tar.tempFlag <> 1 
  AND length(path) >= 1
  AND any(rel IN relationships(path) WHERE rel.etlSystem = $etlSystem)

// 提取路径中的所有实际表节点和相关ETL关系（按顺序）
WITH path, 
     [n IN nodes(path) WHERE n.tempFlag <> 1] as realTables,
     [rel IN relationships(path) WHERE rel.etlSystem = $etlSystem][0] as etlRel
WHERE size(realTables) >= 2

// 构建相邻实际表之间的关系
UNWIND range(0, size(realTables)-2) as i
WITH realTables[i] as src, realTables[i+1] as tar, etlRel

RETURN DISTINCT 
    etlRel.etlSystem as etlSystem,
    etlRel.etlJob as etlJob,
    etlRel.sqlNo as sqlNo,
    etlRel.appName as appName,
    src.dbName as src_db,
    src.tblName as src_tbl,
    src.colName as src_col,
    tar.dbName as tar_db,
    tar.tblName as tar_tbl,
    tar.colName as tar_col

ORDER BY etlJob, sqlNo, src_db, src_tbl, src_col
LIMIT 1000;

/*
🎯 核心算法说明：
================================================================

1. 路径发现：
   - 使用 -[:column_bdp_old_rel*]-> 找到所有可能的路径
   - 确保起点和终点都是实际表（tempFlag <> 1）

2. 实际表提取：
   - [n IN nodes(path) WHERE n.tempFlag <> 1] 提取路径中所有实际表
   - 按照在原路径中的顺序保持排列

3. 相邻关系构建：
   - UNWIND range(0, size(realTables)-2) 遍历相邻实际表对
   - realTables[i] -> realTables[i+1] 构建直接血缘关系

4. ETL信息保留：
   - 使用第一个相关的relationship的ETL信息
   - 确保血缘关系的可追溯性

示例转换：
输入路径：A(实际) -> temp1 -> temp2 -> B(实际) -> C(实际) -> temp3 -> D(实际)
实际表序列：[A, B, C, D]
输出关系：A->B, B->C, C->D

🎯 参数说明：
- dbName: 数据库名称（必填）
- tblName: 表名称（必填）
- columnIds: 字段节点ID列表（字段维度查询必填）
- etlSystem: ETL系统名称（Job维度查询必填）
- etlJobs: ETL作业名称列表（Job维度查询必填）

🎯 性能优化建议：
- 建议索引：dbName, tblName, tempFlag, id, etlSystem, etlJob
- 路径深度：使用 * 无限制深度，确保追踪到真正源头
- 结果限制：LIMIT 1000-2000 避免返回过多数据
- 批量查询时建议分批处理columnIds和etlJobs
- 循环检测：Neo4j自动防止无限循环
*/ 