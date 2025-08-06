// ===================================================================
// Neo4j血缘关系查询 - 过滤临时表版：查询所有血缘但过滤掉临时表节点
// 核心逻辑：A->B(临时表)->C 返回 A->C，过滤掉中间的临时表B
// 结果：完整的业务级血缘关系，包含直接关系和跨越临时表的关系
// ===================================================================

// 🎯 表维度查询
// ===================================================================

// 查询1：表维度溯源查询（找源头）
// 场景：查找指定表的源头表，过滤掉中间临时表节点
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 直接关系（没有中间表）
MATCH (src:COLUMN_BDP_OLD)-[rel:column_bdp_old_rel]->(tar:COLUMN_BDP_OLD)
WHERE tar.dbName = $dbName 
  AND tar.tblName = $tblName
  AND src.tempFlag <> 1 
  AND tar.tempFlag <> 1
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

UNION

// 跨越临时表的关系（A->B(临时表)->C 返回 A->C）
MATCH (tar:COLUMN_BDP_OLD {dbName: $dbName, tblName: $tblName})
WHERE tar.tempFlag <> 1
MATCH path = (src:COLUMN_BDP_OLD)-[:column_bdp_old_rel*2..5]->(tar)
WHERE src.tempFlag <> 1
  AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar)
WITH DISTINCT src, tar
MATCH path = (src)-[:column_bdp_old_rel*1..5]->(tar)
WITH src, tar, relationships(path)[0] as rel
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
// 场景：查找指定表的下游表，过滤掉中间临时表节点
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 直接关系（没有中间表）
MATCH (src:COLUMN_BDP_OLD)-[rel:column_bdp_old_rel]->(tar:COLUMN_BDP_OLD)
WHERE src.dbName = $dbName 
  AND src.tblName = $tblName
  AND src.tempFlag <> 1 
  AND tar.tempFlag <> 1
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

UNION

// 跨越临时表的关系（A->B(临时表)->C 返回 A->C）
MATCH (src:COLUMN_BDP_OLD {dbName: $dbName, tblName: $tblName})
WHERE src.tempFlag <> 1
MATCH path = (src)-[:column_bdp_old_rel*2..5]->(tar:COLUMN_BDP_OLD)
WHERE tar.tempFlag <> 1
  AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar)
WITH DISTINCT src, tar
MATCH path = (src)-[:column_bdp_old_rel*1..5]->(tar)
WITH src, tar, relationships(path)[0] as rel
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

// 🎯 字段维度查询  
// ===================================================================

// 查询3：字段维度溯源查询（找源头）
// 场景：查找指定字段的源头字段，过滤掉中间临时表节点
:param columnIds => ['column_id_1', 'column_id_2', 'column_id_3']
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 直接关系（没有中间表）
MATCH (src:COLUMN_BDP_OLD)-[rel:column_bdp_old_rel]->(tar:COLUMN_BDP_OLD)
WHERE tar.id IN $columnIds
  AND tar.dbName = $dbName
  AND tar.tblName = $tblName
  AND src.tempFlag <> 1 
  AND tar.tempFlag <> 1
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

UNION

// 跨越临时表的关系（A->B(临时表)->C 返回 A->C）
MATCH (tar:COLUMN_BDP_OLD)
WHERE tar.id IN $columnIds
  AND tar.dbName = $dbName
  AND tar.tblName = $tblName
  AND tar.tempFlag <> 1
MATCH path = (src:COLUMN_BDP_OLD)-[:column_bdp_old_rel*2..5]->(tar)
WHERE src.tempFlag <> 1
  AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar)
WITH DISTINCT src, tar
MATCH path = (src)-[:column_bdp_old_rel*1..5]->(tar)
WITH src, tar, relationships(path)[0] as rel
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
// 场景：查找指定字段的下游字段，过滤掉中间临时表节点
:param columnIds => ['column_id_1', 'column_id_2', 'column_id_3']
:param dbName => 'your_database_name'
:param tblName => 'your_table_name'

// 直接关系（没有中间表）
MATCH (src:COLUMN_BDP_OLD)-[rel:column_bdp_old_rel]->(tar:COLUMN_BDP_OLD)
WHERE src.id IN $columnIds
  AND src.dbName = $dbName
  AND src.tblName = $tblName
  AND src.tempFlag <> 1 
  AND tar.tempFlag <> 1
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

UNION

// 跨越临时表的关系（A->B(临时表)->C 返回 A->C）
MATCH (src:COLUMN_BDP_OLD)
WHERE src.id IN $columnIds
  AND src.dbName = $dbName
  AND src.tblName = $tblName
  AND src.tempFlag <> 1
MATCH path = (src)-[:column_bdp_old_rel*2..5]->(tar:COLUMN_BDP_OLD)
WHERE tar.tempFlag <> 1
  AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar)
WITH DISTINCT src, tar
MATCH path = (src)-[:column_bdp_old_rel*1..5]->(tar)
WITH src, tar, relationships(path)[0] as rel
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

// 🎯 Job维度查询
// ===================================================================

// 查询5：Job维度全量查询（不限制表和字段）
// 场景：查找指定ETL作业的所有血缘关系，过滤掉中间临时表节点
:param etlSystem => 'your_etl_system'
:param etlJobs => ['job_1', 'job_2', 'job_3']
:param appName => 'your_app_name' // 可选参数

// 直接关系（没有中间表）  
MATCH (src:COLUMN_BDP_OLD)-[rel:column_bdp_old_rel]->(tar:COLUMN_BDP_OLD)
WHERE rel.etlSystem = $etlSystem 
  AND rel.etlJob IN $etlJobs
  AND ($appName IS NULL OR rel.appName = $appName)
  AND src.tempFlag <> 1 
  AND tar.tempFlag <> 1
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

UNION

// 跨越临时表的关系（ETL作业相关）
MATCH path = (src:COLUMN_BDP_OLD)-[:column_bdp_old_rel*2..5]->(tar:COLUMN_BDP_OLD)
WHERE src.tempFlag <> 1 AND tar.tempFlag <> 1
  AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar)
  AND any(rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs AND ($appName IS NULL OR rel.appName = $appName))
WITH DISTINCT src, tar
MATCH path = (src)-[:column_bdp_old_rel*1..5]->(tar)
WHERE any(rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs AND ($appName IS NULL OR rel.appName = $appName))
WITH src, tar, [rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs AND ($appName IS NULL OR rel.appName = $appName)][0] as firstRel
RETURN DISTINCT 
    firstRel.etlSystem as etlSystem,
    firstRel.etlJob as etlJob,
    firstRel.sqlNo as sqlNo,
    firstRel.appName as appName,
    src.dbName as src_db,
    src.tblName as src_tbl,
    src.colName as src_col,
    tar.dbName as tar_db,
    tar.tblName as tar_tbl,
    tar.colName as tar_col

ORDER BY etlJob, sqlNo, src_db, src_tbl, src_col
LIMIT 1000;



// ===================================================================
// 🎯 使用说明
// ===================================================================

/*
🎯 查询功能说明：

📋 表维度查询（查询1-2）：
- 查询1：表溯源 - 找到指定表的来源表，包含直接关系和跨越临时表的关系
- 查询2：表引用 - 找到指定表的下游表，包含直接关系和跨越临时表的关系
- 参数：dbName, tblName

📋 字段维度查询（查询3-4）：
- 查询3：字段溯源 - 找到指定字段的来源字段，包含直接关系和跨越临时表的关系
- 查询4：字段引用 - 找到指定字段的下游字段，包含直接关系和跨越临时表的关系
- 参数：columnIds（支持多个）, dbName, tblName

📋 Job维度查询（查询5）：
- 查询5：Job全量查询 - 指定作业的所有血缘关系，包含直接关系和跨越临时表的关系
- 参数：etlSystem, etlJobs, appName（可选）

🎯 核心特性：

1. 完整血缘：包含直接关系和跨越临时表的关系
2. 过滤临时表：A->B(临时表)->C 返回 A->C，隐藏中间临时表节点
3. 业务视角：显示端到端的逻辑血缘关系
4. 去重处理：使用UNION DISTINCT确保结果唯一性
5. 支持批量：columnIds 和 etlJobs 都支持多个值

🎯 查询逻辑：

每个查询都分为两部分：
1. 直接关系：MATCH (src)-[rel]->(tar) 查找一跳的直接关系
2. 跨越关系：MATCH path = (src)-[:rel*2..5]->(tar) 查找多跳的跨越关系
   - 确保路径中存在临时表节点：any(n IN nodes(path) WHERE n.tempFlag = 1)
   - 过滤掉临时表的起点和终点：src.tempFlag <> 1 AND tar.tempFlag <> 1
   - 使用UNION合并两种关系，DISTINCT去重

🎯 使用场景：

- 业务血缘分析：需要完整的端到端血缘关系
- 影响性分析：了解数据变更的完整影响范围
- 溯源分析：追踪数据的完整来源路径
- ETL监控：监控作业的完整数据流向

🎯 参数说明：
- dbName: 数据库名称（必填）
- tblName: 表名称（必填）
- columnIds: 字段节点ID列表（字段维度查询必填）
- etlSystem: ETL系统名称（Job维度查询必填）
- etlJobs: ETL作业名称列表（Job维度查询必填）

🎯 性能优化建议：
- 建议索引：dbName, tblName, tempFlag, id, etlSystem, etlJob
- 路径深度限制：*2..5 避免过深的路径查询
- 结果限制：LIMIT 1000 避免返回过多数据
- 批量查询时建议分批处理columnIds和etlJobs
*/ 