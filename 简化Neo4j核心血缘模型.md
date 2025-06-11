# 简化Neo4j核心血缘模型设计

## 一、简化节点设计（只保留Oracle表字段）

### 🔍 **ColumnNode节点**（唯一标签）

```cypher
CREATE (col:ColumnNode {
    // Oracle表中的核心字段
    nodeId: "APP_A.DB1.CUSTOMER.CUSTOMER_ID",    -- 全局唯一标识
    appName: "应用A",                             -- APP_NAME
    etlSystem: "客户数据ETL",                     -- ETL_SYSTEM  
    etlJob: "customer_sync.sql",                 -- ETL_JOB
    database: "DB1",                             -- SOURCE_DB/TARGET_DB
    tableName: "CUSTOMER",                       -- SOURCE_TABLE/TARGET_TABLE
    columnName: "CUSTOMER_ID",                   -- SOURCE_COLUMN/TARGET_COLUMN
    scriptPath: "/etl/customer_sync.sql",        -- SCRIPT_PATH (如果Oracle表有)
    
    // Java程序处理添加的标记字段
    tableType: "NORMAL",                         -- NORMAL/TEMP/SUBQUERY
    isVisible: true,                             -- 是否显示
    
    // 基础时间字段
    createTime: datetime(),
    updateTime: datetime()
})
```

### 📊 **LINEAGE_FLOW关系**（只保留Oracle表字段）

```cypher
CREATE (source:ColumnNode)-[:LINEAGE_FLOW {
    // Oracle表中的核心字段
    sourceApp: "应用A",                          -- SOURCE_APP_NAME (如果支持跨应用)
    targetApp: "应用B",                          -- TARGET_APP_NAME
    etlSystem: "数据集成ETL",                    -- ETL_SYSTEM
    etlJob: "customer_sync.sql",                 -- ETL_JOB
    sqlNo: 1,                                    -- SQL_NO
    relationshipType: "DIRECT",                  -- RELATIONSHIP_TYPE
    sqlExpression: "customer_id as cust_id",     -- SQL_EXPRESSION
    scriptPath: "/etl/customer_sync.sql",        -- SCRIPT_PATH
    
    // 时间字段
    createTime: datetime()
}]->(target:ColumnNode)
```

## 二、上下游血缘查询语句

### 🔍 **查询上游血缘关系**

```cypher
// 查询指定字段的上游血缘关系（溯源）
MATCH path = (target:ColumnNode)-[:LINEAGE_FLOW*1..5]->(source:ColumnNode)
WHERE target.appName = $appName
  AND target.database = $database
  AND target.tableName = $tableName
  AND target.columnName = $columnName
  AND target.isVisible = true
  // 过滤路径中的临时表和子查询表
  AND ALL(node IN nodes(path) WHERE node.isVisible = true)
RETURN path,
       // 返回路径信息
       length(path) as pathLength,
       [node IN nodes(path) | {
         nodeId: node.nodeId,
         appName: node.appName,
         database: node.database,
         tableName: node.tableName,
         columnName: node.columnName,
         tableType: node.tableType
       }] as pathNodes,
       [rel IN relationships(path) | {
         etlJob: rel.etlJob,
         sqlNo: rel.sqlNo,
         relationshipType: rel.relationshipType,
         sqlExpression: rel.sqlExpression
       }] as pathRelationships
ORDER BY length(path), target.nodeId
```

### 🔍 **查询下游血缘关系**

```cypher
// 查询指定字段的下游血缘关系（被引用）
MATCH path = (source:ColumnNode)-[:LINEAGE_FLOW*1..5]->(target:ColumnNode)
WHERE source.appName = $appName
  AND source.database = $database
  AND source.tableName = $tableName
  AND source.columnName = $columnName
  AND source.isVisible = true
  // 过滤路径中的临时表和子查询表
  AND ALL(node IN nodes(path) WHERE node.isVisible = true)
RETURN path,
       length(path) as pathLength,
       [node IN nodes(path) | {
         nodeId: node.nodeId,
         appName: node.appName,
         database: node.database,
         tableName: node.tableName,
         columnName: node.columnName,
         tableType: node.tableType
       }] as pathNodes,
       [rel IN relationships(path) | {
         etlJob: rel.etlJob,
         sqlNo: rel.sqlNo,
         relationshipType: rel.relationshipType,
         sqlExpression: rel.sqlExpression
       }] as pathRelationships
ORDER BY length(path), source.nodeId
```

### 🔍 **查询完整血缘关系（上游+下游）**

```cypher
// 查询指定字段的完整血缘关系
MATCH (center:ColumnNode)
WHERE center.appName = $appName
  AND center.database = $database
  AND center.tableName = $tableName
  AND center.columnName = $columnName
  AND center.isVisible = true

// 查询上游
OPTIONAL MATCH upstreamPath = (center)-[:LINEAGE_FLOW*1..3]->(upstream:ColumnNode)
WHERE ALL(node IN nodes(upstreamPath) WHERE node.isVisible = true)

// 查询下游  
OPTIONAL MATCH downstreamPath = (center)-[:LINEAGE_FLOW*1..3]->(downstream:ColumnNode)
WHERE ALL(node IN nodes(downstreamPath) WHERE node.isVisible = true)

RETURN center,
       COLLECT(DISTINCT upstreamPath) as upstreamPaths,
       COLLECT(DISTINCT downstreamPath) as downstreamPaths
```

### 🔍 **支持临时表/子查询显示的查询**

```cypher
// 可控制显示临时表和子查询的查询
MATCH path = (target:ColumnNode)-[:LINEAGE_FLOW*1..5]->(source:ColumnNode)
WHERE target.appName = $appName
  AND target.database = $database
  AND target.tableName = $tableName
  AND target.columnName = $columnName
  // 根据参数决定是否显示临时表和子查询
  AND (CASE 
    WHEN $showTemporary = true AND $showSubquery = true THEN true
    WHEN $showTemporary = true AND $showSubquery = false THEN 
      ALL(node IN nodes(path) WHERE node.tableType <> 'SUBQUERY')
    WHEN $showTemporary = false AND $showSubquery = true THEN 
      ALL(node IN nodes(path) WHERE node.tableType <> 'TEMP')
    ELSE 
      ALL(node IN nodes(path) WHERE node.tableType = 'NORMAL')
    END)
RETURN path
ORDER BY length(path)
```

## 三、Java处理临时表和子查询逻辑

### 🔧 **简化的Java处理逻辑**

```java
@Service
public class CoreLineageImportService {
    
    @Autowired
    private Driver neo4jDriver;
    
    @Autowired
    private JdbcTemplate oracleJdbcTemplate;
    
    /**
     * 导入字段节点（简化版）
     */
    public void importColumnNodes() {
        log.info("开始导入字段节点");
        
        String sql = """
            SELECT DISTINCT
                APP_NAME, ETL_SYSTEM, ETL_JOB, 
                SOURCE_DB, SOURCE_TABLE, SOURCE_COLUMN,
                TARGET_DB, TARGET_TABLE, TARGET_COLUMN,
                SCRIPT_PATH
            FROM LINEAGE_RELATIONSHIP 
            WHERE IS_ACTIVE = 'Y'
            """;
            
        List<Map<String, Object>> records = oracleJdbcTemplate.queryForList(sql);
        
        Set<ColumnNodeInfo> allColumns = new HashSet<>();
        
        for (Map<String, Object> record : records) {
            // 处理源字段
            ColumnNodeInfo sourceColumn = createColumnInfo(record, "SOURCE");
            allColumns.add(sourceColumn);
            
            // 处理目标字段
            ColumnNodeInfo targetColumn = createColumnInfo(record, "TARGET");
            allColumns.add(targetColumn);
        }
        
        // 批量创建节点
        createColumnNodesBatch(allColumns);
        
        log.info("字段节点导入完成，共处理{}个字段", allColumns.size());
    }
    
    /**
     * 🎯 核心方法：创建字段信息，判断临时表和子查询
     */
    private ColumnNodeInfo createColumnInfo(Map<String, Object> record, String type) {
        String appName = (String) record.get("APP_NAME");
        String etlSystem = (String) record.get("ETL_SYSTEM");
        String etlJob = (String) record.get("ETL_JOB");
        String scriptPath = (String) record.get("SCRIPT_PATH");
        
        String database = (String) record.get(type + "_DB");
        String tableName = (String) record.get(type + "_TABLE");
        String columnName = (String) record.get(type + "_COLUMN");
        
        ColumnNodeInfo columnInfo = new ColumnNodeInfo();
        
        // 设置Oracle表中的字段
        columnInfo.setNodeId(String.format("%s.%s.%s.%s", appName, database, tableName, columnName));
        columnInfo.setAppName(appName);
        columnInfo.setEtlSystem(etlSystem);
        columnInfo.setEtlJob(etlJob);
        columnInfo.setScriptPath(scriptPath);
        columnInfo.setDatabase(database);
        columnInfo.setTableName(tableName);
        columnInfo.setColumnName(columnName);
        
        // 🎯 关键：根据表名判断类型
        String tableType = determineTableType(tableName);
        columnInfo.setTableType(tableType);
        columnInfo.setIsVisible(shouldBeVisible(tableType));
        
        return columnInfo;
    }
    
    /**
     * 🔍 根据表名判断表类型（按您的要求：_temp_tab 和 subquery_tab）
     */
    private String determineTableType(String tableName) {
        if (tableName == null) {
            return "NORMAL";
        }
        
        // 判断临时表：以 _temp_tab 结尾
        if (tableName.endsWith("_temp_tab")) {
            return "TEMP";
        }
        
        // 判断子查询表：以 subquery_tab 结尾
        if (tableName.endsWith("subquery_tab")) {
            return "SUBQUERY";
        }
        
        // 默认为正常表
        return "NORMAL";
    }
    
    /**
     * 🎯 确定是否显示（临时表和子查询默认隐藏）
     */
    private boolean shouldBeVisible(String tableType) {
        return "NORMAL".equals(tableType);
    }
    
    /**
     * 批量创建字段节点（简化版）
     */
    private void createColumnNodesBatch(Set<ColumnNodeInfo> columns) {
        List<Map<String, Object>> nodeParams = new ArrayList<>();
        
        for (ColumnNodeInfo column : columns) {
            Map<String, Object> params = new HashMap<>();
            // 只保留核心字段
            params.put("nodeId", column.getNodeId());
            params.put("appName", column.getAppName());
            params.put("etlSystem", column.getEtlSystem());
            params.put("etlJob", column.getEtlJob());
            params.put("scriptPath", column.getScriptPath());
            params.put("database", column.getDatabase());
            params.put("tableName", column.getTableName());
            params.put("columnName", column.getColumnName());
            params.put("tableType", column.getTableType());
            params.put("isVisible", column.getIsVisible());
            
            nodeParams.add(params);
        }
        
        // 简化的批量导入
        String cypher = """
            UNWIND $nodes AS node
            MERGE (col:ColumnNode {nodeId: node.nodeId})
            ON CREATE SET 
                col.appName = node.appName,
                col.etlSystem = node.etlSystem,
                col.etlJob = node.etlJob,
                col.scriptPath = node.scriptPath,
                col.database = node.database,
                col.tableName = node.tableName,
                col.columnName = node.columnName,
                col.tableType = node.tableType,
                col.isVisible = node.isVisible,
                col.createTime = datetime()
            ON MATCH SET 
                col.updateTime = datetime()
            """;
            
        try (Session session = neo4jDriver.session()) {
            session.run(cypher, Map.of("nodes", nodeParams));
        }
        
        log.info("批量创建了{}个字段节点", nodeParams.size());
    }
    
    /**
     * 导入血缘关系（简化版）
     */
    public void importLineageRelationships() {
        log.info("开始导入血缘关系");
        
        String sql = """
            SELECT 
                APP_NAME, ETL_SYSTEM, ETL_JOB, SQL_NO, SCRIPT_PATH,
                SOURCE_DB, SOURCE_TABLE, SOURCE_COLUMN,
                TARGET_DB, TARGET_TABLE, TARGET_COLUMN,
                RELATIONSHIP_TYPE, SQL_EXPRESSION,
                SOURCE_APP_NAME, TARGET_APP_NAME  -- 如果支持跨应用
            FROM LINEAGE_RELATIONSHIP 
            WHERE IS_ACTIVE = 'Y'
            ORDER BY APP_NAME, ETL_SYSTEM, ETL_JOB, SQL_NO
            """;
            
        List<Map<String, Object>> relationships = oracleJdbcTemplate.queryForList(sql);
        
        for (Map<String, Object> rel : relationships) {
            createLineageRelationship(rel);
        }
        
        log.info("血缘关系导入完成，共导入{}条关系", relationships.size());
    }
    
    private void createLineageRelationship(Map<String, Object> rel) {
        // 生成节点ID
        String sourceNodeId = String.format("%s.%s.%s.%s", 
            rel.get("APP_NAME"), rel.get("SOURCE_DB"), 
            rel.get("SOURCE_TABLE"), rel.get("SOURCE_COLUMN"));
            
        String targetNodeId = String.format("%s.%s.%s.%s",
            rel.get("APP_NAME"), rel.get("TARGET_DB"),
            rel.get("TARGET_TABLE"), rel.get("TARGET_COLUMN"));
        
        String cypher = """
            MATCH (source:ColumnNode {nodeId: $sourceNodeId})
            MATCH (target:ColumnNode {nodeId: $targetNodeId})
            MERGE (source)-[lineage:LINEAGE_FLOW {
                etlJob: $etlJob,
                sqlNo: $sqlNo
            }]->(target)
            ON CREATE SET 
                lineage.sourceApp = $sourceApp,
                lineage.targetApp = $targetApp,
                lineage.etlSystem = $etlSystem,
                lineage.relationshipType = $relationshipType,
                lineage.sqlExpression = $sqlExpression,
                lineage.scriptPath = $scriptPath,
                lineage.createTime = datetime()
            """;
            
        Map<String, Object> params = Map.of(
            "sourceNodeId", sourceNodeId,
            "targetNodeId", targetNodeId,
            "sourceApp", rel.get("SOURCE_APP_NAME") != null ? rel.get("SOURCE_APP_NAME") : rel.get("APP_NAME"),
            "targetApp", rel.get("TARGET_APP_NAME") != null ? rel.get("TARGET_APP_NAME") : rel.get("APP_NAME"),
            "etlSystem", rel.get("ETL_SYSTEM"),
            "etlJob", rel.get("ETL_JOB"),
            "sqlNo", rel.get("SQL_NO"),
            "relationshipType", rel.get("RELATIONSHIP_TYPE"),
            "sqlExpression", rel.get("SQL_EXPRESSION"),
            "scriptPath", rel.get("SCRIPT_PATH")
        );
        
        try (Session session = neo4jDriver.session()) {
            session.run(cypher, params);
        }
    }
}

/**
 * 简化的字段节点信息类
 */
@Data
public class ColumnNodeInfo {
    // Oracle表中的字段
    private String nodeId;
    private String appName;
    private String etlSystem;
    private String etlJob;
    private String scriptPath;
    private String database;
    private String tableName;
    private String columnName;
    
    // Java程序添加的字段
    private String tableType;
    private Boolean isVisible;
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnNodeInfo that = (ColumnNodeInfo) o;
        return Objects.equals(nodeId, that.nodeId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }
}
```

## 四、简化的属性索引

### ⚡ **核心索引（只针对实际使用的字段）**

```cypher
-- 主键约束
CREATE CONSTRAINT constraint_node_id FOR (n:ColumnNode) REQUIRE n.nodeId IS UNIQUE;

-- 查询筛选条件索引（最重要）
CREATE INDEX idx_query_condition FOR (n:ColumnNode) ON (n.appName, n.database, n.tableName, n.columnName);

-- 显示控制索引
CREATE INDEX idx_visibility FOR (n:ColumnNode) ON (n.isVisible);
CREATE INDEX idx_table_type FOR (n:ColumnNode) ON (n.tableType);

-- ETL索引
CREATE INDEX idx_etl_job FOR (n:ColumnNode) ON (n.etlJob);
CREATE INDEX idx_etl_system FOR (n:ColumnNode) ON (n.etlSystem);

-- 血缘关系索引
CREATE INDEX idx_lineage_etl FOR ()-[r:LINEAGE_FLOW]-() ON (r.etlJob, r.sqlNo);
CREATE INDEX idx_lineage_type FOR ()-[r:LINEAGE_FLOW]-() ON (r.relationshipType);
```

## 五、简化的前端接口

### 🎯 **核心查询接口**

```java
@RestController
@RequestMapping("/api/core-lineage")
public class CoreLineageController {
    
    /**
     * 查询上游血缘关系
     */
    @GetMapping("/upstream")
    public Result<LineageGraph> getUpstreamLineage(
            @RequestParam String appName,
            @RequestParam String database,
            @RequestParam String tableName,
            @RequestParam String columnName,
            @RequestParam(defaultValue = "5") Integer maxDepth,
            @RequestParam(defaultValue = "false") Boolean showTemporary,
            @RequestParam(defaultValue = "false") Boolean showSubquery) {
        
        String cypher = """
            MATCH path = (target:ColumnNode)-[:LINEAGE_FLOW*1..$maxDepth]->(source:ColumnNode)
            WHERE target.appName = $appName
              AND target.database = $database
              AND target.tableName = $tableName
              AND target.columnName = $columnName
              AND target.isVisible = true
              AND (CASE 
                WHEN $showTemporary = true AND $showSubquery = true THEN true
                WHEN $showTemporary = true AND $showSubquery = false THEN 
                  ALL(node IN nodes(path) WHERE node.tableType <> 'SUBQUERY')
                WHEN $showTemporary = false AND $showSubquery = true THEN 
                  ALL(node IN nodes(path) WHERE node.tableType <> 'TEMP')
                ELSE 
                  ALL(node IN nodes(path) WHERE node.tableType = 'NORMAL')
                END)
            RETURN path
            ORDER BY length(path)
            """;
            
        Map<String, Object> params = Map.of(
            "appName", appName,
            "database", database,
            "tableName", tableName,
            "columnName", columnName,
            "maxDepth", maxDepth,
            "showTemporary", showTemporary,
            "showSubquery", showSubquery
        );
        
        LineageGraph graph = executeLineageQuery(cypher, params);
        return Result.success(graph);
    }
    
    /**
     * 查询下游血缘关系
     */
    @GetMapping("/downstream")
    public Result<LineageGraph> getDownstreamLineage(
            @RequestParam String appName,
            @RequestParam String database,
            @RequestParam String tableName,
            @RequestParam String columnName,
            @RequestParam(defaultValue = "5") Integer maxDepth,
            @RequestParam(defaultValue = "false") Boolean showTemporary,
            @RequestParam(defaultValue = "false") Boolean showSubquery) {
        
        // 类似上游查询，方向相反
        String cypher = """
            MATCH path = (source:ColumnNode)-[:LINEAGE_FLOW*1..$maxDepth]->(target:ColumnNode)
            WHERE source.appName = $appName
              AND source.database = $database
              AND source.tableName = $tableName
              AND source.columnName = $columnName
              AND source.isVisible = true
              AND (CASE 
                WHEN $showTemporary = true AND $showSubquery = true THEN true
                WHEN $showTemporary = true AND $showSubquery = false THEN 
                  ALL(node IN nodes(path) WHERE node.tableType <> 'SUBQUERY')
                WHEN $showTemporary = false AND $showSubquery = true THEN 
                  ALL(node IN nodes(path) WHERE node.tableType <> 'TEMP')
                ELSE 
                  ALL(node IN nodes(path) WHERE node.tableType = 'NORMAL')
                END)
            RETURN path
            ORDER BY length(path)
            """;
            
        // 执行查询...
    }
}
```

## 六、总结

### ✅ **简化后的优势**

1. **字段精简**：只保留Oracle表中的实际字段，去除冗余属性
2. **判断明确**：通过 `_temp_tab` 和 `subquery_tab` 结尾明确判断表类型
3. **查询清晰**：提供标准的上游、下游、完整血缘查询语句
4. **性能优化**：针对实际使用的字段创建索引
5. **维护简单**：简化的模型更易于理解和维护

### 🎯 **核心要点**

- **单一标签**：`:ColumnNode`
- **核心字段**：基于Oracle表的实际字段
- **智能判断**：`_temp_tab` 和 `subquery_tab` 后缀判断
- **灵活查询**：支持显示/隐藏临时表和子查询
- **索引优化**：针对查询条件创建复合索引

这个简化方案既满足了功能需求，又保持了高性能和易维护性！🚀 