# 简化Neo4j字段血缘模型设计方案

## 一、简化设计理念

### 🎯 **设计原则**
- **单一标签**：只使用 `:ColumnNode` 标签
- **属性丰富**：所有层级信息都作为节点属性存储
- **Java处理**：导入时通过Java程序处理临时表和子查询标记
- **索引优化**：对关键属性创建索引提升查询性能

### 💡 **简化优势**
- 模型更简洁，易于理解和维护
- 查询更直接，不需要复杂的标签组合
- 索引策略更集中，性能优化更有效
- 开发和调试更简单

## 二、唯一标签设计

### 🔍 **ColumnNode标签**（唯一标签）

```cypher
CREATE (col:ColumnNode {
    // 全局唯一标识
    nodeId: "APP_A.DB1.CUSTOMER.CUSTOMER_ID",
    globalId: "APP_A.DB1.CUSTOMER.CUSTOMER_ID",
    
    // 应用层级信息
    appName: "CRM系统",
    appCode: "CRM_APP",
    
    // ETL层级信息
    etlSystem: "客户数据ETL",
    etlJob: "customer_sync.sql",
    scriptPath: "/etl/customer_sync.sql",
    
    // 字段基础信息
    database: "DB1",
    tableName: "CUSTOMER",
    columnName: "CUSTOMER_ID",
    dataType: "VARCHAR2(32)",
    
    // 表类型标记（Java程序处理时添加）
    tableType: "NORMAL",                    // NORMAL/TEMP/SUBQUERY
    isTemporary: false,                     // 是否临时表
    isSubquery: false,                      // 是否子查询
    tempTableSuffix: "",                    // 临时表后缀标识
    subqueryTableSuffix: "",                // 子查询表后缀标识
    
    // 显示控制属性
    isVisible: true,                        // 是否在前端显示
    displayLevel: "NORMAL",                 // NORMAL/HIDDEN/ADMIN_ONLY
    
    // 跨应用支持属性
    dataOwner: "CRM系统",                   // 数据所有者
    accessLevel: "PUBLIC",                  // PUBLIC/PRIVATE/RESTRICTED
    sourceApps: ["CRM系统"],                // 数据源应用列表
    targetApps: ["数据中台"],               // 数据目标应用列表
    
    // 业务属性
    businessMeaning: "客户唯一标识",         // 业务含义
    dataQuality: "HIGH",                    // 数据质量等级
    sensitivityLevel: "LOW",                // 敏感度等级
    
    // 时间属性
    createTime: datetime(),
    updateTime: datetime(),
    lastAccessTime: datetime()
})
```

## 三、血缘关系设计

### 📊 **LINEAGE_FLOW关系**（唯一关系类型）

```cypher
CREATE (source:ColumnNode)-[:LINEAGE_FLOW {
    // 血缘关系标识
    relationshipId: "REL_001",
    relationshipType: "DIRECT",             // DIRECT/DERIVED/CALCULATED
    
    // 应用层级信息
    sourceApp: "电商平台",
    targetApp: "CRM系统", 
    processingApp: "数据中台",              // 处理数据的应用
    
    // ETL处理信息
    etlSystem: "跨应用数据集成ETL",
    etlJob: "customer_integration.sql",
    scriptPath: "/etl/customer_integration.sql",
    sqlNo: 1,                               // SQL编号
    
    // SQL表达式和逻辑
    sqlExpression: "ecom.customer_id as crm.cust_id",
    transformLogic: "直接字段映射",
    
    // 跨应用标识
    isCrossApp: true,                       // 是否跨应用
    dataFlow: "ECOM_TO_CRM",               // 数据流向标识
    
    // 数据处理属性
    processingType: "REAL_TIME",            // REAL_TIME/BATCH/STREAMING
    frequency: "DAILY",                     // 处理频率
    
    // 质量和监控
    dataQualityCheck: true,                 // 是否有数据质量检查
    monitoringEnabled: true,                // 是否启用监控
    
    // 时间属性
    createTime: datetime(),
    updateTime: datetime(),
    lastRunTime: datetime()
}]->(target:ColumnNode)
```

## 四、Java程序处理临时表和子查询

### 🔧 **Java导入处理逻辑**

```java
@Service
public class SimplifiedLineageImportService {
    
    @Autowired
    private Driver neo4jDriver;
    
    @Autowired
    private JdbcTemplate oracleJdbcTemplate;
    
    /**
     * 导入字段节点，处理临时表和子查询标记
     */
    public void importColumnNodesWithProcessing() {
        log.info("开始导入字段节点并处理临时表/子查询标记");
        
        String sql = """
            SELECT DISTINCT
                APP_NAME, ETL_SYSTEM, ETL_JOB, SCRIPT_PATH,
                SOURCE_DB, SOURCE_TABLE, SOURCE_COLUMN,
                TARGET_DB, TARGET_TABLE, TARGET_COLUMN
            FROM LINEAGE_RELATIONSHIP 
            WHERE IS_ACTIVE = 'Y'
            """;
            
        List<Map<String, Object>> records = oracleJdbcTemplate.queryForList(sql);
        
        // 处理源字段和目标字段
        Set<ColumnNodeInfo> allColumns = new HashSet<>();
        
        for (Map<String, Object> record : records) {
            // 处理源字段
            ColumnNodeInfo sourceColumn = processColumnInfo(record, "SOURCE");
            allColumns.add(sourceColumn);
            
            // 处理目标字段
            ColumnNodeInfo targetColumn = processColumnInfo(record, "TARGET");
            allColumns.add(targetColumn);
        }
        
        // 批量创建节点
        createColumnNodesBatch(allColumns);
        
        log.info("字段节点导入完成，共处理{}个字段", allColumns.size());
    }
    
    /**
     * 处理字段信息，识别临时表和子查询
     */
    private ColumnNodeInfo processColumnInfo(Map<String, Object> record, String type) {
        String appName = (String) record.get("APP_NAME");
        String etlSystem = (String) record.get("ETL_SYSTEM");
        String etlJob = (String) record.get("ETL_JOB");
        String scriptPath = (String) record.get("SCRIPT_PATH");
        
        String database = (String) record.get(type + "_DB");
        String tableName = (String) record.get(type + "_TABLE");
        String columnName = (String) record.get(type + "_COLUMN");
        
        ColumnNodeInfo columnInfo = new ColumnNodeInfo();
        
        // 基础信息
        columnInfo.setNodeId(String.format("%s.%s.%s.%s", appName, database, tableName, columnName));
        columnInfo.setAppName(appName);
        columnInfo.setEtlSystem(etlSystem);
        columnInfo.setEtlJob(etlJob);
        columnInfo.setScriptPath(scriptPath);
        columnInfo.setDatabase(database);
        columnInfo.setTableName(tableName);
        columnInfo.setColumnName(columnName);
        
        // 🎯 关键：处理临时表和子查询标记
        TableTypeInfo tableTypeInfo = analyzeTableType(tableName);
        columnInfo.setTableType(tableTypeInfo.getTableType());
        columnInfo.setIsTemporary(tableTypeInfo.isTemporary());
        columnInfo.setIsSubquery(tableTypeInfo.isSubquery());
        columnInfo.setTempTableSuffix(tableTypeInfo.getTempSuffix());
        columnInfo.setSubqueryTableSuffix(tableTypeInfo.getSubquerySuffix());
        
        // 设置显示属性
        columnInfo.setIsVisible(shouldBeVisible(tableTypeInfo));
        columnInfo.setDisplayLevel(determineDisplayLevel(tableTypeInfo));
        
        return columnInfo;
    }
    
    /**
     * 🔍 分析表类型（临时表/子查询识别）
     */
    private TableTypeInfo analyzeTableType(String tableName) {
        TableTypeInfo typeInfo = new TableTypeInfo();
        
        // 检查临时表
        if (tableName.endsWith("_temp_table")) {
            typeInfo.setTableType("TEMP");
            typeInfo.setTemporary(true);
            typeInfo.setTempSuffix("_temp_table");
        }
        // 检查子查询表
        else if (tableName.endsWith("_subquery_table")) {
            typeInfo.setTableType("SUBQUERY");
            typeInfo.setSubquery(true);
            typeInfo.setSubquerySuffix("_subquery_table");
        }
        // 其他临时表模式
        else if (tableName.matches(".*temp.*\\d+$")) {
            typeInfo.setTableType("TEMP");
            typeInfo.setTemporary(true);
            typeInfo.setTempSuffix("temp");
        }
        // 子查询表其他模式
        else if (tableName.matches(".*subq.*\\d+$")) {
            typeInfo.setTableType("SUBQUERY");
            typeInfo.setSubquery(true);
            typeInfo.setSubquerySuffix("subq");
        }
        // 正常表
        else {
            typeInfo.setTableType("NORMAL");
            typeInfo.setTemporary(false);
            typeInfo.setSubquery(false);
        }
        
        return typeInfo;
    }
    
    /**
     * 🎯 确定是否应该显示（临时表和子查询默认隐藏）
     */
    private boolean shouldBeVisible(TableTypeInfo typeInfo) {
        return !typeInfo.isTemporary() && !typeInfo.isSubquery();
    }
    
    /**
     * 确定显示级别
     */
    private String determineDisplayLevel(TableTypeInfo typeInfo) {
        if (typeInfo.isTemporary() || typeInfo.isSubquery()) {
            return "HIDDEN";  // 临时表和子查询默认隐藏
        }
        return "NORMAL";
    }
    
    /**
     * 批量创建字段节点
     */
    private void createColumnNodesBatch(Set<ColumnNodeInfo> columns) {
        List<Map<String, Object>> nodeParams = new ArrayList<>();
        
        for (ColumnNodeInfo column : columns) {
            Map<String, Object> params = new HashMap<>();
            params.put("nodeId", column.getNodeId());
            params.put("appName", column.getAppName());
            params.put("etlSystem", column.getEtlSystem());
            params.put("etlJob", column.getEtlJob());
            params.put("scriptPath", column.getScriptPath());
            params.put("database", column.getDatabase());
            params.put("tableName", column.getTableName());
            params.put("columnName", column.getColumnName());
            params.put("tableType", column.getTableType());
            params.put("isTemporary", column.getIsTemporary());
            params.put("isSubquery", column.getIsSubquery());
            params.put("tempTableSuffix", column.getTempTableSuffix());
            params.put("subqueryTableSuffix", column.getSubqueryTableSuffix());
            params.put("isVisible", column.getIsVisible());
            params.put("displayLevel", column.getDisplayLevel());
            
            nodeParams.add(params);
        }
        
        // 使用批量导入
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
                col.isTemporary = node.isTemporary,
                col.isSubquery = node.isSubquery,
                col.tempTableSuffix = node.tempTableSuffix,
                col.subqueryTableSuffix = node.subqueryTableSuffix,
                col.isVisible = node.isVisible,
                col.displayLevel = node.displayLevel,
                col.displayName = node.appName + '.' + node.database + '.' + node.tableName + '.' + node.columnName,
                col.createTime = datetime()
            ON MATCH SET 
                col.updateTime = datetime()
            """;
            
        try (Session session = neo4jDriver.session()) {
            session.run(cypher, Map.of("nodes", nodeParams));
        }
        
        log.info("批量创建了{}个字段节点", nodeParams.size());
    }
}

/**
 * 表类型信息类
 */
@Data
public class TableTypeInfo {
    private String tableType;           // NORMAL/TEMP/SUBQUERY
    private boolean isTemporary;        // 是否临时表
    private boolean isSubquery;         // 是否子查询
    private String tempSuffix;          // 临时表后缀
    private String subquerySuffix;      // 子查询后缀
}

/**
 * 字段节点信息类
 */
@Data  
public class ColumnNodeInfo {
    private String nodeId;
    private String appName;
    private String etlSystem;
    private String etlJob;
    private String scriptPath;
    private String database;
    private String tableName;
    private String columnName;
    private String tableType;
    private Boolean isTemporary;
    private Boolean isSubquery;
    private String tempTableSuffix;
    private String subqueryTableSuffix;
    private Boolean isVisible;
    private String displayLevel;
    
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

## 五、Neo4j属性索引设计

### ⚡ **Neo4j属性索引完全支持**

Neo4j完全支持对节点属性创建索引，这是性能优化的关键！

### 🚀 **核心属性索引**

```cypher
-- 1. 主键索引（必须）
CREATE CONSTRAINT constraint_node_id FOR (n:ColumnNode) REQUIRE n.nodeId IS UNIQUE;

-- 2. 应用查询索引
CREATE INDEX idx_app_name FOR (n:ColumnNode) ON (n.appName);
CREATE INDEX idx_app_db FOR (n:ColumnNode) ON (n.appName, n.database);
CREATE INDEX idx_app_db_table FOR (n:ColumnNode) ON (n.appName, n.database, n.tableName);

-- 3. 完整字段路径索引（最重要的查询索引）
CREATE INDEX idx_full_path FOR (n:ColumnNode) ON (n.appName, n.database, n.tableName, n.columnName);

-- 4. 数据库和表索引
CREATE INDEX idx_database FOR (n:ColumnNode) ON (n.database);
CREATE INDEX idx_table FOR (n:ColumnNode) ON (n.database, n.tableName);
CREATE INDEX idx_column FOR (n:ColumnNode) ON (n.columnName);

-- 5. ETL层级索引
CREATE INDEX idx_etl_system FOR (n:ColumnNode) ON (n.etlSystem);
CREATE INDEX idx_etl_job FOR (n:ColumnNode) ON (n.etlJob);
CREATE INDEX idx_etl_app_system FOR (n:ColumnNode) ON (n.appName, n.etlSystem);
CREATE INDEX idx_etl_full FOR (n:ColumnNode) ON (n.appName, n.etlSystem, n.etlJob);

-- 6. 表类型和显示控制索引
CREATE INDEX idx_table_type FOR (n:ColumnNode) ON (n.tableType);
CREATE INDEX idx_visibility FOR (n:ColumnNode) ON (n.isVisible);
CREATE INDEX idx_display_level FOR (n:ColumnNode) ON (n.displayLevel);
CREATE INDEX idx_temp_subquery FOR (n:ColumnNode) ON (n.isTemporary, n.isSubquery);

-- 7. 跨应用支持索引
CREATE INDEX idx_data_owner FOR (n:ColumnNode) ON (n.dataOwner);
CREATE INDEX idx_access_level FOR (n:ColumnNode) ON (n.accessLevel);

-- 8. 时间查询索引
CREATE INDEX idx_create_time FOR (n:ColumnNode) ON (n.createTime);
CREATE INDEX idx_update_time FOR (n:ColumnNode) ON (n.updateTime);

-- 9. 复合查询优化索引
CREATE INDEX idx_visible_normal FOR (n:ColumnNode) ON (n.isVisible, n.tableType);
CREATE INDEX idx_app_visible FOR (n:ColumnNode) ON (n.appName, n.isVisible);
```

### 🔗 **血缘关系索引**

```cypher
-- 血缘关系属性索引
CREATE INDEX idx_lineage_apps FOR ()-[r:LINEAGE_FLOW]-() ON (r.sourceApp, r.targetApp);
CREATE INDEX idx_lineage_processing FOR ()-[r:LINEAGE_FLOW]-() ON (r.processingApp);
CREATE INDEX idx_lineage_etl FOR ()-[r:LINEAGE_FLOW]-() ON (r.etlSystem, r.etlJob);
CREATE INDEX idx_lineage_cross_app FOR ()-[r:LINEAGE_FLOW]-() ON (r.isCrossApp);
CREATE INDEX idx_lineage_type FOR ()-[r:LINEAGE_FLOW]-() ON (r.relationshipType);
CREATE INDEX idx_lineage_sql_no FOR ()-[r:LINEAGE_FLOW]-() ON (r.etlJob, r.sqlNo);
```

## 六、优化查询示例

### 🔍 **利用属性索引的高效查询**

```cypher
-- 1. 根据筛选条件查询（利用复合索引）
MATCH (target:ColumnNode)
WHERE target.appName = $appName
  AND target.database = $database
  AND target.tableName = $tableName
  AND target.columnName = $columnName
  AND target.isVisible = true          -- 利用可见性索引
WITH target

MATCH path = (target)-[:LINEAGE_FLOW*1..5]->(source:ColumnNode)
WHERE ALL(node IN nodes(path) WHERE 
  node.isVisible = true 
  AND node.tableType = 'NORMAL'        -- 利用表类型索引
)
RETURN path

-- 2. 过滤临时表和子查询（利用临时表索引）
MATCH (col:ColumnNode)
WHERE col.appName = $appName
  AND col.isTemporary = false          -- 利用临时表索引
  AND col.isSubquery = false           -- 利用子查询索引
  AND col.isVisible = true
RETURN col

-- 3. ETL作业查询（利用ETL索引）
MATCH (col:ColumnNode)
WHERE col.etlSystem = $etlSystem
  AND col.etlJob = $etlJob             -- 利用ETL作业索引
RETURN col

-- 4. 跨应用查询（利用应用索引）
MATCH (source:ColumnNode)-[rel:LINEAGE_FLOW]->(target:ColumnNode)
WHERE rel.sourceApp = $sourceApp
  AND rel.targetApp = $targetApp       -- 利用跨应用索引
  AND rel.isCrossApp = true
RETURN source, rel, target
```

## 七、前端接口简化

### 🎯 **简化后的查询接口**

```java
@RestController
@RequestMapping("/api/simplified-lineage")
public class SimplifiedLineageController {
    
    /**
     * 通用血缘查询（利用属性索引）
     */
    @GetMapping("/query")
    public Result<LineageGraph> queryLineage(
            @RequestParam String appName,
            @RequestParam String database,
            @RequestParam String tableName,
            @RequestParam String columnName,
            @RequestParam(defaultValue = "BOTH") String direction,
            @RequestParam(defaultValue = "5") Integer maxDepth,
            @RequestParam(defaultValue = "false") Boolean showTemporary,
            @RequestParam(defaultValue = "false") Boolean showSubquery) {
        
        LineageQueryOptions options = LineageQueryOptions.builder()
            .appName(appName)
            .database(database)
            .tableName(tableName)
            .columnName(columnName)
            .direction(direction)
            .maxDepth(maxDepth)
            .showTemporary(showTemporary)
            .showSubquery(showSubquery)
            .build();
            
        // 利用属性索引的高效查询
        LineageGraph graph = simplifiedLineageService.queryWithIndexOptimization(options);
        return Result.success(graph);
    }
    
    /**
     * 获取应用列表（利用应用索引）
     */
    @GetMapping("/applications")
    public Result<List<String>> getApplications() {
        String cypher = """
            MATCH (col:ColumnNode)
            WHERE col.isVisible = true
            RETURN DISTINCT col.appName as appName
            ORDER BY appName
            """;
        // 利用 idx_app_visible 索引
        return Result.success(executeQuery(cypher));
    }
    
    /**
     * 级联查询数据库（利用复合索引）
     */
    @GetMapping("/databases")
    public Result<List<String>> getDatabases(@RequestParam String appName) {
        String cypher = """
            MATCH (col:ColumnNode)
            WHERE col.appName = $appName 
              AND col.isVisible = true
            RETURN DISTINCT col.database as database
            ORDER BY database
            """;
        // 利用 idx_app_visible 索引
        return Result.success(executeQuery(cypher, Map.of("appName", appName)));
    }
}
```

## 八、性能测试验证

### 📊 **索引效果验证**

```cypher
-- 1. 查看索引使用情况
EXPLAIN MATCH (col:ColumnNode)
WHERE col.appName = 'CRM系统'
  AND col.database = 'DB1'
  AND col.tableName = 'CUSTOMER'
  AND col.columnName = 'CUSTOMER_ID'
RETURN col

-- 2. 性能分析
PROFILE MATCH path = (target:ColumnNode)-[:LINEAGE_FLOW*1..3]->(source:ColumnNode)
WHERE target.appName = 'CRM系统'
  AND target.isVisible = true
  AND target.tableType = 'NORMAL'
RETURN path

-- 3. 索引统计信息
CALL db.indexes() YIELD name, type, state, populationPercent
WHERE type = 'BTREE'
RETURN name, state, populationPercent
ORDER BY name
```

## 九、总结

### ✅ **简化模型优势**

1. **架构简洁**：只有一个标签 `:ColumnNode`，所有信息通过属性存储
2. **查询高效**：充分利用Neo4j属性索引，查询性能优异
3. **处理智能**：Java程序智能识别和标记临时表/子查询
4. **维护简单**：单一标签降低了模型复杂度，便于维护
5. **扩展灵活**：通过属性扩展功能，不需要修改图结构

### 🚀 **关键技术点**

- **属性索引**：Neo4j完全支持属性索引，是性能优化的核心
- **Java处理**：在导入时智能处理临时表和子查询标记
- **复合索引**：针对常用查询模式创建复合索引
- **批量导入**：使用 `UNWIND` 进行高效批量导入

这个简化方案既保持了功能完整性，又大幅提升了性能和可维护性！🎯 