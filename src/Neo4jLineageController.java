package com.lineage.controller;

import com.alibaba.excel.EasyExcel;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Neo4j血缘关系查询和导出控制器 - SpringBoot 2.1.7版本
 * 统一接口处理表级别和字段级别的血缘关系查询，并导出为Excel
 * 字段级别查询支持传入多个columnIds
 */
@RestController
@RequestMapping("/api/lineage")
public class Neo4jLineageController {

    @Autowired
    private Driver neo4jDriver;

    /**
     * 🎯 表级别血缘关系查询并导出Excel接口
     * 
     * @param labelName 标签名称，默认：COLUMN_BDP_OLD
     * @param relationshipName 关系名称，默认：column_bdp_old_rel  
     * @param dbName 数据库名称
     * @param tblName 表名称
     * @param pageSize 分页大小，默认：1000
     * @param pageNo 分页页码，默认：1
     * @param skipTempTable 是否过滤子查询和临时表，默认：true
     * @param flag 查询类型：1=溯源，2=被引用，其他=双向，默认：1
     * @param level 层级深度，默认：5
     */
    @GetMapping("/table/export")
    public void exportTableLineage(
            @RequestParam String labelName,
            @RequestParam String relationshipName,
            @RequestParam String dbName,
            @RequestParam String tblName,
            @RequestParam Integer pageSize,
            @RequestParam Integer pageNo,
            @RequestParam Boolean skipTempTable,
            @RequestParam Integer flag, // 1=溯源，2=被引用，其他=双向
            @RequestParam(required = false) Integer level,
            HttpServletResponse response) throws IOException {

        // 构建查询参数
        LineageQueryParams params = new LineageQueryParams();
        params.setLabelName(labelName);
        params.setRelationshipName(relationshipName);
        params.setDbName(dbName);
        params.setTblName(tblName);
        params.setPageSize(pageSize);
        params.setPageNo(pageNo);
        params.setSkipTempTable(skipTempTable);
        params.setFlag(flag);
        params.setLevel(level != null ? level : 5); // level可为空，默认值5
        params.setQueryType("TABLE"); // 表级别查询

        // 构建Cypher查询语句
        String cypher = buildTableLineageCypher(params);
        
        // 执行查询
        List<LineageExportDTO> lineageData = executeLineageQuery(cypher, params);
        
        // 导出Excel
        exportToExcel(response, lineageData, "table_lineage");
    }

    /**
     * 🎯 字段级别血缘关系查询并导出Excel接口
     * 支持传入多个columnIds，同时支持数据库和表名称过滤
     * 
     * @param labelName 标签名称，默认：COLUMN_BDP_OLD
     * @param relationshipName 关系名称，默认：column_bdp_old_rel
     * @param columnIds 字段节点ID列表（映射节点中的id属性值，可以传多个）
     * @param dbName 数据库名称（可选，用于进一步过滤）
     * @param tblName 表名称（可选，用于进一步过滤）
     * @param pageSize 分页大小，默认：1000
     * @param pageNo 分页页码，默认：1
     * @param skipTempTable 是否过滤子查询和临时表，默认：true
     * @param flag 查询类型：1=溯源，2=被引用，其他=双向，默认：1
     * @param level 层级深度，默认：5
     */
    @GetMapping("/column/export")
    public void exportColumnLineage(
            @RequestParam String labelName,
            @RequestParam String relationshipName,
            @RequestParam List<String> columnIds,
            @RequestParam String dbName,
            @RequestParam String tblName,
            @RequestParam Integer pageSize,
            @RequestParam Integer pageNo,
            @RequestParam Boolean skipTempTable,
            @RequestParam Integer flag, // 1=溯源，2=被引用，其他=双向
            @RequestParam(required = false) Integer level,
            HttpServletResponse response) throws IOException {

        // 验证columnIds
        if (columnIds == null || columnIds.isEmpty()) {
            throw new IllegalArgumentException("columnIds参数不能为空");
        }

        // 过滤空值
        List<String> validColumnIds = columnIds.stream()
                .filter(id -> id != null && !id.trim().isEmpty())
                .collect(Collectors.toList());

        if (validColumnIds.isEmpty()) {
            throw new IllegalArgumentException("columnIds参数不能包含空值");
        }

        // 构建查询参数
        LineageQueryParams params = new LineageQueryParams();
        params.setLabelName(labelName);
        params.setRelationshipName(relationshipName);
        params.setColumnIds(validColumnIds);
        params.setDbName(dbName);
        params.setTblName(tblName);
        params.setPageSize(pageSize);
        params.setPageNo(pageNo);
        params.setSkipTempTable(skipTempTable);
        params.setFlag(flag);
        params.setLevel(level != null ? level : 5); // level可为空，默认值5
        params.setQueryType("COLUMN"); // 字段级别查询

        // 构建Cypher查询语句
        String cypher = buildColumnLineageCypher(params);
        
        // 执行查询
        List<LineageExportDTO> lineageData = executeLineageQuery(cypher, params);
        
        // 导出Excel
        exportToExcel(response, lineageData, "column_lineage");
    }

    /**
     * 构建表级别血缘关系Cypher查询
     * 🎯 修正：使用UNION查询，包含直接关系和跨越临时表的关系
     */
    private String buildTableLineageCypher(LineageQueryParams params) {
        StringBuilder cypher = new StringBuilder();
        
        // 第一部分：直接关系（没有中间表）
        cypher.append("MATCH (src:").append(params.getLabelName()).append(")")
              .append("-[rel:").append(params.getRelationshipName()).append("]-")
              .append("(tar:").append(params.getLabelName()).append(") ");
        
        // 添加WHERE条件 - 表级别查询
        cypher.append("WHERE ");
        
        if (params.getFlag() == 1) {
            // 溯源查询：目标表匹配条件
            cypher.append("tar.dbName = $dbName AND tar.tblName = $tblName ");
        } else if (params.getFlag() == 2) {
            // 被引用查询：源表匹配条件  
            cypher.append("src.dbName = $dbName AND src.tblName = $tblName ");
        } else {
            // 双向查询：源表或目标表匹配条件
            cypher.append("((src.dbName = $dbName AND src.tblName = $tblName) ")
                  .append("OR (tar.dbName = $dbName AND tar.tblName = $tblName)) ");
        }
        
        // 过滤临时表
        if (params.getSkipTempTable()) {
            cypher.append("AND src.tempFlag <> 1 AND tar.tempFlag <> 1 ");
        }
        
        // 返回结果
        cypher.append("RETURN DISTINCT ")
              .append("rel.etlSystem as etlSystem, ")
              .append("rel.etlJob as etlJob, ")
              .append("rel.sqlNo as sqlNo, ")
              .append("rel.appName as appName, ")
              .append("src.dbName as src_db, ")
              .append("src.tblName as src_tbl, ")
              .append("src.colName as src_col, ")
              .append("tar.dbName as tar_db, ")
              .append("tar.tblName as tar_tbl, ")
              .append("tar.colName as tar_col ");
        
        // UNION - 第二部分：跨越临时表的关系
        cypher.append("UNION ");
        
        // 跨越临时表的关系查询
        cypher.append("MATCH path = (src:").append(params.getLabelName()).append(")")
              .append("-[:").append(params.getRelationshipName()).append("*2..5]-")
              .append(">(tar:").append(params.getLabelName()).append(") ");
        
        cypher.append("WHERE ");
        
        if (params.getFlag() == 1) {
            // 溯源查询：目标表匹配条件
            cypher.append("tar.dbName = $dbName AND tar.tblName = $tblName ");
        } else if (params.getFlag() == 2) {
            // 被引用查询：源表匹配条件  
            cypher.append("src.dbName = $dbName AND src.tblName = $tblName ");
        } else {
            // 双向查询：源表或目标表匹配条件
            cypher.append("((src.dbName = $dbName AND src.tblName = $tblName) ")
                  .append("OR (tar.dbName = $dbName AND tar.tblName = $tblName)) ");
        }
        
        if (params.getSkipTempTable()) {
            cypher.append("AND src.tempFlag <> 1 AND tar.tempFlag <> 1 ");
            cypher.append("AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar) ");
        }
        
        cypher.append("WITH DISTINCT src, tar ");
        cypher.append("MATCH path = (src)-[:").append(params.getRelationshipName()).append("*1..5]->(tar) ");
        cypher.append("WITH src, tar, relationships(path)[0] as rel ");
        
        cypher.append("RETURN DISTINCT ")
              .append("rel.etlSystem as etlSystem, ")
              .append("rel.etlJob as etlJob, ")
              .append("rel.sqlNo as sqlNo, ")
              .append("rel.appName as appName, ")
              .append("src.dbName as src_db, ")
              .append("src.tblName as src_tbl, ")
              .append("src.colName as src_col, ")
              .append("tar.dbName as tar_db, ")
              .append("tar.tblName as tar_tbl, ")
              .append("tar.colName as tar_col ");
        
        // 最终排序和分页
        cypher.append("ORDER BY etlJob, sqlNo, src_db, src_tbl, src_col ");
        
        // 分页
        int skip = (params.getPageNo() - 1) * params.getPageSize();
        cypher.append("SKIP ").append(skip).append(" LIMIT ").append(params.getPageSize());
        
        return cypher.toString();
    }

    /**
     * 构建ETL系统血缘关系Cypher查询
     * 🎯 修正：使用UNION查询，包含直接关系和跨越临时表的关系，不限制表和字段
     */
    private String buildEtlLineageCypher(LineageQueryParams params) {
        StringBuilder cypher = new StringBuilder();
        
        // 第一部分：直接关系（没有中间表）
        cypher.append("MATCH (src:").append(params.getLabelName()).append(")")
              .append("-[rel:").append(params.getRelationshipName()).append("]-")
              .append("(tar:").append(params.getLabelName()).append(") ");
        
        // 添加WHERE条件 - ETL系统查询（不限制表和字段）
        cypher.append("WHERE rel.etlSystem = $etlSystem ");
        cypher.append("AND rel.etlJob IN $etlJobs ");
        
        // 可选的appName过滤
        if (params.getAppName() != null && !params.getAppName().trim().isEmpty()) {
            cypher.append("AND rel.appName = $appName ");
        }
        
        // 过滤临时表
        if (params.getSkipTempTable()) {
            cypher.append("AND src.tempFlag <> 1 AND tar.tempFlag <> 1 ");
        }
        
        // 返回结果
        cypher.append("RETURN DISTINCT ")
              .append("rel.etlSystem as etlSystem, ")
              .append("rel.etlJob as etlJob, ")
              .append("rel.sqlNo as sqlNo, ")
              .append("rel.appName as appName, ")
              .append("src.dbName as src_db, ")
              .append("src.tblName as src_tbl, ")
              .append("src.colName as src_col, ")
              .append("tar.dbName as tar_db, ")
              .append("tar.tblName as tar_tbl, ")
              .append("tar.colName as tar_col ");
        
        // UNION - 第二部分：跨越临时表的关系
        cypher.append("UNION ");
        
        // 跨越临时表的关系查询
        cypher.append("MATCH path = (src:").append(params.getLabelName()).append(")")
              .append("-[:").append(params.getRelationshipName()).append("*2..5]-")
              .append(">(tar:").append(params.getLabelName()).append(") ");
        
        cypher.append("WHERE src.tempFlag <> 1 AND tar.tempFlag <> 1 ");
        
        if (params.getSkipTempTable()) {
            cypher.append("AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar) ");
        }
        
        cypher.append("AND any(rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs");
        
        // 可选的appName过滤
        if (params.getAppName() != null && !params.getAppName().trim().isEmpty()) {
            cypher.append(" AND rel.appName = $appName");
        }
        
        cypher.append(") ");
        
        cypher.append("WITH DISTINCT src, tar ");
        cypher.append("MATCH path = (src)-[:").append(params.getRelationshipName()).append("*1..5]->(tar) ");
        cypher.append("WHERE any(rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs) ");
        cypher.append("WITH src, tar, [rel IN relationships(path) WHERE rel.etlSystem = $etlSystem AND rel.etlJob IN $etlJobs][0] as firstRel ");
        
        cypher.append("RETURN DISTINCT ")
              .append("firstRel.etlSystem as etlSystem, ")
              .append("firstRel.etlJob as etlJob, ")
              .append("firstRel.sqlNo as sqlNo, ")
              .append("firstRel.appName as appName, ")
              .append("src.dbName as src_db, ")
              .append("src.tblName as src_tbl, ")
              .append("src.colName as src_col, ")
              .append("tar.dbName as tar_db, ")
              .append("tar.tblName as tar_tbl, ")
              .append("tar.colName as tar_col ");
        
        // 最终排序和分页
        cypher.append("ORDER BY etlJob, sqlNo, src_db, src_tbl, src_col ");
        
        // 分页
        int skip = (params.getPageNo() - 1) * params.getPageSize();
        cypher.append("SKIP ").append(skip).append(" LIMIT ").append(params.getPageSize());
        
        return cypher.toString();
    }

    /**
     * 构建字段级别血缘关系Cypher查询（支持多个columnIds）
     * 🎯 修正：使用UNION查询，包含直接关系和跨越临时表的关系
     */
    private String buildColumnLineageCypher(LineageQueryParams params) {
        StringBuilder cypher = new StringBuilder();
        
        // 第一部分：直接关系（没有中间表）
        cypher.append("MATCH (src:").append(params.getLabelName()).append(")")
              .append("-[rel:").append(params.getRelationshipName()).append("]-")
              .append("(tar:").append(params.getLabelName()).append(") ");
        
        // 添加WHERE条件 - 字段级别查询
        cypher.append("WHERE ");
        
        if (params.getFlag() == 1) {
            // 溯源查询：目标字段匹配条件
            cypher.append("tar.id IN $columnIds ");
            cypher.append("AND tar.dbName = $dbName ");
            cypher.append("AND tar.tblName = $tblName ");
        } else if (params.getFlag() == 2) {
            // 被引用查询：源字段匹配条件
            cypher.append("src.id IN $columnIds ");
            cypher.append("AND src.dbName = $dbName ");
            cypher.append("AND src.tblName = $tblName ");
        } else {
            // 双向查询：源字段或目标字段匹配条件
            cypher.append("((src.id IN $columnIds AND src.dbName = $dbName AND src.tblName = $tblName) ");
            cypher.append("OR (tar.id IN $columnIds AND tar.dbName = $dbName AND tar.tblName = $tblName)) ");
        }
        
        // 过滤临时表
        if (params.getSkipTempTable()) {
            cypher.append("AND src.tempFlag <> 1 AND tar.tempFlag <> 1 ");
        }
        
        // 返回结果
        cypher.append("RETURN DISTINCT ")
              .append("rel.etlSystem as etlSystem, ")
              .append("rel.etlJob as etlJob, ")
              .append("rel.sqlNo as sqlNo, ")
              .append("rel.appName as appName, ")
              .append("src.dbName as src_db, ")
              .append("src.tblName as src_tbl, ")
              .append("src.colName as src_col, ")
              .append("tar.dbName as tar_db, ")
              .append("tar.tblName as tar_tbl, ")
              .append("tar.colName as tar_col ");
        
        // UNION - 第二部分：跨越临时表的关系
        cypher.append("UNION ");
        
        // 跨越临时表的关系查询
        cypher.append("MATCH path = (src:").append(params.getLabelName()).append(")")
              .append("-[:").append(params.getRelationshipName()).append("*2..5]-")
              .append(">(tar:").append(params.getLabelName()).append(") ");
        
        cypher.append("WHERE ");
        
        if (params.getFlag() == 1) {
            // 溯源查询：目标字段匹配条件
            cypher.append("tar.id IN $columnIds ");
            cypher.append("AND tar.dbName = $dbName ");
            cypher.append("AND tar.tblName = $tblName ");
        } else if (params.getFlag() == 2) {
            // 被引用查询：源字段匹配条件
            cypher.append("src.id IN $columnIds ");
            cypher.append("AND src.dbName = $dbName ");
            cypher.append("AND src.tblName = $tblName ");
        } else {
            // 双向查询：源字段或目标字段匹配条件
            cypher.append("((src.id IN $columnIds AND src.dbName = $dbName AND src.tblName = $tblName) ");
            cypher.append("OR (tar.id IN $columnIds AND tar.dbName = $dbName AND tar.tblName = $tblName)) ");
        }
        
        if (params.getSkipTempTable()) {
            cypher.append("AND src.tempFlag <> 1 AND tar.tempFlag <> 1 ");
            cypher.append("AND any(n IN nodes(path) WHERE n.tempFlag = 1 AND n <> src AND n <> tar) ");
        }
        
        cypher.append("WITH DISTINCT src, tar ");
        cypher.append("MATCH path = (src)-[:").append(params.getRelationshipName()).append("*1..5]->(tar) ");
        cypher.append("WITH src, tar, relationships(path)[0] as rel ");
        
        cypher.append("RETURN DISTINCT ")
              .append("rel.etlSystem as etlSystem, ")
              .append("rel.etlJob as etlJob, ")
              .append("rel.sqlNo as sqlNo, ")
              .append("rel.appName as appName, ")
              .append("src.dbName as src_db, ")
              .append("src.tblName as src_tbl, ")
              .append("src.colName as src_col, ")
              .append("tar.dbName as tar_db, ")
              .append("tar.tblName as tar_tbl, ")
              .append("tar.colName as tar_col ");
        
        // 最终排序和分页
        cypher.append("ORDER BY etlJob, sqlNo, src_db, src_tbl, src_col ");
        
        // 分页
        int skip = (params.getPageNo() - 1) * params.getPageSize();
        cypher.append("SKIP ").append(skip).append(" LIMIT ").append(params.getPageSize());
        
        return cypher.toString();
    }

    /**
     * 执行血缘关系查询
     */
    private List<LineageExportDTO> executeLineageQuery(String cypher, LineageQueryParams params) {
        List<LineageExportDTO> results = new ArrayList<>();
        
        try (Session session = neo4jDriver.session()) {
            // 构建查询参数
            Map<String, Object> parameters = new HashMap<>();
            
            if ("COLUMN".equals(params.getQueryType())) {
                // 字段级别查询：使用columnIds和必填的dbName、tblName
                parameters.put("columnIds", params.getColumnIds());
                parameters.put("dbName", params.getDbName());
                parameters.put("tblName", params.getTblName());
            } else if ("ETL".equals(params.getQueryType())) {
                // ETL系统查询：使用etlSystem、etlJobs和可选的appName
                parameters.put("etlSystem", params.getEtlSystem());
                parameters.put("etlJobs", params.getEtlJobs());
                if (params.getAppName() != null && !params.getAppName().trim().isEmpty()) {
                    parameters.put("appName", params.getAppName());
                }
            } else {
                // 表级别查询：使用dbName和tblName
                parameters.put("dbName", params.getDbName());
                parameters.put("tblName", params.getTblName());
            }
            
            Result result = session.run(cypher, parameters);
            
            while (result.hasNext()) {
                Record record = result.next();
                LineageExportDTO dto = new LineageExportDTO();
                
                dto.setEtlSystem(getStringValue(record, "etlSystem"));
                dto.setEtlJob(getStringValue(record, "etlJob"));
                dto.setSqlNo(getIntValue(record, "sqlNo"));
                dto.setAppName(getStringValue(record, "appName"));
                dto.setSrcDb(getStringValue(record, "src_db"));
                dto.setSrcTbl(getStringValue(record, "src_tbl"));
                dto.setSrcCol(getStringValue(record, "src_col"));
                dto.setTarDb(getStringValue(record, "tar_db"));
                dto.setTarTbl(getStringValue(record, "tar_tbl"));
                dto.setTarCol(getStringValue(record, "tar_col"));
                
                results.add(dto);
            }
        } catch (Exception e) {
            throw new RuntimeException("查询失败: " + e.getMessage(), e);
        }
        
        return results;
    }

    /**
     * 导出到Excel
     */
    private void exportToExcel(HttpServletResponse response, List<LineageExportDTO> data, String fileNamePrefix) throws IOException {
        // 设置响应头 - 适配SpringBoot 2.1.7
        response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        response.setCharacterEncoding("utf-8");
        
        // 生成文件名
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String fileName = URLEncoder.encode(fileNamePrefix + "_" + timestamp + ".xlsx", "UTF-8");
        response.setHeader("Content-Disposition", "attachment;filename*=utf-8''" + fileName);
        
        // 使用EasyExcel导出
        EasyExcel.write(response.getOutputStream(), LineageExportDTO.class)
                .sheet("血缘关系")
                .doWrite(data);
    }

    /**
     * 🎯 ETL系统血缘关系查询并导出Excel接口
     * 通过etlSystem、etlJob、appName查询血缘关系
     * 
     * @param labelName 标签名称
     * @param relationshipName 关系名称
     * @param etlSystem ETL系统名称（必填）
     * @param etlJobs ETL作业名称列表（必填，可传多个）
     * @param appName 应用名称（可选）
     * @param pageSize 分页大小
     * @param pageNo 分页页码
     * @param skipTempTable 是否过滤子查询和临时表
     * @param level 层级深度，默认：5
     */
    @GetMapping("/etl/export")
    public void exportEtlLineage(
            @RequestParam String labelName,
            @RequestParam String relationshipName,
            @RequestParam String etlSystem,
            @RequestParam List<String> etlJobs,
            @RequestParam(required = false) String appName,
            @RequestParam Integer pageSize,
            @RequestParam Integer pageNo,
            @RequestParam Boolean skipTempTable,
            @RequestParam(required = false) Integer level,
            HttpServletResponse response) throws IOException {

        // 验证etlJobs
        if (etlJobs == null || etlJobs.isEmpty()) {
            throw new IllegalArgumentException("etlJobs参数不能为空");
        }

        // 过滤空值
        List<String> validEtlJobs = etlJobs.stream()
                .filter(job -> job != null && !job.trim().isEmpty())
                .collect(Collectors.toList());

        if (validEtlJobs.isEmpty()) {
            throw new IllegalArgumentException("etlJobs参数不能包含空值");
        }

        // 构建查询参数
        LineageQueryParams params = new LineageQueryParams();
        params.setLabelName(labelName);
        params.setRelationshipName(relationshipName);
        params.setEtlSystem(etlSystem);
        params.setEtlJobs(validEtlJobs);
        params.setAppName(appName);
        params.setPageSize(pageSize);
        params.setPageNo(pageNo);
        params.setSkipTempTable(skipTempTable);
        params.setLevel(level != null ? level : 5); // level可为空，默认值5
        params.setQueryType("ETL"); // ETL系统查询

        // 构建Cypher查询语句
        String cypher = buildEtlLineageCypher(params);
        
        // 执行查询
        List<LineageExportDTO> lineageData = executeLineageQuery(cypher, params);
        
        // 导出Excel
        exportToExcel(response, lineageData, "etl_lineage");
    }

    /**
     * 🎯 ETL系统查询接口（返回JSON，不导出文件）
     */
    @GetMapping("/etl/query")
    public Map<String, Object> queryEtlLineage(
            @RequestParam String labelName,
            @RequestParam String relationshipName,
            @RequestParam String etlSystem,
            @RequestParam List<String> etlJobs,
            @RequestParam(required = false) String appName,
            @RequestParam Integer pageSize,
            @RequestParam Integer pageNo,
            @RequestParam Boolean skipTempTable,
            @RequestParam(required = false) Integer level) {

        // 验证etlJobs
        if (etlJobs == null || etlJobs.isEmpty()) {
            throw new IllegalArgumentException("etlJobs参数不能为空");
        }

        // 过滤空值
        List<String> validEtlJobs = etlJobs.stream()
                .filter(job -> job != null && !job.trim().isEmpty())
                .collect(Collectors.toList());

        if (validEtlJobs.isEmpty()) {
            throw new IllegalArgumentException("etlJobs参数不能包含空值");
        }

        // 构建查询参数
        LineageQueryParams params = new LineageQueryParams();
        params.setLabelName(labelName);
        params.setRelationshipName(relationshipName);
        params.setEtlSystem(etlSystem);
        params.setEtlJobs(validEtlJobs);
        params.setAppName(appName);
        params.setPageSize(pageSize);
        params.setPageNo(pageNo);
        params.setSkipTempTable(skipTempTable);
        params.setLevel(level != null ? level : 5); // level可为空，默认值5
        params.setQueryType("ETL");

        // 构建Cypher查询语句
        String cypher = buildEtlLineageCypher(params);
        
        // 执行查询
        List<LineageExportDTO> lineageData = executeLineageQuery(cypher, params);
        
        // 返回结果
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("data", lineageData);
        result.put("total", lineageData.size());
        result.put("pageNo", pageNo);
        result.put("pageSize", pageSize);
        result.put("queryType", "ETL系统查询");
        
        return result;
    }

    /**
     * 🎯 表级别查询接口（返回JSON，不导出文件）
     */
    @GetMapping("/table/query")
    public Map<String, Object> queryTableLineage(
            @RequestParam String labelName,
            @RequestParam String relationshipName,
            @RequestParam String dbName,
            @RequestParam String tblName,
            @RequestParam Integer pageSize,
            @RequestParam Integer pageNo,
            @RequestParam Boolean skipTempTable,
            @RequestParam Integer flag,
            @RequestParam(required = false) Integer level) {

        // 构建查询参数
        LineageQueryParams params = new LineageQueryParams();
        params.setLabelName(labelName);
        params.setRelationshipName(relationshipName);
        params.setDbName(dbName);
        params.setTblName(tblName);
        params.setPageSize(pageSize);
        params.setPageNo(pageNo);
        params.setSkipTempTable(skipTempTable);
        params.setFlag(flag);
        params.setLevel(level != null ? level : 5); // level可为空，默认值5
        params.setQueryType("TABLE");

        // 构建Cypher查询语句
        String cypher = buildTableLineageCypher(params);
        
        // 执行查询
        List<LineageExportDTO> lineageData = executeLineageQuery(cypher, params);
        
        // 返回结果
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("data", lineageData);
        result.put("total", lineageData.size());
        result.put("pageNo", pageNo);
        result.put("pageSize", pageSize);
        result.put("queryType", "表级别查询");
        
        return result;
    }

    /**
     * 🎯 字段级别查询接口（返回JSON，不导出文件）
     */
    @GetMapping("/column/query")
    public Map<String, Object> queryColumnLineage(
            @RequestParam String labelName,
            @RequestParam String relationshipName,
            @RequestParam List<String> columnIds,
            @RequestParam String dbName,
            @RequestParam String tblName,
            @RequestParam Integer pageSize,
            @RequestParam Integer pageNo,
            @RequestParam Boolean skipTempTable,
            @RequestParam Integer flag,
            @RequestParam(required = false) Integer level) {

        try {
            // 验证columnIds
            if (columnIds == null || columnIds.isEmpty()) {
                throw new IllegalArgumentException("columnIds参数不能为空");
            }

            // 过滤空值
            List<String> validColumnIds = columnIds.stream()
                    .filter(id -> id != null && !id.trim().isEmpty())
                    .collect(Collectors.toList());

            if (validColumnIds.isEmpty()) {
                throw new IllegalArgumentException("columnIds参数不能包含空值");
            }

            // 构建查询参数
            LineageQueryParams params = new LineageQueryParams();
            params.setLabelName(labelName);
            params.setRelationshipName(relationshipName);
            params.setColumnIds(validColumnIds);
            params.setDbName(dbName);
            params.setTblName(tblName);
            params.setPageSize(pageSize);
            params.setPageNo(pageNo);
            params.setSkipTempTable(skipTempTable);
            params.setFlag(flag);
            params.setLevel(level);
            params.setQueryType("COLUMN");

            // 构建Cypher查询语句
            String cypher = buildColumnLineageCypher(params);
            
            // 执行查询
            List<LineageExportDTO> lineageData = executeLineageQuery(cypher, params);
            
            // 返回结果
            Map<String, Object> result = new HashMap<>();
            result.put("success", true);
            result.put("data", lineageData);
            result.put("total", lineageData.size());
            result.put("pageNo", pageNo);
            result.put("pageSize", pageSize);
            result.put("queryType", "字段级别查询");
            result.put("columnIdsCount", validColumnIds.size());
            
            return result;
            
        } catch (IllegalArgumentException e) {
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("success", false);
            errorResult.put("message", e.getMessage());
            return errorResult;
        }
    }

    /**
     * 获取字符串值的安全方法
     */
    private String getStringValue(Record record, String key) {
        try {
            return record.get(key).asString("");
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 获取整数值的安全方法
     */
    private Integer getIntValue(Record record, String key) {
        try {
            return record.get(key).asInt(0);
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 查询参数封装类
     */
    public static class LineageQueryParams {
        private String labelName;
        private String relationshipName;
        private String dbName;
        private String tblName;
        private List<String> columnIds; // 🎯 新增：支持多个字段ID
        private String etlSystem; // ETL系统名称
        private List<String> etlJobs; // ETL作业名称列表
        private String appName; // 应用名称
        private Integer pageSize;
        private Integer pageNo;
        private Boolean skipTempTable;
        private Integer flag;
        private Integer level;
        private String queryType; // TABLE 或 COLUMN 或 ETL
        private Boolean crossTempTable; // 🎯 新增：是否跨越中间表

        // Getter和Setter方法
        public String getLabelName() { return labelName; }
        public void setLabelName(String labelName) { this.labelName = labelName; }
        
        public String getRelationshipName() { return relationshipName; }
        public void setRelationshipName(String relationshipName) { this.relationshipName = relationshipName; }
        
        public String getDbName() { return dbName; }
        public void setDbName(String dbName) { this.dbName = dbName; }
        
        public String getTblName() { return tblName; }
        public void setTblName(String tblName) { this.tblName = tblName; }
        
        public List<String> getColumnIds() { return columnIds; }
        public void setColumnIds(List<String> columnIds) { this.columnIds = columnIds; }
        
        public String getEtlSystem() { return etlSystem; }
        public void setEtlSystem(String etlSystem) { this.etlSystem = etlSystem; }
        
        public List<String> getEtlJobs() { return etlJobs; }
        public void setEtlJobs(List<String> etlJobs) { this.etlJobs = etlJobs; }
        
        public String getAppName() { return appName; }
        public void setAppName(String appName) { this.appName = appName; }
        
        public Integer getPageSize() { return pageSize; }
        public void setPageSize(Integer pageSize) { this.pageSize = pageSize; }
        
        public Integer getPageNo() { return pageNo; }
        public void setPageNo(Integer pageNo) { this.pageNo = pageNo; }
        
        public Boolean getSkipTempTable() { return skipTempTable; }
        public void setSkipTempTable(Boolean skipTempTable) { this.skipTempTable = skipTempTable; }
        
        public Integer getFlag() { return flag; }
        public void setFlag(Integer flag) { this.flag = flag; }
        
        public Integer getLevel() { return level; }
        public void setLevel(Integer level) { this.level = level; }
        
        public String getQueryType() { return queryType; }
        public void setQueryType(String queryType) { this.queryType = queryType; }
        
        public Boolean getCrossTempTable() { return crossTempTable; }
        public void setCrossTempTable(Boolean crossTempTable) { this.crossTempTable = crossTempTable; }
    }
}