/**
 * 血缘关系导出DTO - SpringBoot 2.1.7版本
 * 对应Excel导出的列结构：etlSystem, etlJob, sqlNo, appName, src_db, src_tbl, src_col, tar_db, tar_tbl, tar_col
 */
public class LineageExportDTO {
    
    private String etlSystem;
    private String etlJob;
    private Integer sqlNo;
    private String appName;
    private String srcDb;
    private String srcTbl;
    private String srcCol;
    private String tarDb;
    private String tarTbl;
    private String tarCol;
    
    // 无参构造函数
    public LineageExportDTO() {}
    
    // 全参构造函数
    public LineageExportDTO(String etlSystem, String etlJob, Integer sqlNo, String appName,
                           String srcDb, String srcTbl, String srcCol,
                           String tarDb, String tarTbl, String tarCol) {
        this.etlSystem = etlSystem;
        this.etlJob = etlJob;
        this.sqlNo = sqlNo;
        this.appName = appName;
        this.srcDb = srcDb;
        this.srcTbl = srcTbl;
        this.srcCol = srcCol;
        this.tarDb = tarDb;
        this.tarTbl = tarTbl;
        this.tarCol = tarCol;
    }
    
    // Getter和Setter方法
    public String getEtlSystem() {
        return etlSystem;
    }
    
    public void setEtlSystem(String etlSystem) {
        this.etlSystem = etlSystem;
    }
    
    public String getEtlJob() {
        return etlJob;
    }
    
    public void setEtlJob(String etlJob) {
        this.etlJob = etlJob;
    }
    
    public Integer getSqlNo() {
        return sqlNo;
    }
    
    public void setSqlNo(Integer sqlNo) {
        this.sqlNo = sqlNo;
    }
    
    public String getAppName() {
        return appName;
    }
    
    public void setAppName(String appName) {
        this.appName = appName;
    }
    
    public String getSrcDb() {
        return srcDb;
    }
    
    public void setSrcDb(String srcDb) {
        this.srcDb = srcDb;
    }
    
    public String getSrcTbl() {
        return srcTbl;
    }
    
    public void setSrcTbl(String srcTbl) {
        this.srcTbl = srcTbl;
    }
    
    public String getSrcCol() {
        return srcCol;
    }
    
    public void setSrcCol(String srcCol) {
        this.srcCol = srcCol;
    }
    
    public String getTarDb() {
        return tarDb;
    }
    
    public void setTarDb(String tarDb) {
        this.tarDb = tarDb;
    }
    
    public String getTarTbl() {
        return tarTbl;
    }
    
    public void setTarTbl(String tarTbl) {
        this.tarTbl = tarTbl;
    }
    
    public String getTarCol() {
        return tarCol;
    }
    
    public void setTarCol(String tarCol) {
        this.tarCol = tarCol;
    }
    
    @Override
    public String toString() {
        return "LineageExportDTO{" +
                "etlSystem='" + etlSystem + '\'' +
                ", etlJob='" + etlJob + '\'' +
                ", sqlNo=" + sqlNo +
                ", appName='" + appName + '\'' +
                ", srcDb='" + srcDb + '\'' +
                ", srcTbl='" + srcTbl + '\'' +
                ", srcCol='" + srcCol + '\'' +
                ", tarDb='" + tarDb + '\'' +
                ", tarTbl='" + tarTbl + '\'' +
                ", tarCol='" + tarCol + '\'' +
                '}';
    }
}