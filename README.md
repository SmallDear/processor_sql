# SQL血缘关系分析处理器 (Processor SQL)

## 项目概述

这是一个专业的SQL血缘关系分析工具，基于sqllineage库开发，能够从复杂的SQL脚本中自动提取表与字段之间的血缘关系，并生成标准化的Oracle INSERT语句格式输出。该工具特别适用于数据仓库、数据治理和数据血缘分析场景。

## 核心功能

### 🔍 智能SQL解析
- **多SQL语句支持**: 自动拆分复杂SQL脚本，支持多条SQL语句的批量处理
- **DDL语句过滤**: 智能识别并跳过ALTER、USE、DROP、CREATE等无血缘关系意义的语句
- **多数据库兼容**: 支持Oracle、Spark SQL、MySQL、ANSI SQL等多种数据库方言
- **临时表识别**: 智能识别并过滤临时表，确保血缘关系的准确性

### 📊 血缘关系提取
- **字段级血缘**: 提供细粒度的字段级血缘关系分析
- **表级血缘**: 同时支持表级血缘关系提取
- **子查询处理**: 正确处理复杂的子查询和嵌套查询结构
- **CTE支持**: 正确处理WITH子句（公共表表达式）

### 🎯 结果输出
- **Oracle INSERT格式**: 生成标准的Oracle INSERT语句，便于数据入库
- **ETL信息追踪**: 包含etl_system（文件夹名）、etl_job（文件名）、sql_path（脚本路径）、sql_no（SQL序号）信息
- **批量文件处理**: 支持单个文件或目录批量处理
- **内存优化**: 流式处理，避免大量文件同时加载导致的内存问题

### 📁 多种输入方式
- **SQL字符串输入**: 直接传入SQL脚本内容进行分析
- **文件路径输入**: 支持单个SQL文件或包含SQL文件的目录
- **递归目录处理**: 自动递归查找目录下的所有.sql、.hql文件

## 项目结构

```
processor_sql/
├── src/                          # 源代码目录
│   ├── __init__.py              # 包初始化文件 (版本: 0.1.0)
│   ├── lineage_sql.py           # 核心血缘分析处理器（类方式）
│   ├── lineage_sql_functional.py # 优化的函数式血缘分析处理器（推荐）
│   └── demo.py                  # 功能演示和使用示例
├── tests/                       # 测试目录
│   ├── __init__.py              # 测试包初始化
│   └── test_basic.py            # 基础测试用例
├── .venv/                       # Python虚拟环境
├── .idea/                       # IDE配置文件
├── .git/                        # Git版本控制
├── requirements.txt             # 项目依赖
├── .gitignore                   # Git忽略文件
├── README.md                    # 项目说明文档
└── QUICK_START.md               # 快速开始指南
```

## 技术栈

### 核心依赖
- **sqllineage (1.5.4)**: SQL血缘关系分析核心库
- **sqlparse (>=0.4.4)**: SQL解析工具
- **networkx (>=2.8.8)**: 图形网络分析库

### 开发工具
- **pytest (7.3.1)**: 单元测试框架
- **black (22.3.0)**: 代码格式化工具
- **flake8 (6.0.0)**: 代码质量检查工具
- **python-dotenv (1.0.0)**: 环境变量管理

### 系统要求
- **Python**: >= 3.8.3

## 核心架构

### 函数式接口（推荐使用）

项目提供了两个版本的实现：
- `lineage_sql.py`: 面向对象的类方式实现
- `lineage_sql_functional.py`: 优化的函数式实现（推荐）

函数式版本具有更好的性能和更低的内存占用，特别适合处理大量SQL文件。

#### 🔧 核心函数

```python
def lineage_analysis(sql=None, file=None, db_type='oracle')
```
- **功能**: 主要的血缘分析接口函数
- **参数**:
  - `sql`: SQL脚本内容字符串
  - `file`: SQL文件路径（单个文件或目录）
  - `db_type`: 数据库类型，支持'oracle', 'sparksql', 'mysql', 'ansi'等
- **返回**: Oracle INSERT语句格式的血缘关系数据

```python
def extract_temp_tables_from_script(sql_script)
```
- **功能**: 从SQL脚本中提取临时表
- **逻辑**: 识别既有CREATE又有DROP的表作为临时表

```python
def is_ddl_or_control_statement(sql_statement)
```
- **功能**: 智能检测DDL和控制语句
- **特点**: 跳过ALTER、USE、DROP、CREATE等无血缘关系意义的语句

```python
def process_sql_script(sql_script, etl_system='', etl_job='', sql_path='', db_type='oracle')
```
- **功能**: 处理完整SQL脚本的核心方法
- **输出**: Oracle INSERT语句格式的血缘关系数据

### DDL语句检测

项目包含智能的DDL语句检测机制：

```python
class DDLStatementTypes:
    """不需要解析血缘关系的语句类型"""
    
    SKIP_KEYWORDS = frozenset([
        'ALTER', 'USE', 'DROP', 'CREATE', 'GRANT', 'REVOKE',
        'COMMENT', 'TRUNCATE', 'DELETE', 'SET', 'SHOW',
        'DESCRIBE', 'DESC', 'EXPLAIN', 'ANALYZE'
    ])
```

## 使用示例

### 1. SQL字符串分析

```python
from src.lineage_sql_functional import lineage_analysis

# SQL字符串分析
sql_content = """
INSERT INTO target_table 
SELECT 
    source_col1,
    source_col2,
    CASE WHEN condition THEN source_col3 ELSE source_col4 END as processed_col
FROM source_table 
WHERE condition = 'value';
"""

# 分析血缘关系
result = lineage_analysis(sql=sql_content, db_type='oracle')
print(result)
```

### 2. 单个文件分析

```python
# 单个SQL文件分析
result = lineage_analysis(file='path/to/your/script.sql', db_type='oracle')
print(result)
```

### 3. 目录批量分析

```python
# 目录批量分析（递归处理所有.sql和.hql文件）
result = lineage_analysis(file='path/to/sql/directory', db_type='sparksql')
print(result)
```

### 4. 复杂SQL脚本处理

```python
complex_sql = """
-- 创建临时表
CREATE TEMPORARY TABLE temp_data AS 
SELECT * FROM raw_table WHERE status = 'active';

-- 数据处理
INSERT INTO final_table
SELECT 
    t1.col1,
    t2.col2,
    COALESCE(t1.col3, t2.col3) as merged_col
FROM temp_data t1
JOIN dimension_table t2 ON t1.id = t2.id;

-- 删除临时表
DROP TABLE temp_data;
"""

# 处理复杂脚本
result = lineage_analysis(sql=complex_sql, db_type='sparksql')
```

## 输出格式

### Oracle INSERT语句示例

```sql
-- SQL血缘关系数据插入语句
-- 表结构: ETL_SYSTEM, ETL_JOB, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN

INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES ('my_etl_system', 'data_processing_job', NULL, NULL, 'source_table', 'source_col1', NULL, NULL, 'target_table', 'source_col1');

INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES ('my_etl_system', 'data_processing_job', NULL, NULL, 'source_table', 'source_col2', NULL, NULL, 'target_table', 'source_col2');

COMMIT;
```

### 字段说明

| 字段名 | 描述 | 示例 |
|--------|------|------|
| ETL_SYSTEM | 文件夹名（ETL系统名） | 'data_warehouse' |
| ETL_JOB | 文件名（ETL作业名） | 'daily_etl_process' |
| SOURCE_DATABASE | 源数据库名 | 'prod_db' 或 NULL |
| SOURCE_SCHEMA | 源模式名 | 'raw_data' 或 NULL |
| SOURCE_TABLE | 源表名 | 'customer_info' |
| SOURCE_COLUMN | 源字段名 | 'customer_id' |
| TARGET_DATABASE | 目标数据库名 | 'dwh_db' 或 NULL |
| TARGET_SCHEMA | 目标模式名 | 'dim' 或 NULL |
| TARGET_TABLE | 目标表名 | 'dim_customer' |
| TARGET_COLUMN | 目标字段名 | 'customer_key' |

## 安装和运行

### 环境准备

1. **克隆项目**
```bash
git clone <repository-url>
cd processor_sql
```

2. **创建虚拟环境**
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/Mac
source .venv/bin/activate
```

3. **安装依赖**
```bash
pip install -r requirements.txt
```

### 快速开始

```bash
# 运行功能演示
python src/demo.py

# 运行主要接口示例
python src/lineage_sql_functional.py

# 运行测试
pytest tests/
```

## 性能特性

### 🚀 性能优化
- **函数式设计**: 避免类实例化开销，提高处理速度
- **流式处理**: 逐个文件处理，避免内存爆炸
- **智能过滤**: 自动跳过无意义的DDL语句，减少处理时间
- **临时表缓存**: 临时表识别结果缓存，提高重复处理效率

### 🛡️ 鲁棒性
- **异常处理**: 完善的异常处理机制，单个SQL失败不影响整体处理
- **容错设计**: 对格式不规范的SQL具有良好的容错性
- **编码支持**: 自动处理UTF-8编码，支持中文注释和字段名

### 🔄 可扩展性
- **模块化设计**: 核心功能模块化，便于定制和集成
- **多数据库支持**: 轻松扩展支持更多数据库类型
- **标准接口**: 统一的输入输出接口，便于集成到其他系统

## 应用场景

### 📈 数据治理
- **血缘追踪**: 追踪数据从源系统到目标系统的完整流转路径
- **影响分析**: 分析表结构变更对下游系统的影响范围
- **数据质量**: 识别数据处理链路中的质量风险点
- **合规审计**: 满足数据合规和审计要求

### 🏗️ 数据仓库建设
- **ETL流程分析**: 分析复杂ETL脚本的数据依赖关系
- **表关系梳理**: 自动梳理数据仓库中表与表之间的关系
- **文档生成**: 自动生成数据血缘关系文档
- **架构优化**: 识别数据处理瓶颈和优化机会

### 🔍 开发运维
- **代码审查**: 自动分析SQL代码的数据依赖关系
- **迁移规划**: 支持系统迁移时的依赖关系分析
- **监控告警**: 集成到监控系统，实时跟踪数据流向

## 技术特色

### 智能临时表识别
```python
# 自动识别脚本中的临时表
CREATE TEMPORARY TABLE temp_sales AS SELECT...;  -- 被识别为临时表
INSERT INTO final_table SELECT * FROM temp_sales;  -- temp_sales被过滤
DROP TABLE temp_sales;  -- 确认为临时表
```

### 精确的子查询处理
```python
# 正确处理CTE和子查询
WITH monthly_sales AS (
    SELECT customer_id, SUM(amount) as total
    FROM sales 
    GROUP BY customer_id
)
SELECT c.name, ms.total  -- 正确识别血缘关系
FROM customers c
JOIN monthly_sales ms ON c.id = ms.customer_id;
```

### DDL语句智能过滤
```python
USE database_name;           -- 被跳过
CREATE INDEX idx_name...;    -- 被跳过  
ALTER TABLE table_name...;   -- 被跳过
INSERT INTO target...;       -- 正常处理
```

## 常见问题

### Q: 如何处理大量SQL文件导致的内存问题？
A: 使用函数式版本的`lineage_analysis`函数，它采用流式处理，逐个文件处理而不是一次性加载所有文件。

### Q: 支持哪些数据库方言？
A: 支持Oracle、Spark SQL、MySQL、ANSI SQL等，通过`db_type`参数指定。

### Q: 如何处理复杂的嵌套查询？
A: 工具基于sqllineage库，能够自动解析复杂的嵌套查询、CTE和子查询结构。

### Q: 如何自定义需要跳过的SQL语句类型？
A: 可以修改`DDLStatementTypes.SKIP_KEYWORDS`集合来自定义跳过的语句关键字。

## 贡献指南

欢迎提交Issue和Pull Request来改进这个项目！

### 开发规范
- 遵循PEP 8代码规范
- 使用black进行代码格式化
- 编写完整的单元测试
- 添加详细的文档说明

### 测试
```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/test_basic.py

# 生成测试覆盖率报告
pytest --cov=src
```

## 许可证

本项目采用MIT许可证，详见LICENSE文件。

## 更新日志

### v0.1.0 (当前版本)
- ✅ 基础SQL血缘关系分析功能
- ✅ 临时表自动识别和过滤
- ✅ 多数据库方言支持
- ✅ Oracle INSERT格式输出
- ✅ ETL信息字段支持（etl_system, etl_job, sql_path, sql_no）
- ✅ DDL语句智能检测和跳过
- ✅ 函数式接口设计，优化内存使用
- ✅ 批量文件处理支持
- ✅ 完善的异常处理机制

## 联系方式

如有问题或建议，请通过以下方式联系：
- 提交Issue: [GitHub Issues](链接待添加)
- 邮箱: [待添加]

---

**注意**: 本工具基于sqllineage库开发，适用于大多数标准SQL语法。对于特殊的数据库特性或非标准语法，可能需要额外的适配和优化。 