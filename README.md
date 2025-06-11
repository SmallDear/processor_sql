# ETL血缘关系解析系统

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-production-brightgreen.svg)]()

> 🚀 **专业的ETL脚本血缘关系分析工具** - 自动解析SQL脚本中的字段级血缘关系，支持批量处理、异常恢复和数据库直接入库

## 🎯 解决的核心痛点

### 1. **数据血缘关系难以追踪**
- ❌ **传统痛点**: 数千个ETL脚本，血缘关系靠人工梳理，耗时且易错
- ✅ **我们的解决方案**: 自动解析字段级血缘关系，一键生成完整血缘图谱

### 2. **批量处理性能瓶颈**
- ❌ **传统痛点**: 处理大量文件时内存溢出，或者需要手动逐个处理
- ✅ **我们的解决方案**: 流式处理+智能内存管理，支持万级文件批量处理

### 3. **异常处理和重试机制缺失**
- ❌ **传统痛点**: 处理过程中出现异常，整体任务失败，需要重新全量处理
- ✅ **我们的解决方案**: 完善的异常日志+失败文件重处理机制，确保处理完整性

### 4. **ETL元数据管理混乱**
- ❌ **传统痛点**: ETL系统名、作业名、SQL路径等元数据缺失或不统一
- ✅ **我们的解决方案**: 智能路径解析，自动提取ETL元数据并标准化入库

### 5. **复杂SQL语法支持不足**
- ❌ **传统痛点**: 子查询、临时表、CTE等复杂语法解析错误
- ✅ **我们的解决方案**: 基于sqllineage的专业解析引擎，支持各种复杂SQL语法

## 🔥 核心功能特性

### 📊 智能血缘关系分析
- **字段级精度**: 精确到每个字段的血缘关系追踪
- **多SQL语句**: 自动拆分复杂脚本，支持批量SQL处理
- **复杂语法**: 完美支持子查询、CTE、临时表、JOIN等复杂场景
- **数据库兼容**: 支持Oracle、Spark SQL、Hive、MySQL等主流数据库

### 🛠️ 企业级处理能力
- **三种输入方式**: SQL字符串、单文件、目录批量处理
- **智能DDL过滤**: 自动跳过ALTER、DROP、USE等无血缘意义语句
- **USE语句处理**: 智能处理USE语句，维护默认数据库状态
- **临时表标记**: 自动识别临时表和子查询表，添加特殊标记

### 🔄 完善的异常恢复机制
- **失败日志记录**: 自动记录解析失败的文件和错误信息
- **失败文件提取**: 从日志中智能提取失败文件路径
- **批量重处理**: 专门的重处理工具，支持失败文件批量重试
- **错误隔离**: 单个文件失败不影响整体处理进度

### 📁 智能路径管理
- **ETL信息自动提取**: 从文件路径自动解析ETL_SYSTEM、ETL_JOB等信息
- **路径标准化**: 支持Windows和Linux路径格式
- **递归目录处理**: 自动查找.sql、.hql等各种SQL文件

## 🚀 快速开始

### 安装与配置

```bash
# 1. 克隆项目
git clone <repository_url>
cd processor_sql

# 2. 安装依赖
pip install -r requirements.txt

# 3. 快速测试
python src/demo.py
```

### 基础用法示例

#### 方式1: SQL字符串分析
```python
from src.lineage_sql_with_database import lineage_analysis

# 分析SQL字符串
sql_content = """
USE production_db;
INSERT INTO sales_summary
SELECT 
    customer_id,
    SUM(amount) as total_amount,
    COUNT(*) as order_count
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'completed';
"""

result = lineage_analysis(sql=sql_content, db_type='oracle')
print(result)
```

#### 方式2: 单文件处理
```python
# 处理单个SQL文件
result = lineage_analysis(file='/path/to/etl_script.hql', db_type='sparksql')
```

#### 方式3: 批量目录处理
```python
# 批量处理整个ETL脚本目录
result = lineage_analysis(file='/data/etl_scripts/', db_type='sparksql')
```

## 🔧 完整操作流程

### 步骤1: 初始批量解析
```python
from src.lineage_sql_with_database import lineage_analysis

# 处理整个ETL脚本目录
result = lineage_analysis(file='/data/etl_scripts/', db_type='sparksql')

# 生成的Oracle INSERT语句可直接执行入库
with open('lineage_insert.sql', 'w') as f:
    f.write(result)
```

### 步骤2: 处理解析异常
系统在解析过程中遇到异常时，会自动将失败信息记录到 `err_log` 文件中，包括：
- 失败的SQL脚本路径
- 具体的错误信息
- 处理时间戳

### 步骤3: 提取失败文件路径
```python
from src.batch_reprocess_failed_files import read_failed_file_logs

# 从异常日志中提取失败文件路径（自动去重）
read_failed_file_logs('err_log.txt', 'failed_files_list.txt')
```

### 步骤4: 批量重新解析
```bash
# 命令行方式重新处理失败文件
python src/batch_reprocess_failed_files.py failed_files_list.txt
```

或程序调用方式：
```python
from src.batch_reprocess_failed_files import batch_reprocess_files

result = batch_reprocess_files('failed_files_list.txt')
```

## 📄 输出格式示例

### Oracle INSERT语句格式
```sql
-- 第一步：删除现有数据（基于ETL_SYSTEM和ETL_JOB）
DELETE FROM LINEAGE_TABLE WHERE ETL_SYSTEM = 'F-DD_00001' AND ETL_JOB = 'sales_etl.hql';

-- 第二步：批量插入新数据
INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SQL_PATH, SQL_NO, SOURCE_DATABASE, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_TABLE, TARGET_COLUMN)
VALUES ('F-DD_00001', 'sales_etl.hql', '/data/hql/F-DD_00001/sales_etl.hql', 1, 'production_db', 'orders', 'customer_id', 'production_db', 'sales_summary', 'customer_id');

INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SQL_PATH, SQL_NO, SOURCE_DATABASE, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_TARGET, TARGET_COLUMN)
VALUES ('F-DD_00001', 'sales_etl.hql', '/data/hql/F-DD_00001/sales_etl.hql', 1, 'production_db', 'orders', 'amount', 'production_db', 'sales_summary', 'total_amount');

-- 第三步：提交事务
COMMIT;
```

### 支持的数据库表结构
```sql
CREATE TABLE LINEAGE_TABLE (
    ETL_SYSTEM VARCHAR2(100),      -- ETL系统名称（从文件夹名提取：F-DD_00001）
    ETL_JOB VARCHAR2(100),         -- ETL作业名称（文件名：sales_etl.hql）
    SQL_PATH VARCHAR2(500),        -- SQL文件完整路径
    SQL_NO NUMBER,                 -- SQL语句编号（脚本中的第几条SQL）
    SOURCE_DATABASE VARCHAR2(100), -- 源数据库名
    SOURCE_TABLE VARCHAR2(100),    -- 源表名（临时表带_TEMP_TBL后缀）
    SOURCE_COLUMN VARCHAR2(100),   -- 源字段名
    TARGET_DATABASE VARCHAR2(100), -- 目标数据库名
    TARGET_TABLE VARCHAR2(100),    -- 目标表名
    TARGET_COLUMN VARCHAR2(100)    -- 目标字段名
);
```

## 🔧 高级特性

### 临时表和子查询标记
```python
# 系统自动为不同类型的表添加标记
source_table_temp = "temp_data_TEMP_TBL"      # 临时表标记
subquery_table = "sub_query_SUBQRY_TBL"      # 子查询表标记
virtual_db = "<SUBQUERY_DB>"                 # 子查询虚拟数据库
```

### DDL语句智能跳过
```python
# 自动跳过的语句类型
SKIP_KEYWORDS = [
    'ALTER', 'DROP', 'GRANT', 'REVOKE', 'SET', 'SHOW', 
    'DESCRIBE', 'DESC', 'EXPLAIN', 'ANALYZE', 'TRUNCATE',
    'CREATE DATABASE', 'CREATE INDEX', 'CREATE FUNCTION'
]

# 但会解析有血缘关系的CREATE语句
"CREATE TABLE AS SELECT ..." # ✅ 会解析
"CREATE TABLE (...)" # ❌ 跳过（纯定义语句）
```

### 路径解析智能化
```python
# 从文件路径自动解析ETL信息
file_path = "D:/etl/hql/F-DD_00001/customer_analysis.hql"
base_path = "D:/etl/hql"

# 自动解析结果：
etl_system = "F-DD_00001"           # 文件夹名作为系统名
etl_job = "customer_analysis.hql"   # 文件名作为作业名
appname = "F"                       # 系统名按下划线分割的前缀
```

## 📋 项目结构

```
processor_sql/
├── src/
│   ├── lineage_sql_with_database.py      # 🔥 主解析模块（推荐使用）
│   ├── batch_reprocess_failed_files.py   # 🔄 失败文件重处理工具
│   ├── lineage_sql_functional.py         # 函数式版本（轻量）
│   └── lineage_sql.py                    # 类版本（兼容性）
├── tests/                                # 测试用例
├── requirements.txt                      # 项目依赖
├── README.md                            # 本文档
├── QUICK_START.md                       # 快速开始指南
├── ETL血缘关系解析系统使用说明.md          # 详细使用说明
└── failed_files_list.txt               # 失败文件列表（示例）
```

## 🔧 支持的数据库类型

| 数据库类型 | 参数值 | 适用场景 |
|-----------|--------|----------|
| Oracle | `oracle` | Oracle数据库脚本 |
| Spark SQL | `sparksql` | Spark、Databricks脚本 |
| Hive | `hive` | Hive脚本 |
| MySQL | `mysql` | MySQL脚本 |
| 通用SQL | `non-validating` | 特殊语法或FROM开头语句 |

## ⚡ 性能优化

### 内存管理
- **流式处理**: 逐文件处理，避免大量文件同时加载
- **智能缓存**: 临时表信息缓存，避免重复计算
- **垃圾回收**: 及时释放已处理文件的内存

### 处理效率
- **并发支持**: 支持多进程并发处理（规划中）
- **增量更新**: 基于ETL_SYSTEM和ETL_JOB的精确更新
- **批量写入**: 生成批量INSERT语句，减少数据库交互

## 🐛 常见问题解决

### Q1: 处理包含中文注释的SQL文件失败？
**A**: 系统默认使用UTF-8编码，确保文件保存为UTF-8格式。

### Q2: 某些复杂SQL解析失败？
**A**: 尝试使用不同的db_type参数，或使用`non-validating`通用方言。

### Q3: 批量处理时部分文件失败？
**A**: 查看`err_log`文件获取详细错误信息，使用重处理工具处理失败文件。

### Q4: 生成的INSERT语句如何执行？
**A**: 直接复制到Oracle客户端执行，或保存为.sql文件批量执行。

## 📈 版本规划

- **v1.0** ✅ 基础SQL解析和血缘关系提取
- **v1.1** ✅ 批量处理和异常恢复机制
- **v1.2** ✅ USE语句支持和默认数据库处理
- **v1.3** 🚧 多进程并发处理
- **v1.4** 📋 Web界面和可视化血缘图

## 🤝 贡献指南

欢迎提交Issue和Pull Request！

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

---

<div align="center">

**🎉 让ETL血缘关系分析变得简单高效！**

如果这个项目对你有帮助，请给个⭐️支持一下！

</div>

# SQL注释和参数清理工具

专门用于清理Hive、SparkSQL脚本中的注释和参数，使其能够被sqllineage正确解析血缘关系。

## 功能特性

✅ **删除所有注释格式**
- 单行注释：`--` 
- 井号注释：`#`, `####`, `#############`
- 多行注释：`/* ... */`

✅ **删除所有参数**
- 普通参数：`${param}`
- 字符串内参数：`'文本${param}文本'` → `'文本文本'`
- 表名参数：`${database}.${table}` → `.`

✅ **特殊处理IN语句**
- `WHERE col IN ${param}` → `WHERE col IN ()`
- `AND col in ${param}` → `AND col IN ()`

✅ **保持SQL语法完整性**
- 清理连续逗号
- 处理空括号
- 保持基本SQL结构

## 文件说明

- `sql_comment_remover.py` - 完整版，包含详细测试用例
- `sql_cleaner.py` - 简化版，方便实际使用
- `test_input.sql` - 测试输入文件
- `test_output.sql` - 清理后的输出文件

## 使用方法

### 1. 命令行文件处理

```bash
# 处理文件，输出到新文件
python sql_cleaner.py input.sql output.sql

# 直接覆盖原文件
python sql_cleaner.py input.sql
```

### 2. 交互模式

```bash
python sql_cleaner.py -i
```

然后输入SQL，以 `END` 结束或按 Ctrl+D。

### 3. Python代码中使用

```python
from sql_cleaner import clean_sql

# 清理SQL字符串
original_sql = """
-- 注释
SELECT col, '${param}' FROM ${db}.${table} 
WHERE id IN ${list} -- 另一个注释
"""

cleaned_sql = clean_sql(original_sql)
print(cleaned_sql)
# 输出: SELECT col, '' FROM . WHERE id IN ()
```

## 处理示例

### 原始SQL
```sql
-- 业务查询
############# 数据统计 #############
SELECT 
    customer_id,
    '状态：${status}' as desc, -- 状态描述
    amount * ${rate} as amount_usd
FROM ${schema}.${table} 
WHERE region IN ${regions} /* 区域过滤 */
    AND date >= '${start_date}'
ORDER BY ${sort_col}
```

### 清理后SQL
```sql
SELECT
customer_id,
'状态：' as desc,
amount * as amount_usd
FROM .
WHERE region IN ()
AND date >= ''
ORDER BY
```

## 验证功能

运行完整测试：
```bash
python sql_comment_remover.py
```

快速验证：
```bash
python sql_cleaner.py
```

## 注意事项

1. **参数完全删除**：所有 `${param}` 都会被删除，包括字符串内的
2. **IN语句特殊处理**：`IN ${param}` 会被替换为 `IN ()` 保持语法完整
3. **表名处理**：`${db}.${table}` 变成 `.`，但不影响血缘分析
4. **编码支持**：默认使用UTF-8编码处理中文

## 适用场景

- **Hive SQL脚本清理**
- **SparkSQL脚本预处理** 
- **血缘关系分析前的SQL标准化**
- **SQL脚本去注释和去参数化**

## 技术细节

使用正则表达式按照以下顺序处理：
1. 特殊处理 `IN ${param}` → `IN ()`
2. 删除所有参数 `${...}`
3. 删除单行注释 `--`、`#`
4. 删除多行注释 `/* */`
5. 清理多余空白和符号

确保正则之间不冲突，保持SQL语法完整性。 