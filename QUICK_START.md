# SQL血缘关系分析器 - 快速开始指南

## 🚀 5分钟快速上手

### 1. 环境准备

```bash
# 1. 进入项目目录
cd processor_sql

# 2. 激活虚拟环境（如果有）
.venv\Scripts\activate  # Windows
# 或 source .venv/bin/activate  # Linux/Mac

# 3. 安装依赖
pip install -r requirements.txt
```

### 2. 运行演示

```bash
# 运行完整功能演示
python src/demo.py
```

### 3. 基本用法

#### 方法1: 分析SQL字符串

```python
from src.lineage_sql_functional import lineage_analysis

# 准备SQL
sql = """
INSERT INTO target_table
SELECT col1, col2, col3
FROM source_table
WHERE condition = 'value';
"""

# 分析血缘关系
result = lineage_analysis(sql=sql, db_type='oracle')
print(result)
```

#### 方法2: 分析SQL文件

```python
# 单个文件
result = lineage_analysis(file='path/to/script.sql')

# 整个目录（递归）
result = lineage_analysis(file='path/to/sql/directory')
```

### 4. 输出示例

```sql
-- SQL血缘关系数据插入语句
INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES (NULL, NULL, NULL, NULL, 'source_table', 'col1', NULL, NULL, 'target_table', 'col1');

INSERT INTO LINEAGE_TABLE (ETL_SYSTEM, ETL_JOB, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES (NULL, NULL, NULL, NULL, 'source_table', 'col2', NULL, NULL, 'target_table', 'col2');

COMMIT;
```

### 5. 支持的数据库类型

- `'oracle'` - Oracle数据库（默认）
- `'sparksql'` - Spark SQL
- `'mysql'` - MySQL
- `'ansi'` - 标准SQL

### 6. 主要特性

✅ **自动过滤无关语句**: ALTER、USE、DROP等DDL语句会被自动跳过  
✅ **临时表识别**: 自动识别并过滤脚本中的临时表  
✅ **子查询处理**: 正确处理CTE和复杂嵌套查询  
✅ **批量处理**: 支持目录批量处理，自动递归查找SQL文件  
✅ **内存优化**: 流式处理，避免大量文件导致内存问题  

### 7. 常见问题

**Q: 如何处理包含中文注释的SQL文件？**  
A: 工具自动处理UTF-8编码，完全支持中文注释和字段名。

**Q: 分析失败怎么办？**  
A: 工具有完善的异常处理，单个SQL失败不会影响整体处理。查看控制台输出获取详细错误信息。

**Q: 如何自定义跳过的语句类型？**  
A: 修改`src/lineage_sql_functional.py`中的`DDLStatementTypes.SKIP_KEYWORDS`集合。

### 8. 获取帮助

- 查看完整文档: `README.md`
- 运行演示示例: `python src/demo.py`
- 查看测试用例: `tests/test_basic.py`

---

🎉 **恭喜！你已经学会了基本用法，现在可以开始分析你的SQL脚本了！** 