# SQL血缘关系分析处理器 (Processor SQL)

## 项目概述

这是一个专业的SQL血缘关系分析工具，能够从复杂的SQL脚本中自动提取表与字段之间的血缘关系，并生成标准化的Oracle INSERT语句格式输出。该工具特别适用于数据仓库、数据治理和数据血缘分析场景。

## 核心功能

### 🔍 智能SQL解析
- **多SQL语句支持**: 自动拆分复杂SQL脚本，支持多条SQL语句的批量处理
- **多数据库兼容**: 支持Oracle、Spark SQL、MySQL等多种数据库方言
- **临时表识别**: 智能识别并过滤临时表，确保血缘关系的准确性

### 📊 血缘关系提取
- **字段级血缘**: 提供细粒度的字段级血缘关系分析
- **表级血缘**: 同时支持表级血缘关系提取
- **子查询处理**: 正确处理复杂的子查询和嵌套查询结构

### 🎯 结果输出
- **Oracle INSERT格式**: 生成标准的Oracle INSERT语句，便于数据入库
- **血缘关系摘要**: 提供详细的血缘关系统计和摘要信息
- **可视化支持**: 基于Cytoscape格式的数据处理，支持图形化展示

## 项目结构

```
processor_sql/
├── src/                          # 源代码目录
│   ├── __init__.py              # 包初始化文件 (版本: 0.1.0)
│   ├── lineage_sql.py           # 核心血缘分析处理器
│   └── test.py                  # 简单测试脚本
├── tests/                       # 测试目录
│   ├── __init__.py              # 测试包初始化
│   └── test_basic.py            # 基础测试用例
├── .venv/                       # Python虚拟环境
├── .idea/                       # IDE配置文件
├── .git/                        # Git版本控制
├── requirements.txt             # 项目依赖
├── .gitignore                   # Git忽略文件
└── README.md                    # 项目说明文档
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

## 核心类介绍

### OptimizedSQLLineageProcessor

主要的血缘关系处理器类，提供以下核心方法：

#### 🔧 核心方法

```python
def extract_temp_tables_from_script(self, sql_script)
```
- **功能**: 从SQL脚本中提取临时表
- **逻辑**: 识别既有CREATE又有DROP的表作为临时表

```python
def split_sql_statements(self, sql_script)
```
- **功能**: 智能拆分SQL脚本为独立的SQL语句
- **特点**: 使用sqllineage的split方法，确保拆分准确性

```python
def process_cytoscape_lineage(self, cytoscape_data)
```
- **功能**: 处理Cytoscape格式的血缘数据
- **特点**: 自动过滤子查询和临时表

```python
def process_sql_script(self, sql_script, db_type='oracle')
```
- **功能**: 处理完整SQL脚本的主入口方法
- **输出**: Oracle INSERT语句格式的血缘关系数据

## 使用示例

### 基础使用

```python
from src.lineage_sql import OptimizedSQLLineageProcessor

# 创建处理器实例
processor = OptimizedSQLLineageProcessor()

# 准备SQL脚本
sql_script = """
INSERT INTO target_table 
SELECT 
    source_col1,
    source_col2,
    CASE WHEN condition THEN source_col3 ELSE source_col4 END
FROM source_table 
WHERE condition = 'value';
"""

# 处理SQL脚本
oracle_inserts = processor.process_sql_script(sql_script, db_type='oracle')

# 打印血缘关系摘要
processor.print_lineage_summary()

# 输出Oracle INSERT语句
print(oracle_inserts)
```

### 高级用法

```python
# 支持复杂SQL脚本处理
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
result = processor.process_sql_script(complex_sql, db_type='sparksql')
```

## 输出格式

### Oracle INSERT语句示例

```sql
-- SQL血缘关系数据插入语句
-- 表结构: SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN

INSERT INTO LINEAGE_TABLE (SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES (NULL, NULL, 'source_table', 'source_col1', NULL, NULL, 'target_table', 'source_col1');

INSERT INTO LINEAGE_TABLE (SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, TARGET_COLUMN)
VALUES (NULL, NULL, 'source_table', 'source_col2', NULL, NULL, 'target_table', 'source_col2');

COMMIT;
```

### 血缘关系摘要示例

```
=== 血缘关系摘要 (共4条) ===
涉及源表: 2 个
  - source_table
  - dimension_table

涉及目标表: 1 个
  - target_table

血缘关系:
  1. source_table.col1 -> target_table.col1
  2. source_table.col2 -> target_table.col2
  3. source_table.col3 -> target_table.merged_col
  4. dimension_table.col3 -> target_table.merged_col
```

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

### 运行示例

```bash
# 运行主程序示例
python src/lineage_sql.py

# 运行测试
pytest tests/
```

## 特性优势

### 🚀 性能优化
- **批量处理**: 支持大型SQL脚本的批量分析
- **智能缓存**: 临时表识别结果缓存，提高处理效率
- **错误恢复**: 单条SQL失败不影响整体处理流程

### 🛡️ 鲁棒性
- **异常处理**: 完善的异常处理机制
- **容错设计**: 对格式不规范的SQL具有良好的容错性
- **日志输出**: 详细的处理过程日志，便于调试

### 🔄 可扩展性
- **插件架构**: 易于扩展支持更多数据库类型
- **模块化设计**: 核心功能模块化，便于定制和集成
- **接口统一**: 标准化的输入输出接口

## 应用场景

### 📈 数据治理
- **血缘追踪**: 追踪数据从源系统到目标系统的完整流转路径
- **影响分析**: 分析表结构变更对下游系统的影响范围
- **数据质量**: 识别数据处理链路中的质量风险点

### 🏗️ 数据仓库建设
- **ETL流程分析**: 分析复杂ETL脚本的数据依赖关系
- **表关系梳理**: 自动梳理数据仓库中表与表之间的关系
- **文档生成**: 自动生成数据血缘关系文档

### 🔍 合规审计
- **数据流向追踪**: 满足数据合规和审计要求
- **敏感数据识别**: 识别敏感数据的使用和流转情况
- **权限分析**: 分析数据访问权限的合理性

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
- ✅ 血缘关系摘要统计

## 联系方式

如有问题或建议，请通过以下方式联系：
- 提交Issue: [GitHub Issues](链接待添加)
- 邮箱: [待添加]

---

**注意**: 本工具仍在持续开发中，欢迎反馈使用体验和改进建议！ 