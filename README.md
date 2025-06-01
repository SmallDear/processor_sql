# Processor SQL

这是一个Python项目，用于处理SQL相关的操作。

## 环境要求

- Python 3.8.3
- pip（Python包管理器）

## 项目结构

```
processor_sql/
├── src/                    # 源代码目录
│   └── __init__.py
├── tests/                  # 测试目录
│   └── __init__.py
├── requirements.txt        # 项目依赖
└── README.md              # 项目说明文档
```

## 安装

1. 创建虚拟环境（推荐）：
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# 或
.venv\Scripts\activate     # Windows
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

## 开发

- 使用 `pytest` 运行测试
- 使用 `black` 格式化代码
- 使用 `flake8` 进行代码检查

## 许可证

MIT 