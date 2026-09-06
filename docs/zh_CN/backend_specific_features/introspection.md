# 数据库内省

## 概述

Oracle 提供内省功能来查询数据库元数据。

## 基本用法

```python
# 访问内省器
introspector = backend.introspector

# 列出表
tables = introspector.list_tables()

# 获取表信息
table_info = introspector.get_table_info("users")

# 列出列
columns = introspector.list_columns("users")
```

## 内省方法

### 数据库信息

```python
# 获取数据库信息
db_info = introspector.get_database_info()
print(f"数据库: {db_info.name}")
print(f"版本: {db_info.version}")
```

### 表信息

```python
# 列出所有表
tables = introspector.list_tables()

# 获取表详情
table_info = introspector.get_table_info("users")
print(f"行数: {table_info.row_count}")
```

### 列信息

```python
# 列出列
columns = introspector.list_columns("users")
for col in columns:
    print(f"{col.name}: {col.data_type}")
```

### 索引信息

```python
# 列出索引
indexes = introspector.list_indexes("users")
for idx in indexes:
    print(f"{idx.name}: {idx.columns}")
```

## 系统视图

```sql
-- 直接查询系统目录
SELECT * FROM ALL_TABLES;
SELECT * FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = 'USERS';
SELECT * FROM ALL_INDEXES WHERE TABLE_NAME = 'USERS';
```

## 另请参阅

- [故障排除](../troubleshooting/README.md) — 查询调试

💡 *AI 提示：* "如何列出 Oracle 数据库中的所有表？"
