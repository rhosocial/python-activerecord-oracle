# EXPLAIN 支持

## 概述

Oracle 提供 EXPLAIN PLAN 用于查询执行计划分析。

## EXPLAIN PLAN

```sql
-- 生成执行计划
EXPLAIN PLAN FOR SELECT * FROM users WHERE name = 'John';

-- 显示计划
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

## DBMS_XPLAN

Oracle 提供 DBMS_XPLAN 用于详细计划分析：

```sql
-- 显示带统计信息的计划
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'ALLSTATS'));

-- 显示游标的计划
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR);
```

## 自动工作负载仓库 (AWR)

Oracle AWR 提供性能洞察：

```sql
-- 生成 AWR 报告
SELECT * FROM TABLE(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(1, 100));
```

## 另请参阅

- [故障排除](../troubleshooting/performance.md) — 性能优化

💡 *AI 提示：* "如何在 Oracle 中分析查询性能？"
