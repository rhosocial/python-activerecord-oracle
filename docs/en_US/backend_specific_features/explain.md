# EXPLAIN Support

## Overview

Oracle provides EXPLAIN PLAN for query execution plan analysis.

## EXPLAIN PLAN

```sql
-- Generate execution plan
EXPLAIN PLAN FOR SELECT * FROM users WHERE name = 'John';

-- Display the plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
```

## DBMS_XPLAN

Oracle provides DBMS_XPLAN for detailed plan analysis:

```sql
-- Display plan with statistics
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'ALLSTATS'));

-- Display plan with output
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR);
```

## Automatic Workload Repository (AWR)

Oracle AWR provides performance insights:

```sql
-- Generate AWR report
SELECT * FROM TABLE(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML(1, 100));
```

## See Also

- [Troubleshooting](../troubleshooting/performance.md) — Performance optimization

💡 *AI Prompt:* "How to analyze query performance in Oracle?"
