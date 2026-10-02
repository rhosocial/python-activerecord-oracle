# rhosocial-activerecord Oracle 后端文档

Oracle 后端是 [rhosocial-activerecord](https://github.com/rhosocial/python-activerecord)
的 Oracle Database 后端实现。它使用 `oracledb` 驱动。在 Oracle 上，本库的 `schema_name`
对应的是对象的**属主**，标识符折为大写，且未取别名的范围的列引用为三段。

## 目录 (Table of Contents)

- **[Schema 命名空间](oracle_specific_features/schema_namespace.md)**：声明
  `__schema_name__`、schema 即属主这一规则、标识符折大写、三段式列引用、
  `SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')`，以及同义词的限定位置

## 关键结论速览

| 问题 | 结论 |
|---|---|
| `schema_name` 指什么？ | 对象的属主 |
| 限定表渲染为 | `"APP"."ORDERS"`，标识符折为大写 |
| 列引用 | 未取别名的范围为三段：`"APP"."ORDERS"."ID"` |
| 取别名之后 | `"O"."ID"`；Oracle 的表别名之前没有 `AS` |
| 当前 schema | `SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL`，`FROM DUAL` 不可省略 |
| `CREATE SCHEMA` / `DROP SCHEMA` | 不支持；schema 随用户创建 |
| 同义词 | `CREATE SYNONYM s FOR t`，schema 限定的是目标表而非同义词本身 |

## 相关文档

- **[Schema 命名空间（核心库指南）](https://github.com/rhosocial/python-activerecord/tree/docs/docs/modeling/schema_namespace.md)**：
  所有后端共同遵循的、与方言无关的规则

---

> ⚠️ **依赖说明**：本后端依赖核心库 `rhosocial-activerecord`，请与核心库一并安装，
> 不要单独安装本后端。