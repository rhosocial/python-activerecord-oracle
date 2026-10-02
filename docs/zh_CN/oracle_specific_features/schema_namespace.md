# docs/zh_CN/oracle_specific_features/schema_namespace.md

# Oracle Schema 命名空间

> 本文只讲本后端特有的部分：`schema_name` 在这里指向什么、标识符为什么折成大写、
> 带限定的引用为什么是三段而不是两段、Oracle 为什么不在表别名前写 `AS`、不加限定的
> 名字落在哪个命名空间上、同义词的 schema 限定的是哪一头，以及本方言的 `format_table`
> 额外渲染了哪些子句。
>
> 模型层的通用部分——怎么在模型上声明 `__schema_name__`、schema 何时进入 SQL、
> DDL 的边界、各后端支持矩阵——由核心库（`python-activerecord` 仓库）的
> `docs/modeling/schema_namespace.md` 讲，见
> [`docs/zh_CN/modeling/schema_namespace.md`][core-zh]。

[core-zh]: https://github.com/rhosocial/python-activerecord/tree/main/docs/zh_CN/modeling/schema_namespace.md

## 本文结论的验证方式

文中每一段 SQL 都由 `OracleDialect(version=(19, 0, 0))` 配合表达式层渲染得出，
没有连接真实服务端：

```
PYTHONPATH=src .venv3.14-ubuntu26.04/bin/python
```

对应的版本是 `rhosocial-activerecord` 1.0.0.dev30 与
`rhosocial-activerecord-oracle` 1.0.0.dev3。

渲染出的语句都是一整行。本文为了阅读把较长的语句折了行，折行不属于输出的一部分。

描述服务端而非渲染器的部分——`CURRENT_SCHEMA` 与 `SESSION_USER` 的关系、`FROM`
子句为何必需、同义词的目标——来自 Oracle 自身文档化的行为，本仓库没有在真实实例上
验证过。凡属此类内容均在正文中标明；因缺少实例而无法确认的每一点，都在所在位置写明
未验证。

## `schema_name` 在这里指向什么

`OracleDialect` 实现了核心库的 `SchemaSupport` 协议，凡是核心库期待出现
`schema_name` 的地方都能接受。至于这个值**指向什么**，按 Oracle 自己的定义来：
**一个 schema，而 schema 就是拥有它的用户**。每个 schema 恰有一个属主，且以属主的
名字命名。`AR_XCRM."USERS"` 是名为 `USERS`、属主为数据库用户 `AR_XCRM` 的表；没有
任何办法指向一个不是用户的 schema。

这是从 PostgreSQL 迁移过来时最需要留意的一处差别。PostgreSQL 上 schema 是 database
内部一个独立的对象，由 `CREATE SCHEMA` 创建，多个用户都可以在其中拥有对象。Oracle 上
这两个概念是同一个：建 schema 就是建用户，删 schema 就是 `DROP USER ... CASCADE`。

下面这张表说的是两个服务端自身文档化的行为，不是框架行为；框架行为在其后。

| | PostgreSQL | Oracle |
|---|---|---|
| schema 是什么 | database 内的独立对象 | 属主用户 |
| 如何创建 | `CREATE SCHEMA ar_crm` | `CREATE USER ar_crm IDENTIFIED BY ...` |
| 如何删除 | `DROP SCHEMA ar_crm` | `DROP USER ar_crm CASCADE` |
| 一个 schema 多个用户 | 可以，用户可被授予其上的权限 | 不行，schema 与用户一一对应 |

schema 这一组能力标志因此多数为 `False`：

```python
dialect.supports_schema()                # True
dialect.supports_create_schema()         # False
dialect.supports_drop_schema()           # False
dialect.supports_schema_if_not_exists()  # False
dialect.supports_schema_if_exists()      # False
dialect.supports_schema_cascade()        # False
dialect.supports_schema_authorization()  # False
```

`supports_schema()` 回答的是名字能否被命名空间**限定**，在 Oracle 上可以。剩下几个
标志回答的是命名空间能否由 schema DDL 创建与删除，而答案是不能：两条语句都会抛出
异常，而不是渲染。

```python
CreateSchemaExpression(d, "ar_crm").to_sql()[0]
# UnsupportedFeatureError: 'Oracle' dialect does not support CREATE SCHEMA.
#   Suggestion: Oracle does not support CREATE SCHEMA.

DropSchemaExpression(d, "ar_crm").to_sql()[0]
# UnsupportedFeatureError: 'Oracle' dialect does not support DROP SCHEMA.
#   Suggestion: Oracle does not support DROP SCHEMA.
```

### 限定名如何渲染

两个双引号标识符，中间一个点：

| 表达式 | SQL |
|---|---|
| `TableExpression(d, "users", schema_name="APP")` | `"APP"."USERS"` |
| `TableExpression(d, "users")` | `"USERS"` |
| `TableExpression(d, "orders", schema_name="APP", alias="o")` | `"APP"."ORDERS" "O"` |

第三行里别名前没有 `AS`。Oracle 的 `SELECT` 参考文档写明 `AS` 对*列*别名是可选的，
对*表*关联名则没有这样的表述，这正是本方言只用一个空格的原因。手写 SQL——迁移脚本
或视图定义里——同样遵守这条规则。

### 加引号的方式

`format_identifier` 先把值折成大写，把值里自带的双引号写成两个，再用双引号包起来。
本该提前闭合引号的字符因此仍留在同一个标识符内部：

```python
TableExpression(d, "orders", schema_name='a"b').to_sql()[0]
# "A""B"."ORDERS"

TableExpression(d, "my orders", schema_name="app").to_sql()[0]
# "APP"."MY ORDERS"
```

保留字和其他名字一样加引号：

```python
TableExpression(d, "user", schema_name="app").to_sql()[0]
# "APP"."USER"
```

每一段各自加引号，因此值里的点号不构成分隔符，见[常见错误](#常见错误)。

## 在模型上声明

```python
from typing import ClassVar, Optional

class Order(ActiveRecord):
    __table_name__ = "orders"
    __schema_name__ = "app"                # -> "APP"."ORDERS"
    c: ClassVar[FieldProxy] = FieldProxy()

    id: Optional[int] = None
    customer_id: Optional[int] = None
    total: Optional[float] = None
```

`__schema_name__` 可选，不写就是 `None`，即不加限定，由连接决定落在哪里。不设它的
模型渲染为裸名：

```python
class PlainOrder(ActiveRecord):
    __table_name__ = "plain_orders"

PlainOrder.query().select(PlainOrder.c.id).to_sql()[0]
# SELECT "PLAIN_ORDERS"."ID" FROM "PLAIN_ORDERS"
```

一旦设上，模型构造出的每一条语句都带着这个命名空间，其中的列引用也带着，见
[列引用是三段](#列引用是三段)。

命名空间只读一次，入口是 `schema_name()`，随每个列表达式构造时一并带下去。事后再改
`__schema_name__`，已经建好的表达式不会跟着变。这条绑定规则在核心库文档里有完整说明。

`__table_name__` 与 `__schema_name__` 刻意分开，这一条在大写折叠之下更明显：把它们
手工合成一个字符串写成 `__table_name__ = "app.orders"`，那个点会被引号收进同一个
标识符里，见[常见错误](#常见错误)。

## 标识符折为大写

这是影响面最广的一条行为，也最容易让人以为生成的 SQL 出了问题。`format_identifier`
在加引号之前先折大写，因此小写声明与大写声明渲染出完全相同的 SQL：

```python
class Order(ActiveRecord):
    __table_name__ = "orders"
    __schema_name__ = "ar_xcrm"     # 声明时用的小写

Order.query().select(Order.c.id).to_sql()[0]
# SELECT "AR_XCRM"."ORDERS"."ID" FROM "AR_XCRM"."ORDERS"
```

渲染出的 SQL 里不含 `ar_xcrm` 这个串，含的是 `AR_XCRM`。表名、别名，以及本方言加引号
的每一个标识符都是如此。

**代价。** 用小写声明是可以用的，语句也能执行，因为服务端不加引号的
`CREATE USER ar_xcrm` 同样会把名字存成大写的 `AR_XCRM`——服务端对不加引号的名字折大写
的规则与渲染器一致。因此这个折叠是自洽的，能对上。

**需要留意的地方。** 生成的 SQL 与你写的文本不再一致，有三处会受影响：

- **读日志与执行计划。** 每一条都是大写。拿日志与模型定义比对时，要按折叠规则比对，
  而不是按字面完全匹配。
- **对 SQL 做断言。** 去找 `"ar_xcrm"` 的测试会在正确的输出上失败。要匹配折叠后的形式。
- **跨后端比较。** 同一个模型在 PostgreSQL 上渲染成 `"ar_xcrm"."orders"`，在这里渲染成
  `"AR_XCRM"."ORDERS"`。两个后端共用的测试必须按各自产出的形态来写。

**什么情况下会失效。** 用引号创建、或用混合大小写创建的名字不会折叠。
`CREATE USER "Ar_Xcrm"` 把名字原样存成 `Ar_Xcrm`，生成 SQL 里的 `"AR_XCRM"` 并不指向
它，语句会以 ORA-00942（表或视图不存在）失败。

`format_identifier` 带一个 `need_quote` 参数，传 `False` 时原样返回该值——不折大写，
也不加引号。`TableExpression` 的每一段都有自己的开关，两段都关掉就会按原样渲染：

```python
TableExpression(
    d, "orders", schema_name="App",
    name_need_quote=False, schema_need_quote=False,
).to_sql()[0]
# App.orders
```

此时保留字会以不加引号的形式送出去，方言会发出 `IdentifierQuotingWarning`，而不是
无声通过。只在名字确实是混合大小写时才这样用。

## 列引用是三段

Oracle 接受 `schema.table.column`，本后端也照此产出。不带别名的范围会把 schema 一路
带到列引用上：

```python
Order.query().select(Order.c.id).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS"

Order.query().where(Order.c.id > 1).to_sql()[0]
# SELECT * FROM "SHOP"."ORDERS" WHERE "SHOP"."ORDERS"."ID" > ?

Order.query().group_by(Order.c.customer_id).having(Order.c.total > 10).to_sql()[0]
# SELECT * FROM "SHOP"."ORDERS" GROUP BY "SHOP"."ORDERS"."CUSTOMER_ID" HAVING "SHOP"."ORDERS"."TOTAL" > ?
```

`ORDER BY`、`GROUP BY` 与 `HAVING` 遵从与 `SELECT` 相同的规则：schema 落在范围上，
也落在对它的每一次引用上。

这是 Oracle 与 SQL Server 差别最显眼的地方：后者的方言会把 schema 丢掉，渲染成
`[orders].[id]`。两种形式对各自的服务端都是正确的，写给一个后端的文档不能引用另一个
后端的示例。

Oracle 自己对三段式有一条约束：列能否带 schema 限定，取决于 `FROM` 里的范围是否带
**同一个** schema 限定。查询层构造的任何语句都天然满足，因为两个位置拿到的是同一个值。
手工拼的查询可以把两者拆开，而本后端不会替你修正：

```python
QueryExpression(
    dialect=d,
    select=[Column(d, "id", table="orders", schema_name="app")],
    from_=TableExpression(d, "orders"),            # 范围上没有 schema
).to_sql()[0]
# SELECT "APP"."ORDERS"."ID" FROM "ORDERS"       -- 两处对不上
```

另外两条边界仍然生效。只带 schema 而不带表的列会被拒绝，因为没有可用来解析前缀的
对象：

```
ValueError: Oracle: cannot qualify column 'id' with schema 'APP' because no table
was given; a column reference needs a table (or an alias) to be
schema-qualified
```

手工构造的通配符仍然渲染成三段式，因为 `WildcardExpression` 由核心层的渲染器格式化，
本方言没有覆盖它：

```python
WildcardExpression(d, table="orders", schema_name="app").to_sql()[0]
# "APP"."ORDERS".*
```

框架构造的查询不会走到这个形态——查询层创建的每一个通配符都是裸的。渲染结果已实测，
服务端的回答未经验证；本仓库没有可用的 Oracle 实例。

## 别名

取了别名的范围只能用别名标识，别名本身和其他标识符一样加引号并折大写。前面没有 `AS`：

```python
Order.query().join(
    Customer, on=Order.c.customer_id == Customer.c.with_table_alias("u").id, alias="u"
).select(Order.c.id, Customer.c.with_table_alias("u").name).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID", "U"."NAME" FROM "SHOP"."ORDERS"
#   JOIN "CRM"."CUSTOMERS" "U" ON "SHOP"."ORDERS"."CUSTOMER_ID" = "U"."ID"
```

范围上的 schema 保留不变，变的只是列的前缀。去掉 schema 这一步发生在查询层：
`FieldProxy` 构造列表达式时，只要表别名生效，就把该列的 `schema_name` 置为 `None`。

因为这道保护在 `FieldProxy` 里而不在渲染器里，手工构造的 `Column` 会绕过它，产出
Oracle 拒绝的 SQL：

```python
QueryExpression(
    dialect=d,
    select=[Column(d, "id", table="orders", schema_name="app")],
    from_=TableExpression(d, "orders", schema_name="app", alias="o"),
).to_sql()[0]
# SELECT "APP"."ORDERS"."ID" FROM "APP"."ORDERS" "O"   -- "O" 盖住了范围
```

范围带别名时，请通过 `Model.c.<field>` 而不是 `Column` 构造列表达式。

### 两个同名的范围

两个模型绑定到不同属主下的同一张表名时，会渲染出不同的三段前缀，而 Oracle 能把它们
分辨开：

```python
ShopOrder.query().join(CrmOrder, on=ShopOrder.c.user_id == CrmOrder.c.id).to_sql()[0]
# SELECT * FROM "SHOP"."ORDERS" JOIN "CRM"."ORDERS"
#   ON "SHOP"."ORDERS"."USER_ID" = "CRM"."ORDERS"."ID"
```

Oracle 把 `"SHOP"."ORDERS"` 与 `"CRM"."ORDERS"` 解析到两个不同的属主，两个前缀指向两个
不同的列，框架在这里没有任何可标记之处，因此不需要关联名。不过若这条语句要被人反复
阅读，`"SHOP"."ORDERS"."USER_ID"` 比 `"S"."USER_ID"` 更费注意力时，还是给一个关联名。

## 集合操作

`UNION`、`INTERSECT` 与 `MINUS` 本身不指称任何对象，没有可限定的东西。每个分支各自
带着自己的属主。注意 `except_()` 渲染为 `MINUS`，这是 Oracle 对该运算符的拼写：

```python
Order.query().select(Order.c.id).intersect(Customer.query().select(Customer.c.id)).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS"
#   INTERSECT SELECT "CRM"."CUSTOMERS"."ID" FROM "CRM"."CUSTOMERS"

Order.query().select(Order.c.id).except_(Customer.query().select(Customer.c.id)).to_sql()[0]
# SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS"
#   MINUS SELECT "CRM"."CUSTOMERS"."ID" FROM "CRM"."CUSTOMERS"
```

## CTE

CTE 的名字是给同一查询里后续语句用的，不属于 database，因此它自己的名字不加限定。
它内部的查询仍然带着模型的 schema：

```python
from rhosocial.activerecord.query.cte_query import CTEQuery

CTEQuery(backend).with_cte(
    "recent_orders", Order.query().select(Order.c.id)
).from_cte("recent_orders").select("id").to_sql()[0]
# WITH "RECENT_ORDERS" AS (SELECT "SHOP"."ORDERS"."ID" FROM "SHOP"."ORDERS")
#   SELECT "ID" FROM "RECENT_ORDERS"
```

CTE 的名字与其他标识符一样折成大写。给它加限定会让 Oracle 去找该 schema 下名为
`RECENT_ORDERS` 的表，而那张表并不存在。

## DDL 单独传 schema

`__schema_name__` 决定的是读写的命名空间，构建 DDL 时**不读**它——迁移脚本要自己说
明它指的是哪个 schema。凡是涉及带 schema 对象的语句都接受自己的 `schema_name`，不必
再手工拼限定名。下面每个名字都与其他标识符一样折大写。

```python
DropTableExpression(d, TableExpression(d, "orders", schema_name="app")).to_sql()[0]
# DROP TABLE "APP"."ORDERS"

TruncateExpression(d, "orders", schema_name="app").to_sql()[0]
# TRUNCATE TABLE "APP"."ORDERS"

CreateIndexExpression(d, "idx_orders_id", "orders", ["id"], schema_name="app").to_sql()[0]
# CREATE INDEX "APP"."IDX_ORDERS_ID" ON "APP"."ORDERS" ("ID")

DropIndexExpression(d, "idx_orders_id", schema_name="app").to_sql()[0]
# DROP INDEX "APP"."IDX_ORDERS_ID"

CreateSequenceExpression(d, "seq_orders", schema_name="app").to_sql()[0]
# CREATE SEQUENCE "APP"."SEQ_ORDERS" NOCYCLE NOORDER

DropSequenceExpression(d, "seq_orders", schema_name="app").to_sql()[0]
# DROP SEQUENCE "APP"."SEQ_ORDERS"

DropTypeExpression(d, "t_addr", schema_name="app").to_sql()[0]
# DROP TYPE "APP"."T_ADDR"
```

`CREATE INDEX` 只有一个 `schema_name`，同时覆盖两个名字：索引与它所依附的表落在同一个
属主下。Oracle 要求如此——索引必须与它的表建在同一个 schema 里。

`DROP TABLE` 有一处行为值得在第一次迁移前知道。传 `if_exists=True` 既不抛异常，渲染里
也不带守卫条件，语句就是裸的：

```python
dialect.supports_if_exists_table()                                  # False
DropTableExpression(d, TableExpression(d, "o", schema_name="app"), if_exists=True).to_sql()[0]
# DROP TABLE "APP"."O"
```

`DROP INDEX` 对同一个标志的处理方式不同，它会抛 `UnsupportedFeatureError`。要只在表存在
时删除它，可以在迁移里查 `ALL_TABLES`，或者把这条 drop 放进吞掉 ORA-00942 的 PL/SQL
块里——本后端自己的跨 schema 测试用的就是这个写法，见
`tests/rhosocial/activerecord_oracle_test/feature/backend/test_cross_schema.py`。

DML 的限定方式相同，传入带限定的 `TableExpression` 即可：

```python
InsertExpression(d, TableExpression(d, "users", schema_name="app"), source,
                 columns=["id", "name"]).to_sql()[0]
# INSERT INTO "APP"."USERS" ("ID", "NAME") VALUES (?, ?)

UpdateExpression(d, TableExpression(d, "users", schema_name="app"),
                 {"name": value}).to_sql()[0]
# UPDATE "APP"."USERS" SET "NAME" = ?

DeleteExpression(d, TableExpression(d, "users", schema_name="app")).to_sql()[0]
# DELETE FROM "APP"."USERS"
```

软删除的 `restore()` 会针对模型的范围重新构造一条 `UPDATE`，因此与 `delete()` 同样
带着命名空间。否则 restore 会在连接用户 schema 下的同名表上清掉 `deleted_at`，而目标
那一行仍处于软删除状态。

### 同义词限定的是目标，不是自己

`OracleCreateSynonymExpression` 把同义词的名字与目标的名字作为两个参数分别接收，它的
`schema_name` 限定的是**目标**：

```python
OracleCreateSynonymExpression(d, "s_users", "users", schema_name="app").to_sql()[0]
# CREATE SYNONYM "S_USERS" FOR "APP"."USERS"

OracleCreateSynonymExpression(d, "s_users", "users").to_sql()[0]
# CREATE SYNONYM "S_USERS" FOR "USERS"

OracleCreateSynonymExpression(d, "s_users", "users", schema_name="app", public=True).to_sql()[0]
# CREATE PUBLIC SYNONYM "S_USERS" FOR "APP"."USERS"
```

`schema_name` 不会渲染在同义词自己的名字前面，也不存在能产出
`CREATE SYNONYM "APP"."S_USERS"` 的表达式。这不是缺口：同义词建在连接用户的 schema
里，除非显式指定 `PUBLIC`；同义词的名字不是 `schema_name` 能选定的对象。它指向的是
同义词所解析到的那个对象，而不是同义词本身。

### 表引用上的 `@dblink` 与 flashback

本方言覆盖了 `format_table`，追加两个核心表达式并不携带的子句。两者都是表表达式上的
属性：

```python
from rhosocial.activerecord.backend.impl.oracle.expression.flashback import (
    OracleAsOfClause, OracleAsOfMode,
)

t = TableExpression(d, "orders", schema_name="app", alias="o")
t.dblink = "remotedb"
t.to_sql()[0]
# "APP"."ORDERS"@"REMOTEDB" "O"

t2 = TableExpression(d, "orders", schema_name="app")
t2.flashback = OracleAsOfClause(d, OracleAsOfMode.TIMESTAMP,
                                "SYSTIMESTAMP - INTERVAL '1' DAY")
t2.to_sql()[0]
# "APP"."ORDERS" AS OF TIMESTAMP SYSTIMESTAMP - INTERVAL '1' DAY
```

三个都带上时，顺序是固定的：schema 与名字、`@dblink`、flashback 子句、别名。

```python
t3 = TableExpression(d, "orders", schema_name="app", alias="o")
t3.dblink = "dl"
t3.flashback = OracleAsOfClause(d, OracleAsOfMode.SCN, 12345)
t3.to_sql()[0]
# "APP"."ORDERS"@"DL" AS OF SCN 12345 "O"
```

`dblink` 的名字与其他标识符一样折大写，`dl` 变成 `"DL"`。

有两点需要留意。`TableExpression` 没有 `dblink` 或 `flashback` 构造参数——只能在构造
之后赋值，而不带这两个属性的表达式渲染时就没有相应子句。另外，自动生成的能力协议把
`format_table` 声明成接受 `dblink` 与 `flashback` 关键字参数，而实现只接收一个表达式；
用这些关键字调用会得到 `TypeError`。

## 不加限定的名字落在哪个 schema

Oracle 把不加限定的名字解析到会话的*当前 schema* 上。它在连接建立时等于会话认证所用的
用户，并且可以在会话余下的时间里更改：

```sql
ALTER SESSION SET CURRENT_SCHEMA = ar_xcrm
```

Oracle 自己的文档写明，这条语句改变当前 schema，但**不**改变会话用户或当前用户，也不给
会话带来任何额外的系统权限或对象权限。两个值因此会分开：执行之后
`SYS_CONTEXT('USERENV','CURRENT_SCHEMA')` 读到 `AR_XCRM`，而
`SYS_CONTEXT('USERENV','SESSION_USER')` 仍然是登录的那个用户。`get_session_info()`
会一并读取两者，所以这种分离在那里是可见的。这是 Oracle 文档化的行为，不是框架行为，
此处也没有在真实实例上验证。

后端通过下面这个方法读取当前 schema：

```python
backend.get_current_schema()      # AsyncOracleBackend 需要 await
```

它渲染出

```
SELECT SYS_CONTEXT(?, ?) FROM "DUAL"
```

两个函数实参以参数形式携带，而不是拼进 SQL 字符串。`OracleBackend._convert_placeholders_to_oracle`
在送往驱动前把 `?` 改写成 `:1, :2`，因此服务端收到的就是熟悉的形式：

```sql
SELECT SYS_CONTEXT(:1, :2) FROM "DUAL"
```

绑定值为 `'USERENV'` 与 `'CURRENT_SCHEMA'`。

`FROM DUAL` 是这条语句成为合法查询块的原因。`DUAL` 是一张单行的表，给单个值表达式一个
可选择的来源——Oracle 自己的文档里 `SELECT SYSDATE FROM DUAL` 成为标准写法也是同一个
理由。Oracle Database 23ai 让 `FROM` 子句对简单表达式变成可选，但只限该版本，且只限
不涉及表、连接与子查询的语句；后端仍然输出 `FROM DUAL`，因为它要在所有受支持版本上
工作。在 23ai 之前，只有 `SELECT SYS_CONTEXT(...)` 的语句会被拒绝，这也是后端用
`TableExpression(dialect, "DUAL")` 构造 `QueryExpression`、而不是拼接字符串的原因。

连接层面没有可设置的位置。`OracleConnectionConfig` 没有任何与 schema 相关的字段，既没有
`search_path` 的对应项，也没有会话语句的钩子；上面那条 `ALTER SESSION` 只能通过
`backend.execute(...)` 手工下发，而且只作用于 `connect()` 打开的那一条连接。因此命名
空间要么通过在偏离连接用户的模型上声明 `__schema_name__` 来选择，要么由服务端改。

内省的范围划分方式相同，绑定时也会折叠属主：

```sql
SELECT column_name, data_type, ... FROM all_tab_columns
WHERE owner = ? AND table_name = ? ORDER BY column_id
```

`format_column_info_query` 在下发前把两个绑定值都折成大写，因此以 `ar_xcrm` 给出的属主
是与存储的 `AR_XCRM` 比较的。

## 空串，以及它在哪一步被拦下

`""` 是笔误，不表示「不加限定」——那才是 `None` 的含义。空串会被拒绝，但**不是在构造
表达式的时候**：那一刻表达式只收集参数，方言甚至可能还没定下来，严格校验因此推迟到
渲染阶段，那时语句才算是完整的。失败比预期来得晚：

```python
class Bad(ActiveRecord):
    __table_name__ = "empties"
    __schema_name__ = ""

Bad.schema_name()                        # ''           -- 未报错
Bad.c.id                                 # Column       -- 未报错
Bad.query()                              # ActiveQuery  -- 未报错
Bad.query().select(Bad.c.id)             # ActiveQuery  -- 未报错
Bad.query().select(Bad.c.id).to_sql()    # ValueError   -- 到这里才报错
```

报错信息会指明是哪个表达式：

```
ValueError: Column.schema_name must be a non-empty string; use None for an
unqualified reference
```

```
ValueError: TableExpression.schema_name must be a non-empty string; use None for
an unqualified reference
```

纯空白的字符串与空串同样被拒绝——校验先去掉空白，因此 `"   "` 也会被拒绝。非字符串值
另有一条报错信息：

```
ValueError: TableExpression.schema_name must be a string or None, not int
```

之所以拒绝而不是把 `""` 当作没传，是因为 `format_table` 用
`bool(expr.schema_name)` 判断是否限定，空串为假，于是走不加限定的分支。明明要求
`"APP"."ORDERS"` 的调用方拿到的是 `"ORDERS"`——不报错、不告警，连受影响行数都看不出
异常。在 Oracle 上那就是连接用户 schema 里的表，语句照样执行，数据写到了别处。

## 常见错误

**`__table_name__` 里的点号不是命名空间。** 整串会被当作一个标识符加引号，然后折成
大写：

```python
class User(ActiveRecord):
    __table_name__ = "app.users"

User.query().select(User.c.id).to_sql()[0]
# SELECT "APP.USERS"."ID" FROM "APP.USERS"    -- 一张名字就叫 APP.USERS 的表
```

要限定就设 `__schema_name__`，或者给需要限定的语句传入带限定的 `TableExpression`。

**`__schema_name__` 里的点号同样不是命名空间。** 每一段各自加引号，点号留在段内：

```python
TableExpression(d, "orders", schema_name="app.public").to_sql()[0]
# "APP.PUBLIC"."ORDERS"     -- 一个名字就叫 APP.PUBLIC 的用户
```

**在日志或断言里找 `"ar_xcrm"`。** 渲染出的 SQL 写的是 `"AR_XCRM"`。这是折叠规则在
起作用，不是命名空间变了，见[标识符折为大写](#标识符折为大写)。

**用混合大小写创建用户。** `CREATE USER "Ar_Xcrm"` 存下的名字渲染器产不出来，因为每个
加引号的标识符都会折成大写。创建用户时不要加引号，否则这个框架无法访问它。

**在表别名前写 `AS`。** 本方言只输出一个空格，而 Oracle 的 `SELECT` 参考文档只把
`AS` 的可选性授予列别名。手写 SQL——迁移脚本或视图定义——同样遵守这条规则。

**对着带别名或不带限定的范围手工构造 `Column`。** 去掉 schema 这一步发生在
`FieldProxy` 而不是渲染器，因此 `Column(d, "id", table="orders", schema_name="app")`
在 `FROM "APP"."ORDERS" "O"` 之下会产出 Oracle 拒绝的 SQL。请走
`Model.c.<field>`。

**指望构造时报错。** 在语句渲染出来之前，没有任何环节会拒绝不合法的 `schema_name`。
模型上的错误因此能一路存活到查询构建完成的那一刻，在拼装 SQL 时才失败。

**去用 `CREATE SCHEMA` 或 `DROP SCHEMA`。** 两者都会抛 `UnsupportedFeatureError`。
用 `CREATE USER` 开通命名空间，用 `DROP USER ... CASCADE` 回收。

**把 `SESSION_USER` 当成 schema 读。** 刚建立连接时两者一致，执行
`ALTER SESSION SET CURRENT_SCHEMA` 之后就会分开。`get_session_info()` 同时报告两者，
`get_current_schema()` 只报告用于解析名字的那一个。

**指望 `DROP TABLE` 的 `if_exists` 渲染出守卫条件。** 它会被接受然后丢弃，而
`DROP INDEX` 会直接抛异常，见[DDL 单独传 schema](#ddl-单独传-schema)。

## 建议的分层方式

让连接用户承担常规场景，`__schema_name__` 只留给例外：

- **只有一个属主** —— 干脆不设 `__schema_name__`。不加限定的名字能让 DML、DDL 与
  内省保持一致，也不必去推理带限定的范围。
- **多个属主** —— 只在偏离连接用户的模型上设 `__schema_name__`。例外面越小，踩中上面
  这些错误的机会越少。
- **跨属主连接** —— 各自限定自己的范围，无需额外配置：

  ```python
  Order.query().join(
      Customer, on=Order.c.customer_id == Customer.c.id
  ).select(Order.c.id, Customer.c.name)
  # SELECT "SHOP"."ORDERS"."ID", "CRM"."CUSTOMERS"."NAME" FROM "SHOP"."ORDERS"
  #   JOIN "CRM"."CUSTOMERS" ON "SHOP"."ORDERS"."CUSTOMER_ID" = "CRM"."CUSTOMERS"."ID"
  ```

  由于 schema 就是用户，跨属主的连接需要两侧表上的 `SELECT` 权限，由属主授予。
  `__schema_name__` 只限定名字，不授予任何权限。