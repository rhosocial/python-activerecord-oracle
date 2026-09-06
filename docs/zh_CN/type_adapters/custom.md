# 自定义类型适配器

## 概述

虽然 Oracle 后端开箱即用地提供了类型映射，但在某些场景下您可能需要自定义类型转换逻辑。

## Pydantic 自定义类型

建议使用 Pydantic 的自定义验证器进行自定义类型转换：

```python
from typing import Any, ClassVar
from pydantic import field_validator
from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.base import FieldProxy
from rhosocial.activerecord.field import UUIDMixin, DefaultTimestampMixin
import json


class Address:
    def __init__(self, street: str, city: str, country: str):
        self.street = street
        self.city = city
        self.country = country

    def __str__(self) -> str:
        return f"{self.street}, {self.city}, {self.country}"

    @classmethod
    def from_dict(cls, data: dict) -> 'Address':
        return cls(
            street=data.get('street', ''),
            city=data.get('city', ''),
            country=data.get('country', '')
        )


class User(UUIDMixin, DefaultTimestampMixin, ActiveRecord):
    name: str
    address: str  # 存储为 JSON 字符串（VARCHAR2/CLOB）

    c: ClassVar[FieldProxy] = FieldProxy()

    @field_validator('address', mode='before')
    @classmethod
    def parse_address(cls, v: Any) -> str:
        if isinstance(v, dict):
            return json.dumps(v)
        if isinstance(v, Address):
            return json.dumps({
                'street': v.street,
                'city': v.city,
                'country': v.country
            })
        return v

    def get_address(self) -> Address:
        if isinstance(self.address, str):
            return Address.from_dict(json.loads(self.address))
        return Address.from_dict(self.address)

    @classmethod
    def table_name(cls) -> str:
        return 'users'
```

## 注册自定义适配器

对于必须在后端层面完成的类型转换，请在后端的类型注册表中注册自定义适配器：

```python
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter
from rhosocial.activerecord.backend.impl.oracle import OracleBackend


class PhoneNumberAdapter(BaseSQLTypeAdapter):
    def _do_to_database(self, value, target_type, options):
        return str(value)

    def _do_from_database(self, value, target_type, options):
        return PhoneNumber(value)


backend = OracleBackend(...)
backend.type_registry.register(PhoneNumber, PhoneNumberAdapter)
```

## 使用自定义类型

```python
# 创建用户
user = User(
    name='Tom',
    address=Address(street='123 Main St', city='Beijing', country='China')
)
user.save()

# 读取用户
user = User.query().one()
address = user.get_address()
print(address.city)  # Beijing
```

> **Oracle 注意**：由于 Oracle 将空字符串视为 `NULL`，`OracleStringAdapter` 会将非 `Optional[str]` 字段的 `NULL` 转换回 `''`。当自定义适配器与字符串列交互时，请记住这一点。

## 另请参阅

- [自定义类型适配器](../customization/custom_adapters.md) — 在类型注册表层注册转换器
- [类型映射](./mapping.md) — Oracle 到 Python 的类型转换表

💡 *AI Prompt:* "Pydantic 的 field_validator 和 model_validator 有什么区别？"