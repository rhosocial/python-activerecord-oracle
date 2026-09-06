# Custom Type Adapters

## Overview

While the Oracle backend provides out-of-the-box type mapping, you may need custom type conversion logic in some scenarios.

## Pydantic Custom Types

It is recommended to use Pydantic's custom validators for custom type conversion:

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
    address: str  # Stored as JSON string (VARCHAR2/CLOB)

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

## Registering a Custom Adapter

For type conversion that must happen at the backend level, register a custom adapter with the backend's type registry:

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

## Using Custom Types

```python
# Create user
user = User(
    name='Tom',
    address=Address(street='123 Main St', city='Beijing', country='China')
)
user.save()

# Read user
user = User.query().one()
address = user.get_address()
print(address.city)  # Beijing
```

> **Oracle note**: Because Oracle treats empty strings as `NULL`, the `OracleStringAdapter` converts `NULL` back to `''` for non-`Optional[str]` fields. Keep this in mind when custom adapters interact with string columns.

## See Also

- [Custom Type Adapters](../customization/custom_adapters.md) — registering converters at the type registry level
- [Type Mapping](./mapping.md) — Oracle to Python type conversion table

💡 *AI Prompt:* "What is the difference between Pydantic's field_validator and model_validator?"

