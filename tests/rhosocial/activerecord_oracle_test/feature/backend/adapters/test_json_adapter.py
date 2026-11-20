# tests/rhosocial/activerecord_oracle_test/feature/backend/adapters/test_json_adapter.py
"""Database-free coverage for ``OracleJSONAdapter``'s serialisation hooks."""

import json
from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.impl.oracle.adapters import OracleJSONAdapter


class _Tagged:
    """Marker returned by the sample object hook."""

    def __init__(self, type_name, payload):
        self.type_name = type_name
        self.payload = payload


def _object_hook(mapping):
    if "__type__" in mapping:
        return _Tagged(mapping["__type__"], mapping.get("payload"))
    return mapping


class _Lob:
    """Minimal stand-in for an oracledb LOB returned for a JSON column."""

    def __init__(self, text):
        self.text = text

    def read(self):
        return self.text


class _NativeJSONObject(dict):
    """Stand-in for an ``oracledb`` 21c+ native JSON object."""

    def __init__(self, mapping):
        super().__init__(mapping)


class TestJSONAdapterNoHooks:
    def test_constructor_defaults(self):
        adapter = OracleJSONAdapter()
        assert adapter.default is None
        assert adapter.object_hook is None

    def test_supported_types_unchanged(self):
        adapter = OracleJSONAdapter()
        assert adapter.supported_types == {dict: {str}, list: {str}}

    def test_to_database_matches_json_dumps(self):
        adapter = OracleJSONAdapter()
        payload = {"key": "value", "number": 123}
        assert adapter.to_database(payload, str) == json.dumps(payload)
        assert adapter.to_database([1, 2, 3], str) == "[1, 2, 3]"

    def test_to_database_keeps_ascii_escaping(self):
        adapter = OracleJSONAdapter()
        payload = {"title": "\u4e2d\u6587"}
        assert adapter.to_database(payload, str) == json.dumps(payload)

    def test_to_database_none(self):
        assert OracleJSONAdapter().to_database(None, str) is None

    def test_from_database_string_is_parsed(self):
        adapter = OracleJSONAdapter()
        assert adapter.from_database('{"key": "value"}', dict) == {"key": "value"}
        assert adapter.from_database("[1, 2, 3]", list) == [1, 2, 3]

    def test_from_database_str_target_keeps_json_text(self):
        adapter = OracleJSONAdapter()
        payload = {"key": "value"}
        text = json.dumps(payload, ensure_ascii=False)
        assert adapter.from_database(text, str) == text

    def test_from_database_plain_dict_and_list(self):
        adapter = OracleJSONAdapter()
        payload = {"a": 1, "nested": {"b": 2}}
        decoded_list = [1, "two", {"three": 3}]
        assert adapter.from_database(payload, dict) == payload
        assert adapter.from_database(decoded_list, list) == decoded_list

    def test_from_database_invalid_json_returns_raw_value(self):
        adapter = OracleJSONAdapter()
        assert adapter.from_database("not json", dict) == "not json"

    def test_unserialisable_value_raises_without_default(self):
        adapter = OracleJSONAdapter()
        with pytest.raises(TypeError):
            adapter.to_database({"amount": Decimal("1.5")}, str)


class TestJSONAdapterDefaultHook:
    def test_default_serialises_unsupported_objects(self):
        adapter = OracleJSONAdapter(default=str)
        assert adapter.to_database({"amount": Decimal("1.5")}, str) == '{"amount": "1.5"}'

    def test_default_accepts_arbitrary_object(self):
        adapter = OracleJSONAdapter(default=lambda value: "<unserialisable>")
        encoded = json.loads(adapter.to_database({"when": object()}, str))
        assert encoded == {"when": "<unserialisable>"}

    def test_default_applies_to_native_json_str_serialisation(self):
        adapter = OracleJSONAdapter(default=str)
        native = _NativeJSONObject({"amount": Decimal("1.5")})
        assert adapter.from_database(native, str) == '{"amount": "1.5"}'

    def test_default_none_preserves_native_str_serialisation(self):
        adapter = OracleJSONAdapter()
        native = _NativeJSONObject({"key": "value"})
        assert adapter.from_database(native, str) == json.dumps(
            {"key": "value"}, ensure_ascii=False
        )


class TestJSONAdapterObjectHook:
    def test_hook_runs_over_json_string(self):
        adapter = OracleJSONAdapter(object_hook=_object_hook)
        decoded = adapter.from_database('{"__type__": "user", "payload": {"name": "ann"}}', dict)
        assert isinstance(decoded, _Tagged)
        assert decoded.type_name == "user"
        assert decoded.payload == {"name": "ann"}

    def test_hook_runs_over_lob(self):
        adapter = OracleJSONAdapter(object_hook=_object_hook)
        decoded = adapter.from_database(_Lob('{"__type__": "user", "payload": 1}'), dict)
        assert isinstance(decoded, _Tagged)
        assert decoded.payload == 1

    def test_hook_runs_over_already_decoded_dict(self):
        adapter = OracleJSONAdapter(object_hook=_object_hook)
        decoded = adapter.from_database({"__type__": "user", "payload": 1}, dict)
        assert isinstance(decoded, _Tagged)

    def test_hook_runs_over_already_decoded_list(self):
        adapter = OracleJSONAdapter(object_hook=_object_hook)
        decoded = adapter.from_database([{"__type__": "a", "payload": 1}, {"b": 2}], list)
        assert isinstance(decoded[0], _Tagged)
        assert decoded[1] == {"b": 2}

    def test_hook_runs_over_native_json_object(self):
        adapter = OracleJSONAdapter(object_hook=_object_hook)
        decoded = adapter.from_database(_NativeJSONObject({"__type__": "user", "payload": 1}), dict)
        assert isinstance(decoded, _Tagged)

    def test_hook_runs_over_nested_objects(self):
        adapter = OracleJSONAdapter(object_hook=_object_hook)
        decoded = adapter.from_database(
            '{"outer": {"__type__": "inner", "payload": 1}, "plain": 2}', dict
        )
        assert decoded["plain"] == 2
        assert isinstance(decoded["outer"], _Tagged)

    def test_hook_is_not_applied_for_str_target(self):
        adapter = OracleJSONAdapter(object_hook=_object_hook)
        text = '{"__type__": "user"}'
        assert adapter.from_database(text, str) == text
        assert adapter.from_database({"__type__": "user"}, str) == '{"__type__": "user"}'

    def test_plain_decode_without_hook_matches_dict(self):
        adapter = OracleJSONAdapter()
        assert adapter.from_database('{"__type__": "user"}', dict) == {"__type__": "user"}

    def test_default_and_object_hook_together(self):
        adapter = OracleJSONAdapter(default=str, object_hook=_object_hook)
        assert adapter.to_database({"amount": Decimal("1.5")}, str) == '{"amount": "1.5"}'
        decoded = adapter.from_database('{"__type__": "user", "payload": 1}', dict)
        assert isinstance(decoded, _Tagged)
