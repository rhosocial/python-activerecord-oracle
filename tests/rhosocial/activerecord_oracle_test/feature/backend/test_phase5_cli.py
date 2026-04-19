# tests/rhosocial/activerecord_oracle_test/feature/backend/test_phase5_cli.py
"""
Tests for Phase 5 CLI tool.

Tests the command-line interface functionality for Oracle backend.
"""

import pytest
import subprocess
import sys
import json


class TestCLIModule:
    """Test CLI module structure."""

    def test_cli_module_import(self):
        """Test importing the CLI module."""
        from rhosocial.activerecord.backend.impl.oracle import __main__
        assert __main__ is not None

    def test_cli_has_main(self):
        """Test that CLI has main function."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import main
        assert callable(main)

    def test_cli_has_parse_args(self):
        """Test that CLI has argument parser."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import parse_args
        assert callable(parse_args)

    def test_cli_has_get_provider(self):
        """Test that CLI has output provider factory."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import get_provider
        assert callable(get_provider)


class TestCLIInfoCommand:
    """Test CLI info command."""

    def test_parse_version(self):
        """Test version parsing."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import parse_version

        assert parse_version("19.0.0") == (19, 0, 0)
        assert parse_version("21.0.0") == (21, 0, 0)
        assert parse_version("23.1.0") == (23, 1, 0)
        assert parse_version("11.2.0") == (11, 2, 0)

    def test_parse_version_defaults(self):
        """Test version parsing with defaults."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import parse_version

        # Single digit defaults
        assert parse_version("19") == (19, 0, 0)
        assert parse_version("21.3") == (21, 3, 0)

    def test_handle_info_basic(self):
        """Test info command without connection."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import handle_info
        from rhosocial.activerecord.backend.output import JsonOutputProvider

        class MockArgs:
            host = "localhost"
            port = 1521
            service = "ORCL"
            user = "system"
            password = ""
            version = "19.0.0"
            output = "json"
            verbose = 0

        provider = JsonOutputProvider()
        info = handle_info(MockArgs(), provider)

        assert "database" in info
        assert info["database"]["type"] == "oracle"
        assert info["database"]["version"] == "19.0.0"
        assert info["database"]["connected"] is False
        assert "features" in info
        assert "locking" in info
        assert "pagination" in info


class TestCLIOutputProviders:
    """Test CLI output providers."""

    def test_json_provider(self):
        """Test JSON output provider."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import get_provider

        class MockArgs:
            output = "json"
            rich_ascii = False

        provider = get_provider(MockArgs())
        assert provider is not None

    def test_csv_provider(self):
        """Test CSV output provider."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import get_provider

        class MockArgs:
            output = "csv"
            rich_ascii = False

        provider = get_provider(MockArgs())
        assert provider is not None

    def test_tsv_provider(self):
        """Test TSV output provider."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import get_provider

        class MockArgs:
            output = "tsv"
            rich_ascii = False

        provider = get_provider(MockArgs())
        assert provider is not None


class TestCLISerialization:
    """Test CLI serialization utilities."""

    def test_serialize_none(self):
        """Test serializing None."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _serialize_for_output

        assert _serialize_for_output(None) is None

    def test_serialize_primitive(self):
        """Test serializing primitive types."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _serialize_for_output

        assert _serialize_for_output("test") == "test"
        assert _serialize_for_output(123) == 123
        assert _serialize_for_output(12.34) == 12.34
        assert _serialize_for_output(True) is True

    def test_serialize_list(self):
        """Test serializing list."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _serialize_for_output

        result = _serialize_for_output([1, 2, 3])
        assert result == [1, 2, 3]

    def test_serialize_dict(self):
        """Test serializing dict."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _serialize_for_output

        result = _serialize_for_output({"key": "value"})
        assert result == {"key": "value"}

    def test_serialize_nested(self):
        """Test serializing nested structures."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _serialize_for_output

        data = {
            "list": [1, 2, {"nested": "value"}],
            "dict": {"inner": [3, 4]}
        }
        result = _serialize_for_output(data)
        assert result["list"][2]["nested"] == "value"
        assert result["dict"]["inner"] == [3, 4]


class TestCLIFormatSize:
    """Test CLI size formatting."""

    def test_format_bytes(self):
        """Test formatting bytes."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _format_size

        assert _format_size(500) == "500.0 B"
        assert _format_size(1023) == "1023.0 B"

    def test_format_kilobytes(self):
        """Test formatting kilobytes."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _format_size

        assert _format_size(1024) == "1.0 KB"
        assert _format_size(2048) == "2.0 KB"

    def test_format_megabytes(self):
        """Test formatting megabytes."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _format_size

        result = _format_size(1024 * 1024)
        assert "MB" in result

    def test_format_gigabytes(self):
        """Test formatting gigabytes."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import _format_size

        result = _format_size(1024 * 1024 * 1024)
        assert "GB" in result


class TestCLIArgumentParser:
    """Test CLI argument parser."""

    def test_parser_exists(self):
        """Test that parser can be created."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import parse_args
        # This test just ensures the parser doesn't crash
        # We don't actually run it since it calls sys.argv

    def test_introspect_types_defined(self):
        """Test introspection types are defined."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import INTROSPECT_TYPES

        assert "tables" in INTROSPECT_TYPES
        assert "views" in INTROSPECT_TYPES
        assert "table" in INTROSPECT_TYPES
        assert "columns" in INTROSPECT_TYPES
        assert "indexes" in INTROSPECT_TYPES
        assert "foreign-keys" in INTROSPECT_TYPES
        assert "database" in INTROSPECT_TYPES

    def test_status_types_defined(self):
        """Test status types are defined."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import STATUS_TYPES

        assert "all" in STATUS_TYPES
        assert "config" in STATUS_TYPES
        assert "performance" in STATUS_TYPES
        assert "connections" in STATUS_TYPES
        assert "storage" in STATUS_TYPES
        assert "users" in STATUS_TYPES


class TestCLIInfoFeatures:
    """Test CLI info command features."""

    def test_info_structure(self):
        """Test info output structure."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import handle_info
        from rhosocial.activerecord.backend.output import JsonOutputProvider

        class MockArgs:
            host = "localhost"
            port = 1521
            service = "ORCL"
            user = "system"
            password = ""
            version = "19.0.0"
            output = "json"
            verbose = 0

        provider = JsonOutputProvider()
        info = handle_info(MockArgs(), provider)

        # Check top-level keys
        assert "database" in info
        assert "features" in info
        assert "locking" in info
        assert "pagination" in info

        # Check database info
        assert "type" in info["database"]
        assert "version" in info["database"]
        assert "version_tuple" in info["database"]
        assert "connected" in info["database"]

        # Check features
        features = info["features"]
        assert "hierarchical_queries" in features
        assert "pivot" in features
        assert "query_hints" in features

        # Check locking
        locking = info["locking"]
        assert "for_update" in locking
        assert "for_update_nowait" in locking
        assert "for_update_skip_locked" in locking

    def test_info_version_specific(self):
        """Test info for different Oracle versions."""
        from rhosocial.activerecord.backend.impl.oracle.__main__ import handle_info
        from rhosocial.activerecord.backend.output import JsonOutputProvider

        # Test 19c
        class MockArgs19c:
            host = "localhost"
            port = 1521
            service = "ORCL"
            user = "system"
            password = ""
            version = "19.0.0"
            output = "json"
            verbose = 0

        provider = JsonOutputProvider()
        info_19c = handle_info(MockArgs19c(), provider)

        # 19c should support pivot but not boolean/vector
        assert info_19c["features"]["pivot"] is True
        assert info_19c["features"]["boolean_type"] is False
        assert info_19c["features"]["vector_type"] is False

        # Test 23ai
        class MockArgs23ai:
            host = "localhost"
            port = 1521
            service = "ORCL"
            user = "system"
            password = ""
            version = "23.0.0"
            output = "json"
            verbose = 0

        info_23ai = handle_info(MockArgs23ai(), provider)

        # 23ai should support boolean and vector
        assert info_23ai["features"]["boolean_type"] is True
        assert info_23ai["features"]["vector_type"] is True


class TestCLIHelpOutput:
    """Test CLI help output works."""

    def test_module_docstring(self):
        """Test module has proper docstring."""
        from rhosocial.activerecord.backend.impl.oracle import __main__
        assert __main__.__doc__ is not None
        assert "info" in __main__.__doc__.lower() or "query" in __main__.__doc__.lower()
