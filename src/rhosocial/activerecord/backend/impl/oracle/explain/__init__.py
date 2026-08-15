# explain/__init__.py
"""
Oracle EXPLAIN result types.

This module provides type definitions for Oracle EXPLAIN PLAN output,
enabling structured access to query execution plans.
"""

from .types import OracleExplainRow, OracleExplainResult

__all__ = ['OracleExplainRow', 'OracleExplainResult']
