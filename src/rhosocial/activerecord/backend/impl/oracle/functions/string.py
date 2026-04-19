# functions/string.py
"""
Oracle string and regex function factories.
"""

from typing import Union, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase
    from rhosocial.activerecord.backend.expression import bases, core
    from ..dialect import OracleDialect


def decode_expr(dialect: "OracleDialect",
                expr: Union[str, "bases.BaseExpression"],
                *search_result_pairs: Any,
                default: Any = None) -> "core.FunctionCall":
    """Creates a DECODE function call.
    
    Oracle's DECODE is similar to CASE expression.
    
    Args:
        dialect: The Oracle dialect instance
        expr: Expression to compare
        *search_result_pairs: Pairs of search value and result
        default: Optional default value
    
    Returns:
        A FunctionCall instance representing DECODE
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    args = [_convert_to_expression(dialect, expr)]
    for val in search_result_pairs:
        args.append(core.Literal(dialect, val))
    if default is not None:
        args.append(core.Literal(dialect, default))
    return core.FunctionCall(dialect, "DECODE", *args)


def regexp_substr(dialect: "OracleDialect",
                  source: Union[str, "bases.BaseExpression"],
                  pattern: str,
                  position: int = 1,
                  occurrence: int = 1,
                  match_param: Optional[str] = None) -> "core.FunctionCall":
    """Creates a REGEXP_SUBSTR function call.
    
    Extracts substring matching a regular expression.
    
    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        position: Starting position (default 1)
        occurrence: Occurrence to find (default 1)
        match_param: Match parameters (i, c, m, n, x)
    
    Returns:
        A FunctionCall instance representing REGEXP_SUBSTR
    
    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    position_expr = core.Literal(dialect, position)
    occurrence_expr = core.Literal(dialect, occurrence)
    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_SUBSTR", source_expr, pattern_expr,
                                  position_expr, occurrence_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_SUBSTR", source_expr, pattern_expr,
                              position_expr, occurrence_expr)


def regexp_instr(dialect: "OracleDialect",
                 source: Union[str, "bases.BaseExpression"],
                 pattern: str,
                 position: int = 1,
                 occurrence: int = 1,
                 return_opt: int = 0,
                 match_param: Optional[str] = None) -> "core.FunctionCall":
    """Creates a REGEXP_INSTR function call.
    
    Returns the position of a pattern match.
    
    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        position: Starting position (default 1)
        occurrence: Occurrence to find (default 1)
        return_opt: 0 for start position, 1 for end position
        match_param: Match parameters
    
    Returns:
        A FunctionCall instance representing REGEXP_INSTR
    
    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    position_expr = core.Literal(dialect, position)
    occurrence_expr = core.Literal(dialect, occurrence)
    return_opt_expr = core.Literal(dialect, return_opt)
    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_INSTR", source_expr, pattern_expr,
                                  position_expr, occurrence_expr, return_opt_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_INSTR", source_expr, pattern_expr,
                              position_expr, occurrence_expr, return_opt_expr)


def regexp_like(dialect: "OracleDialect",
                source: Union[str, "bases.BaseExpression"],
                pattern: str,
                match_param: Optional[str] = None) -> "core.FunctionCall":
    """Creates a REGEXP_LIKE function call.
    
    Checks if a string matches a regular expression.
    
    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        match_param: Match parameters
    
    Returns:
        A FunctionCall instance representing REGEXP_LIKE
    
    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_LIKE", source_expr, pattern_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_LIKE", source_expr, pattern_expr)


def regexp_replace(dialect: "OracleDialect",
                   source: Union[str, "bases.BaseExpression"],
                   pattern: str,
                   replace: str,
                   position: int = 1,
                   occurrence: int = 0,
                   match_param: Optional[str] = None) -> "core.FunctionCall":
    """Creates a REGEXP_REPLACE function call.
    
    Replaces pattern matches in a string.
    
    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        replace: Replacement string
        position: Starting position (default 1)
        occurrence: Occurrence to replace (0 = all)
        match_param: Match parameters
    
    Returns:
        A FunctionCall instance representing REGEXP_REPLACE
    
    Version: Oracle 10g+
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    replace_expr = core.Literal(dialect, replace)
    position_expr = core.Literal(dialect, position)
    occurrence_expr = core.Literal(dialect, occurrence)
    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_REPLACE", source_expr, pattern_expr,
                                  replace_expr, position_expr, occurrence_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_REPLACE", source_expr, pattern_expr,
                              replace_expr, position_expr, occurrence_expr)


def regexp_count(dialect: "OracleDialect",
                 source: Union[str, "bases.BaseExpression"],
                 pattern: str,
                 position: int = 1,
                 match_param: Optional[str] = None) -> "core.FunctionCall":
    """Creates a REGEXP_COUNT function call.
    
    Counts pattern matches in a string.
    
    Args:
        dialect: The Oracle dialect instance
        source: Source string
        pattern: Regular expression pattern
        position: Starting position (default 1)
        match_param: Match parameters
    
    Returns:
        A FunctionCall instance representing REGEXP_COUNT
    
    Version: Oracle 11g+
    """
    from rhosocial.activerecord.backend.expression import core
    from . import _convert_to_expression
    source_expr = _convert_to_expression(dialect, source)
    pattern_expr = core.Literal(dialect, pattern)
    position_expr = core.Literal(dialect, position)
    if match_param:
        match_expr = core.Literal(dialect, match_param)
        return core.FunctionCall(dialect, "REGEXP_COUNT", source_expr, pattern_expr,
                                  position_expr, match_expr)
    return core.FunctionCall(dialect, "REGEXP_COUNT", source_expr, pattern_expr, position_expr)
