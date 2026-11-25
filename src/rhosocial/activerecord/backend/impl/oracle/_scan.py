# src/rhosocial/activerecord/backend/impl/oracle/_scan.py
"""Regex-free scanning primitives for the Oracle backend.

This backend is required not to import :mod:`re` anywhere under ``src``. The
parsing this package needs is small and closed (an Oracle version number, a type
spelling, a ``HIGH_VALUE`` bound, a PL/SQL token stream), so each site is written
as an explicit character scan instead of a pattern. This module holds the
primitives several of those sites share, so the same boundary rule ("a word ends
where a non-word character begins") is defined once.

Every helper mirrors a specific regular expression that used to live at a call
site. The docstring of each helper names that shape, because the semantics are
the contract: callers were written against it. Concretely:

* word characters are :meth:`str.isalnum` plus ``_`` -- the ``\\w`` set;
* whitespace is :meth:`str.isspace` -- the ``\\s`` set;
* a "word boundary" before a word means the preceding character is not a word
  character, and after it means the following character is not one;
* digits are :meth:`str.isdecimal` characters -- the ``\\d`` set, which
  :func:`int` also accepts.

Nothing here raises on malformed input; helpers that cannot match return
``None`` (or an empty string, for the digit-run readers) so call sites keep their
original refusal paths.
"""

from __future__ import annotations

from typing import Iterable, Iterator, List, NamedTuple, Optional, Sequence, Tuple

#: Punctuation-style tokens produced by :func:`iter_plsql_tokens`.
PLSQL_WORD = "word"
PLSQL_QUOTED = "quoted"
PLSQL_DQUOTED = "dquoted"
PLSQL_LINE_COMMENT = "line_comment"
PLSQL_BLOCK_COMMENT = "block_comment"
PLSQL_SPACE = "space"
PLSQL_PUNCT = "punct"

#: Keywords that open a PL/SQL block for the script splitter.
PLSQL_BLOCK_OPENERS = frozenset(("BEGIN", "DECLARE", "IF", "LOOP", "CASE", "FOR", "WHILE"))
#: Keywords that close one level of nesting for the script splitter.
PLSQL_BLOCK_CLOSERS = frozenset(("END",))


class PlsqlToken(NamedTuple):
    """One lexical unit of a SQL/PL/SQL script.

    ``text`` is the source slice exactly as it appears (quoted text keeps its
    delimiters), and ``start``/``end`` are offsets into the original script.
    """

    kind: str
    text: str
    start: int
    end: int


# --------------------------------------------------------------------------- #
# character classes
# --------------------------------------------------------------------------- #
def is_word_char(char: str) -> bool:
    """Whether *char* belongs to the ``\\w`` set: alphanumeric or ``_``."""
    return char.isalnum() or char == "_"


def is_space(char: str) -> bool:
    """Whether *char* belongs to the ``\\s`` set."""
    return char.isspace()


def is_digit(char: str) -> bool:
    """Whether *char* belongs to the ``\\d`` set and is accepted by :func:`int`."""
    return char.isdecimal()


def skip_space(text: str, index: int) -> int:
    """Return the offset of the first non-whitespace character at or after *index*."""
    length = len(text)
    while index < length and text[index].isspace():
        index += 1
    return index


# --------------------------------------------------------------------------- #
# words
# --------------------------------------------------------------------------- #
def read_word(text: str, index: int = 0) -> Tuple[str, int]:
    """Read the maximal word-character run starting at *index*.

    Returns the run (empty when *index* is not on a word character) and the
    offset just past it.
    """
    start = index
    length = len(text)
    while index < length and is_word_char(text[index]):
        index += 1
    return text[start:index], index


def leading_word(text: str) -> str:
    """The maximal word-character run that starts at offset zero."""
    return read_word(text)[0]


def leading_word_in(text: str, words: Iterable[str], case_insensitive: bool = True) -> bool:
    """Whether the leading whole word is one of *words*.

    This is ``^(?:word1|word2|...)\\b``: the whole leading word must be listed,
    not just a prefix of it.
    """
    word = leading_word(text)
    if not word:
        return False
    if case_insensitive:
        return word.upper() in {candidate.upper() for candidate in words}
    return word in words


def starts_with_word(text: str, word: str, case_insensitive: bool = True) -> bool:
    """Whether *text* begins with *word* followed by a word boundary."""
    length = len(text)
    if length < len(word):
        return False
    for offset in range(len(word)):
        left = text[offset]
        right = word[offset]
        if case_insensitive:
            if left.upper() != right.upper():
                return False
        elif left != right:
            return False
    end = len(word)
    return end >= length or not is_word_char(text[end])


def _skip_required_space(text: str, index: int) -> Optional[int]:
    """Consume one-or-more whitespace characters; ``None`` when none begin at *index*."""
    length = len(text)
    if index >= length or not text[index].isspace():
        return None
    return skip_space(text, index)


def _match_words_at(text: str, index: int, words: Sequence[str]) -> Optional[int]:
    """Match *words* at *index*, separated by at least one whitespace character."""
    for position, word in enumerate(words):
        if position:
            stepped = _skip_required_space(text, index)
            if stepped is None:
                return None
            index = stepped
        if not starts_with_word(text[index:], word):
            return None
        index += len(word)
    return index


def starts_with_word_sequence(text: str, words: Sequence[str]) -> bool:
    """Whether *text* begins with *words*, whitespace-separated, at a boundary.

    This is ``^(?:first\\s+second\\s+...)\\b`` for a multi-word type spelling.
    """
    end = _match_words_at(text, 0, words)
    return end is not None and (end >= len(text) or not is_word_char(text[end]))


def starts_with_any_word_sequence(text: str, alternatives: Sequence[Sequence[str]]) -> bool:
    """Whether any of *alternatives* matches at the start of *text*."""
    return any(starts_with_word_sequence(text, words) for words in alternatives)


def contains_word(text: str, word: str, case_insensitive: bool = True) -> bool:
    """Whether *word* occurs in *text* as a whole word.

    This is ``\\bword\\b`` searched, not anchored.
    """
    length = len(text)
    word_length = len(word)
    index = 0
    while index + word_length <= length:
        if starts_with_word(text[index:], word, case_insensitive):
            if index == 0 or not is_word_char(text[index - 1]):
                return True
        index += 1
    return False


def contains_any_word(text: str, words: Iterable[str], case_insensitive: bool = True) -> bool:
    """Whether any of *words* occurs in *text* as a whole word."""
    return any(contains_word(text, word, case_insensitive) for word in words)


def contains_word_sequence(text: str, words: Sequence[str]) -> bool:
    """Whether *words* occur in *text* as a whitespace-separated word sequence.

    This is ``\\bfirst\\s+second\\b`` searched. Each candidate start has to be a
    word boundary, which is what the leading ``\\b`` contributes.
    """
    length = len(text)
    for index in range(length):
        if is_word_char(text[index]) and (index == 0 or not is_word_char(text[index - 1])):
            end = _match_words_at(text, index, words)
            if end is not None and (end >= length or not is_word_char(text[end])):
                return True
    return False


# --------------------------------------------------------------------------- #
# whitespace normalisation
# --------------------------------------------------------------------------- #
def collapse_whitespace(text: str) -> str:
    """Collapse every whitespace run to one space and strip both ends.

    Equivalent to collapsing every whitespace run and stripping both ends:
    :meth:`str.split` with no argument splits on the same whitespace set and
    discards empty runs.
    """
    return " ".join(text.split())


def squash_whitespace(text: str) -> str:
    """Collapse every whitespace run to one space, keeping the ends.

    Collapses whitespace without stripping: leading and trailing whitespace
    survive as one space, which a caller comparing against an exact spelling
    still rejects.
    """
    pieces: List[str] = []
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char.isspace():
            pieces.append(" ")
            while index < length and text[index].isspace():
                index += 1
        else:
            pieces.append(char)
            index += 1
    return "".join(pieces)


# --------------------------------------------------------------------------- #
# digits
# --------------------------------------------------------------------------- #
def read_digits(text: str, index: int = 0) -> Tuple[str, int]:
    """Read the maximal decimal-digit run starting at *index*."""
    start = index
    length = len(text)
    while index < length and is_digit(text[index]):
        index += 1
    return text[start:index], index


def leading_digit_run(text: str) -> str:
    """The maximal digit run at offset zero; empty when *text* does not start with one."""
    return read_digits(text)[0]


def iter_digit_runs(text: str) -> List[str]:
    """Every maximal decimal-digit run, in order. This is ``findall(\\d+)``."""
    runs: List[str] = []
    index = 0
    length = len(text)
    while index < length:
        if is_digit(text[index]):
            run, index = read_digits(text, index)
            runs.append(run)
        else:
            index += 1
    return runs


def first_digit_run(text: str) -> Optional[str]:
    """The first maximal digit run anywhere in *text*, or ``None``."""
    runs = iter_digit_runs(text)
    return runs[0] if runs else None


def find_ora_code(text: str) -> Optional[int]:
    """The five-digit number after the first ``ORA-`` marker, or ``None``.

    This is ``search(r"ORA-(\\d{5})")`` followed by ``int(group(1))``: the marker
    is case-sensitive, and five digits must follow it immediately.
    """
    marker = "ORA-"
    index = text.find(marker)
    while index >= 0:
        digits = text[index + len(marker) : index + len(marker) + 5]
        if len(digits) == 5 and all(is_digit(char) for char in digits):
            return int(digits)
        index = text.find(marker, index + 1)
    return None


def find_parenthesised_digit_run(text: str) -> Optional[str]:
    """The digits of the first ``(`` followed by decimal digits, or ``None``.

    This is ``search(r"\\((\\d+)")``.
    """
    index = text.find("(")
    while index >= 0:
        digits, _ = read_digits(text, index + 1)
        if digits:
            return digits
        index = text.find("(", index + 1)
    return None


# --------------------------------------------------------------------------- #
# word-prefix edits
# --------------------------------------------------------------------------- #
def strip_word_with_space(text: str, word: str) -> Optional[str]:
    """Remove a leading *word* plus the whitespace after it.

    Removes a leading word and the whitespace after it, case-insensitively:
    returns ``""`` when the word is the whole prefix, and ``None`` when the word
    does not lead the string.
    """
    if not starts_with_word(text, word):
        return None
    index = len(word)
    stepped = _skip_required_space(text, index)
    if stepped is None:
        return None
    return text[stepped:]


def strip_leading_word_with_space(text: str, words: Iterable[str]) -> Optional[str]:
    """Remove any listed leading word and the whitespace after it, or return ``None``.

    This is ``^(?:WORD1|WORD2)\\s+(.+)$``: the remainder must be non-empty, and it
    may not contain a line break except one at the very end (``.`` does not
    cross ``\\n`` and ``$`` accepts a trailing newline).
    """
    for word in words:
        if starts_with_word(text, word):
            remainder = strip_word_with_space(text, word)
            if remainder is None or not remainder:
                return None
            if remainder.endswith("\n"):
                remainder = remainder[:-1]
            if not remainder or "\n" in remainder:
                return None
            return remainder
    return None


def cut_from_word(text: str, word: str) -> str:
    """Return *text* up to the first whole-word occurrence of *word*.

    Drops the word and everything after it, case-insensitively. Callers strip
    the result.
    """
    length = len(text)
    index = 0
    while index < length:
        if is_word_char(text[index]) and (index == 0 or not is_word_char(text[index - 1])):
            if starts_with_word(text[index:], word):
                return text[:index]
        index += 1
    return text


def remove_word_with_space(text: str, word: str) -> str:
    """Remove every whole-word *word* that is followed by whitespace.

    Whole-word matches followed by whitespace are removed, case-insensitively.
    """
    pieces: List[str] = []
    index = 0
    length = len(text)
    copied = 0
    while index < length:
        if is_word_char(text[index]) and (index == 0 or not is_word_char(text[index - 1])):
            if starts_with_word(text[index:], word):
                stepped = _skip_required_space(text, index + len(word))
                if stepped is not None:
                    pieces.append(text[copied:index])
                    copied = stepped
                    index = stepped
                    continue
        index += 1
    pieces.append(text[copied:])
    return "".join(pieces)


# --------------------------------------------------------------------------- #
# identifiers
# --------------------------------------------------------------------------- #
def scan_identifier_part(text: str, index: int = 0) -> Optional[int]:
    """Scan one Oracle identifier part at *index*, returning its end offset.

    An unquoted part is ``[A-Za-z][A-Za-z0-9_$#]*``; a quoted part is
    ``"(?:[^"]|"")*"``. ``None`` means the text at *index* is neither.
    """
    length = len(text)
    if index >= length:
        return None
    char = text[index]
    if char == '"':
        index += 1
        while index < length:
            if text[index] == '"':
                if index + 1 < length and text[index + 1] == '"':
                    index += 2
                    continue
                return index + 1
            index += 1
        return None
    if not (char.isascii() and char.isalpha()):
        return None
    index += 1
    while index < length:
        follower = text[index]
        if follower.isascii() and (follower.isalnum() or follower in "_$#"):
            index += 1
        else:
            break
    return index


def fullmatch_identifier(text: str) -> bool:
    """Whether *text* is exactly one Oracle identifier part."""
    end = scan_identifier_part(text)
    return end is not None and end == len(text)


def fullmatch_dotted_identifier(text: str) -> bool:
    """Whether *text* is one or more identifier parts joined by dots.

    This is ``^part(\\.part)*$``.
    """
    index = 0
    length = len(text)
    end = scan_identifier_part(text, index)
    if end is None:
        return False
    index = end
    while index < length:
        if text[index] != ".":
            return False
        end = scan_identifier_part(text, index + 1)
        if end is None:
            return False
        index = end
    return True


# --------------------------------------------------------------------------- #
# quoted text and numeric literals
# --------------------------------------------------------------------------- #
def scan_single_quoted(text: str, index: int = 0) -> Optional[Tuple[str, int]]:
    """Scan a single-quoted literal whose inner quotes are doubled.

    This is ``'(?:[^']|'')*'``. Returns the decoded contents (doubled quotes
    collapsed) and the offset past the closing quote, or ``None`` when *text*
    does not begin with a quote here or the literal is unterminated.
    """
    if index >= len(text) or text[index] != "'":
        return None
    index += 1
    pieces: List[str] = []
    length = len(text)
    while index < length:
        char = text[index]
        if char == "'":
            if index + 1 < length and text[index + 1] == "'":
                pieces.append("'")
                index += 2
                continue
            return "".join(pieces), index + 1
        pieces.append(char)
        index += 1
    return None


def scan_double_quoted(text: str, index: int = 0) -> Optional[Tuple[str, int]]:
    """Scan a double-quoted name whose inner quotes are doubled.

    This is ``"(?:[^"]|"")*"``; returns the decoded name and the end offset.
    """
    if index >= len(text) or text[index] != '"':
        return None
    index += 1
    pieces: List[str] = []
    length = len(text)
    while index < length:
        char = text[index]
        if char == '"':
            if index + 1 < length and text[index + 1] == '"':
                pieces.append('"')
                index += 2
                continue
            return "".join(pieces), index + 1
        pieces.append(char)
        index += 1
    return None


def _scan_sign(text: str, index: int) -> Tuple[int, int]:
    """Read an optional leading ``+``/``-``; return the sign and the offset after it."""
    if index < len(text) and text[index] in "+-":
        return (-1 if text[index] == "-" else 1), index + 1
    return 1, index


def parse_signed_integer(text: str) -> Optional[int]:
    """Parse ``[+-]?\\d+`` exactly, or return ``None``."""
    sign, index = _scan_sign(text, 0)
    digits, index = read_digits(text, index)
    if not digits or index != len(text):
        return None
    return -int(digits) if sign < 0 else int(digits)


def parse_signed_decimal(text: str) -> Optional[float]:
    """Parse ``[+-]?(?:\\.\\d|\\d+\\.\\d*|\\d+[eE][+-]?\\d+)`` exactly, or ``None``.

    A bare integer is not accepted: the closed grammar at the call site admits
    decimals and exponent forms only.
    """
    sign, index = _scan_sign(text, 0)
    length = len(text)
    integral, index = read_digits(text, index)
    if index < length and text[index] == ".":
        index += 1
        fraction, index = read_digits(text, index)
        if index != length:
            return None
        if not integral and not fraction:
            return None
        return float(text)
    if not integral or index >= length or text[index] not in "eE":
        return None
    index += 1
    _, index = _scan_sign(text, index)
    exponent, index = read_digits(text, index)
    if not exponent or index != length:
        return None
    return float(text)


# --------------------------------------------------------------------------- #
# label sanitising
# --------------------------------------------------------------------------- #
def sanitize_label(text: str) -> str:
    """Replace every ASCII-non-alphanumeric run with one underscore and trim it.

    Every ASCII-non-alphanumeric run becomes one underscore, and the ends are
    trimmed of underscores.
    """
    pieces: List[str] = []
    pending_separator = False
    for char in text:
        if char.isascii() and char.isalnum():
            if pending_separator and pieces:
                pieces.append("_")
            pending_separator = False
            pieces.append(char)
        else:
            pending_separator = True
    return "".join(pieces)


# --------------------------------------------------------------------------- #
# SQL / PL/SQL tokeniser
# --------------------------------------------------------------------------- #
def iter_plsql_tokens(sql: str) -> Iterator[PlsqlToken]:
    """Yield the lexical tokens of *sql*.

    The scanner recognises, in this order: single-quoted text (with ``''``
    escapes), double-quoted names (with ``""`` escapes), ``--`` line comments,
    ``/* */`` block comments, word runs, whitespace runs and single punctuation
    characters. Quoted text and comments become single tokens, so a ``;`` inside
    one is never a punctuation token -- which is what the script splitter needs.

    Unterminated quotes and comments run to the end of the input, matching the
    state machine this replaced: callers report the SQL error, not a lexer
    error.
    """
    length = len(sql)
    index = 0
    while index < length:
        char = sql[index]
        start = index
        if char == "'":
            scanned = scan_single_quoted(sql, index)
            index = scanned[1] if scanned is not None else length
            yield PlsqlToken(PLSQL_QUOTED, sql[start:index], start, index)
            continue
        if char == '"':
            scanned = scan_double_quoted(sql, index)
            index = scanned[1] if scanned is not None else length
            yield PlsqlToken(PLSQL_DQUOTED, sql[start:index], start, index)
            continue
        if char == "-" and index + 1 < length and sql[index + 1] == "-":
            index += 2
            while index < length and sql[index] != "\n":
                index += 1
            yield PlsqlToken(PLSQL_LINE_COMMENT, sql[start:index], start, index)
            continue
        if char == "/" and index + 1 < length and sql[index + 1] == "*":
            index += 2
            while index < length:
                if sql[index] == "*" and index + 1 < length and sql[index + 1] == "/":
                    index += 2
                    break
                index += 1
            yield PlsqlToken(PLSQL_BLOCK_COMMENT, sql[start:index], start, index)
            continue
        if is_word_char(char):
            index += 1
            while index < length and is_word_char(sql[index]):
                index += 1
            yield PlsqlToken(PLSQL_WORD, sql[start:index], start, index)
            continue
        if char.isspace():
            index += 1
            while index < length and sql[index].isspace():
                index += 1
            yield PlsqlToken(PLSQL_SPACE, sql[start:index], start, index)
            continue
        index += 1
        yield PlsqlToken(PLSQL_PUNCT, char, start, index)
