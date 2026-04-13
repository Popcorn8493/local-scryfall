"""Lexical tokenizer for Scryfall-style search strings."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class TokKind(Enum):
    EOF = auto()
    LPAREN = auto()
    RPAREN = auto()
    AND = auto()
    OR = auto()
    NOT = auto()  # unary minus before term
    PRED = auto()  # raw predicate string e.g. f:edh, cmc<4, o:foo


@dataclass
class Token:
    kind: TokKind
    text: str = ""


def tokenize(s: str) -> list[Token]:
    """Split *s* into tokens; implicit AND is inserted by the parser, not here."""
    i = 0
    n = len(s)
    out: list[Token] = []

    def skip_ws() -> None:
        nonlocal i
        while i < n and s[i].isspace():
            i += 1

    while True:
        skip_ws()
        if i >= n:
            out.append(Token(TokKind.EOF))
            return out
        c = s[i]
        if c == "(":
            i += 1
            out.append(Token(TokKind.LPAREN))
            continue
        if c == ")":
            i += 1
            out.append(Token(TokKind.RPAREN))
            continue
        if c == "-":
            i += 1
            out.append(Token(TokKind.NOT))
            continue
        if c == '"':
            i += 1
            buf: list[str] = []
            while i < n:
                if s[i] == "\\" and i + 1 < n:
                    buf.append(s[i + 1])
                    i += 2
                    continue
                if s[i] == '"':
                    i += 1
                    break
                buf.append(s[i])
                i += 1
            out.append(Token(TokKind.PRED, '"' + "".join(buf) + '"'))
            continue

        # read predicate atom: key:value where value may be "quoted phrase"
        start = i
        i = start
        while i < n and s[i] not in "()":
            if s[i].isspace():
                break
            if s[i] == ":":
                i += 1
                if i < n and s[i] == '"':
                    i += 1
                    while i < n:
                        if s[i] == "\\" and i + 1 < n:
                            i += 2
                            continue
                        if s[i] == '"':
                            i += 1
                            break
                        i += 1
                else:
                    while i < n and not s[i].isspace() and s[i] not in "()":
                        i += 1
                break
            i += 1
        atom = s[start:i]
        if not atom:
            continue
        low = atom.lower()
        if low == "and":
            out.append(Token(TokKind.AND))
        elif low == "or":
            out.append(Token(TokKind.OR))
        else:
            out.append(Token(TokKind.PRED, atom))
