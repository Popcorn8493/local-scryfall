"""Recursive-descent parser: AST + lifted sort plan."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from mtg_viewer.query.aliases import canonical_predicate_key, canonical_sort_key
from mtg_viewer.query.tokenizer import TokKind, Token, tokenize


@dataclass
class Pred:
    raw: str
    key: str
    op: str
    value: str


@dataclass
class Not:
    child: "Expr"


@dataclass
class And:
    left: "Expr"
    right: "Expr"


@dataclass
class Or:
    left: "Expr"
    right: "Expr"


Expr = Pred | Not | And | Or


@dataclass
class SortItem:
    key: str
    descending: bool = False


@dataclass
class ParseResult:
    expr: Expr | None  # None = empty query (match all)
    sorts: List[SortItem] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def split_predicate(raw: str) -> Pred | None:
    raw = raw.strip()
    if not raw or raw == '""':
        return None
    # sort:-name
    if raw.lower().startswith("sort:"):
        body = raw[5:]
        desc = body.startswith("-")
        if desc:
            body = body[1:]
        key = canonical_sort_key(body.strip())
        return Pred(raw=raw, key="sort", op=":", value=("-" if desc else "") + key)
    if raw.lower().startswith("sort="):
        body = raw[5:]
        desc = body.startswith("-")
        if desc:
            body = body[1:]
        key = canonical_sort_key(body.strip())
        return Pred(raw=raw, key="sort", op="=", value=("-" if desc else "") + key)

    ops = ["<=", ">=", "<", ">", ":", "="]
    for op in ops:
        idx = raw.find(op)
        if idx > 0:
            key = raw[:idx].strip()
            rest = raw[idx + len(op) :].strip()
            if key:
                return Pred(raw=raw, key=key, op=op, value=rest)
    return Pred(raw=raw, key=raw, op="", value="")


class _Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self.toks = tokens
        self.i = 0
        self.sorts: list[SortItem] = []

    def cur(self) -> Token:
        return self.toks[self.i]

    def advance(self) -> None:
        if self.i < len(self.toks) - 1:
            self.i += 1

    def parse(self) -> Expr | None:
        if self.cur().kind == TokKind.EOF:
            return None
        e = self.parse_or()
        if self.cur().kind != TokKind.EOF:
            raise ValueError(f"Unexpected token after expression: {self.cur()}")
        return e

    def parse_or(self) -> Expr | None:
        left = self.parse_and()
        while self.cur().kind == TokKind.OR:
            self.advance()
            right = self.parse_and()
            if left is None:
                left = right
            elif right is None:
                continue
            else:
                left = Or(left=left, right=right)
        return left

    def parse_and(self) -> Expr | None:
        left = self.parse_unary()
        while self.cur().kind in (TokKind.AND, TokKind.PRED, TokKind.LPAREN, TokKind.NOT):
            # implicit AND between adjacent terms
            if self.cur().kind == TokKind.AND:
                self.advance()
            right = self.parse_unary()
            if left is None:
                left = right
            elif right is None:
                continue
            else:
                left = And(left=left, right=right)
        return left

    def parse_unary(self) -> Expr | None:
        if self.cur().kind == TokKind.NOT:
            self.advance()
            inner = self.parse_unary()
            if inner is None:
                return None
            return Not(child=inner)
        return self.parse_primary()

    def parse_primary(self) -> Expr | None:
        t = self.cur()
        if t.kind == TokKind.LPAREN:
            self.advance()
            inner = self.parse_or()
            if self.cur().kind != TokKind.RPAREN:
                raise ValueError("Expected )")
            self.advance()
            return inner
        if t.kind == TokKind.PRED:
            self.advance()
            p = split_predicate(t.text)
            if p is None:
                return None
            lk = canonical_predicate_key(p.key)
            if lk == "sort":
                desc = p.value.startswith("-")
                sk = p.value[1:] if desc else p.value
                sk = canonical_sort_key(sk)
                self.sorts.append(SortItem(key=sk, descending=desc))
                return None
            p = Pred(raw=p.raw, key=lk, op=p.op, value=p.value)
            return p
        if t.kind in (TokKind.AND, TokKind.OR, TokKind.RPAREN, TokKind.EOF):
            return None
        raise ValueError(f"Unexpected token in primary: {t}")


def parse_query(q: str) -> ParseResult:
    q = (q or "").strip()
    if not q:
        return ParseResult(expr=None, sorts=[])

    toks = tokenize(q)
    # filter trailing EOF for parser length
    if not toks or toks[-1].kind != TokKind.EOF:
        toks.append(Token(TokKind.EOF))

    p = _Parser(toks)
    try:
        expr = p.parse()
    except Exception as e:
        return ParseResult(expr=None, sorts=[], errors=[str(e)])
    return ParseResult(expr=expr, sorts=p.sorts, errors=[])
