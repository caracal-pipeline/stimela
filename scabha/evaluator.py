import glob

import pyparsing
pyparsing.ParserElement.enable_packrat()
from pyparsing import *
from pyparsing import common

from .substitutions import SubstitutionError, SubstitutionContext
from .basetypes import Unresolved
from .exceptions import *

import typing
from typing import Dict, List, Any


_parser = None

def construct_parser():
    lparen = Literal("(").suppress()
    rparen = Literal(")").suppress()
    lbrack = Keyword("[").suppress()
    rbrack = Keyword("]").suppress()
    comma = Literal(",").suppress()
    period = Literal(".").suppress()
    string = (QuotedString('"') | QuotedString("'"))("constant")

    bool_false = (Keyword("False") | Keyword("false"))("bool_false").set_parse_action(lambda:[False])
    bool_true = (Keyword("True") | Keyword("true"))("bool_true").set_parse_action(lambda:[True])
    boolean = (bool_true | bool_false)("constant")

    number = common.number("constant")

    # identifier = Word(alphas, alphanums + "_")
    fieldname = Word(alphas + "_", alphanums + "_-@")
    nested_field = Group(fieldname + ZeroOrMore(period + fieldname).leave_whitespace()).leave_whitespace()("namespace_lookup")

    anyseq = CharsNotIn(",)")("constant")

    UNSET = Keyword("UNSET")("unset")
    # functions
    IF = Keyword("IF")
    IFSET = Keyword("IFSET")
    GLOB = Keyword("GLOB")
    EXISTS = Keyword("EXISTS")
    LIST = Keyword("LIST")

    # allow expression to be used recursively
    expr = Forward()
    
    # parenthesized expression
    subexpr = Group(lparen + expr + rparen)("subexpression")

    atomic_value = (boolean | UNSET | nested_field | string | number) #("atomic_value")
    comma_empty = Group(comma + Empty())("empty")
    varg = expr
    comma_varg = comma + varg

    if_ = IF + lparen + varg + comma_varg + comma_varg + Optional(comma_varg) + rparen
    ifset_ = IFSET + lparen + nested_field + Optional(comma_varg|comma_empty) + \
                     Optional(comma_varg|comma_empty) + Optional(comma_varg|comma_empty) + rparen
    glob_ = GLOB + lparen + (varg|anyseq) + rparen
    exists_ = EXISTS + lparen + (varg|anyseq) + rparen
    #list_ = LIST + lparen + delimited_list(varg, allow_trailing_delim=True) + rparen
    list_ = (LIST + lparen + varg + rparen) | \
            (LIST + lparen + varg + comma_varg + rparen) | \
            (LIST + lparen + varg + comma_varg + comma_varg + rparen) | \
            (LIST + lparen + varg + comma_varg + comma_varg + rparen) | \
            (LIST + lparen + varg + comma_varg + comma_varg + comma_varg + rparen) | \
            (LIST + lparen + varg + comma_varg + comma_varg + comma_varg + comma_varg + rparen) | \
            (LIST + lparen + varg + comma_varg + comma_varg + comma_varg + comma_varg + comma_varg + rparen) 

    # function call
    function = (list_ | ifset_ | if_ | glob_  | exists_)("function")
    
    # list constructor
    #list_constructor = Group(lbrack + delimitedList(expr, ",", allow_trailing_delim=True) + rbrack)("list_constructor")
    list_constructor = Group(lbrack + varg + comma_varg + comma_varg)("list_constructor")

    operators = (
        [(Literal("**")("op2"), 2, opAssoc.LEFT)] + 
        [(Literal(x)("op1"), 1, opAssoc.RIGHT) for x in "-+~"] + 
        [(Literal(x)("op2"), 2, opAssoc.LEFT) for x in ("*", "//", "/", "%")] +
        [(Literal(x)("op2"), 2, opAssoc.LEFT) for x in "+-"] +
        [(Literal(x)("op2"), 2, opAssoc.LEFT) for x in ("<<", ">>")] +
        [
            (Literal("&")("op2"), 2, opAssoc.LEFT),
            (Literal("^")("op2"), 2, opAssoc.LEFT),
            (Literal("|")("op2"), 2, opAssoc.LEFT)
        ] +
        [(Literal(x)("op2"), 2, opAssoc.LEFT) for x in ("==", "!=", ">", "<", ">=", "<=")] +
        [(Keyword(x)("op2"), 2, opAssoc.LEFT) for x in ("in", "not in")] +
        [   (Keyword("not")("op1"), 1, opAssoc.RIGHT),
            (Keyword("and")("op2"), 2, opAssoc.LEFT),
            (Keyword("or")("op2"), 2, opAssoc.LEFT)
        ]
    )
    infix = infix_notation(function | atomic_value, operators)("subexpression")

    expr <<= function | infix | list_constructor

    # expr.setDebug()

    return expr


if _parser is None:
    _parser = construct_parser()

_parse_cache = {}

def parse_string(text: str, location: List[str] = []):
    """parses a formula, returns ParseResults object. Implements cache.

    Args:
        text (str): formula to parse
        location (List[str], optional): Hierarchical location, used for erro messages
        
    Raises:
        ParserError: on any parse error

    Returns:
        ParseResults: on successful parse
    """
    parse_results = _parse_cache.get(text)
    if parse_results is None:
        try:
            parse_results = _parser.parse_string(text, parse_all=True)
        except Exception as exc:
            parse_results = ParserError(f"{'.'.join(location)}: error parsing formula ({exc})")
        _parse_cache[text] = parse_results

    if isinstance(parse_results, Exception):
        raise parse_results

    return parse_results

def is_empty(result):
    return result is None or (type(result) is ParseResults and result._name == "empty")

class UNSET(object):
    def __init__(self, name) -> None:
        self.name = name

class Evaluator(object):
    def __init__(self,  ns: Dict[str, Any], 
                        subst_context: typing.Optional[SubstitutionContext] = None, 
                        location: List[str] = []):
        self.ns = ns
        self.subst_context = subst_context
        self.location = location

        self._UNARY_OPERATORS = {
            '+':    lambda x: +self._evaluate_result(x),
            '-':    lambda x: -self._evaluate_result(x),
            '~':    lambda x: ~self._evaluate_result(x),
            'not':  lambda x: ~self._evaluate_result(x)
        }
        self._BINARY_OPERATORS = {
            '**':   lambda x,y: self._evaluate_result(x) ** self._evaluate_result(y),
            '*':   lambda x,y: self._evaluate_result(x) * self._evaluate_result(y),
            '/':   lambda x,y: self._evaluate_result(x) / self._evaluate_result(y),
            '//':   lambda x,y: self._evaluate_result(x) // self._evaluate_result(y),
            '+':   lambda x,y: self._evaluate_result(x) + self._evaluate_result(y),
            '-':   lambda x,y: self._evaluate_result(x) - self._evaluate_result(y),
            '<<':   lambda x,y: self._evaluate_result(x) << self._evaluate_result(y),
            '>>':   lambda x,y: self._evaluate_result(x) >> self._evaluate_result(y),
            '&':   lambda x,y: self._evaluate_result(x) & self._evaluate_result(y),
            '^':   lambda x,y: self._evaluate_result(x) ^ self._evaluate_result(y),
            '|':   lambda x,y: self._evaluate_result(x) | self._evaluate_result(y),
            '==':   lambda x,y: self._evaluate_result(x) == self._evaluate_result(y),
            '!=':   lambda x,y: self._evaluate_result(x) != self._evaluate_result(y),
            '<=':   lambda x,y: self._evaluate_result(x) <= self._evaluate_result(y),
            '<':   lambda x,y: self._evaluate_result(x) < self._evaluate_result(y),
            '>=':   lambda x,y: self._evaluate_result(x) >= self._evaluate_result(y),
            '>':   lambda x,y: self._evaluate_result(x) > self._evaluate_result(y),
            # 'is':   lambda x,y: self._evaluate_result(x) is self._evaluate_result(y),
            # 'is not':   lambda x,y: self._evaluate_result(x) is not self._evaluate_result(y),
            'in':   lambda x,y: self._evaluate_result(x) in self._evaluate_result(y),
            'not in':   lambda x,y: self._evaluate_result(x) not in self._evaluate_result(y),
            'and':   lambda x,y: self._evaluate_result(x) and self._evaluate_result(y),
            'or':   lambda x,y: self._evaluate_result(x) or self._evaluate_result(y),
        }        

    def _resolve(self, value):
        if type(value) is str and self.subst_context is not None:
            try:
                value = self.subst_context.evaluate(value, location=self.location)
            except Exception as exc:
                raise SubstitutionError(f"{value}: {exc}")
        return value

    def empty(self, *args):
        return None

    def unset(self, *args):
        return UNSET

    def constant(self, value):
        return self._resolve(value)

    def subexpression(self, value):
        return self._evaluate_result(value)
    
    def list_constructor(self, *elements):
        return [self._evaluate_result(value) for value in elements]

    def namespace_lookup(self, *args):
        if len(args) == 1 and type(args[0]) is ParseResults:
            args = args[0]
            assert args._name == "namespace_lookup"
        value = self.ns
        fields = list(args)
        while fields:
            fld = fields.pop(0)
            # last element allowed to be UNSET, otherwise substitution error
            if fld not in value:
                if fields:
                    raise SubstitutionError(f"{'.'.join(self.location)}: '{'.'.join(args)}' is not defined (at '{fld}')")
                else:
                    return UNSET('.'.join(args))
            value = value[fld]
        return self._resolve(value)

    def function(self, funcname, *args):
        method = f"func_{funcname}"
        if not hasattr(self, method):
            raise NameError(f"{'.'.join(self.location)}: unknown function '{funcname}'")
        return getattr(self, method)(*args)

    def func_LIST(self, *args):
        return [self._evaluate_result(value) for value in args]

    def func_IF(self, *args):
        if len(args) < 3 or len(args) > 4:
            raise FormulaError(f"{'.'.join(self.location)}: IF() expects 3 or 4 arguments, got {len(args)}")
        conditional, if_true, if_false = args[:3]
        if_unset = args[3] if len(args) == 4 else None

        cond = self._evaluate_result(conditional, allow_unset=if_unset is not None)
        if type(cond) is UNSET:
            if if_unset is None:
                raise SubstitutionError(f"{'.'.join(self.location)}: '{cond.name}' is not defined")
            result = if_unset
        else:
            result = if_true if cond else if_false    
            
        return self._evaluate_result(result)

    def func_IFSET(self, *args):
        if len(args) < 1 or len(args) > 3:
            raise FormulaError(f"{'.'.join(self.location)}: IFSET() expects 1 to 3 arguments, got {len(args)}")
        
        lookup, if_set, if_unset = list(args) + [None]*(3 - len(args))

        value = self._evaluate_result(lookup, allow_unset=True)
        if type(value) is UNSET:
            if is_empty(if_unset):
                return UNSET
            return self._evaluate_result(if_unset)
        elif is_empty(if_set):
            return value
        else:
            return self._evaluate_result(if_set)            

    def func_GLOB(self, *args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(self.location)}: GLOB() expects 1 argument, got {len(args)}")
        pattern = self._evaluate_result(args[0])
        return sorted(glob.glob(pattern))

    def func_EXISTS(self, *args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(self.location)}: EXISTS() expects 1 argument, got {len(args)}")
        pattern = self._evaluate_result(args[0])
        return bool(glob.glob(pattern))

    def _evaluate_result(self, parse_result, allow_unset=False):
        # if result is already a constant, resolve it
        if type(parse_result) is not ParseResults:
            return self._resolve(parse_result)

        # if result is a unary or binary operator:
        if hasattr(parse_result, 'op1') and parse_result.op1:
            assert parse_result.op1 in self._UNARY_OPERATORS
            return self._UNARY_OPERATORS[parse_result.op1](parse_result[1])
        elif hasattr(parse_result, 'op2') and parse_result.op2:
            assert parse_result.op2 in self._BINARY_OPERATORS
            return self._BINARY_OPERATORS[parse_result.op2](parse_result[0], parse_result[2])

        # else lookup processing method based on name
        assert parse_result._name is not None
        method = parse_result._name
        if not hasattr(self, method):
            raise ParserError(f"{'.'.join(self.location)}: don't know how to deal with an element of type '{method}'")

        value = getattr(self, method)(*parse_result)

        if type(value) is UNSET and not allow_unset:
            raise SubstitutionError(f"{'.'.join(self.location)}: '{value.name}' is not defined")
        
        return value

    def evaluate(self, value, sublocation: List[str] = []):
        if type(value) is not str:
            return value

        loclen = len(self.location)
        self.location += sublocation

        try:
            if value.startswith("="):
                if value.startswith("=="):
                    return self._resolve(value[1:])
                else:
                    try:
                        parse_results = parse_string(value[1:])
                    except Exception as exc:
                        raise ParserError(f"{'.'.join(self.location)}: error parsing formula ({exc})")

                    return self._evaluate_result(parse_results)
            return self._resolve(value)
        finally:
            self.location = self.location[:loclen]
            

    def evaluate_dict(self, params: Dict[str, Any], 
                    corresponding_ns: typing.Optional[Dict[str, Any]] = None, 
                    defaults: Dict[str, Any] = {}, 
                    raise_substitution_errors: bool = True, verbose: bool =False):
        params = params.copy()
        for name, value in list(params.items()):
            if type(value) is not Unresolved:
                if verbose:
                    print(f"{name}: {value} ...")
                try:
                    new_value = self.evaluate(value, sublocation=[name])
                except SubstitutionError as err:
                    if raise_substitution_errors:
                        raise
                    new_value = Unresolved(errors=[err])
                if verbose:
                    print(f"{name}: {value} -> {new_value}")
                # UNSET return means delete or revert to default
                if new_value is UNSET:
                    if name in defaults:
                        params[name] = defaults[name]
                        if corresponding_ns:
                            corresponding_ns[name] = str(defaults[name])
                    else: 
                        del params[name]
                        if corresponding_ns and name in corresponding_ns:
                            del corresponding_ns[name]
                elif new_value is not value and new_value != value:
                    params[name] = new_value
                    if corresponding_ns:
                        corresponding_ns[name] = new_value
        return params

if __name__ == "__main__":
    pass
