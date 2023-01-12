import glob
import os.path
import fnmatch
import pyparsing
pyparsing.ParserElement.enable_packrat()
from pyparsing import *
from pyparsing import common
from functools import reduce
import operator

from .substitutions import SubstitutionError, SubstitutionContext
from .basetypes import Unresolved, UNSET
from .exceptions import *

import typing
from typing import Dict, List, Any


_parser = None

# see https://stackoverflow.com/questions/43244861/pyparsing-infixnotation-optimization for a cleaner parser with functions

def _not_operator(value):
    return value is UNSET or type(value) is UNSET or not value

_UNARY_OPERATORS = {
    '+':    lambda x: +x,
    '-':    lambda x: -x,
    '~':    lambda x: ~x,
    'not':  _not_operator
}

_BINARY_OPERATORS = {
    '**':   lambda x,y: x ** y,
    '*':   lambda x,y: x * y,
    '/':   lambda x,y: x / y,
    '//':   lambda x,y: x // y,
    '+':   lambda x,y: x + y,
    '-':   lambda x,y: x - y,
    '<<':   lambda x,y: x << y,
    '>>':   lambda x,y: x >> y,
    '&':   lambda x,y: x & y,
    '^':   lambda x,y: x ^ y,
    '|':   lambda x,y: x | y,
    '==':   lambda x,y: x == y,
    '!=':   lambda x,y: x != y,
    '<=':   lambda x,y: x <= y,
    '<':   lambda x,y: x < y,
    '>=':   lambda x,y: x >= y,
    '>':   lambda x,y: x > y,
    # 'is':   lambda x,y: x is y,
    # 'is not':   lambda x,y: x is not y,
    'in':   lambda x,y: x in y,
    'not in':   lambda x,y: x not in y,
    'and':   lambda x,y: x and y,
    'or':   lambda x,y: x or y,
}        


class ResultsHandler(object):
    def evaluate(evaluator):
        pass

    def dump(self):
        pass

    @classmethod
    def pa(cls, s, l, t):
        return cls(*t[0])


class UnaryHandler(ResultsHandler):
    def __init__(self, op, arg):
        self.op, self.arg = op, arg
        assert op in _UNARY_OPERATORS
        self._op = _UNARY_OPERATORS[op]
        # not allows UNSET values
        self.allow_unset = (op == 'not')
    
    def evaluate(self, evaluator):
        arg = evaluator._evaluate_result(self.arg, allow_unset=self.allow_unset)
        if type(arg) is UNSET:
            return arg
        return self._op(arg)

    def dump(self):
        return f"UnaryHandler({self.op}\n   {self.arg})"


class BinaryHandler(ResultsHandler):
    def __init__(self, arg1, op, arg2):
        self.op, self.arg1, self.arg2 = op, arg1, arg2
        assert op in _BINARY_OPERATORS
        self._op = _BINARY_OPERATORS[op]
        self.allow_unset = False
    
    def evaluate(self, evaluator):
        arg1, arg2 =    evaluator._evaluate_result(self.arg1, allow_unset=self.allow_unset), \
                        evaluator._evaluate_result(self.arg2, allow_unset=self.allow_unset)
        if type(arg1) is UNSET:
            return arg1
        if type(arg2) is UNSET:
            return arg2
        return self._op(arg1, arg2)

    def dump(self):
        return f"BinaryHandler({self.arg1},\n   {self.op},\n{self.arg2})"

    @staticmethod
    def pa(s, l, t):
        # https://stackoverflow.com/questions/4571441/recursive-expressions-with-pyparsing
        initlen, incr = 3, 2
        t = t[0]
        ret = BinaryHandler(*t[:initlen])
        i = initlen
        while i < len(t):
            ret = BinaryHandler(ret, *t[i:i+incr])
            i += incr
        return ret

class FunctionHandler(ResultsHandler):
    def __init__(self, func, *args):
        self.func, self.args = func, args
        self._func = getattr(self, func)
        if self._func is None:
            raise FormulaError(f"unknown function {func}")

    @staticmethod
    def pa(s, l, t):
        # https://stackoverflow.com/questions/4571441/recursive-expressions-with-pyparsing
        return FunctionHandler(*t[0])

    def evaluate(self, evaluator):
        return self._func(evaluator, self.args)

    def LIST(self, evaluator, args):
        return [evaluator._evaluate_result(value) for value in args]

    def IF(self, evaluator, args):
        if len(args) < 3 or len(args) > 4:
            raise FormulaError(f"{'.'.join(evaluator.location)}: IF() expects 3 or 4 arguments, got {len(args)}")
        conditional, if_true, if_false = args[:3]
        if_unset = args[3] if len(args) == 4 else UNSET("")

        cond = evaluator._evaluate_result(conditional, allow_unset=if_unset is not None)
        if type(cond) is UNSET:
            if if_unset is None:
                raise SubstitutionError(f"{'.'.join(evaluator.location)}: '{cond.name}' is not defined")
            result = if_unset
        else:
            result = if_true if cond else if_false    
            
        return evaluator._evaluate_result(result)

    def IFSET(self, evaluator, args):
        if len(args) < 1 or len(args) > 3:
            raise FormulaError(f"{'.'.join(evaluator.location)}: IFSET() expects 1 to 3 arguments, got {len(args)}")
        
        lookup, if_set, if_unset = list(args) + [None]*(3 - len(args))

        value = evaluator._evaluate_result(lookup, allow_unset=True)
        if type(value) is UNSET:
            if is_missing(if_unset):
                return UNSET
            else:
                return evaluator._evaluate_result(if_unset)
        elif is_missing(if_set) or if_set == 'SELF':
            return value
        else:
            return evaluator._evaluate_result(if_set)            

    def GLOB(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: GLOB() expects 1 argument, got {len(args)}")
        pattern = evaluator._evaluate_result(args[0])
        if type(pattern) is UNSET:
            return pattern
        return sorted(glob.glob(pattern))

    def EXISTS(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: EXISTS() expects 1 argument, got {len(args)}")
        pattern = evaluator._evaluate_result(args[0])
        if type(pattern) is UNSET:
            return pattern
        return bool(glob.glob(pattern))

    def DIRNAME(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: DIRNAME() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if type(path) is UNSET:
            return path
        return os.path.dirname(str(path))

    def BASENAME(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: BASENAME() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if type(path) is UNSET:
            return path
        return os.path.basename(str(path))

    def EXTENSION(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: EXTENSION() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if type(path) is UNSET:
            return path
        return os.path.splitext(str(path))[1]

    def STRIPEXT(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: STRIPEXT() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if type(path) is UNSET:
            return path
        return os.path.splitext(str(path))[0]

def construct_parser():
    lparen = Literal("(").suppress()
    rparen = Literal(")").suppress()
    lbrack = Keyword("[").suppress()
    rbrack = Keyword("]").suppress()
    comma = Literal(",").suppress()
    period = Literal(".").suppress()
    string = (QuotedString('"') | QuotedString("'"))("constant")
    UNSET = Keyword("UNSET")("unset")
    SELF = Keyword("SELF")("self_value")
    EMPTY = Keyword("EMPTY")("empty")
    bool_false = (Keyword("False") | Keyword("false"))("bool_false").set_parse_action(lambda:[False])
    bool_true = (Keyword("True") | Keyword("true"))("bool_true").set_parse_action(lambda:[True])

    boolean = (bool_true | bool_false)("constant")
    number = common.number("constant")

    fieldname = Word(alphas + "_", alphanums + "_-@*?")
    nested_field = Group(fieldname + OneOrMore(period + fieldname))("namespace_lookup")
    anyseq = CharsNotIn(",)")("constant")

    # allow expression to be used recursively
    expr = Forward()
    
    # functions
    functions = reduce(operator.or_, map(Keyword, ["IF", "IFSET", "GLOB", "EXISTS", "LIST", "BASENAME", "DIRNAME", "EXTENSION", "STRIPEXT"]))
    # these functions take one argument, which could also be a sequence
    anyseq_functions = reduce(operator.or_, map(Keyword, ["GLOB", "EXISTS"]))

    atomic_value = (boolean | UNSET | EMPTY | nested_field | string | number)

    function_call_anyseq = Group(anyseq_functions + lparen + anyseq + rparen).setParseAction(FunctionHandler.pa)
    function_call = Group(functions + lparen + 
                    Opt(delimited_list(expr|SELF)) + 
                    rparen).setParseAction(FunctionHandler.pa)
    operators = (
        (Literal("**"), 2, opAssoc.LEFT, BinaryHandler.pa), 
        (Literal("-")|Literal("+")|Literal("~"), 1, opAssoc.RIGHT, UnaryHandler.pa), 
        (Literal("*")|Literal("//")|Literal("/")|Literal("%"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("+")|Literal("-"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("<<")|Literal(">>"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("&"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("^"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("|"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (reduce(operator.or_, map(Literal, ("==", "!=", ">", "<", ">=", "<="))), 2, opAssoc.LEFT, BinaryHandler.pa),
        (CaselessKeyword("in")|CaselessKeyword("not in"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (CaselessKeyword("not"), 1, opAssoc.RIGHT, UnaryHandler.pa),
        (CaselessKeyword("and")|CaselessKeyword("or"), 2, opAssoc.LEFT, BinaryHandler.pa),
    )

    infix = infix_notation(atomic_value | function_call | function_call_anyseq | nested_field,
                            operators)("subexpression")

    expr <<= infix

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

def is_missing(result):
    return result is None

class SELF(object):
    pass


class Evaluator(object):
    def __init__(self,  ns: Dict[str, Any], 
                        subst_context: typing.Optional[SubstitutionContext] = None, 
                        allow_unresolved: bool = False,
                        location: List[str] = []):
        self.ns = ns
        self.subst_context = subst_context
        self.location = location
        self.allow_unresolved = allow_unresolved

    def _resolve(self, value, in_formula=True):
        if type(value) is str:
            if in_formula and value == "SELF":
                return SELF
            elif in_formula and value == "UNSET":
                return UNSET
            elif self.subst_context is not None:
                try:
                    value = self.subst_context.evaluate(value, location=self.location)
                except (KeyError, AttributeError) as exc:
                    raise SubstitutionError(f"{value}: invalid key {exc}")
                except Exception as exc:
                    raise SubstitutionError(f"{value}: {exc}")
        return value

    def empty(self, *args):
        return ""

    def unset(self, *args):
        return UNSET

    def self_value(self, *args):
        return SELF

    def constant(self, value):
        return self._resolve(value)

    def subexpression(self, value):
        return self._evaluate_result(value, allow_unset=True)
    
    def namespace_lookup(self, *args):
        if len(args) == 1 and type(args[0]) is ParseResults:
            args = args[0]
            assert args._name == "namespace_lookup"
        value = self.ns
        fields = list(args)
        while fields:
            fld = fields.pop(0)
            # check for wildcards
            if fld not in value and ('*' in fld or '?' in fld):
                names = sorted(fnmatch.filter(value.keys(), fld))
                if names:
                    fld = names[-1]
            # last element allowed to be UNSET, otherwise substitution error
            if fld not in value:
                if fields:
                    raise SubstitutionError(f"{'.'.join(self.location)}: '{fld}' undefined (in '{'.'.join(args)}')")
                else:
                    return UNSET('.'.join(args))
            # this can still throw an error if a nested interpolation is invoked
            try:
                value = value[fld]
            except (KeyError, AttributeError) as exc:
                raise SubstitutionError(f"{'.'.join(self.location)}: '{'.'.join(args)}' unresolved (at '{exc}')")
        return self._resolve(value)

    def _evaluate_result(self, parse_result, allow_unset=False):
        allow_unset = allow_unset or self.allow_unresolved
        # if result is a handler, use evaluate
        if isinstance(parse_result, ResultsHandler):
            value = parse_result.evaluate(self)

        # if result is already a constant, resolve it
        elif type(parse_result) is not ParseResults:
            return self._resolve(parse_result)
        
        # lookup processing method based on name
        else:
            method = parse_result.getName()
            assert method is not None
            if not hasattr(self, method):
                raise ParserError(f"{'.'.join(self.location)}: don't know how to deal with an element of type '{method}'")

            try:
                value = getattr(self, method)(*parse_result)
            except SubstitutionError as exc:
                if allow_unset:
                    return UNSET("", [exc])
                raise

        if type(value) is UNSET and not allow_unset:
            raise UnsetError(f"'{value.value}' undefined")

        return value

    def evaluate(self, value, sublocation: List[str] = []):
        if type(value) is not str:
            return value

        loclen = len(self.location)
        self.location += sublocation

        try:
            if value.startswith("="):
                if value.startswith("=="):
                    return self._resolve(value[1:], in_formula=False)
                else:
                    try:
                        parse_results = parse_string(value[1:])
                    except Exception as exc:
                        raise ParserError(f"{'.'.join(self.location)}: error parsing formula '{value}'", exc)

                    try:
                        return self._evaluate_result(parse_results, allow_unset=True)
                    except Exception as exc:
                        raise FormulaError(f"{'.'.join(self.location)}: evaluation of '{value}' failed", exc, tb=True)
            else:
                return self._resolve(value, in_formula=False)
        finally:
            self.location = self.location[:loclen]
            

    def evaluate_dict(self, params: Dict[str, Any], 
                    corresponding_ns: typing.Optional[Dict[str, Any]] = None, 
                    defaults: Dict[str, Any] = {}, 
                    raise_substitution_errors: bool = True, 
                    verbose: bool =False):
        params = params.copy()
        for name, value in list(params.items()):
            if type(value) is not Unresolved:
                retry = True
                while retry:
                    retry = False
                    if verbose: # or type(value) is UNSET:
                        print(f"{name}: {value} ...")
                    try:
                        new_value = self.evaluate(value, sublocation=[name])
                    except AttributeError as err:
                        if raise_substitution_errors:
                            raise
                        new_value = Unresolved(errors=[err])
                    except SubstitutionError as err:
                        if raise_substitution_errors:
                            raise
                        new_value = Unresolved(errors=[err])
                    if verbose:
                        print(f"{name}: {value} -> {new_value}")
                    # UNSET return means delete or revert to default
                    if new_value is UNSET:
                        # if value is in defaults, try to evaluate that instead
                        if name in defaults and defaults[name] is not UNSET:
                            value = params[name] = defaults[name]
                            if corresponding_ns:
                                corresponding_ns[name] = str(defaults[name])
                            retry = True
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
