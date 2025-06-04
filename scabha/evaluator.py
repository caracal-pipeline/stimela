
import glob
import os.path
import fnmatch
import pyparsing
pyparsing.ParserElement.enable_packrat()
from pyparsing import *
from pyparsing import common
from functools import reduce
import operator
import dataclasses

from .substitutions import SubstitutionError, SubstitutionContext
from .basetypes import Unresolved, UNSET
from .exceptions import *

import typing
from typing import Dict, List, Any
from omegaconf import DictConfig, ListConfig

_parser = None

# see https://stackoverflow.com/questions/43244861/pyparsing-infixnotation-optimization for a cleaner parser with functions

def _not_operator(value):
    return value is UNSET or isinstance(value, Unresolved) or not value

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
        if isinstance(arg, Unresolved):
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
        arg1, arg2 = evaluator._evaluate_result(self.arg1, allow_unset=self.allow_unset), \
                     evaluator._evaluate_result(self.arg2, allow_unset=self.allow_unset)
        if isinstance(arg1, Unresolved):
            return arg1
        if isinstance(arg2, Unresolved):
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

class GetItemHandler(ResultsHandler):
    def __init__(self, base):
        self.base, self.index = base[0], base[1]

    @staticmethod
    def pa(s, l, t):
        # https://stackoverflow.com/questions/4571441/recursive-expressions-with-pyparsing
        return GetItemHandler(*t)

    def evaluate(self, evaluator):
        base = evaluator._evaluate_result(self.base)
        index = evaluator._evaluate_result(self.index)
        if isinstance(base, Unresolved):
            return base
        if isinstance(index, Unresolved):
            return index
        return base[index]

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

    def evaluate_generic_callable(self, evaluator, name, callable, args, min_args=None, max_args=None):
        if min_args is not None and len(args) < min_args:
            raise FormulaError(f"{'.'.join(evaluator.location)}: {name}() expects at least {min_args} argument(s)")
        if max_args is not None and len(args) > max_args:
            raise FormulaError(f"{'.'.join(evaluator.location)}: {name}() expects at most {max_args} argument(s)")
        eval_args = [evaluator._evaluate_result(arg) for arg in args]
        # if any argument is UNSET, return it as our result
        unsets = [arg for arg in eval_args if isinstance(arg, Unresolved)]
        if unsets:
            return unsets[0]
        return callable(*eval_args)

    def ERROR(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: ERROR() expects one argument, got {len(args)}")
        cond = evaluator._evaluate_result(args[0], allow_unset=True)
        raise FormulaError(f"ERROR: {cond}")

    def LIST(self, evaluator, args):
        def make_list(*x):
            return list(x)
        return self.evaluate_generic_callable(evaluator, "LIST", make_list, args)

    def LEN(self, evaluator, args):
        def make_len(x):
            return len(x)
        return self.evaluate_generic_callable(evaluator, "LEN", make_len,
                                              args, min_args=1, max_args=1)

    def RANGE(self, evaluator, args):
        def make_range(*x):
            return list(range(*x))
        return self.evaluate_generic_callable(evaluator, "RANGE", make_range, args, min_args=1, max_args=3)
    
    def VALID(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: VALID() expects one argument, got {len(args)}")
        try:
            result = evaluator._evaluate_result(args[0], allow_unset=True)
        except (TypeError, ValueError) as exc:
            return False
        if isinstance(result, Unresolved):
            return False        
        return result

    def TRY(self, evaluator, args):
        for arg in args:
            result = evaluator._evaluate_result(arg, allow_unset=True)
            if not isinstance(result, Unresolved):
                return result
        return result

    def MIN(self, evaluator, args):
        return self.evaluate_generic_callable(evaluator, "MIN", min, args, min_args=1)

    def MAX(self, evaluator, args):
        return self.evaluate_generic_callable(evaluator, "MAX", max, args, min_args=1)
    
    def GETITEM(self, evaluator, args):
        def get_item(x, y):
            return x[y]
        return self.evaluate_generic_callable(evaluator, "GETITEM", get_item, args, min_args=2, max_args=2)

    def IS_STR(self, evaluator, args):
        def is_str(x):
            return type(x) is str
        return self.evaluate_generic_callable(evaluator, "IS_STR", is_str, args, min_args=1, max_args=1)
    
    def IS_NUM(self, evaluator, args):
        def is_num(x):
            return isinstance(x, (bool, int, float, complex)) 
        return self.evaluate_generic_callable(evaluator, "IS_NUM", is_num, args, min_args=1, max_args=1)
    
    def CASES(self, evaluator, args):
        # set default case
        if len(args)%2:
            default_case = args[-1]
            args = args[:-1]
        else: 
            default_case = None
        # return first True case
        for i in range(0, len(args), 2):
            conditional, result = args[i:i+2]
            cond = evaluator._evaluate_result(conditional, allow_unset=False)
            if cond:
                return evaluator._evaluate_result(result, allow_unset=True)
        # return default
        if default_case is None:
            return UNSET("no match in CASES()")
        return evaluator._evaluate_result(default_case, allow_unset=True)

    def IF(self, evaluator, args):
        if len(args) < 3 or len(args) > 4:
            raise FormulaError(f"{'.'.join(evaluator.location)}: IF() expects 3 or 4 arguments, got {len(args)}")
        conditional, if_true, if_false = args[:3]
        if_unset = args[3] if len(args) == 4 else UNSET("")

        cond = evaluator._evaluate_result(conditional, allow_unset=if_unset is not None)
        if isinstance(cond, Unresolved):
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
        if isinstance(value, Unresolved):
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
        if isinstance(pattern, Unresolved):
            return pattern
        return sorted(glob.glob(pattern))

    def EXISTS(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: EXISTS() expects 1 argument, got {len(args)}")
        pattern = evaluator._evaluate_result(args[0])
        if isinstance(pattern, Unresolved):
            return pattern
        return bool(glob.glob(pattern))

    def DIRNAME(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: DIRNAME() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if isinstance(path, Unresolved):
            return path
        if not isinstance(path, str):
            raise FormulaError(f"DIRNAME() expects a string, got a {str(type(path))}") 
        return os.path.dirname(path)

    def BASENAME(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: BASENAME() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if isinstance(path, Unresolved):
            return path
        if not isinstance(path, str):
            raise FormulaError(f"BASENAME() expects a string, got a {str(type(path))}") 
        return os.path.basename(path)

    def EXTENSION(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: EXTENSION() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if isinstance(path, Unresolved):
            return path
        if not isinstance(path, str):
            raise FormulaError(f"EXTENSION() expects a string, got a {str(type(path))}") 
        return os.path.splitext(path)[1]

    def STRIPEXT(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: STRIPEXT() expects 1 argument, got {len(args)}")
        path = evaluator._evaluate_result(args[0])
        if isinstance(path, Unresolved):
            return path
        if not isinstance(path, str):
            raise FormulaError(f"STRIPEXT() expects a string, got a {str(type(path))}") 
        return os.path.splitext(path)[0]

    def NOSUBST(self, evaluator, args):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: NOSUBST() expects 1 argument, got {len(args)}")
        return evaluator._evaluate_result(args[0], subst=False)
    
    def SORT(self, evaluator, args):
        return self._sort_impl(evaluator, args, "SORT")
    
    def RSORT(self, evaluator, args):
        return self._sort_impl(evaluator, args, "RSORT", reverse=True)

    def _sort_impl(self, evaluator, args, funcname, reverse=False):
        if len(args) != 1:
            raise FormulaError(f"{'.'.join(evaluator.location)}: {funcname}() expects 1 argument, got {len(args)}")
        sortlist = evaluator._evaluate_result(args[0])
        if isinstance(sortlist, Unresolved):
            return sortlist
        if not isinstance(sortlist, (list, tuple)):
            raise FormulaError(f"{funcname}() expects a list, got a {str(type(sortlist))}") 
        return sorted(sortlist, reverse=reverse)
    

def construct_parser():
    lparen = Literal("(").suppress()
    rparen = Literal(")").suppress()
    lbrack = Literal("[").suppress()
    rbrack = Literal("]").suppress()
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
    
    # functions -- get all all-uppercase members from FunctionHandler
    anyseq_funcnames = {"GLOB", "EXISTS", "ERROR"}
    all_funcnames = set(func for func in dir(FunctionHandler) 
                        if callable(getattr(FunctionHandler, func)) and func.upper() == func)
    all_funcnames -= anyseq_funcnames

    functions = reduce(operator.or_, map(Keyword, all_funcnames))
    # these functions take one argument, which could also be a sequence
    anyseq_functions = reduce(operator.or_, map(Keyword, anyseq_funcnames))

    atomic_value = (boolean | UNSET | EMPTY | nested_field | string | number)

    function_call_anyseq = Group(anyseq_functions + lparen + (expr | anyseq) + rparen).setParseAction(FunctionHandler.pa)
    function_call = Group(functions + lparen + 
                    Opt(delimited_list(expr|SELF)) + 
                    rparen).setParseAction(FunctionHandler.pa)

    operators = [
        ((lbrack + expr + rbrack), 1, opAssoc.LEFT, GetItemHandler.pa),
        (Literal("**"), 2, opAssoc.LEFT, BinaryHandler.pa), 
        (Literal("-")|Literal("+")|Literal("~"), 1, opAssoc.RIGHT, UnaryHandler.pa), 
        (Literal("*")|Literal("//")|Literal("/")|Literal("%"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("+")|Literal("-"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("<<")|Literal(">>"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("&"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("^"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (Literal("|"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (reduce(operator.or_, map(Literal, ("==", "!=", ">=", "<=", ">", "<"))), 2, opAssoc.LEFT, BinaryHandler.pa),
        (CaselessKeyword("in")|CaselessKeyword("not in"), 2, opAssoc.LEFT, BinaryHandler.pa),
        (CaselessKeyword("not"), 1, opAssoc.RIGHT, UnaryHandler.pa),
        (CaselessKeyword("and")|CaselessKeyword("or"), 2, opAssoc.LEFT, BinaryHandler.pa),
    ]

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

    def _resolve(self, value, in_formula=True, subst=True):
        if type(value) is str:
            if in_formula and value == "SELF":
                return SELF
            elif in_formula and value == "UNSET":
                return UNSET
            elif self.subst_context is not None and subst:
                try:
                    value = self.subst_context.evaluate(value, location=self.location)
                except (KeyError, AttributeError) as exc:
                    raise SubstitutionError(f"{value}: invalid key {exc}")
                except Exception as exc:
                    raise SubstitutionError(f"{value}: {exc}")
        return value

    def empty(self, *args, **kw):
        return ""

    def unset(self, *args, **kw):
        return UNSET

    def self_value(self, *args, **kw):
        return SELF

    def constant(self, value, subst=True, **kw):
        return self._resolve(value, subst=subst)

    def subexpression(self, value, subst=True):
        return self._evaluate_result(value, allow_unset=True, subst=subst)
    
    def namespace_lookup(self, *args, subst=True):
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
                value = value.get(fld, subst=subst)
            except (KeyError, AttributeError) as exc:
                raise SubstitutionError(f"{'.'.join(self.location)}: '{'.'.join(args)}' unresolved (at '{exc}')")
        return self._resolve(value, subst=subst)

    def _evaluate_result(self, parse_result, allow_unset=False, subst=True):
        allow_unset = allow_unset or self.allow_unresolved
        # if result is a handler, use evaluate
        if isinstance(parse_result, ResultsHandler):
            value = parse_result.evaluate(self)

        # if result is already a constant, resolve it
        elif type(parse_result) is not ParseResults:
            return self._resolve(parse_result, subst=subst)
        
        # lookup processing method based on name
        else:
            method = parse_result.getName()
            assert method is not None
            if not hasattr(self, method):
                raise ParserError(f"{'.'.join(self.location)}: don't know how to deal with an element of type '{method}'")

            try:
                value = getattr(self, method)(*parse_result, subst=subst)
            except SubstitutionError as exc:
                if allow_unset:
                    return UNSET("", [exc])
                raise

        if isinstance(value, Unresolved) and not allow_unset:
            raise UnsetError(f"'{value.value}' undefined", value.errors)

        return value

    def evaluate(self, value: Any, sublocation: List[str] = []) -> Any:
        """evaluates a single value, which can be a string with a formula and/or substitutions

        Args:
            value (Any): value to be evaluated. If not a str, returned as is
            sublocation (List[str]): location of value inside of nested container, as a list
                    of strings. Defaults to []

        Raises:
            ParserError: error parsing formula
            FormulaError: error evaluating formula
            SubstitutionError: unresolved symbol in formula or {}-substitution

        Returns:
            Any: result of evaluation
        """
        
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
                try:
                    return self._resolve(value, in_formula=False)
                except Exception as exc:
                    raise SubstitutionError(f"{'.'.join(self.location)}: evaluation of '{value}' failed", exc)
        finally:
            self.location = self.location[:loclen]

    def evaluate_object(self, obj: Any,
                        sublocation: List[str] = [],
                        raise_substitution_errors: bool = True, 
                        recursion_level: int = 1,
                        verbose: bool = False) -> Any:
        """evaluates object, which can be a nested container, in which case string elements
        are evaluated recursively. Evaluations are done in place.

        Args:
            obj (Any): object to evaluate
            sublocation (List[str]): location of value inside of nested container, as a list
                    of strings. Defaults to []
            raise_substitution_errors (bool, optional): raise substitution errors, instead of returning
                Unresolved values. Defaults to True.
            recursion_level (int, optional): Limits recursion level into subcontainers. Defaults to 1,
                which means recurse once.
            verbose (bool, optional): Prints debug messages. Defaults to False.

        Raises:
            SubstitutionError: error in substitutions

        Returns:
            Any: object with evaluations
        """
        # string? evaluate directly and return
        if type(obj) is str:
            try:
                value = self.evaluate(obj, sublocation=sublocation)
                if isinstance(value, Unresolved) and (raise_substitution_errors or not self.allow_unresolved):
                    raise SubstitutionError(f"{'.'.join(sublocation)}: unresolved substitution", value.errors)
                return value
            except AttributeError as err:
                if raise_substitution_errors or not self.allow_unresolved:
                    raise SubstitutionError(f"{'.'.join(sublocation)}: substitution error", [err])
                return Unresolved(errors=[err])
            except SubstitutionError as err:
                if raise_substitution_errors or not self.allow_unresolved:
                    raise SubstitutionError(f"{'.'.join(sublocation)}: substitution error", [err])
                return Unresolved(errors=[err])
            
        # helper function
        def update(value, sloc):
            if isinstance(value, Unresolved):
                return value, False
            subloc = sublocation + [sloc]
            if verbose: 
                print(f"{subloc}: {value} ...")
            new_value = self.evaluate_object(value, raise_substitution_errors=raise_substitution_errors,
                                                recursion_level=recursion_level, verbose=verbose,
                                                sublocation=subloc)
            if verbose:
                print(f"{subloc}: {value} -> {new_value}")
            # UNSET return means delete or revert to default
            if new_value is UNSET:
                raise SubstitutionError(f"{'.'.join(self.location + subloc)}: UNSET not allowed here")
            # compare
            if isinstance(value, (dict, DictConfig, list, ListConfig)) or dataclasses.is_dataclass(value):
                updated = value is not new_value
            else:
                updated = value != new_value
            return new_value, updated

        obj_out = obj
        # recurse into containers?
        if recursion_level:
            recursion_level -= 1
            # use evaluate_dict() to recurse into dicts
            if isinstance(obj, (dict, DictConfig)):
                for key, value in obj.items():
                    new_value, value_updated = update(value, key)
                    new_key = self.evaluate(key, sublocation=sublocation)
                    if new_key != key:
                        value_updated = True
                    if value_updated:
                        if obj_out is obj:
                            obj_out = obj.copy()
                        if new_key != key:
                            del obj_out[key]
                            key = new_key
                        obj_out[key] = new_value
            # recurse into lists
            elif isinstance(obj, (list, ListConfig)):
                for i, value in enumerate(obj):
                    new_value, updated = update(value, f"#{i}")
                    if updated:
                        if obj_out is obj:
                            obj_out = obj.copy()
                        obj_out[i] = new_value
            # recurse into dataclasses
            elif dataclasses.is_dataclass(obj):
                newvals = {}
                for fld in dataclasses.fields(obj):
                    value = getattr(obj, fld.name)
                    new_value, updated = update(value, fld.name)
                    if updated:
                        newvals[fld.name] = new_value
                if newvals:
                    obj_out = dataclasses.replace(obj, **newvals)

        return obj_out


    def evaluate_dict(self, params: Dict[str, Any], 
                    corresponding_ns: typing.Optional[Dict[str, Any]] = None, 
                    defaults: Dict[str, Any] = {}, 
                    sublocation = [],
                    raise_substitution_errors: bool = True, 
                    collapse_substitution_errors: bool = False,  # true for subcontainers
                    subcontainer_type: typing.Optional[str] = None,
                    recursive: bool = True,
                    verbose: bool = False) -> Dict[str, Any]:
        """evaluates dict of parameters, which can contain nested containers which can be evaluated recursively.
        Returns new dict.

        Args:
            params (Dict[str, Any]): parameters to evaluate
            corresponding_ns (Optional[Dict[str, Any]], optional): corresponding namespace, into which evluations
                are propagated. Defaults to None.
            defaults (Dict[str, Any], optional): dictionary of defaults; UNSET values are cause defaults to
                be substituted in.
            sublocation (list, optional): location inside of nested container, as a list
                of strings. Defaults to []
            raise_substitution_errors (bool, optional): raises substitution errors. Defaults to True.
                if False, substitution errors are assigned to dict as Unresolved values.
            collapse_substitution_errors (bool, optional): if True, a substituion error inside the dict
                causes an Unresolved value to be returned instead of the dict. Defaults to False.
            subcontainer_type (str): type of subcontainer being evaluated. Defaults to None. Must be set
                if collapse_substitution_errors is True.
            recursive (bool, optional): recurse into subcontainers. Defaults to True.
            verbose (bool, optional): print debug messages. Defaults to False.

        Returns:
            Dict[str, Any]: _descopy of input dict with substitutions performed
        """
        if collapse_substitution_errors:
            assert subcontainer_type is not None
        params_out = params
        for name, value in list(params.items()):
            if isinstance(value, Unresolved):
                continue
            # 
            retry = True
            while retry:
                retry = False
                if verbose: # or type(value) is UNSET:
                    print(f"{name}: {value} ...")
                if type(value) is str:
                    try:
                        new_value = self.evaluate(value, sublocation=sublocation + [name])
                    except (AttributeError, SubstitutionError, ParserError, FormulaError) as err:
                        if raise_substitution_errors:
                            raise
                        new_value = Unresolved(errors=[str(err)])
                    if verbose:
                        print(f"{name}: {value} -> {new_value}")
                    # UNSET return means delete or revert to default
                    if new_value is UNSET:
                        if subcontainer_type:
                            raise SubstitutionError(f"{'.'.join(self.location + sublocation)}: UNSET not allowed here")
                        if params_out is params:
                            params_out = params.copy()
                        # if value is in defaults and is different, try to evaluate that instead
                        if name in defaults and defaults[name] is not UNSET and defaults[name] != value:
                            value = params_out[name] = defaults[name]
                            if corresponding_ns:
                                corresponding_ns[name] = defaults[name]
                            retry = True
                        else: 
                            if name in params_out:
                                del params_out[name]
                            if corresponding_ns and name in corresponding_ns:
                                del corresponding_ns[name]
                    elif new_value is not value and new_value != value:
                        if params_out is params:
                            params_out = OrderedDict(**params)
                        params_out[name] = new_value
                        if corresponding_ns:
                            corresponding_ns[name] = new_value
                elif isinstance(value, (dict, DictConfig)) and recursive:
                    value = self.evaluate_dict(
                        value,
                        corresponding_ns,
                        defaults,
                        sublocation=sublocation + [name],
                        raise_substitution_errors=raise_substitution_errors,
                        collapse_substitution_errors=True, subcontainer_type="Dict",
                        recursive=True,
                        verbose=verbose
                    )
                    params_out[name] = value
                    if corresponding_ns:
                        corresponding_ns[name] = value
                elif isinstance(value, (list, ListConfig)) and recursive:
                    # convert list to dict, and evaluate
                    proxy_dict = self.evaluate_dict(
                                {f"[{i}]": v for i, v in enumerate(value)},
                                corresponding_ns,
                                defaults,
                                sublocation=sublocation + [name],
                                raise_substitution_errors=raise_substitution_errors,
                                collapse_substitution_errors=True, subcontainer_type="List",
                                recursive=True,
                                verbose=verbose
                            )
                    if isinstance(proxy_dict, Unresolved):
                        value = proxy_dict
                    else:
                        value = type(value)(list(proxy_dict.values()))
                    params_out[name] = value
                    if corresponding_ns:
                        corresponding_ns[name] = value

        if collapse_substitution_errors:
            errors = []
            for elem in params_out.values():
                if isinstance(elem, Unresolved):
                    errors += elem.errors
            if errors:
                return Unresolved(f"unresolved {subcontainer_type} elements", errors)

        return params_out



if __name__ == "__main__":
    pass
