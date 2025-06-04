import re, string
from collections import OrderedDict
from dataclasses import dataclass
from contextlib import contextmanager
from typing import Any, Dict, Optional, Union, List
import threading
import fnmatch

from .exceptions import Error, SubstitutionError, CyclicSubstitutionError
from .basetypes import EmptyDictDefault, Unresolved

from omegaconf import DictConfig

# thanks to https://gist.github.com/bgusach/a967e0587d6e01e889fd1d776c5f3729
def multireplace(string, replacements, ignore_case=False):
    """
    Given a string and a replacement map, it returns the replaced string.
    :param str string: string to execute replacements on
    :param dict replacements: replacement dictionary {value to find: value to replace}
    :param bool ignore_case: whether the match should be case insensitive
    :rtype: str
    """
    # If case insensitive, we need to normalize the old string so that later a replacement
    # can be found. For instance with {"HEY": "lol"} we should match and find a replacement for "hey",
    # "HEY", "hEy", etc.
    if ignore_case:
        def normalize_old(s):
            return s.lower()
        re_mode = re.IGNORECASE

    else:
        def normalize_old(s):
            return s
        re_mode = 0

    replacements = {normalize_old(key): val for key, val in replacements.items()}

    # Place longer ones first to keep shorter substrings from matching where the longer ones should take place
    # For instance given the replacements {'ab': 'AB', 'abc': 'ABC'} against the string 'hey abc', it should produce
    # 'hey ABC' and not 'hey ABc'
    rep_sorted = sorted(replacements, key=len, reverse=True)
    rep_escaped = map(re.escape, rep_sorted)

    # Create a big OR regex that matches any of the substrings to replace
    pattern = re.compile("|".join(rep_escaped), re_mode)

    # For each match, look up the new string in the replacements, being the key the normalized old string
    return pattern.sub(
        lambda match: replacements[normalize_old(match.group(0))], string
    )


class SubstitutionNS(OrderedDict):
    """Implements a namespace for {}-substitutions"""

    class StringWrapper(object):
        """Helper class used in forgiving substitutions: returns itself for all attribute access.

        This allows substitutions like "{x.y.z} xxx" to fail gracefully if "x" is missing. When the string formater
        looks up "x", it gets back a wrapper, in which it proceeds to look up "y" and "z", eventually evaluating to
        the given value that the wrapper was constructed with.
        """
        def __init__(self,  value):
            self.value = str(value)
        
        def __getitem__(self, key):
            return self

        def __getattr__(self, key):
            return self

        def __str__(self):
            return getattr(self, 'value')

    def __init__(self, **kw):
        """Initializes the namespace. Keywords are _add_'ed as items in the namespace"""
        name = kw.pop("_name_", [])
        nosubst = kw.pop("_nosubst_", False)
        super().__setattr__("_name_", name)
        super().__setattr__("_nosubst_", nosubst)
        SubstitutionNS._update_(self, **kw)

    def copy(self):
        newcopy = SubstitutionNS()
        OrderedDict.__setattr__(newcopy, "_name_", self._name_)
        OrderedDict.__setattr__(newcopy, "_nosubst_", set())
        for key, value in self.items():
            OrderedDict.__setitem__(newcopy, key, value)
        return newcopy

    def _update_(self, **kw):
        """Updates items in the namespace using _add_()"""
        for name, value in kw.items():
            SubstitutionNS._add_(self, name, value)

    def _merge_(self, ns):
        """Recursively merges in one namespace into another"""
        for name, value in ns.items():
            if name not in self:
                SubstitutionNS._add_(self, name, value)
            else:
                old_value = super().get(name)
                if isinstance(old_value, SubstitutionNS) and \
                    isinstance(value, (dict, OrderedDict, SubstitutionNS, DictConfig)):
                    old_value._merge_(value)
                else:
                    SubstitutionNS._add_(self, name, value)

    def _add_(self, name: str, value: Any, nosubst=False):
        """Adds an item to the namespace.

        Args:
            name (str): item key
            value (Any): item value. A dict or OrderedDict value becomes a SubstitutionNS automatically, with nosubst property
            nosubst (bool): use this as the nosubst property of the sub-namespace
        """
        # names with dots turned into sub-namespaces
        if '.' in name:
            subns_name, key = name.split('.', 1)
            if subns_name in self:
                subns = super().__getitem__(subns_name)
                if type(subns) is not SubstitutionNS:
                    raise TypeError(f"can't insert '{name}': {subns_name} is not a nested namespace")
            else:
                subns = SubstitutionNS(_nosubst_=nosubst or self._nosubst_, _name_=self._name_ + [subns_name])
            subns._add_(key, value)
            super().__setitem__(subns_name, subns)
        else:
            # a nested dict value becomes a substitution namespace automatically
            if type(value) in (dict, OrderedDict, DictConfig):
                value = SubstitutionNS(_nosubst_=nosubst or self._nosubst_, _name_=self._name_ + [name], **value)
            # if isinstance(value, SubstitutionNS):
            #     OrderedDict.__setattr__(v, "_props_", props)
            super().__setitem__(name, value)

    def __setattr__(self, name: str, value: Any) -> None:
        SubstitutionNS._add_(self, name, value)

    def __setitem__(self, k: str, v: Any) -> None:
        SubstitutionNS._add_(self, k, v)

    def get(self, name, default=None, subst=True):
        context = SubstitutionContext.current()
        # keep track of nested lookups, if doing substitutions
        nestloc = context.nested_location if context else None
        value = None # set to None, in case exception handler is invoked before value is set
        try:
            if nestloc is not None:
                nestloc.append(name)
                # see if this location is already being substituted -- this is a cyclic substitution
                for otherloc, otherfrom in context.loc_stack[:-1]:
                    if otherloc == nestloc:
                        raise CyclicSubstitutionError(context.loc_stack[-1][1], otherfrom)
            # check wildcards, substitute last
            if name not in self and ('*' in name or '?' in name):
                names = sorted(fnmatch.filter(super().keys(), name))
                if names:
                    name = names[-1]
            # now look up again
            if name in self:
                value = super().get(name)
                if context:
                    if context.raise_errors and isinstance(value, Unresolved):
                        raise SubstitutionError(f"unresolved substitution for {name} ({value})")
                    if subst and not self._nosubst_:
                        # recursive=False will invoke substitution on strings, but will return containers as is
                        value = context.evaluate(value, location=nestloc, recursive=False)
                return value
            elif default in (KeyError, AttributeError):
                raise default(name)
            else:
                return default
        # catch errors
        except Exception as exc:
            if context is not None:
                forgive = context.forgive_errors.get(type(exc))
                if type(forgive) is str:
                    return SubstitutionNS.StringWrapper(forgive.format(name=''.join(nestloc or []), value=value, target=name, exc=exc))
                elif forgive:
                    return SubstitutionNS.StringWrapper(f"({type(exc).__name__}: {exc})")
            # else re-raise exception
            raise

    def __getitem__(self, name):
        return self.get(name, KeyError)

    def __getattr__(self, name):
        return self.get(name, AttributeError)

    def _print_(self, prefix="", printfunc=print):
        for name, value in self.items():
            if name.startswith("_") or name.endswith("_"):
                continue
            if isinstance(value, SubstitutionNS):
                printfunc(f"{prefix}{name}:")
                value._print_(prefix + "  ")
            elif isinstance(value, Exception):
                printfunc(f"{prefix}{name}: ERR: {value}")
            else:
                printfunc(f"{prefix}{name}: {value}")


class SubstitutionFormatter(string.Formatter):
    def __init__(self, context: 'SubstitutionContext'):
        self.context = context
    
    def get_value(self, key, args, kwargs):
        return self.context.get_value(key, args, kwargs)


@dataclass
class SubstitutionContext(object):
    ns: Optional[SubstitutionNS]
    forgive_errors: Dict[Any, Optional[Union[str, bool]]] = EmptyDictDefault()
    raise_errors: bool = False

    def __post_init__(self):
        self.formatter = SubstitutionFormatter(self)
        # current substitution location. This is appended to with every nested attribute lookup
        self.nested_location = []
        # current location stack. A new element is inserted every time a nested substitution starts
        self.loc_stack = []
        # 
        self.enabled = True
        # list of erros and list of forgiven errors
        self.errors = []
        self.forgivens = []

    _current_contexts = {}

    @staticmethod
    def current() -> 'SubstitutionContext':
        return SubstitutionContext._current_contexts.get(threading.get_ident())

    def evaluate(self, value: Any, location: List[str] = [], recursive=True):
        """Formats value using the current substitution context

        Args:
            value (str): string to be formatted, or a container to be recursed into
            location:    list describing nested location of value, e.g. ['foo', 'bar', '1']
            recursive:   recurse into lists and dicts
        """
        if not self.enabled:
            raise SubstitutionError("substitution invoked outside of with clause")

        if not recursive and isinstance(value, (list, tuple, dict, OrderedDict)):
            return value

        # not a string, or an Error, or no "{" symbol -- return as is
        if self.ns is not None and isinstance(value, (str, list, tuple, dict, OrderedDict)):

            # an evaluate() call means a new substitution is being done. 
            # add a location list to the stack. This list will be appended to as we look up sub-attributes
            nesting = len(self.loc_stack)
            self.nested_location = []
            self.loc_stack.append((None, location))

            try:
                value = self._evaluate_element(value, location, nesting)
            finally:
                while self.loc_stack[-1][0] is not None:
                    self.loc_stack.pop()
                self.loc_stack.pop()
                self.nested_location = self.loc_stack[-1][0] if self.loc_stack else []

        return value

    def _evaluate_element(self, value: Any, location: List[str], nesting:int):
        newvalue = value
        if isinstance(value, str):
            if isinstance(value, Error) or "{" not in value:
                return value
            newvalue = self._evaluate_str(value, location, nesting)
        elif isinstance(value, (list, tuple)):
            for i, element in enumerate(value):
                newelement = self._evaluate_element(element, location + [str(i)], nesting)
                if newelement is not element:
                    if newvalue is value:
                        newvalue = list(value)
                    newvalue[i] = newelement
        elif isinstance(value, (dict, OrderedDict)):
            for key, element in value.items():
                newelement = self._evaluate_element(element, location + [key], nesting)
                if newelement is not element:
                    if newvalue is value:
                        newvalue = OrderedDict(value)
                    newvalue[key] = newelement
        return newvalue


    def _evaluate_str(self, value: str, location: List[str], nesting:int):
        try:
            # if we're doing a nested substitution, protect "{{" and "}}" from getting converted to a single brace by pre-replacing them
            if nesting:
                newvalue = multireplace(value, {"{{": "\u00AB", "}}": "\u00BB"})
            newvalue = self.formatter.format(value)
            if nesting:
                newvalue = multireplace(newvalue, {"\u00AB": "{{", "\u00BB": "}}"})
        except Exception as exc:
            # name is the object being formatted
            name = '.'.join(location)
            # target is the object that failed to be substituted in
            target = '.'.join(self.nested_location)
            # this gives us the current forgiveness policy for failed substitutions
            forgive = self.forgive_errors.get(type(exc))
            if type(forgive) is str:
                newvalue = forgive.format(name=name, value=value, target=target, exc=exc)
            elif forgive:
                newvalue = f"({exc})"
            # not forgiving error -- add to list in context, and raise if contexts wants us to raise
            else:
                locstr = f"{name}='{value}'" if name else f"'{value}'"
                if type(exc) is AttributeError:
                    err = SubstitutionError(
                        f"'{{{target}}} unresolved, in {locstr}"
                    )
                elif type(exc) is CyclicSubstitutionError:
                    err = SubstitutionError(
                        f"{{{target}}}: {exc}, in {locstr}"
                    )
                else:
                    err = SubstitutionError(
#                            f"{type(exc)} in {{{target}}}: {exc}, in {name}='{value}'"
                        f"{type(exc).__name__} at {{{target}}}: {exc}, in {locstr}"
                    )
                self.errors.append(err)
                if self.raise_errors:
                    raise
                return ''
            # dropped here, so forgive the error
            self.forgivens.append(name)
        return newvalue

            
    def get_value(self, key, args, kwargs):
        """Implements get_value for string formatter"""
        if type(key) is int:
            return args[key]
        elif key in kwargs:
            return kwargs[key]
        else:
            # this starts an attribute lookup, so remove previous lookup locations
            # a call to evaluate() will add a location of None, so rewind back to that
            while self.loc_stack[-1][0] is not None:
                self.loc_stack.pop()
            # add a new root location onto the stack
            self.nested_location = []
            self.loc_stack.append((self.nested_location, self.loc_stack[-1][1]))
            # now look up the attribute
            return self.ns.get(key, KeyError)


@contextmanager
def substitutions_from(ns: Optional[SubstitutionNS], raise_errors=False, forgive_errors: dict={}):
    thread = threading.get_ident()

    previous = SubstitutionContext._current_contexts.get(thread)
    current = SubstitutionContext(ns, forgive_errors=forgive_errors, raise_errors=raise_errors)

    SubstitutionContext._current_contexts[thread] = current

    try:
        yield current
    finally:
        SubstitutionContext._current_contexts[thread] = previous
        current.enabled = False


def forgiving_substitutions_from(ns: SubstitutionNS, forgive="", raise_errors=False):
    """"""
    forgive_errors = {err: forgive for err in [AttributeError, KeyError, TypeError, ValueError, SubstitutionError]}

    return substitutions_from(ns, raise_errors=raise_errors, forgive_errors=forgive_errors)



def perform_ll_substitutions(subst: Dict[str, Any], params: Dict[str, Any], 
                             raise_exceptions: bool=False) -> List[Exception]:
    """performs <<-substitutions on dict of parameters

    Args:
        subst (Dict[str, Any]): substitution ictionary
        params (Dict[str, Any]): parameter dictionary, modified in place
        raise_exceptions (bool, optional): raise exceptions on error. Defaults to False.

    Returns:
        List[Exception]: list of errors (if raise_exceptions is False), or [] on success
    """    

    errors = OrderedDict()
    repeat = True
    while repeat:
        repeat = False  # will reraise if need to run again (going to add that feature)
        
        for name, value in list(params.items()):
            if name not in errors and type(value) is str:
                # << is escape character -- maps back to single <
                if value.startswith("<<<"):
                    params[name] = value[1:]
                elif value.startswith("<<"):
                    target = value[2:].strip()
                    if not target:
                        exc = SubstitutionError(f"{name} = << {value}: has no <<-substitution target")
                        if raise_exceptions:
                            raise exc
                        errors[name] = exc
                        continue
                    # check for ?value :value and !value specifiers
                    components = target.split(" ")
                    true_value = false_value = undef_value = exc = None
                    if len(components) > 1:
                        target = components[0]
                        for comp in components[1:]:
                            if comp.startswith("?"):
                                true_value = comp[1:]
                            elif comp.startswith(":"):
                                false_value = comp[1:]
                            elif comp.startswith("!"):
                                undef_value = comp[1:]
                                # add trailing ? to target if it was missing
                                if '?' not in target:
                                    target = target + "?"
                            else:
                                exc = SubstitutionError(f"{name} = '{value}': invalid component '{comp}'")
                                break
                    if exc is not None:
                        if raise_exceptions:
                            raise exc
                        errors[name] = exc
                        continue
                    keys = target.split('.')
                    # look up keys in substitution dict recursively
                    # a ?-lookup is optional
                    target_value = subst
                    while keys:
                        key = keys.pop(0)
                        optional = key.endswith('?')
                        if optional:
                            key = key[:-1]
                        target_value = target_value.get(key, SubstitutionError)
                        if target_value is SubstitutionError:
                            break
                    # repeat loop eventually, since something has changed
                    repeat = True
                    # if a lookup has failed -- for optional lookups, delete the parameter, otherwise
                    # report error
                    if target_value is SubstitutionError:
                        if optional:
                            if undef_value is not None:
                                params[name] = undef_value
                            else:
                                del params[name]
                        else:
                            exc = SubstitutionError(f"{name} = '{value}': lookup failed for '{key}'")
                            if raise_exceptions:
                                raise exc
                            errors[name] = exc
                        continue
                    # success!
                    if target_value:
                        params[name] = true_value if true_value is not None else target_value
                    else:
                        params[name] = false_value if false_value is not None else target_value
                
    return list(errors.values())
