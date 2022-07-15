import sys
from typing import List, Union
from typing import Optional as _Optional
from types import TracebackType

logger = None

def set_logger(log):
    global logger
    logger = log


class Error(str):
    """A string that's marked as an error"""
    pass

ALWAYS_REPORT_TRACEBACK = True

class ScabhaBaseException(Exception):
    def __init__(self, message: str, 
                 nested: _Optional[Union[Exception, TracebackType, List[Union[Exception, TracebackType]]]] = None, log=None, tb=False):
        """Initializes exception object

        Args:
            message (str): error message
            nested (_Optional[Union[Exception, List[Exception]]]): Nested exception(s). Defaults to None.
            log (logger): if not None, logs the exception to the given logger
        """
        self.message = message
        # include traceback automatically?
        if isinstance(nested, Exception) and (tb or ALWAYS_REPORT_TRACEBACK) and nested is sys.exc_info()[1]:
            nested = [nested, sys.exc_info()[2]]
        if isinstance(nested, (Exception, TracebackType)):
            nested = [nested]
        self.nested = nested or []
        nested_exc = [str(exc) for exc in self.nested if isinstance(exc, Exception)]
        if nested_exc:
            message = f"{message}: {', '.join(nested_exc)}"
        Exception.__init__(self, message)
        if log is not None:
            if not hasattr(log, 'error'):
                log = logger
            if log is not None:
                log.error(message)
        self.logged = log is not None

class ConfigError(ScabhaBaseException):
    pass

class SchemaError(ScabhaBaseException):
    pass

class NestedSchemaError(SchemaError):
    pass

class DefinitionError(ScabhaBaseException):
    pass

class StepValidationError(ScabhaBaseException):
    pass

class ParameterValidationError(ScabhaBaseException):
    pass

class SubstitutionError(ScabhaBaseException):
    pass

class UnsetError(ScabhaBaseException):
    def __init__(self, message, element, log=None):
        super().__init__(message, log)
        self.element = element

class ParserError(ScabhaBaseException):
    pass

class FormulaError(ScabhaBaseException):
    pass

class CyclicSubstitutionError(SubstitutionError):
    def __init__(self, location: List[str], other_location: List[str]):
        self.location = ".".join(location)
        self.other_location = ".".join(other_location)
        super().__init__(f"'{{{self.location}}}' is a cyclic substition")

class SubstitutionErrorList(ScabhaBaseException):
    def __init__(self, *errors):
        self.errors = errors
        super().__init__(f"{len(errors)} substitution error(s): {'; '.join(map(str, errors))}")