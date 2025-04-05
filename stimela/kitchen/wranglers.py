import re, logging, json, yaml
from typing import Any, List, Dict, Optional, Union
from omegaconf import ListConfig


from scabha.cargo import ListOrString
from stimela.exceptions import CabValidationError, StimelaCabOutputError, \
                    StimelaCabRuntimeError
from stimela.stimelogging import FunkyMessage

# wranglers specified as a single string, or a list
WranglerSpecList = ListOrString

# defined just for type hinting below
CabStatus = "stimela.kitchen.cab.Cab.RuntimeStatus"


def create(regex: re.Pattern, spec: str):
    """Creates wrangler object from a regex and a specification string, using the spec
    string to look up a wrangler class below.

    Args:
        regex (re.Pattern): pattern in output which wrangler will match
        spec (str): specification string

    Raises:
        CabValidationError: if the specification is incorrect

    Returns:
        wrangler: a wrangler object
    """
    for specifier, wrangler_class in all_wranglers.items():
        match = re.fullmatch(specifier, spec)
        if match:
            return wrangler_class(regex, spec, **match.groupdict())
    raise CabValidationError(f"'{regex.pattern}': '{spec}' is not a valid wrangler specifier")


def create_list(pattern: Union[str, re.Pattern], spec_list: WranglerSpecList):
    """Creates list of wranglers based on list of specifiers.

    Args:
        pattern (Union[str, re.Pattern]): pattern in output which wrangler will match
        spec_list (WranglerSpecList): list of wrangler specs

    Raises:
        CabValidationError: if a specification is incorrect, or pattern is not a valid regex

    Returns:
        (re.Pattern, list): compiled regex pattern, plus list of wrangler objects
    """
    if type(pattern) is str:
        try:
            regex = re.compile(pattern)
        except Exception as exc:
            raise CabValidationError(f"wrangler pattern '{pattern}' is not a valid regular expression", exc)
    else:
        regex = pattern
        pattern = regex.pattern
    
    if type(spec_list) is str:
        spec_list = [spec_list]
    if not isinstance(spec_list, (list, tuple, ListConfig)):
        raise CabValidationError(f"wrangler entry '{pattern}': expected list of wranglers")

    return regex, [create(regex, spec) for spec in spec_list]


class _BaseWrangler(object):
    """Abstract base class for wrangler action classes.
    
    Each action class has a static 'specifier' attribute, which is a pattern used to match (and parse)
    the specifier for that action. The pattern may contain named ()-groups (see e.g. Replace below),
    note that the create() function above maps these to keyword arguments of the constructor.
    """

    specifier = None

    def __init__(self, regex: re.Pattern, spec: str, **kw):
        """Action class constructor. Copies all keywords into object attributes.

        Args:
            regex (re.Pattern): pattern object which triggers this action
            spec (str):         original action specifier string 
            **kw (Any):         converted to object attributes
        """
        self.regex, self.spec = regex, spec
        for key, value in kw.items():
            setattr(self, key, value)

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        """Abstract method. Applies the wrangler to a line of cab output.

        Args:
            cabstat (CabStatus): cab runtime status object, which may be updated based on output
            output (str): line of output
            match (re.Match): match object resulting from pattern matching
        
        Returns:
            output (str or None), severity (int or None): [possibly] modified output string, or None to suppress
                                                          modified log severity level, or None to not modify
        """
        raise RuntimeError("derived class does not implement an apply() method")

class Replace(_BaseWrangler):
    """
    This wrangler will replace the matching pattern with the given string. Specified as REPLACE:replacement.
    Uses re.sub() internally, so look that up for more complex usage.
    """ 

    specifier = "REPLACE:(?P<replace>.*)"
    
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return self.regex.sub(self.replace, output), None

class Highlight(_BaseWrangler):
    """
    This wrangler will highlight the matching pattern using the given rich markup. Specified as REPLACE:replacement.
    Uses re.sub() internally, so look that up for more complex usage.
    """
    specifier = "HIGHLIGHT:(?P<style>.*)"
    
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return self.regex.sub(f"[{self.style}]\\g<0>[/{self.style}]", output), None


class ChangeSeverity(_BaseWrangler):
    """
    This wrangler will cause the output to be reported at the given severity level (instead of the
    normal INFO level). Specified as SEVERITY:level, where level is one of the symbolic levels
    defined in the standard logging module.
    """
    specifier = "SEVERITY:(?P<severity>ERROR|WARNING|INFO|DEBUG|CRITICAL|FATAL)"
    
    def __init__(self, regex: re.Pattern, spec: str, severity: str):
        super().__init__(regex, spec, severity=severity)
        level = getattr(logging, severity, None)
        if level is None:
            raise CabValidationError(f"wrangler action '{spec}' for '{regex.pattern}': invalid logging level")
        self.severity = level

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return output, self.severity

class Suppress(_BaseWrangler):
    """
    This wrangler will cause the matching output to be suppressed. Specified as simply SUPPRESS.
    """
    specifier = "SUPPRESS"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return None, None

class DeclareWarning(_BaseWrangler):
    """
    This wrangler will issue a warning once the cab completes. Specified as WARNING:message
    """
    specifier = "WARNING:(?P<message>.*)"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        cabstat.declare_warning(self.message)
        return output, logging.WARNING

class DeclareError(_BaseWrangler):
    """
    This wrangler will mark the cab as failed (even if the exit code is 0), and issue an optional error.
    Specified as ERROR[:message]
    """
    specifier = "ERROR(?::(?P<message>.*))?"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        if self.message:
            message = self.message.format(**match.groupdict())
        else:
            message = f"cab marked as failed based on encountering '{self.regex.pattern}' in output"
        cabstat.declare_failure(StimelaCabRuntimeError(message))
        return FunkyMessage(f"[bold red]{output}[/bold red]", output), logging.ERROR

class DeclareSuccess(_BaseWrangler):
    """
    This wrangler will mark the cab as succeeded (even if the exit code is non 0).
    Specified as DECLARE_SUCCESS.
    """
    specifier = "DECLARE_SUCCESS"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        cabstat.declare_success()
        return FunkyMessage(f"[bold green]{output}[/bold green]", output, 
            prefix=FunkyMessage(":white_check_mark:","+")), None

class ParseOutput(_BaseWrangler):
    """
    This wrangler will parse a named output parameter out of the output. Specified as 
    PARSE_OUTPUT:name:type or PARSE_OUTPUT:name:group:type. 
    In the first case, the matching pattern must have a ()-group with the same name.
    In the second case, a ()-group name (or number) is explicitly specified.
    Type can be an atomic Python type (str, bool, int, float, complex), or 'json' to use
    json.loads(), or 'yaml' to use yaml.safe_load
    """

    loaders = dict(str=str, bool=bool, int=int, float=float, complex=complex, 
        json=json.loads, JSON=json.loads,
        yaml=yaml.safe_load, YAML=yaml.safe_load)
    specifier = f"PARSE_OUTPUT:((?P<name>.*):)?(?P<group>.*):(?P<dtype>{'|'.join(loaders.keys())})"

    def __init__(self, regex: re.Pattern, spec: str, name: Optional[str], group: str, dtype: str):
        super().__init__(regex, spec, name=name)
        self.loader = self.loaders[dtype]
        self.name = name or group
        if group in regex.groupindex:
            self.gid = group
        elif re.fullmatch(r'\d+', group):
            gid = int(group)
            if gid > regex.groups:
                raise CabValidationError(f"wrangler action '{spec}' for '{regex.pattern}': {gid} is not a valid ()-group")
            self.gid = gid
        else:
            raise CabValidationError(f"wrangler action '{spec}' for '{regex.pattern}': {group} is not a valid ()-group")

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        value = match[self.gid]
        try:
            value = self.loader(value)
        except Exception as exc:
            cabstat.declare_failure(StimelaCabOutputError(f"error parsing string \"{value}\" for output '{self.name}'", exc))
        cabstat.declare_outputs({self.name: value})
        return output, None

class ParseJSONOutput(_BaseWrangler):
    """
    This wrangler will parse multiple named output parameters (using JSON) out of the output. Specified as 
    PARSE_JSON_OUTPUTS.
    The pattern is expected to have one or more named ()-groups, which are matched to parameters.
    """

    specifier = "PARSE_JSON_OUTPUTS"

    def __init__(self, regex: re.Pattern, spec: str):
        super().__init__(regex, spec)
        self.names = regex.groupindex.keys()
        if not self.names:
            raise CabValidationError(f"wrangler action '{spec}' for '{regex.pattern}': no ()-groups")

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        outputs = {}
        for name, value in match.groupdict().items():
            if value is not None:
                try:
                    outputs[name] = json.loads(value)
                except Exception as exc:
                    cabstat.declare_failure(StimelaCabOutputError(f"error parsing string \"{value}\" for output '{name}'", exc))
        cabstat.declare_outputs(outputs)
        return output, None


class ParseJSONOutputDict(_BaseWrangler):
    """
    This wrangler will parse a dict of output parameters (using JSON) out of the output. Specified as 
    PARSE_JSON_OUTPUT_DICT.
    The first ()-group from the pattern will be parsed.
    """

    specifier = "PARSE_JSON_OUTPUT_DICT"

    def __init__(self, regex: re.Pattern, spec: str):
        super().__init__(regex, spec)
        if regex.groups < 1:
            raise CabValidationError(f"wrangler action '{spec}' for '{regex.pattern}': no ()-groups")

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        try:
            outputs = json.loads(match.group(1))
        except Exception as exc:
            cabstat.declare_failure(StimelaCabOutputError(f"error parsing output dict from \"{self.name}'\"", exc))
        cabstat.declare_outputs(outputs)
        return output, None


# build dictionary of all available wrangler action classes

all_wranglers = {
    cls.specifier: cls for _, cls in vars().items()
        if isinstance(cls, type) and issubclass(cls, _BaseWrangler) and cls.specifier is not None
}



