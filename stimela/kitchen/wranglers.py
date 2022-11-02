import re, logging, json
from typing import Any, List, Dict, Optional, Union
from omegaconf import ListConfig

from scabha.cargo import ListOrString
from stimela.exceptions import CabValidationError, StimelaCabOutputError, StimelaCabRuntimeError


WranglerSpecList = ListOrString

# just for type hinting
CabStatus = "stimela.kitchen.cab.Cab.RuntimeStatus"

def create(regex: re.Pattern, spec: str):
    for specifier, wrangler_class in all_wranglers.items():
        match = re.fullmatch(specifier, spec)
        if match:
            return wrangler_class(regex, spec, **match.groupdict())
    raise CabValidationError(f"'{regex.pattern}': '{spec}' is not a valid wrangler specifier")


def create_list(pattern: Union[str, re.Pattern], spec_list: WranglerSpecList):
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
    specifier = None

    def __init__(self, regex: re.Pattern, action: str, **kw):
        self.regex = regex
        for key, value in kw.items():
            setattr(self, key, value)

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return output, None

class Replace(_BaseWrangler):
    specifier = "REPLACE:(?P<replace>.*)"
    
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return self.regex.sub(self.replace, output), None

class ChangeSeverity(_BaseWrangler):
    specifier = "SEVERITY:(?P<severity>ERROR|WARNING|INFO)"
    
    def __init__(self, regex: re.Pattern, action: str, severity: str):
        super().__init__(regex, action, severity=severity)
        level = getattr(logging, severity, None)
        if level is None:
            raise CabValidationError(f"wrangler action '{action}' for '{regex.pattern}': invalid logging level")
        self.severity = level

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return output, self.severity

class Suppress(_BaseWrangler):
    specifier = "SUPPRESS"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        return None, None

class DeclareWarning(_BaseWrangler):
    specifier = "WARNING:(?P<message>.*)"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        cabstat.declare_warning(self.message)
        return output, logging.WARNING

class DeclareError(_BaseWrangler):
    specifier = "ERROR(?::(?P<message>.*))?"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        err = StimelaCabRuntimeError(self.message or 
            f"cab marked as failed based on encountering '{self.regex.pattern}' in output")
        cabstat.declare_failure(err)
        return f"[FAILURE] {output}", logging.ERROR

class DeclareSuccess(_BaseWrangler):
    specifier = "DECLARE_SUCCESS"
    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        cabstat.declare_success()
        return f"[SUCCESS] {output}", None

class ParseOutput(_BaseWrangler):
    loaders = dict(str=str, bool=bool, int=int, float=float, json=json.loads, JSON=json.loads)
    specifier = f"PARSE_OUTPUT:(?P<name>.*):(?P<dtype>{'|'.join(loaders.keys())})"

    def __init__(self, regex: re.Pattern, action: str, name: str, dtype: str):
        super().__init__(regex, action, name=name)
        self.loader = self.loaders[dtype]
        if regex.groups < 1:
            raise CabValidationError(f"wrangler action '{action}' for '{regex.pattern}': no ()-groups")

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        try:
            value = self.loader(match.group(1))
        except Exception as exc:
            cabstat.declare_failure(StimelaCabOutputError(f"error parsing string \"{match.group(1)}\" for output '{name}'", exc))
        cabstat.declare_outputs({self.name: value})
        return output, None

class ParseJSONOutput(_BaseWrangler):
    specifier = "PARSE_JSON_OUTPUTS"

    def __init__(self, regex: re.Pattern, action: str):
        super().__init__(regex, action)
        self.names = regex.groupindex.keys()
        if not self.names:
            raise CabValidationError(f"wrangler action '{action}' for '{regex.pattern}': no ()-groups")

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
    specifier = "PARSE_JSON_OUTPUT_DICT"

    def __init__(self, regex: re.Pattern, action: str):
        super().__init__(self, regex, action)
        if regex.groups < 1:
            raise CabValidationError(f"wrangler action '{action}' for '{regex.pattern}': no ()-groups")

    def apply(self, cabstat: CabStatus, output: str, match: re.Match):
        try:
            outputs = json.load(match.group(1))
        except Exception as exc:
            cabstat.declare_failure(StimelaCabOutputError(f"error parsing output dict from \"{self.name}'\"", exc))
        cabstat.declare_outputs(outputs)
        return output, None


# build dictionary of all available wrangler action classes

all_wranglers = {
    cls.specifier: cls for _, cls in vars().items()
        if isinstance(cls, type) and issubclass(cls, _BaseWrangler) and cls.specifier is not None
}



