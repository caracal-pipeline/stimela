import dataclasses
import re, importlib, sys
from collections import OrderedDict
from enum import Enum, IntEnum
from dataclasses import dataclass
from omegaconf import MISSING, ListConfig, DictConfig, OmegaConf

import rich.box
import rich.markup
from rich.table import Table
from rich.markdown import Markdown

from .exceptions import NestedSchemaError, ParameterValidationError, DefinitionError, SchemaError
from .validate import validate_parameters, Unresolved
from .substitutions import SubstitutionNS
from .basetypes import EmptyDictDefault, EmptyListDefault, UNSET

# need * imports from both to make eval(self.dtype, globals()) work
from typing import *
from .basetypes import *

## almost supported by omegaconf, see https://github.com/omry/omegaconf/issues/144, for now just use Any
ListOrString = Any

Conditional = Optional[str]

_UNSET_DEFAULT = "<UNSET DEFAULT VALUE>"

@dataclass
class ParameterPolicies(object):
    """This class describes policies that determine how a Parameter is turned into
    cab arguments. Most policies refer to how command-line arguments are formed up,
    although some also apply to Python callable cabs.
    """
    # if true, parameter is passed as key=value, not command line option
    key_value: Optional[bool] = None
    # if true, value is passed as a positional argument, not an option
    positional: Optional[bool] = None
    # if true, value is head-positional, i.e. passed *before* any options
    positional_head: Optional[bool] = None
    # for list-type values, use this as a separator to paste them together into one argument. Otherwise:
    #  * use "list" to pass list-type values as multiple arguments (--option X Y)
    #  * use "[]" to pass list-type values as a list  (--option [X,Y])
    #  * use "repeat" to repeat the option (--option X --option Y)
    repeat: Optional[str] = None
    # prefix for non-positional arguments
    prefix: Optional[str] = None

    # skip this parameter
    skip: Optional[bool] = None
    # if True, implicit parameters will be skipped automatically
    skip_implicits: Optional[bool] = None

    # if set, {}-substitutions on this paramater will not be done
    disable_substitutions: Optional[bool] = None

    # how to pass boolean True values. None = pass option name alone, else pass option name + given value
    explicit_true: Optional[str] = None
    # how to pass boolean False values. None = skip option, else pass option name + given value
    explicit_false: Optional[str] = None

    # if set, a string-type value will be split into a list of arguments using this separator
    split: Optional[str] = None

    # dict of character replacements
    replace: Optional[Dict[str, str]] = None

    # Value formatting policies.
    # If set, specifies {}-type format strings used to convert the value(s) to string(s).
    # For a non-list value:
    #   * if 'format_list_scalar' is set, formats the value into a list of strings as fmt[i].format(value, **dict)
    #     example:  ["{0}", "{0}"] will simply repeat the value twice
    #   * if 'format' is set, value is formatted as format.format(value, **dict)
    # For a list-type value:
    #   * if 'format_list' is set, each element #i formatted separately as fmt[i].format(*value, **dict)
    #     example:  ["{0}", "{2}"] will output elements 0 and 2, and skip element 1
    #   * if 'format' is set, each element #i is formatted as format.format(value[i], **dict)
    # **dict contains all parameters passed to a cab, so these can be used in the formatting
    format: Optional[str] = None
    format_list: Optional[List[str]] = None
    format_list_scalar: Optional[List[str]] = None

    # for Python callable cabs: if set, then missing parameters are passed as None values
    # if not set, missing parameters are not passed at all
    pass_missing_as_none: Optional[bool] = None


# This is used to classify parameters, for cosmetic and help purposes.
# Usually set automatically based on whether a parameter is required, whether a default is provided, etc.
ParameterCategory = IntEnum("ParameterCategory",
                            dict(Required=0, Optional=1, Implicit=2, Obscure=3, Hidden=4),
                            module=__name__)

@dataclass
class Parameter(object):
    """Parameter (of cab or recipe)"""
    info: str = ""
    # for input parameters, this flag indicates a read-write (aka input-output aka mixed-mode) parameter e.g. an MS
    writable: bool = False
    # data type
    dtype: str = "str"
    # specifies that the value is implicitly set inside the step (i.e. not a free parameter). Typically used with filenames
    implicit: Any = None
    # optonal list of arbitrary tags, used to group parameters
    tags: List[str] = EmptyListDefault()

    # If True, parameter is required. None/False, not required.
    # For aliases, False at recipe level will override the target setting, while the default of None won't.
    required: Optional[bool] = None

    # restrict value choices, i.e. making for an option-type parameter
    choices:  Optional[List[Any]] = ()

    # for List or Dict-type parameters, restict values of list elements or dict entries to a list of choices
    element_choices: Optional[List[Any]] = None

    # default value
    default: Any = _UNSET_DEFAULT

    # list of aliases for this parameter (i.e. references to other parameters whose schemas/values this parameter shares)
    aliases: Optional[List[str]] = ()

    # if true, treat parameter as a path, and ensure that the parent directories it refers to exist
    mkdir: bool = False

    # for file and dir-type parameters: if True, the file(s)/dir(s) must exist. If False, they can be missing.
    # if None, then the default logic applies: inputs must exist, and outputs don't
    must_exist: Optional[bool] = None

    # if command-line option for underlying binary has a different name, specify it here
    nom_de_guerre: Optional[str] = None

    # policies object, specifying a non-default way to handle this parameter
    policies: ParameterPolicies = ParameterPolicies()

    # Parameter category, purely cosmetic, used for generating help and debug messages.
    # Assigned automatically if None, but a schema may explicitly mark parameters as e.g.
    # "obscure" or "hidden"
    category: Optional[ParameterCategory] = None

    # metavar corresponding to this parameter. Used when constructing command-line interfaces
    metavar: Optional[str] = None

    # abbreviated option name for this parameter.  Used when constructing command-line interfaces
    abbreviation: Optional[str] = None

    # arbitrary metadata associated with parameter
    metadata: Dict[str, Any] = EmptyDictDefault()

    def __post_init__(self):
        def natify(value):
            # convert OmegaConf lists and dicts to native types
            if type(value) in (list, ListConfig):
                return [natify(x) for x in value]
            elif type(value) in (dict, OrderedDict, DictConfig):
                return OrderedDict([(name, natify(value)) for name, value in value.items()])
            elif value is _UNSET_DEFAULT:
                return UNSET
            return value
        self.default = natify(self.default)
        self.choices = natify(self.choices)

        # converts string dtype into proper type object
        # yes I know eval() is naughty but this is the best we can do for now
        # see e.g. https://stackoverflow.com/questions/67500755/python-convert-type-hint-string-representation-from-docstring-to-actual-type-t
        # The alternative is a non-standard API call i.e. typing._eval_type()
        self._dtype = eval(self.dtype, globals())

    def get_category(self):
        """Returns category of parameter, auto-setting it if not already preset"""
        if self.category is None:
            if self.required:
                self.category = ParameterCategory.Required
            elif self.implicit is not None:
                self.category = ParameterCategory.Implicit
            else:
                self.category = ParameterCategory.Optional
        return self.category

ParameterSchema = OmegaConf.structured(Parameter)

ParameterFields = set(f.name for f in dataclasses.fields(Parameter))

@dataclass
class Cargo(object):
    name: Optional[str] = None                    # cab name (if None, use image or command name)
    fqname: Optional[str] = None                  # fully-qualified name (recipe_name.step_label.etc.etc.)

    info: Optional[str] = None                    # description

    # schemas are postentially nested (dicts of dicts), which omegaconf doesn't quite recognize,
    # (or in my ignorance I can't specify it -- in any case Union support is weak), so do a dict to Any
    # "Leaf" elements of the nested dict must be Parameters
    inputs: Dict[str, Any]   = EmptyDictDefault()
    outputs: Dict[str, Any]  = EmptyDictDefault()
    defaults: Dict[str, Any] = EmptyDictDefault()

    backend: Optional[str] = None                 # backend, if not default

    dynamic_schema: Optional[str] = None          # function to call to augment inputs/outputs dynamically

    @staticmethod
    def flatten_schemas(io_dest, io, label, prefix=""):
        for name, value in io.items():
            if name == "subsection":
                continue
            name = f"{prefix}{name}"
            if not isinstance(value, Parameter):
                if not isinstance(value, (DictConfig, dict)):
                    raise SchemaError(f"{label}.{name} is not a valid schema")
                # try to treat as Parameter based on field names
                if not (set(value.keys()) - ParameterFields):
                    try:
                        value = OmegaConf.unsafe_merge(ParameterSchema.copy(), value)
                        io_dest[name] = Parameter(**value)
                    except Exception as exc0:
                        raise SchemaError(f"{label}.{name} is not a valid parameter definition", exc0)
                else:
                    Cargo.flatten_schemas(io_dest, value, label=label, prefix=f"{name}.")
        return io_dest

    def flatten_param_dict(self, output_params, input_params, prefix=""):
        for name, value in input_params.items():
            name = f"{prefix}{name}"
            if isinstance(value, (dict, DictConfig)):
                # if prefix.name. is present in schemas, treat as nested mapping
                if any(k.startswith(f"{name}.") for k in self.inputs_outputs):
                    self.flatten_param_dict(output_params, value, prefix=f"{name}.")
                    continue
            output_params[name] = value
        return output_params

    def __post_init__(self):
        self.fqname = self.fqname or self.name
        # flatten inputs/outputs into a single dict (with entries like sub.foo.bar)
        self.inputs = Cargo.flatten_schemas(OrderedDict(), self.inputs, "inputs")
        self.outputs = Cargo.flatten_schemas(OrderedDict(), self.outputs, "outputs")
        for name in self.inputs.keys():
            if name in self.outputs:
                raise DefinitionError(f"parameter '{name}' appears in both inputs and outputs")
        self._inputs_outputs = None
        self._implicit_params = set()   # marks implicitly set values
        # flatten defaults and aliases
        self.defaults = self.flatten_param_dict(OrderedDict(), self.defaults)
        # pausterized name
        self.name_ = re.sub(r'\W', '_', self.name or "")  # pausterized name
        # config and logger objects
        self.config = self.log = self.logopts = None
        # resolve callable for dynamic schemas
        self._dyn_schema = None
        if self.dynamic_schema is not None:
            if '.' not in self.dynamic_schema:
                raise DefinitionError(f"{self.dynamic_schema}: module_name.function_name expected")
            modulename, funcname = self.dynamic_schema.rsplit(".", 1)
            try:
                mod = importlib.import_module(modulename)
            except ImportError as exc:
                raise DefinitionError(f"can't import {modulename}: {exc}")
            self._dyn_schema = getattr(mod, funcname, None)
            if not callable(self._dyn_schema):
                raise DefinitionError(f"{modulename}.{funcname} is not a valid callable")

    @property
    def inputs_outputs(self):
        if self._inputs_outputs is None:
            self._inputs_outputs = self.inputs.copy()
            self._inputs_outputs.update(**self.outputs)
        return self._inputs_outputs

    @property
    def finalized(self):
        return self.config is not None

    def unresolved_params(self, params):
        """Returns list of unresolved parameters"""
        return [name for name, value in params.items() if isinstance(value, Unresolved)]


    def finalize(self, config=None, log=None, fqname=None, nesting=0):
        if not self.finalized:
            if fqname is not None:
                self.fqname = fqname
            self.config = config
            self.nesting = nesting
            self.log = log
            self.logopts = config.opts.log.copy()

    def prevalidate(self, params: Optional[Dict[str, Any]], subst: Optional[SubstitutionNS]=None, root=False):
        """Does pre-validation.
        No parameter substitution is done, but will check for missing params and such.
        A dynamic schema, if defined, is applied at this point."""
        self.finalize()
        # update schemas, if dynamic schema is enabled
        if self._dyn_schema:
            self._inputs_outputs = None
            try:
                self.inputs, self.outputs = self._dyn_schema(params, self.inputs, self.outputs)
            except Exception as exc:
                raise SchemaError(f"error evaluating dynamic schema", exc) # [exc, sys.exc_info()[2]])
            for io in self.inputs, self.outputs:
                for name, schema in list(io.items()):
                    if isinstance(schema, DictConfig):
                        try:
                            schema = OmegaConf.unsafe_merge(ParameterSchema.copy(), schema)
                        except Exception  as exc:
                            raise SchemaError(f"error in dynamic schema for parameter 'name'", exc)
                        io[name] = Parameter(**schema)
        # add implicits, if resolved
        for name, schema in self.inputs_outputs.items():
            if schema.implicit is not None and type(schema.implicit) is not Unresolved:
                if name in params and name not in self._implicit_params:
                    raise ParameterValidationError(f"implicit parameter {name} was supplied explicitly")
                if name in self.defaults:
                   raise SchemaError(f"implicit parameter {name} also has a default value")
                params[name] = schema.implicit
                self._implicit_params.add(name)
        # assign unset categories
        for name, schema in self.inputs_outputs.items():
            schema.get_category()

        params = validate_parameters(params, self.inputs_outputs, defaults=self.defaults, subst=subst, fqname=self.fqname,
                                          check_unknowns=True, check_required=False, check_exist=False,
                                          create_dirs=False, ignore_subst_errors=True)

        return params

    def validate_inputs(self, params: Dict[str, Any], subst: Optional[SubstitutionNS]=None, loosely=False):
        """Validates inputs.
        If loosely is True, then doesn't check for required parameters, and doesn't check for files to exist etc.
        This is used when skipping a step.
        """
        assert(self.finalized)

        # check inputs
        params1 = validate_parameters(params, self.inputs, defaults=self.defaults, subst=subst, fqname=self.fqname,
                                                check_unknowns=False, check_required=not loosely, check_exist=not loosely,
                                                create_dirs=not loosely)
        # check outputs
        params1.update(**validate_parameters(params, self.outputs, defaults=self.defaults, subst=subst, fqname=self.fqname,
                                                check_unknowns=False, check_required=False, check_exist=False,
                                                create_dirs=not loosely))
        return params1

    def validate_outputs(self, params: Dict[str, Any], subst: Optional[SubstitutionNS]=None, loosely=False):
        """Validates outputs. Parameter substitution is done.
        If loosely is True, then doesn't check for required parameters, and doesn't check for files to exist etc.
        """
        assert(self.finalized)
        # update implicits that weren't marked as unresolved
        for name in self._implicit_params:
            impl = self.inputs_outputs[name].implicit
            if type(impl) is not Unresolved:
                params[name] = self.inputs_outputs[name].implicit
        params.update(**validate_parameters(params, self.outputs, defaults=self.defaults, subst=subst, fqname=self.fqname,
                                                check_unknowns=False, check_required=not loosely, check_exist=not loosely))
        return params

    def rich_help(self, tree, max_category=ParameterCategory.Optional):
        """Generates help into a rich.tree.Tree object"""
        if self.info:
            tree.add("Description:").add(Markdown(self.info))
        # adds tables for inputs and outputs
        for io, title in (self.inputs, "inputs"), (self.outputs, "outputs"):
            for cat in ParameterCategory:
                schemas = [(name, schema) for name, schema in io.items() if schema.get_category() == cat]
                if not schemas:
                    continue
                if cat > max_category:
                    subtree = tree.add(f"[dim]{cat.name} {title}: omitting {len(schemas)}[/dim]")
                    continue
                subtree = tree.add(f"{cat.name} {title}:")
                table = Table.grid("", "", "", padding=(0,2)) # , show_header=False, show_lines=False, box=rich.box.SIMPLE)
                subtree.add(table)
                for name, schema in schemas:
                    attrs = []
                    default = self.defaults.get(name, schema.default)
                    if schema.implicit:
                        attrs.append(f"implicit: {schema.implicit}")
                    if default is not UNSET and not isinstance(default, Unresolved):
                        attrs.append(f"default: {default}")
                    if schema.choices:
                        attrs.append(f"choices: {', '.join(schema.choices)}")
                    info = []
                    schema.info and info.append(rich.markup.escape(schema.info))
                    attrs and info.append(f"[dim]\[{rich.markup.escape(', '.join(attrs))}][/dim]")
                    table.add_row(f"[bold]{name}[/bold]",
                                  f"[dim]{rich.markup.escape(str(schema.dtype))}[/dim]",
                                  " ".join(info))


