# ruff: noqa: E731 - ignore assignment of lambda expressions. TODO(JSKenyon): Fix this.
import dataclasses
import keyword
import logging
import os
import os.path
import pathlib
import re
from collections import OrderedDict
from collections.abc import Callable
from types import NoneType
from typing import Any, Dict, List, Optional, Tuple, get_args, get_origin

import pydantic
import pydantic.dataclasses
import yaml
from omegaconf import DictConfig, ListConfig, OmegaConf

from scabha.basetypes import MS, UNSET, URI, Directory, File, Unresolved
from scabha.evaluator import Evaluator
from scabha.exceptions import Error, ParameterValidationError, SubstitutionErrorList
from scabha.substitutions import SubstitutionNS, substitutions_from

COERCERS: dict[tuple[type, type], Callable[[Any], Any]] = {}
BOOL_TRUTHY = {"true", "1", "1.0"}
BOOL_FALSY = {"false", "0", "0.0"}
NONE_STRINGS = {"null", "none"}


def register_coercer(from_type: type, to_type: type) -> Callable[[Any], Any]:
    def decorator(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
        COERCERS[(from_type, to_type)] = func
        return func

    return decorator


def join_quote(values):
    return "'" + "', '".join(values) + "'" if values else ""


def is_simple_dtype(dtype: Any) -> bool:
    """True for plain Python types (str, int, float, bool, custom classes)."""

    return get_origin(dtype) is None and isinstance(dtype, type)


def is_any_dtype(dtype: Any) -> bool:
    """Return True when dtype represents typing.Any.

    Args:
        dtype: Schema dtype or annotation to evaluate.

    Returns:
        True if dtype is typing.Any, otherwise False.
    """

    return dtype is Any


@register_coercer(str, bool)
def str_to_bool(value: str) -> bool:
    value = value.lower()
    if value in BOOL_TRUTHY:
        return True
    if value in BOOL_FALSY:
        return False
    raise ValueError(f"Cannot interpret {value!r} as boolean")


@register_coercer(str, int)
def str_to_int(value: str) -> int:
    cleaned = value.strip()
    if "." in cleaned:
        f = float(cleaned)
        if f == int(f):
            return int(f)
        raise ValueError(f"Cannot losslessly convert {value!r} to int")
    return int(cleaned)


@register_coercer(str, NoneType)
def str_to_none(value: str) -> None:
    if value.lower() in NONE_STRINGS:
        return None
    raise ValueError(f"Cannot interpret {value!r} as None")


@register_coercer(float, int)
def float_to_int(value: float) -> int:
    if value != int(value):
        raise ValueError(f"Cannot losslessly convert {value!r} to int")
    return int(value)


def coerce_scalar(value: Any, dtype: Any, label: str) -> Any:
    """Cast a scalar value to the requested dtype when possible.

    Args:
        value: Raw value extracted from the schema inputs.
        dtype: Target type used for coercion.
        label: Parameter label included in error messages.

    Returns:
        The coerced value if casting succeeds or the original value when dtype is typing.Any.

    Raises:
        TypeError: Raised when casting fails for strict dtypes.
    """

    # Do not coerce if correct type or Any
    if is_any_dtype(dtype) or isinstance(value, dtype):
        return value

    # Look for register coercer
    coercer = COERCERS.get((type(value), dtype))
    if coercer:
        try:
            return coercer(value)
        except Exception as error:
            typename = getattr(dtype, "__name__", str(dtype))
            raise TypeError(f"{label}: failed to coerce value {value!r} to {typename}") from error

    # Try direct coercion through constructor
    try:
        return dtype(value)
    except Exception as error:
        typename = getattr(dtype, "__name__", str(dtype))
        raise TypeError(f"{label}: failed to coerce value {value!r} to {typename}") from error


def coerce_homogeneous_sequence(
    value: Any,
    elem_dtype: Any,
    label: str,
    target_type: type,
) -> tuple[Any, bool]:
    """Coerce each element of a sequence to the same dtype."""
    changed = False
    coerced = []
    for idx, elem in enumerate(value):
        new_elem, elem_changed = maybe_coerce_value(elem, elem_dtype, f"{label}[{idx}]")
        coerced.append(new_elem)
        changed = changed or elem_changed
    return target_type(coerced), changed


def coerce_heterogeneous_tuple(
    value: Any,
    elem_dtypes: tuple[type, ...],
    label: str,
) -> tuple[Any, bool]:
    """Coerce each element of a fixed-length tuple to its positional dtype."""
    if len(value) != len(elem_dtypes):
        raise TypeError(f"{label}: expected {len(elem_dtypes)} elements, got {len(value)}")
    changed = False
    coerced = []
    for idx, (elem, edtype) in enumerate(zip(value, elem_dtypes)):
        new_elem, elem_changed = maybe_coerce_value(elem, edtype, f"{label}[{idx}]")
        coerced.append(new_elem)
        changed = changed or elem_changed
    return tuple(coerced), changed


def maybe_coerce_value(value: Any, dtype: Any, label: str) -> tuple[Any, bool]:
    """Attempt to coerce primitive and container values prior to validation."""

    origin = get_origin(dtype)
    args = get_args(dtype)

    if origin is None:
        if is_any_dtype(dtype):
            return value, False
        if isinstance(dtype, type):
            coerced = coerce_scalar(value, dtype, label)
            return coerced, coerced is not value
        return value, False

    # Handle typing.List[foo]
    # NOTE(Brian): This is expecting homogeneous lists. Not sure if this is
    # expected behaviour to have them homogeneous.
    if origin in (list, List):
        if not isinstance(value, (list, tuple)):
            return value, False
        elem_dtype = args[0] if args else Any
        return coerce_homogeneous_sequence(value, elem_dtype, label, list)

    # Handle typing.Tuple[foo, bar, ...] and typing.Tuple[foo, bar, baz]
    # NOTE(Brian): This is expecting heterogeneous or homogeneous tuples.
    # Not sure if this is expected behaviour to have them this way.
    if origin in (tuple, Tuple):
        if not isinstance(value, (list, tuple)):
            return value, False

        if not args or args == ((),):
            # Bare Tuple with no type info
            return tuple(value), False

        if len(args) == 2 and args[1] is Ellipsis:
            # Tuple[int, ...] - variable-length homogeneous
            return coerce_homogeneous_sequence(value, args[0], label, tuple)

        # Tuple[int, str, bool] - fixed-length heterogeneous
        return coerce_heterogeneous_tuple(value, args, label)

    # Handle typing.Dict[key, val]
    if origin in (dict, Dict):
        key_dtype, val_dtype = get_args(dtype) or (Any, Any)
        if isinstance(value, dict):
            changed = False
            coerced_dict = {}
            for k, v in value.items():
                new_k, key_changed = maybe_coerce_value(k, key_dtype, f"{label}.key")
                new_v, val_changed = maybe_coerce_value(v, val_dtype, f"{label}.{new_k}")
                coerced_dict[new_k] = new_v
                changed = changed or key_changed or val_changed
            return coerced_dict, changed
        return value, False

    # Fallback: no coercion for unions, literals, etc.
    return value, False


def evaluate_and_substitute_object(
    obj: Any,
    subst: SubstitutionNS,
    recursion_level: int = 1,
    location: List[str] = [],
    log: Optional[logging.Logger] = None,
    log_and_remember: Optional[Callable] = None,
):
    with substitutions_from(subst, raise_errors=True) as context:
        evaltor = Evaluator(
            subst, context, location=location, allow_unresolved=False, log=log, log_and_remember=log_and_remember
        )
        return evaltor.evaluate_object(obj, raise_substitution_errors=True, recursion_level=recursion_level)


def evaluate_and_substitute(
    inputs: Dict[str, Any],
    subst: SubstitutionNS,
    corresponding_ns: SubstitutionNS,
    defaults: Dict[str, Any] = {},
    ignore_subst_errors: bool = False,
    location: List[str] = [],
    log: Optional[logging.Logger] = None,
):
    with substitutions_from(subst, raise_errors=True) as context:
        evaluator = Evaluator(subst, context, location=location, allow_unresolved=True, log=log)
        inputs = evaluator.evaluate_dict(
            inputs, corresponding_ns=corresponding_ns, defaults=defaults, raise_substitution_errors=False
        )
        # collect errors
        if not ignore_subst_errors:
            errors = []
            for value in inputs.values():
                if type(value) is Unresolved:
                    errors += value.errors
            # check for substitution errors
            if errors:
                raise SubstitutionErrorList("unresolved {}-substitutions", errors)
    return inputs


def validate_parameters(
    params: Dict[str, Any],
    schemas: Dict[str, Any],
    defaults: Optional[Dict[str, Any]] = None,
    subst: Optional[SubstitutionNS] = None,
    fqname: str = "",
    check_unknowns=True,
    check_required=True,
    check_inputs_exist=True,
    check_outputs_exist=True,
    create_dirs=False,
    ignore_subst_errors=False,
    log: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """Validates a dict of parameter values against a given schema

    Args:
        params (Dict[str, Any]):   map of input parameter values
        schemas (Dict[str, Any]):   map of parameter names to schemas. Each schema must contain a dtype field and a
            choices field.
        defaults (Dict[str, Any], optional): dictionary of default values to be used when a value is missing
        subst (SubsititionNS, optional): namespace to do {}-substitutions on parameter values
        fqname: fully-qualified name of the parameter set (e.g. "recipe_name.step_name"), used in error messages. If
            not given, errors will report parameter names only

        check_unknowns (bool): if True, unknown parameters (not in schema) raise an error
        check_required (bool): if True, missing parameters with required=True will raise an error
        check_inputs_exist (bool): if True, input files with must_exist={None,True} in schema must exist, or will
            raise an error. If False, doesn't check.
        check_outputs_exist (bool): if True, output files with must_exist=True in schema must exist, or will raise an
            error. If False, doesn't check.
        create_dirs (bool): if True, non-existing directories in filenames (and parameters with mkdir=True in schema)
                            will be created.
        ignore_subst_errors (bool): if True, substitution errors will be ignored


    Raises:
        ParameterValidationError: parameter fails validation
        SchemaError: bad schema
        SubstitutionErrorList: list of substitution errors, if they occur

    Returns:
        Dict[str, Any]: validated dict of parameters

    TODO:
        add options to propagate all errors out (as values of type Error) in place of exceptions?
    """
    # define function for converting parameter name into "fully-qualified" name
    if fqname:
        mkname = lambda name: f"{fqname}.{name}"
    else:
        mkname = lambda name: name

    # check for unknowns
    if check_unknowns:
        for name in params:
            if name not in schemas:
                raise ParameterValidationError(f"unknown parameter '{mkname(name)}'")

    # only evaluate the subset for which we have schemas
    inputs = OrderedDict((name, value) for name, value in params.items() if name in schemas)

    # build dict of all defaults
    all_defaults = {
        name: schema.default
        for name, schema in schemas.items()
        if schema.default is not UNSET and schema.default != "UNSET"
    }
    if defaults:
        all_defaults.update(**{name: value for name, value in defaults.items() if name in schemas})

    # update missing inputs from defaults
    inputs.update(**{name: value for name, value in all_defaults.items() if name not in inputs})

    # update implicit values
    #

    # perform substitution
    if subst is not None:
        inputs = evaluate_and_substitute(
            inputs,
            subst,
            subst.current,
            defaults=all_defaults,
            ignore_subst_errors=ignore_subst_errors,
            location=[fqname],
            log=log,
        )

    # split inputs into unresolved substitutions, and proper inputs
    unresolved = {name: value for name, value in inputs.items() if isinstance(value, Unresolved)}
    inputs = {name: value for name, value in inputs.items() if not isinstance(value, Unresolved)}

    # check that required args are present
    if check_required:
        missing = [
            mkname(name)
            for name, schema in schemas.items()
            if schema.required and inputs.get(name) is UNSET and name not in unresolved
        ]
        if missing:
            raise ParameterValidationError(f"missing required parameters: {join_quote(missing)}")

    # create dataclass from parameter schema
    validated = {}
    fields = []

    # maps parameter names to/from field names. Fields have "_" not "-"
    name2field = {}
    field2name = {}

    for name, schema in schemas.items():
        value = inputs.get(name, UNSET)
        if value is not UNSET:
            # sanitize name: dataclass won't take hyphens or periods
            fldname = orig_fldname = re.sub("\\W", "_", name)
            # avoid Python keywords and clashes with other field names by adding _x as needed
            num = 0
            while (
                keyword.iskeyword(fldname)
                or (hasattr(keyword, "issoftkeyword") and keyword.issoftkeyword(fldname))
                or fldname in field2name
            ):
                fldname += f"{orig_fldname}_{num}"
                num += 1
            # add to mapping
            field2name[fldname] = name
            name2field[name] = fldname

            fields.append((fldname, schema._dtype))

            # OmegaConf dicts/lists need to be converted to standard containers for pydantic to take them
            normalized_value = value
            if isinstance(value, (ListConfig, DictConfig)):
                normalized_value = OmegaConf.to_container(value)
                inputs[name] = normalized_value

            # NOTE (Brian): This part could be dangerous as it gives JavaScript like functionality
            # to CLI parameter parsing. Trusts the user has correct alias types set.
            coerced_value, changed = maybe_coerce_value(normalized_value, schema._dtype, mkname(name))
            if changed:
                inputs[name] = coerced_value

    dcls = dataclasses.make_dataclass("Parameters", fields)

    # convert this to a pydantic dataclass which does validation
    pcls = pydantic.dataclasses.dataclass(dcls)

    # check Files etc.
    for name, value in list(inputs.items()):
        # get schema from those that need validation, skip if not in schemas
        schema = schemas.get(name)
        if schema is None:
            continue
        # skip errors
        if value is UNSET or isinstance(value, Error):
            continue
        dtype = schema._dtype

        # must this file exist? Schema may force this check, otherwise follow the default check_exist policy
        if schema.is_input:
            must_exist = check_inputs_exist and schema.must_exist is not False
        elif schema.is_output:
            must_exist = check_outputs_exist and schema.must_exist

        if schema.is_file_type or schema.is_file_list_type:
            # match to existing file(s)
            if isinstance(value, str):
                # try to interpret string as a formatted list (a list substituted in would come out like that)
                try:
                    files = yaml.safe_load(value)
                    if type(files) is not list:
                        files = [value]
                except Exception:
                    files = [value]
            elif isinstance(value, (list, tuple)):
                files = value
            else:
                raise ParameterValidationError(f"'{mkname(name)}={value}': invalid type '{type(value)}'")
            # convert to appropriate type
            files = [URI(f) for f in files]

            # check for existence of all files in list, if needed
            if must_exist:
                if not files and not schema.is_file_list_type:
                    raise ParameterValidationError(f"'{mkname(name)}': file doesn't exist")
                not_exists = [uri.path for uri in files if not uri.remote and not os.path.exists(uri.path)]
                if not_exists:
                    raise ParameterValidationError(f"'{mkname(name)}': {','.join(not_exists)} doesn't exist")

            # check for single file/dir
            if schema.is_file_type:
                if len(files) > 1:
                    raise ParameterValidationError(f"'{mkname(name)}': multiple files given ({value})")
                # no files? must_exist was checked above, so return empty filename
                elif not files:
                    inputs[name] = ""
                # else one file/dir as expected, check it
                else:
                    # check that files are files and dirs are dirs
                    uri = files[0]
                    if not uri.remote and os.path.exists(uri.path):
                        if dtype == File:
                            if not os.path.isfile(uri.path):
                                raise ParameterValidationError(f"'{mkname(name)}': {uri} is not a regular file")
                        elif dtype == Directory or dtype == MS:
                            if not os.path.isdir(uri.path):
                                raise ParameterValidationError(f"'{mkname(name)}': {uri} is not a directory")
                    inputs[name] = str(uri)
            # else make list
            else:
                # check that files are files and dirs are dirs
                if dtype == List[File]:
                    if not all(
                        os.path.isfile(uri.path) for uri in files if not uri.remote and os.path.exists(uri.path)
                    ):
                        raise ParameterValidationError(f"{mkname(name)}: {value} contains non-files")
                elif dtype == List[Directory] or dtype == List[MS]:
                    if not all(os.path.isdir(uri.path) for uri in files if not uri.remote and os.path.exists(uri.path)):
                        raise ParameterValidationError(f"{mkname(name)}: {value} contains non-directories")
                inputs[name] = list(map(str, files))

    # validate
    try:
        validated = pcls(
            **{name2field[name]: value for name, value in inputs.items() if name in schemas and value is not UNSET}
        )
    except pydantic.ValidationError as exc:
        errors = []
        for err in exc.errors():
            loc = ".".join([field2name.get(x, str(x)) for x in err["loc"]])
            if loc in inputs:
                errors.append(ParameterValidationError(f"{loc} = {inputs[loc]}: {err['msg']}"))
            else:
                errors.append(ParameterValidationError(f"{loc}: {err['msg']}"))
        raise ParameterValidationError(f"{len(errors)} parameter(s) failed validation:", errors)

    validated = {field2name[fld]: value for fld, value in dataclasses.asdict(validated).items()}

    # check choice-type parameters
    for name, value in validated.items():
        if value is None:
            continue
        schema = schemas[name]
        if schema.choices and value not in schema.choices:
            raise ParameterValidationError(f"{mkname(name)}: invalid value '{value}'")
        if schema.element_choices:
            listval = value if isinstance(value, (list, tuple, ListConfig)) else [value]
            for elem in listval:
                if elem not in schema.element_choices:
                    raise ParameterValidationError(f"{mkname(name)}: invalid list element '{elem}'")

    # check for mkdir directives
    if create_dirs:
        for name, value in validated.items():
            schema = schemas[name]
            if schema.is_output and (schema.mkdir or schema.path_policies.mkdir_parent):
                if schema.is_file_type:
                    files = [URI(value)]
                elif schema.is_file_list_type:
                    files = map(URI, value)
                else:
                    continue
                for uri in files:
                    if not uri.remote:
                        path = pathlib.Path(uri.path)
                        # Directory-type outputs
                        if schema.mkdir and (schema._dtype == Directory or schema._dtype == List[Directory]):
                            path.mkdir(parents=True, exist_ok=True)
                        elif schema.path_policies.mkdir_parent:
                            path.parent.mkdir(parents=True, exist_ok=True)

    # add in unresolved values
    validated.update(**unresolved)

    return validated
