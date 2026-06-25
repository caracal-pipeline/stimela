"""Patches for scabha to fix known issues until they are upstreamed.

This module monkey-patches scabha.validate.validate_parameters to add
string-to-complex-type coercion. When a parameter value arrives as a string
(e.g. from {}-substitution which stringifies list/tuple values) but the
expected dtype is a composite type (List, Tuple, Dict, etc.), the original
validate_parameters fails with a pydantic validation error.

The fix attempts to parse string values via ast.literal_eval or
yaml.safe_load before passing them to pydantic, recovering the intended
Python object.

See: https://github.com/caracal-pipeline/stimela/issues/364
"""

import ast
import logging
from typing import get_origin

import scabha.validate as _scabha_validate
import yaml

log = logging.getLogger(__name__)


def _coerce_string_to_complex_type(value, dtype):
    """Try to coerce a string value into the expected complex type.

    When a value passes through {}-substitution, Python's string formatter
    converts it to its str() representation. For composite types like
    List[Tuple[float, float]], this produces a string like
    '[(1.0, 2.0), (3.0, 4.0)]' which pydantic cannot validate directly.

    This function attempts to recover the original Python object by first
    trying ast.literal_eval (which correctly handles Python literal syntax
    including tuples), then falling back to yaml.safe_load (which handles
    YAML-style list/dict literals). ast.literal_eval is preferred because
    str() on a Python object produces valid Python literal syntax, while
    YAML incorrectly parses parenthesized tuples as strings.

    Returns the parsed value on success, or the original string on failure.
    """
    if not isinstance(value, str):
        return value

    # Only attempt coercion for composite types (List, Tuple, Dict, Set, etc.)
    origin = get_origin(dtype)
    if origin is None:
        # Not a generic type; skip coercion for simple types like str, int, float
        return value

    # Don't attempt to parse file-type parameters -- those have their own
    # handling logic later in validate_parameters.
    from scabha.basetypes import is_file_list_type, is_file_type

    if is_file_type(dtype) or is_file_list_type(dtype):
        return value

    # First try ast.literal_eval -- correctly handles Python literal syntax
    # including tuples. This is the preferred parser because {}-substitution
    # produces str() output which is valid Python syntax.
    try:
        parsed = ast.literal_eval(value)
        if parsed is not None and not isinstance(parsed, str):
            return parsed
    except Exception:
        pass

    # Fall back to yaml.safe_load for YAML-style list/dict literals
    try:
        parsed = yaml.safe_load(value)
        if parsed is not None and not isinstance(parsed, str):
            return parsed
    except Exception:
        pass

    return value


def _make_patched_validate_parameters():
    """Build and return the patched validate_parameters function.

    We capture the original function at patch-time to avoid import loops.
    The patch inserts string-to-complex-type coercion into the type-coercion
    section of validate_parameters -- after {}-substitution has already run,
    but before pydantic validation. This is the correct insertion point
    because:

    1. {}-substitution stringifies complex values (e.g. list -> "[(1,2)]")
    2. The existing coercion only handles OmegaConf containers and bool->str
    3. We need to parse those strings back into Python objects for pydantic

    Rather than duplicating the entire function, we use a thin wrapper that
    patches the inputs dict after calling the original substitution logic
    but before the pydantic validation step. We do this by intercepting the
    inputs after substitution.
    """
    import dataclasses
    import keyword
    import os
    import os.path
    import pathlib
    import re
    from collections import OrderedDict
    from typing import Any, Dict, List, Optional

    import pydantic
    import pydantic.dataclasses
    from omegaconf import DictConfig, ListConfig, OmegaConf
    from scabha.basetypes import MS, UNSET, URI, Directory, File, Unresolved
    from scabha.exceptions import Error, ParameterValidationError
    from scabha.validate import (
        _VALIDATION_CONFIG,
        evaluate_and_substitute,
        join_quote,
    )

    def patched_validate_parameters(
        params: Dict[str, Any],
        schemas: Dict[str, Any],
        defaults: Optional[Dict[str, Any]] = None,
        subst=None,
        fqname: str = "",
        check_unknowns=True,
        check_required=True,
        check_inputs_exist=True,
        check_outputs_exist=True,
        create_dirs=False,
        ignore_subst_errors=False,
        log: Optional[logging.Logger] = None,
    ) -> Dict[str, Any]:
        """Patched validate_parameters with string-to-complex-type coercion.

        This is a copy of scabha.validate.validate_parameters with an additional
        coercion step: after {}-substitution, string values whose target dtype is
        a composite type (List, Tuple, Dict, etc.) are parsed via ast.literal_eval
        or yaml.safe_load to recover the original Python object.
        """
        if fqname:
            mkname = lambda name: f"{fqname}.{name}"  # noqa: E731
        else:
            mkname = lambda name: name  # noqa: E731

        if check_unknowns:
            for name in params:
                if name not in schemas:
                    raise ParameterValidationError(f"unknown parameter '{mkname(name)}'")

        inputs = OrderedDict((name, value) for name, value in params.items() if name in schemas)

        all_defaults = {
            name: schema.default
            for name, schema in schemas.items()
            if schema.default is not UNSET and schema.default != "UNSET"
        }
        if defaults:
            all_defaults.update(**{name: value for name, value in defaults.items() if name in schemas})

        inputs.update(**{name: value for name, value in all_defaults.items() if name not in inputs})

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

        unresolved = {name: value for name, value in inputs.items() if isinstance(value, Unresolved)}
        inputs = {name: value for name, value in inputs.items() if not isinstance(value, Unresolved)}

        if check_required:
            missing = [
                mkname(name)
                for name, schema in schemas.items()
                if schema.required and inputs.get(name) is UNSET and name not in unresolved
            ]
            if missing:
                raise ParameterValidationError(f"missing required parameters: {join_quote(missing)}")

        validated = {}
        fields = []
        name2field = {}
        field2name = {}

        for name, schema in schemas.items():
            value = inputs.get(name, UNSET)
            if value is not UNSET:
                fldname = orig_fldname = re.sub("\\W", "_", name)
                num = 0
                while (
                    keyword.iskeyword(fldname)
                    or (hasattr(keyword, "issoftkeyword") and keyword.issoftkeyword(fldname))
                    or fldname in field2name
                ):
                    fldname += f"{orig_fldname}_{num}"
                    num += 1
                field2name[fldname] = name
                name2field[name] = fldname

                fields.append((fldname, schema._dtype))

                # OmegaConf dicts/lists need to be converted to standard containers for pydantic to take them
                if isinstance(value, (ListConfig, DictConfig)):
                    inputs[name] = OmegaConf.to_container(value)
                elif isinstance(value, bool) and schema._dtype is str:
                    inputs[name] = str(value)
                # === PATCH: coerce string values to complex types (issue #364) ===
                elif isinstance(value, str) and not isinstance(value, Unresolved):
                    coerced = _coerce_string_to_complex_type(value, schema._dtype)
                    if coerced is not value:
                        inputs[name] = coerced

        dcls = dataclasses.make_dataclass("Parameters", fields)
        pcls = pydantic.dataclasses.dataclass(dcls, config=_VALIDATION_CONFIG)

        for name, value in list(inputs.items()):
            schema = schemas.get(name)
            if schema is None:
                continue
            if value is UNSET or isinstance(value, Error):
                continue
            dtype = schema._dtype

            if schema.is_input:
                must_exist = check_inputs_exist and schema.must_exist is not False
            elif schema.is_output:
                must_exist = check_outputs_exist and schema.must_exist

            if schema.is_file_type or schema.is_file_list_type:
                if isinstance(value, str):
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
                files = [URI(f) for f in files]

                if must_exist:
                    if not files and not schema.is_file_list_type:
                        raise ParameterValidationError(f"'{mkname(name)}': file doesn't exist")
                    not_exists = [uri.path for uri in files if not uri.remote and not os.path.exists(uri.path)]
                    if not_exists:
                        raise ParameterValidationError(f"'{mkname(name)}': {','.join(not_exists)} doesn't exist")

                if schema.is_file_type:
                    if len(files) > 1:
                        raise ParameterValidationError(f"'{mkname(name)}': multiple files given ({value})")
                    elif not files:
                        inputs[name] = ""
                    else:
                        uri = files[0]
                        if not uri.remote and os.path.exists(uri.path):
                            if dtype == File:
                                if not os.path.isfile(uri.path):
                                    raise ParameterValidationError(f"'{mkname(name)}': {uri} is not a regular file")
                            elif dtype == Directory or dtype == MS:
                                if not os.path.isdir(uri.path):
                                    raise ParameterValidationError(f"'{mkname(name)}': {uri} is not a directory")
                        inputs[name] = str(uri)
                else:
                    if dtype == List[File]:
                        if not all(
                            os.path.isfile(uri.path) for uri in files if not uri.remote and os.path.exists(uri.path)
                        ):
                            raise ParameterValidationError(f"{mkname(name)}: {value} contains non-files")
                    elif dtype == List[Directory] or dtype == List[MS]:
                        if not all(
                            os.path.isdir(uri.path) for uri in files if not uri.remote and os.path.exists(uri.path)
                        ):
                            raise ParameterValidationError(f"{mkname(name)}: {value} contains non-directories")
                    inputs[name] = list(map(str, files))

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
                            if schema.mkdir and (schema._dtype == Directory or schema._dtype == List[Directory]):
                                path.mkdir(parents=True, exist_ok=True)
                            elif schema.path_policies.mkdir_parent:
                                path.parent.mkdir(parents=True, exist_ok=True)

        validated.update(**unresolved)

        return validated

    return patched_validate_parameters


def apply_patches():
    """Apply all scabha patches. Should be called once at stimela import time."""
    patched = _make_patched_validate_parameters()
    _scabha_validate.validate_parameters = patched
    # Also patch the reference in scabha.cargo since it imports validate_parameters directly
    import scabha.cargo as _scabha_cargo

    _scabha_cargo.validate_parameters = patched
    log.debug("Applied scabha validation patches for complex type coercion (issue #364)")
