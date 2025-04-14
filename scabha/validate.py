import dataclasses
import os
import os.path
import yaml
import re
import keyword
import pathlib
from typing import *
from collections import OrderedDict

import pydantic
import pydantic.dataclasses

from omegaconf import OmegaConf, ListConfig, DictConfig

from scabha.basetypes import Unresolved
from .exceptions import Error, ParameterValidationError, SchemaError, SubstitutionErrorList
from .substitutions import SubstitutionNS, substitutions_from
from .basetypes import URI, File, Directory, MS, UNSET
from .evaluator import Evaluator

def join_quote(values):
    return "'" + "', '".join(values) + "'" if values else ""


def evaluate_and_substitute_object(obj: Any,  
                                    subst: SubstitutionNS, 
                                    recursion_level: int = 1,
                                    location: List[str] = []):
    with substitutions_from(subst, raise_errors=True) as context:
        evaltor = Evaluator(subst, context, location=location, allow_unresolved=False)
        return evaltor.evaluate_object(obj, raise_substitution_errors=True, recursion_level=recursion_level)


def evaluate_and_substitute(inputs: Dict[str, Any], 
                            subst: SubstitutionNS, 
                            corresponding_ns: SubstitutionNS,
                            defaults: Dict[str, Any] = {},
                            ignore_subst_errors: bool = False, 
                            location: List[str] = []):
    with substitutions_from(subst, raise_errors=True) as context:
        evaltor = Evaluator(subst, context, location=location, allow_unresolved=True)
        inputs = evaltor.evaluate_dict(inputs, corresponding_ns=corresponding_ns, defaults=defaults,
                                        raise_substitution_errors=False)
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


def validate_parameters(params: Dict[str, Any], schemas: Dict[str, Any], 
                        defaults: Optional[Dict[str, Any]] = None,
                        subst: Optional[SubstitutionNS] = None,
                        fqname: str = "",
                        check_unknowns=True,    
                        check_required=True,
                        check_inputs_exist=True,
                        check_outputs_exist=True,
                        create_dirs=False,
                        ignore_subst_errors=False,
                        ) -> Dict[str, Any]:
    """Validates a dict of parameter values against a given schema 

    Args:
        params (Dict[str, Any]):   map of input parameter values
        schemas (Dict[str, Any]):   map of parameter names to schemas. Each schema must contain a dtype field and a choices field.
        defaults (Dict[str, Any], optional): dictionary of default values to be used when a value is missing
        subst (SubsititionNS, optional): namespace to do {}-substitutions on parameter values
        fqname: fully-qualified name of the parameter set (e.g. "recipe_name.step_name"), used in error messages. If not given,
                errors will report parameter names only

        check_unknowns (bool): if True, unknown parameters (not in schema) raise an error
        check_required (bool): if True, missing parameters with required=True will raise an error
        check_inputs_exist (bool): if True, input files with must_exist={None,True} in schema must exist, or will raise an error. 
                                   If False, doesn't check.
        check_outputs_exist (bool): if True, output files with must_exist=True in schema must exist, or will raise an error. 
                                   If False, doesn't check.
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
    all_defaults = {name: schema.default for name, schema in schemas.items() if schema.default is not UNSET and schema.default != "UNSET"}
    if defaults:
        all_defaults.update(**{name: value for name, value in defaults.items() if name in schemas})

    # update missing inputs from defaults
    inputs.update(**{name: value for name, value in all_defaults.items() if name not in inputs})

    # update implicit values
    #  

    # perform substitution
    if subst is not None:
        inputs = evaluate_and_substitute(inputs, subst, subst.current, defaults=all_defaults, 
                                        ignore_subst_errors=ignore_subst_errors, 
                                        location=[fqname])

    # split inputs into unresolved substitutions, and proper inputs
    unresolved = {name: value for name, value in inputs.items() if isinstance(value, Unresolved)}
    inputs = {name: value for name, value in inputs.items() if not isinstance(value, Unresolved)}

    # check that required args are present
    if check_required:
        missing = [mkname(name) for name, schema in schemas.items() 
                    if schema.required and inputs.get(name) is UNSET and name not in unresolved]
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
            while keyword.iskeyword(fldname) or \
                    (hasattr(keyword, 'issoftkeyword') and keyword.issoftkeyword(fldname)) or \
                    fldname in field2name:
                fldname += f"{orig_fldname}_{num}"
                num += 1
            # add to mapping
            field2name[fldname] = name
            name2field[name] = fldname

            fields.append((fldname, schema._dtype))
            
            # OmegaConf dicts/lists need to be converted to standard containers for pydantic to take them
            if isinstance(value, (ListConfig, DictConfig)):
                inputs[name] = OmegaConf.to_container(value)

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
            if type(value) is str:
                # try to interpret string as a formatted list (a list substituted in would come out like that)
                try:
                    files = yaml.safe_load(value)
                    if type(files) is not list:
                        files = [value]
                except Exception as exc:
                    files = [value]
            elif isinstance(value, (list, tuple)):
                files = value
            else:
                raise ParameterValidationError(f"'{mkname(name)}={value}': invalid type '{type(value)}'")
            # convert to appropriate type 
            files = [URI(f) for f in files]
          
            # check for existence of all files in list, if needed
            if must_exist: 
                if not files:
                    raise ParameterValidationError(f"'{mkname(name)}': file(s) don't exist")
                not_exists = [uri.path for uri in files 
                              if not uri.remote and not os.path.exists(uri.path)]
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
                    if not all(os.path.isfile(uri.path) for uri in files 
                               if not uri.remote and os.path.exists(uri.path)):
                        raise ParameterValidationError(f"{mkname(name)}: {value} contains non-files")
                elif dtype == List[Directory] or dtype == List[MS]:
                    if not all(os.path.isdir(uri.path) for uri in files 
                               if not uri.remote and os.path.exists(uri.path)):
                        raise ParameterValidationError(f"{mkname(name)}: {value} contains non-directories")
                inputs[name] = list(map(str, files))

    # validate
    try:   
        validated = pcls(**{name2field[name]: value for name, value in inputs.items() if name in schemas and value is not UNSET})
    except pydantic.ValidationError as exc:
        errors = []
        for err  in exc.errors():
            loc = '.'.join([field2name.get(x, str(x)) for x in err['loc']])
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
