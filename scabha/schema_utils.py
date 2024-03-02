import os
import re
import click
from scabha.exceptions import SchemaError
from .cargo import Parameter, UNSET, _UNSET_DEFAULT
from typing import List, Union, Optional, Callable, Dict, DefaultDict, Any
from .basetypes import EmptyDictDefault, File, is_file_type
from dataclasses import dataclass, make_dataclass, field
from omegaconf import OmegaConf, MISSING
from collections.abc import MutableSet, MutableSequence, MutableMapping
from scabha import configuratt

def schema_to_dataclass(io: Dict[str, Parameter], class_name: str, bases=(), post_init: Optional[Callable] =None):
    """
    Converts a scabha schema to a dataclass.
    Each parameter in the schema will correspond to a field. Metadata of fields will contain:
    'help' for info, 'choices' for the choices field, 'parameter' for parameter name

    Args:
        io (Dict[str, Parameter]): dict of parameters
        class_name (str):           name of dataclass
        bases (tuple, optional): [description]. Base classes (passed to bases of make_dataclass())
        post_init (callable, optional): Inserts a __post__init__ method into the class if needed.

    Raises:
        SchemaError: if any parameters are malformed

    Returns:
        dataclass
    """
    field2name = {}
    fields = []
    for name, schema in io.items():
        if type(schema) is not Parameter:
            schema = Parameter(**schema)

        if is_file_type(schema._dtype):
            schema._dtype = str
            schema.dtype = "str"

        # sanitize name: dataclass won't take hyphens or periods
        # so replace with "_" and ensure uniqueness
        fldname = name.replace("-", "_").replace(".", "_")
        while fldname in field2name:
            fldname += "_"
        field2name[fldname] = name

        # form up metadata
        metadata = dict(help=schema.info, parameter=name)
        metadata.update(schema.metadata)
        if schema.choices:
            metadata['choices'] = schema.choices
        if schema.element_choices:
            metadata['element_choices'] = schema.element_choices
        metadata['required'] = required = schema.required

        if required and schema.default is not UNSET:
            raise SchemaError(
                f"Field '{fldname}' is required but specifies a default. "
                f"This behaviour is unsupported/ambiguous."
            )

        if required:
            fld = field(default=MISSING, metadata=metadata)
        elif isinstance(schema.default, MutableSequence):
            fld = field(default_factory=default_wrapper(list, schema.default),
                        metadata=metadata)
        elif isinstance(schema.default, MutableSet):
            fld = field(default_factory=default_wrapper(set, schema.default),
                        metadata=metadata)
        elif isinstance(schema.default, MutableMapping):
            fld = field(default_factory=default_wrapper(dict, schema.default),
                        metadata=metadata)
        elif schema.default is UNSET:
            fld = field(default=None, metadata=metadata)
        else:
            fld = field(default=schema.default, metadata=metadata)

        fields.append((fldname, schema._dtype, fld))

    namespace = None if post_init is None else dict(__post_init__=post_init)

    return make_dataclass(class_name, fields, bases=bases, namespace=namespace)


def default_wrapper(default_type, default_value):

    def default_factory():
        return default_type(default_value)

    return default_factory


def nested_schema_to_dataclass(nested: Dict[str, Dict], class_name: str, bases=(), section_bases=(), post_init_map={}):
    """Converts a nested schema (sections consisting of parameters) into a dataclass.

    Args:
        nested (Dict[str, Dict]):           schema sections
        class_name (str):                   name of dataclass created
        bases (tuple, optional):            base classes for outer class
        section_bases (tuple, optional):    base class for section classes
        post_init_map (dict, optional):     dict of specific __post_init__ methods to be attached to section classes

    Returns:
        tuple of (dataclass, dict):         resulting dataclass, plus dict of dataclasses for the sections
    """

    # make dataclass based on schema contents
    nested_structure = make_dataclass(f"_{class_name}_schemas", [(name, Dict[str, Parameter]) for name in nested.keys()])
    # turn into a structured config
    nested = OmegaConf.unsafe_merge(OmegaConf.structured(nested_structure), nested)
    fields = []

    # convert per-section schemas into dataclasses and make list of fields for outer dataclass
    for section, content in nested.items():
        dcls = schema_to_dataclass(content, f"{class_name}_{section}",
                                    bases=section_bases, post_init=post_init_map.get(section))

        fields.append((section, dcls, field(default_factory=dcls)))

    # return the outer dataclass
    return make_dataclass(class_name, fields, bases=bases)


_atomic_types = dict(bool=bool, str=str, int=int, float=float)

def _validate_list(text: str, element_type, schema):
    if not text:
        return schema.default
    if text == "[]":
        return []
    if text[0] == "[" and text[-1] == "]":
        text = text[1:-1]
    try:
        return [element_type(x) for x in text.split(",")]
    except ValueError:
        raise click.BadParameter(f"can't convert to '{schema.dtype}'")

@dataclass
class Schema(object):
    inputs: Dict[str, Parameter] = EmptyDictDefault()
    outputs: Dict[str, Parameter] = EmptyDictDefault()


def clickify_parameters(schemas: Union[str, Dict[str, Any]]):

    if type(schemas) is str:
        schemas = OmegaConf.merge(OmegaConf.structured(Schema),
                                OmegaConf.load(schemas))

    decorator_chain = None
    for io in schemas.inputs, schemas.outputs:
        for name, schema in io.items():
            # skip outputs, unless they're named outputs
            if io is schemas.outputs and not (schema.is_file_type and not schema.implicit):
                continue

            name = name.replace("_", "-")
            optname = f"--{name}"
            dtype = schema.dtype
            validator = None

            # sort out option type. Atomic type?
            if dtype in _atomic_types:
                dtype = _atomic_types[dtype]
                if dtype is bool:
                    optname = f"{optname}/--no-{name}"
            # file type? NB: URI not included deliberately -- this becomes a str in the else: clause below
            elif dtype in ("MS", "File", "Directory"):
                dtype = click.Path(exists=(io is schemas.inputs))
            else:
                match = re.fullmatch("List\[(.*)\]", dtype)
                # List[x] type? Add validation callback to convert elements
                if match:
                    elem_type_name = match.group(1)
                    # convert "x" to type object -- unknown element types will get treated as a string
                    elem_type = _atomic_types.get(elem_type_name, str)
                    validator = lambda ctx, param, value, etype=elem_type, schema=schema: _validate_list(value, element_type=etype, schema=schema)
                # anything else will be just a string
                dtype = str

            # choices?
            if schema.choices:
                dtype = click.Choice(schema.choices)

            # aliases?
            optnames = [optname]
            if schema.abbreviation:
                optnames.append(f"-{schema.abbreviation}")

            if schema.policies.positional:
                if schema.default in (UNSET, _UNSET_DEFAULT):
                    deco = click.argument(name, type=dtype, callback=validator,
                                        required=schema.required,
                                        metavar=schema.metavar)
                else:
                    deco = click.argument(name, type=dtype, callback=validator,
                                        default=schema.default, required=schema.required,
                                        metavar=schema.metavar)
            else:
                if schema.default in (UNSET, _UNSET_DEFAULT):
                    deco = click.option(*optnames, type=dtype, callback=validator,
                                        required=schema.required,
                                        metavar=schema.metavar, help=schema.info)
                else:
                    deco = click.option(*optnames, type=dtype, callback=validator,
                                        default=schema.default, required=schema.required,
                                        metavar=schema.metavar, help=schema.info)

            if decorator_chain is None:
                decorator_chain = deco
            else:
                decorator_chain = lambda x,deco=deco,chain=decorator_chain: chain(deco(x))

    return decorator_chain

@dataclass
class SchemaSpec:
    inputs: Dict[str, Parameter]
    outputs: Dict[str, Parameter]
    libs: Dict[str, Any]

def paramfile_loader(paramfiles: Union[File, List[File]], sources: Union[File, List[File]] = [], 
                     schema_spec=None, use_cache=False) -> Dict:
    """Load a scabha-style parameter defintion using.

    Args:
        paramfiles (List[File]): Name of parameter definition files
        sources (List[Dict], optional): Parameter definition dependencies 
        (a.k.a files specified via_include)

    Returns:
        Dict: Schema object
    """
    args_defn = OmegaConf.structured(schema_spec or SchemaSpec)
    if isinstance(paramfiles, File):
        paramfiles = [paramfiles]
    
    if isinstance(sources, File):
        sources = [sources]
        
    srcs = []
    for src in sources:
        if not src.EXISTS:
            raise FileNotFoundError(f"Source file for either of {paramfiles} could not be found at {src.PATH}")
        srcs.append(configuratt.load(src, use_cache=use_cache)[0])
        
    struct_args, _ = configuratt.load_nested(paramfiles, structured=args_defn,
                                            use_sources=srcs, use_cache=use_cache)
    
    return OmegaConf.create(struct_args)

