import click
from scabha.exceptions import SchemaError
from .cargo import Parameter, UNSET
from typing import *
from .basetypes import *
from dataclasses import make_dataclass, field
from omegaconf import OmegaConf, MISSING
from collections import OrderedDict, MutableSet, MutableSequence, MutableMapping

def schema_to_dataclass(io: Dict[str, Parameter], class_name: str, bases=(), post_init: Optional[Callable] =None):
    """Converts a scabha schema to a dataclass.
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

        fields.append((section, dcls, field(default=dcls())))

    # return the outer dataclass
    return make_dataclass(class_name, fields, bases=bases)


def clickify_parameters(schemas: Dict[str, Any]):

    decorator_chain = None
    for io in schemas.inputs, schemas.outputs:
        for name, schema in io.items():

            name = name.replace("_", "-")
            optname = f"--{name}"

            # sort out option type
            if schema.dtype == "bool":
                optname = f"{optname}/--no-{name}"
                dtype = bool
            elif schema.dtype == "str":
                dtype = str
            elif schema.dtype == "int":
                dtype = int
            elif schema.dtype == "float":
                dtype = float
            elif schema.dtype == "MS":
                dtype = click.Path(exists=True)

            # choices?
            if schema.choices:
                dtype = click.Choice(schema.choices)

            # aliases?
            optnames = [optname]
            if schema.abbreviation:
                optnames.append(f"-{schema.abbreviation}")

            if schema.default is UNSET:
                deco = click.option(*optnames, type=dtype,
                                    required=schema.required, 
                                    metavar=schema.metavar,help=schema.info)
            else:
                deco = click.option(*optnames, type=dtype,
                                    default=schema.default, required=schema.required, 
                                    metavar=schema.metavar,help=schema.info)

            if decorator_chain is None:
                decorator_chain = deco
            else:
                decorator_chain = lambda x,deco=deco,chain=decorator_chain: chain(deco(x))

    return decorator_chain
