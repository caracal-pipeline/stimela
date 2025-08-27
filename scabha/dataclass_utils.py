from copy import deepcopy
from dataclasses import fields, is_dataclass


def merge_dataclass_instances(inst1: object, inst2: object):
    """Given two instances of a given dataclass, create a new instance which combines their fields.

    Given dataclass instances, checks that they are the same before creating a new instance which
    combines their values. Values from the second instance are given priority unless they are None.
    This provides a mechanism for combining dataclasses after they have been instantiated as
    opposed to the approach taken by OmegaConf. If the second instance is None, will return a copy
    of the first object.

    Args:
        inst1: Instance of an arbitrary dataclass object.
        inst2: Instance of an arbitrary dataclass object.
    """

    _type = type(inst1)

    if not is_dataclass(_type):
        raise TypeError(f"Object of type {_type} is not a dataclass.")

    if inst2 is None:
        return deepcopy(inst1)

    if not isinstance(inst2, _type):
        raise TypeError(f"Instances are of different types: {_type} and {type(inst2)}.")

    kwargs = {}

    for field in fields(_type):
        field_name = field.name
        val_inst1 = getattr(inst1, field_name)
        val_inst2 = getattr(inst2, field_name)
        kwargs[field_name] = val_inst2 if val_inst2 is not None else val_inst1

    return _type(**kwargs)
