from scabha.cargo import Parameter
from typing import Dict, Any


_jones_schema = dict(
    t_int=Parameter(dtype="Union[str, int]", info="time solution interval"),
    f_int=Parameter(dtype="Union[str, int]", info="frequency solution interval"),
) 


def cubical_schema(params: Dict[str, Any], inputs: Dict[str, Parameter], outputs: Dict[str, Parameter]):
    inputs = inputs.copy()

    for jones in params['jones']:
        for key, value in _jones_schema.items():
            inputs[f"{jones}.{key}"] = value

    return inputs, outputs
