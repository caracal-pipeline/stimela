from scabha.basetypes import get_filelikes, File, URI, Directory, MS
from typing import Dict, List, Set, Tuple, Union, Optional
import pytest


@pytest.fixture(scope="module", params=[File, URI, Directory, MS])
def templates(request):
    
    ft = request.param
    
    TEMPLATES = (
        (Tuple, (), set()),
        (Tuple[int, ...], [1, 2], set()),
        (Tuple[ft, ...], ("foo", "bar"), {"foo", "bar"}),
        (Tuple[ft, str], ("foo", "bar"), {"foo"}),
        (Dict[str, int], {"a": 1, "b": 2}, set()),
        (Dict[str, ft], {"a": "foo", "b": "bar"}, {"foo", "bar"}),
        (Dict[ft, str], {"foo": "a", "bar": "b"}, {"foo", "bar"}),
        (List[ft], [], set()),
        (List[int], [1, 2], set()),
        (List[ft], ["foo", "bar"], {"foo", "bar"}),
        (Set[ft], set(), set()),
        (Set[int], {1, 2}, set()),
        (Set[ft], {"foo", "bar"}, {"foo", "bar"}),
        (Union[str, List[ft]], "foo", set()),
        (Union[str, List[ft]], ["foo"], {"foo"}),
        (Union[str, Tuple[ft]], "foo", set()),
        (Union[str, Tuple[ft]], ("foo",), {"foo"}),
        (Optional[ft], None, set()),
        (Optional[ft], "foo", {"foo"}),
        (Optional[Union[ft, int]], 1, set()),
        (Optional[Union[ft, int]], "foo", {"foo"}),
        (Dict[str, Tuple[ft, str]], {"a": ("foo", "bar")}, {"foo"})
    )

    return TEMPLATES


def test_get_filelikes(templates):

    for dt, v, res in templates:
        assert get_filelikes(dt, v) == res, f"Failed for dtype {dt} and value {v}."
