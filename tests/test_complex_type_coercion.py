"""Tests for complex type coercion in {}-substitution (issue #364).

When a parameter value passes through {}-substitution, Python's string
formatter converts it to its str() representation. For composite types like
List[Tuple[float, float]], this produces a string that pydantic cannot
validate directly. The patch in stimela._patches fixes this by parsing
string values back into Python objects before validation.
"""

import pytest
import scabha.validate  # noqa: E402
from scabha.cargo import Parameter  # noqa: E402
from scabha.substitutions import SubstitutionNS  # noqa: E402

# Import stimela to apply the scabha patches before importing scabha.validate
import stimela  # noqa: F401 -- side-effect: applies scabha patches


@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(request.fspath.dirname)


def _make_schemas(**dtypes):
    """Helper: build a schemas dict from name=dtype_string pairs."""
    return {name: Parameter(dtype=dtype) for name, dtype in dtypes.items()}


def _validate(params, schemas, **kwargs):
    """Shortcut for validate_parameters with common defaults.

    Uses scabha.validate.validate_parameters (accessed via module attribute)
    so that the patched version is called rather than the original.
    """
    return scabha.validate.validate_parameters(
        params,
        schemas,
        check_required=False,
        check_inputs_exist=False,
        check_outputs_exist=False,
        **kwargs,
    )


# ---- List[Tuple[float, float]] - the original bug ----


class TestListTupleCoercion:
    """Tests for the core bug: List[Tuple[float, float]] via {}-substitution."""

    def test_string_with_tuple_syntax(self):
        """String produced by str() on a list of tuples should be parsed."""
        schemas = _make_schemas(kernel="List[Tuple[float,float]]")
        result = _validate({"kernel": "[(1.0, 2.0), (3.0, 4.0)]"}, schemas)
        assert result["kernel"] == [(1.0, 2.0), (3.0, 4.0)]

    def test_string_with_list_syntax(self):
        """String produced by str() on a list of lists should be parsed."""
        schemas = _make_schemas(kernel="List[Tuple[float,float]]")
        result = _validate({"kernel": "[[1.0, 2.0], [3.0, 4.0]]"}, schemas)
        assert result["kernel"] == [(1.0, 2.0), (3.0, 4.0)]

    def test_direct_list_of_tuples(self):
        """Direct list of tuples should still work (regression check)."""
        schemas = _make_schemas(kernel="List[Tuple[float,float]]")
        result = _validate({"kernel": [(1.0, 2.0), (3.0, 4.0)]}, schemas)
        assert result["kernel"] == [(1.0, 2.0), (3.0, 4.0)]

    def test_direct_list_of_lists(self):
        """Direct list of lists should be coerced to tuples by pydantic."""
        schemas = _make_schemas(kernel="List[Tuple[float,float]]")
        result = _validate({"kernel": [[1.0, 2.0], [3.0, 4.0]]}, schemas)
        assert result["kernel"] == [(1.0, 2.0), (3.0, 4.0)]

    def test_formula_substitution(self):
        """=recipe.kernel should resolve to the Python object directly."""
        schemas = _make_schemas(kernel="List[Tuple[float,float]]")
        current = SubstitutionNS(kernel="=recipe.kernel")
        subst = SubstitutionNS(
            recipe=SubstitutionNS(kernel=[[1.0, 2.0], [3.0, 4.0]]),
            current=current,
        )
        result = _validate({"kernel": "=recipe.kernel"}, schemas, subst=subst)
        assert result["kernel"] == [(1.0, 2.0), (3.0, 4.0)]

    def test_brace_substitution(self):
        """{recipe.kernel} should stringify then be parsed back correctly."""
        schemas = _make_schemas(kernel="List[Tuple[float,float]]")
        current = SubstitutionNS(kernel="{recipe.kernel}")
        subst = SubstitutionNS(
            recipe=SubstitutionNS(kernel=[[1.0, 2.0], [3.0, 4.0]]),
            current=current,
        )
        result = _validate({"kernel": "{recipe.kernel}"}, schemas, subst=subst)
        assert result["kernel"] == [(1.0, 2.0), (3.0, 4.0)]


# ---- Other composite types ----


class TestOtherCompositeTypes:
    """Verify coercion works for other composite types too."""

    def test_list_int_from_string(self):
        schemas = _make_schemas(nums="List[int]")
        result = _validate({"nums": "[1, 2, 3]"}, schemas)
        assert result["nums"] == [1, 2, 3]

    def test_tuple_float_from_string(self):
        schemas = _make_schemas(point="Tuple[float, float]")
        result = _validate({"point": "(1.0, 2.0)"}, schemas)
        assert result["point"] == (1.0, 2.0)

    def test_dict_str_int_from_string(self):
        schemas = _make_schemas(mapping="Dict[str, int]")
        result = _validate({"mapping": "{'a': 1, 'b': 2}"}, schemas)
        assert result["mapping"] == {"a": 1, "b": 2}

    def test_optional_list_float_from_string(self):
        schemas = _make_schemas(vals="Optional[List[float]]")
        result = _validate({"vals": "[1.0, 2.0, 3.0]"}, schemas)
        assert result["vals"] == [1.0, 2.0, 3.0]

    def test_list_list_int_from_string(self):
        schemas = _make_schemas(matrix="List[List[int]]")
        result = _validate({"matrix": "[[1, 2], [3, 4]]"}, schemas)
        assert result["matrix"] == [[1, 2], [3, 4]]

    def test_tuple_str_int_from_string(self):
        schemas = _make_schemas(pair="Tuple[str, int]")
        result = _validate({"pair": "('hello', 42)"}, schemas)
        assert result["pair"] == ("hello", 42)


# ---- Non-composite types should NOT be affected ----


class TestSimpleTypesUnaffected:
    """Ensure simple types are not broken by the coercion patch."""

    def test_str_value_unchanged(self):
        schemas = _make_schemas(name="str")
        result = _validate({"name": "hello world"}, schemas)
        assert result["name"] == "hello world"

    def test_int_from_string(self):
        """Pydantic's own coercion handles str->int."""
        schemas = _make_schemas(count="int")
        result = _validate({"count": "42"}, schemas)
        assert result["count"] == 42

    def test_float_from_string(self):
        """Pydantic's own coercion handles str->float."""
        schemas = _make_schemas(value="float")
        result = _validate({"value": "3.14"}, schemas)
        assert result["value"] == pytest.approx(3.14)

    def test_bool_from_direct_value(self):
        schemas = _make_schemas(flag="bool")
        result = _validate({"flag": True}, schemas)
        assert result["flag"] is True


# ---- File types should NOT be affected ----


class TestFileTypesUnaffected:
    """Ensure file-type parameters are handled by original logic, not coerced."""

    def test_file_type(self):
        schemas = _make_schemas(fname="File")
        result = _validate({"fname": "/tmp/test.txt"}, schemas)
        assert "test.txt" in result["fname"]

    def test_list_file_type(self):
        schemas = _make_schemas(fnames="List[File]")
        result = _validate({"fnames": "[/tmp/a.txt, /tmp/b.txt]"}, schemas)
        assert len(result["fnames"]) == 2


# ---- Edge cases ----


class TestEdgeCases:
    """Edge cases for the coercion logic."""

    def test_empty_list_string(self):
        schemas = _make_schemas(items="List[int]")
        result = _validate({"items": "[]"}, schemas)
        assert result["items"] == []

    def test_single_element_list_string(self):
        schemas = _make_schemas(items="List[int]")
        result = _validate({"items": "[42]"}, schemas)
        assert result["items"] == [42]

    def test_nested_tuples_string(self):
        schemas = _make_schemas(data="List[Tuple[int, int, int]]")
        result = _validate({"data": "[(1, 2, 3), (4, 5, 6)]"}, schemas)
        assert result["data"] == [(1, 2, 3), (4, 5, 6)]

    def test_invalid_string_passes_through(self):
        """A string that can't be parsed should be passed to pydantic as-is."""
        schemas = _make_schemas(items="List[int]")
        with pytest.raises(Exception):
            _validate({"items": "not a list at all"}, schemas)
