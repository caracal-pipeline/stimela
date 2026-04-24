"""Characterisation tests for scabha.validate.validate_parameters.

Pins current behaviour of the pydantic-backed parameter validator so we can
migrate v1 -> v2 without silently changing semantics. Every test here must
stay green across the migration; any intentional behavioural change must
flip a specific assertion and be recorded in plan/migrate_pydantic.md.
"""

from typing import Any, Dict, List, Optional, Tuple, Union

import pytest

from scabha.basetypes import UNSET, Directory, File, Unresolved
from scabha.cargo import Parameter
from scabha.exceptions import ParameterValidationError, ScabhaBaseException
from scabha.validate import validate_parameters


def make_schema(dtype: str, *, required: bool = False, default: Any = None, choices=(), is_output: bool = False):
    """Build a Parameter (schema) with the fields validate_parameters actually reads."""
    kwargs: Dict[str, Any] = {"dtype": dtype, "required": required, "choices": choices}
    if default is not None:
        kwargs["default"] = default
    p = Parameter(**kwargs)
    if is_output:
        p._is_input = False
    return p


def run_validate(params, schemas, **kwargs):
    defaults = {"check_inputs_exist": False, "check_outputs_exist": False, "check_required": False}
    defaults.update(kwargs)
    return validate_parameters(params, schemas, **defaults)


# --- scalar coercion ---------------------------------------------------------


class TestScalarCoercion:
    def test_int_happy(self):
        out = run_validate({"x": 5}, {"x": make_schema("int")})
        assert out["x"] == 5
        assert type(out["x"]) is int

    def test_int_from_str(self):
        out = run_validate({"x": "5"}, {"x": make_schema("int")})
        assert out["x"] == 5
        assert type(out["x"]) is int

    def test_int_from_lossy_str_raises(self):
        with pytest.raises(ParameterValidationError):
            run_validate({"x": "1.5"}, {"x": make_schema("int")})

    def test_float_happy(self):
        out = run_validate({"x": 1.5}, {"x": make_schema("float")})
        assert out["x"] == 1.5

    def test_float_from_int(self):
        out = run_validate({"x": 1}, {"x": make_schema("float")})
        assert out["x"] == 1.0
        assert type(out["x"]) is float

    def test_float_from_str(self):
        out = run_validate({"x": "1.5"}, {"x": make_schema("float")})
        assert out["x"] == 1.5

    def test_str_happy(self):
        out = run_validate({"x": "hello"}, {"x": make_schema("str")})
        assert out["x"] == "hello"

    def test_str_from_int(self):
        out = run_validate({"x": 1}, {"x": make_schema("str")})
        assert out["x"] == "1"

    def test_bool_happy_true(self):
        out = run_validate({"x": True}, {"x": make_schema("bool")})
        assert out["x"] is True

    def test_bool_happy_false(self):
        out = run_validate({"x": False}, {"x": make_schema("bool")})
        assert out["x"] is False

    def test_bool_from_str_true(self):
        out = run_validate({"x": "true"}, {"x": make_schema("bool")})
        assert out["x"] is True

    def test_bool_from_str_false(self):
        out = run_validate({"x": "false"}, {"x": make_schema("bool")})
        assert out["x"] is False

    def test_bool_from_int_1(self):
        out = run_validate({"x": 1}, {"x": make_schema("bool")})
        assert out["x"] is True

    def test_bool_from_int_0(self):
        out = run_validate({"x": 0}, {"x": make_schema("bool")})
        assert out["x"] is False


# --- Optional / Union / Literal ---------------------------------------------


class TestOptional:
    def test_optional_int_with_int(self):
        out = run_validate({"x": 1}, {"x": make_schema("Optional[int]")})
        assert out["x"] == 1

    def test_optional_int_with_none(self):
        out = run_validate({"x": None}, {"x": make_schema("Optional[int]")})
        assert out["x"] is None

    def test_optional_int_from_str_coerces(self):
        out = run_validate({"x": "5"}, {"x": make_schema("Optional[int]")})
        assert out["x"] == 5


class TestUnion:
    def test_union_str_int_picks_str(self):
        out = run_validate({"x": "hello"}, {"x": make_schema("Union[str, int]")})
        assert out["x"] == "hello"

    def test_union_str_int_picks_int(self):
        out = run_validate({"x": 5}, {"x": make_schema("Union[str, int]")})
        assert out["x"] in (5, "5")


# --- containers --------------------------------------------------------------


class TestContainers:
    def test_list_int_happy(self):
        out = run_validate({"x": [1, 2, 3]}, {"x": make_schema("List[int]")})
        assert out["x"] == [1, 2, 3]

    def test_list_int_mixed_coerces(self):
        out = run_validate({"x": [1, "2", 3]}, {"x": make_schema("List[int]")})
        assert out["x"] == [1, 2, 3]

    def test_list_str_happy(self):
        out = run_validate({"x": ["a", "b"]}, {"x": make_schema("List[str]")})
        assert out["x"] == ["a", "b"]

    def test_tuple_heterogeneous(self):
        """Pin the fixed-length tuple behaviour - each position has its own type."""
        out = run_validate({"x": [1, "two"]}, {"x": make_schema("Tuple[int, str]")})
        assert list(out["x"]) == [1, "two"]

    def test_tuple_variadic(self):
        out = run_validate({"x": [1, 2, 3]}, {"x": make_schema("Tuple[int, ...]")})
        assert list(out["x"]) == [1, 2, 3]

    def test_dict_str_int_happy(self):
        out = run_validate({"x": {"a": 1}}, {"x": make_schema("Dict[str, int]")})
        assert out["x"] == {"a": 1}

    def test_dict_str_int_from_str_values(self):
        out = run_validate({"x": {"a": "1"}}, {"x": make_schema("Dict[str, int]")})
        assert out["x"] == {"a": 1}


# --- File / Directory / URI -------------------------------------------------


class TestFileTypes:
    def test_file_instance_from_string(self, tmp_path):
        p = tmp_path / "foo.txt"
        p.touch()
        out = run_validate(
            {"x": str(p)},
            {"x": make_schema("File")},
            check_inputs_exist=True,
        )
        assert out["x"] == str(p)

    def test_file_missing_raises_when_must_exist(self, tmp_path):
        with pytest.raises(ParameterValidationError):
            run_validate(
                {"x": str(tmp_path / "nope.txt")},
                {"x": make_schema("File")},
                check_inputs_exist=True,
            )

    def test_list_file_accepts_list_of_paths(self, tmp_path):
        p1 = tmp_path / "a.txt"
        p2 = tmp_path / "b.txt"
        p1.touch()
        p2.touch()
        out = run_validate(
            {"x": [str(p1), str(p2)]},
            {"x": make_schema("List[File]")},
            check_inputs_exist=True,
        )
        assert out["x"] == [str(p1), str(p2)]

    def test_directory_type(self, tmp_path):
        out = run_validate(
            {"x": str(tmp_path)},
            {"x": make_schema("Directory")},
            check_inputs_exist=True,
        )
        assert out["x"] == str(tmp_path)


# --- Unresolved / UNSET sentinels -------------------------------------------


class TestSentinels:
    def test_unresolved_passes_through(self):
        u = Unresolved("{some.ref}")
        out = run_validate({"x": u}, {"x": make_schema("int")})
        assert isinstance(out["x"], Unresolved)

    def test_unset_value_omitted_from_output(self):
        out = run_validate({"x": UNSET}, {"x": make_schema("int")})
        assert "x" not in out or out.get("x") is UNSET


# --- error flow --------------------------------------------------------------


class TestErrorFlow:
    def test_missing_required_raises_param_validation_error(self):
        """Required+UNSET triggers the explicit missing-param check."""
        with pytest.raises(ParameterValidationError):
            run_validate(
                {"x": UNSET},
                {"x": make_schema("int", required=True)},
                check_required=True,
            )

    def test_param_validation_error_is_scabha_base_exception(self):
        """Critical: step.py catches ScabhaBaseException to downgrade on skip."""
        with pytest.raises(ScabhaBaseException):
            run_validate({"x": "not-an-int"}, {"x": make_schema("int")})

    def test_pydantic_validation_error_never_leaks(self):
        """Pydantic's own ValidationError must be wrapped, not raised directly."""
        import pydantic  # noqa: PLC0415

        try:
            run_validate({"x": "not-an-int"}, {"x": make_schema("int")})
        except ParameterValidationError:
            pass
        except pydantic.ValidationError as exc:
            pytest.fail(f"raw pydantic.ValidationError leaked: {exc}")

    def test_unknown_param_raises(self):
        with pytest.raises(ParameterValidationError):
            run_validate({"mystery": 1}, {"x": make_schema("int")}, check_unknowns=True)

    def test_error_message_contains_param_name(self):
        with pytest.raises(ParameterValidationError) as exc_info:
            run_validate({"my_param": "not-an-int"}, {"my_param": make_schema("int")})
        assert "my_param" in str(exc_info.value)


# --- choices ----------------------------------------------------------------


class TestChoices:
    def test_choice_accepts_valid(self):
        out = run_validate({"x": "a"}, {"x": make_schema("str", choices=["a", "b"])})
        assert out["x"] == "a"

    def test_choice_rejects_invalid(self):
        with pytest.raises(ParameterValidationError):
            run_validate({"x": "z"}, {"x": make_schema("str", choices=["a", "b"])})


# --- sanity: pydantic import still works -----------------------------------


def test_pydantic_importable():
    """Sanity check: the module loads and pydantic is available."""
    from scabha import validate  # noqa: PLC0415

    assert hasattr(validate, "validate_parameters")


# --- guard against typing imports breaking downstream -----------------------


def test_typing_imports_stable():
    """Ensure public typing names used by recipes still resolve via dtype strings."""
    for dtype in [
        "int",
        "str",
        "bool",
        "float",
        "List[int]",
        "Dict[str, int]",
        "Optional[str]",
        "Union[int, str]",
        "Tuple[int, ...]",
        "File",
        "Directory",
        "List[File]",
    ]:
        p = Parameter(dtype=dtype)
        assert p._dtype is not None, f"dtype {dtype!r} failed to resolve"


# Re-export type imports so linters don't complain they're unused.
_ = (Any, Dict, List, Optional, Tuple, Union, Directory, File)
