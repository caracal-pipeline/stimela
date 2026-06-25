"""Tests for _abbreviate_repr in python_flavours."""

from stimela.backends.flavours.python_flavours import _abbreviate_repr


class TestAbbreviateRepr:
    """Tests for _abbreviate_repr."""

    def test_short_string_unchanged(self):
        assert _abbreviate_repr("hello") == "'hello'"

    def test_long_string_truncated(self):
        long_str = "a" * 100
        result = _abbreviate_repr(long_str, max_str_len=10)
        assert result == repr("a" * 10 + "...")

    def test_string_at_max_len_unchanged(self):
        s = "a" * 80
        result = _abbreviate_repr(s, max_str_len=80)
        assert result == repr(s)

    def test_short_list_unchanged(self):
        result = _abbreviate_repr([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_long_list_truncated(self):
        result = _abbreviate_repr(list(range(10)), max_collection_items=3)
        assert result == "[0, 1, 2, ...7 more]"

    def test_short_tuple_unchanged(self):
        result = _abbreviate_repr((1, 2))
        assert result == "(1, 2)"

    def test_long_tuple_truncated(self):
        result = _abbreviate_repr(tuple(range(8)), max_collection_items=3)
        assert result == "(0, 1, 2, ...5 more)"

    def test_short_dict_unchanged(self):
        result = _abbreviate_repr({"a": 1})
        assert result == "{'a': 1}"

    def test_long_dict_truncated(self):
        d = {f"k{i}": i for i in range(10)}
        result = _abbreviate_repr(d, max_collection_items=2)
        assert result.startswith("{")
        assert result.endswith("...8 more}")
        assert "'k0': 0" in result
        assert "'k1': 1" in result

    def test_nested_list_elements_abbreviated(self):
        """Nested structures in lists should also be abbreviated."""
        nested = ["a" * 200, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]
        result = _abbreviate_repr(nested, max_str_len=10, max_collection_items=5)
        # The long string element should be truncated
        assert "..." in result
        assert "a" * 200 not in result

    def test_nested_dict_values_abbreviated(self):
        """Nested values in dicts should also be abbreviated."""
        d = {"key": "b" * 200}
        result = _abbreviate_repr(d, max_str_len=10)
        # The long string value should be truncated
        assert "b" * 200 not in result

    def test_int_unchanged(self):
        assert _abbreviate_repr(42) == "42"

    def test_none_unchanged(self):
        assert _abbreviate_repr(None) == "None"

    def test_bool_unchanged(self):
        assert _abbreviate_repr(True) == "True"

    def test_list_at_max_items_unchanged(self):
        result = _abbreviate_repr([1, 2, 3], max_collection_items=3)
        assert result == "[1, 2, 3]"

    def test_dict_does_not_materialize_full_items(self):
        """Large dict should not build a full items list."""
        large_dict = {f"k{i}": i for i in range(1000)}
        result = _abbreviate_repr(large_dict, max_collection_items=2)
        assert "...998 more" in result
