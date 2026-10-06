from .test_recipe import run, verify_output


def test_len_loop_subscripts():
    """A for-loop that indexes two lists by the loop variable (=recipe.list-a[recipe.i]).

    Subscripts in formulas broke under pyparsing 3.3.3 ('string indices must be integers'),
    and no test checked the values the loop produced.
    """
    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_len_loop.yml main_recipe")
    assert retcode == 0
    print(output)
    assert verify_output(output, r"args = \['test', 'x'\]", r"args = \['me', 'y'\]", r"args = \['now', 'z'\]")
    assert not verify_output(output, "string indices must be integers")


def test_zip_loop_subscripts():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_zip_loops.yml main_recipe")
    assert retcode == 0
    print(output)
    assert verify_output(output, r"args = \['test', 'x'\]", r"args = \['me', 'y'\]")
    assert not verify_output(output, "string indices must be integers")
