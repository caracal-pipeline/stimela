from .test_recipe import run, verify_output


def test_wrangler_replace_suppress():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -v -b native run test_callables.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, "y = 46barbar")
