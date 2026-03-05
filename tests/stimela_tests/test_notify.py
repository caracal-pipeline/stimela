from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_notifications():
    retcode, output = run("stimela -b native run test_notify.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, r"INFO: This is a pre-notification about \d+ \d+ in general") == 3
    assert verify_output(output, r"INFO: This is a step-specific post-notification about life in general") == 1
    assert verify_output(output, r"WARNING: This is a step-specific post-notification about 2 == 2 ") == 2
    assert verify_output(output, r"WARNING: This is a post-notification about 3 == 3") == 2

    retcode, output = run("stimela -b native run test_notify.yml step3.x=42")
    assert retcode == 1
    print(output)
    assert verify_output(output, r"INFO: This is a pre-notification about \d+ \d+ in general") == 3
    assert verify_output(output, r"INFO: This is a step-specific post-notification about life in general") == 1
    assert verify_output(output, r"WARNING: This is a step-specific post-notification about 2 == 2 ") == 2
    assert verify_output(output, r"CRITICAL: x is 42, I'm out of here!") == 2
