import os

from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_grumbles():
    retcode, output = run("stimela -b native run test_grumbles.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, "INFO: This is a pre-grumble about \d+ \d+ in general") == 3
    assert verify_output(output, "INFO: This is a step-specific post-grumble about life in general") == 1
    assert verify_output(output, "WARNING: This is a step-specific post-grumble about 2 == 2 ") == 2
    assert verify_output(output, "WARNING: This is a post-grumble about 3 == 3") == 2

    retcode, output = run("stimela -b native run test_grumbles.yml step3.x=42")
    assert retcode == 1
    print(output)
    assert verify_output(output, "INFO: This is a pre-grumble about \d+ \d+ in general") == 3
    assert verify_output(output, "INFO: This is a step-specific post-grumble about life in general") == 1
    assert verify_output(output, "WARNING: This is a step-specific post-grumble about 2 == 2 ") == 2
    assert verify_output(output, "CRITICAL: x is 42, I'm out of here!") == 2
        