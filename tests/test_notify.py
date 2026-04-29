from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_notifications():
    retcode, output = run("stimela -b native run test_notify.yml")
    assert retcode == 0
    print(output)
    # INFO-level messages appear once (no summary by default), WARNING+ appear twice (execution + summary)
    assert verify_output(output, r"INFO: This is a preamble about \d+ \d+ in general") == 5
    assert verify_output(output, r"INFO: This is a step-specific epilogue about life in general") == 1
    assert verify_output(output, r"WARNING: This is a step-specific epilogue about 2 == 2") == 2
    assert verify_output(output, r"WARNING: This is an epilogue about 3 == 3") == 2

    retcode, output = run("stimela -b native run test_notify.yml step4.x=42")
    assert retcode == 1
    print(output)
    assert verify_output(output, r"INFO: This is a preamble about \d+ \d+ in general") == 5
    assert verify_output(output, r"INFO: This is a step-specific epilogue about life in general") == 1
    assert verify_output(output, r"WARNING: This is a step-specific epilogue about 2 == 2") == 2
    assert verify_output(output, r"CRITICAL: ABORT: x is 42, I'm out of here!") == 1

    retcode, output = run("stimela -b native run test_notify.yml step4.x=43")
    assert retcode == 1
    print(output)
    assert verify_output(output, r"INFO: This is a preamble about \d+ \d+ in general") == 5
    assert verify_output(output, r"INFO: This is a step-specific epilogue about life in general") == 1
    assert verify_output(output, r"WARNING: This is a step-specific epilogue about 2 == 2") == 2
    assert verify_output(output, r"CRITICAL: epilogue: assert:") == 1
