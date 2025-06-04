import os, re, subprocess, pytest
from .test_recipe import change_test_dir, run, verify_output

def test_conditional_skips():
    os.system("rm -fr test_conditional_skips[1234].tmp")
    print("===== all three files touched =====")
    retcode, output = run("stimela -b native run test_conditional_skips.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips1.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips2.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips3.tmp")

    print("===== 2 and 3 touched =====")
    os.system("touch test_conditional_skips4.tmp")
    retcode, output = run("stimela -b native run test_conditional_skips.yml")
    assert retcode == 0
    print(output)
    assert not verify_output(output, "---INVOKING---", "touch test_conditional_skips1.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips2.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips3.tmp")

    print("===== only 3 touched =====")
    retcode, output = run("stimela -b native run test_conditional_skips.yml")
    assert retcode == 0
    print(output)
    assert not verify_output(output, "---INVOKING---", "touch test_conditional_skips1.tmp")
    assert not verify_output(output, "---INVOKING---", "touch test_conditional_skips2.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips3.tmp")

    print("===== 2 and 3 touched =====")
    retcode, output = run("stimela -b native run test_conditional_skips.yml -f")
    assert retcode == 0
    print(output)
    assert not verify_output(output, "---INVOKING---", "touch test_conditional_skips1.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips2.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips3.tmp")

    print("===== 1 and 3 touched =====")
    retcode, output = run("stimela -b native run test_conditional_skips.yml -F")
    assert retcode == 0
    print(output)
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips1.tmp")
    assert not verify_output(output, "---INVOKING---", "touch test_conditional_skips2.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips3.tmp")

    print("===== all three files touched =====")
    retcode, output = run("stimela -b native run test_conditional_skips.yml -f -F")
    assert retcode == 0
    print(output)
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips1.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips2.tmp")
    assert verify_output(output, "---INVOKING---", "touch test_conditional_skips3.tmp")

    print("===== cleaning up =====")
    os.system("rm -fr test_conditional_skips[1234].tmp")
