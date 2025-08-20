from .test_recipe import change_test_dir as change_test_dir, run, verify_output


def test_param_file_input():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_param_file.yml test-param-file -pf param_file.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, "A string!", "['A', 'list', '!']")
    assert not verify_output(output, "gremlin")
    assert verify_output(output, "Found a unicorn!")
