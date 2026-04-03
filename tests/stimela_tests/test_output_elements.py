from .test_recipe import change_test_dir as change_test_dir
from .test_recipe import run, verify_output


def test_output_elements():
    retcode, output = run("stimela -b native run test_output_elements.yml")
    print(output)
    assert retcode == 0
    # x iterates over [1, 2, 3], y = x*2
    # sums = [1+2, 2+4, 3+6] = [3, 6, 9]
    # products = [1*2, 2*4, 3*6] = [2, 8, 18]
    # epilogue logs the message output which contains the formatted results
    assert verify_output(output, r"sums: \[3, 6, 9\], products: \[2, 8, 18\]") >= 1
