
import sys

def example_function(x, y):
    print(f"x is {x}")
    print(f"y is {y}", file=sys.stderr)
    return 0

def example_function_dict(x, y):
    print(f"x is {x}")
    print(f"y is {y}", file=sys.stderr)
    return dict(a=x, b=y)
