import os, re, subprocess, pytest
from .test_recipe import change_test_dir, run, verify_output

def callable_function(a: int, b: str):
    print(f"callable_function({a},'{b}')")
    return a*2

def callable_function_dict(a: int, b: str):
    print(f"callable_function_dict({a},'{b}')")
    return dict(x=a*2, y=b+b)

def test_wrangler_replace_suppress():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -v -b native run test_callables.yml")
    assert retcode == 0
    print(output)
    assert verify_output(output, 'y = 46barbar')
