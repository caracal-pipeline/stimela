import os, re, subprocess, pytest
from .test_recipe import change_test_dir, run, verify_output

def test_wrangler_replace_suppress():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_wranglers.yml test_replace_suppress")
    assert retcode == 0
    print(output)
    assert verify_output(output, "Michael J. Fox", "don't need roads!")
    assert not verify_output(output, "cheetah")


def test_wrangler_force_success():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_wranglers.yml test_force_success")
    assert retcode == 0
    print(output)
    assert verify_output(output, "deliberately declared")


def test_wrangler_force_failure():
    print("===== expecting an error =====")
    retcode, output = run("stimela -b native run test_wranglers.yml test_force_failure")
    assert retcode != 0
    print(output)
    assert verify_output(output, "cab marked as failed")

    print("===== expecting an error =====")
    retcode, output = run("stimela -b native run test_wranglers.yml test_force_failure2")
    assert retcode != 0
    print(output)
    assert verify_output(output, "Nobody expected the fox!")

def test_wrangler_parse():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_wranglers.yml test_parse")
    assert retcode == 0
    print(output)
    assert verify_output(output, "The bloody cheetah ate 22 dogs!")

    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_wranglers.yml test_parse2")
    assert retcode == 0
    print(output)
    assert verify_output(output, "The bloody cheetah ate 22 dogs!")

    print("===== expecting no errors =====")
    retcode, output = run("stimela -b native run test_wranglers.yml test_parse3")
    assert retcode == 0
    print(output)
    assert verify_output(output, "The bloody cheetah ate 22 dogs!")

