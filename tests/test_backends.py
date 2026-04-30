from .test_recipe import run, verify_output


def test_backend_varieties():
    print("===== expecting no errors =====")
    retcode, output = run("stimela -v -b native run test_backends.yml test_recipe")
    assert retcode == 0
    print(output)
    assert verify_output(output, "full command line is sleep 1")

    print("===== expecting an error (broken backend selected) =====")
    retcode, output = run("stimela -v -b native run test_backends.yml test_recipe2")
    assert retcode != 0
    print(output)
    assert verify_output(output, "unable to select a backend")

    print("===== expecting an error (broken backend selected) =====")
    retcode, output = run("stimela -v -b native run test_backends.yml test_recipe3")
    assert retcode != 0
    print(output)
    assert verify_output(output, "unable to select a backend")

    print("===== expecting an error (bad singularoty image) =====")
    retcode, output = run("stimela -v -b native run test_backends.yml test_recipe4")
    assert retcode != 0
    print(output)
    # not verifying output here -- can fail due to bad image, or due to missing singularity
    # (the latter I suppose will happen in gh actions)
