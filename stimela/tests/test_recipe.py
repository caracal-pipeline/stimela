import stimela
import os

def test_test_recipe():
    print("===== expecting an error since 'msname' parameter is missing =====")
    retcode = os.system("stimela -v exec test_recipe.yml selfcal_image_name=bar")
    assert retcode != 0 

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v exec test_recipe.yml selfcal_image_name=bar msname=foo")
    assert retcode == 0

def test_test_loop_recipe():
    print("===== expecting an error since 'ms' parameter is missing =====")
    retcode = os.system("stimela -v exec test_loop_recipe.yml cubical_image")
    assert retcode != 0

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v exec test_loop_recipe.yml cubical_image_loop ms=foo")
    assert retcode == 0

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v exec test_loop_recipe.yml same_as_cubical_image_loop ms=foo")
    assert retcode == 0

    print("===== expecting no errors now =====")
    retcode = os.system("stimela -v exec test_loop_recipe.yml loop_recipe")
    assert retcode == 0



def test_runtime_recipe():
    ## OMS
    ## disabling for now, need to revise to use "dummy" cabs (or add real cabs?)
    return

    DIRS = {
        "indir": "input",
        "outdir": "outdir",
        "msdir": "msdir",
    }

    MS = "example.ms"


    recipe = stimela.Recipe("test recipe")

    recipe.add("simms", label="makems", params={
        "msname": MS,
        "synthesis": 1,
        "telescope": "kat-7",
        "dtime": 1,
        "dfreq": "1MHz",
        "nchan": 5,
    }, 
    info="Make simulated MS")

    recipe.add("wscleam", label="image", params={
        "ms": recipe.makems.outputs.ms,  # this can't work, since a recipe is a runtime object not an OmegaConf dict
                                         # need to define an API for this...
        "name": "example",
        "scale": 1,
        "size": 512,
        "make-psf-only": True,
        "weight": "uniform",
    },
    info="Image MS PSF")

    recipe.run()
