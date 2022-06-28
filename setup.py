#!/usr/bin/env python

import os
import sys
from setuptools import setup, find_packages
import glob


requirements = [
                "pyyaml",
#                "nose>=1.3.7", #do we need nose when we do pytest?
                "future-fstrings",
                "omegaconf",
                ## OMS: not a fan of this:
                # @ git+https://github.com/caracal-pipeline/scabha2",
                ## ...because it interferes with running scabha2 off a dev branch (i.e. if you have a local dev install of scabha,
                ## pip install stimela will blow it away and replace with master branch...)

#                "ruamel.yaml",  # do we need ruamel when we do pyyaml?
                "munch",
                "omegaconf>=2.1pre1",
                "click",
                "pyparsing",
                "pytest",
                "pydantic",
                "pathos",
                "psutil",
                "rich",
                "dill"
                ],

PACKAGE_NAME = "stimela"
__version__ = "2.0rc2"

packages = set(find_packages())
# these aren't auto-discovered with find_packages() currenty, so add them
packages.add("stimela_tests")
packages.add("scabha_tests")

setup(name=PACKAGE_NAME,
      version=__version__,
      description="Radio interferometry workflow management framework",
      author="Sphesihle Makhathini & Oleg Smirnov & RATT",
      author_email="sphemakh@gmail.com",
      url="https://github.com/caracal-pipeline/stimela2",
      packages=packages,
      # tell it where to find the xx_tests packages
      package_dir={
            "scabha_tests": "tests/scabha_tests",
            "stimela_tests": "tests/stimela_tests"
      },
      include_package_data=True,
      python_requires='>=3.7',
      install_requires=requirements,
      entry_points="""
            [console_scripts]
            stimela = stimela.main:cli
      """,
#      scripts=glob.glob("stimela/cargo/cab/stimela_runscript"),
      classifiers=[],
      )
