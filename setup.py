#!/usr/bin/env python

import os
import sys
from setuptools import setup, find_packages
import glob


requirements = [  "pyyaml",
                  "pytest",
                  "munch",
                  "omegaconf>=2.1",
                  "click",
                  "pyparsing",
                  "pydantic",
                  "pathos",
                  "psutil",
                  "rich",
                  "dill"
]

extras = dict(
      kube = [ "kubernetes", "dask-kubernetes" ]
)

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
      extras_require=extras,
      entry_points="""
            [console_scripts]
            stimela = stimela.main:cli
      """,
#      scripts=glob.glob("stimela/cargo/cab/stimela_runscript"),
      classifiers=[],
      )
