import os
import sys
from dataclasses import dataclass
from typing import Dict

from omegaconf import OmegaConf

from scabha import cargo, configuratt

testdir = os.path.dirname(os.path.abspath(__file__))


@dataclass
class SimpleSchema:
    test: Dict[str, cargo.Parameter]


def test_schema():
    path = os.path.join(testdir, "test_schema.yaml")
    conf, _ = configuratt.load(path, use_sources=[])
    conf = OmegaConf.merge(OmegaConf.structured(SimpleSchema), conf)
    OmegaConf.save(conf, sys.stdout)

    print(type(conf.test.a.default))

    obj = OmegaConf.to_object(conf)
    print(obj)


if __name__ == "__main__":
    test_schema()
