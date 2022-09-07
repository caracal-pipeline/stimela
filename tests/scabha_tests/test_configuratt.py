import sys
import os.path
import pytest
from scabha import configuratt
from scabha.configuratt import ConfigurattError
from omegaconf import OmegaConf
from typing import *

testdir = os.path.dirname(os.path.abspath(__file__))

def test_includes(path=None):
    path = path or os.path.join(testdir, "testconf.yaml")
    conf, deps = configuratt.load(path, use_sources=[], verbose=True, use_cache=False)
    assert conf.x.y2.z1 == 1

    missing = configuratt.check_requirements(conf, [], strict=False)
    assert len(missing) == 2                   # 2 failed reqs
    assert 'yy' not in conf.requirements2.x    # this one was contingent, so section was deleted
    assert missing[0][2] is None               # this one was contingent, so no error
    assert type(missing[1][2]) is ConfigurattError  # this one was required, so yes error

    try:
        conf, deps = configuratt.load(path, use_sources=[], verbose=True, use_cache=False)
        missing = configuratt.check_requirements(conf, [], strict=True)
        raise RuntimeError("Error was expected here!")
    except configuratt.ConfigurattError as exc:
        print(f"Exception as expected: {exc}")

    nested = ["test_nest_a.yml", "test_nest_b.yml", "test_nest_c.yml"]
    nested = [os.path.join(os.path.dirname(path), name) for name in nested]

    conf1, deps1 = configuratt.load_nested(nested, 
                                            typeinfo=Dict[str, Any], nameattr="_name", verbose=True, use_cache=False)
    conf['nested'] = conf1
    OmegaConf.save(conf, sys.stderr)

    deps.update(deps1)

    print(f"Dependencies are: {deps.get_description()}")


if __name__ == "__main__":
    test_includes(sys.argv[1] if len(sys.argv) > 1 else None)