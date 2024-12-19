#!/usr/bin/env python
from typing import Tuple, List, Optional
import sys
import os.path
import click
from scabha.schema_utils import clickify_parameters

schema_file = os.path.join(os.path.dirname(__file__), "test_clickify.yaml")

@click.command()
@clickify_parameters(schema_file)
def func(name: str, i: int, j: Optional[float] = 1,
         remainder: Optional[List[str]] = None, 
         k: float=2,
         tup: Optional[Tuple[int, str]] = None, 
         files1: Optional[List[str]] = None,
         files2: Optional[List[str]] = None,
         files3: Optional[List[str]] = None,
         output: str = None):
    print(f"name:{name} i:{i} j:{j} k:{k} tup:{tup}")
    print(f"remainder: {remainder}")
    print(f"files1: {files1}")
    print(f"files2: {files2}")
    print(f"files3: {files3}")
    print(f"output: {output}")

if __name__ == "__main__":
    sys.exit(func())

