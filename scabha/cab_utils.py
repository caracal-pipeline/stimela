import json
from typing import Any

CAB_OUTPUT_PREFIX = "### YIELDING CAB OUTPUT ## "

def yield_output(**kw):
    print(f"{CAB_OUTPUT_PREFIX}{json.dumps(kw)}")
