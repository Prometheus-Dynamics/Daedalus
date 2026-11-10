"""The Python fixture worker: serves the canonical fixture nodes on stdin and stdout."""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "sdk"))
sys.path.insert(0, _HERE)

import nodes  # noqa: E402,F401 - declares the canonical fixture nodes
import showcase  # noqa: E402,F401 - declares the showcase nodes
from daedalus import serve  # noqa: E402
from daedalus.worker import NODES  # noqa: E402

if __name__ == "__main__":
    # The conformance worker serves the canonical fixtures only. The showcase is not in the
    # conformance registry, so declaring it here would be refused at registration.
    serve([declared for declared in NODES if declared.id.startswith("fixture.")])
