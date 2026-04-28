#!/bin/sh
""":"
SCRIPT_DIR="$(CDPATH= cd "$(dirname "$0")" && pwd)"
exec "${PYTHON:-python3}" "$SCRIPT_DIR/build_python_standalone_bundle.py" "$@"
":"""

import os
import sys


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON = os.environ.get("PYTHON", "python3")
os.execvp(
    PYTHON,
    [
        PYTHON,
        os.path.join(SCRIPT_DIR, "build_python_standalone_bundle.py"),
    ] + sys.argv[1:],
)
