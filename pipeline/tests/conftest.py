"""Pytest configuration: add pipeline/ to sys.path so tests can import api.openf1 and ingest."""

import os
import sys

# Add the pipeline/ directory to sys.path so that `import ingest` and
# `from api import openf1` resolve correctly regardless of where pytest is
# invoked from.
_PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)
