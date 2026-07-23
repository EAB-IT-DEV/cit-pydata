import os
import sys

# Ensure the in-repo source (src-layout) is imported for tests, ahead of any
# version of cit-pydata that happens to be pip-installed in site-packages.
_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
