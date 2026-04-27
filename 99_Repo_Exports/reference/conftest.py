import sys
import os

# Add tests dir to sys.path so we can import fakeredis from it
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tests"))

try:
    import fakeredis
    sys.modules["fakeredis"] = fakeredis
except ImportError:
    pass
