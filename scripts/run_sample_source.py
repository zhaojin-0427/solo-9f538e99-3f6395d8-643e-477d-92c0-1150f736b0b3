"""Run the sample upstream on the host (no Docker)."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from sample_source.app import main

if __name__ == "__main__":
    main()
