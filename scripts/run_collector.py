"""Run the collector web app on the host (no Docker)."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app import main

if __name__ == "__main__":
    main()
