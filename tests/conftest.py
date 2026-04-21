import os
import sys

# Make the repo root importable so tests can `import command_parser`.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
