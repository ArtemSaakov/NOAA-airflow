"""
A super simple debugging script for debugging in vscode.

Set a breakpoint, import the module that will run that set of
code, and call the relevant function.
"""

import sys
from pathlib import Path
# This is the module being imported for debugging.
from dags.weather_pipeline import fetch_noaa_task

# Adds the file's parent directory to the front of Python's
# path so the module above can be imported and run without
# installing the package or worrying about relative imports.
sys.path.insert(0, str(Path(__file__).parent))

if __name__ == "__main__":
    fetch_noaa_task()
