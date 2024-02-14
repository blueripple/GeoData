import areal.py
import importlib.machinery
import importlib.util
from pathlib import Path

script_dir = Path( __file__ ).parent
mymodule_path = str( script_dir.joinpath( '..', 'alpha', 'beta', 'mymodule' ) )
