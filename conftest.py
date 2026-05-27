import sys
import os

_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_root, "generator", "src"))
sys.path.insert(0, os.path.join(_root, "transformations", "src"))

# Ensure Spark workers use the same Python interpreter as the driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
